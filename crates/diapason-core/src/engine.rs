//! Run orchestration: checks the system, starts the workers and the
//! aggregator, fixes T0, and assembles the summary.

use crate::clock::{raw_now_ns, rfc3339, wall_now_ns};
use crate::config::{Arrival, ConfigError, Mode, RunConfig};
use crate::fidelity::{evaluate, FidelityInputs};
use crate::hist::{DelayHists, LatencyStats};
use crate::host::{host_info, raise_nofile, HostInfo};
use crate::protocol::{ConnectErrorKind, FailKind, NotSentReason, Protocol};
use crate::schedule::{Arrivals, RateProfile};
use crate::stats::PhaseCounters;
use crate::summary::*;
use crate::trace::TraceWriter;
use crate::window::{run_aggregator, AggregatorConfig, Observer, WindowSnapshot};
use crate::worker::{run_worker, ConnStats, StartSync, WorkerFinal, WorkerSetup};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, OnceLock};

/// A run started with [`Runner::spawn`].
pub struct RunHandle {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<Result<Summary, RunError>>,
}

impl RunHandle {
    /// Ends the schedule now: no request is scheduled after this point, and
    /// the drain starts at once. The summary says `run.stopped_early`.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Waits for the run to finish.
    pub fn join(self) -> Result<Summary, RunError> {
        self.thread
            .join()
            .map_err(|_| RunError::Runtime("the run thread panicked".into()))?
    }

    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("runtime error: {0}")]
    Runtime(String),
}

impl RunError {
    pub fn exit_code(&self) -> i32 {
        match self {
            RunError::Config(_) => exit::CONFIG,
            RunError::Runtime(_) => exit::RUNTIME,
        }
    }
}

impl From<ConfigError> for RunError {
    fn from(e: ConfigError) -> Self {
        RunError::Config(e.0)
    }
}

/// Builds and runs one load test.
pub struct Runner<P: Protocol> {
    cfg: RunConfig,
    protocol: P,
    observer: Option<Observer>,
}

fn split(n: usize, parts: usize, i: usize) -> usize {
    n / parts + usize::from(i < n % parts)
}

pub fn tool_info() -> ToolInfo {
    ToolInfo {
        name: "diapason".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        git_hash: option_env!("DIAPASON_GIT_HASH")
            .filter(|s| !s.is_empty())
            .map(String::from),
    }
}

/// Checks the replay file once: parseable, non-decreasing.
fn check_replay(cfg: &RunConfig, profile: &Arc<RateProfile>) -> Result<(), RunError> {
    let mut a = Arrivals::new(cfg, profile.clone(), 0, 0, 1)
        .map_err(|e| RunError::Config(format!("load.replay_file: {e}")))?;
    loop {
        match a.next_arrival() {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(e) => return Err(RunError::Config(format!("load.replay_file: {e}"))),
        }
    }
}

impl<P: Protocol> Runner<P> {
    pub fn new(cfg: RunConfig, protocol: P) -> Runner<P> {
        Runner {
            cfg,
            protocol,
            observer: None,
        }
    }

    /// Calls `f` for every window once all workers have reported it.
    pub fn on_window(mut self, f: impl FnMut(&WindowSnapshot) + Send + 'static) -> Self {
        self.observer = Some(Box::new(f));
        self
    }

    /// Delivers every window snapshot on a channel as well.
    pub fn window_channel(mut self) -> (Self, std::sync::mpsc::Receiver<WindowSnapshot>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut prev = self.observer.take();
        self.observer = Some(Box::new(move |w: &WindowSnapshot| {
            if let Some(p) = prev.as_mut() {
                p(w);
            }
            let _ = tx.send(w.clone());
        }));
        (self, rx)
    }

    /// Runs on the calling thread until the end of the schedule and the drain.
    pub fn run(self) -> Result<Summary, RunError> {
        self.run_with_stop(Arc::new(AtomicBool::new(false)))
    }

    /// Runs on a new thread; the handle can stop the run early.
    pub fn spawn(self) -> RunHandle {
        let stop = Arc::new(AtomicBool::new(false));
        let s = stop.clone();
        let thread = std::thread::Builder::new()
            .name("diapason-run".into())
            .spawn(move || self.run_with_stop(s))
            .expect("spawn the run thread");
        RunHandle { stop, thread }
    }

    fn run_with_stop(self, stop: Arc<AtomicBool>) -> Result<Summary, RunError> {
        let Runner {
            mut cfg,
            protocol,
            observer,
        } = self;
        cfg.validate()?;
        let mut warnings = Vec::new();

        // System limits.
        let fd = raise_nofile();
        let host = host_info(fd);
        let depth = protocol
            .max_outstanding()
            .unwrap_or(cfg.connections.pipeline)
            .max(1);
        let threads = fit_threads(
            &cfg,
            depth,
            cfg.threads.unwrap_or(host.cores),
            &mut warnings,
        );
        let conns_max: usize = (0..threads)
            .map(|g| worker_share(&cfg, depth, g, threads).1)
            .sum();
        check_fd(&host, &cfg, conns_max, threads)?;
        let port_range_size = port_range(&host, conns_max, &mut warnings);
        cfg.threads = Some(threads);
        let prep = prepare(&mut cfg, &mut warnings)?;
        protocol.set_seed(prep.seed);
        let (observer, live) = live_observer(&cfg, prep.seed, observer)?;
        let metrics_addr = live.as_ref().map(|(_, s)| s.addr.to_string());

        let cfg = Arc::new(cfg);
        let protocol = Arc::new(protocol);
        let (wtx, wrx) = std::sync::mpsc::channel();
        let local = start_workers(
            protocol.clone(),
            cfg.clone(),
            &prep,
            0..threads,
            threads,
            depth,
            wtx,
            stop,
            host.cores,
        )?;

        // T0, a little in the future so that every worker is ready.
        local.sync.barrier.wait();
        let lead = 20_000_000;
        let t0_raw = raw_now_ns() + lead;
        let t0_wall = wall_now_ns() + lead;
        local.sync.t0.set(t0_raw).expect("T0 is set once");
        local.sync.barrier.wait();

        let agg = start_aggregator(&cfg, &prep, threads, t0_wall, wrx, observer)?;
        let (finals, trace_rows, mut runtime_errors) = local.join()?;
        let wall_duration_s = raw_now_ns().saturating_sub(t0_raw) as f64 / 1e9;
        let agg = agg
            .join()
            .map_err(|_| RunError::Runtime("the aggregator thread panicked".into()))?;
        let summary = conclude(Conclusion {
            cfg: &cfg,
            host,
            prep: &prep,
            t0_wall,
            finals: &finals,
            agg: &agg,
            port_range_size,
            wall_duration_s,
            protocol: protocol.describe(),
            protocol_report: protocol.report(),
            trace_rows,
            metrics_addr,
            warnings,
            runtime_errors: std::mem::take(&mut runtime_errors),
            distributed: None,
        });
        Ok(summary)
    }
}

/// The share of global worker `gid` among `total`: connections at T0,
/// connections at most, in-flight bound.
pub(crate) fn worker_share(
    cfg: &RunConfig,
    depth: usize,
    gid: usize,
    total: usize,
) -> (usize, usize, usize) {
    let conns = split(cfg.connections.count, total, gid);
    let conns_max = split(
        cfg.connections.max.unwrap_or(cfg.connections.count),
        total,
        gid,
    );
    let in_flight = match cfg.connections.max_in_flight {
        Some(t) => split(t, total, gid),
        None => conns_max * depth,
    };
    (conns, conns_max, in_flight)
}

/// Lowers the number of workers so that each has a connection and an
/// in-flight slot.
pub(crate) fn fit_threads(
    cfg: &RunConfig,
    depth: usize,
    want: usize,
    warnings: &mut Vec<String>,
) -> usize {
    let mut threads = want.max(1);
    let conns_max = cfg.connections.max.unwrap_or(cfg.connections.count);
    let max_in_flight = cfg.connections.max_in_flight.unwrap_or(conns_max * depth);
    if threads > conns_max {
        warnings.push(format!(
            "threads lowered from {threads} to {conns_max}: a worker needs at least one connection"
        ));
        threads = conns_max;
    }
    if threads > max_in_flight {
        warnings.push(format!(
            "threads lowered from {threads} to {max_in_flight}: a worker needs at least one in-flight slot"
        ));
        threads = max_in_flight;
    }
    threads
}

pub(crate) fn check_fd(
    host: &HostInfo,
    cfg: &RunConfig,
    conns_max: usize,
    threads: usize,
) -> Result<(), RunError> {
    let need = conns_max as u64 + cfg.system.fd_margin + 16 * threads as u64;
    if need > host.fd_soft {
        return Err(RunError::Config(format!(
            "file-descriptor limit too low: RLIMIT_NOFILE is {} (hard {}), the run needs {} \
             ({} connections at most + fd_margin {} + 16 per thread for {} threads)",
            host.fd_soft, host.fd_hard, need, conns_max, cfg.system.fd_margin, threads
        )));
    }
    Ok(())
}

pub(crate) fn port_range(
    host: &HostInfo,
    conns_max: usize,
    warnings: &mut Vec<String>,
) -> Option<u64> {
    let n = host
        .ephemeral_ports
        .map(|[lo, hi]| (hi.saturating_sub(lo) + 1) as u64)?;
    if conns_max as u64 > n {
        warnings.push(format!(
            "{conns_max} connections to one destination exceed the ephemeral port range ({n} ports); \
             connects beyond it will fail with port_exhausted"
        ));
    }
    Some(n)
}

/// What every node derives from the configuration.
pub(crate) struct Prepared {
    pub seed: u64,
    pub profile: Arc<RateProfile>,
    pub measure: Option<usize>,
    pub measure_iv: (u64, u64),
}

/// Fixes the seed in `cfg`, builds the profile and finds the measurement
/// phase.
pub(crate) fn prepare(
    cfg: &mut RunConfig,
    warnings: &mut Vec<String>,
) -> Result<Prepared, RunError> {
    let seed = cfg.seed.unwrap_or_else(rand::random);
    cfg.seed = Some(seed);
    let profile = Arc::new(RateProfile::from_config(cfg)?);
    if cfg.load.arrival == Arrival::Replay && cfg.mode == Mode::Open {
        check_replay(cfg, &profile)?;
    }
    let measure = profile
        .phases
        .iter()
        .position(|p| p.name == cfg.load.measure);
    let measure_iv = match measure {
        Some(i) => (profile.phases[i].start_ns, profile.phases[i].end_ns),
        None => (0, profile.end_ns),
    };
    if measure.is_none() {
        warnings.push(format!(
            "no phase is named '{}': fidelity checks cover the whole run",
            cfg.load.measure
        ));
    }
    Ok(Prepared {
        seed,
        profile,
        measure,
        measure_iv,
    })
}

type Live = Option<(Arc<crate::live::LiveMetrics>, crate::live::LiveServer)>;

/// Starts the live endpoint if configured, and chains it with the observer.
pub(crate) fn live_observer(
    cfg: &RunConfig,
    seed: u64,
    observer: Option<Observer>,
) -> Result<(Option<Observer>, Live), RunError> {
    let live = match &cfg.output.metrics {
        Some(addr) => {
            let m = Arc::new(crate::live::LiveMetrics::new(cfg.mode.as_str(), seed));
            let server = crate::live::LiveServer::start(addr, m.clone()).map_err(|e| {
                RunError::Config(format!("output.metrics: cannot listen on {addr}: {e}"))
            })?;
            Some((m, server))
        }
        None => None,
    };
    let observer: Option<Observer> = match (&live, observer) {
        (None, o) => o,
        (Some((m, _)), mut o) => {
            let m = m.clone();
            Some(Box::new(move |w: &WindowSnapshot| {
                m.publish(w);
                if let Some(o) = o.as_mut() {
                    o(w);
                }
            }))
        }
    };
    Ok((observer, live))
}

/// Workers started on this node, waiting at the start-up rendezvous.
pub(crate) struct LocalWorkers {
    pub sync: Arc<StartSync>,
    handles: Vec<std::thread::JoinHandle<WorkerFinal>>,
    trace: Option<TraceWriter>,
}

impl LocalWorkers {
    /// Waits for the workers; returns their finals, the trace rows written,
    /// and runtime errors.
    pub fn join(self) -> Result<(Vec<WorkerFinal>, u64, Vec<String>), RunError> {
        let mut finals = Vec::with_capacity(self.handles.len());
        for h in self.handles {
            finals.push(
                h.join()
                    .map_err(|_| RunError::Runtime("a worker thread panicked".into()))?,
            );
        }
        let mut errors: Vec<String> = finals
            .iter()
            .filter_map(|f| f.error.as_ref().map(|e| format!("worker {}: {e}", f.id)))
            .collect();
        let mut rows = 0;
        if let Some(t) = self.trace {
            match t.finish() {
                Ok(n) => rows = n,
                Err(e) => errors.push(format!("trace: {e}")),
            }
        }
        Ok((finals, rows, errors))
    }
}

/// Starts the workers `gids` of a run of `total` workers. They open their
/// connections and wait at the rendezvous (`sync`, `gids.len() + 1` parties).
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_workers<P: Protocol>(
    protocol: Arc<P>,
    cfg: Arc<RunConfig>,
    prep: &Prepared,
    gids: std::ops::Range<usize>,
    total: usize,
    depth: usize,
    window_tx: std::sync::mpsc::Sender<crate::stats::WindowMsg>,
    stop: Arc<AtomicBool>,
    cores: usize,
) -> Result<LocalWorkers, RunError> {
    let trace = match &cfg.output.trace {
        Some(p) => Some(
            TraceWriter::create(p, cfg.output.trace_sample).map_err(|e| {
                RunError::Config(format!("cannot create trace {}: {e}", p.display()))
            })?,
        ),
        None => None,
    };
    let sync = Arc::new(StartSync {
        barrier: Barrier::new(gids.len() + 1),
        t0: OnceLock::new(),
    });
    let mut handles = Vec::with_capacity(gids.len());
    for (local, gid) in gids.enumerate() {
        let (conns, conns_max, max_in_flight) = worker_share(&cfg, depth, gid, total);
        let setup = WorkerSetup {
            id: gid,
            workers: total,
            conns,
            conns_max,
            max_in_flight,
            depth,
            stop: stop.clone(),
            protocol: protocol.clone(),
            cfg: cfg.clone(),
            profile: prep.profile.clone(),
            seed: prep.seed,
            sync: sync.clone(),
            window_tx: window_tx.clone(),
            trace: trace.as_ref().map(|t| t.sink()),
            pin: cfg.system.pin_threads.then(|| local % cores.max(1)),
        };
        let h = std::thread::Builder::new()
            .name(format!("diapason-w{gid}"))
            .spawn(move || run_worker(setup))
            .map_err(|e| RunError::Runtime(format!("cannot start worker thread: {e}")))?;
        handles.push(h);
    }
    Ok(LocalWorkers {
        sync,
        handles,
        trace,
    })
}

pub(crate) fn start_aggregator(
    cfg: &RunConfig,
    prep: &Prepared,
    total: usize,
    t0_wall: u64,
    rx: std::sync::mpsc::Receiver<crate::stats::WindowMsg>,
    observer: Option<Observer>,
) -> Result<std::thread::JoinHandle<crate::window::AggregatorOutput>, RunError> {
    let agg_cfg = AggregatorConfig {
        workers: total,
        phases: prep.profile.phases.len(),
        window_ns: cfg.output.window.as_nanos(),
        measure: prep.measure_iv,
        histogram_log: cfg.output.histogram_log.clone(),
        windows_file: cfg.output.windows.clone(),
        t0_wall_ns: t0_wall,
    };
    std::thread::Builder::new()
        .name("diapason-agg".into())
        .spawn(move || run_aggregator(rx, agg_cfg, observer))
        .map_err(|e| RunError::Runtime(format!("cannot start aggregator thread: {e}")))
}

pub(crate) struct Conclusion<'a> {
    pub cfg: &'a RunConfig,
    pub host: HostInfo,
    pub prep: &'a Prepared,
    pub t0_wall: u64,
    pub finals: &'a [WorkerFinal],
    pub agg: &'a crate::window::AggregatorOutput,
    pub port_range_size: Option<u64>,
    pub wall_duration_s: f64,
    pub protocol: String,
    pub protocol_report: serde_json::Value,
    pub trace_rows: u64,
    pub metrics_addr: Option<String>,
    pub warnings: Vec<String>,
    pub runtime_errors: Vec<String>,
    pub distributed: Option<Distributed>,
}

/// Builds the summary from the workers' finals and the aggregator's output.
pub(crate) fn conclude(c: Conclusion) -> Summary {
    let cfg = c.cfg;
    let mut warnings = c.warnings;
    let mut runtime_errors = c.runtime_errors;
    let outputs = Outputs {
        summary: cfg.output.summary.as_ref().map(|p| p.display().to_string()),
        histogram_log: cfg
            .output
            .histogram_log
            .as_ref()
            .map(|p| p.display().to_string()),
        windows: cfg.output.windows.as_ref().map(|p| p.display().to_string()),
        windows_written: c.agg.windows,
        trace: cfg.output.trace.as_ref().map(|p| p.display().to_string()),
        trace_rows: c.trace_rows,
        trace_dropped: c.finals.iter().map(|f| f.trace_dropped).sum(),
        trace_sample: cfg.output.trace_sample,
        metrics: c.metrics_addr,
    };
    if let Some(e) = &c.agg.error {
        runtime_errors.push(e.clone());
    }
    if outputs.trace_dropped > 0 {
        warnings.push(format!(
            "{} trace rows dropped: the trace writer could not keep up",
            outputs.trace_dropped
        ));
    }
    // The configuration as used: the in-flight bound is the workers' total.
    let mut used = cfg.clone();
    used.connections.max_in_flight = Some(c.finals.iter().map(|f| f.max_in_flight).sum());
    let mut summary = assemble(Assembly {
        cfg: &used,
        host: c.host,
        seed: c.prep.seed,
        t0_wall: c.t0_wall,
        profile: &c.prep.profile,
        measure: c.prep.measure,
        finals: c.finals,
        phase_hists: &c.agg.phase_hists,
        burstiness: &c.agg.burstiness,
        port_range_size: c.port_range_size,
        wall_duration_s: c.wall_duration_s,
        protocol: c.protocol,
        protocol_report: c.protocol_report,
        outputs,
        warnings,
        runtime_errors,
    });
    summary.distributed = c.distributed;
    summary
}

struct Assembly<'a> {
    cfg: &'a RunConfig,
    host: HostInfo,
    seed: u64,
    t0_wall: u64,
    profile: &'a RateProfile,
    measure: Option<usize>,
    finals: &'a [WorkerFinal],
    phase_hists: &'a [DelayHists],
    burstiness: &'a crate::window::Burstiness,
    port_range_size: Option<u64>,
    wall_duration_s: f64,
    protocol: String,
    protocol_report: serde_json::Value,
    outputs: Outputs,
    warnings: Vec<String>,
    runtime_errors: Vec<String>,
}

fn fail_map(a: &[u64; 6]) -> BTreeMap<String, u64> {
    FailKind::ALL
        .iter()
        .filter(|k| a[k.index()] > 0)
        .map(|k| (k.as_str().to_string(), a[k.index()]))
        .collect()
}

fn not_sent_map(a: &[u64; 3]) -> BTreeMap<String, u64> {
    NotSentReason::ALL
        .iter()
        .filter(|k| a[k.index()] > 0)
        .map(|k| (k.as_str().to_string(), a[k.index()]))
        .collect()
}

fn connect_map(a: &[u64; 7]) -> BTreeMap<String, u64> {
    ConnectErrorKind::ALL
        .iter()
        .filter(|k| a[k.index()] > 0)
        .map(|k| (k.as_str().to_string(), a[k.index()]))
        .collect()
}

fn assemble(a: Assembly) -> Summary {
    let cfg = a.cfg;
    let nphases = a.profile.phases.len();

    // Per-phase counters, all workers merged.
    let mut merged = vec![PhaseCounters::default(); nphases];
    for f in a.finals {
        for (p, pc) in f.phases.iter().enumerate() {
            merged[p].add(pc);
        }
    }

    let mut cons = Conservation::default();
    let mut failed = [0u64; 6];
    let mut not_sent = [0u64; 3];
    for pc in &merged {
        cons.scheduled += pc.scheduled;
        cons.completed += pc.completed;
        cons.in_flight_at_end += pc.in_flight_at_end;
        for (a, b) in failed.iter_mut().zip(&pc.failed) {
            *a += b;
        }
        for (a, b) in not_sent.iter_mut().zip(&pc.not_sent) {
            *a += b;
        }
    }
    cons.failed = fail_map(&failed);
    cons.failed_total = failed.iter().sum();
    cons.not_sent = not_sent_map(&not_sent);
    cons.not_sent_total = not_sent.iter().sum();
    cons.balance = cons.scheduled as i64
        - (cons.completed + cons.failed_total + cons.not_sent_total + cons.in_flight_at_end) as i64;
    cons.ok = cons.balance == 0;

    let phases: Vec<PhaseSummary> = a
        .profile
        .phases
        .iter()
        .enumerate()
        .map(|(i, span)| {
            let pc = &merged[i];
            let d = span.duration_s();
            let h = &a.phase_hists[i];
            let dur_ns = (span.end_ns - span.start_ns) as f64;
            let sent_mean = pc.sent_area as f64 / dur_ns;
            let throughput = pc.departed_in_interval as f64 / d;
            let mean_server = if pc.departed_in_interval > 0 {
                pc.departed_ns_in_interval as f64 / pc.departed_in_interval as f64 / 1e9
            } else {
                0.0
            };
            let predicted = throughput * mean_server;
            PhaseSummary {
                name: span.name.clone(),
                start_s: span.start_ns as f64 / 1e9,
                end_s: span.end_ns as f64 / 1e9,
                duration_s: d,
                measured: a.measure == Some(i),
                nominal_rate: span.nominal_rate,
                scheduled: pc.scheduled,
                offered_rate: pc.scheduled as f64 / d,
                completed: pc.completed,
                success: pc.success,
                failed: fail_map(&pc.failed),
                not_sent: not_sent_map(&pc.not_sent),
                in_flight_at_end: pc.in_flight_at_end,
                status: pc.status.iter().map(|(c, n)| (c.to_string(), *n)).collect(),
                achieved_rate_by_intended: pc.completed as f64 / d,
                achieved_rate_by_completion: pc.completed_in_interval as f64 / d,
                success_ratio: if pc.scheduled > 0 {
                    pc.success as f64 / pc.scheduled as f64
                } else {
                    0.0
                },
                latency: Latency {
                    corrected: LatencyStats::from_hist(&h.corrected),
                    server: LatencyStats::from_hist(&h.server),
                    connection_wait: LatencyStats::from_hist(&h.conn_wait),
                    generator_lag: LatencyStats::from_hist(&h.gen_lag),
                },
                in_flight: InFlight {
                    sent_mean,
                    sent_max: pc.sent_max,
                    system_mean: pc.system_area as f64 / dur_ns,
                    system_max: pc.system_max,
                },
                scenarios: {
                    let names = crate::template::ScenarioPicker::names(cfg);
                    let total_w: f64 = cfg.scenarios.iter().map(|s| s.weight).sum();
                    names
                        .iter()
                        .enumerate()
                        .map(|(k, name)| {
                            let c = pc.scenarios.get(k).cloned().unwrap_or_default();
                            ScenarioSummary {
                                name: name.clone(),
                                weight_share: if cfg.scenarios.is_empty() {
                                    1.0
                                } else {
                                    cfg.scenarios[k].weight / total_w
                                },
                                share: if pc.scheduled > 0 {
                                    c.scheduled as f64 / pc.scheduled as f64
                                } else {
                                    0.0
                                },
                                scheduled: c.scheduled,
                                completed: c.completed,
                                success: c.success,
                                failed: c.failed,
                                not_sent: c.not_sent,
                                in_flight_at_end: c.in_flight_at_end,
                            }
                        })
                        .collect()
                },
                little: Little {
                    departure_rate: throughput,
                    mean_time_in_flight_s: mean_server,
                    predicted_in_flight: predicted,
                    measured_in_flight: sent_mean,
                    relative_error: (predicted > 0.0).then(|| (sent_mean - predicted) / predicted),
                },
            }
        })
        .collect();

    let mut conn = ConnStats::default();
    for f in a.finals {
        conn.add(&f.conn);
    }

    // Counters of the measurement interval: one phase, or all of them.
    let measured_of = |phases: &[PhaseCounters]| -> PhaseCounters {
        match a.measure {
            Some(i) => phases[i].clone(),
            None => {
                let mut t = PhaseCounters::default();
                for p in phases {
                    t.add(p);
                }
                t
            }
        }
    };
    let measured = measured_of(&merged);
    let per_worker: Vec<PhaseCounters> = a.finals.iter().map(|f| measured_of(&f.phases)).collect();
    let measured_hists = match a.measure {
        Some(i) => a.phase_hists[i].clone(),
        None => {
            let mut t = DelayHists::new();
            for h in a.phase_hists {
                t.add(h);
            }
            t
        }
    };

    let workers: Vec<WorkerSummary> = a
        .finals
        .iter()
        .zip(&per_worker)
        .map(|(f, pc)| {
            let wall = f.wall_ns.max(1) as f64;
            let busy = f.busy_ns as f64 / wall;
            WorkerSummary {
                id: f.id,
                connections: f.connections,
                max_in_flight: f.max_in_flight,
                scheduled: pc.scheduled,
                sent: pc.sent_in_interval,
                send_ratio: (pc.scheduled > 0)
                    .then(|| pc.sent_in_interval as f64 / pc.scheduled as f64),
                wall_s: f.wall_ns as f64 / 1e9,
                busy_share: busy.clamp(0.0, 1.0),
                spin_share: f.spin_ns as f64 / wall,
                cpu_s: f.cpu.cpu_ns as f64 / 1e9,
                runqueue_s: f.cpu.runqueue_ns.map(|n| n as f64 / 1e9),
                runqueue_share: f.cpu.runqueue_ns.map(|n| n as f64 / wall),
                involuntary_switches: f.cpu.involuntary_switches,
            }
        })
        .collect();

    let fidelity = evaluate(&FidelityInputs {
        mode: cfg.mode,
        cfg: &cfg.fidelity,
        measured: &measured,
        per_worker: &per_worker,
        hists: &measured_hists,
        burstiness: a.burstiness,
        conn: &conn,
        workers: &workers,
    });

    let thresholds: Vec<crate::threshold::ThresholdResult> = cfg
        .thresholds
        .iter()
        .map(|t| {
            let phase = t.phase.clone().unwrap_or_else(|| cfg.load.measure.clone());
            let th = crate::threshold::Threshold::parse(&t.expr).expect("validated");
            let p = phases.iter().find(|p| p.name == phase).expect("validated");
            let (value, limit, ok) = th.evaluate(p);
            crate::threshold::ThresholdResult {
                expr: t.expr.clone(),
                phase,
                value,
                limit,
                ok,
            }
        })
        .collect();

    let (exit_code, exit_reason) = if !a.runtime_errors.is_empty() {
        (
            exit::RUNTIME,
            format!("runtime error: {}", a.runtime_errors.join("; ")),
        )
    } else if !cons.ok {
        (
            exit::CONSERVATION,
            format!(
                "conservation broken: balance {} (this is a bug in diapason)",
                cons.balance
            ),
        )
    } else if !fidelity.ok {
        let failed: Vec<&str> = fidelity
            .checks
            .iter()
            .filter(|c| !c.ok)
            .map(|c| c.name.as_str())
            .collect();
        (
            exit::FIDELITY,
            format!("fidelity checks failed: {}", failed.join(", ")),
        )
    } else if thresholds.iter().any(|t| !t.ok) {
        let failed: Vec<&str> = thresholds
            .iter()
            .filter(|t| !t.ok)
            .map(|t| t.expr.as_str())
            .collect();
        (
            exit::THRESHOLD,
            format!("thresholds failed: {}", failed.join(", ")),
        )
    } else {
        (exit::OK, "ok".to_string())
    };

    let mut warnings = a.warnings;
    let short = conn.requested - conn.established.min(conn.requested);
    if short > 0 {
        warnings.push(format!(
            "{short} of {} connections could not be opened before T0",
            conn.requested
        ));
    }

    Summary {
        schema: SCHEMA.into(),
        schema_version: SCHEMA_VERSION,
        tool: tool_info(),
        mode: cfg.mode.as_str().into(),
        seed: a.seed,
        started_at: StartedAt {
            unix_ns: a.t0_wall,
            rfc3339: rfc3339(a.t0_wall),
        },
        host: a.host,
        config: serde_json::to_value(cfg).unwrap_or(serde_json::Value::Null),
        run: RunInfo {
            scheduled_duration_s: a.profile.end_ns as f64 / 1e9,
            wall_duration_s: a.wall_duration_s,
            threads: a.finals.len(),
            measure_phase: a.measure.map(|i| a.profile.phases[i].name.clone()),
            stopped_early: a.finals.iter().any(|f| f.stopped),
            protocol: a.protocol,
        },
        conservation: cons,
        phases,
        connections: ConnectionsSummary {
            requested: conn.requested,
            established: conn.established,
            shortfall: connect_map(&conn.shortfall),
            connect_attempts: conn.connect_attempts,
            connect_errors: connect_map(&conn.connect_errors),
            reconnects: conn.reconnects,
            closes: Closes {
                server: conn.closes_server,
                client: conn.closes_client,
                error: conn.closes_error,
            },
            requeued: conn.requeued,
            opened_on_demand: conn.opened_on_demand,
            peak_open: conn.peak_open,
            cancelled: conn.cancelled,
            port_range_size: a.port_range_size,
        },
        workers,
        fidelity,
        thresholds,
        protocol_report: a.protocol_report,
        outputs: a.outputs,
        warnings,
        distributed: None,
        exit_code,
        exit_reason,
    }
}

impl Summary {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("summary serializes")
    }

    pub fn measured_phase(&self) -> Option<&PhaseSummary> {
        self.phases.iter().find(|p| p.measured)
    }
}
