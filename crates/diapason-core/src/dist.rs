//! Distributed mode: a coordinator and agents on several client nodes.
//!
//! The run has `total` workers, numbered globally; agent `a` runs workers
//! `[first_a, first_a + threads_a)`. Each worker takes its share of the
//! schedule and of the connections from its global number, exactly as in a
//! single-node run with `total` threads, so the requests (seq, intended time,
//! content) are the same.
//!
//! Agents do not summarise anything: they forward their workers' raw windows
//! (histograms, counts, slices) and final counters to the coordinator, which
//! runs the same aggregator and builds the same summary as a single-node run.
//!
//! Clocks: the coordinator estimates each agent's wall-clock offset with
//! request/response exchanges and keeps the one with the smallest round trip
//! (offset known to within half of it). It then sends each agent T0 expressed
//! in that agent's wall clock; the agent maps it onto its monotonic clock.
//!
//! Wire format: messages are JSON, each preceded by its length as a 4-byte
//! big-endian integer.

use crate::clock::{raw_now_ns, wall_now_ns};
use crate::config::{ConfigError, RunConfig};
use crate::engine::{
    check_fd, conclude, fit_threads, prepare, start_aggregator, start_workers, worker_share,
    Conclusion, RunError,
};
use crate::host::{host_info, raise_nofile, HostInfo};
use crate::protocol::Protocol;
use crate::stats::WindowMsg;
use crate::summary::{AgentSummary, Distributed, Summary};
use crate::window::{Observer, WindowSnapshot};
use crate::worker::WorkerFinal;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MAX_FRAME: usize = 256 << 20;
const CLOCK_ROUNDS: usize = 32;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Msg {
    /// Agent to coordinator, on connection.
    Hello {
        version: String,
        host: HostInfo,
    },
    /// Coordinator to agent: what to run.
    Setup {
        config: Box<RunConfig>,
        first_worker: usize,
        threads: usize,
        total: usize,
    },
    /// Agent: connections opened, workers waiting for T0.
    Ready {
        protocol: String,
        depth: usize,
        warnings: Vec<String>,
    },
    Ping {
        t1: u64,
    },
    Pong {
        t1: u64,
        t2: u64,
        t3: u64,
    },
    /// Coordinator: T0 in the agent's wall clock, Unix ns.
    Start {
        t0_wall: u64,
    },
    Stop,
    Window {
        msg: WindowMsg,
    },
    Final {
        finals: Vec<WorkerFinal>,
        t0_monotonic: u64,
        trace_rows: u64,
        protocol_report: serde_json::Value,
        errors: Vec<String>,
    },
    Error {
        message: String,
        config: bool,
    },
}

fn send(w: &mut impl Write, m: &Msg) -> io::Result<()> {
    let body = serde_json::to_vec(m).map_err(io::Error::other)?;
    w.write_all(&(body.len() as u32).to_be_bytes())?;
    w.write_all(&body)?;
    w.flush()
}

fn recv(r: &mut impl Read) -> io::Result<Msg> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(io::Error::other(format!("frame of {n} bytes")));
    }
    let mut body = vec![0u8; n];
    r.read_exact(&mut body)?;
    serde_json::from_slice(&body).map_err(|e| io::Error::other(format!("bad message: {e}")))
}

/// Trace files of agents get a prefix naming their workers, so that agents
/// sharing a file system do not write to the same file.
fn agent_trace_path(p: &std::path::Path, first: usize, threads: usize) -> std::path::PathBuf {
    let name = p
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    p.with_file_name(format!("w{first}-{}.{name}", first + threads - 1))
}

/// Serves coordinators on `listen`, one run at a time. `make` builds the
/// protocol from the run's configuration. `skew_ns` is added to every
/// wall-clock reading, to test the offset correction on one machine.
pub fn serve_agent<P, F>(
    listen: &str,
    make: F,
    skew_ns: i64,
    on_listen: impl FnOnce(SocketAddr),
) -> io::Result<()>
where
    P: Protocol,
    F: Fn(&RunConfig) -> Result<P, ConfigError>,
{
    raise_nofile();
    let listener = TcpListener::bind(listen)?;
    on_listen(listener.local_addr()?);
    for conn in listener.incoming() {
        let conn = match conn {
            Ok(c) => c,
            Err(_) => continue,
        };
        let _ = conn.set_nodelay(true);
        if let Err(e) = agent_session(conn, &make, skew_ns) {
            eprintln!("diapason agent: session ended: {e}");
        }
    }
    Ok(())
}

fn agent_session<P, F>(stream: TcpStream, make: &F, skew_ns: i64) -> Result<(), String>
where
    P: Protocol,
    F: Fn(&RunConfig) -> Result<P, ConfigError>,
{
    let wall = move || (wall_now_ns() as i64 + skew_ns) as u64;
    let mut rd = stream.try_clone().map_err(|e| e.to_string())?;
    let wr = Arc::new(Mutex::new(stream));
    let tx = |m: &Msg| -> Result<(), String> {
        send(&mut *wr.lock().unwrap(), m).map_err(|e| e.to_string())
    };
    let host = host_info(raise_nofile());
    tx(&Msg::Hello {
        version: env!("CARGO_PKG_VERSION").into(),
        host: host.clone(),
    })?;
    let (mut cfg, first, threads, total) = match recv(&mut rd).map_err(|e| e.to_string())? {
        Msg::Setup {
            config,
            first_worker,
            threads,
            total,
        } => (*config, first_worker, threads, total),
        other => return Err(format!("expected setup, got {other:?}")),
    };
    let fail = |e: RunError| {
        let config = matches!(e, RunError::Config(_));
        let _ = tx(&Msg::Error {
            message: e.to_string(),
            config,
        });
        e.to_string()
    };
    // The coordinator writes the summary, windows, logs and live metrics.
    cfg.output.summary = None;
    cfg.output.windows = None;
    cfg.output.histogram_log = None;
    cfg.output.metrics = None;
    cfg.output.trace = cfg
        .output
        .trace
        .as_deref()
        .map(|p| agent_trace_path(p, first, threads));
    let proto = make(&cfg).map_err(|e| fail(e.into()))?;
    let depth = proto
        .max_outstanding()
        .unwrap_or(cfg.connections.pipeline)
        .max(1);
    let mut warnings = Vec::new();
    let conns_max: usize = (first..first + threads)
        .map(|g| worker_share(&cfg, depth, g, total).1)
        .sum();
    check_fd(&host, &cfg, conns_max, threads).map_err(fail)?;
    let prep = prepare(&mut cfg, &mut warnings).map_err(fail)?;
    proto.set_seed(prep.seed);
    let proto = Arc::new(proto);
    let stop = Arc::new(AtomicBool::new(false));
    let (wtx, wrx) = std::sync::mpsc::channel::<WindowMsg>();
    let local = start_workers(
        proto.clone(),
        Arc::new(cfg),
        &prep,
        first..first + threads,
        total,
        depth,
        wtx,
        stop.clone(),
        host.cores,
    )
    .map_err(fail)?;
    local.sync.barrier.wait();
    tx(&Msg::Ready {
        protocol: proto.describe(),
        depth,
        warnings,
    })?;

    // Clock exchanges, then T0.
    let t0_wall = loop {
        match recv(&mut rd) {
            Ok(Msg::Ping { t1 }) => {
                let t2 = wall();
                let mut w = wr.lock().unwrap();
                let t3 = wall();
                send(&mut *w, &Msg::Pong { t1, t2, t3 }).map_err(|e| e.to_string())?;
            }
            Ok(Msg::Start { t0_wall }) => break Ok(t0_wall),
            Ok(other) => break Err(format!("expected start, got {other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    let t0_wall = match t0_wall {
        Ok(t) => t,
        Err(e) => {
            // Release the workers: they run an empty schedule and stop.
            stop.store(true, Ordering::Relaxed);
            let _ = local.sync.t0.set(raw_now_ns());
            local.sync.barrier.wait();
            let _ = local.join();
            return Err(e);
        }
    };
    let t0_raw = (raw_now_ns() as i64 + (t0_wall as i64 - wall() as i64)) as u64;
    local.sync.t0.set(t0_raw).expect("T0 is set once");
    local.sync.barrier.wait();

    // Stop requests and a vanished coordinator end the schedule.
    {
        let stop = stop.clone();
        std::thread::spawn(move || loop {
            match recv(&mut rd) {
                Ok(Msg::Stop) | Err(_) => {
                    stop.store(true, Ordering::Relaxed);
                    return;
                }
                Ok(_) => {}
            }
        });
    }
    let fwd = {
        let wr = wr.clone();
        std::thread::spawn(move || {
            for msg in wrx {
                if send(&mut *wr.lock().unwrap(), &Msg::Window { msg }).is_err() {
                    return;
                }
            }
        })
    };
    let (finals, trace_rows, errors) = local.join().map_err(|e| e.to_string())?;
    let _ = fwd.join();
    tx(&Msg::Final {
        finals,
        t0_monotonic: t0_raw,
        trace_rows,
        protocol_report: proto.report(),
        errors,
    })
}

struct Agent {
    address: String,
    host: HostInfo,
    stream: TcpStream,
    first: usize,
    threads: usize,
    protocol: String,
    depth: usize,
    warnings: Vec<String>,
    offset_ns: i64,
    rtt_ns: u64,
}

/// Runs one load test on several agents.
pub struct Coordinator {
    cfg: RunConfig,
    agents: Vec<String>,
    observer: Option<Observer>,
}

fn rt(e: impl std::fmt::Display) -> RunError {
    RunError::Runtime(e.to_string())
}

impl Coordinator {
    pub fn new(cfg: RunConfig, agents: Vec<String>) -> Coordinator {
        Coordinator {
            cfg,
            agents,
            observer: None,
        }
    }

    /// Calls `f` for every window once every worker of every agent has
    /// reported it.
    pub fn on_window(mut self, f: impl FnMut(&WindowSnapshot) + Send + 'static) -> Self {
        self.observer = Some(Box::new(f));
        self
    }

    pub fn run(self) -> Result<Summary, RunError> {
        self.run_with_stop(Arc::new(AtomicBool::new(false)))
    }

    pub fn run_with_stop(self, stop: Arc<AtomicBool>) -> Result<Summary, RunError> {
        let Coordinator {
            mut cfg,
            agents: addrs,
            observer,
        } = self;
        cfg.validate()?;
        if addrs.is_empty() {
            return Err(RunError::Config("no agent given".into()));
        }
        let mut warnings = Vec::new();
        let host = host_info(raise_nofile());
        let seed = cfg.seed.unwrap_or_else(rand::random);
        cfg.seed = Some(seed);

        // 1. Connect and hear from every agent.
        let mut agents = Vec::new();
        for a in &addrs {
            let sa = a
                .to_socket_addrs()
                .map_err(|e| RunError::Config(format!("agent {a}: {e}")))?
                .next()
                .ok_or_else(|| RunError::Config(format!("agent {a}: no address")))?;
            let mut stream = TcpStream::connect_timeout(&sa, Duration::from_secs(5))
                .map_err(|e| RunError::Runtime(format!("agent {a}: {e}")))?;
            let _ = stream.set_nodelay(true);
            let host = match recv(&mut stream).map_err(rt)? {
                Msg::Hello { version, host } => {
                    if version != env!("CARGO_PKG_VERSION") {
                        warnings.push(format!(
                            "agent {a} runs diapason {version}, the coordinator {}",
                            env!("CARGO_PKG_VERSION")
                        ));
                    }
                    host
                }
                other => return Err(rt(format!("agent {a}: expected hello, got {other:?}"))),
            };
            agents.push(Agent {
                address: a.clone(),
                threads: cfg.threads.unwrap_or(host.cores).max(1),
                host,
                stream,
                first: 0,
                protocol: String::new(),
                depth: 1,
                warnings: Vec::new(),
                offset_ns: 0,
                rtt_ns: 0,
            });
        }

        // 2. Split the workers. `threads` applies to every agent.
        let want: usize = agents.iter().map(|a| a.threads).sum();
        let total = fit_threads(&cfg, 1, want, &mut warnings);
        let mut excess = want - total;
        for a in agents.iter_mut().rev() {
            let cut = excess.min(a.threads.saturating_sub(1));
            a.threads -= cut;
            excess -= cut;
        }
        if excess > 0 {
            return Err(RunError::Config(format!(
                "{} agents need at least one connection each; there are fewer",
                agents.len()
            )));
        }
        let total: usize = agents.iter().map(|a| a.threads).sum();
        let mut first = 0;
        for a in agents.iter_mut() {
            a.first = first;
            first += a.threads;
        }
        let mut sent_cfg = cfg.clone();
        sent_cfg.threads = None;
        for a in agents.iter_mut() {
            send(
                &mut a.stream,
                &Msg::Setup {
                    config: Box::new(sent_cfg.clone()),
                    first_worker: a.first,
                    threads: a.threads,
                    total,
                },
            )
            .map_err(|e| rt(format!("agent {}: {e}", a.address)))?;
        }

        // 3. Wait until every agent has its connections open.
        for a in agents.iter_mut() {
            match recv(&mut a.stream).map_err(|e| rt(format!("agent {}: {e}", a.address)))? {
                Msg::Ready {
                    protocol,
                    depth,
                    warnings: w,
                } => {
                    a.protocol = protocol;
                    a.depth = depth;
                    a.warnings = w;
                }
                Msg::Error { message, config } => {
                    let m = format!("agent {}: {message}", a.address);
                    return Err(if config {
                        RunError::Config(m)
                    } else {
                        RunError::Runtime(m)
                    });
                }
                other => {
                    return Err(rt(format!(
                        "agent {}: expected ready, got {other:?}",
                        a.address
                    )))
                }
            }
        }

        // 4. Clock offsets.
        for a in agents.iter_mut() {
            let mut best: Option<(u64, i64)> = None;
            for _ in 0..CLOCK_ROUNDS {
                let t1 = wall_now_ns();
                send(&mut a.stream, &Msg::Ping { t1 }).map_err(rt)?;
                let (t2, t3) = match recv(&mut a.stream).map_err(rt)? {
                    Msg::Pong { t1: e, t2, t3 } if e == t1 => (t2, t3),
                    other => {
                        return Err(rt(format!(
                            "agent {}: expected pong, got {other:?}",
                            a.address
                        )))
                    }
                };
                let t4 = wall_now_ns();
                let rtt = (t4 - t1).saturating_sub(t3.saturating_sub(t2));
                let offset = ((t2 as i64 - t1 as i64) + (t3 as i64 - t4 as i64)) / 2;
                if best.is_none_or(|(r, _)| rtt < r) {
                    best = Some((rtt, offset));
                }
            }
            let (rtt, offset) = best.expect("at least one round");
            a.rtt_ns = rtt;
            a.offset_ns = offset;
        }

        // 5. The rest of the preparation.
        let prep = prepare(&mut cfg, &mut warnings)?;
        cfg.threads = Some(total);
        let (observer, live) = crate::engine::live_observer(&cfg, seed, observer)?;
        let metrics_addr = live.as_ref().map(|(_, s)| s.addr.to_string());

        // 6. T0, far enough ahead for the slowest agent to hear of it.
        let max_rtt = agents.iter().map(|a| a.rtt_ns).max().unwrap_or(0);
        let t0_wall = wall_now_ns() + (100_000_000u64).max(4 * max_rtt);
        for a in agents.iter_mut() {
            let t0 = (t0_wall as i64 + a.offset_ns) as u64;
            send(&mut a.stream, &Msg::Start { t0_wall: t0 }).map_err(rt)?;
        }
        let (atx, arx) = std::sync::mpsc::channel::<WindowMsg>();
        let agg = start_aggregator(&cfg, &prep, total, t0_wall, arx, observer)?;

        // 7. Relay windows; collect the finals.
        let mut stoppers = Vec::new();
        let mut readers = Vec::new();
        for a in agents.iter() {
            let mut rd = a.stream.try_clone().map_err(rt)?;
            stoppers.push(a.stream.try_clone().map_err(rt)?);
            let atx = atx.clone();
            let name = a.address.clone();
            readers.push(std::thread::spawn(move || loop {
                match recv(&mut rd) {
                    Ok(Msg::Window { msg }) => {
                        let _ = atx.send(msg);
                    }
                    Ok(Msg::Final {
                        finals,
                        t0_monotonic,
                        trace_rows,
                        protocol_report,
                        errors,
                    }) => return Ok((finals, t0_monotonic, trace_rows, protocol_report, errors)),
                    Ok(Msg::Error { message, .. }) => {
                        return Err(format!("agent {name}: {message}"))
                    }
                    Ok(other) => return Err(format!("agent {name}: unexpected {other:?}")),
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        return Err(format!("agent {name}: connection lost during the run"))
                    }
                    Err(e) => return Err(format!("agent {name}: {e}")),
                }
            }));
        }
        drop(atx);
        let watcher = {
            let stop = stop.clone();
            let done = Arc::new(AtomicBool::new(false));
            let d = done.clone();
            let h = std::thread::spawn(move || {
                while !d.load(Ordering::Relaxed) {
                    if stop.load(Ordering::Relaxed) {
                        for s in stoppers.iter_mut() {
                            let _ = send(s, &Msg::Stop);
                        }
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            });
            (done, h)
        };

        let mut finals = Vec::new();
        let mut errors = Vec::new();
        let mut reports = Vec::new();
        let mut agent_rows = Vec::new();
        let mut agent_t0 = Vec::new();
        for r in readers {
            match r.join().map_err(|_| rt("an agent reader panicked"))? {
                Ok((f, t0m, rows, report, errs)) => {
                    finals.extend(f);
                    agent_t0.push(Some(t0m));
                    agent_rows.push(rows);
                    reports.push(report);
                    errors.extend(errs);
                }
                Err(e) => {
                    agent_t0.push(None);
                    agent_rows.push(0);
                    reports.push(serde_json::Value::Null);
                    errors.push(e);
                }
            }
        }
        watcher.0.store(true, Ordering::Relaxed);
        let _ = watcher.1.join();
        let wall_duration_s = wall_now_ns().saturating_sub(t0_wall) as f64 / 1e9;
        let agg = agg
            .join()
            .map_err(|_| rt("the aggregator thread panicked"))?;
        finals.sort_by_key(|f| f.id);
        // A failed agent leaves workers without finals; their windows were
        // never complete, and the books cannot be checked.
        if finals.len() != total {
            errors.push(format!("{} of {total} workers reported", finals.len()));
        }

        let protocol_report = if reports.iter().all(|r| r.is_null()) {
            serde_json::Value::Null
        } else {
            serde_json::json!({ "agents": reports })
        };
        let distributed = Distributed {
            agents: agents
                .iter()
                .zip(&agent_rows)
                .zip(&agent_t0)
                .map(|((a, rows), t0m)| AgentSummary {
                    address: a.address.clone(),
                    host: a.host.clone(),
                    protocol: a.protocol.clone(),
                    first_worker: a.first,
                    threads: a.threads,
                    clock_offset_ms: a.offset_ns as f64 / 1e6,
                    rtt_ms: a.rtt_ns as f64 / 1e6,
                    warnings: a.warnings.clone(),
                    trace_rows: *rows,
                    t0_monotonic_ns: *t0m,
                })
                .collect(),
        };
        for a in &agents {
            warnings.extend(
                a.warnings
                    .iter()
                    .map(|w| format!("agent {}: {w}", a.address)),
            );
        }
        let trace_rows = agent_rows.iter().sum();
        Ok(conclude(Conclusion {
            cfg: &cfg,
            host,
            prep: &prep,
            t0_wall,
            finals: &finals,
            agg: &agg,
            port_range_size: None,
            wall_duration_s,
            protocol: agents[0].protocol.clone(),
            protocol_report,
            trace_rows,
            metrics_addr,
            warnings,
            runtime_errors: errors,
            distributed: Some(distributed),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let mut buf = Vec::new();
        send(&mut buf, &Msg::Ping { t1: 42 }).unwrap();
        send(&mut buf, &Msg::Stop).unwrap();
        let mut r = &buf[..];
        assert!(matches!(recv(&mut r).unwrap(), Msg::Ping { t1: 42 }));
        assert!(matches!(recv(&mut r).unwrap(), Msg::Stop));
    }

    #[test]
    fn trace_paths_do_not_collide() {
        let p = std::path::Path::new("/tmp/out.csv.zst");
        assert_eq!(
            agent_trace_path(p, 4, 4),
            std::path::PathBuf::from("/tmp/w4-7.out.csv.zst")
        );
    }
}
