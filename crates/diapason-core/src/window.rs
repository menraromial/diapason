//! The aggregator: merges the workers' windows, writes the interval log and
//! the window file, calls the window observer, and accumulates the per-phase
//! histograms and the slice statistics used by the fidelity checks.

use crate::hist::{DelayHists, Hist};
use crate::stats::{WindowAccum, WindowMsg, SLICE_NS};
use hdrhistogram::serialization::interval_log::{IntervalLogWriterBuilder, Tag};
use hdrhistogram::serialization::V2DeflateSerializer;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::time::Duration;

/// What the observer receives for every window, after all workers reported it.
#[derive(Clone, Debug)]
pub struct WindowSnapshot {
    pub index: u64,
    pub start_s: f64,
    pub end_s: f64,
    pub scheduled: u64,
    pub sent: u64,
    pub completed: u64,
    pub failed: u64,
    pub not_sent: u64,
    pub in_flight_mean: f64,
    /// Sum over workers of each worker's maximum: an upper bound.
    pub in_flight_max: u64,
    pub in_system_mean: f64,
    pub in_system_max: u64,
    /// Sum over workers of the most connections each had open: an upper bound.
    pub connections_open_max: u64,
    /// Histograms of the events in the window, all phases merged.
    pub hists: DelayHists,
}

#[derive(Serialize)]
struct WindowLine {
    index: u64,
    start_s: f64,
    end_s: f64,
    scheduled: u64,
    sent: u64,
    completed: u64,
    failed: u64,
    not_sent: u64,
    in_flight_mean: f64,
    in_flight_max: u64,
    in_system_mean: f64,
    in_system_max: u64,
    connections_open_max: u64,
    corrected_p50_ms: f64,
    corrected_p99_ms: f64,
    server_p99_ms: f64,
    connection_wait_p99_ms: f64,
    generator_lag_p99_ms: f64,
}

fn q_ms(h: &Hist, q: f64) -> f64 {
    if h.is_empty() {
        0.0
    } else {
        h.value_at_quantile(q) as f64 / 1e6
    }
}

impl WindowSnapshot {
    fn line(&self) -> WindowLine {
        WindowLine {
            index: self.index,
            start_s: self.start_s,
            end_s: self.end_s,
            scheduled: self.scheduled,
            sent: self.sent,
            completed: self.completed,
            failed: self.failed,
            not_sent: self.not_sent,
            in_flight_mean: self.in_flight_mean,
            in_flight_max: self.in_flight_max,
            in_system_mean: self.in_system_mean,
            in_system_max: self.in_system_max,
            connections_open_max: self.connections_open_max,
            corrected_p50_ms: q_ms(&self.hists.corrected, 0.5),
            corrected_p99_ms: q_ms(&self.hists.corrected, 0.99),
            server_p99_ms: q_ms(&self.hists.server, 0.99),
            connection_wait_p99_ms: q_ms(&self.hists.conn_wait, 0.99),
            generator_lag_p99_ms: q_ms(&self.hists.gen_lag, 0.99),
        }
    }
}

/// Requests per time slice over the measurement interval.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SliceStats {
    pub slices: u64,
    pub mean: f64,
    pub peak: u64,
    pub peak_to_mean: f64,
    pub empty_share: f64,
    #[serde(skip)]
    total: u64,
    #[serde(skip)]
    empty: u64,
}

impl SliceStats {
    fn push(&mut self, n: u64) {
        self.slices += 1;
        self.total += n;
        self.peak = self.peak.max(n);
        if n == 0 {
            self.empty += 1;
        }
    }

    fn finish(&mut self) {
        if self.slices > 0 {
            self.mean = self.total as f64 / self.slices as f64;
            self.empty_share = self.empty as f64 / self.slices as f64;
            self.peak_to_mean = if self.mean > 0.0 {
                self.peak as f64 / self.mean
            } else {
                0.0
            };
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Burstiness {
    pub sched_10: SliceStats,
    pub send_10: SliceStats,
    pub sched_50: SliceStats,
    pub send_50: SliceStats,
}

pub struct AggregatorConfig {
    pub workers: usize,
    pub phases: usize,
    pub window_ns: u64,
    /// The measurement interval, in run-clock ns.
    pub measure: (u64, u64),
    pub histogram_log: Option<PathBuf>,
    pub windows_file: Option<PathBuf>,
    /// Wall-clock time of T0, Unix ns.
    pub t0_wall_ns: u64,
}

pub struct AggregatorOutput {
    pub phase_hists: Vec<DelayHists>,
    pub burstiness: Burstiness,
    pub windows: u64,
    pub error: Option<String>,
}

pub type Observer = Box<dyn FnMut(&WindowSnapshot) + Send>;

struct Merged {
    reported: usize,
    acc: WindowAccum,
}

fn merge(into: &mut WindowAccum, w: WindowAccum) {
    for (i, h) in w.hists.into_iter().enumerate() {
        if let Some(h) = h {
            match &mut into.hists[i] {
                Some(t) => t.add(&h),
                None => into.hists[i] = Some(h),
            }
        }
    }
    into.counts.add(&w.counts);
    into.sent_area += w.sent_area;
    into.sent_max += w.sent_max;
    into.system_area += w.system_area;
    into.system_max += w.system_max;
    into.conns_open_max += w.conns_open_max;
    for (a, b) in into.sched_slices.iter_mut().zip(&w.sched_slices) {
        *a += b;
    }
    for (a, b) in into.send_slices.iter_mut().zip(&w.send_slices) {
        *a += b;
    }
}

struct Sinks {
    windows: Option<BufWriter<File>>,
    log_buf: Option<BufWriter<File>>,
}

pub fn run_aggregator(
    rx: Receiver<WindowMsg>,
    cfg: AggregatorConfig,
    mut observer: Option<Observer>,
) -> AggregatorOutput {
    let mut out = AggregatorOutput {
        phase_hists: (0..cfg.phases).map(|_| DelayHists::new()).collect(),
        burstiness: Burstiness::default(),
        windows: 0,
        error: None,
    };
    let open = |p: &Option<PathBuf>| -> io::Result<Option<BufWriter<File>>> {
        Ok(match p {
            Some(p) => Some(BufWriter::new(File::create(p)?)),
            None => None,
        })
    };
    let mut sinks = match (open(&cfg.windows_file), open(&cfg.histogram_log)) {
        (Ok(w), Ok(l)) => Sinks {
            windows: w,
            log_buf: l,
        },
        (Err(e), _) | (_, Err(e)) => {
            out.error = Some(format!("cannot create output file: {e}"));
            Sinks {
                windows: None,
                log_buf: None,
            }
        }
    };
    let mut serializer = V2DeflateSerializer::new();
    let t0_wall = std::time::UNIX_EPOCH + Duration::from_nanos(cfg.t0_wall_ns);
    let mut log_writer = match sinks.log_buf.as_mut() {
        Some(buf) => {
            let mut b = IntervalLogWriterBuilder::new();
            b.add_comment("diapason interval log: values in ns, tags corrected, server, connection_wait, generator_lag")
                .with_start_time(t0_wall)
                .with_base_time(t0_wall)
                .with_max_value_divisor(1e6);
            match b.begin_log_with(buf, &mut serializer) {
                Ok(w) => Some(w),
                Err(e) => {
                    out.error = Some(format!("histogram log: {e}"));
                    None
                }
            }
        }
        None => None,
    };

    let mut next_expected = vec![0u64; cfg.workers];
    let mut finished = vec![false; cfg.workers];
    let mut pending: BTreeMap<u64, Merged> = BTreeMap::new();
    let mut next_index = 0u64;
    let (mstart, mend) = cfg.measure;

    let mut process = |acc: WindowAccum, out: &mut AggregatorOutput| {
        let start = acc.index * cfg.window_ns;
        let end = start + cfg.window_ns;
        let mut all = DelayHists::new();
        for (p, h) in acc.hists.iter().enumerate() {
            if let Some(h) = h {
                out.phase_hists[p].add(h);
                all.add(h);
            }
        }
        // Slices fully inside the measurement interval.
        let b = &mut out.burstiness;
        for (k, (&s, &d)) in acc.sched_slices.iter().zip(&acc.send_slices).enumerate() {
            let t = start + k as u64 * SLICE_NS;
            if t >= mstart && t + SLICE_NS <= mend {
                b.sched_10.push(s as u64);
                b.send_10.push(d as u64);
            }
        }
        for g in 0..acc.sched_slices.len() / 5 {
            let t = start + (g * 5) as u64 * SLICE_NS;
            if t >= mstart && t + 5 * SLICE_NS <= mend {
                let s: u64 = acc.sched_slices[g * 5..g * 5 + 5]
                    .iter()
                    .map(|&x| x as u64)
                    .sum();
                let d: u64 = acc.send_slices[g * 5..g * 5 + 5]
                    .iter()
                    .map(|&x| x as u64)
                    .sum();
                b.sched_50.push(s);
                b.send_50.push(d);
            }
        }
        let snap = WindowSnapshot {
            index: acc.index,
            start_s: start as f64 / 1e9,
            end_s: end as f64 / 1e9,
            scheduled: acc.counts.scheduled,
            sent: acc.counts.sent,
            completed: acc.counts.completed,
            failed: acc.counts.failed,
            not_sent: acc.counts.not_sent,
            in_flight_mean: acc.sent_area as f64 / cfg.window_ns as f64,
            in_flight_max: acc.sent_max,
            in_system_mean: acc.system_area as f64 / cfg.window_ns as f64,
            in_system_max: acc.system_max,
            connections_open_max: acc.conns_open_max,
            hists: all,
        };
        if let Some(w) = sinks.windows.as_mut() {
            if let Ok(s) = serde_json::to_string(&snap.line()) {
                let _ = writeln!(w, "{s}");
            }
        }
        if let Some(lw) = log_writer.as_mut() {
            let st = Duration::from_nanos(start);
            let d = Duration::from_nanos(cfg.window_ns);
            for (tag, h) in [
                ("corrected", &snap.hists.corrected),
                ("server", &snap.hists.server),
                ("connection_wait", &snap.hists.conn_wait),
                ("generator_lag", &snap.hists.gen_lag),
            ] {
                let _ = lw.write_histogram(h, st, d, Tag::new(tag));
            }
        }
        if let Some(obs) = observer.as_mut() {
            obs(&snap);
        }
        out.windows += 1;
    };

    loop {
        let msg = rx.recv();
        let closed = msg.is_err();
        match msg {
            Ok(WindowMsg::Window { worker, w }) => {
                next_expected[worker] = w.index + 1;
                let idx = w.index;
                match pending.get_mut(&idx) {
                    Some(m) => {
                        m.reported += 1;
                        merge(&mut m.acc, w);
                    }
                    None => {
                        pending.insert(
                            idx,
                            Merged {
                                reported: 1,
                                acc: w,
                            },
                        );
                    }
                }
            }
            Ok(WindowMsg::Finished { worker }) => finished[worker] = true,
            Err(_) => {
                // Every sender is gone: treat all workers as finished.
                finished.iter_mut().for_each(|f| *f = true);
            }
        }
        // Process complete windows in order.
        loop {
            let complete = (0..cfg.workers).all(|w| finished[w] || next_expected[w] > next_index);
            if !complete {
                break;
            }
            match pending.remove(&next_index) {
                Some(m) => {
                    let _ = m.reported;
                    process(m.acc, &mut out);
                }
                None => {
                    if finished.iter().all(|&f| f) && pending.is_empty() {
                        break;
                    }
                    if finished.iter().all(|&f| f) {
                        // A gap left by a worker that stopped early.
                        next_index = *pending.keys().next().unwrap();
                        continue;
                    }
                }
            }
            next_index += 1;
        }
        if finished.iter().all(|&f| f) && pending.is_empty() {
            break;
        }
        if closed {
            break;
        }
    }
    drop(log_writer);
    for w in [sinks.windows.as_mut(), sinks.log_buf.as_mut()]
        .into_iter()
        .flatten()
    {
        if let Err(e) = w.flush() {
            out.error.get_or_insert(format!("writing outputs: {e}"));
        }
    }
    let b = &mut out.burstiness;
    for s in [
        &mut b.sched_10,
        &mut b.send_10,
        &mut b.sched_50,
        &mut b.send_50,
    ] {
        s.finish();
    }
    out
}
