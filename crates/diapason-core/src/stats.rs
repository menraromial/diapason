//! Per-worker recording. Everything here is owned by one worker thread; it is
//! merged with the other workers' at window boundaries and at the end.

use crate::hist::{record, DelayHists};
use crate::protocol::{FailKind, NotSentReason};
use crate::schedule::RateProfile;
use crate::trace::{TraceRow, TraceSink};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub const SLICE_NS: u64 = 10_000_000;

/// u128 values travel as decimal strings: serde_json does not read u128
/// numbers back without a crate-wide feature.
mod u128_str {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

/// Counters of one scenario within a phase, by intended time.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScenarioCounters {
    pub scheduled: u64,
    pub completed: u64,
    pub success: u64,
    pub failed: u64,
    pub not_sent: u64,
    pub in_flight_at_end: u64,
}

/// Counters of one phase. Fields marked "by intended" count requests whose
/// intended time is in the phase; fields marked "by time" count events whose
/// own time falls in the phase interval.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PhaseCounters {
    /// By intended.
    pub scheduled: u64,
    pub completed: u64,
    pub success: u64,
    pub failed: [u64; 6],
    pub not_sent: [u64; 3],
    pub in_flight_at_end: u64,
    pub sent: u64,
    /// Sent with connection wait above the admission threshold.
    pub conn_wait_over: u64,
    pub status: Vec<(u16, u64)>,
    /// By time: completions with `done` in the interval.
    pub completed_in_interval: u64,
    /// By time: requests that left the sent state (completed or failed)
    /// with `done` in the interval, and the sum of their `done - send`, in ns.
    pub departed_in_interval: u64,
    #[serde(with = "u128_str")]
    pub departed_ns_in_interval: u128,
    /// By time: requests with `send` in the interval.
    pub sent_in_interval: u64,
    /// By time: integral of the gauges over the interval (count × ns), and
    /// their maxima.
    #[serde(with = "u128_str")]
    pub sent_area: u128,
    pub sent_max: u64,
    #[serde(with = "u128_str")]
    pub system_area: u128,
    pub system_max: u64,
    pub scenarios: Vec<ScenarioCounters>,
}

impl PhaseCounters {
    pub fn add(&mut self, o: &PhaseCounters) {
        if self.scenarios.len() < o.scenarios.len() {
            self.scenarios
                .resize(o.scenarios.len(), ScenarioCounters::default());
        }
        for (a, b) in self.scenarios.iter_mut().zip(&o.scenarios) {
            a.scheduled += b.scheduled;
            a.completed += b.completed;
            a.success += b.success;
            a.failed += b.failed;
            a.not_sent += b.not_sent;
            a.in_flight_at_end += b.in_flight_at_end;
        }
        self.scheduled += o.scheduled;
        self.completed += o.completed;
        self.success += o.success;
        for i in 0..6 {
            self.failed[i] += o.failed[i];
        }
        for i in 0..3 {
            self.not_sent[i] += o.not_sent[i];
        }
        self.in_flight_at_end += o.in_flight_at_end;
        self.sent += o.sent;
        self.conn_wait_over += o.conn_wait_over;
        for &(code, n) in &o.status {
            self.count_status(code, n);
        }
        self.completed_in_interval += o.completed_in_interval;
        self.departed_in_interval += o.departed_in_interval;
        self.departed_ns_in_interval += o.departed_ns_in_interval;
        self.sent_in_interval += o.sent_in_interval;
        self.sent_area += o.sent_area;
        self.sent_max = self.sent_max.max(o.sent_max);
        self.system_area += o.system_area;
        self.system_max = self.system_max.max(o.system_max);
    }

    fn count_status(&mut self, code: u16, n: u64) {
        match self.status.iter_mut().find(|(c, _)| *c == code) {
            Some(e) => e.1 += n,
            None => {
                self.status.push((code, n));
                self.status.sort_unstable();
            }
        }
    }
}

/// Counts of events in one window, by the time of the event (scheduled: by
/// intended time).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WindowCounts {
    pub scheduled: u64,
    pub sent: u64,
    pub completed: u64,
    pub failed: u64,
    pub not_sent: u64,
}

impl WindowCounts {
    pub fn add(&mut self, o: &WindowCounts) {
        self.scheduled += o.scheduled;
        self.sent += o.sent;
        self.completed += o.completed;
        self.failed += o.failed;
        self.not_sent += o.not_sent;
    }
}

/// What a worker records about one window.
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowAccum {
    pub index: u64,
    /// Histograms by phase of intended time; allocated on first use.
    pub hists: Vec<Option<Box<DelayHists>>>,
    pub counts: WindowCounts,
    #[serde(with = "u128_str")]
    pub sent_area: u128,
    pub sent_max: u64,
    #[serde(with = "u128_str")]
    pub system_area: u128,
    pub system_max: u64,
    /// Requests per 10 ms slice of the window: by intended time and by send time.
    pub sched_slices: Vec<u32>,
    pub send_slices: Vec<u32>,
    /// Most connections open at once during the window.
    pub conns_open_max: u64,
}

impl WindowAccum {
    fn new(
        index: u64,
        phases: usize,
        slices: usize,
        sent_now: u64,
        system_now: u64,
        open_now: u64,
    ) -> WindowAccum {
        WindowAccum {
            index,
            hists: (0..phases).map(|_| None).collect(),
            counts: WindowCounts::default(),
            sent_area: 0,
            sent_max: sent_now,
            system_area: 0,
            system_max: system_now,
            sched_slices: vec![0; slices],
            send_slices: vec![0; slices],
            conns_open_max: open_now,
        }
    }

    fn hist(&mut self, phase: u16) -> &mut DelayHists {
        self.hists[phase as usize].get_or_insert_with(|| Box::new(DelayHists::new()))
    }
}

/// A message from a worker to the aggregator.
#[derive(Debug, Serialize, Deserialize)]
pub enum WindowMsg {
    Window { worker: usize, w: WindowAccum },
    Finished { worker: usize },
}

/// A gauge integrated over time, split at window and phase boundaries.
#[derive(Clone, Debug, Default)]
struct Gauge {
    count: u64,
    last_t: u64,
}

pub struct Stats {
    pub profile: Arc<RateProfile>,
    pub phases: Vec<PhaseCounters>,
    window_ns: u64,
    slices: usize,
    open: VecDeque<WindowAccum>,
    tx: Sender<WindowMsg>,
    worker: usize,
    sent_g: Gauge,
    system_g: Gauge,
    conn_wait_thr_ns: u64,
    success: crate::config::StatusSet,
    pub trace: Option<TraceSink>,
    open_now: u64,
}

/// A request as the worker tracks it.
#[derive(Clone, Copy, Debug)]
pub struct Job {
    pub seq: u64,
    pub intended: u64,
    pub attempt: u64,
    pub send: u64,
    pub phase: u16,
    pub scenario: u16,
    pub conn: u32,
}

pub const NO_CONN: u32 = u32::MAX;

impl Stats {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        profile: Arc<RateProfile>,
        window_ns: u64,
        tx: Sender<WindowMsg>,
        worker: usize,
        conn_wait_thr_ns: u64,
        success: crate::config::StatusSet,
        trace: Option<TraceSink>,
        scenarios: usize,
    ) -> Stats {
        let n = profile.phases.len();
        let slices = (window_ns / SLICE_NS) as usize;
        // Window 0 exists from the start, so every worker reports every index.
        let mut open = VecDeque::new();
        open.push_back(WindowAccum::new(0, n, slices, 0, 0, 0));
        Stats {
            profile,
            phases: vec![
                PhaseCounters {
                    scenarios: vec![ScenarioCounters::default(); scenarios.max(1)],
                    ..Default::default()
                };
                n
            ],
            window_ns,
            slices,
            open,
            tx,
            worker,
            sent_g: Gauge::default(),
            system_g: Gauge::default(),
            conn_wait_thr_ns,
            success,
            trace,
            open_now: 0,
        }
    }

    /// Records the number of open connections at time t.
    pub fn set_open(&mut self, t: u64, n: u64) {
        self.open_now = n;
        let w = self.window(t);
        w.conns_open_max = w.conns_open_max.max(n);
    }

    fn phase_at(&self, t: u64) -> Option<usize> {
        if t >= self.profile.end_ns {
            return None;
        }
        Some(self.profile.phase_of(t))
    }

    /// The window that contains time t. Times before the oldest open window go
    /// to the oldest open window.
    fn window(&mut self, t: u64) -> &mut WindowAccum {
        let idx = t / self.window_ns;
        let phases = self.profile.phases.len();
        let (sent_now, system_now, open_now) =
            (self.sent_g.count, self.system_g.count, self.open_now);
        if self.open.is_empty() {
            // Only after finish(); keep recording somewhere harmless.
            self.open.push_back(WindowAccum::new(
                idx,
                phases,
                self.slices,
                sent_now,
                system_now,
                open_now,
            ));
        }
        while self.open.back().unwrap().index < idx {
            let next = self.open.back().unwrap().index + 1;
            self.open.push_back(WindowAccum::new(
                next,
                phases,
                self.slices,
                sent_now,
                system_now,
                open_now,
            ));
        }
        let front = self.open.front().unwrap().index;
        let pos = idx.saturating_sub(front) as usize;
        &mut self.open[pos]
    }

    fn slice_in_window(&self, t: u64) -> usize {
        ((t % self.window_ns) / SLICE_NS) as usize
    }

    /// Integrates both gauges up to time t.
    fn advance(&mut self, t: u64) {
        for which in 0..2 {
            let g = if which == 0 {
                self.sent_g.clone()
            } else {
                self.system_g.clone()
            };
            let mut cur = g.last_t;
            if t <= cur {
                continue;
            }
            // Split at window boundaries.
            while cur < t {
                let wend = (cur / self.window_ns + 1) * self.window_ns;
                let seg_end = wend.min(t);
                let area = g.count as u128 * (seg_end - cur) as u128;
                let w = self.window(cur);
                if which == 0 {
                    w.sent_area += area;
                } else {
                    w.system_area += area;
                }
                cur = seg_end;
            }
            // Split at phase boundaries.
            let mut cur = g.last_t;
            while cur < t {
                let Some(p) = self.phase_at(cur) else { break };
                let pend = self.profile.phases[p].end_ns;
                let seg_end = pend.min(t);
                let area = g.count as u128 * (seg_end - cur) as u128;
                let pc = &mut self.phases[p];
                if which == 0 {
                    pc.sent_area += area;
                    pc.sent_max = pc.sent_max.max(g.count);
                } else {
                    pc.system_area += area;
                    pc.system_max = pc.system_max.max(g.count);
                }
                cur = seg_end;
            }
            if which == 0 {
                self.sent_g.last_t = t;
            } else {
                self.system_g.last_t = t;
            }
        }
    }

    fn gauge_change(&mut self, t: u64, sent: i64, system: i64) {
        self.advance(t);
        self.sent_g.count = (self.sent_g.count as i64 + sent) as u64;
        self.system_g.count = (self.system_g.count as i64 + system) as u64;
        let (s, y) = (self.sent_g.count, self.system_g.count);
        let w = self.window(t);
        w.sent_max = w.sent_max.max(s);
        w.system_max = w.system_max.max(y);
        if let Some(p) = self.phase_at(t) {
            let pc = &mut self.phases[p];
            pc.sent_max = pc.sent_max.max(s);
            pc.system_max = pc.system_max.max(y);
        }
    }

    pub fn on_scheduled(&mut self, phase: u16, scenario: u16, intended: u64) {
        let pc = &mut self.phases[phase as usize];
        pc.scheduled += 1;
        pc.scenarios[scenario as usize].scheduled += 1;
        let slice = self.slice_in_window(intended);
        let idx = intended / self.window_ns;
        let w = self.window(intended);
        w.counts.scheduled += 1;
        // A record that lands in an older window (severe lag) is not counted
        // in the slices, which belong to its own window.
        if w.index == idx {
            w.sched_slices[slice] += 1;
        }
    }

    pub fn on_attempt(&mut self, job: &Job) {
        let lag = job.attempt.saturating_sub(job.intended);
        record(&mut self.window(job.attempt).hist(job.phase).gen_lag, lag);
        self.gauge_change(job.attempt, 0, 1);
    }

    pub fn on_send(&mut self, job: &Job) {
        let wait = job.send.saturating_sub(job.attempt);
        let pc = &mut self.phases[job.phase as usize];
        pc.sent += 1;
        if wait > self.conn_wait_thr_ns {
            pc.conn_wait_over += 1;
        }
        if let Some(p) = self.phase_at(job.send) {
            self.phases[p].sent_in_interval += 1;
        }
        let slice = self.slice_in_window(job.send);
        let idx = job.send / self.window_ns;
        let w = self.window(job.send);
        record(&mut w.hist(job.phase).conn_wait, wait);
        w.counts.sent += 1;
        if w.index == idx {
            w.send_slices[slice] += 1;
        }
        self.gauge_change(job.send, 1, 0);
    }

    pub fn on_complete(
        &mut self,
        job: &Job,
        first_byte: u64,
        done: u64,
        status: u16,
        bytes_in: u64,
    ) {
        let server = done.saturating_sub(job.send);
        let corrected = done.saturating_sub(job.intended);
        let ok = self.success.contains(status);
        let pc = &mut self.phases[job.phase as usize];
        pc.completed += 1;
        let sc = &mut pc.scenarios[job.scenario as usize];
        sc.completed += 1;
        if ok {
            pc.success += 1;
            sc.success += 1;
        }
        pc.count_status(status, 1);
        if let Some(p) = self.phase_at(done) {
            let pc = &mut self.phases[p];
            pc.completed_in_interval += 1;
            pc.departed_in_interval += 1;
            pc.departed_ns_in_interval += server as u128;
        }
        let w = self.window(done);
        let h = w.hist(job.phase);
        record(&mut h.corrected, corrected);
        record(&mut h.server, server);
        w.counts.completed += 1;
        self.gauge_change(done, -1, -1);
        if let Some(t) = &mut self.trace {
            t.push(TraceRow {
                seq: job.seq,
                worker: self.worker as u32,
                conn: job.conn,
                phase: job.phase,
                scenario: job.scenario,
                intended: job.intended,
                attempt: Some(job.attempt),
                send: Some(job.send),
                first_byte: Some(first_byte),
                done: Some(done),
                status: Some(status),
                bytes_in,
                outcome: "",
            });
        }
    }

    pub fn on_fail(&mut self, job: &Job, done: u64, kind: FailKind) {
        let pc = &mut self.phases[job.phase as usize];
        pc.failed[kind.index()] += 1;
        pc.scenarios[job.scenario as usize].failed += 1;
        if let Some(p) = self.phase_at(done) {
            let pc = &mut self.phases[p];
            pc.departed_in_interval += 1;
            pc.departed_ns_in_interval += done.saturating_sub(job.send) as u128;
        }
        self.window(done).counts.failed += 1;
        self.gauge_change(done, -1, -1);
        if let Some(t) = &mut self.trace {
            t.push(TraceRow {
                seq: job.seq,
                worker: self.worker as u32,
                conn: job.conn,
                phase: job.phase,
                scenario: job.scenario,
                intended: job.intended,
                attempt: Some(job.attempt),
                send: Some(job.send),
                first_byte: None,
                done: Some(done),
                status: None,
                bytes_in: 0,
                outcome: kind.as_str(),
            });
        }
    }

    /// A request that was attempted (and counted in the system gauge) and
    /// will never be sent.
    pub fn on_not_sent(&mut self, job: &Job, t: u64, reason: NotSentReason) {
        let pc = &mut self.phases[job.phase as usize];
        pc.not_sent[reason.index()] += 1;
        pc.scenarios[job.scenario as usize].not_sent += 1;
        self.window(t).counts.not_sent += 1;
        self.gauge_change(t, 0, -1);
        if let Some(tr) = &mut self.trace {
            tr.push(TraceRow {
                seq: job.seq,
                worker: self.worker as u32,
                conn: NO_CONN,
                phase: job.phase,
                scenario: job.scenario,
                intended: job.intended,
                attempt: Some(job.attempt),
                send: None,
                first_byte: None,
                done: None,
                status: None,
                bytes_in: 0,
                outcome: match reason {
                    NotSentReason::Queue => "not_sent:queue",
                    NotSentReason::QueueFull => "not_sent:queue_full",
                    NotSentReason::EndOfRun => "not_sent:end_of_run",
                },
            });
        }
    }

    pub fn on_in_flight_at_end(&mut self, job: &Job) {
        let pc = &mut self.phases[job.phase as usize];
        pc.in_flight_at_end += 1;
        pc.scenarios[job.scenario as usize].in_flight_at_end += 1;
        if let Some(tr) = &mut self.trace {
            tr.push(TraceRow {
                seq: job.seq,
                worker: self.worker as u32,
                conn: job.conn,
                phase: job.phase,
                scenario: job.scenario,
                intended: job.intended,
                attempt: Some(job.attempt),
                send: Some(job.send),
                first_byte: None,
                done: None,
                status: None,
                bytes_in: 0,
                outcome: "in_flight_at_end",
            });
        }
    }

    /// Sends every window that ended at least one full window before `now`.
    /// Keeping one extra window open lets late intended-time records land in
    /// the right window.
    pub fn flush(&mut self, now: u64) {
        self.advance(now);
        let _ = self.window(now);
        let cur = now / self.window_ns;
        while let Some(front) = self.open.front() {
            if front.index + 2 > cur {
                break;
            }
            let w = self.open.pop_front().unwrap();
            let _ = self.tx.send(WindowMsg::Window {
                worker: self.worker,
                w,
            });
        }
    }

    /// Sends all open windows and the end marker.
    pub fn finish(&mut self, now: u64) {
        self.advance(now);
        let _ = self.window(now);
        while let Some(w) = self.open.pop_front() {
            let _ = self.tx.send(WindowMsg::Window {
                worker: self.worker,
                w,
            });
        }
        let _ = self.tx.send(WindowMsg::Finished {
            worker: self.worker,
        });
        if let Some(t) = &mut self.trace {
            t.flush();
        }
    }
}
