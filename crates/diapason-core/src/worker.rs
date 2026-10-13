//! One worker: an OS thread with a single-threaded tokio runtime that owns a
//! share of the connections and of the schedule.
//!
//! All mutable state lives in one `RefCell<State>`, borrowed briefly and never
//! across an await. Request state is held there too (queue, per-connection
//! inbox and pending lists), so the final accounting can count every request
//! from its own state.

use crate::clock::{raw_now_ns, RunClock};
use crate::config::{Mode, RunConfig, StatusSet};
use crate::host::{pin_to_cpu, thread_cpu, ThreadCpu};
use crate::protocol::{
    ConnectErrorKind, FailKind, NotSentReason, Protocol, RequestCtx, RequestWriter, ResponseReader,
};
use crate::schedule::{Arrivals, RateProfile};
use crate::stats::{Job, PhaseCounters, Stats, WindowMsg, NO_CONN};
use crate::template::ScenarioPicker;
use crate::timer::{reduce_timer_slack, Timer};
use crate::trace::TraceSink;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Barrier, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Start-up rendezvous: every worker opens its connections, then waits twice
/// on the barrier; between the two waits the coordinator sets T0.
pub struct StartSync {
    pub barrier: Barrier,
    pub t0: OnceLock<u64>,
}

pub struct WorkerSetup<P: Protocol> {
    pub id: usize,
    pub workers: usize,
    /// Connections opened before T0.
    pub conns: usize,
    /// Most connections this worker may have, counting those opened on demand.
    pub conns_max: usize,
    pub max_in_flight: usize,
    /// Requests that may be outstanding on one connection.
    pub depth: usize,
    pub protocol: Arc<P>,
    pub cfg: Arc<RunConfig>,
    pub profile: Arc<RateProfile>,
    pub seed: u64,
    pub sync: Arc<StartSync>,
    pub window_tx: Sender<WindowMsg>,
    pub trace: Option<TraceSink>,
    pub pin: Option<usize>,
    /// Set by `RunHandle::stop`: the schedule ends now.
    pub stop: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ConnStats {
    pub requested: u64,
    pub established: u64,
    pub shortfall: [u64; 7],
    pub connect_attempts: u64,
    pub connect_errors: [u64; 7],
    pub reconnects: u64,
    pub closes_server: u64,
    pub closes_client: u64,
    pub closes_error: u64,
    pub requeued: u64,
    pub opened_on_demand: u64,
    pub peak_open: u64,
    pub cancelled: u64,
}

impl ConnStats {
    pub fn add(&mut self, o: &ConnStats) {
        self.requested += o.requested;
        self.established += o.established;
        for i in 0..7 {
            self.shortfall[i] += o.shortfall[i];
            self.connect_errors[i] += o.connect_errors[i];
        }
        self.connect_attempts += o.connect_attempts;
        self.reconnects += o.reconnects;
        self.closes_server += o.closes_server;
        self.closes_client += o.closes_client;
        self.closes_error += o.closes_error;
        self.requeued += o.requeued;
        self.opened_on_demand += o.opened_on_demand;
        self.peak_open += o.peak_open;
        self.cancelled += o.cancelled;
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkerFinal {
    pub id: usize,
    pub connections: usize,
    pub max_in_flight: usize,
    pub phases: Vec<PhaseCounters>,
    pub conn: ConnStats,
    pub wall_ns: u64,
    pub busy_ns: u64,
    pub spin_ns: u64,
    pub cpu: ThreadCpu,
    pub trace_dropped: u64,
    /// The schedule was cut short by a stop request.
    pub stopped: bool,
    pub error: Option<String>,
}

thread_local! {
    /// Time spent polling this worker's tasks, timer spin excluded.
    static BUSY: Cell<u64> = const { Cell::new(0) };
    /// Set while the dispatcher yields in its spin loop.
    static SPINNING: Cell<bool> = const { Cell::new(false) };
}

/// Adds the time spent in each poll of the inner future to `BUSY`. For the
/// dispatcher (`dispatcher = true`), polls that end in the timer spin are not
/// counted.
struct Timed<F: Future> {
    inner: Pin<Box<F>>,
    dispatcher: bool,
}

impl<F: Future> Future for Timed<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let t0 = raw_now_ns();
        let r = self.inner.as_mut().poll(cx);
        if !(self.dispatcher && SPINNING.with(|s| s.get())) {
            let dt = raw_now_ns() - t0;
            BUSY.with(|b| b.set(b.get() + dt));
        }
        r
    }
}

/// Spawns a task on the current worker, counting its polls in the worker's
/// busy share. Protocols that start tasks of their own (one per HTTP/2
/// stream, for instance) should use this instead of `spawn_local`.
pub fn spawn_task<F: Future + 'static>(f: F) -> JoinHandle<F::Output> {
    tokio::task::spawn_local(timed(f))
}

fn timed<F: Future>(f: F) -> Timed<F> {
    Timed {
        inner: Box::pin(f),
        dispatcher: false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Close {
    Server,
    Client,
    Error,
}

struct Slot {
    /// Assigned to this connection, not yet written.
    inbox: VecDeque<Job>,
    /// Written, waiting for a response, in send order.
    pending: VecDeque<Job>,
    open: bool,
    in_idle: bool,
    w_notify: Rc<Notify>,
    r_notify: Rc<Notify>,
}

impl Slot {
    fn new() -> Slot {
        Slot {
            inbox: VecDeque::new(),
            pending: VecDeque::new(),
            open: false,
            in_idle: false,
            w_notify: Rc::new(Notify::new()),
            r_notify: Rc::new(Notify::new()),
        }
    }
}

/// Starts the supervisor of connection `c`; set once the state exists.
type Spawner = Rc<dyn Fn(u32)>;

struct State {
    clock: RunClock,
    stats: Stats,
    queue: VecDeque<Job>,
    idle: VecDeque<u32>,
    conns: Vec<Slot>,
    conns_max: usize,
    /// On-demand connections whose first connect has not succeeded yet.
    connecting: usize,
    open_count: u64,
    spawn: Option<Spawner>,
    in_flight: usize,
    max_in_flight: usize,
    depth: usize,
    max_queue_len: usize,
    max_queue_delay: u64,
    mode: Mode,
    closed_running: bool,
    closed_j: u64,
    worker: u64,
    workers: u64,
    end_ns: u64,
    conn_stats: ConnStats,
    stopping: bool,
    stop_requested: bool,
    stop_notify: Rc<Notify>,
    picker: ScenarioPicker,
}

impl State {
    fn has_room(&self, c: u32) -> bool {
        let s = &self.conns[c as usize];
        s.open && s.inbox.len() + s.pending.len() < self.depth
    }

    fn mark_idle(&mut self, c: u32) {
        if self.has_room(c) && !self.conns[c as usize].in_idle {
            self.conns[c as usize].in_idle = true;
            self.idle.push_back(c);
        }
    }

    fn take_idle(&mut self) -> Option<u32> {
        while let Some(c) = self.idle.pop_front() {
            self.conns[c as usize].in_idle = false;
            if self.has_room(c) {
                return Some(c);
            }
        }
        None
    }

    fn assign(&mut self, c: u32, mut job: Job) {
        job.conn = c;
        let slot = &mut self.conns[c as usize];
        slot.inbox.push_back(job);
        slot.w_notify.notify_one();
        self.in_flight += 1;
        // Back of the ring if it still has room: idle connections are used in
        // round-robin order.
        self.mark_idle(c);
    }

    /// Opens connections on demand, up to `conns_max`: while queued requests
    /// outnumber the room of the connections being opened, and one ahead when
    /// no connection has room left, so that the next request does not wait
    /// for a connect.
    fn grow(&mut self) {
        while self.conns.len() < self.conns_max
            && self.in_flight < self.max_in_flight
            && (self.queue.len() > self.connecting * self.depth
                || (self.idle.is_empty() && self.connecting == 0))
        {
            let Some(spawn) = self.spawn.clone() else {
                return;
            };
            let c = self.conns.len() as u32;
            self.conns.push(Slot::new());
            self.connecting += 1;
            self.conn_stats.opened_on_demand += 1;
            spawn(c);
        }
    }

    /// Admits a request that the dispatcher has just handled.
    fn admit(&mut self, job: Job) {
        if self.in_flight < self.max_in_flight {
            if let Some(c) = self.take_idle() {
                self.assign(c, job);
                if self.conns.len() < self.conns_max {
                    self.grow();
                }
                return;
            }
        }
        if self.max_queue_delay == 0 && self.conns.len() >= self.conns_max {
            self.stats
                .on_not_sent(&job, job.attempt, NotSentReason::Queue);
        } else if self.queue.len() >= self.max_queue_len {
            self.stats
                .on_not_sent(&job, job.attempt, NotSentReason::QueueFull);
        } else {
            self.queue.push_back(job);
            self.grow();
        }
    }

    fn expire_queue(&mut self, now: u64) {
        while let Some(j) = self.queue.front() {
            if now.saturating_sub(j.attempt) <= self.max_queue_delay {
                break;
            }
            let j = self.queue.pop_front().unwrap();
            self.stats.on_not_sent(&j, now, NotSentReason::Queue);
        }
    }

    /// Moves queued requests to free slots; in closed-loop mode, creates new
    /// requests for free slots.
    fn pump(&mut self) {
        let now = self.clock.now();
        if self.mode == Mode::Closed {
            while self.closed_running && self.in_flight < self.max_in_flight {
                if now >= self.end_ns || self.stop_requested {
                    self.closed_running = false;
                    break;
                }
                let Some(c) = self.take_idle() else { break };
                let phase = self.stats.profile.phase_of(now) as u16;
                let seq = self.closed_j * self.workers + self.worker;
                let scenario = self.picker.pick(seq);
                let job = Job {
                    seq,
                    intended: now,
                    attempt: now,
                    send: 0,
                    phase,
                    scenario,
                    conn: NO_CONN,
                };
                self.closed_j += 1;
                self.stats.on_scheduled(phase, scenario, now);
                self.stats.on_attempt(&job);
                self.assign(c, job);
            }
            return;
        }
        self.expire_queue(now);
        while !self.queue.is_empty() && self.in_flight < self.max_in_flight {
            let Some(c) = self.take_idle() else { break };
            let job = self.queue.pop_front().unwrap();
            self.assign(c, job);
        }
    }

    fn set_open(&mut self, c: u32, open: bool) {
        let slot = &mut self.conns[c as usize];
        if slot.open == open {
            return;
        }
        slot.open = open;
        if open {
            self.open_count += 1;
            self.conn_stats.peak_open = self.conn_stats.peak_open.max(self.open_count);
        } else {
            self.open_count -= 1;
        }
        let now = self.clock.now();
        self.stats.set_open(now, self.open_count);
    }

    /// The connection is gone. Pending requests fail (the first with `first`
    /// if given, the others with `rest`); requests not yet written go back to
    /// the head of the queue.
    fn conn_down(&mut self, c: u32, first: Option<FailKind>, rest: FailKind, close: Close) {
        let now = self.clock.now();
        self.set_open(c, false);
        let slot = &mut self.conns[c as usize];
        let pending = std::mem::take(&mut slot.pending);
        let inbox = std::mem::take(&mut slot.inbox);
        for (i, job) in pending.iter().enumerate() {
            let kind = if i == 0 { first.unwrap_or(rest) } else { rest };
            self.stats.on_fail(job, now, kind);
            self.in_flight -= 1;
        }
        for mut job in inbox.into_iter().rev() {
            job.conn = NO_CONN;
            self.in_flight -= 1;
            self.conn_stats.requeued += 1;
            if self.mode == Mode::Closed {
                // No queue in closed-loop mode: the request ends here.
                self.stats.on_not_sent(&job, now, NotSentReason::Queue);
            } else {
                self.queue.push_front(job);
            }
        }
        match close {
            Close::Server => self.conn_stats.closes_server += 1,
            Close::Client => self.conn_stats.closes_client += 1,
            Close::Error => self.conn_stats.closes_error += 1,
        }
        if !self.stopping {
            self.pump();
        }
    }

    fn in_system(&self) -> usize {
        self.queue.len() + self.in_flight
    }
}

type Shared = Rc<RefCell<State>>;

async fn writer_loop<W: RequestWriter>(st: &Shared, c: u32, mut w: W, notify: Rc<Notify>) {
    loop {
        let job = {
            let mut s = st.borrow_mut();
            let now = s.clock.now();
            let s = &mut *s;
            match s.conns[c as usize].inbox.pop_front() {
                Some(mut job) => {
                    job.send = now;
                    s.stats.on_send(&job);
                    let slot = &mut s.conns[c as usize];
                    slot.pending.push_back(job);
                    slot.r_notify.notify_one();
                    Some(job)
                }
                None => None,
            }
        };
        let Some(job) = job else {
            notify.notified().await;
            continue;
        };
        let ctx = RequestCtx {
            seq: job.seq,
            scenario: job.scenario,
        };
        if let Err(kind) = w.write_request(&ctx).await {
            st.borrow_mut().conn_down(c, None, kind, Close::Error);
            return;
        }
    }
}

const FAR: Duration = Duration::from_secs(365 * 86_400);

enum Event<T> {
    Response(T),
    Timeout,
    Wake,
}

async fn reader_loop<R: ResponseReader>(
    st: &Shared,
    c: u32,
    mut r: R,
    notify: Rc<Notify>,
    timeout_ns: u64,
) {
    let sleep = tokio::time::sleep(FAR);
    tokio::pin!(sleep);
    let mut armed: Option<u64> = None;
    loop {
        // Pending requests are in send order, so the oldest has the earliest
        // deadline.
        let (deadline, now) = {
            let s = st.borrow();
            let d = s.conns[c as usize]
                .pending
                .front()
                .map(|j| j.send + timeout_ns);
            (d, s.clock.now())
        };
        if deadline != armed {
            let at = match deadline {
                Some(d) => {
                    tokio::time::Instant::now() + Duration::from_nanos(d.saturating_sub(now))
                }
                None => tokio::time::Instant::now() + FAR,
            };
            sleep.as_mut().reset(at);
            armed = deadline;
        }
        let ev = tokio::select! {
            biased;
            res = r.read_response() => Event::Response(res),
            _ = &mut sleep, if deadline.is_some() => Event::Timeout,
            _ = notify.notified(), if deadline.is_none() => Event::Wake,
        };
        match ev {
            Event::Wake => {}
            Event::Timeout => {
                let seq = st.borrow().conns[c as usize].pending.front().map(|j| j.seq);
                let Some(seq) = seq else { continue };
                if r.cancel(seq) {
                    // Only this request is abandoned; the connection stays.
                    let mut s = st.borrow_mut();
                    let now = s.clock.now();
                    let job = s.conns[c as usize].pending.pop_front().unwrap();
                    s.stats.on_fail(&job, now, FailKind::Timeout);
                    s.conn_stats.cancelled += 1;
                    s.in_flight -= 1;
                    s.mark_idle(c);
                    s.pump();
                    armed = None;
                } else {
                    st.borrow_mut().conn_down(
                        c,
                        Some(FailKind::Timeout),
                        FailKind::Aborted,
                        Close::Client,
                    );
                    return;
                }
            }
            Event::Response(res) => {
                let mut s = st.borrow_mut();
                let now = s.clock.now();
                match res {
                    Ok(Some(resp)) => {
                        let pending = &mut s.conns[c as usize].pending;
                        let pos = match resp.seq {
                            Some(seq) => pending.iter().position(|j| j.seq == seq),
                            None => (!pending.is_empty()).then_some(0),
                        };
                        let Some(job) = pos.and_then(|p| pending.remove(p)) else {
                            // A response nobody asked for.
                            s.conn_down(c, None, FailKind::Protocol, Close::Error);
                            return;
                        };
                        if pos == Some(0) {
                            armed = None;
                        }
                        match resp.fail {
                            Some(kind) => s.stats.on_fail(&job, now, kind),
                            None => {
                                let fb = s
                                    .clock
                                    .from_raw(resp.first_byte_raw_ns)
                                    .clamp(job.send, now);
                                s.stats
                                    .on_complete(&job, fb, now, resp.status, resp.bytes_in);
                            }
                        }
                        s.in_flight -= 1;
                        if resp.close {
                            s.conn_down(c, None, FailKind::Read, Close::Server);
                            return;
                        }
                        s.mark_idle(c);
                        s.pump();
                    }
                    Ok(None) => {
                        s.conn_down(c, None, FailKind::Read, Close::Server);
                        return;
                    }
                    Err(kind) => {
                        s.conn_down(c, None, kind, Close::Error);
                        return;
                    }
                }
            }
        }
    }
}

async fn connect_once<P: Protocol>(
    proto: &P,
    timeout: Duration,
) -> Result<(P::Writer, P::Reader), ConnectErrorKind> {
    match tokio::time::timeout(timeout, proto.connect()).await {
        Ok(r) => r,
        Err(_) => Err(ConnectErrorKind::ConnectTimeout),
    }
}

async fn supervise<P: Protocol>(
    st: Shared,
    proto: Arc<P>,
    c: u32,
    mut halves: Option<(P::Writer, P::Reader)>,
    on_demand: bool,
    connect_timeout: Duration,
    timeout_ns: u64,
) {
    let mut backoff = Duration::from_millis(10);
    let mut first = true;
    loop {
        let (w, r) = match halves.take() {
            Some(h) => h,
            None => {
                if st.borrow().stopping {
                    return;
                }
                st.borrow_mut().conn_stats.connect_attempts += 1;
                match connect_once(&*proto, connect_timeout).await {
                    Ok(h) => {
                        let mut s = st.borrow_mut();
                        if first && on_demand {
                            s.connecting -= 1;
                        } else {
                            s.conn_stats.reconnects += 1;
                        }
                        backoff = Duration::from_millis(10);
                        h
                    }
                    Err(kind) => {
                        st.borrow_mut().conn_stats.connect_errors[kind.index()] += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(1));
                        continue;
                    }
                }
            }
        };
        first = false;
        let (wn, rn) = {
            let mut s = st.borrow_mut();
            s.set_open(c, true);
            let slot = &s.conns[c as usize];
            let n = (slot.w_notify.clone(), slot.r_notify.clone());
            s.mark_idle(c);
            s.pump();
            n
        };
        tokio::select! {
            _ = writer_loop(&st, c, w, wn) => {}
            _ = reader_loop(&st, c, r, rn, timeout_ns) => {}
        }
    }
}

async fn ticker(st: Shared, stop: Arc<AtomicBool>) {
    let mut iv = tokio::time::interval(Duration::from_millis(10));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        iv.tick().await;
        let mut s = st.borrow_mut();
        let now = s.clock.now();
        if stop.load(Ordering::Relaxed) && !s.stop_requested {
            s.stop_requested = true;
            s.closed_running = false;
            s.stop_notify.notify_one();
        }
        if s.mode == Mode::Open {
            s.expire_queue(now);
        } else if now >= s.end_ns {
            s.closed_running = false;
        }
        s.stats.flush(now);
    }
}

/// Runs the open-loop dispatcher. Returns the spin time, and whether a stop
/// request cut the schedule short.
async fn dispatch_open(
    st: &Shared,
    mut arrivals: Arrivals,
    timer: &Timer,
    spin_ns: u64,
) -> Result<(u64, bool), String> {
    let (clock, stop) = {
        let s = st.borrow();
        (s.clock, s.stop_notify.clone())
    };
    let mut spin_total = 0u64;
    let mut batch = 0u32;
    loop {
        if st.borrow().stop_requested {
            return Ok((spin_total, true));
        }
        let next = match arrivals.next_arrival() {
            Ok(Some(n)) => n,
            Ok(None) => break,
            Err(e) => return Err(e.to_string()),
        };
        let mut now = clock.now();
        if next.intended_ns > now {
            batch = 0;
            if next.intended_ns - now > spin_ns {
                let stopped = tokio::select! {
                    r = timer.wait_until(clock.to_raw(next.intended_ns - spin_ns)) => {
                        r.map_err(|e| format!("timer: {e}"))?;
                        false
                    }
                    _ = stop.notified() => true,
                };
                if stopped {
                    // This request was never scheduled: it lies after the stop.
                    return Ok((spin_total, true));
                }
            }
            let spin_start = raw_now_ns();
            SPINNING.with(|s| s.set(true));
            loop {
                now = clock.now();
                if now >= next.intended_ns {
                    break;
                }
                tokio::task::yield_now().await;
            }
            SPINNING.with(|s| s.set(false));
            spin_total += raw_now_ns() - spin_start;
        } else {
            // Behind schedule: dispatch in a batch, but let the connections
            // run from time to time.
            batch += 1;
            if batch >= 32 {
                batch = 0;
                tokio::task::yield_now().await;
                now = clock.now();
            }
        }
        let scenario = st.borrow().picker.pick(next.seq);
        let job = Job {
            seq: next.seq,
            intended: next.intended_ns,
            attempt: now,
            send: 0,
            phase: next.phase,
            scenario,
            conn: NO_CONN,
        };
        let mut s = st.borrow_mut();
        s.stats.on_scheduled(next.phase, scenario, next.intended_ns);
        s.stats.on_attempt(&job);
        s.admit(job);
    }
    Ok((spin_total, false))
}

pub fn run_worker<P: Protocol>(setup: WorkerSetup<P>) -> WorkerFinal {
    if let Some(cpu) = setup.pin {
        pin_to_cpu(cpu);
    }
    let _ = reduce_timer_slack();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            // Keep the rendezvous so that the other workers do not hang.
            setup.sync.barrier.wait();
            setup.sync.barrier.wait();
            let _ = setup
                .window_tx
                .send(WindowMsg::Finished { worker: setup.id });
            return failed_final(&setup, format!("runtime: {e}"));
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, worker_main(setup))
}

fn failed_final<P: Protocol>(s: &WorkerSetup<P>, error: String) -> WorkerFinal {
    WorkerFinal {
        id: s.id,
        connections: s.conns,
        max_in_flight: s.max_in_flight,
        phases: vec![PhaseCounters::default(); s.profile.phases.len()],
        conn: ConnStats::default(),
        wall_ns: 0,
        busy_ns: 0,
        spin_ns: 0,
        cpu: ThreadCpu::default(),
        trace_dropped: 0,
        stopped: false,
        error: Some(error),
    }
}

async fn worker_main<P: Protocol>(setup: WorkerSetup<P>) -> WorkerFinal {
    let cfg = setup.cfg.clone();
    let connect_timeout = cfg.connections.connect_timeout.0;
    let timeout_ns = cfg.timeouts.request.as_nanos();

    // 1. Open the connections, before T0.
    let mut conn_stats = ConnStats {
        requested: setup.conns as u64,
        ..Default::default()
    };
    let sem = Rc::new(tokio::sync::Semaphore::new(256));
    let mut tasks = Vec::with_capacity(setup.conns);
    for _ in 0..setup.conns {
        let sem = sem.clone();
        let proto = setup.protocol.clone();
        tasks.push(tokio::task::spawn_local(async move {
            let _permit = sem.acquire().await;
            connect_once(&*proto, connect_timeout).await
        }));
    }
    let mut initial = Vec::with_capacity(setup.conns);
    for t in tasks {
        conn_stats.connect_attempts += 1;
        match t.await {
            Ok(Ok(h)) => {
                conn_stats.established += 1;
                initial.push(Some(h));
            }
            Ok(Err(kind)) => {
                conn_stats.shortfall[kind.index()] += 1;
                conn_stats.connect_errors[kind.index()] += 1;
                initial.push(None);
            }
            Err(_) => {
                conn_stats.shortfall[ConnectErrorKind::Other.index()] += 1;
                initial.push(None);
            }
        }
    }
    let arrivals = Arrivals::new(
        &cfg,
        setup.profile.clone(),
        setup.seed,
        setup.id,
        setup.workers,
    );
    let timer = Timer::new();

    // 2. Rendezvous; T0 is set between the two waits.
    setup.sync.barrier.wait();
    setup.sync.barrier.wait();
    let t0 = *setup
        .sync
        .t0
        .get()
        .expect("T0 is set before the second wait");
    let clock = RunClock { t0_raw: t0 };

    let (arrivals, timer) = match (arrivals, timer) {
        (Ok(a), Ok(t)) => (a, t),
        (Err(e), _) => {
            let _ = setup
                .window_tx
                .send(WindowMsg::Finished { worker: setup.id });
            return failed_final(&setup, format!("schedule: {e}"));
        }
        (_, Err(e)) => {
            let _ = setup
                .window_tx
                .send(WindowMsg::Finished { worker: setup.id });
            return failed_final(&setup, format!("timer: {e}"));
        }
    };

    let success = StatusSet::parse(&cfg.http.success_status).expect("validated");
    let stats = Stats::new(
        setup.profile.clone(),
        cfg.output.window.as_nanos(),
        setup.window_tx.clone(),
        setup.id,
        cfg.fidelity.conn_wait_threshold.as_nanos(),
        success,
        setup.trace,
        cfg.scenarios.len(),
    );
    let st: Shared = Rc::new(RefCell::new(State {
        clock,
        stats,
        queue: VecDeque::new(),
        idle: VecDeque::new(),
        conns: (0..setup.conns).map(|_| Slot::new()).collect(),
        conns_max: setup.conns_max,
        connecting: 0,
        open_count: 0,
        spawn: None,
        in_flight: 0,
        max_in_flight: setup.max_in_flight,
        depth: setup.depth,
        max_queue_len: cfg.connections.max_queue_len,
        max_queue_delay: cfg.connections.max_queue_delay.as_nanos(),
        mode: cfg.mode,
        closed_running: false,
        closed_j: 0,
        worker: setup.id as u64,
        workers: setup.workers as u64,
        end_ns: setup.profile.end_ns,
        conn_stats,
        stopping: false,
        stop_requested: false,
        stop_notify: Rc::new(Notify::new()),
        picker: ScenarioPicker::new(&cfg, setup.seed),
    }));

    let cpu0 = thread_cpu();
    let busy0 = BUSY.with(|b| b.get());
    let wall0 = raw_now_ns();

    // 3. Connection supervisors and the ticker. Connections opened on demand
    // get their supervisor through the spawner.
    let handles: Rc<RefCell<Vec<JoinHandle<()>>>> = Rc::new(RefCell::new(Vec::new()));
    for (i, h) in initial.into_iter().enumerate() {
        handles
            .borrow_mut()
            .push(tokio::task::spawn_local(timed(supervise(
                st.clone(),
                setup.protocol.clone(),
                i as u32,
                h,
                false,
                connect_timeout,
                timeout_ns,
            ))));
    }
    {
        let weak: Weak<RefCell<State>> = Rc::downgrade(&st);
        let proto = setup.protocol.clone();
        let hs = handles.clone();
        let spawner: Spawner = Rc::new(move |c: u32| {
            if let Some(st) = weak.upgrade() {
                hs.borrow_mut()
                    .push(tokio::task::spawn_local(timed(supervise(
                        st,
                        proto.clone(),
                        c,
                        None,
                        true,
                        connect_timeout,
                        timeout_ns,
                    ))));
            }
        });
        st.borrow_mut().spawn = Some(spawner);
    }
    handles
        .borrow_mut()
        .push(tokio::task::spawn_local(timed(ticker(
            st.clone(),
            setup.stop.clone(),
        ))));

    // 4. Load.
    let mut error = None;
    let mut spin_ns = 0;
    let mut stopped = false;
    match cfg.mode {
        Mode::Open => match (Timed {
            inner: Box::pin(dispatch_open(
                &st,
                arrivals,
                &timer,
                cfg.timer.spin.as_nanos(),
            )),
            dispatcher: true,
        })
        .await
        {
            Ok((s, cut)) => {
                spin_ns = s;
                stopped = cut;
            }
            Err(e) => error = Some(e),
        },
        Mode::Closed => {
            if let Some(d) = clock.to_raw(0).checked_sub(raw_now_ns()) {
                tokio::time::sleep(Duration::from_nanos(d)).await;
            }
            {
                let mut s = st.borrow_mut();
                s.closed_running = true;
                s.pump();
            }
            let end = setup.profile.end_ns;
            let now = clock.now();
            let stop = st.borrow().stop_notify.clone();
            if end > now {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_nanos(end - now)) => {}
                    _ = stop.notified() => stopped = true,
                }
            }
            st.borrow_mut().closed_running = false;
        }
    }

    // 5. Drain. After a stop, the drain starts at once.
    let dispatch_end = clock.now();
    let drain_from = if stopped {
        dispatch_end
    } else {
        dispatch_end.max(setup.profile.end_ns)
    };
    let deadline = drain_from + cfg.timeouts.drain.as_nanos();
    loop {
        if st.borrow().in_system() == 0 {
            break;
        }
        let now = clock.now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1).min(Duration::from_nanos(deadline - now)))
            .await;
    }

    // 6. Stop and count what is left.
    st.borrow_mut().stopping = true;
    st.borrow_mut().spawn = None;
    for h in handles.borrow().iter() {
        h.abort();
    }
    let mut s = st.borrow_mut();
    let now = clock.now();
    let s = &mut *s;
    while let Some(job) = s.queue.pop_front() {
        s.stats.on_not_sent(&job, now, NotSentReason::EndOfRun);
    }
    for c in 0..s.conns.len() {
        let inbox = std::mem::take(&mut s.conns[c].inbox);
        for job in inbox {
            s.stats.on_not_sent(&job, now, NotSentReason::EndOfRun);
        }
        let pending = std::mem::take(&mut s.conns[c].pending);
        for job in pending {
            s.stats.on_in_flight_at_end(&job);
        }
    }
    s.stats.finish(now);
    let cpu1 = thread_cpu();
    let trace_dropped = s.stats.trace.take().map(|t| t.dropped).unwrap_or(0);
    WorkerFinal {
        id: setup.id,
        connections: s.conns.len(),
        max_in_flight: setup.max_in_flight,
        phases: s.stats.phases.clone(),
        conn: s.conn_stats.clone(),
        wall_ns: raw_now_ns() - wall0,
        busy_ns: BUSY.with(|b| b.get()) - busy0,
        spin_ns,
        cpu: ThreadCpu {
            cpu_ns: cpu1.cpu_ns.saturating_sub(cpu0.cpu_ns),
            involuntary_switches: cpu1
                .involuntary_switches
                .saturating_sub(cpu0.involuntary_switches),
            runqueue_ns: match (cpu0.runqueue_ns, cpu1.runqueue_ns) {
                (Some(a), Some(b)) => Some(b.saturating_sub(a)),
                _ => None,
            },
        },
        trace_dropped,
        stopped,
        error,
    }
}
