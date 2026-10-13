//! A timer with sub-millisecond precision for the reference server.
//!
//! tokio's timer has a 1 ms resolution, which would add up to 1 ms to every
//! response. One dedicated thread keeps a heap of deadlines, waits on a
//! condition variable with a nanosecond timeout (timer slack lowered to 1 ns)
//! and wakes the tasks whose deadline has passed.
//!
//! Each waiter owns a [`TimerSlot`]. Arming a slot with the deadline it
//! already has does not add a heap entry, and entries whose slot has been
//! re-armed since are dropped without waking anyone, so the heap holds at
//! most one live entry per slot however often the waiter is polled.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// One waiter's registration.
#[derive(Default)]
pub struct TimerSlot {
    /// The armed deadline; 0 when disarmed.
    deadline: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

struct Entry {
    deadline: u64,
    slot: Arc<TimerSlot>,
}

impl PartialEq for Entry {
    fn eq(&self, o: &Self) -> bool {
        self.deadline == o.deadline
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Entry {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.deadline.cmp(&o.deadline)
    }
}

#[derive(Default)]
struct Shared {
    heap: Mutex<(BinaryHeap<Reverse<Entry>>, bool)>,
    cv: Condvar,
}

#[derive(Clone)]
pub struct PreciseTimer(Arc<Shared>);

impl PreciseTimer {
    pub fn start() -> PreciseTimer {
        let shared = Arc::new(Shared::default());
        let s = shared.clone();
        std::thread::Builder::new()
            .name("diapason-server-timer".into())
            .spawn(move || run(s))
            .expect("timer thread");
        PreciseTimer(shared)
    }

    /// Completes at or after the raw monotonic time `deadline`.
    pub fn sleep_until(&self, slot: &Arc<TimerSlot>, deadline: u64) -> Sleep {
        Sleep {
            deadline,
            slot: slot.clone(),
            timer: self.0.clone(),
        }
    }

    pub fn stop(&self) {
        self.0.heap.lock().unwrap().1 = true;
        self.0.cv.notify_one();
    }
}

fn run(s: Arc<Shared>) {
    #[cfg(target_os = "linux")]
    // SAFETY: prctl with an integer argument, on the calling thread.
    unsafe {
        libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong, 0, 0, 0);
    }
    let mut due = Vec::new();
    let mut g = s.heap.lock().unwrap();
    loop {
        if g.1 {
            return;
        }
        let now = crate::now_ns();
        while g.0.peek().is_some_and(|e| e.0.deadline <= now) {
            let e = g.0.pop().unwrap().0;
            // Skip entries whose slot was re-armed for another deadline.
            if e.slot.deadline.load(Ordering::Acquire) == e.deadline {
                due.push(e.slot);
            }
        }
        if !due.is_empty() {
            drop(g);
            for slot in due.drain(..) {
                if let Some(w) = slot.waker.lock().unwrap().take() {
                    w.wake();
                }
            }
            g = s.heap.lock().unwrap();
            continue;
        }
        let next = g.0.peek().map(|e| e.0.deadline);
        g = match next {
            None => s.cv.wait(g).unwrap(),
            Some(d) => {
                s.cv.wait_timeout(g, Duration::from_nanos(d - now))
                    .unwrap()
                    .0
            }
        };
    }
}

pub struct Sleep {
    deadline: u64,
    slot: Arc<TimerSlot>,
    timer: Arc<Shared>,
}

impl Future for Sleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if crate::now_ns() >= self.deadline {
            return Poll::Ready(());
        }
        *self.slot.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.slot.deadline.swap(self.deadline, Ordering::AcqRel) != self.deadline {
            let mut g = self.timer.heap.lock().unwrap();
            let earliest = g.0.peek().is_none_or(|e| self.deadline < e.0.deadline);
            g.0.push(Reverse(Entry {
                deadline: self.deadline,
                slot: self.slot.clone(),
            }));
            drop(g);
            if earliest {
                self.timer.cv.notify_one();
            }
        }
        Poll::Pending
    }
}
