//! A timer with sub-millisecond wake-ups.
//!
//! tokio's timer has a 1 ms resolution. On Linux the worker waits on a
//! timerfd armed with an absolute CLOCK_MONOTONIC deadline, which has
//! nanosecond resolution. Elsewhere (and on Linux when the environment
//! variable `DIAPASON_PORTABLE_TIMER` is set to a non-empty value) one process-wide thread keeps a
//! heap of deadlines, sleeps on a condition variable with a nanosecond
//! timeout and wakes the waiting worker. In both cases the caller spins
//! (yielding) for the remainder.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[cfg(target_os = "linux")]
mod fd {
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use tokio::io::unix::AsyncFd;

    pub struct FdTimer {
        fd: AsyncFd<OwnedFd>,
    }

    impl FdTimer {
        pub fn new() -> io::Result<FdTimer> {
            // SAFETY: plain syscall; the returned fd is owned below.
            let fd = unsafe {
                libc::timerfd_create(
                    libc::CLOCK_MONOTONIC,
                    libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fd is a fresh descriptor that nothing else owns.
            let owned = unsafe { OwnedFd::from_raw_fd(fd as RawFd) };
            Ok(FdTimer {
                fd: AsyncFd::new(owned)?,
            })
        }

        /// Returns at or shortly after the raw monotonic time `deadline`.
        pub async fn wait_until(&self, deadline: u64) -> io::Result<()> {
            let spec = libc::itimerspec {
                it_interval: libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: libc::timespec {
                    tv_sec: (deadline / 1_000_000_000) as libc::time_t,
                    tv_nsec: (deadline % 1_000_000_000).max(1) as libc::c_long,
                },
            };
            let raw = self.fd.as_raw_fd();
            // SAFETY: valid fd and itimerspec; old value not requested.
            let rc = unsafe {
                libc::timerfd_settime(raw, libc::TFD_TIMER_ABSTIME, &spec, std::ptr::null_mut())
            };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
            loop {
                let mut guard = self.fd.readable().await?;
                let mut buf = [0u8; 8];
                // SAFETY: reading 8 bytes into an 8-byte buffer.
                let n = unsafe { libc::read(raw, buf.as_mut_ptr() as *mut libc::c_void, 8) };
                if n == 8 {
                    return Ok(());
                }
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::WouldBlock {
                    guard.clear_ready();
                    if crate::clock::raw_now_ns() >= deadline {
                        return Ok(());
                    }
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// One waiter's registration with the timer thread. Arming it again with
/// the same deadline adds nothing to the heap, and entries whose slot was
/// re-armed since are dropped without waking anyone.
#[derive(Default)]
pub(crate) struct Slot {
    deadline: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

struct Entry {
    deadline: u64,
    slot: Arc<Slot>,
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
struct Wheel {
    heap: Mutex<BinaryHeap<Reverse<Entry>>>,
    cv: Condvar,
}

fn wheel() -> &'static Wheel {
    static WHEEL: OnceLock<&'static Wheel> = OnceLock::new();
    WHEEL.get_or_init(|| {
        let w: &'static Wheel = Box::leak(Box::default());
        std::thread::Builder::new()
            .name("diapason-timer".into())
            .spawn(move || run_wheel(w))
            .expect("timer thread");
        w
    })
}

fn run_wheel(w: &'static Wheel) {
    let _ = tune_thread_timers();
    let mut due = Vec::new();
    let mut heap = w.heap.lock().unwrap();
    loop {
        let now = crate::clock::raw_now_ns();
        while heap.peek().is_some_and(|e| e.0.deadline <= now) {
            let e = heap.pop().unwrap().0;
            if e.slot.deadline.load(Ordering::Acquire) == e.deadline {
                due.push(e.slot);
            }
        }
        if !due.is_empty() {
            drop(heap);
            for slot in due.drain(..) {
                if let Some(wk) = slot.waker.lock().unwrap().take() {
                    wk.wake();
                }
            }
            heap = w.heap.lock().unwrap();
            continue;
        }
        let next = heap.peek().map(|e| e.0.deadline);
        heap = match next {
            None => w.cv.wait(heap).unwrap(),
            Some(d) => {
                w.cv.wait_timeout(heap, Duration::from_nanos(d - now))
                    .unwrap()
                    .0
            }
        };
    }
}

struct WheelSleep<'a> {
    deadline: u64,
    slot: &'a Arc<Slot>,
}

impl Future for WheelSleep<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if crate::clock::raw_now_ns() >= self.deadline {
            return Poll::Ready(());
        }
        *self.slot.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.slot.deadline.swap(self.deadline, Ordering::AcqRel) != self.deadline {
            let w = wheel();
            let mut heap = w.heap.lock().unwrap();
            let earliest = heap.peek().is_none_or(|e| self.deadline < e.0.deadline);
            heap.push(Reverse(Entry {
                deadline: self.deadline,
                slot: self.slot.clone(),
            }));
            drop(heap);
            if earliest {
                w.cv.notify_one();
            }
        }
        Poll::Pending
    }
}

pub enum Timer {
    #[cfg(target_os = "linux")]
    Fd(fd::FdTimer),
    Thread(Arc<Slot>),
}

impl Timer {
    pub fn new() -> io::Result<Timer> {
        #[cfg(target_os = "linux")]
        if std::env::var_os("DIAPASON_PORTABLE_TIMER").is_none_or(|v| v.is_empty()) {
            return Ok(Timer::Fd(fd::FdTimer::new()?));
        }
        Ok(Timer::Thread(Arc::new(Slot::default())))
    }

    /// Returns at or shortly after the raw monotonic time `deadline`.
    pub async fn wait_until(&self, deadline: u64) -> io::Result<()> {
        match self {
            #[cfg(target_os = "linux")]
            Timer::Fd(t) => t.wait_until(deadline).await,
            Timer::Thread(slot) => {
                WheelSleep { deadline, slot }.await;
                Ok(())
            }
        }
    }
}

/// Makes the kernel wake the calling thread's timers on time: on Linux the
/// timer slack drops to 1 ns (the default 50 µs would defer wake-ups); on
/// macOS the thread gets the user-interactive quality of service, whose
/// timers are not coalesced.
pub fn tune_thread_timers() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: prctl with an integer argument.
        let rc = unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong, 0, 0, 0) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(target_os = "macos")]
    {
        // SAFETY: sets the calling thread's own QoS class.
        let rc = unsafe {
            libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0)
        };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The timer thread wakes a current-thread runtime within a few tens of
    /// microseconds of the deadline (on an idle machine).
    #[test]
    fn wheel_precision() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let slot = Arc::new(Slot::default());
        let mut late = rt.block_on(async {
            let mut late = Vec::new();
            for _ in 0..200 {
                let d = crate::clock::raw_now_ns() + 1_000_000;
                WheelSleep {
                    deadline: d,
                    slot: &slot,
                }
                .await;
                late.push(crate::clock::raw_now_ns() - d);
            }
            late
        });
        late.sort_unstable();
        let p90 = late[180];
        assert!(p90 < 500_000, "p90 lateness {p90} ns");
    }
}
