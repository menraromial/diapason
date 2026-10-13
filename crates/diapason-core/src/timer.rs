//! A timer with sub-millisecond wake-ups.
//!
//! tokio's timer has a 1 ms resolution. On Linux the worker waits on a
//! timerfd armed with an absolute CLOCK_MONOTONIC deadline. On macOS it waits
//! on its own kqueue, armed with an `EVFILT_TIMER` in nanoseconds with
//! `NOTE_CRITICAL`, which asks the kernel not to coalesce it. Both
//! descriptors are watched by tokio like any socket. Elsewhere (and on Linux
//! or macOS when the environment variable `DIAPASON_PORTABLE_TIMER` is set to
//! a non-empty value) the worker sleeps on tokio's timer until
//! `PORTABLE_EARLY` before the deadline. In every case the caller spins
//! (yielding) for the remainder.

use std::io;
use std::time::Duration;

/// How long before the deadline the portable timer returns.
pub const PORTABLE_EARLY: Duration = Duration::from_micros(1500);

/// The portable timer is selected on platforms with a precise timer too.
#[allow(dead_code)]
fn portable_requested() -> bool {
    std::env::var_os("DIAPASON_PORTABLE_TIMER").is_some_and(|v| !v.is_empty())
}

#[cfg(target_os = "macos")]
mod kq {
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use tokio::io::unix::AsyncFd;

    pub struct KqTimer {
        fd: AsyncFd<OwnedFd>,
    }

    fn timer_event(flags: u16, ns: i64) -> libc::kevent {
        libc::kevent {
            ident: 1,
            filter: libc::EVFILT_TIMER,
            flags,
            fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
            data: ns as libc::intptr_t,
            udata: std::ptr::null_mut(),
        }
    }

    impl KqTimer {
        pub fn new() -> io::Result<KqTimer> {
            // SAFETY: plain syscalls on a descriptor we own.
            let fd = unsafe { libc::kqueue() };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fd is a fresh descriptor that nothing else owns.
            let owned = unsafe { OwnedFd::from_raw_fd(fd as RawFd) };
            // SAFETY: fcntl on our descriptor.
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                let fl = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK);
            }
            Ok(KqTimer {
                fd: AsyncFd::new(owned)?,
            })
        }

        /// Returns at or shortly after the raw monotonic time `deadline`.
        pub async fn wait_until(&self, deadline: u64) -> io::Result<()> {
            let now = crate::clock::raw_now_ns();
            if deadline <= now {
                return Ok(());
            }
            let raw = self.fd.as_raw_fd();
            let arm = timer_event(
                (libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT) as u16,
                (deadline - now) as i64,
            );
            // SAFETY: one change, no event list.
            let rc =
                unsafe { libc::kevent(raw, &arm, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
            let zero = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            loop {
                let mut guard = self.fd.readable().await?;
                let mut out = timer_event(0, 0);
                // SAFETY: collects at most one event without blocking.
                let n = unsafe { libc::kevent(raw, std::ptr::null(), 0, &mut out, 1, &zero) };
                if n > 0 {
                    return Ok(());
                }
                if n < 0 {
                    return Err(io::Error::last_os_error());
                }
                guard.clear_ready();
                if crate::clock::raw_now_ns() >= deadline {
                    return Ok(());
                }
            }
        }
    }
}

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

pub enum Timer {
    #[cfg(target_os = "linux")]
    Fd(fd::FdTimer),
    #[cfg(target_os = "macos")]
    Kq(kq::KqTimer),
    Portable,
}

impl Timer {
    pub fn new() -> io::Result<Timer> {
        #[cfg(target_os = "linux")]
        if !portable_requested() {
            return Ok(Timer::Fd(fd::FdTimer::new()?));
        }
        #[cfg(target_os = "macos")]
        if !portable_requested() {
            return Ok(Timer::Kq(kq::KqTimer::new()?));
        }
        Ok(Timer::Portable)
    }

    /// Returns at or shortly after the raw monotonic time `deadline` (Linux),
    /// or up to `PORTABLE_EARLY` before it (portable).
    pub async fn wait_until(&self, deadline: u64) -> io::Result<()> {
        match self {
            #[cfg(target_os = "linux")]
            Timer::Fd(t) => t.wait_until(deadline).await,
            #[cfg(target_os = "macos")]
            Timer::Kq(t) => t.wait_until(deadline).await,
            Timer::Portable => {
                let now = crate::clock::raw_now_ns();
                let early = deadline.saturating_sub(PORTABLE_EARLY.as_nanos() as u64);
                if early > now {
                    tokio::time::sleep(Duration::from_nanos(early - now)).await;
                }
                Ok(())
            }
        }
    }
}

/// Makes the kernel wake the calling thread's timers on time: on Linux the
/// timer slack drops to 1 ns (the default 50 µs would defer wake-ups); on
/// macOS the thread gets the user-interactive quality of service, which
/// lowers timer coalescing.
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
