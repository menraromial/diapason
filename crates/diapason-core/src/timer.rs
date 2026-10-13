//! A timer with sub-millisecond wake-ups.
//!
//! tokio's timer has a 1 ms resolution. On Linux the worker waits on a
//! timerfd armed with an absolute CLOCK_MONOTONIC deadline, which has
//! nanosecond resolution. Elsewhere (and on Linux when the environment
//! variable `DIAPASON_PORTABLE_TIMER` is set to a non-empty value) it sleeps
//! on tokio's timer until `PORTABLE_EARLY` before the deadline. In both cases
//! the caller spins (yielding) for the remainder.
//!
//! On a macOS CI machine, tokio's sleep (kevent) woke more than 1.2 ms late
//! for 1 % of the waits, and a condition variable with a nanosecond timeout
//! woke 6 ms late at p90; hence a generous early margin rather than a timer
//! thread.

use std::io;
use std::time::Duration;

/// How long before the deadline the portable timer returns.
pub const PORTABLE_EARLY: Duration = Duration::from_millis(3);

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
    Portable,
}

impl Timer {
    pub fn new() -> io::Result<Timer> {
        #[cfg(target_os = "linux")]
        if std::env::var_os("DIAPASON_PORTABLE_TIMER").is_none_or(|v| v.is_empty()) {
            return Ok(Timer::Fd(fd::FdTimer::new()?));
        }
        Ok(Timer::Portable)
    }

    /// Returns at or shortly after the raw monotonic time `deadline` (Linux),
    /// or up to `PORTABLE_EARLY` before it (portable).
    pub async fn wait_until(&self, deadline: u64) -> io::Result<()> {
        match self {
            #[cfg(target_os = "linux")]
            Timer::Fd(t) => t.wait_until(deadline).await,
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
