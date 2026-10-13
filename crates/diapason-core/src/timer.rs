//! A timer with sub-millisecond wake-ups.
//!
//! tokio's timer has a 1 ms resolution. On Linux the worker waits on a
//! timerfd armed with an absolute CLOCK_MONOTONIC deadline, which has
//! nanosecond resolution. Elsewhere it uses a tokio sleep that ends 1.1 ms
//! early. In both cases the caller spins (yielding) for the remainder.

use std::io;

#[cfg(target_os = "linux")]
mod imp {
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use tokio::io::unix::AsyncFd;

    pub struct Timer {
        fd: AsyncFd<OwnedFd>,
    }

    impl Timer {
        pub fn new() -> io::Result<Timer> {
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
            Ok(Timer {
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

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;

    pub struct Timer;

    impl Timer {
        pub fn new() -> io::Result<Timer> {
            Ok(Timer)
        }

        /// Returns about 1.1 ms before the raw monotonic time `deadline`, or
        /// at once if that is already past.
        pub async fn wait_until(&self, deadline: u64) -> io::Result<()> {
            let now = crate::clock::raw_now_ns();
            let early = deadline.saturating_sub(1_100_000);
            if early > now {
                tokio::time::sleep(std::time::Duration::from_nanos(early - now)).await;
            }
            Ok(())
        }
    }
}

pub use imp::Timer;

/// Lowers the kernel timer slack of the calling thread to 1 ns, so that
/// timerfd wake-ups are not deferred by the default 50 µs slack.
pub fn reduce_timer_slack() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: prctl with an integer argument.
        let rc = unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong, 0, 0, 0) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
