//! Host description and process limits.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HostInfo {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub kernel: String,
    pub cores: usize,
    pub fd_soft_initial: u64,
    pub fd_soft: u64,
    pub fd_hard: u64,
    pub ephemeral_ports: Option<[u32; 2]>,
}

fn uname() -> (String, String) {
    // SAFETY: utsname is plain data, filled by uname().
    unsafe {
        let mut u: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut u) != 0 {
            return (String::new(), String::new());
        }
        let s = |f: &[libc::c_char]| {
            std::ffi::CStr::from_ptr(f.as_ptr())
                .to_string_lossy()
                .into_owned()
        };
        (s(&u.nodename), s(&u.release))
    }
}

// rlim_t is u64 on Linux and macOS, but not on every Unix.
#[allow(clippy::unnecessary_cast)]
pub fn get_nofile() -> (u64, u64) {
    let mut r = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: valid out-pointer.
    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) };
    (r.rlim_cur as u64, r.rlim_max as u64)
}

/// Raises the soft RLIMIT_NOFILE to the hard limit. Returns (initial soft, new
/// soft, hard).
pub fn raise_nofile() -> (u64, u64, u64) {
    let (soft, hard) = get_nofile();
    // macOS refuses values above OPEN_MAX for the soft limit.
    #[cfg(target_os = "macos")]
    let target = hard.min(10240);
    #[cfg(not(target_os = "macos"))]
    let target = hard;
    if target > soft {
        let r = libc::rlimit {
            rlim_cur: target as libc::rlim_t,
            rlim_max: hard as libc::rlim_t,
        };
        // SAFETY: valid rlimit value.
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &r) };
    }
    let (new_soft, hard) = get_nofile();
    (soft, new_soft, hard)
}

pub fn ephemeral_ports() -> Option<[u32; 2]> {
    let s = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").ok()?;
    let mut it = s.split_whitespace().filter_map(|x| x.parse().ok());
    Some([it.next()?, it.next()?])
}

pub fn host_info(fd: (u64, u64, u64)) -> HostInfo {
    let (hostname, kernel) = uname();
    HostInfo {
        hostname,
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        kernel,
        cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        fd_soft_initial: fd.0,
        fd_soft: fd.1,
        fd_hard: fd.2,
        ephemeral_ports: ephemeral_ports(),
    }
}

/// CPU accounting for the calling thread.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ThreadCpu {
    pub cpu_ns: u64,
    pub involuntary_switches: u64,
    /// Time spent runnable but waiting for a CPU, when the kernel reports it.
    pub runqueue_ns: Option<u64>,
}

pub fn thread_cpu() -> ThreadCpu {
    let mut out = ThreadCpu::default();
    #[cfg(target_os = "linux")]
    {
        // SAFETY: valid out-pointers.
        unsafe {
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            if libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) == 0 {
                out.cpu_ns = ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
            }
            let mut ru: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_THREAD, &mut ru) == 0 {
                out.involuntary_switches = ru.ru_nivcsw as u64;
            }
        }
        // SAFETY: gettid has no preconditions.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) };
        if let Ok(s) = std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat")) {
            out.runqueue_ns = s.split_whitespace().nth(1).and_then(|x| x.parse().ok());
        }
    }
    out
}

/// Pins the calling thread to one CPU (Linux only; a no-op elsewhere).
pub fn pin_to_cpu(cpu: usize) {
    #[cfg(target_os = "linux")]
    // SAFETY: cpu_set_t is plain data; sched_setaffinity on the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu % libc::CPU_SETSIZE as usize, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = cpu;
}
