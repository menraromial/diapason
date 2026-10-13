//! Monotonic and wall clocks.

/// CLOCK_MONOTONIC in nanoseconds.
#[inline]
pub fn raw_now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a valid out-pointer; CLOCK_MONOTONIC always exists.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Nanoseconds since the Unix epoch, from the wall clock.
pub fn wall_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// The run clock: monotonic nanoseconds since T0.
#[derive(Clone, Copy, Debug)]
pub struct RunClock {
    pub t0_raw: u64,
}

impl RunClock {
    #[inline]
    pub fn now(&self) -> u64 {
        raw_now_ns().saturating_sub(self.t0_raw)
    }
    #[inline]
    pub fn to_raw(&self, t: u64) -> u64 {
        self.t0_raw + t
    }
    #[inline]
    pub fn from_raw(&self, raw: u64) -> u64 {
        raw.saturating_sub(self.t0_raw)
    }
}

/// Formats Unix nanoseconds as RFC 3339 in UTC, with microseconds.
pub fn rfc3339(unix_ns: u64) -> String {
    let secs = (unix_ns / 1_000_000_000) as i64;
    let micros = (unix_ns % 1_000_000_000) / 1000;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + (m <= 2) as i64;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{micros:06}Z",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn rfc3339_known_dates() {
        assert_eq!(super::rfc3339(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            super::rfc3339(1_700_000_000_123_456_000),
            "2023-11-14T22:13:20.123456Z"
        );
        assert_eq!(
            super::rfc3339(951_782_400_000_000_000),
            "2000-02-29T00:00:00.000000Z"
        );
    }
}
