//! Durations written as strings with a unit ("100us", "1.5s", "2m").

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::time::Duration;

/// A duration that (de)serializes as a string with a unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Dur(pub Duration);

impl Dur {
    pub const ZERO: Dur = Dur(Duration::ZERO);

    pub fn from_secs_f64(s: f64) -> Dur {
        Dur(Duration::from_secs_f64(s))
    }
    pub fn from_millis(ms: u64) -> Dur {
        Dur(Duration::from_millis(ms))
    }
    pub fn from_micros(us: u64) -> Dur {
        Dur(Duration::from_micros(us))
    }
    pub fn as_nanos(self) -> u64 {
        self.0.as_nanos().min(u64::MAX as u128) as u64
    }
    pub fn as_secs_f64(self) -> f64 {
        self.0.as_secs_f64()
    }
}

/// Parses "10s", "1.5ms", "100us", "250ns", "2m", "1h". A bare "0" is accepted.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .ok_or_else(|| format!("duration '{s}' has no unit (ns, us, ms, s, m, h)"))?;
    let (num, unit) = s.split_at(split);
    let value: f64 = num
        .parse()
        .map_err(|_| format!("duration '{s}': '{num}' is not a number"))?;
    let scale = match unit.trim() {
        "ns" => 1e-9,
        "us" | "µs" => 1e-6,
        "ms" => 1e-3,
        "s" => 1.0,
        "m" | "min" => 60.0,
        "h" => 3600.0,
        u => return Err(format!("duration '{s}': unknown unit '{u}'")),
    };
    let secs = value * scale;
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("duration '{s}' is out of range"));
    }
    Ok(Duration::from_secs_f64(secs))
}

impl std::str::FromStr for Dur {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_duration(s).map(Dur)
    }
}

impl fmt::Display for Dur {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ns = self.0.as_nanos();
        if ns == 0 {
            write!(f, "0s")
        } else if ns.is_multiple_of(1_000_000_000) {
            write!(f, "{}s", ns / 1_000_000_000)
        } else if ns.is_multiple_of(1_000_000) {
            write!(f, "{}ms", ns / 1_000_000)
        } else if ns.is_multiple_of(1_000) {
            write!(f, "{}us", ns / 1_000)
        } else {
            write!(f, "{ns}ns")
        }
    }
}

impl Serialize for Dur {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(parse_duration("100us").unwrap(), Duration::from_micros(100));
        assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("10parsecs").is_err());
    }

    #[test]
    fn displays_round_trip() {
        for s in ["0s", "10s", "250ms", "100us", "7ns"] {
            assert_eq!(s.parse::<Dur>().unwrap().to_string(), s);
        }
    }
}
