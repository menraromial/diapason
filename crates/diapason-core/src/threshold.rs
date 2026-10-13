//! User thresholds: assertions such as `p99 < 200ms`, `error_rate < 0.1%` or
//! `achieved_rate >= 0.99 * offered_rate`, evaluated on one phase after the
//! run. They never replace the fidelity checks.
//!
//! Grammar: `METRIC OP RHS`, where OP is `<`, `<=`, `>` or `>=` and RHS is a
//! number with an optional unit (`ns`, `us`, `ms`, `s` for latencies, `%`
//! for ratios), another metric, or `NUMBER * METRIC`.
//!
//! Metrics: latency statistics `[KIND.]STAT`, with KIND one of `corrected`
//! (the default), `server`, `connection_wait`, `generator_lag` and STAT one of
//! `min`, `mean`, `p50`, `p90`, `p99`, `p99.9`, `p99.99`, `max`, in
//! milliseconds; rates `offered_rate`, `nominal_rate`, `achieved_rate` (by
//! intended time), `achieved_rate_by_completion`, in requests per second;
//! ratios `success_ratio`, `error_rate` (1 - success_ratio), `failed_rate`,
//! `not_sent_rate` (over scheduled requests).

use crate::hist::LatencyStats;
use crate::summary::PhaseSummary;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Corrected,
    Server,
    ConnectionWait,
    GeneratorLag,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Stat {
    Min,
    Mean,
    P50,
    P90,
    P99,
    P999,
    P9999,
    Max,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Metric {
    Latency(Kind, Stat),
    OfferedRate,
    NominalRate,
    AchievedRate,
    AchievedRateByCompletion,
    SuccessRatio,
    ErrorRate,
    FailedRate,
    NotSentRate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Unit {
    Latency,
    Rate,
    Ratio,
}

impl Metric {
    fn unit(self) -> Unit {
        match self {
            Metric::Latency(..) => Unit::Latency,
            Metric::OfferedRate
            | Metric::NominalRate
            | Metric::AchievedRate
            | Metric::AchievedRateByCompletion => Unit::Rate,
            _ => Unit::Ratio,
        }
    }

    fn parse(s: &str) -> Result<Metric, String> {
        let s = s.trim();
        let simple = match s {
            "offered_rate" | "offered" => Some(Metric::OfferedRate),
            "nominal_rate" => Some(Metric::NominalRate),
            "achieved_rate" | "achieved_rate_by_intended" => Some(Metric::AchievedRate),
            "achieved_rate_by_completion" => Some(Metric::AchievedRateByCompletion),
            "success_ratio" => Some(Metric::SuccessRatio),
            "error_rate" => Some(Metric::ErrorRate),
            "failed_rate" => Some(Metric::FailedRate),
            "not_sent_rate" => Some(Metric::NotSentRate),
            _ => None,
        };
        if let Some(m) = simple {
            return Ok(m);
        }
        let (kind, stat) = match s.split_once('.') {
            Some((k, st)) if !k.starts_with('p') => {
                let kind = match k {
                    "corrected" => Kind::Corrected,
                    "server" => Kind::Server,
                    "connection_wait" => Kind::ConnectionWait,
                    "generator_lag" => Kind::GeneratorLag,
                    _ => return Err(format!("unknown metric '{s}'")),
                };
                (kind, st)
            }
            _ => (Kind::Corrected, s),
        };
        let stat = match stat {
            "min" => Stat::Min,
            "mean" => Stat::Mean,
            "p50" | "median" => Stat::P50,
            "p90" => Stat::P90,
            "p99" => Stat::P99,
            "p99.9" | "p999" => Stat::P999,
            "p99.99" | "p9999" => Stat::P9999,
            "max" => Stat::Max,
            _ => return Err(format!("unknown metric '{s}'")),
        };
        Ok(Metric::Latency(kind, stat))
    }

    fn value(self, p: &PhaseSummary) -> f64 {
        let ratio = |n: u64| {
            if p.scheduled > 0 {
                n as f64 / p.scheduled as f64
            } else {
                0.0
            }
        };
        match self {
            Metric::Latency(k, st) => {
                let l: &LatencyStats = match k {
                    Kind::Corrected => &p.latency.corrected,
                    Kind::Server => &p.latency.server,
                    Kind::ConnectionWait => &p.latency.connection_wait,
                    Kind::GeneratorLag => &p.latency.generator_lag,
                };
                match st {
                    Stat::Min => l.min_ms,
                    Stat::Mean => l.mean_ms,
                    Stat::P50 => l.p50_ms,
                    Stat::P90 => l.p90_ms,
                    Stat::P99 => l.p99_ms,
                    Stat::P999 => l.p99_9_ms,
                    Stat::P9999 => l.p99_99_ms,
                    Stat::Max => l.max_ms,
                }
            }
            Metric::OfferedRate => p.offered_rate,
            Metric::NominalRate => p.nominal_rate,
            Metric::AchievedRate => p.achieved_rate_by_intended,
            Metric::AchievedRateByCompletion => p.achieved_rate_by_completion,
            Metric::SuccessRatio => p.success_ratio,
            Metric::ErrorRate => 1.0 - p.success_ratio,
            Metric::FailedRate => ratio(p.failed.values().sum()),
            Metric::NotSentRate => ratio(p.not_sent.values().sum()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Rhs {
    Const(f64),
    Scaled(f64, Metric),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Threshold {
    metric: Metric,
    op: Op,
    rhs: Rhs,
}

fn parse_number(s: &str, unit: Unit) -> Result<f64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == 'e' || c == '+'))
        .unwrap_or(s.len());
    let (num, suffix) = s.split_at(split);
    let v: f64 = num.parse().map_err(|_| format!("'{s}' is not a number"))?;
    match (unit, suffix.trim()) {
        (Unit::Latency, "ns") => Ok(v / 1e6),
        (Unit::Latency, "us") => Ok(v / 1e3),
        (Unit::Latency, "ms") => Ok(v),
        (Unit::Latency, "s") => Ok(v * 1e3),
        (Unit::Latency, "") => Err(format!("'{s}': a latency needs a unit (ns, us, ms, s)")),
        (Unit::Ratio, "%") => Ok(v / 100.0),
        (Unit::Ratio, "") | (Unit::Rate, "") => Ok(v),
        (Unit::Rate, "/s") => Ok(v),
        (_, u) => Err(format!("'{s}': unit '{u}' does not fit this metric")),
    }
}

impl Threshold {
    pub fn parse(expr: &str) -> Result<Threshold, String> {
        let ops = [("<=", Op::Le), (">=", Op::Ge), ("<", Op::Lt), (">", Op::Gt)];
        let (pos, len, op) = ops
            .iter()
            .filter_map(|(t, op)| expr.find(t).map(|p| (p, t.len(), *op)))
            .min_by_key(|(p, len, _)| (*p, usize::MAX - len))
            .ok_or_else(|| format!("threshold '{expr}': no comparison operator (<, <=, >, >=)"))?;
        let metric = Metric::parse(&expr[..pos]).map_err(|e| format!("threshold '{expr}': {e}"))?;
        let rhs_s = expr[pos + len..].trim();
        let rhs = if let Some((k, m)) = rhs_s.split_once('*') {
            let k: f64 = k
                .trim()
                .parse()
                .map_err(|_| format!("threshold '{expr}': '{k}' is not a number"))?;
            let m = Metric::parse(m).map_err(|e| format!("threshold '{expr}': {e}"))?;
            Rhs::Scaled(k, m)
        } else if let Ok(m) = Metric::parse(rhs_s) {
            Rhs::Scaled(1.0, m)
        } else {
            Rhs::Const(
                parse_number(rhs_s, metric.unit())
                    .map_err(|e| format!("threshold '{expr}': {e}"))?,
            )
        };
        if let Rhs::Scaled(_, m) = rhs {
            if m.unit() != metric.unit() {
                return Err(format!(
                    "threshold '{expr}': the two sides have different units"
                ));
            }
        }
        Ok(Threshold { metric, op, rhs })
    }

    /// Returns (value, limit, ok).
    pub fn evaluate(&self, p: &PhaseSummary) -> (f64, f64, bool) {
        let value = self.metric.value(p);
        let limit = match self.rhs {
            Rhs::Const(c) => c,
            Rhs::Scaled(k, m) => k * m.value(p),
        };
        let ok = match self.op {
            Op::Lt => value < limit,
            Op::Le => value <= limit,
            Op::Gt => value > limit,
            Op::Ge => value >= limit,
        };
        (value, limit, ok)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ThresholdResult {
    pub expr: String,
    pub phase: String,
    pub value: f64,
    pub limit: f64,
    pub ok: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase() -> PhaseSummary {
        let mut p = PhaseSummary {
            scheduled: 1000,
            offered_rate: 100.0,
            achieved_rate_by_intended: 99.5,
            success_ratio: 0.998,
            ..Default::default()
        };
        p.latency.corrected.p99_ms = 150.0;
        p.latency.server.p99_ms = 20.0;
        p.failed.insert("timeout".into(), 2);
        p
    }

    #[test]
    fn evaluates() {
        let p = phase();
        let ok = |e: &str| Threshold::parse(e).unwrap().evaluate(&p).2;
        assert!(ok("p99 < 200ms"));
        assert!(!ok("p99 < 0.1s"));
        assert!(ok("server.p99 <= 20ms"));
        assert!(!ok("error_rate < 0.1%"));
        assert!(ok("error_rate < 0.3%"));
        assert!(ok("achieved_rate >= 0.99 * offered_rate"));
        assert!(!ok("achieved_rate >= offered_rate"));
        assert!(ok("failed_rate <= 0.002"));
    }

    #[test]
    fn rejects() {
        for e in [
            "p99 200ms",
            "p98 < 1ms",
            "p99 < 200",
            "error_rate < 5ms",
            "p99 < 2 * offered_rate",
        ] {
            assert!(Threshold::parse(e).is_err(), "{e}");
        }
    }
}
