//! Fidelity checks: was the load that reached the target the load that was
//! configured? See the README for each check.

use crate::config::{FidelityConfig, Mode};
use crate::hist::DelayHists;
use crate::protocol::NotSentReason;
use crate::stats::PhaseCounters;
use crate::summary::{
    Admission, BurstinessSummary, Check, Fidelity, LagSummary, SendRate, SlicePair, WorkerSummary,
};
use crate::window::Burstiness;
use crate::worker::ConnStats;

pub struct FidelityInputs<'a> {
    pub mode: Mode,
    pub cfg: &'a FidelityConfig,
    /// Counters of the measurement interval, all workers merged.
    pub measured: &'a PhaseCounters,
    /// The same, per worker.
    pub per_worker: &'a [PhaseCounters],
    pub hists: &'a DelayHists,
    pub burstiness: &'a Burstiness,
    pub conn: &'a ConnStats,
    pub workers: &'a [WorkerSummary],
}

fn check(
    name: impl Into<String>,
    ok: bool,
    value: f64,
    limit: f64,
    detail: impl Into<String>,
) -> Check {
    Check {
        name: name.into(),
        ok,
        skipped: false,
        value: Some(value),
        limit: Some(limit),
        detail: detail.into(),
    }
}

fn skipped(name: impl Into<String>, detail: impl Into<String>) -> Check {
    Check {
        name: name.into(),
        ok: true,
        skipped: true,
        value: None,
        limit: None,
        detail: detail.into(),
    }
}

pub fn evaluate(inp: &FidelityInputs) -> Fidelity {
    let f = inp.cfg;
    let open = inp.mode == Mode::Open;
    let m = inp.measured;
    let mut checks = Vec::new();

    let lag = &inp.hists.gen_lag;
    let lag_summary = if lag.is_empty() {
        LagSummary::default()
    } else {
        LagSummary {
            p99_ms: lag.value_at_quantile(0.99) as f64 / 1e6,
            max_ms: lag.max() as f64 / 1e6,
        }
    };
    if open && !lag.is_empty() {
        let lim = f.lag_p99.as_secs_f64() * 1e3;
        checks.push(check(
            "generator_lag_p99",
            lag_summary.p99_ms <= lim,
            lag_summary.p99_ms,
            lim,
            "p99 of attempt - intended, in ms",
        ));
        let lim = f.lag_max.as_secs_f64() * 1e3;
        checks.push(check(
            "generator_lag_max",
            lag_summary.max_ms <= lim,
            lag_summary.max_ms,
            lim,
            "max of attempt - intended, in ms",
        ));
    } else {
        let why = if open {
            "no request was attempted"
        } else {
            "closed-loop mode has no schedule"
        };
        checks.push(skipped("generator_lag_p99", why));
        checks.push(skipped("generator_lag_max", why));
    }

    let ratio = |sent: u64, sched: u64| {
        if sched > 0 {
            Some(sent as f64 / sched as f64)
        } else {
            None
        }
    };
    let send_rate = SendRate {
        scheduled: m.scheduled,
        sent: m.sent_in_interval,
        ratio: ratio(m.sent_in_interval, m.scheduled),
    };
    match (open, send_rate.ratio) {
        (true, Some(r)) => checks.push(check(
            "send_rate",
            r >= f.send_ratio_min,
            r,
            f.send_ratio_min,
            "requests sent in the interval / requests scheduled in it",
        )),
        (false, _) => checks.push(skipped("send_rate", "closed-loop mode has no schedule")),
        (true, None) => checks.push(skipped("send_rate", "nothing was scheduled")),
    }
    if open {
        for (w, pc) in inp.per_worker.iter().enumerate() {
            if pc.scheduled >= 100 {
                let r = pc.sent_in_interval as f64 / pc.scheduled as f64;
                checks.push(check(
                    format!("send_rate_worker_{w}"),
                    r >= f.send_ratio_min,
                    r,
                    f.send_ratio_min,
                    "same ratio for one worker",
                ));
            }
        }
    }

    let b = inp.burstiness;
    for (name, sched, send, factor) in [
        (
            "burstiness_10ms",
            &b.sched_10,
            &b.send_10,
            f.burst_factor_10ms,
        ),
        (
            "burstiness_50ms",
            &b.sched_50,
            &b.send_50,
            f.burst_factor_50ms,
        ),
    ] {
        if !open {
            checks.push(skipped(name, "closed-loop mode has no schedule"));
        } else if sched.slices == 0 || sched.mean == 0.0 {
            checks.push(skipped(name, "no complete slice with scheduled requests"));
        } else {
            let lim = factor * sched.peak_to_mean;
            let ok =
                send.peak_to_mean <= lim && send.empty_share <= sched.empty_share + f.empty_slack;
            checks.push(check(
                name,
                ok,
                send.peak_to_mean,
                lim,
                format!(
                    "sends peak/mean {:.3} (schedule {:.3}), empty slices {:.4} (schedule {:.4}, slack {})",
                    send.peak_to_mean, sched.peak_to_mean, send.empty_share, sched.empty_share, f.empty_slack
                ),
            ));
        }
    }

    let c = inp.conn;
    checks.push(check(
        "connections",
        c.established == c.requested,
        c.established as f64,
        c.requested as f64,
        "connections established before T0 / requested",
    ));

    let over = m.conn_wait_over
        + m.not_sent[NotSentReason::Queue.index()]
        + m.not_sent[NotSentReason::QueueFull.index()];
    let share = if m.scheduled > 0 {
        over as f64 / m.scheduled as f64
    } else {
        0.0
    };
    let admission_bound = open && m.scheduled > 0 && share > f.admission_share_max;
    if open && m.scheduled > 0 {
        checks.push(check(
            "admission",
            !admission_bound,
            share,
            f.admission_share_max,
            format!(
                "share of requests that waited more than {} for a connection or slot, or were dropped from the queue",
                f.conn_wait_threshold
            ),
        ));
    } else {
        checks.push(skipped(
            "admission",
            if open {
                "nothing was scheduled"
            } else {
                "closed-loop mode"
            },
        ));
    }

    for w in inp.workers {
        checks.push(check(
            format!("worker_busy_{}", w.id),
            w.busy_share <= f.busy_share_max,
            w.busy_share,
            f.busy_share_max,
            "share of wall time the worker was running, timer spin excluded",
        ));
    }
    for w in inp.workers {
        match w.runqueue_share {
            Some(share) => checks.push(check(
                format!("worker_runqueue_{}", w.id),
                share <= f.runqueue_share_max,
                share,
                f.runqueue_share_max,
                "share of wall time the worker was runnable but not running",
            )),
            None => checks.push(skipped(
                format!("worker_runqueue_{}", w.id),
                "not reported by this kernel",
            )),
        }
    }

    let ok = checks.iter().all(|c| c.ok);
    Fidelity {
        ok,
        admission_bound,
        checks,
        generator_lag: lag_summary,
        send_rate,
        burstiness: BurstinessSummary {
            slice_10ms: SlicePair {
                schedule: b.sched_10.clone(),
                sends: b.send_10.clone(),
            },
            slice_50ms: SlicePair {
                schedule: b.sched_50.clone(),
                sends: b.send_50.clone(),
            },
        },
        admission: Admission {
            over_threshold: over,
            share,
        },
    }
}
