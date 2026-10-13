//! The text summary printed at the end of a run. For humans only; use the
//! JSON summary for anything automatic.

use crate::summary::{PhaseSummary, Summary};
use std::fmt::Write;

fn map(m: &std::collections::BTreeMap<String, u64>) -> String {
    if m.is_empty() {
        return "none".into();
    }
    m.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn phase(out: &mut String, p: &PhaseSummary) {
    let _ = writeln!(
        out,
        "phase {}{} [{:.1}s, {:.1}s): scheduled {} ({:.1}/s, nominal {:.1}/s), completed {}, success {}",
        p.name,
        if p.measured { " (measured)" } else { "" },
        p.start_s,
        p.end_s,
        p.scheduled,
        p.offered_rate,
        p.nominal_rate,
        p.completed,
        p.success
    );
    let _ = writeln!(
        out,
        "  achieved {:.1}/s by intended time, {:.1}/s by completion time; failed: {}; not sent: {}; in flight at end: {}",
        p.achieved_rate_by_intended,
        p.achieved_rate_by_completion,
        map(&p.failed),
        map(&p.not_sent),
        p.in_flight_at_end
    );
    let _ = writeln!(out, "  status: {}", map(&p.status));
    let _ = writeln!(
        out,
        "  {:<17} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "ms", "p50", "p90", "p99", "p99.9", "max", "mean"
    );
    for (name, l) in [
        ("corrected", &p.latency.corrected),
        ("server", &p.latency.server),
        ("connection wait", &p.latency.connection_wait),
        ("generator lag", &p.latency.generator_lag),
    ] {
        let _ = writeln!(
            out,
            "  {:<17} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3}",
            name, l.p50_ms, l.p90_ms, l.p99_ms, l.p99_9_ms, l.max_ms, l.mean_ms
        );
    }
    let rel = p
        .little
        .relative_error
        .map(|e| format!("{:+.1} %", e * 100.0))
        .unwrap_or_else(|| "n/a".into());
    let _ = writeln!(
        out,
        "  in flight: mean {:.1} (Little's law predicts {:.1}, {rel}), in system: mean {:.1}",
        p.in_flight.sent_mean, p.little.predicted_in_flight, p.in_flight.system_mean
    );
    if p.scenarios.len() > 1 {
        for sc in &p.scenarios {
            let _ = writeln!(
                out,
                "  scenario {:<16} {:>6.2} % of requests (weight {:>6.2} %), completed {}, success {}, failed {}, not sent {}",
                sc.name,
                sc.share * 100.0,
                sc.weight_share * 100.0,
                sc.completed,
                sc.success,
                sc.failed,
                sc.not_sent
            );
        }
    }
}

pub fn text(s: &Summary) -> String {
    let mut out = String::new();
    if s.mode == "closed" {
        let _ = writeln!(
            out,
            "CLOSED-LOOP run: no schedule; latencies suffer from coordinated omission."
        );
    }
    let _ = writeln!(
        out,
        "diapason {} ({}) {} mode, seed {}, {} threads, {}",
        s.tool.version,
        s.tool.git_hash.as_deref().unwrap_or("no git hash"),
        s.mode,
        s.seed,
        s.run.threads,
        s.run.protocol
    );
    let _ = writeln!(
        out,
        "started {}, wall {:.1}s",
        s.started_at.rfc3339, s.run.wall_duration_s
    );
    let c = &s.conservation;
    let _ = writeln!(
        out,
        "requests: scheduled {} = completed {} + failed {} + not sent {} + in flight {} -> {}",
        c.scheduled,
        c.completed,
        c.failed_total,
        c.not_sent_total,
        c.in_flight_at_end,
        if c.ok {
            "balanced".to_string()
        } else {
            format!("UNBALANCED by {}", c.balance)
        }
    );
    let k = &s.connections;
    let _ = writeln!(
        out,
        "connections: {}/{} established, shortfall {}, connect errors {}, reconnects {}, closes server/client/error {}/{}/{}",
        k.established,
        k.requested,
        map(&k.shortfall),
        map(&k.connect_errors),
        k.reconnects,
        k.closes.server,
        k.closes.client,
        k.closes.error
    );
    for p in &s.phases {
        phase(&mut out, p);
    }
    let f = &s.fidelity;
    let _ = writeln!(
        out,
        "fidelity: {}{}",
        if f.ok { "ok" } else { "VIOLATED" },
        if f.admission_bound {
            " (ADMISSION-BOUND: the client's connection or in-flight limits shaped the load)"
        } else {
            ""
        }
    );
    for ch in &f.checks {
        if ch.skipped {
            continue;
        }
        if !ch.ok || !ch.name.starts_with("worker_") && !ch.name.starts_with("send_rate_worker_") {
            let _ = writeln!(
                out,
                "  {:<4} {:<22} {:>10.4} limit {:>10.4}  {}",
                if ch.ok { "ok" } else { "FAIL" },
                ch.name,
                ch.value.unwrap_or(f64::NAN),
                ch.limit.unwrap_or(f64::NAN),
                ch.detail
            );
        }
    }
    if !s.thresholds.is_empty() {
        let _ = writeln!(out, "thresholds:");
        for t in &s.thresholds {
            let _ = writeln!(
                out,
                "  {:<4} {} on {}: value {:.4}, limit {:.4}",
                if t.ok { "ok" } else { "FAIL" },
                t.expr,
                t.phase,
                t.value,
                t.limit
            );
        }
    }
    let busiest = s
        .workers
        .iter()
        .map(|w| w.busy_share)
        .fold(0.0f64, f64::max);
    let _ = writeln!(out, "  busiest worker: {:.1} % busy", busiest * 100.0);
    for w in &s.warnings {
        let _ = writeln!(out, "warning: {w}");
    }
    let _ = writeln!(out, "exit {}: {}", s.exit_code, s.exit_reason);
    out
}
