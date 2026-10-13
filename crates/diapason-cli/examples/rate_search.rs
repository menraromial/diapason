//! Finds the highest rate at which the p99 of corrected latency stays under a
//! target, by bisection, driving diapason through its library API.
//!
//! Each step runs with `Runner::spawn`, watches the windows on the channel,
//! and stops early with `RunHandle::stop` when three windows in a row exceed
//! the target. A step counts only if the run kept to its schedule (fidelity
//! checks passed), so the answer describes the server, not the client.
//!
//!     cargo run --release -p diapason-cli --example rate_search
//!
//! By default each step starts a fresh in-process reference server (8
//! workers, exponential service of mean 1 ms, capacity 8000 req/s): a server
//! keeps serving the requests of a step that was stopped in overload, and
//! that backlog would delay the next step. Give a URL to test another server
//! (`rate_search http://host:port/ [p99_ms]`); steps then pause 10 s after a
//! failure so that the server can drain.

use diapason_core::{RunConfig, Runner};
use diapason_proto_http::HttpProtocol;

fn step(url: &str, rate: f64, target_ms: f64) -> (bool, String) {
    let cfg = RunConfig::from_toml(&format!(
        r#"seed = 1
threads = 2
[target]
url = "{url}"
[load]
arrival = "poisson"
[[load.phases]]
name = "warmup"
duration = "2s"
rate = {rate}
[[load.phases]]
name = "measure"
duration = "6s"
rate = {rate}
[connections]
count = 64
max = 2048
[timeouts]
drain = "2s"
"#
    ))
    .expect("valid configuration");
    let proto = HttpProtocol::from_config(&cfg).expect("valid target");
    let (runner, windows) = Runner::new(cfg, proto).window_channel();
    let handle = runner.spawn();
    let mut over = 0;
    for w in windows {
        let h = &w.hists.corrected;
        let p99 = if h.is_empty() {
            0.0
        } else {
            h.value_at_quantile(0.99) as f64 / 1e6
        };
        over = if p99 > target_ms { over + 1 } else { 0 };
        if over == 3 {
            handle.stop();
            return (
                false,
                format!("stopped at {:.0} s: p99 {p99:.2} ms in 3 windows", w.end_s),
            );
        }
    }
    let s = handle.join().expect("run");
    let m = s.measured_phase().expect("measure phase");
    let p99 = m.latency.corrected.p99_ms;
    let ok = s.fidelity.ok && p99 <= target_ms && m.success == m.scheduled;
    let why = if !s.fidelity.ok {
        format!("client fidelity violated: {}", s.exit_reason)
    } else {
        format!(
            "p99 {p99:.2} ms, {:.0} req/s achieved",
            m.achieved_rate_by_completion
        )
    };
    (ok, why)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let target_ms: f64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10.0);
    let given = args.get(1).cloned();
    let (mut lo, mut hi) = (100.0f64, 20_000.0f64);
    println!(
        "target: p99 of corrected latency <= {target_ms} ms on {}",
        given
            .as_deref()
            .unwrap_or("a reference server (capacity 8000 req/s)")
    );
    while hi / lo > 1.05 {
        let rate = (lo * hi).sqrt().round();
        let (ok, why) = match &given {
            Some(url) => {
                let r = step(url, rate, target_ms);
                if !r.0 {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                }
                r
            }
            None => {
                let server = diapason_server::spawn(diapason_server::ServerConfig {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    workers: 8,
                    service: diapason_server::Service::Exp(0.001),
                    threads: 4,
                    ..Default::default()
                })
                .expect("reference server");
                step(&format!("http://{}/", server.addr), rate, target_ms)
            }
        };
        println!(
            "{rate:>8.0} req/s: {} ({why})",
            if ok { "ok  " } else { "fail" }
        );
        if ok {
            lo = rate;
        } else {
            hi = rate;
        }
    }
    println!("highest rate meeting the target: about {lo:.0} req/s");
}
