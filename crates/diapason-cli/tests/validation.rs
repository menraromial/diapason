//! Validation tests 1 to 9 of the README, run against the reference server.
//!
//! They start `diapason server` and `diapason run` as separate processes and
//! read the JSON summary and the trace. They run one at a time, since they
//! measure timing on a shared machine. Run them with
//! `cargo test --release -p diapason-cli`.

use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

const BIN: &str = env!("CARGO_BIN_EXE_diapason");

fn tmp(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("validation");
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

struct Server {
    child: Child,
    url: String,
}

impl Server {
    fn start(args: &[&str]) -> Server {
        let mut child = Command::new(BIN)
            .arg("server")
            .args(["--listen", "127.0.0.1:0"])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start server");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let addr = line
            .strip_prefix("listening ")
            .and_then(|r| r.split_whitespace().next())
            .unwrap_or_else(|| panic!("unexpected server output: {line}"))
            .to_string();
        Server {
            child,
            url: format!("http://{addr}/"),
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

struct Outcome {
    code: i32,
    summary: Value,
    stderr: String,
}

fn run_cmd(mut cmd: Command, name: &str) -> Outcome {
    let summary_path = tmp(&format!("{name}.json"));
    let _ = std::fs::remove_file(&summary_path);
    let out = cmd
        .arg("--summary")
        .arg(&summary_path)
        .arg("--quiet")
        .output()
        .expect("run diapason");
    let summary = std::fs::read_to_string(&summary_path)
        .map(|s| serde_json::from_str(&s).expect("summary is JSON"))
        .unwrap_or(Value::Null);
    Outcome {
        code: out.status.code().unwrap_or(-1),
        summary,
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn run(name: &str, args: &[&str]) -> Outcome {
    let mut cmd = Command::new(BIN);
    cmd.arg("run").args(args);
    run_cmd(cmd, name)
}

fn measured(s: &Value) -> &Value {
    s["phases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["measured"].as_bool() == Some(true))
        .expect("a measured phase")
}

fn f(v: &Value) -> f64 {
    v.as_f64().unwrap_or_else(|| panic!("not a number: {v}"))
}

fn u(v: &Value) -> u64 {
    v.as_u64().unwrap_or_else(|| panic!("not an integer: {v}"))
}

fn failed_checks(s: &Value) -> Vec<String> {
    s["fidelity"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["ok"].as_bool() == Some(false))
        .map(|c| format!("{} = {} (limit {})", c["name"], c["value"], c["limit"]))
        .collect()
}

fn assert_conserved(s: &Value) {
    let c = &s["conservation"];
    assert_eq!(c["ok"].as_bool(), Some(true), "conservation: {c}");
    let sum: u64 = u(&c["completed"])
        + u(&c["failed_total"])
        + u(&c["not_sent_total"])
        + u(&c["in_flight_at_end"]);
    assert_eq!(u(&c["scheduled"]), sum, "conservation: {c}");
}

/// Rows of a zstd CSV trace, as maps from column name to field.
fn trace_rows(path: &PathBuf) -> Vec<std::collections::HashMap<String, String>> {
    let raw = std::fs::read(path).unwrap();
    let text = String::from_utf8(zstd::decode_all(&raw[..]).unwrap()).unwrap();
    let mut lines = text.lines();
    let header: Vec<String> = lines.next().unwrap().split(',').map(String::from).collect();
    lines
        .map(|l| {
            header
                .iter()
                .cloned()
                .zip(l.split(',').map(String::from))
                .collect()
        })
        .collect()
}

/// Test 1: Below capacity: achieved rate equals offered rate within 0.5 %, and
/// corrected latency is close to server time.
#[test]
fn t1_below_capacity() {
    let _g = serial();
    let srv = Server::start(&[
        "--workers",
        "50",
        "--service",
        "const:1ms",
        "--threads",
        "4",
    ]);
    let o = run(
        "t1",
        &[
            "--url",
            &srv.url,
            "--rate",
            "5000",
            "--arrival",
            "constant",
            "--warmup",
            "1s",
            "--duration",
            "5s",
            "--threads",
            "2",
            "--connections",
            "100",
            "--seed",
            "1",
        ],
    );
    assert_eq!(
        o.code,
        0,
        "exit {}: {:?} {}",
        o.code,
        failed_checks(&o.summary),
        o.stderr
    );
    let s = &o.summary;
    assert_conserved(s);
    let m = measured(s);
    let offered = f(&m["offered_rate"]);
    let achieved = f(&m["achieved_rate_by_intended"]);
    assert!(
        (achieved / offered - 1.0).abs() < 0.005,
        "achieved {achieved} offered {offered}"
    );
    assert!((offered / 5000.0 - 1.0).abs() < 0.005, "offered {offered}");
    let by_completion = f(&m["achieved_rate_by_completion"]);
    assert!(
        (by_completion / offered - 1.0).abs() < 0.005,
        "by completion {by_completion}"
    );
    let l = &m["latency"];
    for q in ["p50_ms", "p99_ms"] {
        let c = f(&l["corrected"][q]);
        let sv = f(&l["server"][q]);
        assert!(c - sv < 0.5, "{q}: corrected {c} ms vs server {sv} ms");
    }
}

/// Test 2: Above capacity (r = 1.1 μ) with unbounded admission: corrected latency
/// grows with slope r/μ - 1 within 10 %, and throughput is close to μ.
#[test]
fn t2_above_capacity_slope() {
    let _g = serial();
    // μ = 2 / 2 ms = 1000 req/s.
    let srv = Server::start(&["--workers", "2", "--service", "const:2ms", "--threads", "4"]);
    let trace = tmp("t2.csv.zst");
    let o = run(
        "t2",
        &[
            "--url",
            &srv.url,
            "--rate",
            "1100",
            "--arrival",
            "constant",
            "--duration",
            "20s",
            "--threads",
            "4",
            "--connections",
            "4000",
            "--seed",
            "2",
            "--trace",
            trace.to_str().unwrap(),
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    assert_eq!(
        o.code,
        0,
        "exit {}: {:?} {}",
        o.code,
        failed_checks(s),
        o.stderr
    );
    let m = measured(s);
    let thr = f(&m["achieved_rate_by_completion"]);
    assert!((thr / 1000.0 - 1.0).abs() < 0.02, "throughput {thr}");
    // Least-squares slope of corrected latency against intended time.
    let rows = trace_rows(&trace);
    let pts: Vec<(f64, f64)> = rows
        .iter()
        .filter(|r| r["error_kind"].is_empty())
        .map(|r| {
            let i: f64 = r["intended_ns"].parse().unwrap();
            let d: f64 = r["done_ns"].parse().unwrap();
            (i / 1e9, (d - i) / 1e9)
        })
        .collect();
    assert!(pts.len() > 20_000, "{} completed rows", pts.len());
    let n = pts.len() as f64;
    let (mx, my) = (
        pts.iter().map(|p| p.0).sum::<f64>() / n,
        pts.iter().map(|p| p.1).sum::<f64>() / n,
    );
    let cov: f64 = pts.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    let var: f64 = pts.iter().map(|p| (p.0 - mx) * (p.0 - mx)).sum();
    let slope = cov / var;
    assert!(
        (slope / 0.1 - 1.0).abs() < 0.1,
        "slope {slope}, expected 0.1"
    );
}

/// Test 3: Bounded admission with few connections: connection wait grows, the run
/// is flagged admission-bound, and corrected latency includes the wait.
#[test]
fn t3_admission_bound() {
    let _g = serial();
    let srv = Server::start(&["--workers", "2", "--service", "const:2ms", "--threads", "2"]);
    let o = run(
        "t3",
        &[
            "--url",
            &srv.url,
            "--rate",
            "800",
            "--arrival",
            "constant",
            "--duration",
            "4s",
            "--threads",
            "1",
            "--connections",
            "1",
            "--seed",
            "3",
            "--drain",
            "20s",
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    assert_eq!(o.code, 3, "exit {} {}", o.code, o.stderr);
    assert_eq!(s["fidelity"]["admission_bound"].as_bool(), Some(true));
    let m = measured(s);
    assert_eq!(
        u(&m["completed"]),
        u(&m["scheduled"]),
        "all requests complete during the drain"
    );
    let l = &m["latency"];
    let wait = f(&l["connection_wait"]["mean_ms"]);
    let server = f(&l["server"]["mean_ms"]);
    let lag = f(&l["generator_lag"]["mean_ms"]);
    let corrected = f(&l["corrected"]["mean_ms"]);
    assert!(wait > 100.0, "connection wait {wait} ms");
    assert!(
        ((lag + wait + server) / corrected - 1.0).abs() < 0.01,
        "corrected {corrected} vs lag {lag} + wait {wait} + server {server}"
    );
}

/// Test 4: A file-descriptor limit below the requested connections: diapason
/// refuses to start.
#[test]
fn t4_fd_limit() {
    let _g = serial();
    let mut cmd = Command::new(BIN);
    cmd.args([
        "run",
        "--url",
        "http://127.0.0.1:9/",
        "--rate",
        "10",
        "--duration",
        "1s",
        "--connections",
        "1000",
        "--threads",
        "1",
    ]);
    // SAFETY: setrlimit is async-signal-safe.
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| {
            let r = libc::rlimit {
                rlim_cur: 256,
                rlim_max: 256,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &r) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let o = run_cmd(cmd, "t4");
    assert_eq!(o.code, 2, "exit {} {}", o.code, o.stderr);
    assert!(o.stderr.contains("file-descriptor limit"), "{}", o.stderr);
}

/// Test 5: Burstiness: 64 threads x 4032 connections at 220 req/s give a 50 ms
/// peak/mean below 2 and no empty slice.
#[test]
fn t5_burstiness() {
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--threads", "4"]);
    let o = run(
        "t5",
        &[
            "--url",
            &srv.url,
            "--rate",
            "220",
            "--arrival",
            "constant",
            "--warmup",
            "1s",
            "--duration",
            "5s",
            "--threads",
            "64",
            "--connections",
            "4032",
            "--seed",
            "5",
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    assert_eq!(u(&s["connections"]["established"]), 4032);
    let b = &s["fidelity"]["burstiness"]["slice_50ms"]["sends"];
    assert!(f(&b["peak_to_mean"]) < 2.0, "50 ms slices: {b}");
    assert_eq!(f(&b["empty_share"]), 0.0, "50 ms slices: {b}");
    assert_eq!(o.code, 0, "exit {}: {:?}", o.code, failed_checks(s));
}

/// Test 6a: Conservation holds with resets, server closes and stalled requests.
#[test]
fn t6a_conservation_under_errors() {
    let _g = serial();
    let srv = Server::start(&[
        "--workers",
        "4",
        "--service",
        "const:1ms",
        "--threads",
        "2",
        "--reset-prob",
        "0.02",
        "--close-prob",
        "0.02",
        "--stall-prob",
        "0.01",
    ]);
    let o = run(
        "t6a",
        &[
            "--url",
            &srv.url,
            "--rate",
            "1000",
            "--duration",
            "4s",
            "--threads",
            "2",
            "--connections",
            "50",
            "--timeout",
            "300ms",
            "--seed",
            "6",
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    assert_ne!(o.code, 4);
    let failed = &s["conservation"]["failed"];
    assert!(u(&failed["timeout"]) > 0, "{failed}");
    assert!(
        failed.get("reset").is_some() || failed.get("read").is_some(),
        "{failed}"
    );
    assert!(u(&s["connections"]["closes"]["server"]) > 0);
    assert!(u(&s["connections"]["reconnects"]) > 0);
}

/// Test 6b: Conservation holds when the server is killed in the middle of the run.
#[test]
fn t6b_server_killed() {
    let _g = serial();
    let mut srv = Server::start(&["--workers", "4", "--service", "const:1ms", "--threads", "2"]);
    let summary_path = tmp("t6b.json");
    let _ = std::fs::remove_file(&summary_path);
    let mut client = Command::new(BIN)
        .args([
            "run",
            "--url",
            &srv.url,
            "--rate",
            "500",
            "--duration",
            "5s",
            "--threads",
            "2",
            "--connections",
            "20",
            "--max-queue-delay",
            "1s",
            "--drain",
            "2s",
            "--seed",
            "6",
            "--quiet",
            "--summary",
        ])
        .arg(&summary_path)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(2500));
    srv.kill();
    let status = client.wait().unwrap();
    let s: Value = serde_json::from_str(&std::fs::read_to_string(&summary_path).unwrap()).unwrap();
    assert_conserved(&s);
    assert_ne!(status.code(), Some(4));
    assert!(
        u(&s["conservation"]["not_sent"]["queue"]) > 0,
        "{}",
        s["conservation"]
    );
    assert!(
        u(&s["connections"]["connect_errors"]["connect_refused"]) > 0,
        "{}",
        s["connections"]
    );
}

/// Test 7: A timeout is counted once per request.
#[test]
fn t7_timeout_once() {
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--stall-prob", "1", "--threads", "1"]);
    let o = run(
        "t7",
        &[
            "--url",
            &srv.url,
            "--rate",
            "20",
            "--arrival",
            "constant",
            "--duration",
            "2s",
            "--threads",
            "1",
            "--connections",
            "50",
            "--timeout",
            "300ms",
            "--seed",
            "7",
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    let c = &s["conservation"];
    assert_eq!(u(&c["scheduled"]), 40);
    assert_eq!(u(&c["failed"]["timeout"]), 40, "{c}");
    assert_eq!(u(&c["failed_total"]), 40, "{c}");
    assert_eq!(u(&s["connections"]["closes"]["client"]), 40);
}

/// Test 8: A CPU-starved client (pinned to one core at a high rate) fails its
/// fidelity checks.
#[test]
fn t8_cpu_starved() {
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--threads", "4"]);
    let mut cmd = Command::new(BIN);
    cmd.args([
        "run",
        "--url",
        &srv.url,
        "--rate",
        "150000",
        "--duration",
        "3s",
        "--threads",
        "8",
        "--connections",
        "256",
        "--seed",
        "8",
    ]);
    // SAFETY: sched_setaffinity is async-signal-safe.
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(0, &mut set);
            if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let o = run_cmd(cmd, "t8");
    let s = &o.summary;
    assert_conserved(s);
    assert_eq!(o.code, 3, "exit {} {}", o.code, o.stderr);
    let failed = failed_checks(s);
    assert!(
        failed
            .iter()
            .any(|c| c.contains("generator_lag") || c.contains("runqueue") || c.contains("busy")),
        "{failed:?}"
    );
}

/// Test 9: Same seed, same configuration: identical request sequence.
#[test]
fn t9_reproducible() {
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--threads", "2"]);
    let seq = |name: &str, seed: &str| -> Vec<(u64, u64)> {
        let trace = tmp(&format!("{name}.csv.zst"));
        let o = run(
            name,
            &[
                "--url",
                &srv.url,
                "--rate",
                "2000",
                "--arrival",
                "poisson",
                "--duration",
                "2s",
                "--threads",
                "3",
                "--connections",
                "30",
                "--seed",
                seed,
                "--trace",
                trace.to_str().unwrap(),
            ],
        );
        assert_conserved(&o.summary);
        let mut v: Vec<(u64, u64)> = trace_rows(&trace)
            .iter()
            .map(|r| (r["seq"].parse().unwrap(), r["intended_ns"].parse().unwrap()))
            .collect();
        v.sort_unstable();
        assert_eq!(v.len() as u64, u(&o.summary["conservation"]["scheduled"]));
        v
    };
    let a = seq("t9a", "42");
    let b = seq("t9b", "42");
    let c = seq("t9c", "43");
    assert!(a.len() > 3000);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

/// The interval log is readable by the HdrHistogram library, and the window
/// file and the trace agree with the summary.
#[test]
fn outputs_are_readable() {
    use hdrhistogram::serialization::interval_log::{IntervalLogIterator, LogEntry};
    use hdrhistogram::serialization::Deserializer;
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--threads", "2"]);
    let (hlog, windows, trace) = (tmp("out.hlog"), tmp("out.jsonl"), tmp("out.csv"));
    let o = run(
        "outputs",
        &[
            "--url",
            &srv.url,
            "--rate",
            "1000",
            "--duration",
            "2s",
            "--threads",
            "2",
            "--connections",
            "20",
            "--seed",
            "10",
            "--window",
            "500ms",
            "--histogram-log",
            hlog.to_str().unwrap(),
            "--windows",
            windows.to_str().unwrap(),
            "--trace",
            trace.to_str().unwrap(),
        ],
    );
    assert_eq!(o.code, 0, "{:?}", failed_checks(&o.summary));
    let scheduled = u(&o.summary["conservation"]["scheduled"]);

    let raw = std::fs::read(&hlog).unwrap();
    let mut de = Deserializer::new();
    let mut corrected = 0u64;
    let mut tags = std::collections::BTreeSet::new();
    for entry in IntervalLogIterator::new(&raw) {
        if let LogEntry::Interval(h) = entry.expect("valid log line") {
            let decoded = base64_decode(h.encoded_histogram());
            let hist: hdrhistogram::Histogram<u64> =
                de.deserialize(&mut &decoded[..]).expect("valid histogram");
            let tag = h.tag().map(|t| t.as_str().to_string()).unwrap_or_default();
            if tag == "corrected" {
                corrected += hist.len();
            }
            tags.insert(tag);
        }
    }
    assert_eq!(corrected, u(&o.summary["conservation"]["completed"]));
    assert_eq!(tags.len(), 4, "{tags:?}");

    let lines = std::fs::read_to_string(&windows).unwrap();
    let sched: u64 = lines
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["scheduled"]
                .as_u64()
                .unwrap()
        })
        .sum();
    assert_eq!(sched, scheduled);

    let text = std::fs::read_to_string(&trace).unwrap();
    assert_eq!(text.lines().count() as u64, scheduled + 1);
}

fn base64_decode(s: &str) -> Vec<u8> {
    let val = |c: u8| -> u32 {
        match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a' + 26) as u32,
            b'0'..=b'9' => (c - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("bad base64"),
        }
    };
    let bytes: Vec<u8> = s.bytes().filter(|&c| c != b'=').collect();
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= val(c) << (18 - 6 * i);
        }
        for i in 0..chunk.len() - 1 {
            out.push((acc >> (16 - 8 * i)) as u8);
        }
    }
    out
}

/// User thresholds: a failed one gives exit code 5, passing ones exit 0.
#[test]
fn m2_thresholds() {
    let _g = serial();
    let srv = Server::start(&["--workers", "4", "--service", "const:1ms", "--threads", "2"]);
    let common = [
        "--url",
        &srv.url,
        "--rate",
        "500",
        "--duration",
        "2s",
        "--threads",
        "2",
        "--connections",
        "20",
        "--seed",
        "11",
    ];
    let mut args: Vec<&str> = common.to_vec();
    args.extend([
        "--threshold",
        "p99 < 0.5ms",
        "--threshold",
        "error_rate < 1%",
    ]);
    let o = run("m2_thr_fail", &args);
    assert_eq!(o.code, 5, "exit {} {}", o.code, o.stderr);
    let t = o.summary["thresholds"].as_array().unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t[0]["ok"].as_bool(), Some(false));
    assert_eq!(t[0]["phase"].as_str(), Some("measure"));
    assert_eq!(t[1]["ok"].as_bool(), Some(true));
    let mut args: Vec<&str> = common.to_vec();
    args.extend([
        "--threshold",
        "p99 < 1s",
        "--threshold",
        "achieved_rate >= 0.99 * offered_rate",
        "--threshold",
        "success_ratio >= 100%",
    ]);
    let o = run("m2_thr_ok", &args);
    assert_eq!(o.code, 0, "exit {}: {}", o.code, o.summary["thresholds"]);
    // A bad expression is a configuration error.
    let o = run(
        "m2_thr_bad",
        &[
            "--url",
            &srv.url,
            "--rate",
            "1",
            "--duration",
            "1s",
            "--threshold",
            "p98 < 1ms",
        ],
    );
    assert_eq!(o.code, 2);
}

/// Weighted scenarios with templated paths: the realised mix follows the
/// weights, statuses follow the rendered paths, and the trace carries the
/// scenario.
#[test]
fn m2_scenarios() {
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--threads", "2"]);
    let cfg = tmp("m2_scen.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"seed = 12
threads = 2
[target]
url = "{}"
[load]
arrival = "constant"
[[load.phases]]
name = "measure"
duration = "4s"
rate = 2000
[connections]
count = 20
[vars]
code = {{ dist = "choice", values = [404, 503] }}
id = {{ dist = "zipf", n = 1000, s = 1.1 }}
[[scenarios]]
name = "ok"
weight = 3
path = "/item/{{{{id}}}}"
headers = {{ "x-seq" = "{{{{seq}}}}" }}
[[scenarios]]
name = "err"
weight = 1
method = "POST"
path = "/status/{{{{code}}}}"
body = "id={{{{id}}}}"
"#,
            srv.url
        ),
    )
    .unwrap();
    let trace = tmp("m2_scen.csv");
    let o = run(
        "m2_scen",
        &[
            "--config",
            cfg.to_str().unwrap(),
            "--trace",
            trace.to_str().unwrap(),
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    let m = measured(s);
    let sc = m["scenarios"].as_array().unwrap();
    assert_eq!(sc.len(), 2);
    let share = f(&sc[0]["share"]);
    assert!((share - 0.75).abs() < 0.02, "share {share}");
    assert_eq!(f(&sc[0]["weight_share"]), 0.75);
    let err_completed = u(&sc[1]["completed"]);
    let st = &m["status"];
    assert_eq!(u(&st["404"]) + u(&st["503"]), err_completed, "{st}");
    assert!(
        u(&st["404"]) > err_completed / 3 && u(&st["503"]) > err_completed / 3,
        "{st}"
    );
    assert_eq!(u(&st["200"]), u(&sc[0]["completed"]));
    assert_eq!(u(&sc[1]["success"]), 0);
    let rows = std::fs::read_to_string(&trace).unwrap();
    let mut by_scen = [0u64; 2];
    let mut lines = rows.lines();
    let header: Vec<&str> = lines.next().unwrap().split(',').collect();
    let (si, sti) = (
        header.iter().position(|h| *h == "scenario").unwrap(),
        header.iter().position(|h| *h == "status").unwrap(),
    );
    for l in lines {
        let f: Vec<&str> = l.split(',').collect();
        let k: usize = f[si].parse().unwrap();
        by_scen[k] += 1;
        assert_eq!(k == 0, f[sti] == "200", "{l}");
    }
    assert_eq!(by_scen[0], u(&sc[0]["scheduled"]));
}

/// HTTPS with a private CA, SNI override and client certificates.
#[test]
fn m2_tls() {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair,
    };
    let _g = serial();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "diapason test CA");
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
    let leaf = |names: Vec<String>, eku: ExtendedKeyUsagePurpose| {
        let key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::new(names).unwrap();
        p.extended_key_usages = vec![eku];
        p.distinguished_name
            .push(DnType::CommonName, "diapason test");
        let cert = p.signed_by(&key, &ca).unwrap();
        (cert.pem(), key.serialize_pem())
    };
    let (srv_cert, srv_key) = leaf(
        vec!["localhost".into()],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (cli_cert, cli_key) = leaf(vec!["client".into()], ExtendedKeyUsagePurpose::ClientAuth);
    let files: Vec<(PathBuf, String)> = [
        ("ca.pem", ca.pem()),
        ("srv.pem", srv_cert),
        ("srv.key", srv_key),
        ("cli.pem", cli_cert),
        ("cli.key", cli_key),
    ]
    .into_iter()
    .map(|(n, c)| (tmp(n), c))
    .collect();
    for (p, c) in &files {
        std::fs::write(p, c).unwrap();
    }
    let p = |i: usize| files[i].0.to_str().unwrap().to_string();
    let srv = Server::start(&[
        "--workers",
        "0",
        "--threads",
        "2",
        "--tls-cert",
        &p(1),
        "--tls-key",
        &p(2),
        "--tls-client-ca",
        &p(0),
    ]);
    let url = srv.url.replace("http://", "https://");
    let (ca_f, cc, ck) = (p(0), p(3), p(4));
    let base = [
        "--url",
        &url,
        "--rate",
        "500",
        "--duration",
        "2s",
        "--threads",
        "2",
        "--connections",
        "10",
        "--seed",
        "13",
        "--set",
        "tls.server_name=localhost",
        "--ca-file",
        &ca_f,
    ];
    let mut args = base.to_vec();
    args.extend(["--client-cert", &cc, "--client-key", &ck]);
    let o = run("m2_tls", &args);
    assert_eq!(
        o.code,
        0,
        "exit {}: {:?} {}",
        o.code,
        failed_checks(&o.summary),
        o.stderr
    );
    assert_conserved(&o.summary);
    let m = measured(&o.summary);
    assert_eq!(u(&m["success"]), u(&m["scheduled"]));
    assert!(o.summary["run"]["protocol"]
        .as_str()
        .unwrap()
        .starts_with("https/1.1"));
    // The same over HTTP/2, negotiated with ALPN.
    let mut args = base.to_vec();
    args.extend(["--client-cert", &cc, "--client-key", &ck, "--http2"]);
    let o = run("m3_h2_tls", &args);
    assert_eq!(
        o.code,
        0,
        "exit {}: {:?}",
        o.code,
        failed_checks(&o.summary)
    );
    assert!(o.summary["run"]["protocol"]
        .as_str()
        .unwrap()
        .starts_with("h2 "));
    let m = measured(&o.summary);
    assert_eq!(u(&m["success"]), u(&m["scheduled"]));
    // Without a client certificate the server refuses every connection.
    let o = run("m2_tls_nocert", &base);
    assert_conserved(&o.summary);
    assert_ne!(o.code, 0);
    assert_eq!(u(&o.summary["conservation"]["completed"]), 0);
    // Without the CA, the server certificate is not trusted.
    let o = run(
        "m2_tls_noca",
        &[
            "--url",
            &url,
            "--rate",
            "10",
            "--duration",
            "1s",
            "--connections",
            "2",
            "--threads",
            "1",
        ],
    );
    assert_eq!(
        u(&o.summary["connections"]["connect_errors"]["tls"]),
        u(&o.summary["connections"]["connect_attempts"])
    );
}

/// HEAD and GET mixed on pipelined connections: every response is framed
/// according to its own request's method.
#[test]
fn m2_head_pipelined() {
    let _g = serial();
    let srv = Server::start(&[
        "--workers",
        "4",
        "--service",
        "exp:1ms",
        "--threads",
        "2",
        "--body-size",
        "100",
    ]);
    let cfg = tmp("m2_head.toml");
    std::fs::write(
        &cfg,
        format!(
            "[target]\nurl = \"{}\"\n[load]\n[[load.phases]]\nname = \"measure\"\nduration = \"2s\"\nrate = 3000\n\
             [connections]\ncount = 4\npipeline = 8\n[[scenarios]]\nname = \"get\"\n[[scenarios]]\nname = \"head\"\nmethod = \"HEAD\"\n",
            srv.url
        ),
    )
    .unwrap();
    let o = run(
        "m2_head",
        &[
            "--config",
            cfg.to_str().unwrap(),
            "--threads",
            "2",
            "--seed",
            "14",
        ],
    );
    let c = &o.summary["conservation"];
    assert_conserved(&o.summary);
    assert_eq!(u(&c["failed_total"]), 0, "{c}");
    assert_eq!(u(&c["completed"]), u(&c["scheduled"]), "{c}");
}

/// Connect-on-demand: above capacity, connections follow the backlog up to
/// the cap, and every one is reported.
#[test]
fn m3_connect_on_demand() {
    let _g = serial();
    let srv = Server::start(&["--workers", "2", "--service", "const:2ms", "--threads", "4"]);
    let windows = tmp("m3_od.jsonl");
    let o = run(
        "m3_od",
        &[
            "--url",
            &srv.url,
            "--rate",
            "1100",
            "--arrival",
            "constant",
            "--duration",
            "8s",
            "--threads",
            "4",
            "--connections",
            "10",
            "--max-connections",
            "4000",
            "--seed",
            "15",
            "--windows",
            windows.to_str().unwrap(),
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    assert_eq!(o.code, 0, "exit {}: {:?}", o.code, failed_checks(s));
    let c = &s["connections"];
    // The backlog grows by about 100 requests per second.
    assert!(u(&c["opened_on_demand"]) > 500, "{c}");
    assert!(u(&c["peak_open"]) >= u(&c["opened_on_demand"]), "{c}");
    let opens: Vec<u64> = std::fs::read_to_string(&windows)
        .unwrap()
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["connections_open_max"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert!(opens.windows(2).take(7).all(|w| w[1] > w[0]), "{opens:?}");
    // With a low cap the client becomes the bottleneck, and says so.
    let o = run(
        "m3_od_cap",
        &[
            "--url",
            &srv.url,
            "--rate",
            "1100",
            "--arrival",
            "constant",
            "--duration",
            "4s",
            "--threads",
            "2",
            "--connections",
            "2",
            "--max-connections",
            "6",
            "--seed",
            "15",
            "--drain",
            "1s",
        ],
    );
    assert_conserved(&o.summary);
    assert_eq!(o.code, 3);
    assert_eq!(
        o.summary["fidelity"]["admission_bound"].as_bool(),
        Some(true)
    );
    assert_eq!(u(&o.summary["connections"]["peak_open"]), 6);
}

/// HTTP/2 with prior knowledge: one connection carries all the streams.
#[test]
fn m3_h2c() {
    let _g = serial();
    let srv = Server::start(&["--workers", "8", "--service", "exp:1ms", "--threads", "4"]);
    let o = run(
        "m3_h2c",
        &[
            "--url",
            &srv.url,
            "--http2",
            "--rate",
            "3000",
            "--duration",
            "3s",
            "--threads",
            "1",
            "--connections",
            "1",
            "--seed",
            "16",
        ],
    );
    let s = &o.summary;
    assert_eq!(o.code, 0, "exit {}: {:?}", o.code, failed_checks(s));
    assert!(s["run"]["protocol"].as_str().unwrap().starts_with("h2c "));
    let m = measured(s);
    assert_eq!(u(&m["success"]), u(&m["scheduled"]));
    assert_eq!(u(&s["connections"]["peak_open"]), 1);
    // In-flight above one on a single connection: streams run concurrently.
    assert!(f(&m["in_flight"]["sent_mean"]) > 1.5, "{}", m["in_flight"]);
}

/// HTTP/2 faults: stream resets fail one request each, timeouts cancel one
/// stream each, and the connections stay open.
#[test]
fn m3_h2_faults() {
    let _g = serial();
    let srv = Server::start(&[
        "--workers",
        "8",
        "--service",
        "exp:1ms",
        "--threads",
        "4",
        "--reset-prob",
        "0.02",
        "--stall-prob",
        "0.01",
    ]);
    let o = run(
        "m3_h2_faults",
        &[
            "--url",
            &srv.url,
            "--http2",
            "--rate",
            "2000",
            "--duration",
            "3s",
            "--threads",
            "2",
            "--connections",
            "4",
            "--timeout",
            "300ms",
            "--seed",
            "17",
        ],
    );
    let s = &o.summary;
    assert_conserved(s);
    let failed = &s["conservation"]["failed"];
    let timeouts = u(&failed["timeout"]);
    assert!(u(&failed["reset"]) > 0 && timeouts > 0, "{failed}");
    let c = &s["connections"];
    assert_eq!(u(&c["cancelled"]), timeouts, "{c}");
    assert_eq!(u(&c["reconnects"]), 0, "{c}");
    assert_eq!(u(&c["closes"]["client"]), 0, "{c}");
}

/// Library API: a run spawned on its own thread, watched through the window
/// channel and stopped early, still balances its books.
#[test]
fn m3_library_stop() {
    let _g = serial();
    let srv = Server::start(&["--workers", "4", "--threads", "2"]);
    let cfg = diapason_core::RunConfig::from_toml(&format!(
        "seed = 18\nthreads = 2\n[target]\nurl = \"{}\"\n[load]\n[[load.phases]]\nname = \"measure\"\nduration = \"60s\"\nrate = 1000\n[connections]\ncount = 10\n",
        srv.url
    ))
    .unwrap();
    let proto = diapason_proto_http::HttpProtocol::from_config(&cfg).unwrap();
    let (runner, windows) = diapason_core::Runner::new(cfg, proto).window_channel();
    let started = std::time::Instant::now();
    let handle = runner.spawn();
    let mut seen = 0;
    for w in windows {
        seen += 1;
        assert_eq!(w.index + 1, seen);
        if seen == 2 {
            handle.stop();
        }
    }
    let s = handle.join().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert!(s.run.stopped_early);
    assert!(s.conservation.ok);
    let scheduled = s.conservation.scheduled;
    assert!((2000..=6000).contains(&scheduled), "{scheduled}");
    assert_eq!(s.conservation.completed, scheduled);
}

/// Live metrics: the OpenMetrics endpoint answers during the run.
#[test]
fn m3_live_metrics() {
    use std::io::{Read, Write};
    let _g = serial();
    let srv = Server::start(&["--workers", "4", "--threads", "2"]);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr = format!("127.0.0.1:{port}");
    let mut client = Command::new(BIN)
        .args([
            "run",
            "--url",
            &srv.url,
            "--rate",
            "500",
            "--duration",
            "4s",
            "--threads",
            "1",
            "--connections",
            "5",
            "--metrics",
            &addr,
            "--quiet",
        ])
        .spawn()
        .unwrap();
    // Windows are published one to two windows late: poll until the first
    // full window (about 500 requests) shows up.
    let scrape = || -> Option<String> {
        let mut conn = std::net::TcpStream::connect(&addr).ok()?;
        conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .ok()?;
        let mut text = String::new();
        conn.read_to_string(&mut text).ok()?;
        Some(text)
    };
    let mut text = String::new();
    let mut scheduled = 0.0;
    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(100));
        if let Some(t) = scrape() {
            scheduled = t
                .lines()
                .find_map(|l| l.strip_prefix("diapason_requests_scheduled_total "))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            text = t;
            if scheduled >= 500.0 {
                break;
            }
        }
    }
    assert!(text.contains("application/openmetrics-text"), "{text}");
    assert!(scheduled >= 500.0, "{text}");
    assert!(text.contains("diapason_window_latency_seconds{kind=\"corrected\",quantile=\"0.99\"}"));
    assert!(text.trim_end().ends_with("# EOF"));
    assert_eq!(client.wait().unwrap().code(), Some(0));
}

struct Agent {
    child: Child,
    addr: String,
}

impl Agent {
    fn start(extra: &[&str]) -> Agent {
        let mut child = Command::new(BIN)
            .args(["agent", "--listen", "127.0.0.1:0"])
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start agent");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let addr = line
            .strip_prefix("agent listening ")
            .unwrap_or_else(|| panic!("unexpected agent output: {line}"))
            .trim()
            .to_string();
        Agent { child, addr }
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn trace_seq(paths: &[PathBuf]) -> Vec<(u64, u64, u64)> {
    let mut v = Vec::new();
    for p in paths {
        let text = std::fs::read_to_string(p).unwrap();
        let mut lines = text.lines();
        let header: Vec<&str> = lines.next().unwrap().split(',').collect();
        let col = |n: &str| header.iter().position(|h| *h == n).unwrap();
        let (s, i, w) = (col("seq"), col("intended_ns"), col("worker"));
        for l in lines {
            let f: Vec<&str> = l.split(',').collect();
            v.push((
                f[s].parse().unwrap(),
                f[i].parse().unwrap(),
                f[w].parse().unwrap(),
            ));
        }
    }
    v.sort_unstable();
    v
}

/// Distributed mode: two agents, one with its wall clock 500 ms ahead, run
/// the same requests as one node with the same number of workers; the
/// offsets are measured and T0 is aligned.
#[test]
fn m4_distributed() {
    let _g = serial();
    let srv = Server::start(&["--workers", "8", "--service", "exp:1ms", "--threads", "4"]);
    let a1 = Agent::start(&[]);
    let a2 = Agent::start(&["--clock-skew-ms", "500"]);
    let agents = format!("{},{}", a1.addr, a2.addr);
    let dtrace = tmp("m4_dist.csv");
    let common = [
        "--url",
        &srv.url,
        "--rate",
        "2000",
        "--arrival",
        "poisson",
        "--warmup",
        "1s",
        "--duration",
        "3s",
        "--threads",
        "2",
        "--connections",
        "40",
        "--seed",
        "19",
    ];
    let mut args = common.to_vec();
    args.extend(["--agents", &agents, "--trace", dtrace.to_str().unwrap()]);
    let o = run("m4_dist", &args);
    let s = &o.summary;
    assert_eq!(
        o.code,
        0,
        "exit {}: {} {:?}",
        o.code,
        s["exit_reason"],
        failed_checks(s)
    );
    assert_conserved(s);
    assert_eq!(u(&s["run"]["threads"]), 4);
    let ag = s["distributed"]["agents"].as_array().unwrap();
    assert_eq!(ag.len(), 2);
    assert_eq!(
        (u(&ag[0]["first_worker"]), u(&ag[1]["first_worker"])),
        (0, 2)
    );
    let off1 = f(&ag[0]["clock_offset_ms"]);
    let off2 = f(&ag[1]["clock_offset_ms"]);
    assert!(
        off1.abs() < 1.0 && (off2 - 500.0).abs() < 1.0,
        "offsets {off1} {off2}"
    );
    let t0a = u(&ag[0]["t0_monotonic_ns"]) as i64;
    let t0b = u(&ag[1]["t0_monotonic_ns"]) as i64;
    assert!(
        (t0a - t0b).abs() < 1_000_000,
        "T0 differ by {} ns",
        t0a - t0b
    );

    let ltrace = tmp("m4_local.csv");
    let mut args = common.to_vec();
    let i = args.iter().position(|a| *a == "--threads").unwrap() + 1;
    args[i] = "4";
    args.extend(["--trace", ltrace.to_str().unwrap()]);
    let o = run("m4_local", &args);
    assert_conserved(&o.summary);
    let d = trace_seq(&[tmp("w0-1.m4_dist.csv"), tmp("w2-3.m4_dist.csv")]);
    let l = trace_seq(&[ltrace]);
    assert!(d.len() > 7000, "{}", d.len());
    assert_eq!(d, l, "the distributed run sent other requests");
}

/// An agent that dies during the run makes the coordinator stop with a
/// runtime error instead of hanging.
#[test]
fn m4_agent_lost() {
    let _g = serial();
    let srv = Server::start(&["--workers", "0", "--threads", "2"]);
    let a1 = Agent::start(&[]);
    let mut a2 = Agent::start(&[]);
    let summary = tmp("m4_lost.json");
    let _ = std::fs::remove_file(&summary);
    let started = std::time::Instant::now();
    let mut coord = Command::new(BIN)
        .args([
            "run",
            "--agents",
            &format!("{},{}", a1.addr, a2.addr),
            "--url",
            &srv.url,
            "--rate",
            "500",
            "--duration",
            "4s",
            "--threads",
            "1",
            "--connections",
            "4",
            "--quiet",
            "--summary",
        ])
        .arg(&summary)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    let _ = a2.child.kill();
    let status = coord.wait().unwrap();
    assert!(started.elapsed() < Duration::from_secs(20));
    assert_eq!(status.code(), Some(1));
    let s: Value = serde_json::from_str(&std::fs::read_to_string(&summary).unwrap()).unwrap();
    assert!(
        s["exit_reason"].as_str().unwrap().contains("agent"),
        "{}",
        s["exit_reason"]
    );
}
