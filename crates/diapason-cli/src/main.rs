//! The `diapason` command: `run`, `server`, `selftest`.

use clap::{Args, Parser, Subcommand};
use diapason_core::config::RunConfig;
use diapason_core::summary::{exit, Summary};
use diapason_core::{report, Runner, WindowSnapshot};
use diapason_proto_http::HttpProtocol;
use diapason_server::{ServerConfig, Service, Slowdown};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "diapason",
    version,
    about = "Open-loop load generator with trustworthy accounting"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a load test.
    Run(Box<RunArgs>),
    /// Run the reference server (an M/G/c queue in virtual time).
    Server(Box<ServerArgs>),
    /// Run a short test against an in-process reference server.
    Selftest(SelftestArgs),
    /// Serve a coordinator (`diapason run --agents ...`) on this client node.
    Agent(AgentArgs),
}

#[derive(Args)]
struct AgentArgs {
    #[arg(long, default_value = "0.0.0.0:7700")]
    listen: String,
    /// Shift this agent's wall clock, to test the offset correction.
    #[arg(long, default_value_t = 0.0, hide = true)]
    clock_skew_ms: f64,
}

#[derive(Args, Default)]
struct RunArgs {
    /// TOML configuration file; the options below override it.
    #[arg(long, short)]
    config: Option<PathBuf>,
    #[arg(long)]
    url: Option<String>,
    #[arg(long)]
    method: Option<String>,
    /// Request header "name: value" (repeatable).
    #[arg(long = "header", short = 'H')]
    headers: Vec<String>,
    #[arg(long)]
    body: Option<String>,
    /// Requests per second. With --warmup, --duration or --cooldown, replaces
    /// the phases of the configuration file.
    #[arg(long)]
    rate: Option<f64>,
    /// constant, poisson or replay.
    #[arg(long)]
    arrival: Option<String>,
    #[arg(long)]
    replay_file: Option<PathBuf>,
    #[arg(long)]
    warmup: Option<String>,
    /// Length of the measurement phase.
    #[arg(long)]
    duration: Option<String>,
    #[arg(long)]
    cooldown: Option<String>,
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long)]
    connections: Option<usize>,
    /// Run on these agents (host:port, comma-separated) instead of locally.
    #[arg(long, value_delimiter = ',')]
    agents: Vec<String>,
    /// Serve live OpenMetrics on this address, e.g. 127.0.0.1:9100.
    #[arg(long)]
    metrics: Option<String>,
    /// Use HTTP/2 (h2 over TLS, h2c with prior knowledge over cleartext).
    #[arg(long)]
    http2: bool,
    /// Open connections on demand, up to this many in total.
    #[arg(long)]
    max_connections: Option<usize>,
    #[arg(long)]
    pipeline: Option<usize>,
    #[arg(long)]
    max_in_flight: Option<usize>,
    #[arg(long)]
    max_queue_delay: Option<String>,
    #[arg(long)]
    max_queue_len: Option<usize>,
    #[arg(long)]
    connect_timeout: Option<String>,
    /// Per-request timeout.
    #[arg(long)]
    timeout: Option<String>,
    #[arg(long)]
    drain: Option<String>,
    #[arg(long)]
    seed: Option<u64>,
    /// Closed-loop mode, for comparison only: every connection sends its next
    /// request when the previous one finishes.
    #[arg(long)]
    closed_loop: bool,
    /// Write the JSON summary here ("-" for stdout).
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    trace: Option<PathBuf>,
    #[arg(long)]
    trace_sample: Option<u64>,
    #[arg(long)]
    histogram_log: Option<PathBuf>,
    #[arg(long)]
    windows: Option<PathBuf>,
    /// Window length for the histogram log and the window file.
    #[arg(long)]
    window: Option<String>,
    /// Timer spin before each intended time.
    #[arg(long)]
    spin: Option<String>,
    /// Pin worker threads to CPUs.
    #[arg(long)]
    pin: bool,
    /// User threshold on the measurement phase, e.g. "p99 < 200ms"
    /// (repeatable). A failed threshold makes the exit code 5.
    #[arg(long = "threshold")]
    thresholds: Vec<String>,
    /// Accept any TLS server certificate (tests only).
    #[arg(long)]
    insecure: bool,
    /// Extra trusted CA certificates (PEM).
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// Client certificate chain (PEM), with --client-key.
    #[arg(long)]
    client_cert: Option<PathBuf>,
    #[arg(long)]
    client_key: Option<PathBuf>,
    /// Set any configuration key, e.g. --set fidelity.lag_p99=2ms (repeatable).
    #[arg(long = "set")]
    sets: Vec<String>,
    /// Print one line per window on stderr.
    #[arg(long)]
    progress: bool,
    /// Do not print the text summary.
    #[arg(long, short)]
    quiet: bool,
}

#[derive(Args)]
struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Number of virtual servers c; 0 answers at once.
    #[arg(long, conflicts_with = "capacity")]
    workers: Option<usize>,
    /// Target capacity μ in requests per second; sets workers = μ × E[S].
    #[arg(long)]
    capacity: Option<f64>,
    /// const:D, exp:MEAN, uniform:A:B or lognormal:MEAN:SIGMA.
    #[arg(long, default_value = "const:1ms")]
    service: String,
    /// at:for:factor, multiplies service times in an interval (repeatable).
    #[arg(long = "slowdown")]
    slowdowns: Vec<String>,
    #[arg(long, default_value_t = 0.0)]
    reset_prob: f64,
    #[arg(long, default_value_t = 0.0)]
    close_prob: f64,
    #[arg(long, default_value_t = 0.0)]
    stall_prob: f64,
    #[arg(long, default_value_t = 2)]
    body_size: usize,
    /// Runtime threads of the server process.
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Serve TLS with this certificate chain (PEM), with --tls-key.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// Require client certificates signed by this CA (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_client_ca: Option<PathBuf>,
}

#[derive(Args)]
struct SelftestArgs {
    #[arg(long, default_value_t = 2000.0)]
    rate: f64,
    #[arg(long, default_value = "3s")]
    duration: String,
}

fn set_path(root: &mut toml::Table, path: &str, value: toml::Value) -> Result<(), String> {
    let mut parts: Vec<&str> = path.split('.').collect();
    let last = parts.pop().ok_or("empty key")?;
    let mut t = root;
    for p in parts {
        let entry = t
            .entry(p.to_string())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        t = entry
            .as_table_mut()
            .ok_or_else(|| format!("'{path}': '{p}' is not a table"))?;
    }
    t.insert(last.to_string(), value);
    Ok(())
}

fn parse_value(v: &str) -> toml::Value {
    match format!("v = {v}").parse::<toml::Table>() {
        Ok(mut t) => t
            .remove("v")
            .unwrap_or_else(|| toml::Value::String(v.into())),
        Err(_) => toml::Value::String(v.into()),
    }
}

fn s(v: &str) -> toml::Value {
    toml::Value::String(v.into())
}

fn build_config(a: &RunArgs) -> Result<RunConfig, String> {
    let mut root: toml::Table = match &a.config {
        Some(p) => std::fs::read_to_string(p)
            .map_err(|e| format!("{}: {e}", p.display()))?
            .parse()
            .map_err(|e| format!("{}: {e}", p.display()))?,
        None => toml::Table::new(),
    };
    let mut set = |k: &str, v: toml::Value| set_path(&mut root, k, v);
    if let Some(v) = &a.url {
        set("target.url", s(v))?;
    }
    if let Some(v) = &a.method {
        set("target.method", s(v))?;
    }
    for h in &a.headers {
        let (k, v) = h
            .split_once(':')
            .ok_or_else(|| format!("header '{h}' must be 'name: value'"))?;
        set(&format!("target.headers.{}", k.trim()), s(v.trim()))?;
    }
    if let Some(v) = &a.body {
        set("target.body", s(v))?;
    }
    if let Some(v) = &a.arrival {
        set("load.arrival", s(v))?;
    }
    if let Some(v) = &a.replay_file {
        set("load.replay_file", s(&v.display().to_string()))?;
    }
    if a.rate.is_some() || a.warmup.is_some() || a.duration.is_some() || a.cooldown.is_some() {
        let mut phases = Vec::new();
        let mut phase = |name: &str, d: &str| {
            let mut t = toml::Table::new();
            t.insert("name".into(), s(name));
            t.insert("duration".into(), s(d));
            if let Some(r) = a.rate {
                t.insert("rate".into(), toml::Value::Float(r));
            }
            phases.push(toml::Value::Table(t));
        };
        if let Some(w) = &a.warmup {
            phase("warmup", w);
        }
        phase("measure", a.duration.as_deref().unwrap_or("10s"));
        if let Some(c) = &a.cooldown {
            phase("cooldown", c);
        }
        set("load.phases", toml::Value::Array(phases))?;
        set("load.measure", s("measure"))?;
    }
    let ints: [(&str, Option<usize>); 6] = [
        ("connections.max", a.max_connections),
        ("threads", a.threads),
        ("connections.count", a.connections),
        ("connections.pipeline", a.pipeline),
        ("connections.max_in_flight", a.max_in_flight),
        ("connections.max_queue_len", a.max_queue_len),
    ];
    for (k, v) in ints {
        if let Some(v) = v {
            set(k, toml::Value::Integer(v as i64))?;
        }
    }
    let durs = [
        ("connections.max_queue_delay", &a.max_queue_delay),
        ("connections.connect_timeout", &a.connect_timeout),
        ("timeouts.request", &a.timeout),
        ("timeouts.drain", &a.drain),
        ("output.window", &a.window),
        ("timer.spin", &a.spin),
    ];
    for (k, v) in durs {
        if let Some(v) = v {
            set(k, s(v))?;
        }
    }
    if let Some(v) = a.seed {
        set("seed", toml::Value::Integer(v as i64))?;
    }
    if a.closed_loop {
        set("mode", s("closed"))?;
    }
    if let Some(v) = &a.summary {
        if v != "-" {
            set("output.summary", s(v))?;
        }
    }
    for (k, v) in [
        ("output.trace", &a.trace),
        ("output.histogram_log", &a.histogram_log),
        ("output.windows", &a.windows),
    ] {
        if let Some(v) = v {
            set(k, s(&v.display().to_string()))?;
        }
    }
    if let Some(v) = a.trace_sample {
        set("output.trace_sample", toml::Value::Integer(v as i64))?;
    }
    if a.pin {
        set("system.pin_threads", toml::Value::Boolean(true))?;
    }
    if let Some(m) = &a.metrics {
        set("output.metrics", s(m))?;
    }
    if a.http2 {
        set("http.version", s("2"))?;
    }
    if a.insecure {
        set("tls.insecure_skip_verify", toml::Value::Boolean(true))?;
    }
    for (k, v) in [
        ("tls.ca_file", &a.ca_file),
        ("tls.client_cert", &a.client_cert),
        ("tls.client_key", &a.client_key),
    ] {
        if let Some(v) = v {
            set(k, s(&v.display().to_string()))?;
        }
    }
    for kv in &a.sets {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("--set '{kv}' must be key=value"))?;
        set(k.trim(), parse_value(v.trim()))?;
    }
    if !a.thresholds.is_empty() {
        let list = root
            .entry("thresholds")
            .or_insert_with(|| toml::Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or("thresholds must be an array")?;
        for e in &a.thresholds {
            let mut t = toml::Table::new();
            t.insert("expr".into(), s(e));
            list.push(toml::Value::Table(t));
        }
    }
    if !root.contains_key("target") {
        return Err("no target: give --url or a configuration file with [target]".into());
    }
    let cfg: RunConfig = toml::Value::Table(root)
        .try_into()
        .map_err(|e: toml::de::Error| e.to_string())?;
    cfg.validate().map_err(|e| e.0)?;
    Ok(cfg)
}

fn progress_line(w: &WindowSnapshot) {
    let p99 = if w.hists.corrected.is_empty() {
        0.0
    } else {
        w.hists.corrected.value_at_quantile(0.99) as f64 / 1e6
    };
    eprintln!(
        "[{:>7.1}s] sched {:>7} sent {:>7} done {:>7} fail {:>5} notsent {:>5} inflight {:>8.1} p99 {:>9.3} ms",
        w.end_s, w.scheduled, w.sent, w.completed, w.failed, w.not_sent, w.in_flight_mean, p99
    );
}

fn finish(summary: &Summary, summary_arg: Option<&str>, quiet: bool) -> ExitCode {
    let json = summary.to_json();
    let mut code = summary.exit_code;
    match summary_arg {
        Some("-") => println!("{json}"),
        Some(path) => {
            if let Err(e) = std::fs::write(path, &json) {
                eprintln!("diapason: cannot write summary {path}: {e}");
                code = exit::RUNTIME;
            }
        }
        None => {}
    }
    if !quiet {
        let text = report::text(summary);
        if summary_arg == Some("-") {
            eprint!("{text}");
        } else {
            print!("{text}");
        }
    }
    let _ = std::io::stdout().flush();
    ExitCode::from(code as u8)
}

fn cmd_run(a: RunArgs) -> ExitCode {
    let cfg = match build_config(&a) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("diapason: configuration error: {e}");
            return ExitCode::from(exit::CONFIG as u8);
        }
    };
    if !a.agents.is_empty() {
        let mut c = diapason_core::Coordinator::new(cfg, a.agents.clone());
        if a.progress {
            c = c.on_window(progress_line);
        }
        return match c.run() {
            Ok(summary) => finish(&summary, a.summary.as_deref(), a.quiet),
            Err(e) => {
                eprintln!("diapason: {e}");
                ExitCode::from(e.exit_code() as u8)
            }
        };
    }
    let proto = match HttpProtocol::from_config(&cfg) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("diapason: configuration error: {e}");
            return ExitCode::from(exit::CONFIG as u8);
        }
    };
    let mut runner = Runner::new(cfg, proto);
    if a.progress {
        runner = runner.on_window(progress_line);
    }
    match runner.run() {
        Ok(summary) => finish(&summary, a.summary.as_deref(), a.quiet),
        Err(e) => {
            eprintln!("diapason: {e}");
            ExitCode::from(e.exit_code() as u8)
        }
    }
}

fn server_config(a: &ServerArgs) -> Result<ServerConfig, String> {
    let service = Service::parse(&a.service)?;
    let workers = match (a.workers, a.capacity) {
        (Some(w), _) => w,
        (None, Some(c)) if c > 0.0 => ServerConfig::workers_for(c, &service),
        (None, Some(_)) => return Err("--capacity must be positive".into()),
        (None, None) => 0,
    };
    for (name, p) in [
        ("reset", a.reset_prob),
        ("close", a.close_prob),
        ("stall", a.stall_prob),
    ] {
        if !(0.0..=1.0).contains(&p) {
            return Err(format!("--{name}-prob must be in [0, 1]"));
        }
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    Ok(ServerConfig {
        listen: a.listen,
        workers,
        service,
        slowdowns: a
            .slowdowns
            .iter()
            .map(|s| Slowdown::parse(s))
            .collect::<Result<_, _>>()?,
        reset_prob: a.reset_prob,
        close_prob: a.close_prob,
        stall_prob: a.stall_prob,
        body_size: a.body_size,
        threads: a.threads.unwrap_or((cores / 2).max(1)),
        seed: a.seed,
        tls: match (&a.tls_cert, &a.tls_key) {
            (Some(cert), Some(key)) => Some(diapason_server::ServerTls {
                cert: cert.clone(),
                key: key.clone(),
                client_ca: a.tls_client_ca.clone(),
            }),
            _ => None,
        },
    })
}

fn cmd_server(a: ServerArgs) -> ExitCode {
    let cfg = match server_config(&a) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("diapason server: {e}");
            return ExitCode::from(exit::CONFIG as u8);
        }
    };
    let capacity = cfg.capacity();
    let desc = format!(
        "workers {}, service {}, capacity {} req/s, threads {}",
        cfg.workers,
        a.service,
        if capacity.is_finite() {
            format!("{capacity:.1}")
        } else {
            "unbounded".into()
        },
        cfg.threads
    );
    let r = diapason_server::run_blocking(cfg, |addr| {
        println!("listening {addr} ({desc})");
        let _ = std::io::stdout().flush();
    });
    match r {
        Ok(stats) => {
            eprintln!("{}", serde_json::to_string(&stats).unwrap_or_default());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("diapason server: {e}");
            ExitCode::from(exit::RUNTIME as u8)
        }
    }
}

fn cmd_selftest(a: SelftestArgs) -> ExitCode {
    let server = match diapason_server::spawn(ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        workers: 8,
        service: Service::Const(0.001),
        threads: 2,
        ..Default::default()
    }) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("selftest: cannot start the reference server: {e}");
            return ExitCode::from(exit::RUNTIME as u8);
        }
    };
    println!(
        "selftest: reference server on {} (capacity 8000 req/s)",
        server.addr
    );
    let args = RunArgs {
        url: Some(format!("http://{}/", server.addr)),
        rate: Some(a.rate),
        arrival: Some("poisson".into()),
        warmup: Some("1s".into()),
        duration: Some(a.duration.clone()),
        threads: Some(2),
        connections: Some(64),
        seed: Some(1),
        ..Default::default()
    };
    let cfg = match build_config(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("selftest: {e}");
            return ExitCode::from(exit::CONFIG as u8);
        }
    };
    let proto = HttpProtocol::from_config(&cfg).expect("local URL");
    let summary = match Runner::new(cfg, proto).run() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("selftest: {e}");
            return ExitCode::from(e.exit_code() as u8);
        }
    };
    print!("{}", report::text(&summary));
    let m = summary.measured_phase().expect("measure phase");
    let mut problems = Vec::new();
    if !summary.conservation.ok {
        problems.push("conservation".to_string());
    }
    if m.completed != m.scheduled {
        problems.push(format!(
            "{} of {} requests completed",
            m.completed, m.scheduled
        ));
    }
    if m.success != m.completed {
        problems.push(format!("{} non-success responses", m.completed - m.success));
    }
    if problems.is_empty() && summary.exit_code == 0 {
        println!("selftest: ok");
        ExitCode::SUCCESS
    } else {
        println!("selftest: FAILED ({})", problems.join(", "));
        ExitCode::from(if summary.exit_code != 0 {
            summary.exit_code as u8
        } else {
            exit::RUNTIME as u8
        })
    }
}

fn cmd_agent(a: AgentArgs) -> ExitCode {
    let skew = (a.clock_skew_ms * 1e6) as i64;
    let r = diapason_core::serve_agent(&a.listen, HttpProtocol::from_config, skew, |addr| {
        println!("agent listening {addr}");
        let _ = std::io::stdout().flush();
    });
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("diapason agent: {e}");
            ExitCode::from(exit::RUNTIME as u8)
        }
    }
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            let code = if e.use_stderr() { exit::CONFIG } else { 0 };
            let _ = e.print();
            return ExitCode::from(code as u8);
        }
    };
    match cli.cmd {
        Cmd::Run(a) => cmd_run(*a),
        Cmd::Server(a) => cmd_server(*a),
        Cmd::Selftest(a) => cmd_selftest(a),
        Cmd::Agent(a) => cmd_agent(a),
    }
}
