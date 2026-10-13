//! Run configuration. The TOML shape is documented in the README; unknown keys
//! are rejected.

use crate::duration::Dur;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

fn err<T>(msg: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError(msg.into()))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Open,
    Closed,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Open => "open",
            Mode::Closed => "closed",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Arrival {
    Constant,
    #[default]
    Poisson,
    Replay,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threads: Option<usize>,
    pub target: TargetConfig,
    pub load: LoadConfig,
    #[serde(default)]
    pub connections: ConnectionsConfig,
    #[serde(default)]
    pub timeouts: TimeoutsConfig,
    #[serde(default)]
    pub timer: TimerConfig,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub fidelity: FidelityConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub system: SystemConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scenarios: Vec<ScenarioConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub vars: BTreeMap<String, VarConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub data: BTreeMap<String, DataConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thresholds: Vec<ThresholdConfig>,
    #[serde(default)]
    pub tls: TlsConfig,
}

/// One weighted request template. Unset fields come from `[target]`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioConfig {
    pub name: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

fn default_weight() -> f64 {
    1.0
}

/// A value of a `choice` variable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Scalar {
    Int(i64),
    Float(f64),
    Str(String),
}

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Int(i) => write!(f, "{i}"),
            Scalar::Float(x) => write!(f, "{x}"),
            Scalar::Str(s) => f.write_str(s),
        }
    }
}

/// A template variable, drawn per request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "dist", rename_all = "lowercase", deny_unknown_fields)]
pub enum VarConfig {
    /// Integer uniform on [min, max].
    Uniform { min: i64, max: i64 },
    /// Integer in [1, n], P(k) proportional to 1/k^s.
    Zipf { n: u64, s: f64 },
    Normal {
        mean: f64,
        stdev: f64,
        #[serde(default)]
        round: bool,
    },
    Choice {
        values: Vec<Scalar>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        weights: Option<Vec<f64>>,
    },
    /// start + step × seq.
    Sequence {
        #[serde(default)]
        start: i64,
        #[serde(default = "default_step")]
        step: i64,
    },
}

fn default_step() -> i64 {
    1
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    Csv,
    Jsonl,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataMode {
    /// Row seq mod rows.
    #[default]
    Sequential,
    /// A row drawn from (seed, seq).
    Random,
    /// Row seq; requests beyond the last row wrap around and are counted.
    Unique,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataConfig {
    pub file: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<DataFormat>,
    #[serde(default)]
    pub mode: DataMode,
}

/// A user threshold, such as "p99 < 200ms", evaluated on one phase.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdConfig {
    pub expr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Accept any server certificate. For tests only.
    #[serde(default)]
    pub insecure_skip_verify: bool,
    /// PEM file of extra trusted roots; the Mozilla roots are always trusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    /// PEM certificate chain and private key for client authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_cert: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key: Option<PathBuf>,
    /// Server name sent in SNI and checked in the certificate; default: the
    /// URL host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: String,
}

fn default_method() -> String {
    "GET".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadConfig {
    #[serde(default)]
    pub arrival: Arrival,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_file: Option<PathBuf>,
    #[serde(default = "default_measure")]
    pub measure: String,
    pub phases: Vec<PhaseConfig>,
}

fn default_measure() -> String {
    "measure".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<Dur>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_end: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<StepConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub points: Option<Vec<PointConfig>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepConfig {
    pub duration: Dur,
    pub rate: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointConfig {
    pub at: Dur,
    pub rate: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsConfig {
    #[serde(default = "default_connections")]
    pub count: usize,
    /// Most connections, counting those opened on demand; default `count`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<usize>,
    #[serde(default = "default_pipeline")]
    pub pipeline: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_in_flight: Option<usize>,
    #[serde(default = "default_max_queue_delay")]
    pub max_queue_delay: Dur,
    #[serde(default = "default_max_queue_len")]
    pub max_queue_len: usize,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: Dur,
}

fn default_connections() -> usize {
    100
}
fn default_pipeline() -> usize {
    1
}
fn default_max_queue_delay() -> Dur {
    Dur::from_secs_f64(60.0)
}
fn default_max_queue_len() -> usize {
    1_000_000
}
fn default_connect_timeout() -> Dur {
    Dur::from_secs_f64(5.0)
}

impl Default for ConnectionsConfig {
    fn default() -> Self {
        ConnectionsConfig {
            count: default_connections(),
            max: None,
            pipeline: default_pipeline(),
            max_in_flight: None,
            max_queue_delay: default_max_queue_delay(),
            max_queue_len: default_max_queue_len(),
            connect_timeout: default_connect_timeout(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeoutsConfig {
    #[serde(default = "default_request_timeout")]
    pub request: Dur,
    #[serde(default = "default_drain")]
    pub drain: Dur,
}

fn default_request_timeout() -> Dur {
    Dur::from_secs_f64(60.0)
}
fn default_drain() -> Dur {
    Dur::from_secs_f64(10.0)
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        TimeoutsConfig {
            request: default_request_timeout(),
            drain: default_drain(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerConfig {
    #[serde(default = "default_spin")]
    pub spin: Dur,
}

fn default_spin() -> Dur {
    Dur::from_micros(100)
}

impl Default for TimerConfig {
    fn default() -> Self {
        TimerConfig {
            spin: default_spin(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_success")]
    pub success_status: Vec<String>,
    /// "1.1" or "2". HTTP/2 uses ALPN over TLS and prior knowledge (h2c)
    /// over cleartext.
    #[serde(default)]
    pub version: HttpVersion,
    /// HTTP/2: most concurrent streams per connection (the server's own limit
    /// applies too).
    #[serde(default = "default_h2_streams")]
    pub h2_max_streams: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpVersion {
    #[default]
    #[serde(rename = "1.1")]
    Http11,
    #[serde(rename = "2")]
    Http2,
}

fn default_h2_streams() -> usize {
    100
}

fn default_success() -> Vec<String> {
    vec!["200-399".into()]
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            success_status: default_success(),
            version: HttpVersion::default(),
            h2_max_streams: default_h2_streams(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FidelityConfig {
    #[serde(default = "d_lag_p99")]
    pub lag_p99: Dur,
    #[serde(default = "d_lag_max")]
    pub lag_max: Dur,
    #[serde(default = "d_send_ratio")]
    pub send_ratio_min: f64,
    #[serde(default = "d_burst_10")]
    pub burst_factor_10ms: f64,
    #[serde(default = "d_burst_50")]
    pub burst_factor_50ms: f64,
    #[serde(default = "d_empty_slack")]
    pub empty_slack: f64,
    #[serde(default = "d_conn_wait")]
    pub conn_wait_threshold: Dur,
    #[serde(default = "d_admission")]
    pub admission_share_max: f64,
    #[serde(default = "d_busy")]
    pub busy_share_max: f64,
    #[serde(default = "d_runqueue")]
    pub runqueue_share_max: f64,
}

fn d_lag_p99() -> Dur {
    Dur::from_millis(1)
}
fn d_lag_max() -> Dur {
    Dur::from_millis(50)
}
fn d_send_ratio() -> f64 {
    0.99
}
fn d_burst_10() -> f64 {
    2.0
}
fn d_burst_50() -> f64 {
    1.5
}
fn d_empty_slack() -> f64 {
    0.01
}
fn d_conn_wait() -> Dur {
    Dur::from_millis(1)
}
fn d_admission() -> f64 {
    0.01
}
fn d_busy() -> f64 {
    0.9
}
fn d_runqueue() -> f64 {
    0.05
}

impl Default for FidelityConfig {
    fn default() -> Self {
        FidelityConfig {
            lag_p99: d_lag_p99(),
            lag_max: d_lag_max(),
            send_ratio_min: d_send_ratio(),
            burst_factor_10ms: d_burst_10(),
            burst_factor_50ms: d_burst_50(),
            empty_slack: d_empty_slack(),
            conn_wait_threshold: d_conn_wait(),
            admission_share_max: d_admission(),
            busy_share_max: d_busy(),
            runqueue_share_max: d_runqueue(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    #[serde(default = "default_window")]
    pub window: Dur,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub histogram_log: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub windows: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<PathBuf>,
    #[serde(default = "default_sample")]
    pub trace_sample: u64,
    /// Address of the live OpenMetrics endpoint, e.g. "127.0.0.1:9100".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<String>,
}

fn default_window() -> Dur {
    Dur::from_secs_f64(1.0)
}
fn default_sample() -> u64 {
    1
}

impl Default for OutputConfig {
    fn default() -> Self {
        OutputConfig {
            window: default_window(),
            summary: None,
            histogram_log: None,
            windows: None,
            trace: None,
            trace_sample: default_sample(),
            metrics: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemConfig {
    #[serde(default = "default_fd_margin")]
    pub fd_margin: u64,
    #[serde(default)]
    pub pin_threads: bool,
}

fn default_fd_margin() -> u64 {
    64
}

impl Default for SystemConfig {
    fn default() -> Self {
        SystemConfig {
            fd_margin: default_fd_margin(),
            pin_threads: false,
        }
    }
}

/// A set of status codes, parsed from strings such as "200-399" or "404".
#[derive(Clone, Debug)]
pub struct StatusSet(Vec<(u16, u16)>);

impl StatusSet {
    pub fn parse(items: &[String]) -> Result<StatusSet, ConfigError> {
        let mut ranges = Vec::new();
        for item in items {
            let item = item.trim();
            let parse = |s: &str| -> Result<u16, ConfigError> {
                s.trim()
                    .parse::<u16>()
                    .ok()
                    .filter(|c| (100..=999).contains(c))
                    .ok_or_else(|| ConfigError(format!("http.success_status: bad code '{s}'")))
            };
            let (lo, hi) = match item.split_once('-') {
                Some((a, b)) => (parse(a)?, parse(b)?),
                None if item.len() == 3 && item.ends_with("xx") => {
                    let d = parse(&format!("{}00", &item[..1]))?;
                    (d, d + 99)
                }
                None => {
                    let c = parse(item)?;
                    (c, c)
                }
            };
            if lo > hi {
                return err(format!("http.success_status: empty range '{item}'"));
            }
            ranges.push((lo, hi));
        }
        Ok(StatusSet(ranges))
    }

    pub fn contains(&self, code: u16) -> bool {
        self.0.iter().any(|&(lo, hi)| (lo..=hi).contains(&code))
    }
}

impl RunConfig {
    pub fn from_toml(text: &str) -> Result<RunConfig, ConfigError> {
        toml::from_str(text).map_err(|e| ConfigError(e.to_string()))
    }

    /// Checks everything that can be checked without touching the system.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.load.phases.is_empty() {
            return err("load.phases is empty");
        }
        let mut names = std::collections::HashSet::new();
        for p in &self.load.phases {
            if !names.insert(p.name.as_str()) {
                return err(format!("phase name '{}' is used twice", p.name));
            }
        }
        if self.load.arrival == Arrival::Replay && self.load.replay_file.is_none() {
            return err("load.arrival = \"replay\" needs load.replay_file");
        }
        match self.connections.max {
            Some(m) if m < self.connections.count => {
                return err("connections.max must be at least connections.count")
            }
            Some(0) | None if self.connections.count == 0 => {
                return err("connections.count must be at least 1, or connections.max must be set")
            }
            _ => {}
        }
        if self.connections.pipeline == 0 {
            return err("connections.pipeline must be at least 1");
        }
        if self.connections.max_in_flight == Some(0) {
            return err("connections.max_in_flight must be at least 1");
        }
        if self.threads == Some(0) {
            return err("threads must be at least 1");
        }
        let w = self.output.window.as_nanos();
        if w == 0 || !w.is_multiple_of(50_000_000) {
            return err("output.window must be a positive multiple of 50ms");
        }
        if self.output.trace_sample == 0 {
            return err("output.trace_sample must be at least 1");
        }
        if self.timeouts.request.0.is_zero() {
            return err("timeouts.request must be positive");
        }
        StatusSet::parse(&self.http.success_status)?;
        let profile = crate::schedule::RateProfile::from_config(self)?;
        let mut names = std::collections::HashSet::new();
        for sc in &self.scenarios {
            if !names.insert(sc.name.as_str()) {
                return err(format!("scenario name '{}' is used twice", sc.name));
            }
            if !(sc.weight.is_finite() && sc.weight >= 0.0) {
                return err(format!(
                    "scenario '{}': weight must be a non-negative number",
                    sc.name
                ));
            }
        }
        if !self.scenarios.is_empty() && self.scenarios.iter().all(|s| s.weight == 0.0) {
            return err("scenarios: at least one weight must be positive");
        }
        if self.scenarios.len() > u16::MAX as usize {
            return err("too many scenarios");
        }
        for t in &self.thresholds {
            crate::threshold::Threshold::parse(&t.expr).map_err(ConfigError)?;
            let phase = t.phase.as_deref().unwrap_or(&self.load.measure);
            if !profile.phases.iter().any(|p| p.name == phase) {
                return err(format!("threshold '{}': no phase named '{phase}'", t.expr));
            }
        }
        if self.http.h2_max_streams == 0 {
            return err("http.h2_max_streams must be at least 1");
        }
        if self.http.version == HttpVersion::Http2 && self.connections.pipeline != 1 {
            return err(
                "connections.pipeline applies to HTTP/1.1; HTTP/2 uses http.h2_max_streams",
            );
        }
        if self.tls.client_cert.is_some() != self.tls.client_key.is_some() {
            return err("tls.client_cert and tls.client_key go together");
        }
        Ok(())
    }
}
