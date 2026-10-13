//! The typed form of the JSON summary (schema `diapason.summary`, version 1).
//! Every field is documented in the README.

use crate::hist::LatencyStats;
use crate::host::HostInfo;
use crate::window::SliceStats;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SCHEMA: &str = "diapason.summary";
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Summary {
    pub schema: String,
    pub schema_version: u32,
    pub tool: ToolInfo,
    pub mode: String,
    pub seed: u64,
    pub started_at: StartedAt,
    pub host: HostInfo,
    pub config: serde_json::Value,
    pub run: RunInfo,
    pub conservation: Conservation,
    pub phases: Vec<PhaseSummary>,
    pub connections: ConnectionsSummary,
    pub workers: Vec<WorkerSummary>,
    pub fidelity: Fidelity,
    pub thresholds: Vec<crate::threshold::ThresholdResult>,
    pub protocol_report: serde_json::Value,
    pub outputs: Outputs,
    pub warnings: Vec<String>,
    /// Present for a distributed run: the agents and their clocks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distributed: Option<Distributed>,
    pub exit_code: i32,
    pub exit_reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolInfo {
    pub name: String,
    pub version: String,
    pub git_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartedAt {
    pub unix_ns: u64,
    pub rfc3339: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunInfo {
    pub scheduled_duration_s: f64,
    pub wall_duration_s: f64,
    pub threads: usize,
    pub measure_phase: Option<String>,
    pub stopped_early: bool,
    pub protocol: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Conservation {
    pub scheduled: u64,
    pub completed: u64,
    pub failed: BTreeMap<String, u64>,
    pub failed_total: u64,
    pub not_sent: BTreeMap<String, u64>,
    pub not_sent_total: u64,
    pub in_flight_at_end: u64,
    pub balance: i64,
    pub ok: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Latency {
    pub corrected: LatencyStats,
    pub server: LatencyStats,
    pub connection_wait: LatencyStats,
    pub generator_lag: LatencyStats,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InFlight {
    pub sent_mean: f64,
    pub sent_max: u64,
    pub system_mean: f64,
    pub system_max: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Little {
    pub departure_rate: f64,
    pub mean_time_in_flight_s: f64,
    pub predicted_in_flight: f64,
    pub measured_in_flight: f64,
    pub relative_error: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PhaseSummary {
    pub name: String,
    pub start_s: f64,
    pub end_s: f64,
    pub duration_s: f64,
    pub measured: bool,
    pub nominal_rate: f64,
    pub scheduled: u64,
    pub offered_rate: f64,
    pub completed: u64,
    pub success: u64,
    pub failed: BTreeMap<String, u64>,
    pub not_sent: BTreeMap<String, u64>,
    pub in_flight_at_end: u64,
    pub status: BTreeMap<String, u64>,
    pub achieved_rate_by_intended: f64,
    pub achieved_rate_by_completion: f64,
    pub success_ratio: f64,
    pub latency: Latency,
    pub in_flight: InFlight,
    pub little: Little,
    pub scenarios: Vec<ScenarioSummary>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScenarioSummary {
    pub name: String,
    /// Configured weight over the sum of weights.
    pub weight_share: f64,
    /// Realised share of the phase's scheduled requests.
    pub share: f64,
    pub scheduled: u64,
    pub completed: u64,
    pub success: u64,
    pub failed: u64,
    pub not_sent: u64,
    pub in_flight_at_end: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConnectionsSummary {
    pub requested: u64,
    pub established: u64,
    pub shortfall: BTreeMap<String, u64>,
    pub connect_attempts: u64,
    pub connect_errors: BTreeMap<String, u64>,
    pub reconnects: u64,
    pub closes: Closes,
    pub requeued: u64,
    /// Connections opened after T0 because requests were waiting.
    pub opened_on_demand: u64,
    /// Sum over workers of the most connections each had open at once.
    pub peak_open: u64,
    /// Requests abandoned alone after a timeout, their connection kept open.
    pub cancelled: u64,
    pub port_range_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Closes {
    pub server: u64,
    pub client: u64,
    pub error: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub id: usize,
    pub connections: usize,
    pub max_in_flight: usize,
    pub scheduled: u64,
    pub sent: u64,
    pub send_ratio: Option<f64>,
    pub wall_s: f64,
    pub busy_share: f64,
    pub spin_share: f64,
    pub cpu_s: f64,
    pub runqueue_s: Option<f64>,
    pub runqueue_share: Option<f64>,
    pub involuntary_switches: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub skipped: bool,
    pub value: Option<f64>,
    pub limit: Option<f64>,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LagSummary {
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SendRate {
    pub scheduled: u64,
    pub sent: u64,
    pub ratio: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SlicePair {
    pub schedule: SliceStats,
    pub sends: SliceStats,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BurstinessSummary {
    pub slice_10ms: SlicePair,
    pub slice_50ms: SlicePair,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Admission {
    pub over_threshold: u64,
    pub share: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Fidelity {
    pub ok: bool,
    pub admission_bound: bool,
    pub checks: Vec<Check>,
    pub generator_lag: LagSummary,
    pub send_rate: SendRate,
    pub burstiness: BurstinessSummary,
    pub admission: Admission,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Outputs {
    pub summary: Option<String>,
    pub histogram_log: Option<String>,
    pub windows: Option<String>,
    pub windows_written: u64,
    pub trace: Option<String>,
    pub trace_rows: u64,
    pub trace_dropped: u64,
    pub trace_sample: u64,
    /// Address the live metrics endpoint listened on.
    pub metrics: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Distributed {
    pub agents: Vec<AgentSummary>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AgentSummary {
    pub address: String,
    pub host: HostInfo,
    pub protocol: String,
    /// Global worker ids run by this agent: [first, first + threads).
    pub first_worker: usize,
    pub threads: usize,
    /// Agent wall clock minus coordinator wall clock, estimated from the
    /// round trip with the smallest delay.
    pub clock_offset_ms: f64,
    /// That round trip; the offset is known to within half of it.
    pub rtt_ms: f64,
    pub warnings: Vec<String>,
    pub trace_rows: u64,
    /// The agent's CLOCK_MONOTONIC reading at T0. Comparable only between
    /// agents on the same host, where it shows how well T0 was aligned.
    pub t0_monotonic_ns: Option<u64>,
}

/// Exit codes.
pub mod exit {
    pub const OK: i32 = 0;
    pub const RUNTIME: i32 = 1;
    pub const CONFIG: i32 = 2;
    pub const FIDELITY: i32 = 3;
    pub const CONSERVATION: i32 = 4;
    pub const THRESHOLD: i32 = 5;
}
