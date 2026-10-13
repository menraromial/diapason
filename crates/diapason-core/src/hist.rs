//! Histogram helpers. Values are recorded in nanoseconds.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

pub type Hist = Histogram<u64>;

/// A histogram with 3 significant digits that grows as needed.
pub fn new_hist() -> Hist {
    let mut h = Hist::new(3).expect("3 significant digits is valid");
    h.auto(true);
    h
}

#[inline]
pub fn record(h: &mut Hist, ns: u64) {
    // auto-resizing histograms only fail on values above 2^63.
    let _ = h.record(ns.min(i64::MAX as u64));
}

/// The four per-request delays, as histograms.
#[derive(Clone, Debug)]
pub struct DelayHists {
    pub corrected: Hist,
    pub server: Hist,
    pub conn_wait: Hist,
    pub gen_lag: Hist,
}

/// Serialised as four base64 strings of the HdrHistogram V2 compressed
/// encoding, the form agents send to the coordinator.
fn encode(h: &Hist) -> String {
    use base64::Engine;
    use hdrhistogram::serialization::{Serializer as _, V2DeflateSerializer};
    let mut buf = Vec::new();
    V2DeflateSerializer::new()
        .serialize(h, &mut buf)
        .expect("histograms serialise into memory");
    base64::engine::general_purpose::STANDARD.encode(buf)
}

fn decode(s: &str) -> Result<Hist, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| e.to_string())?;
    let mut h: Hist = hdrhistogram::serialization::Deserializer::new()
        .deserialize(&mut &bytes[..])
        .map_err(|e| format!("{e:?}"))?;
    h.auto(true);
    Ok(h)
}

impl Serialize for DelayHists {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        [
            &self.corrected,
            &self.server,
            &self.conn_wait,
            &self.gen_lag,
        ]
        .map(encode)
        .serialize(s)
    }
}

impl<'de> Deserialize<'de> for DelayHists {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let [a, b, c, e] = <[String; 4]>::deserialize(d)?;
        let f = |x: &str| decode(x).map_err(serde::de::Error::custom);
        Ok(DelayHists {
            corrected: f(&a)?,
            server: f(&b)?,
            conn_wait: f(&c)?,
            gen_lag: f(&e)?,
        })
    }
}

impl DelayHists {
    pub fn new() -> DelayHists {
        DelayHists {
            corrected: new_hist(),
            server: new_hist(),
            conn_wait: new_hist(),
            gen_lag: new_hist(),
        }
    }

    pub fn add(&mut self, o: &DelayHists) {
        let _ = self.corrected.add(&o.corrected);
        let _ = self.server.add(&o.server);
        let _ = self.conn_wait.add(&o.conn_wait);
        let _ = self.gen_lag.add(&o.gen_lag);
    }
}

impl Default for DelayHists {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary statistics of one histogram, in milliseconds.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LatencyStats {
    pub count: u64,
    pub min_ms: f64,
    pub mean_ms: f64,
    pub stdev_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p99_9_ms: f64,
    pub p99_99_ms: f64,
    pub max_ms: f64,
}

const MS: f64 = 1e6;

impl LatencyStats {
    pub fn from_hist(h: &Hist) -> LatencyStats {
        if h.is_empty() {
            return LatencyStats::default();
        }
        let q = |p: f64| h.value_at_quantile(p) as f64 / MS;
        LatencyStats {
            count: h.len(),
            min_ms: h.min() as f64 / MS,
            mean_ms: h.mean() / MS,
            stdev_ms: h.stdev() / MS,
            p50_ms: q(0.5),
            p90_ms: q(0.9),
            p99_ms: q(0.99),
            p99_9_ms: q(0.999),
            p99_99_ms: q(0.9999),
            max_ms: h.max() as f64 / MS,
        }
    }
}
