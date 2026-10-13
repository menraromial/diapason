//! Live metrics: an OpenMetrics text endpoint, updated at every window.
//!
//! The aggregator publishes each window once every worker has reported it,
//! so the values trail real time by one to two windows. A small blocking
//! HTTP server answers `GET /metrics` (any path) from its own thread.

use crate::window::WindowSnapshot;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Default)]
struct Totals {
    windows: u64,
    end_s: f64,
    scheduled: u64,
    sent: u64,
    completed: u64,
    failed: u64,
    not_sent: u64,
    last: Option<WindowSnapshot>,
}

pub struct LiveMetrics {
    totals: Mutex<Totals>,
    labels: String,
}

fn q(h: &crate::hist::Hist, p: f64) -> f64 {
    if h.is_empty() {
        f64::NAN
    } else {
        h.value_at_quantile(p) as f64 / 1e9
    }
}

impl LiveMetrics {
    pub fn new(mode: &str, seed: u64) -> LiveMetrics {
        LiveMetrics {
            totals: Mutex::new(Totals::default()),
            labels: format!(
                "mode=\"{mode}\",seed=\"{seed}\",version=\"{}\"",
                env!("CARGO_PKG_VERSION")
            ),
        }
    }

    pub fn publish(&self, w: &WindowSnapshot) {
        let mut t = self.totals.lock().unwrap();
        t.windows += 1;
        t.end_s = w.end_s;
        t.scheduled += w.scheduled;
        t.sent += w.sent;
        t.completed += w.completed;
        t.failed += w.failed;
        t.not_sent += w.not_sent;
        t.last = Some(w.clone());
    }

    /// The exposition, in the OpenMetrics text format.
    pub fn render(&self) -> String {
        let t = self.totals.lock().unwrap();
        let mut o = String::new();
        let mut family = |name: &str, kind: &str, help: &str, samples: &[(String, f64)]| {
            o.push_str(&format!("# TYPE {name} {kind}\n# HELP {name} {help}\n"));
            for (suffix_labels, v) in samples {
                o.push_str(&format!("{name}{suffix_labels} {v}\n"));
            }
        };
        family(
            "diapason",
            "info",
            "Run identity.",
            &[(format!("_info{{{}}}", self.labels), 1.0)],
        );
        family(
            "diapason_window_end_seconds",
            "gauge",
            "End of the last published window, in seconds since T0.",
            &[(String::new(), t.end_s)],
        );
        for (name, help, v) in [
            (
                "diapason_requests_scheduled",
                "Requests scheduled, by intended time.",
                t.scheduled,
            ),
            (
                "diapason_requests_sent",
                "Requests written to a connection.",
                t.sent,
            ),
            (
                "diapason_requests_completed",
                "Requests that got a full response.",
                t.completed,
            ),
            (
                "diapason_requests_failed",
                "Requests sent that failed.",
                t.failed,
            ),
            (
                "diapason_requests_not_sent",
                "Requests never written.",
                t.not_sent,
            ),
        ] {
            family(name, "counter", help, &[("_total".to_string(), v as f64)]);
        }
        if let Some(w) = &t.last {
            family(
                "diapason_in_flight",
                "gauge",
                "Requests written and not finished in the last window.",
                &[
                    ("{stat=\"mean\"}".into(), w.in_flight_mean),
                    ("{stat=\"max\"}".into(), w.in_flight_max as f64),
                ],
            );
            family(
                "diapason_in_system",
                "gauge",
                "In-flight plus queued requests in the last window.",
                &[
                    ("{stat=\"mean\"}".into(), w.in_system_mean),
                    ("{stat=\"max\"}".into(), w.in_system_max as f64),
                ],
            );
            family(
                "diapason_connections_open",
                "gauge",
                "Most connections open during the last window.",
                &[("{stat=\"max\"}".into(), w.connections_open_max as f64)],
            );
            let mut lat = Vec::new();
            for (kind, h) in [
                ("corrected", &w.hists.corrected),
                ("server", &w.hists.server),
                ("connection_wait", &w.hists.conn_wait),
                ("generator_lag", &w.hists.gen_lag),
            ] {
                for p in [0.5, 0.9, 0.99, 0.999] {
                    lat.push((format!("{{kind=\"{kind}\",quantile=\"{p}\"}}"), q(h, p)));
                }
            }
            family(
                "diapason_window_latency_seconds",
                "gauge",
                "Latency quantiles of the last window.",
                &lat,
            );
        }
        o.push_str("# EOF\n");
        o
    }
}

/// The endpoint's thread.
pub struct LiveServer {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    pub addr: SocketAddr,
}

impl LiveServer {
    pub fn start(addr: &str, metrics: Arc<LiveMetrics>) -> std::io::Result<LiveServer> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let s = stop.clone();
        let thread = std::thread::Builder::new()
            .name("diapason-metrics".into())
            .spawn(move || {
                while !s.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut conn, _)) => {
                            let _ = conn.set_nonblocking(false);
                            let _ = conn.set_read_timeout(Some(Duration::from_secs(2)));
                            let mut buf = [0u8; 4096];
                            let _ = conn.read(&mut buf);
                            let body = metrics.render();
                            let _ = write!(
                                conn,
                                "HTTP/1.1 200 OK\r\nContent-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(20)),
                    }
                }
            })?;
        Ok(LiveServer {
            stop,
            thread: Some(thread),
            addr,
        })
    }
}

impl Drop for LiveServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
