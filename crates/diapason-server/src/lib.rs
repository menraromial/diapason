//! The diapason reference server.
//!
//! It models an M/G/c queue in virtual time: `workers` servers, each request
//! assigned on arrival to the server that frees first, with a service time
//! drawn from a distribution. Its response is written when its service ends.
//! The capacity is therefore μ = workers / E[S], independent of the CPU the
//! server process gets. `workers = 0` answers at once.

use bytes::{Buf, BytesMut};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Exp, LogNormal};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

mod timer;

/// A service-time distribution, in seconds.
#[derive(Clone, Debug, PartialEq)]
pub enum Service {
    Const(f64),
    Exp(f64),
    Uniform(f64, f64),
    /// Mean and sigma of the underlying normal.
    LogNormal(f64, f64),
}

fn parse_secs(s: &str) -> Result<f64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .ok_or_else(|| format!("'{s}' needs a unit (us, ms, s)"))?;
    let v: f64 = s[..split]
        .parse()
        .map_err(|_| format!("'{s}' is not a duration"))?;
    let scale = match &s[split..] {
        "ns" => 1e-9,
        "us" => 1e-6,
        "ms" => 1e-3,
        "s" => 1.0,
        u => return Err(format!("unknown unit '{u}'")),
    };
    Ok(v * scale)
}

impl Service {
    /// Parses "const:1ms", "exp:1ms", "uniform:1ms:3ms", "lognormal:1ms:0.5".
    pub fn parse(s: &str) -> Result<Service, String> {
        let parts: Vec<&str> = s.split(':').collect();
        let svc = match parts.as_slice() {
            ["const", d] => Service::Const(parse_secs(d)?),
            ["exp", d] => Service::Exp(parse_secs(d)?),
            ["uniform", a, b] => Service::Uniform(parse_secs(a)?, parse_secs(b)?),
            ["lognormal", m, sigma] => Service::LogNormal(
                parse_secs(m)?,
                sigma.parse().map_err(|_| format!("bad sigma '{sigma}'"))?,
            ),
            _ => return Err(format!("unknown service distribution '{s}'")),
        };
        if svc.mean().is_nan() || svc.mean() <= 0.0 {
            return Err(format!(
                "service distribution '{s}' must have a positive mean"
            ));
        }
        Ok(svc)
    }

    pub fn mean(&self) -> f64 {
        match *self {
            Service::Const(d) | Service::Exp(d) | Service::LogNormal(d, _) => d,
            Service::Uniform(a, b) => (a + b) / 2.0,
        }
    }

    pub fn sample(&self, rng: &mut impl Rng) -> f64 {
        match *self {
            Service::Const(d) => d,
            Service::Exp(m) => Exp::new(1.0 / m).map(|e| e.sample(rng)).unwrap_or(m),
            Service::Uniform(a, b) => {
                if b > a {
                    rng.gen_range(a..b)
                } else {
                    a
                }
            }
            Service::LogNormal(mean, sigma) => {
                let mu = mean.ln() - sigma * sigma / 2.0;
                LogNormal::new(mu, sigma)
                    .map(|l| l.sample(rng))
                    .unwrap_or(mean)
            }
        }
    }
}

/// Service times are multiplied by `factor` for requests whose service
/// starts in [at, at + for), measured from server start.
#[derive(Clone, Debug, PartialEq)]
pub struct Slowdown {
    pub at: f64,
    pub dur: f64,
    pub factor: f64,
}

impl Slowdown {
    /// Parses "10s:5s:3".
    pub fn parse(s: &str) -> Result<Slowdown, String> {
        let p: Vec<&str> = s.split(':').collect();
        if p.len() != 3 {
            return Err(format!("slowdown '{s}' must be at:for:factor"));
        }
        Ok(Slowdown {
            at: parse_secs(p[0])?,
            dur: parse_secs(p[1])?,
            factor: p[2].parse().map_err(|_| format!("bad factor '{}'", p[2]))?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub workers: usize,
    pub service: Service,
    pub slowdowns: Vec<Slowdown>,
    pub reset_prob: f64,
    pub close_prob: f64,
    pub stall_prob: f64,
    pub body_size: usize,
    pub threads: usize,
    pub seed: u64,
    pub tls: Option<ServerTls>,
}

/// TLS for the reference server: PEM certificate chain and key, and an
/// optional CA that client certificates must chain to.
#[derive(Clone, Debug)]
pub struct ServerTls {
    pub cert: std::path::PathBuf,
    pub key: std::path::PathBuf,
    pub client_ca: Option<std::path::PathBuf>,
}

fn load_pem(tls: &ServerTls) -> io::Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::CertificateDer;
    let certs = |p: &std::path::Path| -> io::Result<Vec<CertificateDer<'static>>> {
        let f = std::fs::File::open(p)?;
        rustls_pemfile::certs(&mut io::BufReader::new(f)).collect()
    };
    let chain = certs(&tls.cert)?;
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(std::fs::File::open(&tls.key)?))?
        .ok_or_else(|| io::Error::other("no private key"))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?;
    let builder = match &tls.client_ca {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for c in certs(ca)? {
                roots.add(c).map_err(io::Error::other)?;
            }
            let v = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .map_err(io::Error::other)?;
            builder.with_client_cert_verifier(v)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(io::Error::other)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen: "127.0.0.1:8080".parse().unwrap(),
            workers: 0,
            service: Service::Const(0.001),
            slowdowns: Vec::new(),
            reset_prob: 0.0,
            close_prob: 0.0,
            stall_prob: 0.0,
            body_size: 2,
            threads: 1,
            seed: 1,
            tls: None,
        }
    }
}

impl ServerConfig {
    /// μ = workers / E[S]; infinite when workers = 0.
    pub fn capacity(&self) -> f64 {
        if self.workers == 0 {
            f64::INFINITY
        } else {
            self.workers as f64 / self.service.mean()
        }
    }

    /// Workers needed for capacity μ with the configured service time.
    pub fn workers_for(capacity: f64, service: &Service) -> usize {
        ((capacity * service.mean()).round() as usize).max(1)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ServerStats {
    pub connections: u64,
    pub requests: u64,
    pub responses: u64,
    pub resets: u64,
    pub stalls: u64,
    pub closes: u64,
    pub bad_requests: u64,
    pub tls_errors: u64,
}

#[derive(Default)]
struct Counters {
    connections: AtomicU64,
    requests: AtomicU64,
    responses: AtomicU64,
    resets: AtomicU64,
    stalls: AtomicU64,
    closes: AtomicU64,
    bad_requests: AtomicU64,
    tls_errors: AtomicU64,
}

fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out-pointer.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

struct Model {
    timer: timer::PreciseTimer,
    h2_body: bytes::Bytes,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    cfg: ServerConfig,
    start: u64,
    free: Mutex<BinaryHeap<Reverse<u64>>>,
    counters: Counters,
    ok: Vec<u8>,
    ok_close: Vec<u8>,
}

impl Drop for Model {
    fn drop(&mut self) {
        self.timer.stop();
    }
}

impl Model {
    fn new(cfg: ServerConfig) -> io::Result<Model> {
        let start = now_ns();
        let free = (0..cfg.workers).map(|_| Reverse(start)).collect();
        let ok = response(200, cfg.body_size, false);
        let ok_close = response(200, cfg.body_size, true);
        Ok(Model {
            timer: timer::PreciseTimer::start(),
            h2_body: bytes::Bytes::from("x".repeat(cfg.body_size)),
            acceptor: match &cfg.tls {
                Some(t) => Some(load_pem(t)?),
                None => None,
            },
            cfg,
            start,
            free: Mutex::new(free),
            counters: Counters::default(),
            ok,
            ok_close,
        })
    }

    fn factor(&self, t: u64) -> f64 {
        let s = t.saturating_sub(self.start) as f64 / 1e9;
        self.cfg
            .slowdowns
            .iter()
            .filter(|d| s >= d.at && s < d.at + d.dur)
            .map(|d| d.factor)
            .product()
    }

    /// Time at which a request arriving at `arrival` finishes service.
    fn assign(&self, arrival: u64, rng: &mut ChaCha8Rng) -> u64 {
        if self.cfg.workers == 0 {
            return arrival;
        }
        let sample = self.cfg.service.sample(rng);
        let mut heap = self.free.lock().unwrap();
        let Reverse(f) = heap.pop().expect("workers > 0");
        let start = arrival.max(f);
        let done = start + (sample * self.factor(start) * 1e9) as u64;
        heap.push(Reverse(done));
        done
    }

    fn stats(&self) -> ServerStats {
        let c = &self.counters;
        ServerStats {
            connections: c.connections.load(Relaxed),
            requests: c.requests.load(Relaxed),
            responses: c.responses.load(Relaxed),
            resets: c.resets.load(Relaxed),
            stalls: c.stalls.load(Relaxed),
            closes: c.closes.load(Relaxed),
            bad_requests: c.bad_requests.load(Relaxed),
            tls_errors: c.tls_errors.load(Relaxed),
        }
    }
}

enum Parsed {
    /// Length of the request, and the status asked for by a `/status/NNN` path.
    Request(usize, u16, bool),
    Partial,
    Bad,
}

fn parse_request(buf: &[u8]) -> Parsed {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    match req.parse(buf) {
        Ok(httparse::Status::Complete(n)) => {
            let status = req
                .path
                .and_then(|p| p.strip_prefix("/status/"))
                .and_then(|p| p.get(..3))
                .and_then(|c| c.parse::<u16>().ok())
                .filter(|c| (200..=599).contains(c))
                .unwrap_or(200);
            let mut body = 0usize;
            for h in req.headers.iter() {
                if h.name.eq_ignore_ascii_case("content-length") {
                    match std::str::from_utf8(h.value)
                        .ok()
                        .and_then(|v| v.trim().parse().ok())
                    {
                        Some(v) => body = v,
                        None => return Parsed::Bad,
                    }
                } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
                    return Parsed::Bad;
                }
            }
            if buf.len() < n + body {
                Parsed::Partial
            } else {
                Parsed::Request(n + body, status, req.method == Some("HEAD"))
            }
        }
        Ok(httparse::Status::Partial) => Parsed::Partial,
        Err(_) => Parsed::Bad,
    }
}

/// A server-side connection: plain TCP or TLS.
trait Conn: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {
    /// Makes the next close send a reset.
    fn arm_reset(&self);
}

impl Conn for TcpStream {
    fn arm_reset(&self) {
        // A zero linger makes close() send a reset; it does not block.
        #[allow(deprecated)]
        let _ = self.set_linger(Some(Duration::ZERO));
    }
}

impl Conn for tokio_rustls::server::TlsStream<TcpStream> {
    fn arm_reset(&self) {
        self.get_ref().0.arm_reset();
    }
}

fn response(code: u16, body_size: usize, close: bool) -> Vec<u8> {
    let reason = match code {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Length: {body_size}\r\n{}\r\n{}",
        if close { "Connection: close\r\n" } else { "" },
        "x".repeat(body_size)
    )
    .into_bytes()
}

async fn handle<S: Conn>(mut stream: S, m: Arc<Model>, mut rng: ChaCha8Rng) {
    let slot = Arc::new(timer::TimerSlot::default());
    let c = &m.counters;
    let mut buf = BytesMut::with_capacity(8192);
    let mut queue: VecDeque<(u64, bool, u16, bool)> = VecDeque::new();
    let mut out = Vec::with_capacity(4096);
    let (mut stalled, mut closing, mut peer_closed) = (false, false, false);
    loop {
        while !stalled && !closing {
            match parse_request(&buf) {
                Parsed::Partial => break,
                Parsed::Bad => {
                    c.bad_requests.fetch_add(1, Relaxed);
                    let _ = stream
                        .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                    return;
                }
                Parsed::Request(n, status, head) => {
                    buf.advance(n);
                    let now = now_ns();
                    c.requests.fetch_add(1, Relaxed);
                    let u: f64 = rng.gen();
                    let cfg = &m.cfg;
                    if u < cfg.reset_prob {
                        c.resets.fetch_add(1, Relaxed);
                        stream.arm_reset();
                        return;
                    }
                    if u < cfg.reset_prob + cfg.stall_prob {
                        c.stalls.fetch_add(1, Relaxed);
                        stalled = true;
                        break;
                    }
                    let close = u < cfg.reset_prob + cfg.stall_prob + cfg.close_prob;
                    queue.push_back((m.assign(now, &mut rng), close, status, head));
                    if close {
                        closing = true;
                    }
                }
            }
        }
        if stalled {
            buf.clear();
        }
        let front = queue.front().map(|q| q.0);
        if front.is_none() && (peer_closed || closing) {
            return;
        }
        // Write at once what is due; otherwise wait for the first deadline
        // or for more bytes.
        let ready = match front {
            Some(d) if d <= now_ns() => true,
            _ => {
                let sleep = m.timer.sleep_until(&slot, front.unwrap_or(u64::MAX));
                tokio::select! {
                    biased;
                    _ = sleep, if front.is_some() => true,
                    r = stream.read_buf(&mut buf), if !peer_closed => {
                        match r {
                            Ok(0) => {
                                peer_closed = true;
                                if stalled {
                                    return;
                                }
                            }
                            Ok(_) => {}
                            Err(_) => return,
                        }
                        false
                    }
                }
            }
        };
        if ready {
            {
                let now = now_ns();
                out.clear();
                let mut close_after = false;
                while let Some(&(d, close, status, head)) = queue.front() {
                    if d > now {
                        break;
                    }
                    queue.pop_front();
                    if status == 200 && !head {
                        out.extend_from_slice(if close { &m.ok_close } else { &m.ok });
                    } else {
                        let mut r = response(status, m.cfg.body_size, close);
                        if head {
                            r.truncate(r.len() - m.cfg.body_size);
                        }
                        out.extend_from_slice(&r);
                    }
                    c.responses.fetch_add(1, Relaxed);
                    if close {
                        close_after = true;
                        break;
                    }
                }
                if stream.write_all(&out).await.is_err() || stream.flush().await.is_err() {
                    return;
                }
                if close_after {
                    c.closes.fetch_add(1, Relaxed);
                    return;
                }
            }
        }
    }
}

/// Waits for the first bytes of a cleartext connection and tells whether
/// they start the HTTP/2 connection preface ("PRI * HTTP/2.0").
async fn is_h2_preface(stream: &TcpStream) -> bool {
    let mut buf = [0u8; 4];
    loop {
        match stream.peek(&mut buf).await {
            Ok(0) | Err(_) => return false,
            Ok(n) if n >= 4 => return &buf == b"PRI ",
            Ok(n) => {
                if !b"PRI ".starts_with(&buf[..n]) {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

/// Serves HTTP/2: each stream gets its response when its service ends.
/// `--close-prob` does not apply; `--reset-prob` resets the stream and
/// `--stall-prob` leaves it without an answer.
async fn handle_h2<S>(io: S, m: Arc<Model>, mut rng: ChaCha8Rng)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = match h2::server::Builder::new()
        .max_concurrent_streams(10_000)
        .initial_window_size(1 << 20)
        .initial_connection_window_size(1 << 24)
        .handshake::<_, bytes::Bytes>(io)
        .await
    {
        Ok(c) => c,
        Err(_) => return,
    };
    let c = &m.counters;
    let mut stalled = Vec::new();
    while let Some(r) = conn.accept().await {
        let Ok((req, mut respond)) = r else { return };
        let now = now_ns();
        c.requests.fetch_add(1, Relaxed);
        let u: f64 = rng.gen();
        let cfg = &m.cfg;
        if u < cfg.reset_prob {
            c.resets.fetch_add(1, Relaxed);
            respond.send_reset(h2::Reason::INTERNAL_ERROR);
            continue;
        }
        if u < cfg.reset_prob + cfg.stall_prob {
            c.stalls.fetch_add(1, Relaxed);
            stalled.push(respond);
            continue;
        }
        let status = req
            .uri()
            .path()
            .strip_prefix("/status/")
            .and_then(|p| p.get(..3))
            .and_then(|p| p.parse::<u16>().ok())
            .filter(|c| (200..=599).contains(c))
            .unwrap_or(200);
        let head = req.method() == http::Method::HEAD;
        let done = m.assign(now, &mut rng);
        let m = m.clone();
        let mut body = req.into_body();
        tokio::spawn(async move {
            while let Some(Ok(d)) = body.data().await {
                let _ = body.flow_control().release_capacity(d.len());
            }
            if done > now_ns() {
                m.timer
                    .sleep_until(&Arc::new(timer::TimerSlot::default()), done)
                    .await;
            }
            let resp = http::Response::builder()
                .status(status)
                .header("content-length", m.cfg.body_size)
                .body(())
                .expect("valid response");
            let end = head || m.cfg.body_size == 0;
            if let Ok(mut s) = respond.send_response(resp, end) {
                if !end {
                    let _ = s.send_data(m.h2_body.clone(), true);
                }
                m.counters.responses.fetch_add(1, Relaxed);
            }
        });
    }
}

async fn serve(listener: TcpListener, m: Arc<Model>, shutdown: impl Future<Output = ()>) {
    tokio::pin!(shutdown);
    let acceptor = m.acceptor.clone();
    let mut conn_id = 0u64;
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            r = listener.accept() => {
                match r {
                    Ok((stream, _)) => {
                        conn_id += 1;
                        m.counters.connections.fetch_add(1, Relaxed);
                        let mut rng = ChaCha8Rng::seed_from_u64(m.cfg.seed);
                        rng.set_stream(conn_id);
                        match acceptor.clone() {
                            None => {
                                let m = m.clone();
                                tokio::spawn(async move {
                                    if is_h2_preface(&stream).await {
                                        let _ = stream.set_nodelay(true);
                                        handle_h2(stream, m, rng).await;
                                    } else {
                                        handle(stream, m, rng).await;
                                    }
                                });
                            }
                            Some(acc) => {
                                let m = m.clone();
                                tokio::spawn(async move {
                                    let _ = stream.set_nodelay(true);
                                    match acc.accept(stream).await {
                                        Ok(s) if s.get_ref().1.alpn_protocol() == Some(b"h2") => {
                                            handle_h2(s, m, rng).await
                                        }
                                        Ok(s) => handle(s, m, rng).await,
                                        Err(_) => {
                                            m.counters.tls_errors.fetch_add(1, Relaxed);
                                        }
                                    }
                                });
                            }
                        }
                    }
                    Err(_) => {
                        // Out of descriptors or similar: back off briefly.
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
        }
    }
}

fn runtime(threads: usize) -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .thread_name("diapason-server")
        .enable_all()
        .build()
}

fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    let sock = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    sock.set_reuseaddr(true)?;
    sock.bind(addr)?;
    sock.listen(65_535)
}

/// Raises RLIMIT_NOFILE to the hard limit, so that many connections fit.
fn raise_nofile() {
    let mut r = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: valid pointers.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) == 0 && r.rlim_cur < r.rlim_max {
            r.rlim_cur = r.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &r);
        }
    }
}

/// Runs the server until SIGINT or SIGTERM. `on_listen` receives the bound
/// address before the first connection is accepted.
pub fn run_blocking(
    cfg: ServerConfig,
    on_listen: impl FnOnce(SocketAddr),
) -> io::Result<ServerStats> {
    raise_nofile();
    let rt = runtime(cfg.threads)?;
    rt.block_on(async move {
        let listen = cfg.listen;
        let m = Arc::new(Model::new(cfg)?);
        let listener = bind(listen)?;
        on_listen(listener.local_addr()?);
        let shutdown = async {
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
        };
        serve(listener, m.clone(), shutdown).await;
        Ok(m.stats())
    })
}

/// A server running on a background thread, for tests and `selftest`.
pub struct ServerHandle {
    pub addr: SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<ServerStats>>,
}

impl ServerHandle {
    pub fn stop(mut self) -> ServerStats {
        self.shutdown()
    }

    fn shutdown(&mut self) -> ServerStats {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        self.thread
            .take()
            .and_then(|t| t.join().ok())
            .unwrap_or_default()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub fn spawn(cfg: ServerConfig) -> io::Result<ServerHandle> {
    raise_nofile();
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("diapason-server-main".into())
        .spawn(move || {
            let rt = match runtime(cfg.threads) {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = addr_tx.send(Err(e));
                    return ServerStats::default();
                }
            };
            rt.block_on(async move {
                let listener = match bind(cfg.listen) {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = addr_tx.send(Err(e));
                        return ServerStats::default();
                    }
                };
                let m = match Model::new(cfg) {
                    Ok(m) => Arc::new(m),
                    Err(e) => {
                        let _ = addr_tx.send(Err(e));
                        return ServerStats::default();
                    }
                };
                let _ = addr_tx.send(listener.local_addr());
                serve(listener, m.clone(), async {
                    let _ = stop_rx.await;
                })
                .await;
                m.stats()
            })
        })?;
    let addr = addr_rx
        .recv()
        .map_err(|_| io::Error::other("server thread exited"))??;
    Ok(ServerHandle {
        addr,
        stop: Some(stop_tx),
        thread: Some(thread),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_distributions() {
        assert_eq!(Service::parse("const:2ms").unwrap(), Service::Const(0.002));
        assert_eq!(Service::parse("uniform:1ms:3ms").unwrap().mean(), 0.002);
        assert!(Service::parse("exp:0ms").is_err());
        assert_eq!(
            Slowdown::parse("10s:5s:3").unwrap(),
            Slowdown {
                at: 10.0,
                dur: 5.0,
                factor: 3.0
            }
        );
        assert_eq!(
            ServerConfig::workers_for(20_000.0, &Service::Const(0.001)),
            20
        );
    }

    #[test]
    fn virtual_time_capacity() {
        // 2 workers, 10 ms each: 100 requests arriving at once finish over 0.5 s.
        let cfg = ServerConfig {
            workers: 2,
            service: Service::Const(0.010),
            ..Default::default()
        };
        let m = Model::new(cfg).unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let t = m.start;
        let last = (0..100).map(|_| m.assign(t, &mut rng)).max().unwrap();
        assert_eq!(last - t, 500_000_000);
    }
}
