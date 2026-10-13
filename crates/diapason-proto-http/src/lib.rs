//! HTTP for diapason. HTTP/1.1: keep-alive connections, optional pipelining,
//! and a response parser that handles Content-Length, chunked transfer
//! coding, bodiless responses and read-until-close bodies. HTTP/2 (crate
//! `h2`): one stream per request, up to `http.h2_max_streams` per connection,
//! over TLS with ALPN or over cleartext with prior knowledge. Both use TLS
//! from rustls and request templates per scenario.
//!
//! The parser keeps its state in the reader, so `read_response` can be
//! cancelled at any await point and resumed (the engine needs this to apply
//! request timeouts).

mod h2c;
mod parse;
mod tls;

use bytes::BytesMut;
use diapason_core::clock::raw_now_ns;
use diapason_core::config::{ConfigError, HttpVersion, RunConfig};
use diapason_core::protocol::{
    fail_kind_from_io, ConnectErrorKind, FailKind, Protocol, RequestCtx, RequestWriter, Response,
    ResponseReader,
};
use diapason_core::template::{Renderer, Template, TemplateEnv};
use rustls::pki_types::ServerName;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpSocket, TcpStream};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

pub use parse::ResponseParser;

/// One scenario's request, compiled.
struct ScenarioReq {
    head: bool,
    method: String,
    /// Headers without references, for HTTP/2.
    static_headers: Vec<(String, Vec<u8>)>,
    /// "METHOD " (with the trailing space).
    start: Vec<u8>,
    path: Template,
    /// " HTTP/1.1\r\n" followed by the fixed headers.
    fixed_headers: Vec<u8>,
    headers: Vec<(String, Template)>,
    body: Template,
    needs_length: bool,
    /// The whole request when nothing in it is templated.
    whole: Option<Vec<u8>>,
}

struct Shared {
    scenarios: Vec<ScenarioReq>,
    env: Arc<TemplateEnv>,
    any_head: bool,
    /// "http://host:port", the prefix of HTTP/2 request URIs.
    origin: String,
}

/// A request rendered into its parts, for HTTP/2.
#[derive(Default)]
struct Parts {
    path: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

fn render_parts(sc: &ScenarioReq, env: &TemplateEnv, r: &mut Renderer, seq: u64, p: &mut Parts) {
    r.start(env, seq);
    p.path.clear();
    env.render(&sc.path, r, &mut p.path);
    p.headers.clear();
    for (name, v) in &sc.headers {
        let mut out = Vec::new();
        env.render(v, r, &mut out);
        p.headers.push((name.clone(), out));
    }
    p.body.clear();
    env.render(&sc.body, r, &mut p.body);
}

struct Tls {
    connector: TlsConnector,
    name: ServerName<'static>,
}

pub struct HttpProtocol {
    addr: SocketAddr,
    shared: Arc<Shared>,
    tls: Option<Tls>,
    desc: String,
    h2_streams: Option<usize>,
}

fn valid_method(m: &str) -> bool {
    !m.is_empty() && m.bytes().all(|b| b.is_ascii_alphabetic())
}

impl HttpProtocol {
    /// Builds the protocol from `[target]`, `[[scenarios]]`, `[vars]`,
    /// `[data]` and `[tls]`. Resolves the host name once.
    pub fn from_config(cfg: &RunConfig) -> Result<HttpProtocol, ConfigError> {
        let t = &cfg.target;
        let url = url::Url::parse(&t.url)
            .map_err(|e| ConfigError(format!("target.url '{}': {e}", t.url)))?;
        let is_tls = match url.scheme() {
            "http" => false,
            "https" => true,
            s => return Err(ConfigError(format!("target.url: unsupported scheme '{s}'"))),
        };
        let host = url
            .host_str()
            .ok_or_else(|| ConfigError("target.url has no host".into()))?
            .to_string();
        let port = url.port_or_known_default().unwrap_or(80);
        let bare_host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let addr = (bare_host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| ConfigError(format!("dns: cannot resolve '{host}': {e}")))?
            .next()
            .ok_or_else(|| ConfigError(format!("dns: '{host}' has no address")))?;

        // Templates are compiled against an environment whose seed is set
        // once the run's seed is known (`set_seed`).
        let env = TemplateEnv::load(cfg, cfg.seed.unwrap_or(0))?;
        let mut default_path = url.path().to_string();
        if let Some(q) = url.query() {
            default_path.push('?');
            default_path.push_str(q);
        }
        let host_header = match url.port() {
            Some(p) => format!("{host}:{p}"),
            None => host.clone(),
        };

        struct Spec<'a> {
            name: &'a str,
            method: &'a str,
            path: &'a str,
            headers: Vec<(&'a String, &'a String)>,
            body: &'a str,
        }
        let mut specs = Vec::new();
        if cfg.scenarios.is_empty() {
            specs.push(Spec {
                name: "default",
                method: &t.method,
                path: &default_path,
                headers: t.headers.iter().collect(),
                body: &t.body,
            });
        } else {
            for s in &cfg.scenarios {
                let mut headers: Vec<(&String, &String)> = t
                    .headers
                    .iter()
                    .filter(|(k, _)| !s.headers.keys().any(|o| o.eq_ignore_ascii_case(k)))
                    .collect();
                headers.extend(s.headers.iter());
                specs.push(Spec {
                    name: &s.name,
                    method: s.method.as_deref().unwrap_or(&t.method),
                    path: s.path.as_deref().unwrap_or(&default_path),
                    headers,
                    body: s.body.as_deref().unwrap_or(&t.body),
                });
            }
        }

        let mut scenarios = Vec::new();
        for sp in specs {
            let e = |m: String| ConfigError(format!("scenario '{}': {m}", sp.name));
            let method = sp.method.to_ascii_uppercase();
            if !valid_method(&method) {
                return Err(e(format!("'{}' is not a valid method", sp.method)));
            }
            if !(sp.path.starts_with('/') || sp.path.starts_with("{{") || sp.path == "*") {
                return Err(e(format!("path '{}' must start with '/'", sp.path)));
            }
            let has = |name: &str| sp.headers.iter().any(|(k, _)| k.eq_ignore_ascii_case(name));
            let mut fixed = b" HTTP/1.1\r\n".to_vec();
            let mut static_headers = Vec::new();
            if !has("host") {
                fixed.extend_from_slice(format!("Host: {host_header}\r\n").as_bytes());
            }
            if !has("user-agent") {
                let ua = format!("diapason/{}", env!("CARGO_PKG_VERSION"));
                fixed.extend_from_slice(format!("User-Agent: {ua}\r\n").as_bytes());
                static_headers.push(("user-agent".to_string(), ua.into_bytes()));
            }
            let mut headers = Vec::new();
            for (k, v) in &sp.headers {
                if k.is_empty() || k.contains(['\r', '\n', ':', ' ']) || v.contains(['\r', '\n']) {
                    return Err(e(format!("invalid header '{k}'")));
                }
                let tpl = env.compile(v)?;
                match tpl.as_static() {
                    Some(b) => {
                        fixed.extend_from_slice(format!("{k}: ").as_bytes());
                        fixed.extend_from_slice(b);
                        fixed.extend_from_slice(b"\r\n");
                        static_headers.push((k.to_string(), b.to_vec()));
                    }
                    None => headers.push((k.to_string(), tpl)),
                }
            }
            let needs_length = !has("content-length")
                && (!sp.body.is_empty() || matches!(method.as_str(), "POST" | "PUT" | "PATCH"));
            let path = env.compile(sp.path)?;
            let body = env.compile(sp.body)?;
            let mut req = ScenarioReq {
                head: method == "HEAD",
                start: format!("{method} ").into_bytes(),
                method: method.clone(),
                static_headers,
                path,
                fixed_headers: fixed,
                headers,
                body,
                needs_length,
                whole: None,
            };
            if req.headers.is_empty()
                && req.path.as_static().is_some()
                && req.body.as_static().is_some()
            {
                let mut whole = Vec::new();
                build(
                    &req,
                    &env,
                    &mut Renderer::default(),
                    0,
                    &mut whole,
                    &mut Vec::new(),
                );
                req.whole = Some(whole);
            }
            scenarios.push(req);
        }

        let tls = if is_tls {
            let name = cfg
                .tls
                .server_name
                .clone()
                .unwrap_or_else(|| bare_host.clone());
            let name = ServerName::try_from(name.clone())
                .map_err(|_| ConfigError(format!("tls: '{name}' is not a valid server name")))?;
            let alpn: &[u8] = if cfg.http.version == HttpVersion::Http2 {
                b"h2"
            } else {
                b"http/1.1"
            };
            Some(Tls {
                connector: TlsConnector::from(tls::client_config(&cfg.tls, alpn)?),
                name,
            })
        } else {
            None
        };
        let any_head = scenarios.iter().any(|s| s.head);
        let h2 = cfg.http.version == HttpVersion::Http2;
        let scheme = if is_tls { "https" } else { "http" };
        let proto = match (h2, is_tls) {
            (false, false) => "http/1.1",
            (false, true) => "https/1.1",
            (true, false) => "h2c",
            (true, true) => "h2",
        };
        Ok(HttpProtocol {
            addr,
            shared: Arc::new(Shared {
                scenarios,
                env,
                any_head,
                origin: format!("{scheme}://{host_header}"),
            }),
            desc: format!("{proto} {addr}"),
            tls,
            h2_streams: h2.then_some(cfg.http.h2_max_streams),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The bytes of the request with sequence number `seq` in `scenario`.
    pub fn render(&self, scenario: u16, seq: u64) -> Vec<u8> {
        let mut out = Vec::new();
        let sc = &self.shared.scenarios[scenario as usize];
        build(
            sc,
            &self.shared.env,
            &mut Renderer::default(),
            seq,
            &mut out,
            &mut Vec::new(),
        );
        out
    }
}

/// Writes the request into `out`; `body` is scratch space.
fn build(
    sc: &ScenarioReq,
    env: &TemplateEnv,
    r: &mut Renderer,
    seq: u64,
    out: &mut Vec<u8>,
    body: &mut Vec<u8>,
) {
    out.clear();
    if let Some(w) = &sc.whole {
        out.extend_from_slice(w);
        return;
    }
    r.start(env, seq);
    out.extend_from_slice(&sc.start);
    env.render(&sc.path, r, out);
    out.extend_from_slice(&sc.fixed_headers);
    for (name, v) in &sc.headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        env.render(v, r, out);
        out.extend_from_slice(b"\r\n");
    }
    body.clear();
    env.render(&sc.body, r, body);
    if sc.needs_length {
        out.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
}

enum Rd {
    Plain(OwnedReadHalf),
    Tls(ReadHalf<TlsStream<TcpStream>>),
}

enum Wr {
    Plain(OwnedWriteHalf),
    Tls(WriteHalf<TlsStream<TcpStream>>),
}

/// The write half of an HTTP/1.1 or HTTP/2 connection.
pub enum HttpWriter {
    H1(H1Writer),
    H2(h2c::H2Writer),
}

/// The read half of an HTTP/1.1 or HTTP/2 connection.
pub enum HttpReader {
    H1(H1Reader),
    H2(h2c::H2Reader),
}

pub struct H1Writer {
    w: Wr,
    shared: Arc<Shared>,
    renderer: Renderer,
    buf: Vec<u8>,
    body: Vec<u8>,
    heads: Option<Rc<RefCell<VecDeque<bool>>>>,
}

pub struct H1Reader {
    r: Rd,
    buf: BytesMut,
    parser: ResponseParser,
    last_read_raw: u64,
    heads: Option<Rc<RefCell<VecDeque<bool>>>>,
    head_set: bool,
}

impl Protocol for HttpProtocol {
    type Writer = HttpWriter;
    type Reader = HttpReader;

    async fn connect(&self) -> Result<(HttpWriter, HttpReader), ConnectErrorKind> {
        let (w, r) = self.connect_h1_or_h2().await?;
        Ok((w, r))
    }

    fn max_outstanding(&self) -> Option<usize> {
        self.h2_streams
    }

    fn describe(&self) -> String {
        self.desc.clone()
    }

    fn set_seed(&self, seed: u64) {
        self.shared.env.set_seed(seed);
    }

    fn report(&self) -> serde_json::Value {
        let wrapped: serde_json::Map<String, serde_json::Value> = self
            .shared
            .env
            .wrapped()
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::from(v)))
            .collect();
        if wrapped.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::json!({ "data_wrapped": wrapped })
        }
    }
}

impl HttpProtocol {
    async fn connect_h1_or_h2(&self) -> Result<(HttpWriter, HttpReader), ConnectErrorKind> {
        let sock = if self.addr.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(|e| ConnectErrorKind::from_io(&e))?;
        let stream = sock
            .connect(self.addr)
            .await
            .map_err(|e| ConnectErrorKind::from_io(&e))?;
        let _ = stream.set_nodelay(true);
        let h2 = self.h2_streams.is_some();
        let (r, w) = match &self.tls {
            None if h2 => {
                let (w, r) = h2c::handshake(stream, self.shared.clone()).await?;
                return Ok((HttpWriter::H2(w), HttpReader::H2(r)));
            }
            None => {
                let (r, w) = stream.into_split();
                (Rd::Plain(r), Wr::Plain(w))
            }
            Some(t) => {
                let s = t
                    .connector
                    .connect(t.name.clone(), stream)
                    .await
                    .map_err(|_| ConnectErrorKind::Tls)?;
                if h2 {
                    if s.get_ref().1.alpn_protocol() != Some(b"h2") {
                        // The server did not agree to HTTP/2.
                        return Err(ConnectErrorKind::Tls);
                    }
                    let (w, r) = h2c::handshake(s, self.shared.clone()).await?;
                    return Ok((HttpWriter::H2(w), HttpReader::H2(r)));
                }
                let (r, w) = tokio::io::split(s);
                (Rd::Tls(r), Wr::Tls(w))
            }
        };
        let heads = self
            .shared
            .any_head
            .then(|| Rc::new(RefCell::new(VecDeque::new())));
        Ok((
            HttpWriter::H1(H1Writer {
                w,
                shared: self.shared.clone(),
                renderer: Renderer::default(),
                buf: Vec::with_capacity(512),
                body: Vec::new(),
                heads: heads.clone(),
            }),
            HttpReader::H1(H1Reader {
                r,
                buf: BytesMut::with_capacity(16 * 1024),
                parser: ResponseParser::new(false),
                last_read_raw: 0,
                heads,
                head_set: false,
            }),
        ))
    }
}

impl RequestWriter for HttpWriter {
    async fn write_request(&mut self, req: &RequestCtx) -> Result<(), FailKind> {
        match self {
            HttpWriter::H1(w) => w.write_request(req).await,
            HttpWriter::H2(w) => w.write_request(req).await,
        }
    }
}

impl ResponseReader for HttpReader {
    async fn read_response(&mut self) -> Result<Option<Response>, FailKind> {
        match self {
            HttpReader::H1(r) => r.read_response().await,
            HttpReader::H2(r) => r.read_response().await,
        }
    }

    fn cancel(&mut self, seq: u64) -> bool {
        match self {
            HttpReader::H1(_) => false,
            HttpReader::H2(r) => r.cancel(seq),
        }
    }
}

impl H1Writer {
    async fn write_request(&mut self, req: &RequestCtx) -> Result<(), FailKind> {
        let sc = &self.shared.scenarios[req.scenario as usize];
        if let Some(h) = &self.heads {
            h.borrow_mut().push_back(sc.head);
        }
        let bytes: &[u8] = match &sc.whole {
            Some(w) => w,
            None => {
                build(
                    sc,
                    &self.shared.env,
                    &mut self.renderer,
                    req.seq,
                    &mut self.buf,
                    &mut self.body,
                );
                &self.buf
            }
        };
        let r = match &mut self.w {
            Wr::Plain(w) => w.write_all(bytes).await,
            Wr::Tls(w) => match w.write_all(bytes).await {
                Ok(()) => w.flush().await,
                Err(e) => Err(e),
            },
        };
        r.map_err(|e| fail_kind_from_io(&e, true))
    }
}

impl H1Reader {
    async fn read_response(&mut self) -> Result<Option<Response>, FailKind> {
        loop {
            // The writer records each request's method before writing it, so
            // the flag for this response exists once its first byte is here.
            if !self.head_set && !self.buf.is_empty() {
                if let Some(h) = &self.heads {
                    let head = h.borrow_mut().pop_front().unwrap_or(false);
                    self.parser.set_head(head);
                }
                self.head_set = true;
            }
            if self.parser.at_start() && !self.buf.is_empty() {
                // Bytes of this response arrived with the previous read.
                self.parser.set_first_byte(self.last_read_raw);
            }
            if let Some(r) = self.parser.step(&mut self.buf)? {
                self.head_set = false;
                return Ok(Some(r));
            }
            if self.buf.capacity() - self.buf.len() < 4096 {
                self.buf.reserve(16 * 1024);
            }
            let n = match &mut self.r {
                Rd::Plain(r) => r.read_buf(&mut self.buf).await,
                Rd::Tls(r) => r.read_buf(&mut self.buf).await,
            }
            .map_err(|e| fail_kind_from_io(&e, false))?;
            self.last_read_raw = raw_now_ns();
            if n == 0 {
                return self.parser.eof();
            }
            self.parser.set_first_byte(self.last_read_raw);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proto(extra: &str) -> HttpProtocol {
        let cfg = RunConfig::from_toml(&format!(
            "seed = 5\n[target]\nurl = \"http://127.0.0.1:8080/base?x=1\"\nheaders = {{ \"x-a\" = \"1\" }}\n\
             [load]\n[[load.phases]]\nname = \"m\"\nduration = \"1s\"\nrate = 1\n{extra}"
        ))
        .unwrap();
        cfg.validate().unwrap();
        HttpProtocol::from_config(&cfg).unwrap()
    }

    #[test]
    fn static_request() {
        let p = proto("");
        let r = String::from_utf8(p.render(0, 0)).unwrap();
        assert!(
            r.starts_with("GET /base?x=1 HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n"),
            "{r}"
        );
        assert!(r.contains("x-a: 1\r\n"));
        assert!(r.ends_with("\r\n\r\n"));
    }

    #[test]
    fn templated_scenarios() {
        let p = proto(
            "[vars]\nid = { dist = \"uniform\", min = 10, max = 99 }\n\
             [[scenarios]]\nname = \"get\"\npath = \"/item/{{id}}\"\nheaders = { \"x-seq\" = \"{{seq}}\" }\n\
             [[scenarios]]\nname = \"post\"\nmethod = \"post\"\npath = \"/item\"\nbody = \"{\\\"id\\\": {{id}}}\"\n\
             headers = { \"x-a\" = \"2\" }\n",
        );
        let get = String::from_utf8(p.render(0, 7)).unwrap();
        let post = String::from_utf8(p.render(1, 7)).unwrap();
        let id = get
            .split('/')
            .nth(2)
            .unwrap()
            .split(' ')
            .next()
            .unwrap()
            .to_string();
        assert!(get.contains("x-seq: 7\r\n"));
        assert!(get.contains("x-a: 1\r\n"));
        assert!(post.starts_with("POST /item HTTP/1.1\r\n"));
        assert!(post.contains("x-a: 2\r\n") && !post.contains("x-a: 1"));
        let body = format!("{{\"id\": {id}}}");
        assert!(
            post.ends_with(&format!("Content-Length: {}\r\n\r\n{body}", body.len())),
            "{post}"
        );
        p.set_seed(6);
        let other: Vec<Vec<u8>> = (0..20).map(|s| p.render(0, s)).collect();
        p.set_seed(5);
        let again: Vec<Vec<u8>> = (0..20).map(|s| p.render(0, s)).collect();
        assert_ne!(other, again);
        assert_eq!(get.as_bytes(), p.render(0, 7));
    }

    #[test]
    fn rejects_bad_scenarios() {
        let bad = |extra: &str| {
            let cfg = RunConfig::from_toml(&format!(
                "[target]\nurl = \"http://127.0.0.1:8080/\"\n[load]\n[[load.phases]]\nname = \"m\"\nduration = \"1s\"\nrate = 1\n{extra}"
            ))
            .unwrap();
            HttpProtocol::from_config(&cfg).is_err()
        };
        assert!(bad("[[scenarios]]\nname = \"a\"\npath = \"nope\"\n"));
        assert!(bad("[[scenarios]]\nname = \"a\"\nmethod = \"G T\"\n"));
        assert!(bad(
            "[[scenarios]]\nname = \"a\"\npath = \"/{{missing}}\"\n"
        ));
    }
}
