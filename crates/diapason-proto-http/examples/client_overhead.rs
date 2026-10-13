//! Client CPU cost per request: diapason's HTTP/1.1 writer and parser
//! against hyper's client connection API.
//!
//! One current-thread runtime drives N keep-alive connections in a closed
//! loop against the in-process reference server (answers at once, on its own
//! threads). The client thread's CPU time divided by the number of responses
//! is the client's cost per request, independent of the server's speed.
//!
//!     cargo run --release -p diapason-proto-http --example client_overhead [conns] [secs]

use diapason_core::protocol::{Protocol, RequestCtx, RequestWriter, ResponseReader};
use diapason_core::RunConfig;
use diapason_proto_http::HttpProtocol;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out-pointer.
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn report(name: &str, n: u64, cpu_ns: u64, wall: Duration) {
    println!(
        "{name:<10} {:>9.0} req/s  {:>6.2} us CPU per request  ({n} requests)",
        n as f64 / wall.as_secs_f64(),
        cpu_ns as f64 / n as f64 / 1e3
    );
}

async fn run_diapason(url: &str, conns: usize, secs: u64) {
    let cfg = RunConfig::from_toml(&format!(
        "[target]\nurl = \"{url}\"\n[load]\n[[load.phases]]\nname = \"m\"\nduration = \"1s\"\nrate = 1\n"
    ))
    .unwrap();
    let proto = HttpProtocol::from_config(&cfg).unwrap();
    let count = Rc::new(Cell::new(0u64));
    let end = Instant::now() + Duration::from_secs(secs);
    let (cpu0, t0) = (thread_cpu_ns(), Instant::now());
    let mut tasks = Vec::new();
    for _ in 0..conns {
        let (mut w, mut r) = proto.connect().await.unwrap();
        let count = count.clone();
        tasks.push(tokio::task::spawn_local(async move {
            let mut seq = 0;
            while Instant::now() < end {
                w.write_request(&RequestCtx { seq, scenario: 0 })
                    .await
                    .unwrap();
                r.read_response().await.unwrap().unwrap();
                count.set(count.get() + 1);
                seq += 1;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    report(
        "diapason",
        count.get(),
        thread_cpu_ns() - cpu0,
        t0.elapsed(),
    );
}

async fn run_hyper(addr: std::net::SocketAddr, conns: usize, secs: u64) {
    let count = Rc::new(Cell::new(0u64));
    let end = Instant::now() + Duration::from_secs(secs);
    let (cpu0, t0) = (thread_cpu_ns(), Instant::now());
    let mut tasks = Vec::new();
    for _ in 0..conns {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut send, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .unwrap();
        tokio::task::spawn_local(conn);
        let count = count.clone();
        let uri: hyper::Uri = format!("http://{addr}/").parse().unwrap();
        tasks.push(tokio::task::spawn_local(async move {
            while Instant::now() < end {
                let req = hyper::Request::get(uri.clone())
                    .header("host", addr.to_string())
                    .body(Empty::<Bytes>::new())
                    .unwrap();
                let resp = send.send_request(req).await.unwrap();
                let _ = resp.into_body().collect().await.unwrap();
                count.set(count.get() + 1);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    report("hyper", count.get(), thread_cpu_ns() - cpu0, t0.elapsed());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let conns: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(64);
    let secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let server = diapason_server::spawn(diapason_server::ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        workers: 0,
        threads: 4,
        ..Default::default()
    })
    .unwrap();
    let url = format!("http://{}/", server.addr);
    println!("{conns} connections, {secs} s each, one client thread");
    for _ in 0..2 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, run_diapason(&url, conns, secs));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, run_hyper(server.addr, conns, secs));
    }
}
