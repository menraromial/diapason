//! Client CPU cost per request of the same minimal HTTP/1.1 loop on tokio's
//! current-thread runtime (epoll) and on monoio (io_uring). Linux only.
//!
//! Each runtime drives N keep-alive connections in a closed loop on one
//! thread against the in-process reference server; both loops write a fixed
//! request and parse the response with diapason's parser.
//!
//!     cargo run --release -p diapason-proto-http --example runtime_overhead [conns] [secs]

use bytes::BytesMut;
use diapason_proto_http::ResponseParser;
use std::cell::Cell;
use std::net::SocketAddr;
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
        "{name:<16} {:>9.0} req/s  {:>6.2} us CPU per request  ({n} requests)",
        n as f64 / wall.as_secs_f64(),
        cpu_ns as f64 / n as f64 / 1e3
    );
}

fn request(addr: SocketAddr) -> Vec<u8> {
    format!("GET / HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: bench\r\n\r\n").into_bytes()
}

async fn tokio_loop(addr: SocketAddr, conns: usize, secs: u64) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let count = Rc::new(Cell::new(0u64));
    let end = Instant::now() + Duration::from_secs(secs);
    let (cpu0, t0) = (thread_cpu_ns(), Instant::now());
    let mut tasks = Vec::new();
    for _ in 0..conns {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        let count = count.clone();
        let req = request(addr);
        tasks.push(tokio::task::spawn_local(async move {
            let mut parser = ResponseParser::new(false);
            let mut acc = BytesMut::with_capacity(4096);
            let mut buf = vec![0u8; 4096];
            while Instant::now() < end {
                s.write_all(&req).await.unwrap();
                loop {
                    if parser.step(&mut acc).unwrap().is_some() {
                        break;
                    }
                    let n = s.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    acc.extend_from_slice(&buf[..n]);
                }
                count.set(count.get() + 1);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    report(
        "tokio (epoll)",
        count.get(),
        thread_cpu_ns() - cpu0,
        t0.elapsed(),
    );
}

#[cfg(target_os = "linux")]
async fn monoio_loop(addr: SocketAddr, conns: usize, secs: u64) {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
    let count = Rc::new(Cell::new(0u64));
    let end = Instant::now() + Duration::from_secs(secs);
    let (cpu0, t0) = (thread_cpu_ns(), Instant::now());
    let mut tasks = Vec::new();
    for _ in 0..conns {
        let mut s = monoio::net::TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        let count = count.clone();
        let mut req = request(addr);
        tasks.push(monoio::spawn(async move {
            let mut parser = ResponseParser::new(false);
            let mut acc = BytesMut::with_capacity(4096);
            let mut buf = vec![0u8; 4096];
            while Instant::now() < end {
                let (r, b) = s.write_all(req).await;
                r.unwrap();
                req = b;
                loop {
                    if parser.step(&mut acc).unwrap().is_some() {
                        break;
                    }
                    let (r, b) = s.read(buf).await;
                    let n = r.unwrap();
                    assert!(n > 0);
                    acc.extend_from_slice(&b[..n]);
                    buf = b;
                }
                count.set(count.get() + 1);
            }
        }));
    }
    for t in tasks {
        t.await;
    }
    report(
        "monoio (io_uring)",
        count.get(),
        thread_cpu_ns() - cpu0,
        t0.elapsed(),
    );
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
    println!("{conns} connections, {secs} s each, one client thread");
    for _ in 0..2 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&rt, tokio_loop(server.addr, conns, secs));
        #[cfg(target_os = "linux")]
        {
            let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .build()
                .unwrap();
            rt.block_on(monoio_loop(server.addr, conns, secs));
        }
    }
}
