//! HTTP/2 client halves (crate `h2`).
//!
//! The writer sends each request on a new stream and starts a local task that
//! waits for the response and reads its body; the task reports to the reader
//! through a channel, tagged with the request's `seq`, so responses may
//! arrive in any order. A timeout cancels one stream (the task is aborted,
//! which resets the stream) and keeps the connection. The connection task
//! reports its end on the same channel.

use crate::{render_parts, Parts, Shared};
use bytes::Bytes;
use diapason_core::clock::raw_now_ns;
use diapason_core::protocol::{ConnectErrorKind, FailKind, RequestCtx, Response};
use diapason_core::template::Renderer;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

enum Done {
    Response(Response),
    /// The connection ended: cleanly (`None`) or with an error.
    ConnEnd(Option<FailKind>),
}

type Streams = Rc<RefCell<HashMap<u64, AbortHandle>>>;

pub struct H2Writer {
    send: h2::client::SendRequest<Bytes>,
    shared: Arc<Shared>,
    renderer: Renderer,
    parts: Parts,
    tx: mpsc::UnboundedSender<Done>,
    streams: Streams,
}

pub struct H2Reader {
    rx: mpsc::UnboundedReceiver<Done>,
    /// Streams cancelled after a timeout; their late responses are dropped.
    cancelled: HashSet<u64>,
    streams: Streams,
    conn: AbortHandle,
}

impl Drop for H2Reader {
    fn drop(&mut self) {
        self.conn.abort();
        for (_, h) in self.streams.borrow_mut().drain() {
            h.abort();
        }
    }
}

fn fail_kind(e: &h2::Error) -> FailKind {
    if e.is_reset() || e.is_go_away() {
        FailKind::Reset
    } else if let Some(io) = e.get_io() {
        diapason_core::protocol::fail_kind_from_io(io, false)
    } else if e.is_io() {
        FailKind::Read
    } else {
        FailKind::Protocol
    }
}

pub async fn handshake<T>(
    io: T,
    shared: Arc<Shared>,
) -> Result<(H2Writer, H2Reader), ConnectErrorKind>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (send, conn) = h2::client::Builder::new()
        .initial_window_size(1 << 20)
        .initial_connection_window_size(1 << 24)
        .handshake::<_, Bytes>(io)
        .await
        .map_err(|_| ConnectErrorKind::Other)?;
    let (tx, rx) = mpsc::unbounded_channel();
    let ctx = tx.clone();
    let conn = diapason_core::spawn_task(async move {
        let end = conn.await.err().map(|e| fail_kind(&e));
        let _ = ctx.send(Done::ConnEnd(end));
    })
    .abort_handle();
    let streams: Streams = Rc::new(RefCell::new(HashMap::new()));
    Ok((
        H2Writer {
            send,
            shared,
            renderer: Renderer::default(),
            parts: Parts::default(),
            tx,
            streams: streams.clone(),
        },
        H2Reader {
            rx,
            cancelled: HashSet::new(),
            streams,
            conn,
        },
    ))
}

impl H2Writer {
    pub async fn write_request(&mut self, req: &RequestCtx) -> Result<(), FailKind> {
        let sc = &self.shared.scenarios[req.scenario as usize];
        render_parts(
            sc,
            &self.shared.env,
            &mut self.renderer,
            req.seq,
            &mut self.parts,
        );
        let p = &self.parts;
        let mut uri = Vec::with_capacity(self.shared.origin.len() + p.path.len());
        uri.extend_from_slice(self.shared.origin.as_bytes());
        uri.extend_from_slice(&p.path);
        let mut b = http::Request::builder()
            .method(sc.method.as_str())
            .uri(http::Uri::try_from(uri).map_err(|_| FailKind::Protocol)?);
        for (k, v) in sc.static_headers.iter().chain(p.headers.iter()) {
            // Connection-specific headers do not exist in HTTP/2; Host becomes
            // the :authority of the URI.
            if [
                "connection",
                "keep-alive",
                "transfer-encoding",
                "upgrade",
                "host",
            ]
            .iter()
            .any(|h| k.eq_ignore_ascii_case(h))
            {
                continue;
            }
            b = b.header(k.as_str(), v.as_slice());
        }
        if sc.needs_length {
            b = b.header("content-length", p.body.len());
        }
        let request = b.body(()).map_err(|_| FailKind::Protocol)?;
        let mut send = self.send.clone().ready().await.map_err(|e| fail_kind(&e))?;
        let end_of_stream = p.body.is_empty();
        let (fut, mut stream) = send
            .send_request(request, end_of_stream)
            .map_err(|e| fail_kind(&e))?;
        if !end_of_stream {
            stream
                .send_data(Bytes::copy_from_slice(&p.body), true)
                .map_err(|e| fail_kind(&e))?;
        }
        let seq = req.seq;
        let tx = self.tx.clone();
        let streams = self.streams.clone();
        let handle = diapason_core::spawn_task(async move {
            let respond = |r: Response| {
                streams.borrow_mut().remove(&seq);
                let _ = tx.send(Done::Response(r));
            };
            let failed = |kind| Response {
                status: 0,
                bytes_in: 0,
                first_byte_raw_ns: 0,
                close: false,
                seq: Some(seq),
                fail: Some(kind),
            };
            let resp = match fut.await {
                Ok(r) => r,
                Err(e) => return respond(failed(fail_kind(&e))),
            };
            let first_byte = raw_now_ns();
            let status = resp.status().as_u16();
            let mut body = resp.into_body();
            let mut n = 0u64;
            while let Some(chunk) = body.data().await {
                match chunk {
                    Ok(c) => {
                        n += c.len() as u64;
                        let _ = body.flow_control().release_capacity(c.len());
                    }
                    Err(e) => return respond(failed(fail_kind(&e))),
                }
            }
            respond(Response {
                status,
                bytes_in: n,
                first_byte_raw_ns: first_byte,
                close: false,
                seq: Some(seq),
                fail: None,
            });
        });
        self.streams.borrow_mut().insert(seq, handle.abort_handle());
        Ok(())
    }
}

impl H2Reader {
    pub async fn read_response(&mut self) -> Result<Option<Response>, FailKind> {
        loop {
            match self.rx.recv().await {
                Some(Done::Response(r)) => {
                    if r.seq.is_some_and(|s| self.cancelled.remove(&s)) {
                        continue;
                    }
                    return Ok(Some(r));
                }
                Some(Done::ConnEnd(None)) | None => return Ok(None),
                Some(Done::ConnEnd(Some(kind))) => return Err(kind),
            }
        }
    }

    pub fn cancel(&mut self, seq: u64) -> bool {
        match self.streams.borrow_mut().remove(&seq) {
            // Dropping the stream's futures resets it (RST_STREAM CANCEL).
            Some(h) => h.abort(),
            // Its response is already on the channel: drop it there.
            None => {
                self.cancelled.insert(seq);
            }
        }
        true
    }
}
