//! The interface between the protocol-agnostic engine and a wire protocol.

use std::future::Future;
use std::io;

/// Why a request that was sent got no full response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FailKind {
    Timeout,
    Aborted,
    Reset,
    Read,
    Write,
    Protocol,
}

impl FailKind {
    pub const ALL: [FailKind; 6] = [
        FailKind::Timeout,
        FailKind::Aborted,
        FailKind::Reset,
        FailKind::Read,
        FailKind::Write,
        FailKind::Protocol,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            FailKind::Timeout => "timeout",
            FailKind::Aborted => "aborted",
            FailKind::Reset => "reset",
            FailKind::Read => "read",
            FailKind::Write => "write",
            FailKind::Protocol => "protocol",
        }
    }
    pub fn index(self) -> usize {
        self as usize
    }
}

/// Why a scheduled request was never written to a socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NotSentReason {
    Queue,
    QueueFull,
    EndOfRun,
}

impl NotSentReason {
    pub const ALL: [NotSentReason; 3] = [
        NotSentReason::Queue,
        NotSentReason::QueueFull,
        NotSentReason::EndOfRun,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            NotSentReason::Queue => "queue",
            NotSentReason::QueueFull => "queue_full",
            NotSentReason::EndOfRun => "end_of_run",
        }
    }
    pub fn index(self) -> usize {
        self as usize
    }
}

/// Why a connection could not be opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectErrorKind {
    Dns,
    ConnectRefused,
    ConnectTimeout,
    FdExhausted,
    PortExhausted,
    Tls,
    Other,
}

impl ConnectErrorKind {
    pub const ALL: [ConnectErrorKind; 7] = [
        ConnectErrorKind::Dns,
        ConnectErrorKind::ConnectRefused,
        ConnectErrorKind::ConnectTimeout,
        ConnectErrorKind::FdExhausted,
        ConnectErrorKind::PortExhausted,
        ConnectErrorKind::Tls,
        ConnectErrorKind::Other,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectErrorKind::Dns => "dns",
            ConnectErrorKind::ConnectRefused => "connect_refused",
            ConnectErrorKind::ConnectTimeout => "connect_timeout",
            ConnectErrorKind::FdExhausted => "fd_exhausted",
            ConnectErrorKind::PortExhausted => "port_exhausted",
            ConnectErrorKind::Tls => "tls",
            ConnectErrorKind::Other => "connect_other",
        }
    }
    pub fn index(self) -> usize {
        self as usize
    }

    /// Classifies an error from socket() or connect().
    pub fn from_io(e: &io::Error) -> ConnectErrorKind {
        match e.raw_os_error() {
            Some(libc::EMFILE) | Some(libc::ENFILE) => return ConnectErrorKind::FdExhausted,
            Some(libc::EADDRNOTAVAIL) | Some(libc::EADDRINUSE) => {
                return ConnectErrorKind::PortExhausted
            }
            // A reset during the handshake (a listener closing, for instance)
            // is the peer refusing the attempt.
            Some(libc::ECONNREFUSED) | Some(libc::ECONNRESET) | Some(libc::ECONNABORTED) => {
                return ConnectErrorKind::ConnectRefused
            }
            Some(libc::ETIMEDOUT) => return ConnectErrorKind::ConnectTimeout,
            _ => {}
        }
        match e.kind() {
            io::ErrorKind::ConnectionRefused => ConnectErrorKind::ConnectRefused,
            io::ErrorKind::TimedOut => ConnectErrorKind::ConnectTimeout,
            _ => ConnectErrorKind::Other,
        }
    }
}

/// Maps an error on an open connection to a failure kind.
pub fn fail_kind_from_io(e: &io::Error, writing: bool) -> FailKind {
    match e.kind() {
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::ConnectionAborted => FailKind::Reset,
        io::ErrorKind::InvalidData => FailKind::Protocol,
        _ if writing => FailKind::Write,
        _ => FailKind::Read,
    }
}

/// A complete response.
#[derive(Clone, Copy, Debug)]
pub struct Response {
    pub status: u16,
    pub bytes_in: u64,
    /// Monotonic ns (raw clock) when the first byte of this response was read.
    pub first_byte_raw_ns: u64,
    /// The server announced it closes the connection after this response.
    pub close: bool,
    /// For multiplexed protocols, the `seq` of the request this response
    /// answers; `None` means the oldest pending request.
    pub seq: Option<u64>,
    /// For multiplexed protocols, a failure of this request alone (an HTTP/2
    /// stream reset, for instance); the connection stays open.
    pub fail: Option<FailKind>,
}

/// What a protocol knows about the request it must write.
#[derive(Clone, Copy, Debug)]
pub struct RequestCtx {
    pub seq: u64,
    pub scenario: u16,
}

/// The read half of a connection.
pub trait ResponseReader: 'static {
    /// Reads one full response. Returns `Ok(None)` on a clean end of stream
    /// before any byte of a new response.
    fn read_response(&mut self) -> impl Future<Output = Result<Option<Response>, FailKind>>;

    /// Abandons one request after its timeout. Returns true if the protocol
    /// did so without closing the connection (an HTTP/2 stream reset, for
    /// instance); false makes the engine close the connection.
    fn cancel(&mut self, _seq: u64) -> bool {
        false
    }
}

/// The write half of a connection.
pub trait RequestWriter: 'static {
    fn write_request(&mut self, req: &RequestCtx) -> impl Future<Output = Result<(), FailKind>>;
}

/// A wire protocol. One value is shared by all workers; `connect` runs on the
/// worker's thread and the halves stay there.
pub trait Protocol: Send + Sync + 'static {
    type Writer: RequestWriter;
    type Reader: ResponseReader;

    fn connect(
        &self,
    ) -> impl Future<Output = Result<(Self::Writer, Self::Reader), ConnectErrorKind>>;

    /// A short description for reports, such as "http/1.1 127.0.0.1:8080".
    fn describe(&self) -> String;

    /// Requests that may be outstanding on one connection. `None` uses
    /// `connections.pipeline`.
    fn max_outstanding(&self) -> Option<usize> {
        None
    }

    /// Called once, before the workers start, with the run's master seed, so
    /// that request contents drawn from it are reproducible.
    fn set_seed(&self, _seed: u64) {}

    /// Protocol-specific counters for the summary (`protocol_report`).
    fn report(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}
