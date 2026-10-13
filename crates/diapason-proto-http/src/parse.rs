//! A resumable HTTP/1.1 response parser.

use bytes::{Buf, BytesMut};
use diapason_core::protocol::{FailKind, Response};

const MAX_HEAD: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Head,
    Length(u64),
    ChunkSize,
    ChunkData(u64),
    ChunkCrlf,
    Trailer,
    UntilEof,
}

#[derive(Debug)]
pub struct ResponseParser {
    head_request: bool,
    state: State,
    status: u16,
    close: bool,
    bytes: u64,
    first_byte: u64,
}

fn find_crlf(b: &[u8]) -> Option<usize> {
    b.windows(2).position(|w| w == b"\r\n")
}

impl ResponseParser {
    pub fn new(head_request: bool) -> ResponseParser {
        ResponseParser {
            head_request,
            state: State::Head,
            status: 0,
            close: false,
            bytes: 0,
            first_byte: 0,
        }
    }

    /// Whether the next response answers a HEAD request. Call before its
    /// first byte is parsed.
    pub fn set_head(&mut self, head: bool) {
        self.head_request = head;
    }

    /// True when no byte of the next response has been consumed.
    pub fn at_start(&self) -> bool {
        self.state == State::Head && self.first_byte == 0
    }

    /// Records the time of the first byte, once per response.
    pub fn set_first_byte(&mut self, raw_ns: u64) {
        if self.first_byte == 0 {
            self.first_byte = raw_ns;
        }
    }

    fn done(&mut self) -> Response {
        let r = Response {
            status: self.status,
            bytes_in: self.bytes,
            first_byte_raw_ns: self.first_byte,
            close: self.close,
            seq: None,
            fail: None,
        };
        self.state = State::Head;
        self.status = 0;
        self.close = false;
        self.bytes = 0;
        self.first_byte = 0;
        r
    }

    fn consume(&mut self, buf: &mut BytesMut, n: usize) {
        buf.advance(n);
        self.bytes += n as u64;
    }

    /// Consumes what it can from `buf`. Returns a response when one is
    /// complete, `None` when more bytes are needed.
    pub fn step(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, FailKind> {
        loop {
            match self.state {
                State::Head => {
                    if buf.is_empty() {
                        return Ok(None);
                    }
                    let mut headers = [httparse::EMPTY_HEADER; 64];
                    let mut resp = httparse::Response::new(&mut headers);
                    let len = match resp.parse(buf) {
                        Ok(httparse::Status::Complete(n)) => n,
                        Ok(httparse::Status::Partial) => {
                            if buf.len() > MAX_HEAD {
                                return Err(FailKind::Protocol);
                            }
                            return Ok(None);
                        }
                        Err(_) => return Err(FailKind::Protocol),
                    };
                    let code = resp.code.unwrap_or(0);
                    let version = resp.version.unwrap_or(1);
                    let mut length = None;
                    let mut chunked = false;
                    let mut conn_close = version == 0;
                    for h in resp.headers.iter() {
                        if h.name.eq_ignore_ascii_case("content-length") {
                            let v = std::str::from_utf8(h.value).map_err(|_| FailKind::Protocol)?;
                            length = Some(v.trim().parse::<u64>().map_err(|_| FailKind::Protocol)?);
                        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
                            chunked = h
                                .value
                                .split(|&b| b == b',')
                                .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"chunked"));
                        } else if h.name.eq_ignore_ascii_case("connection") {
                            for t in h.value.split(|&b| b == b',') {
                                let t = t.trim_ascii();
                                if t.eq_ignore_ascii_case(b"close") {
                                    conn_close = true;
                                } else if t.eq_ignore_ascii_case(b"keep-alive") {
                                    conn_close = false;
                                }
                            }
                        }
                    }
                    self.consume(buf, len);
                    if (100..200).contains(&code) && code != 101 {
                        // Interim response: the final one follows.
                        continue;
                    }
                    self.status = code;
                    self.close = conn_close;
                    let bodiless = self.head_request
                        || code == 204
                        || code == 304
                        || (100..200).contains(&code);
                    self.state = if bodiless {
                        return Ok(Some(self.done()));
                    } else if chunked {
                        State::ChunkSize
                    } else if let Some(n) = length {
                        if n == 0 {
                            return Ok(Some(self.done()));
                        }
                        State::Length(n)
                    } else {
                        self.close = true;
                        State::UntilEof
                    };
                }
                State::Length(rem) => {
                    let n = (buf.len() as u64).min(rem) as usize;
                    self.consume(buf, n);
                    let rem = rem - n as u64;
                    if rem == 0 {
                        return Ok(Some(self.done()));
                    }
                    self.state = State::Length(rem);
                    return Ok(None);
                }
                State::ChunkSize => {
                    let Some(i) = find_crlf(buf) else {
                        if buf.len() > MAX_HEAD {
                            return Err(FailKind::Protocol);
                        }
                        return Ok(None);
                    };
                    let line = &buf[..i];
                    let hex_end = line.iter().position(|&b| b == b';').unwrap_or(line.len());
                    let hex =
                        std::str::from_utf8(&line[..hex_end]).map_err(|_| FailKind::Protocol)?;
                    let size =
                        u64::from_str_radix(hex.trim(), 16).map_err(|_| FailKind::Protocol)?;
                    self.consume(buf, i + 2);
                    self.state = if size == 0 {
                        State::Trailer
                    } else {
                        State::ChunkData(size)
                    };
                }
                State::ChunkData(rem) => {
                    if buf.is_empty() {
                        return Ok(None);
                    }
                    let n = (buf.len() as u64).min(rem) as usize;
                    self.consume(buf, n);
                    let rem = rem - n as u64;
                    self.state = if rem == 0 {
                        State::ChunkCrlf
                    } else {
                        State::ChunkData(rem)
                    };
                }
                State::ChunkCrlf => {
                    if buf.len() < 2 {
                        return Ok(None);
                    }
                    if &buf[..2] != b"\r\n" {
                        return Err(FailKind::Protocol);
                    }
                    self.consume(buf, 2);
                    self.state = State::ChunkSize;
                }
                State::Trailer => {
                    let Some(i) = find_crlf(buf) else {
                        if buf.len() > MAX_HEAD {
                            return Err(FailKind::Protocol);
                        }
                        return Ok(None);
                    };
                    self.consume(buf, i + 2);
                    if i == 0 {
                        return Ok(Some(self.done()));
                    }
                }
                State::UntilEof => {
                    let n = buf.len();
                    self.consume(buf, n);
                    return Ok(None);
                }
            }
        }
    }

    /// The peer closed the connection.
    pub fn eof(&mut self) -> Result<Option<Response>, FailKind> {
        match self.state {
            State::UntilEof => Ok(Some(self.done())),
            State::Head if self.bytes == 0 => Ok(None),
            _ => Err(FailKind::Read),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Parsed = Vec<Result<Option<(u16, u64, bool)>, FailKind>>;

    fn parse_all(input: &[u8], chunk: usize, head: bool) -> Parsed {
        let mut p = ResponseParser::new(head);
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for piece in input.chunks(chunk) {
            buf.extend_from_slice(piece);
            loop {
                match p.step(&mut buf) {
                    Ok(Some(r)) => out.push(Ok(Some((r.status, r.bytes_in, r.close)))),
                    Ok(None) => break,
                    Err(e) => {
                        out.push(Err(e));
                        return out;
                    }
                }
            }
        }
        if let Ok(Some(r)) = p.eof().map(|o| o.map(|r| (r.status, r.bytes_in, r.close))) {
            out.push(Ok(Some(r)));
        }
        out
    }

    #[test]
    fn content_length_pipelined_any_split() {
        let one = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let mut input = one.to_vec();
        input.extend_from_slice(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
        for chunk in [1, 2, 7, 1000] {
            let r = parse_all(&input, chunk, false);
            assert_eq!(
                r,
                vec![
                    Ok(Some((200, one.len() as u64, false))),
                    Ok(Some((404, 45, false)))
                ],
                "chunk {chunk}"
            );
        }
    }

    #[test]
    fn chunked_with_trailer_and_interim() {
        let input = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4;x=y\r\nWiki\r\n5\r\npedia\r\n0\r\nX-T: 1\r\n\r\n";
        for chunk in [1, 3, 1000] {
            let r = parse_all(input, chunk, false);
            assert_eq!(r, vec![Ok(Some((200, input.len() as u64, false)))]);
        }
    }

    #[test]
    fn bodiless_and_close() {
        let input = b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
        assert_eq!(
            parse_all(input, 5, false),
            vec![Ok(Some((204, input.len() as u64, true)))]
        );
        let input = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n";
        assert_eq!(
            parse_all(input, 5, true),
            vec![Ok(Some((200, input.len() as u64, false)))]
        );
        let input = b"HTTP/1.0 200 OK\r\n\r\nuntil close";
        assert_eq!(
            parse_all(input, 4, false),
            vec![Ok(Some((200, input.len() as u64, true)))]
        );
    }

    #[test]
    fn errors() {
        assert_eq!(
            parse_all(b"garbage\r\n\r\n", 100, false),
            vec![Err(FailKind::Protocol)]
        );
        let mut p = ResponseParser::new(false);
        let mut buf = BytesMut::from(&b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc"[..]);
        assert!(p.step(&mut buf).unwrap().is_none());
        assert!(matches!(p.eof(), Err(FailKind::Read)));
    }
}
