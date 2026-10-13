//! The binary columnar trace format (".dtr").
//!
//! A file is a header followed by blocks. Header: the 8 bytes `DIAPTRC1`,
//! a little-endian u32 format version (1), a little-endian u32 length, and
//! that many bytes of JSON describing the columns. Block: a little-endian u32
//! row count, a little-endian u32 byte count, and that many bytes of zstd
//! data. Decompressed, a block holds its columns one after the other, each as
//! one unsigned LEB128 varint per row:
//!
//! | column | encoding |
//! | --- | --- |
//! | `seq`, `worker`, `phase`, `scenario`, `bytes_in` | the value |
//! | `conn` | 0 when absent, else conn + 1 |
//! | `intended_ns` | zigzag of the difference with the previous row of the block (the first row: with 0) |
//! | `attempt_ns`, `send_ns`, `first_byte_ns`, `done_ns` | 0 when absent, else zigzag(value - intended_ns) + 1 |
//! | `status` | 0 when absent, else the status code |
//! | `outcome` | index in [`OUTCOMES`] |
//!
//! zigzag(x) maps a signed x to 2x when x >= 0 and to -2x - 1 otherwise.

use std::io::{self, Read, Write};

pub const MAGIC: &[u8; 8] = b"DIAPTRC1";
pub const VERSION: u32 = 1;

/// Outcomes, by index: completed, the failure kinds, the not-sent reasons,
/// and requests in flight at the end.
pub const OUTCOMES: [&str; 11] = [
    "",
    "timeout",
    "aborted",
    "reset",
    "read",
    "write",
    "protocol",
    "not_sent:queue",
    "not_sent:queue_full",
    "not_sent:end_of_run",
    "in_flight_at_end",
];

pub const COLUMNS: [&str; 13] = [
    "seq",
    "worker",
    "conn",
    "phase",
    "scenario",
    "intended_ns",
    "attempt_ns",
    "send_ns",
    "first_byte_ns",
    "done_ns",
    "status",
    "bytes_in",
    "error_kind",
];

/// One request of a trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRecord {
    pub seq: u64,
    pub worker: u32,
    pub conn: Option<u32>,
    pub phase: u16,
    pub scenario: u16,
    pub intended_ns: u64,
    pub attempt_ns: Option<u64>,
    pub send_ns: Option<u64>,
    pub first_byte_ns: Option<u64>,
    pub done_ns: Option<u64>,
    pub status: Option<u16>,
    pub bytes_in: u64,
    /// Empty for a completed request; see [`OUTCOMES`].
    pub outcome: &'static str,
}

fn put(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(b);
            return;
        }
        buf.push(b | 0x80);
    }
}

fn zigzag(x: i64) -> u64 {
    ((x << 1) ^ (x >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

fn bad(m: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("dtr: {m}"))
}

impl Cursor<'_> {
    fn get(&mut self) -> io::Result<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = *self.b.get(self.pos).ok_or_else(|| bad("truncated block"))?;
            self.pos += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(bad("varint too long"))
    }
}

/// Encodes one block (uncompressed columns).
pub fn encode_block(rows: &[TraceRecord]) -> Vec<u8> {
    let mut b = Vec::with_capacity(rows.len() * 24);
    for r in rows {
        put(&mut b, r.seq);
    }
    for r in rows {
        put(&mut b, r.worker as u64);
    }
    for r in rows {
        put(&mut b, r.conn.map_or(0, |c| c as u64 + 1));
    }
    for r in rows {
        put(&mut b, r.phase as u64);
    }
    for r in rows {
        put(&mut b, r.scenario as u64);
    }
    let mut prev = 0u64;
    for r in rows {
        put(&mut b, zigzag(r.intended_ns.wrapping_sub(prev) as i64));
        prev = r.intended_ns;
    }
    for get in [
        |r: &TraceRecord| r.attempt_ns,
        |r: &TraceRecord| r.send_ns,
        |r: &TraceRecord| r.first_byte_ns,
        |r: &TraceRecord| r.done_ns,
    ] {
        for r in rows {
            put(
                &mut b,
                get(r).map_or(0, |t| zigzag(t.wrapping_sub(r.intended_ns) as i64) + 1),
            );
        }
    }
    for r in rows {
        put(&mut b, r.status.map_or(0, |s| s as u64));
    }
    for r in rows {
        put(&mut b, r.bytes_in);
    }
    for r in rows {
        let i = OUTCOMES.iter().position(|o| *o == r.outcome).unwrap_or(0);
        b.push(i as u8);
    }
    b
}

pub fn decode_block(n: usize, b: &[u8]) -> io::Result<Vec<TraceRecord>> {
    let mut c = Cursor { b, pos: 0 };
    let col = |c: &mut Cursor| -> io::Result<Vec<u64>> { (0..n).map(|_| c.get()).collect() };
    let seq = col(&mut c)?;
    let worker = col(&mut c)?;
    let conn = col(&mut c)?;
    let phase = col(&mut c)?;
    let scenario = col(&mut c)?;
    let intended_d = col(&mut c)?;
    let attempt = col(&mut c)?;
    let send = col(&mut c)?;
    let first = col(&mut c)?;
    let done = col(&mut c)?;
    let status = col(&mut c)?;
    let bytes = col(&mut c)?;
    let outcome = b
        .get(c.pos..c.pos + n)
        .ok_or_else(|| bad("truncated outcomes"))?;
    let mut out = Vec::with_capacity(n);
    let mut prev = 0u64;
    let rel = |v: u64, base: u64| (v != 0).then(|| base.wrapping_add(unzigzag(v - 1) as u64));
    for i in 0..n {
        let intended = prev.wrapping_add(unzigzag(intended_d[i]) as u64);
        prev = intended;
        out.push(TraceRecord {
            seq: seq[i],
            worker: worker[i] as u32,
            conn: (conn[i] != 0).then(|| (conn[i] - 1) as u32),
            phase: phase[i] as u16,
            scenario: scenario[i] as u16,
            intended_ns: intended,
            attempt_ns: rel(attempt[i], intended),
            send_ns: rel(send[i], intended),
            first_byte_ns: rel(first[i], intended),
            done_ns: rel(done[i], intended),
            status: (status[i] != 0).then_some(status[i] as u16),
            bytes_in: bytes[i],
            outcome: OUTCOMES
                .get(outcome[i] as usize)
                .copied()
                .ok_or_else(|| bad("unknown outcome"))?,
        });
    }
    Ok(out)
}

pub fn write_header(w: &mut impl Write) -> io::Result<()> {
    let header = serde_json::json!({
        "format": "diapason binary columnar trace",
        "columns": COLUMNS,
        "outcomes": OUTCOMES,
        "time_unit": "ns since T0",
    })
    .to_string();
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    w.write_all(&(header.len() as u32).to_le_bytes())?;
    w.write_all(header.as_bytes())
}

pub fn write_block(w: &mut impl Write, rows: &[TraceRecord]) -> io::Result<()> {
    let z = zstd::bulk::compress(&encode_block(rows), 3)?;
    w.write_all(&(rows.len() as u32).to_le_bytes())?;
    w.write_all(&(z.len() as u32).to_le_bytes())?;
    w.write_all(&z)
}

fn read_u32(r: &mut impl Read) -> io::Result<Option<u32>> {
    let mut b = [0u8; 4];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u32::from_le_bytes(b))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// Reads a `.dtr` stream, calling `f` for every record in file order.
pub fn read(r: &mut impl Read, mut f: impl FnMut(&TraceRecord)) -> io::Result<u64> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(bad("not a diapason binary trace"));
    }
    let version = read_u32(r)?.ok_or_else(|| bad("truncated header"))?;
    if version != VERSION {
        return Err(bad(&format!("format version {version} is not supported")));
    }
    let hlen = read_u32(r)?.ok_or_else(|| bad("truncated header"))? as usize;
    let mut header = vec![0u8; hlen];
    r.read_exact(&mut header)?;
    let mut total = 0u64;
    while let Some(n) = read_u32(r)? {
        let len = read_u32(r)?.ok_or_else(|| bad("truncated block"))? as usize;
        let mut z = vec![0u8; len];
        r.read_exact(&mut z)?;
        let raw = zstd::stream::decode_all(&z[..])?;
        for rec in decode_block(n as usize, &raw)? {
            f(&rec);
        }
        total += n as u64;
    }
    Ok(total)
}

fn opt(w: &mut dyn Write, v: Option<u64>) -> io::Result<()> {
    match v {
        Some(x) => write!(w, "{x},"),
        None => w.write_all(b","),
    }
}

/// Writes one record as a line of the CSV trace format.
pub fn write_csv_record(w: &mut dyn Write, r: &TraceRecord) -> io::Result<()> {
    write!(w, "{},{},", r.seq, r.worker)?;
    opt(w, r.conn.map(u64::from))?;
    write!(w, "{},{},{},", r.phase, r.scenario, r.intended_ns)?;
    opt(w, r.attempt_ns)?;
    opt(w, r.send_ns)?;
    opt(w, r.first_byte_ns)?;
    opt(w, r.done_ns)?;
    opt(w, r.status.map(u64::from))?;
    writeln!(w, "{},{}", r.bytes_in, r.outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64, intended: u64, outcome: &'static str) -> TraceRecord {
        TraceRecord {
            seq,
            worker: (seq % 3) as u32,
            conn: (outcome.is_empty()).then_some(seq as u32 % 7),
            phase: 1,
            scenario: 2,
            intended_ns: intended,
            attempt_ns: Some(intended + 120),
            send_ns: (outcome != "not_sent:queue").then_some(intended + 500),
            first_byte_ns: outcome.is_empty().then_some(intended + 90_000),
            done_ns: outcome.is_empty().then_some(intended + 91_000),
            status: outcome.is_empty().then_some(200),
            bytes_in: if outcome.is_empty() { 40 } else { 0 },
            outcome,
        }
    }

    #[test]
    fn round_trip() {
        // Intended times out of order across workers, as in a real trace.
        let rows: Vec<TraceRecord> = (0..10_000u64)
            .map(|i| {
                let t = 1_000_000_000 + i * 997 - (i % 5) * 3000;
                rec(
                    i,
                    t,
                    ["", "", "timeout", "not_sent:queue", "in_flight_at_end"][(i % 5) as usize],
                )
            })
            .collect();
        let mut file = Vec::new();
        write_header(&mut file).unwrap();
        for chunk in rows.chunks(4096) {
            write_block(&mut file, chunk).unwrap();
        }
        let mut back = Vec::new();
        let n = read(&mut &file[..], |r| back.push(r.clone())).unwrap();
        assert_eq!(n, 10_000);
        assert_eq!(back, rows);
        assert!(file.len() < rows.len() * 8, "{} bytes", file.len());
    }

    #[test]
    fn rejects_other_files() {
        assert!(read(&mut &b"seq,worker\n1,2\n"[..], |_| {}).is_err());
    }
}
