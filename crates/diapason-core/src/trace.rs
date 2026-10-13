//! The per-request trace: CSV, compressed with zstd when the path ends in
//! ".zst", or the binary columnar format of [`crate::dtr`] when it ends in
//! ".dtr". Workers hand batches of rows to a writer thread through a bounded
//! channel; rows that do not fit are dropped and counted.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::thread::JoinHandle;

const BATCH: usize = 4096;
const CHANNEL_BATCHES: usize = 64;

pub const HEADER: &str =
    "seq,worker,conn,phase,scenario,intended_ns,attempt_ns,send_ns,first_byte_ns,done_ns,status,bytes_in,error_kind";

#[derive(Clone, Copy, Debug)]
pub struct TraceRow {
    pub seq: u64,
    pub worker: u32,
    pub conn: u32,
    pub phase: u16,
    pub scenario: u16,
    pub intended: u64,
    pub attempt: Option<u64>,
    pub send: Option<u64>,
    pub first_byte: Option<u64>,
    pub done: Option<u64>,
    pub status: Option<u16>,
    pub bytes_in: u64,
    pub outcome: &'static str,
}

/// splitmix64, used to sample rows by sequence number.
pub fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// The worker side of the trace.
pub struct TraceSink {
    buf: Vec<TraceRow>,
    tx: SyncSender<Vec<TraceRow>>,
    sample: u64,
    pub dropped: u64,
}

impl TraceSink {
    #[inline]
    pub fn push(&mut self, row: TraceRow) {
        if self.sample > 1 && !mix(row.seq).is_multiple_of(self.sample) {
            return;
        }
        self.buf.push(row);
        if self.buf.len() >= BATCH {
            let batch = std::mem::replace(&mut self.buf, Vec::with_capacity(BATCH));
            match self.tx.try_send(batch) {
                Ok(()) => {}
                Err(TrySendError::Full(b)) | Err(TrySendError::Disconnected(b)) => {
                    self.dropped += b.len() as u64
                }
            }
        }
    }

    /// Sends what is buffered, waiting if needed. Used at the end of the run.
    pub fn flush(&mut self) {
        if !self.buf.is_empty() {
            let batch = std::mem::take(&mut self.buf);
            let n = batch.len() as u64;
            if self.tx.send(batch).is_err() {
                self.dropped += n;
            }
        }
    }
}

pub struct TraceWriter {
    tx: Option<SyncSender<Vec<TraceRow>>>,
    handle: JoinHandle<io::Result<u64>>,
    sample: u64,
}

impl TraceRow {
    fn record(&self) -> crate::dtr::TraceRecord {
        crate::dtr::TraceRecord {
            seq: self.seq,
            worker: self.worker,
            conn: (self.conn != crate::stats::NO_CONN).then_some(self.conn),
            phase: self.phase,
            scenario: self.scenario,
            intended_ns: self.intended,
            attempt_ns: self.attempt,
            send_ns: self.send,
            first_byte_ns: self.first_byte,
            done_ns: self.done,
            status: self.status,
            bytes_in: self.bytes_in,
            outcome: self.outcome,
        }
    }
}

fn write_csv(rx: Receiver<Vec<TraceRow>>, out: &mut dyn Write) -> io::Result<u64> {
    writeln!(out, "{HEADER}")?;
    let mut n = 0u64;
    for batch in rx {
        for r in &batch {
            crate::dtr::write_csv_record(out, &r.record())?;
        }
        n += batch.len() as u64;
    }
    Ok(n)
}

/// The binary columnar format: one block per batch.
fn write_dtr(rx: Receiver<Vec<TraceRow>>, out: &mut impl Write) -> io::Result<u64> {
    crate::dtr::write_header(out)?;
    let mut n = 0u64;
    let mut recs = Vec::with_capacity(BATCH);
    for batch in rx {
        recs.clear();
        recs.extend(batch.iter().map(TraceRow::record));
        crate::dtr::write_block(out, &recs)?;
        n += batch.len() as u64;
    }
    Ok(n)
}

impl TraceWriter {
    pub fn create(path: &Path, sample: u64) -> io::Result<TraceWriter> {
        let file = File::create(path)?;
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let (zst, dtr) = (ext == "zst", ext == "dtr");
        let (tx, rx) = sync_channel::<Vec<TraceRow>>(CHANNEL_BATCHES);
        let handle = std::thread::Builder::new()
            .name("diapason-trace".into())
            .spawn(move || -> io::Result<u64> {
                let buffered = BufWriter::with_capacity(1 << 20, file);
                if dtr {
                    let mut w = buffered;
                    let n = write_dtr(rx, &mut w)?;
                    w.flush()?;
                    Ok(n)
                } else if zst {
                    let mut enc = zstd::stream::write::Encoder::new(buffered, 3)?;
                    let n = write_csv(rx, &mut enc)?;
                    enc.finish()?.flush()?;
                    Ok(n)
                } else {
                    let mut w = buffered;
                    let n = write_csv(rx, &mut w)?;
                    w.flush()?;
                    Ok(n)
                }
            })?;
        Ok(TraceWriter {
            tx: Some(tx),
            handle,
            sample,
        })
    }

    pub fn sink(&self) -> TraceSink {
        TraceSink {
            buf: Vec::with_capacity(BATCH),
            tx: self.tx.clone().expect("sink taken before finish"),
            sample: self.sample,
            dropped: 0,
        }
    }

    /// Waits for the writer to finish; all sinks must have been dropped.
    pub fn finish(mut self) -> io::Result<u64> {
        self.tx.take();
        self.handle
            .join()
            .map_err(|_| io::Error::other("trace writer panicked"))?
    }
}
