//! Streaming reads of the framed batches of a segment, for `.wal` files, `active.wal` and the
//! decompressed stream of `.wal.zst` files.
//!
//! Frames before the requested cursor are skipped by reading only their header: the header
//! has its own checksum, so the length used to skip is trustworthy, and the sequence of
//! each frame follows from the first sequence of the segment and the operation counts of the
//! frames before it. The payloads skipped this way are not checked against their checksum;
//! recovery and compression check them.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use zemdb_core::id::{MutationId, SequenceNumber};
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::{
    decode_batch_payload, find_valid_frame, parse_batch_header, BatchPayloadDecode, FrameAnomaly,
    BATCH_HEADER_SIZE,
};

use crate::error::ServerError;
use crate::fail_point;
use crate::log::budget::{op_bytes, ReadBudget};
use crate::log::io_probe::{self, IoEvent};

/// Operations and mutation assignment pairs recovered from disk segments.
pub type RecoveredLogData = (Vec<SequencedOperation>, Vec<(MutationId, SequenceNumber)>);

/// A reader of frames that can skip bytes.
pub(crate) trait FrameSource: Read {
    /// Skips `n` bytes and returns how many were skipped: fewer only at the end of a stream.
    /// A seekable source may skip past its end; the caller checks the frame against the
    /// length it knows.
    fn skip(&mut self, n: u64) -> io::Result<u64>;
}

impl<R: Read + Seek> FrameSource for BufReader<R> {
    fn skip(&mut self, n: u64) -> io::Result<u64> {
        let offset = i64::try_from(n).map_err(|_| io::Error::other("skip too long"))?;
        self.seek_relative(offset)?;
        Ok(n)
    }
}

impl<R: io::BufRead> FrameSource for zstd::stream::read::Decoder<'_, R> {
    fn skip(&mut self, n: u64) -> io::Result<u64> {
        io::copy(&mut self.by_ref().take(n), &mut io::sink())
    }
}

/// Operations read from a segment, and where the reading stopped if it was not at the end.
#[derive(Debug, Default)]
pub(crate) struct FrameRead {
    pub(crate) ops: Vec<SequencedOperation>,
    pub(crate) mutations: Vec<(MutationId, SequenceNumber)>,
    /// The frame that could not be read and its offset, if the reading stopped at one.
    pub(crate) anomaly: Option<(u64, FrameAnomaly)>,
}

/// The segment file at `path`, opened for a read within `budget` after recording `event`:
/// `None` if the budget is exhausted or the file is missing (the caller detects the gap it
/// leaves).
pub(crate) fn open_segment(
    path: &Path,
    event: IoEvent,
    budget: &ReadBudget,
) -> Result<Option<File>, ServerError> {
    if budget.is_exhausted() {
        return Ok(None);
    }
    io_probe::record(event, path.parent().unwrap_or_else(|| Path::new("")));
    fail_point::check("open_segment", path)?;
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Reads until `buf` is full or the source ends, and returns the bytes read.
pub(crate) fn read_full<R: Read>(source: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match source.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Reads the operations after `from_seq` from the frames of `source`, as many as `budget`
/// admits, with the mutation IDs of their frames.
///
/// `end` is the length of the source when it is known (a file), so that a frame running past
/// it is caught without reading it. `start_seq` is the sequence of the first operation in the
/// source; without it no frame can be skipped and every one is decoded. A decoded frame whose
/// first operation is not the one the operation counts lead to is corruption.
pub(crate) fn read_frames<S: FrameSource>(
    source: &mut S,
    end: Option<u64>,
    start_seq: Option<SequenceNumber>,
    from_seq: SequenceNumber,
    budget: &mut ReadBudget,
) -> Result<FrameRead, ServerError> {
    let mut read = FrameRead::default();
    let mut position = 0u64;
    let mut next_seq = start_seq.map(|seq| seq.get());

    'frames: while !budget.is_exhausted() {
        let mut header_bytes = [0u8; BATCH_HEADER_SIZE];
        let available = read_full(source, &mut header_bytes)?;
        if available == 0 {
            break;
        }
        if available < BATCH_HEADER_SIZE {
            read.anomaly = Some((position, FrameAnomaly::IncompleteHeader { available }));
            break;
        }
        let header = match parse_batch_header(&header_bytes) {
            Ok(header) => header,
            Err(invalid) => {
                read.anomaly = Some((position, FrameAnomaly::InvalidHeader(invalid)));
                break;
            }
        };

        let frame_len = header.frame_len();
        let truncated = |available: u64| FrameAnomaly::TruncatedPayload {
            available,
            frame_len,
        };
        if let Some(end) = end {
            let available = end.saturating_sub(position);
            if available < frame_len as u64 {
                read.anomaly = Some((position, truncated(available)));
                break;
            }
        }

        let payload_len = header.payload_len() as u64;
        let ops_count = header.ops_count() as u64;
        let frame_end_seq = next_seq.map(|seq| seq.saturating_add(ops_count));
        if frame_end_seq.is_some_and(|end_seq| end_seq <= from_seq.get() + 1) {
            let skipped = source.skip(payload_len)?;
            if skipped < payload_len {
                let available = BATCH_HEADER_SIZE as u64 + skipped;
                read.anomaly = Some((position, truncated(available)));
                break;
            }
            next_seq = frame_end_seq;
            position += frame_len as u64;
            continue;
        }

        let mut payload = vec![0u8; header.payload_len()];
        let payload_read = read_full(source, &mut payload)?;
        if payload_read < payload.len() {
            let available = (BATCH_HEADER_SIZE + payload_read) as u64;
            read.anomaly = Some((position, truncated(available)));
            break;
        }
        let decoded = decode_batch_payload(&header, &payload)
            .map_err(|e| ServerError::WalCorruption(e.to_string()))?;
        let (ops, mutation_id) = match decoded {
            BatchPayloadDecode::Batch { ops, mutation_id } => (ops, mutation_id),
            BatchPayloadDecode::ChecksumMismatch { expected, actual } => {
                read.anomaly = Some((
                    position,
                    FrameAnomaly::ChecksumMismatch {
                        expected,
                        actual,
                        frame_len,
                    },
                ));
                break;
            }
        };

        if let (Some(expected), Some(first)) = (next_seq, ops.first()) {
            if first.seq.get() != expected {
                return Err(ServerError::WalCorruption(format!(
                    "WAL frame at offset {position} starts at sequence {}, expected {expected}",
                    first.seq.get()
                )));
            }
        }
        next_seq = ops.last().map(|op| op.seq.get() + 1).or(next_seq);
        position += frame_len as u64;

        for op in ops {
            if op.seq <= from_seq {
                continue;
            }
            if !budget.admit(op_bytes(&op)) {
                break 'frames;
            }
            if let Some(mutation_id) = mutation_id {
                read.mutations.push((mutation_id, op.seq));
            }
            read.ops.push(op);
        }
    }

    Ok(read)
}

/// Classifies the anomaly found at `at` in a seekable source of length `end`: the torn-write
/// description, or corruption if a valid frame follows (see [`FrameAnomaly`]).
pub(crate) fn classify_in<R: Read + Seek>(
    source: &mut R,
    at: u64,
    anomaly: FrameAnomaly,
    end: u64,
) -> Result<String, ServerError> {
    let from = at.saturating_add(anomaly.search_offset());
    let following = find_valid_frame(source, from, end)?;
    source.seek(SeekFrom::Start(end))?;
    anomaly
        .classify(following)
        .map_err(|e| ServerError::WalCorruption(e.to_string()))
}

#[cfg(test)]
#[path = "tests/segment_reader.rs"]
mod tests;
