use std::io::{Cursor, Read, Seek, SeekFrom};

use thiserror::Error;

use crate::id::MutationId;
use crate::protocol::codec::MAX_MESSAGE_SIZE;
use crate::protocol::messages::SequencedOperation;

/// Magic bytes identifying a framed WAL batch (0xBA7C).
pub const BATCH_MAGIC: [u8; 2] = [0xBA, 0x7C];

/// Fixed WAL batch header size (18 bytes; layout in [`encode_wal_batch`]).
pub const BATCH_HEADER_SIZE: usize = 18;

/// Bytes of the header covered by its own checksum: every field before `header_crc32`.
const HEADER_CHECKED_LEN: usize = BATCH_HEADER_SIZE - 4;

/// Errors that can occur during WAL batch framing and decoding.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum WalFrameError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("WAL frame corruption: {0}")]
    Corruption(String),
}

/// Payload of a framed WAL batch: the operations and the optional mutation id for deduplication.
/// The field order is the on-disk encoding and must match [`WalBatchPayloadRef`].
#[derive(serde::Deserialize)]
struct WalBatchPayload {
    mutation_id: Option<MutationId>,
    ops: Vec<SequencedOperation>,
}

/// Borrowed form of [`WalBatchPayload`], with the same encoding, used to encode and measure
/// records without copying the operations.
#[derive(serde::Serialize)]
struct WalBatchPayloadRef<'a> {
    mutation_id: Option<MutationId>,
    ops: &'a [SequencedOperation],
}

/// Result of attempting to decode a WAL batch from a byte buffer.
#[derive(Debug, PartialEq, Clone)]
pub enum WalBatchDecodeResult {
    /// A decoded batch and the length of its frame.
    Ok {
        ops: Vec<SequencedOperation>,
        mutation_id: Option<MutationId>,
        bytes_consumed: usize,
    },
    /// Clean end-of-file reached (no more bytes remaining in WAL segment).
    CleanEof,
    /// The next frame cannot be read and no valid frame follows it (see [`FrameAnomaly`]): a
    /// torn write.
    TornWrite { reason: String },
}

/// Encodes `ops` and the optional `mutation_id` as one append-only, atomically framed WAL batch.
///
/// Framing:
/// - `magic`: 2 bytes, `[0xBA, 0x7C]`
/// - `payload_len`: 4 bytes (`u32`, little-endian payload length)
/// - `payload_crc32`: 4 bytes (`u32`, little-endian CRC32 checksum of payload)
/// - `ops_count`: 4 bytes (`u32`, little-endian count of operations in batch)
/// - `header_crc32`: 4 bytes (`u32`, little-endian CRC32 checksum of the 14 bytes above)
/// - `payload`: serialized `WalBatchPayload` via bincode
pub fn encode_wal_batch(
    ops: &[SequencedOperation],
    mutation_id: Option<MutationId>,
) -> Result<Vec<u8>, WalFrameError> {
    let payload = bincode::serialize(&WalBatchPayloadRef { mutation_id, ops })
        .map_err(|e| WalFrameError::Serialization(e.to_string()))?;
    if payload.len() as u64 > MAX_MESSAGE_SIZE {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch payload size {} exceeds MAX_MESSAGE_SIZE limit {}",
            payload.len(),
            MAX_MESSAGE_SIZE
        )));
    }

    let mut record = Vec::with_capacity(BATCH_HEADER_SIZE + payload.len());
    record.extend_from_slice(&BATCH_MAGIC);
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    record.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    let header_crc = crc32fast::hash(&record);
    record.extend_from_slice(&header_crc.to_le_bytes());
    record.extend_from_slice(&payload);
    Ok(record)
}

/// Size of the payload of the WAL record that `encode_wal_record` would produce for `op`,
/// without encoding it. `encode_wal_record` fails if it exceeds `MAX_MESSAGE_SIZE`.
pub fn wal_record_payload_len(
    op: &SequencedOperation,
    mutation_id: Option<MutationId>,
) -> Result<u64, WalFrameError> {
    let payload = WalBatchPayloadRef {
        mutation_id,
        ops: std::slice::from_ref(op),
    };
    bincode::serialized_size(&payload).map_err(|e| WalFrameError::Serialization(e.to_string()))
}

/// [`encode_wal_batch`] for a single operation.
pub fn encode_wal_record(
    op: &SequencedOperation,
    mutation_id: Option<MutationId>,
) -> Result<Vec<u8>, WalFrameError> {
    encode_wal_batch(std::slice::from_ref(op), mutation_id)
}

/// Header of a framed WAL batch, as parsed by [`parse_batch_header`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    payload_len: usize,
    payload_crc32: u32,
    ops_count: usize,
}

impl BatchHeader {
    /// Length of the payload that follows the header, at most `MAX_MESSAGE_SIZE`.
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Length of the whole frame: header plus payload.
    pub fn frame_len(&self) -> usize {
        BATCH_HEADER_SIZE + self.payload_len
    }

    /// Number of operations the payload declares.
    pub fn ops_count(&self) -> usize {
        self.ops_count
    }
}

/// Outcome of [`decode_batch_payload`].
#[derive(Debug, PartialEq, Clone)]
pub enum BatchPayloadDecode {
    /// The payload matches its checksum and holds the declared number of operations.
    Batch {
        ops: Vec<SequencedOperation>,
        mutation_id: Option<MutationId>,
    },
    /// The payload does not match its checksum. Whether that is a torn write or corruption
    /// depends on what follows the frame; see [`FrameAnomaly`].
    ChecksumMismatch { expected: u32, actual: u32 },
}

/// Why a header is not the header of a frame.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum InvalidHeader {
    #[error("magic is {0:?}, expected {BATCH_MAGIC:?}")]
    Magic([u8; 2]),

    #[error("header CRC32 is {actual}, expected {expected}")]
    Checksum { expected: u32, actual: u32 },

    #[error("payload length {0} exceeds MAX_MESSAGE_SIZE {MAX_MESSAGE_SIZE}")]
    PayloadTooLong(u32),
}

/// Parses the header of a frame, checking its magic, its own checksum and the payload length.
///
/// Together with [`decode_batch_payload`], [`find_valid_frame`] and [`FrameAnomaly`], this is
/// the decoder used by [`decode_wal_batch_from_slice`], exposed for readers that stream a log
/// instead of holding it in memory, so that both apply the same torn-write rules.
pub fn parse_batch_header(bytes: &[u8; BATCH_HEADER_SIZE]) -> Result<BatchHeader, InvalidHeader> {
    let [m0, m1, l0, l1, l2, l3, c0, c1, c2, c3, n0, n1, n2, n3, h0, h1, h2, h3] = *bytes;
    if [m0, m1] != BATCH_MAGIC {
        return Err(InvalidHeader::Magic([m0, m1]));
    }

    let expected = u32::from_le_bytes([h0, h1, h2, h3]);
    let actual = crc32fast::hash(&bytes[..HEADER_CHECKED_LEN]);
    if actual != expected {
        return Err(InvalidHeader::Checksum { expected, actual });
    }

    let payload_len = u32::from_le_bytes([l0, l1, l2, l3]);
    if u64::from(payload_len) > MAX_MESSAGE_SIZE {
        return Err(InvalidHeader::PayloadTooLong(payload_len));
    }

    Ok(BatchHeader {
        payload_len: payload_len as usize,
        payload_crc32: u32::from_le_bytes([c0, c1, c2, c3]),
        ops_count: u32::from_le_bytes([n0, n1, n2, n3]) as usize,
    })
}

/// Verifies a frame's payload against its header and decodes its operations.
///
/// `payload` must be exactly `header.payload_len()` bytes long.
pub fn decode_batch_payload(
    header: &BatchHeader,
    payload: &[u8],
) -> Result<BatchPayloadDecode, WalFrameError> {
    if payload.len() != header.payload_len {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch payload is {} bytes, header declares {}",
            payload.len(),
            header.payload_len
        )));
    }

    let actual_crc = crc32fast::hash(payload);
    if actual_crc != header.payload_crc32 {
        return Ok(BatchPayloadDecode::ChecksumMismatch {
            expected: header.payload_crc32,
            actual: actual_crc,
        });
    }

    let WalBatchPayload { ops, mutation_id } = bincode::deserialize(payload).map_err(|e| {
        WalFrameError::Corruption(format!("Failed to deserialize WAL batch operations: {e}"))
    })?;

    if ops.len() != header.ops_count {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch ops count mismatch: expected {}, got {}",
            header.ops_count,
            ops.len()
        )));
    }

    Ok(BatchPayloadDecode::Batch { ops, mutation_id })
}

/// Why no frame could be read where the next one should start.
///
/// An anomaly is either a torn write, the last frame cut short by a crash, or corruption in
/// the middle of the log. It is corruption exactly when a complete valid frame follows it
/// (see [`find_valid_frame`]): that frame was written after the damaged one was durable, so
/// dropping the tail would lose it.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum FrameAnomaly {
    #[error("incomplete WAL batch header: {available} bytes left, expected {BATCH_HEADER_SIZE}")]
    IncompleteHeader { available: usize },

    #[error("invalid WAL batch header: {0}")]
    InvalidHeader(InvalidHeader),

    #[error("truncated WAL batch: {available} bytes left, the frame takes {frame_len}")]
    TruncatedPayload { available: u64, frame_len: usize },

    #[error("WAL batch payload CRC32 is {actual}, expected {expected}")]
    ChecksumMismatch {
        expected: u32,
        actual: u32,
        frame_len: usize,
    },
}

impl FrameAnomaly {
    /// Offset, from the start of the damaged frame, where the search for a following frame
    /// starts.
    ///
    /// A valid header is trusted (its checksum matched), so the search starts after the frame
    /// it declares: a frame nested in its payload, which a client can write as a value, is not
    /// taken for one written later. An invalid header says nothing about where the frame ends,
    /// so the search starts at the next byte.
    pub fn search_offset(&self) -> u64 {
        match self {
            FrameAnomaly::IncompleteHeader { available } => *available as u64,
            FrameAnomaly::InvalidHeader(_) => 1,
            FrameAnomaly::TruncatedPayload { frame_len, .. }
            | FrameAnomaly::ChecksumMismatch { frame_len, .. } => *frame_len as u64,
        }
    }

    /// Classifies the anomaly given the position of the first valid frame after it, as found by
    /// [`find_valid_frame`]: with none it is a torn write, whose description is returned;
    /// otherwise it is corruption.
    pub fn classify(self, following_frame: Option<u64>) -> Result<String, WalFrameError> {
        match following_frame {
            Some(offset) => Err(WalFrameError::Corruption(format!(
                "{self}, followed by a valid frame at offset {offset}"
            ))),
            None => Ok(format!(
                "{self}, with no valid frame after it: treating as torn write"
            )),
        }
    }
}

/// Size of the window that [`find_valid_frame`] scans for headers at a time.
const SEARCH_WINDOW: usize = 64 * 1024;

/// Payloads that [`find_valid_frame`] checksums before it takes a valid header as a frame.
///
/// Each check reads up to `MAX_MESSAGE_SIZE` bytes. Data that is not a frame passes a header
/// checksum about once in 2^32 positions, so more failed checks than this come from headers
/// someone wrote on purpose, such as fake headers packed into a value; without a limit they
/// make the search quadratic in the length of the log.
const MAX_PAYLOAD_CHECKS: usize = 16;

/// Finds the first complete valid frame (magic, header checksum and payload checksum) that
/// starts at or after `from` and ends at or before `end`, and returns where it starts.
///
/// After `MAX_PAYLOAD_CHECKS` payloads that do not match their checksum, the next frame whose
/// header is valid is returned without checking its payload. That can only turn a torn write
/// into corruption, which stops recovery without truncating anything.
///
/// A frame stored as a value inside the payload of a frame whose header is invalid is found
/// too, so a crash that damages that header reports corruption. Telling them apart would take
/// the header that was lost.
///
/// The reader is scanned in windows and each payload is checksummed in chunks, so memory does
/// not grow with the log or the frame. Its position afterwards is unspecified.
pub fn find_valid_frame<R: Read + Seek>(
    reader: &mut R,
    from: u64,
    end: u64,
) -> std::io::Result<Option<u64>> {
    let mut window = vec![0u8; SEARCH_WINDOW + BATCH_HEADER_SIZE - 1];
    let mut failed_checks = 0;
    let mut start = from;
    while end.saturating_sub(start) >= BATCH_HEADER_SIZE as u64 {
        let len = (end - start).min(window.len() as u64) as usize;
        reader.seek(SeekFrom::Start(start))?;
        reader.read_exact(&mut window[..len])?;

        // Headers starting in the last `BATCH_HEADER_SIZE - 1` bytes are scanned with the next
        // window.
        let candidates = len - BATCH_HEADER_SIZE + 1;
        for i in 0..candidates {
            if window[i..i + 2] != BATCH_MAGIC {
                continue;
            }
            let header_bytes = window[i..i + BATCH_HEADER_SIZE]
                .try_into()
                .expect("a candidate has a whole header in the window");
            let Ok(header) = parse_batch_header(header_bytes) else {
                continue;
            };
            let position = start + i as u64;
            if end - position < header.frame_len() as u64 {
                continue;
            }
            if failed_checks == MAX_PAYLOAD_CHECKS
                || payload_matches(reader, position + BATCH_HEADER_SIZE as u64, &header)?
            {
                return Ok(Some(position));
            }
            failed_checks += 1;
        }
        start += candidates as u64;
    }
    Ok(None)
}

/// Whether the payload at `position` matches the checksum in `header`.
fn payload_matches<R: Read + Seek>(
    reader: &mut R,
    position: u64,
    header: &BatchHeader,
) -> std::io::Result<bool> {
    reader.seek(SeekFrom::Start(position))?;
    let mut hasher = crc32fast::Hasher::new();
    let mut chunk = [0u8; 8 * 1024];
    let mut remaining = header.payload_len;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        reader.read_exact(&mut chunk[..n])?;
        hasher.update(&chunk[..n]);
        remaining -= n;
    }
    Ok(hasher.finalize() == header.payload_crc32)
}

/// Decodes the next framed WAL batch from a byte slice: `Ok` for a complete batch, `CleanEof`
/// for an empty slice, and `TornWrite` if the next frame cannot be read and no valid frame
/// follows it (see [`FrameAnomaly`]). Fails with `WalFrameError::Corruption` if a valid frame
/// follows the one that cannot be read, or if a payload that matches its checksum does not
/// decode.
pub fn decode_wal_batch_from_slice(slice: &[u8]) -> Result<WalBatchDecodeResult, WalFrameError> {
    if slice.is_empty() {
        return Ok(WalBatchDecodeResult::CleanEof);
    }

    let anomaly = |anomaly: FrameAnomaly| {
        let following = find_valid_frame(
            &mut Cursor::new(slice),
            anomaly.search_offset(),
            slice.len() as u64,
        )
        .expect("reads stay within the slice");
        Ok(WalBatchDecodeResult::TornWrite {
            reason: anomaly.classify(following)?,
        })
    };

    let Some(header_bytes) = slice.first_chunk::<BATCH_HEADER_SIZE>() else {
        return anomaly(FrameAnomaly::IncompleteHeader {
            available: slice.len(),
        });
    };

    let header = match parse_batch_header(header_bytes) {
        Ok(header) => header,
        Err(invalid) => return anomaly(FrameAnomaly::InvalidHeader(invalid)),
    };

    let frame_len = header.frame_len();
    if slice.len() < frame_len {
        return anomaly(FrameAnomaly::TruncatedPayload {
            available: slice.len() as u64,
            frame_len,
        });
    }

    match decode_batch_payload(&header, &slice[BATCH_HEADER_SIZE..frame_len])? {
        BatchPayloadDecode::Batch { ops, mutation_id } => Ok(WalBatchDecodeResult::Ok {
            ops,
            mutation_id,
            bytes_consumed: frame_len,
        }),
        BatchPayloadDecode::ChecksumMismatch { expected, actual } => {
            anomaly(FrameAnomaly::ChecksumMismatch {
                expected,
                actual,
                frame_len,
            })
        }
    }
}

/// Decodes every frame of `wal_bytes` in order. Stops at a torn write and returns its
/// description with the operations and the length of the valid prefix, so the caller can
/// truncate the damaged tail.
pub fn replay_wal_records(
    wal_bytes: &[u8],
) -> Result<(Vec<SequencedOperation>, usize, Option<String>), WalFrameError> {
    let mut all_ops = Vec::new();
    let mut offset = 0;
    loop {
        match decode_wal_batch_from_slice(&wal_bytes[offset..])? {
            WalBatchDecodeResult::Ok {
                ops,
                bytes_consumed,
                ..
            } => {
                all_ops.extend(ops);
                offset += bytes_consumed;
            }
            WalBatchDecodeResult::CleanEof => return Ok((all_ops, offset, None)),
            WalBatchDecodeResult::TornWrite { reason, .. } => {
                return Ok((all_ops, offset, Some(reason)))
            }
        }
    }
}
