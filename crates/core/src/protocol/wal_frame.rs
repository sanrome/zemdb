use thiserror::Error;

use crate::id::MutationId;
use crate::protocol::codec::MAX_MESSAGE_SIZE;
use crate::protocol::messages::SequencedOperation;

/// Magic bytes identifying a framed WAL batch (0xBA7C).
pub const BATCH_MAGIC: [u8; 2] = [0xBA, 0x7C];

/// Fixed WAL batch header size: 2B magic + 4B batch_len + 4B batch_crc32 + 4B ops_count = 14 bytes.
pub const BATCH_HEADER_SIZE: usize = 14;

/// Errors that can occur during WAL batch framing and decoding.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum WalFrameError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("WAL frame corruption: {0}")]
    Corruption(String),
}

/// Payload serialized within a framed WAL batch, carrying operations and optional mutation metadata for deduplication.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WalBatchPayload {
    pub mutation_id: Option<MutationId>,
    pub ops: Vec<SequencedOperation>,
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
    /// Successfully decoded a batch of sequenced operations, optional mutation metadata, and the number of bytes consumed.
    Ok {
        ops: Vec<SequencedOperation>,
        mutation_id: Option<MutationId>,
        bytes_consumed: usize,
    },
    /// Clean end-of-file reached (no more bytes remaining in WAL segment).
    CleanEof,
    /// Incomplete batch found at EOF (torn write caused by power loss/crash).
    /// Indicates the byte count consumed so far before encountering the incomplete batch.
    TornWrite {
        valid_bytes_offset: usize,
        reason: String,
    },
}

/// Encodes a batch of `SequencedOperation` along with optional `MutationId` metadata into an append-only, atomically framed WAL batch.
///
/// Framing:
/// - `magic`: 2 bytes (`0xBA7C`, little-endian: `[0xBA, 0x7C]`)
/// - `batch_len`: 4 bytes (`u32`, little-endian payload length)
/// - `batch_crc32`: 4 bytes (`u32`, little-endian CRC32 checksum of payload)
/// - `ops_count`: 4 bytes (`u32`, little-endian count of operations in batch)
/// - `payload`: serialized `WalBatchPayload` via bincode
pub fn encode_wal_batch(
    ops: &[SequencedOperation],
    mutation_id: Option<MutationId>,
) -> Result<Vec<u8>, WalFrameError> {
    let payload_struct = WalBatchPayloadRef { mutation_id, ops };
    let payload = bincode::serialize(&payload_struct)
        .map_err(|e| WalFrameError::Serialization(e.to_string()))?;
    if payload.len() as u64 > MAX_MESSAGE_SIZE {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch payload size {} exceeds MAX_MESSAGE_SIZE limit {}",
            payload.len(),
            MAX_MESSAGE_SIZE
        )));
    }
    let batch_len = payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    let ops_count = ops.len() as u32;

    let mut record = Vec::with_capacity(BATCH_HEADER_SIZE + payload.len());
    record.extend_from_slice(&BATCH_MAGIC);
    record.extend_from_slice(&batch_len.to_le_bytes());
    record.extend_from_slice(&crc.to_le_bytes());
    record.extend_from_slice(&ops_count.to_le_bytes());
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

/// Encodes a single `SequencedOperation` along with optional `MutationId` metadata into an append-only WAL batch.
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
    /// depends on what follows the frame; see [`classify_checksum_mismatch`].
    ChecksumMismatch { expected: u32, actual: u32 },
}

/// Parses the header of a frame.
///
/// Returns `Ok(None)` when every byte of the header is zero, which is not a frame: it is a
/// zero-filled tail only if every byte up to the end of the log is zero too (see
/// [`classify_zeroed_header`]). Any other invalid magic, or a payload length above
/// `MAX_MESSAGE_SIZE`, is corruption.
///
/// Together with [`decode_batch_payload`] and the `classify_*` functions, this is the decoder
/// used by [`decode_wal_batch_from_slice`], exposed for readers that stream a log instead of
/// holding it in memory, so that both apply the same torn-write rules.
pub fn parse_batch_header(
    bytes: &[u8; BATCH_HEADER_SIZE],
) -> Result<Option<BatchHeader>, WalFrameError> {
    let magic = [bytes[0], bytes[1]];
    if magic != BATCH_MAGIC {
        if bytes.iter().all(|&b| b == 0) {
            return Ok(None);
        }
        return Err(invalid_magic(magic));
    }

    let [_, _, l0, l1, l2, l3, c0, c1, c2, c3, n0, n1, n2, n3] = *bytes;
    let payload_len = u32::from_le_bytes([l0, l1, l2, l3]);
    if u64::from(payload_len) > MAX_MESSAGE_SIZE {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch length {payload_len} exceeds MAX_MESSAGE_SIZE {MAX_MESSAGE_SIZE}"
        )));
    }

    Ok(Some(BatchHeader {
        payload_len: payload_len as usize,
        payload_crc32: u32::from_le_bytes([c0, c1, c2, c3]),
        ops_count: u32::from_le_bytes([n0, n1, n2, n3]) as usize,
    }))
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

    let (ops, mutation_id) = match bincode::deserialize::<WalBatchPayload>(payload) {
        Ok(batch) => (batch.ops, batch.mutation_id),
        Err(_) => {
            // Fallback for payloads that serialized Vec<SequencedOperation> directly
            let ops: Vec<SequencedOperation> = bincode::deserialize(payload).map_err(|e| {
                WalFrameError::Corruption(format!(
                    "Failed to deserialize WAL batch operations: {e}"
                ))
            })?;
            (ops, None)
        }
    };

    if ops.len() != header.ops_count {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch ops count mismatch: expected {}, got {}",
            header.ops_count,
            ops.len()
        )));
    }

    Ok(BatchPayloadDecode::Batch { ops, mutation_id })
}

/// Decides what a frame whose payload does not match its checksum means, given the bytes that
/// follow the frame (only the first `BATCH_HEADER_SIZE` are looked at).
///
/// If a complete frame header follows, later writes were made after the damaged frame was
/// durable, so the damage is corruption in the middle of the log and dropping the tail would
/// lose data. Otherwise the frame is the last one, cut short by a crash: a torn write, whose
/// description is returned.
pub fn classify_checksum_mismatch(
    expected: u32,
    actual: u32,
    following: &[u8],
) -> Result<String, WalFrameError> {
    let frame_follows = following.len() >= BATCH_HEADER_SIZE && following[0..2] == BATCH_MAGIC;
    if frame_follows {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch CRC32 mismatch: expected {expected}, actual {actual}"
        )));
    }
    Ok(format!(
        "WAL batch CRC32 mismatch at EOF (expected {expected}, actual {actual}), treating as torn write"
    ))
}

/// Decides what a frame position holding a zeroed header means (see [`parse_batch_header`]).
///
/// `tail_is_zero` tells whether every byte from that position to the end of the log is zero,
/// and `tail_len` is the number of those bytes. A zero-filled tail, as left by file systems that
/// extend a file before writing its data, is a torn write, whose description is returned; zeros
/// followed by anything else are corruption.
pub fn classify_zeroed_header(tail_is_zero: bool, tail_len: u64) -> Result<String, WalFrameError> {
    if !tail_is_zero {
        return Err(invalid_magic([0, 0]));
    }
    Ok(format!(
        "Zero-filled tail at EOF ({tail_len} zero bytes), treating as torn write"
    ))
}

fn invalid_magic(magic: [u8; 2]) -> WalFrameError {
    WalFrameError::Corruption(format!(
        "Invalid WAL batch magic: expected {:?}, got {:?}",
        BATCH_MAGIC, magic
    ))
}

/// Decodes the next framed WAL batch from a byte slice.
///
/// Returns `WalBatchDecodeResult::Ok` if a complete batch was decoded,
/// `WalBatchDecodeResult::CleanEof` if the slice is empty,
/// or `WalBatchDecodeResult::TornWrite` if a partial batch, a zero-filled tail or a terminal CRC
/// mismatch is present at EOF.
/// Returns `Err(WalFrameError::Corruption)` if invalid magic, bit-flip, or corruption followed by
/// a complete frame header is detected.
pub fn decode_wal_batch_from_slice(slice: &[u8]) -> Result<WalBatchDecodeResult, WalFrameError> {
    if slice.is_empty() {
        return Ok(WalBatchDecodeResult::CleanEof);
    }

    let torn = |reason: String| {
        Ok(WalBatchDecodeResult::TornWrite {
            valid_bytes_offset: 0,
            reason,
        })
    };

    let Some(header_bytes) = slice.first_chunk::<BATCH_HEADER_SIZE>() else {
        return torn(format!(
            "Incomplete WAL batch header: available {} bytes, expected at least {}",
            slice.len(),
            BATCH_HEADER_SIZE
        ));
    };

    let Some(header) = parse_batch_header(header_bytes)? else {
        let tail_is_zero = slice.iter().all(|&b| b == 0);
        return torn(classify_zeroed_header(tail_is_zero, slice.len() as u64)?);
    };

    let frame_len = header.frame_len();
    if slice.len() < frame_len {
        return torn(format!(
            "Truncated WAL batch payload: available {} bytes, expected {}",
            slice.len(),
            frame_len
        ));
    }

    match decode_batch_payload(&header, &slice[BATCH_HEADER_SIZE..frame_len])? {
        BatchPayloadDecode::Batch { ops, mutation_id } => Ok(WalBatchDecodeResult::Ok {
            ops,
            mutation_id,
            bytes_consumed: frame_len,
        }),
        BatchPayloadDecode::ChecksumMismatch { expected, actual } => {
            let following = &slice[frame_len..];
            let following = &following[..following.len().min(BATCH_HEADER_SIZE)];
            torn(classify_checksum_mismatch(expected, actual, following)?)
        }
    }
}

/// Helper to read all valid WAL records from a byte buffer.
///
/// Returns the decoded operations and the total number of valid bytes consumed.
/// If a torn write is encountered at EOF, it stops safely and reports the valid offset,
/// allowing the caller to truncate the damaged tail.
pub fn replay_wal_records(
    wal_bytes: &[u8],
) -> Result<(Vec<SequencedOperation>, usize, Option<String>), WalFrameError> {
    let mut all_ops = Vec::new();
    let mut offset = 0;
    let mut torn_write_detected = None;

    while offset < wal_bytes.len() {
        match decode_wal_batch_from_slice(&wal_bytes[offset..])? {
            WalBatchDecodeResult::Ok {
                ops,
                bytes_consumed,
                ..
            } => {
                all_ops.extend(ops);
                offset += bytes_consumed;
            }
            WalBatchDecodeResult::CleanEof => break,
            WalBatchDecodeResult::TornWrite { reason, .. } => {
                torn_write_detected = Some(reason);
                break;
            }
        }
    }

    Ok((all_ops, offset, torn_write_detected))
}
