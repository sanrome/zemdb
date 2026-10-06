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

/// Result of attempting to decode a single WAL entry from a byte buffer.
#[derive(Debug, PartialEq, Clone)]
pub enum WalDecodeResult {
    /// Successfully decoded a sequenced operation, optional mutation metadata, and the number of bytes consumed.
    Ok {
        op: SequencedOperation,
        mutation_id: Option<MutationId>,
        bytes_consumed: usize,
    },
    /// Clean end-of-file reached (no more bytes remaining in WAL segment).
    CleanEof,
    /// Incomplete record found at EOF (torn write caused by power loss/crash).
    /// Indicates the byte count consumed so far before encountering the incomplete record.
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

/// Decodes the next framed WAL batch from a byte slice.
///
/// Returns `WalBatchDecodeResult::Ok` if a complete batch was decoded,
/// `WalBatchDecodeResult::CleanEof` if the slice is empty,
/// or `WalBatchDecodeResult::TornWrite` if a partial batch or terminal CRC mismatch is present at EOF.
/// Returns `Err(WalFrameError::Corruption)` if invalid magic, bit-flip, or corruption followed by valid frames is detected.
pub fn decode_wal_batch_from_slice(slice: &[u8]) -> Result<WalBatchDecodeResult, WalFrameError> {
    if slice.is_empty() {
        return Ok(WalBatchDecodeResult::CleanEof);
    }

    if slice.len() < BATCH_HEADER_SIZE {
        return Ok(WalBatchDecodeResult::TornWrite {
            valid_bytes_offset: 0,
            reason: format!(
                "Incomplete WAL batch header: available {} bytes, expected at least {}",
                slice.len(),
                BATCH_HEADER_SIZE
            ),
        });
    }

    let magic = [slice[0], slice[1]];
    if magic != BATCH_MAGIC {
        if slice.iter().all(|&b| b == 0) {
            return Ok(WalBatchDecodeResult::TornWrite {
                valid_bytes_offset: 0,
                reason: format!(
                    "Zero-filled tail at EOF ({} zero bytes), treating as torn write",
                    slice.len()
                ),
            });
        }
        return Err(WalFrameError::Corruption(format!(
            "Invalid WAL batch magic: expected {:?}, got {:?}",
            BATCH_MAGIC, magic
        )));
    }

    let batch_len = u32::from_le_bytes(slice[2..6].try_into().unwrap()) as usize;
    let expected_crc = u32::from_le_bytes(slice[6..10].try_into().unwrap());
    let ops_count = u32::from_le_bytes(slice[10..14].try_into().unwrap()) as usize;

    if batch_len as u64 > MAX_MESSAGE_SIZE {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch length {batch_len} exceeds MAX_MESSAGE_SIZE {MAX_MESSAGE_SIZE}"
        )));
    }

    let total_expected_len = match BATCH_HEADER_SIZE.checked_add(batch_len) {
        Some(len) => len,
        None => {
            return Err(WalFrameError::Corruption(
                "WAL batch length caused integer overflow".to_string(),
            ));
        }
    };

    if slice.len() < total_expected_len {
        return Ok(WalBatchDecodeResult::TornWrite {
            valid_bytes_offset: 0,
            reason: format!(
                "Truncated WAL batch payload: available {} bytes, expected {}",
                slice.len(),
                total_expected_len
            ),
        });
    }

    let payload = &slice[BATCH_HEADER_SIZE..total_expected_len];
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        // If there are no subsequent valid batch headers remaining, treat CRC mismatch as a torn write at EOF
        let remaining = &slice[total_expected_len..];
        let has_subsequent_valid_batch = if remaining.len() >= BATCH_HEADER_SIZE {
            remaining[0..2] == BATCH_MAGIC
        } else {
            false
        };

        if !has_subsequent_valid_batch {
            return Ok(WalBatchDecodeResult::TornWrite {
                valid_bytes_offset: 0,
                reason: format!(
                    "WAL batch CRC32 mismatch at EOF (expected {expected_crc}, actual {actual_crc}), treating as torn write"
                ),
            });
        }

        return Err(WalFrameError::Corruption(format!(
            "WAL batch CRC32 mismatch: expected {expected_crc}, actual {actual_crc}"
        )));
    }

    let (ops, mutation_id) = match bincode::deserialize::<WalBatchPayload>(payload) {
        Ok(batch) => (batch.ops, batch.mutation_id),
        Err(_) => {
            // Fallback for slices that serialized Vec<SequencedOperation> directly
            let ops: Vec<SequencedOperation> = bincode::deserialize(payload).map_err(|e| {
                WalFrameError::Corruption(format!(
                    "Failed to deserialize WAL batch operations: {e}"
                ))
            })?;
            (ops, None)
        }
    };

    if ops.len() != ops_count {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch ops count mismatch: expected {ops_count}, got {}",
            ops.len()
        )));
    }

    Ok(WalBatchDecodeResult::Ok {
        ops,
        mutation_id,
        bytes_consumed: total_expected_len,
    })
}

/// Decodes the next WAL record from a byte slice.
pub fn decode_wal_record_from_slice(slice: &[u8]) -> Result<WalDecodeResult, WalFrameError> {
    match decode_wal_batch_from_slice(slice)? {
        WalBatchDecodeResult::Ok {
            mut ops,
            mutation_id,
            bytes_consumed,
        } => {
            if let Some(op) = ops.drain(..).next() {
                Ok(WalDecodeResult::Ok {
                    op,
                    mutation_id,
                    bytes_consumed,
                })
            } else {
                Err(WalFrameError::Corruption("Empty WAL batch".to_string()))
            }
        }
        WalBatchDecodeResult::CleanEof => Ok(WalDecodeResult::CleanEof),
        WalBatchDecodeResult::TornWrite {
            valid_bytes_offset,
            reason,
        } => Ok(WalDecodeResult::TornWrite {
            valid_bytes_offset,
            reason,
        }),
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
