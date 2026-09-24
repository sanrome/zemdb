use thiserror::Error;

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

/// Result of attempting to decode a WAL batch from a byte buffer.
#[derive(Debug, PartialEq, Clone)]
pub enum WalBatchDecodeResult {
    /// Successfully decoded a batch of sequenced operations and the number of bytes consumed.
    Ok {
        ops: Vec<SequencedOperation>,
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
    /// Successfully decoded a sequenced operation and the number of bytes consumed.
    Ok {
        op: SequencedOperation,
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

/// Encodes a batch of `SequencedOperation` into an append-only, atomically framed WAL batch.
///
/// Framing:
/// - `magic`: 2 bytes (`0xBA7C`, little-endian: `[0xBA, 0x7C]`)
/// - `batch_len`: 4 bytes (`u32`, little-endian payload length)
/// - `batch_crc32`: 4 bytes (`u32`, little-endian CRC32 checksum of payload)
/// - `ops_count`: 4 bytes (`u32`, little-endian count of operations in batch)
/// - `payload`: serialized `Vec<SequencedOperation>` via bincode
pub fn encode_wal_batch(ops: &[SequencedOperation]) -> Result<Vec<u8>, WalFrameError> {
    let payload = bincode::serialize(ops).map_err(|e| WalFrameError::Serialization(e.to_string()))?;
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

/// Encodes a single `SequencedOperation` into an append-only WAL batch.
pub fn encode_wal_record(op: &SequencedOperation) -> Result<Vec<u8>, WalFrameError> {
    encode_wal_batch(std::slice::from_ref(op))
}

/// Decodes the next framed WAL batch from a byte slice.
///
/// Returns `WalBatchDecodeResult::Ok` if a complete batch was decoded,
/// `WalBatchDecodeResult::CleanEof` if the slice is empty,
/// or `WalBatchDecodeResult::TornWrite` if a partial batch is present at EOF.
/// Returns `Err(WalFrameError::Corruption)` if invalid magic, bit-flip, or corruption is detected.
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
        return Err(WalFrameError::Corruption(format!(
            "WAL batch CRC32 mismatch: expected {expected_crc}, actual {actual_crc}"
        )));
    }

    let ops: Vec<SequencedOperation> = bincode::deserialize(payload).map_err(|e| {
        WalFrameError::Corruption(format!("Failed to deserialize WAL batch operations: {e}"))
    })?;

    if ops.len() != ops_count {
        return Err(WalFrameError::Corruption(format!(
            "WAL batch ops count mismatch: expected {ops_count}, got {}",
            ops.len()
        )));
    }

    Ok(WalBatchDecodeResult::Ok {
        ops,
        bytes_consumed: total_expected_len,
    })
}

/// Decodes the next WAL record from a byte slice.
pub fn decode_wal_record_from_slice(slice: &[u8]) -> Result<WalDecodeResult, WalFrameError> {
    match decode_wal_batch_from_slice(slice)? {
        WalBatchDecodeResult::Ok {
            mut ops,
            bytes_consumed,
        } => {
            if let Some(op) = ops.drain(..).next() {
                Ok(WalDecodeResult::Ok { op, bytes_consumed })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Operation;
    use crate::value::{CompactRow, PrimaryKey, Value};

    #[test]
    fn test_wal_frame_roundtrip() {
        let row = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
        let op = SequencedOperation::new(1, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
        let batch = vec![op.clone()];

        let encoded = encode_wal_batch(&batch).expect("encode ok");
        assert_eq!(&encoded[0..2], &BATCH_MAGIC);

        let decoded = decode_wal_batch_from_slice(&encoded).expect("decode ok");
        match decoded {
            WalBatchDecodeResult::Ok {
                ops,
                bytes_consumed,
            } => {
                assert_eq!(ops, batch);
                assert_eq!(bytes_consumed, encoded.len());
            }
            other => panic!("Expected Ok, got {:?}", other),
        }
    }

    #[test]
    fn test_wal_frame_detects_crc_tampering() {
        let op = SequencedOperation::new(1, Operation::delete(0, PrimaryKey::single(1i64), 100));
        let mut encoded = encode_wal_batch(&[op]).expect("encode ok");

        // Tamper with payload byte
        let last_idx = encoded.len() - 1;
        encoded[last_idx] ^= 0xFF;

        let err = decode_wal_batch_from_slice(&encoded).unwrap_err();
        assert!(matches!(err, WalFrameError::Corruption(_)));
    }

    #[test]
    fn test_wal_frame_torn_write_detection() {
        let op = SequencedOperation::new(1, Operation::delete(0, PrimaryKey::single(1i64), 100));
        let encoded = encode_wal_batch(&[op]).expect("encode ok");

        // Truncate to just partial header
        let partial = &encoded[0..5];
        let res = decode_wal_batch_from_slice(partial).expect("returns TornWrite");
        assert!(matches!(res, WalBatchDecodeResult::TornWrite { .. }));
    }

    #[test]
    fn test_wal_frame_zero_filled_eof_torn_write_detection() {
        // Zero-filled tail at EOF (e.g., 64 zero bytes from power outage fallocate)
        let zeros = vec![0u8; 64];
        let res = decode_wal_batch_from_slice(&zeros).expect("returns TornWrite for zero tail");
        assert!(matches!(res, WalBatchDecodeResult::TornWrite { valid_bytes_offset: 0, .. }));
    }
}
