use rimdb_core::{SequencedOperation, MAX_MESSAGE_SIZE};

use crate::error::StorageError;

/// Magic bytes identifying a RimDB table room file ("RIM1").
pub const MAGIC: [u8; 4] = *b"RIM1";

/// Alias for magic bytes for backward compatibility.
pub const MAGIC_BYTES: [u8; 4] = MAGIC;

/// Current binary format version.
pub const FORMAT_VERSION: u16 = 1;

/// Alias for format version for backward compatibility.
pub const FILE_VERSION: u16 = FORMAT_VERSION;

/// Fixed file header size (exactly 64 bytes, L1 cache line aligned).
pub const HEADER_SIZE: usize = 64;

/// Header of a `.rimdb` database room file.
///
/// Layout:
/// - `magic`: 4 bytes (`b"RIM1"`)
/// - `version`: 2 bytes (`u16`, little-endian)
/// - `flags`: 2 bytes (`u16`, little-endian)
/// - `snapshot_seq`: 8 bytes (`u64`, little-endian)
/// - `head_seq`: 8 bytes (`u64`, little-endian)
/// - `snapshot_compressed_len`: 8 bytes (`u64`, little-endian)
/// - `header_crc`: 4 bytes (`u32`, little-endian CRC32 over preceding 32 bytes)
/// - `reserved`: 28 bytes padding (zeros)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub flags: u16,
    pub snapshot_seq: u64,
    pub head_seq: u64,
    pub snapshot_compressed_len: u64,
    pub header_crc: u32,
    pub reserved: [u8; 28],
}

impl FileHeader {
    /// Creates a new `FileHeader` with computed CRC32 checksum and zeroed padding.
    pub fn new(snapshot_seq: u64, head_seq: u64, snapshot_compressed_len: u64) -> Self {
        let mut header = Self {
            magic: MAGIC,
            version: FORMAT_VERSION,
            flags: 0,
            snapshot_seq,
            head_seq,
            snapshot_compressed_len,
            header_crc: 0,
            reserved: [0u8; 28],
        };
        header.header_crc = header.compute_crc();
        header
    }

    /// Computes the CRC32 checksum over the first 32 bytes of the header.
    fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.snapshot_seq.to_le_bytes());
        buf[16..24].copy_from_slice(&self.head_seq.to_le_bytes());
        buf[24..32].copy_from_slice(&self.snapshot_compressed_len.to_le_bytes());
        crc32fast::hash(&buf)
    }

    /// Encodes the header into an exact 64-byte array.
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.flags.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.snapshot_seq.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.head_seq.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.snapshot_compressed_len.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.header_crc.to_le_bytes());
        bytes[36..64].copy_from_slice(&self.reserved);
        bytes
    }

    /// Decodes and validates a `FileHeader` from a 64-byte buffer.
    pub fn decode(bytes: &[u8; HEADER_SIZE]) -> Result<Self, StorageError> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if magic != MAGIC {
            return Err(StorageError::WalCorruption(format!(
                "Invalid magic bytes in file header: expected RIM1, got {:?}",
                magic
            )));
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != FORMAT_VERSION {
            return Err(StorageError::WalCorruption(format!(
                "Unsupported file version: expected {FORMAT_VERSION}, got {version}"
            )));
        }

        let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
        let snapshot_seq = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let head_seq = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let snapshot_compressed_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let header_crc = u32::from_le_bytes(bytes[32..36].try_into().unwrap());

        let mut reserved = [0u8; 28];
        reserved.copy_from_slice(&bytes[36..64]);

        let header = Self {
            magic,
            version,
            flags,
            snapshot_seq,
            head_seq,
            snapshot_compressed_len,
            header_crc,
            reserved,
        };

        let computed_crc = header.compute_crc();
        if header.header_crc != computed_crc {
            return Err(StorageError::WalCorruption(format!(
                "Corrupted file header: CRC32 mismatch (expected {}, got {})",
                header.header_crc, computed_crc
            )));
        }

        Ok(header)
    }
}

/// Magic bytes identifying a framed WAL batch (0xBA7C).
pub const BATCH_MAGIC: [u8; 2] = [0xBA, 0x7C];

/// Fixed WAL batch header size: 2B magic + 4B batch_len + 4B batch_crc32 + 4B ops_count = 14 bytes.
pub const BATCH_HEADER_SIZE: usize = 14;

/// Result of attempting to decode a WAL batch from a byte buffer.
#[derive(Debug, PartialEq)]
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
#[derive(Debug, PartialEq)]
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
pub fn encode_wal_batch(ops: &[SequencedOperation]) -> Result<Vec<u8>, StorageError> {
    let payload = bincode::serialize(ops).map_err(|e| StorageError::Serialization(e.to_string()))?;
    if payload.len() as u64 > MAX_MESSAGE_SIZE {
        return Err(StorageError::WalCorruption(format!(
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
pub fn encode_wal_record(op: &SequencedOperation) -> Result<Vec<u8>, StorageError> {
    encode_wal_batch(std::slice::from_ref(op))
}

/// Decodes the next framed WAL batch from a byte slice.
///
/// Returns `WalBatchDecodeResult::Ok` if a complete batch was decoded,
/// `WalBatchDecodeResult::CleanEof` if the slice is empty,
/// or `WalBatchDecodeResult::TornWrite` if a partial batch is present at EOF.
/// Returns `Err(StorageError::WalCorruption)` if invalid magic, bit-flip, or corruption is detected.
pub fn decode_wal_batch_from_slice(slice: &[u8]) -> Result<WalBatchDecodeResult, StorageError> {
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
        return Err(StorageError::WalCorruption(format!(
            "Invalid WAL batch magic: expected {:?}, got {:?}",
            BATCH_MAGIC, magic
        )));
    }

    let batch_len = u32::from_le_bytes(slice[2..6].try_into().unwrap()) as usize;
    let expected_crc = u32::from_le_bytes(slice[6..10].try_into().unwrap());
    let ops_count = u32::from_le_bytes(slice[10..14].try_into().unwrap()) as usize;

    if batch_len as u64 > MAX_MESSAGE_SIZE {
        return Err(StorageError::WalCorruption(format!(
            "WAL batch length {batch_len} exceeds MAX_MESSAGE_SIZE {MAX_MESSAGE_SIZE}"
        )));
    }

    let total_expected_len = match BATCH_HEADER_SIZE.checked_add(batch_len) {
        Some(len) => len,
        None => {
            return Err(StorageError::WalCorruption(
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
        return Err(StorageError::WalCorruption(format!(
            "WAL batch CRC32 mismatch: expected {expected_crc}, actual {actual_crc}"
        )));
    }

    let ops: Vec<SequencedOperation> = bincode::deserialize(payload)
        .map_err(|e| StorageError::WalCorruption(format!("Failed to deserialize WAL batch operations: {e}")))?;

    if ops.len() != ops_count {
        return Err(StorageError::WalCorruption(format!(
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
pub fn decode_wal_record_from_slice(slice: &[u8]) -> Result<WalDecodeResult, StorageError> {
    match decode_wal_batch_from_slice(slice)? {
        WalBatchDecodeResult::Ok { mut ops, bytes_consumed } => {
            if let Some(op) = ops.drain(..).next() {
                Ok(WalDecodeResult::Ok { op, bytes_consumed })
            } else {
                Err(StorageError::WalCorruption("Empty WAL batch".to_string()))
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
) -> Result<(Vec<SequencedOperation>, usize, Option<String>), StorageError> {
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
