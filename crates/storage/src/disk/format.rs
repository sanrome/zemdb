use zemdb_core::{MutationId, SequencedOperation};

use crate::error::StorageError;

/// Magic bytes identifying a ZemDB table room file ("ZEM1").
pub const MAGIC: [u8; 4] = *b"ZEM1";

/// Alias for magic bytes for backward compatibility.
pub const MAGIC_BYTES: [u8; 4] = MAGIC;

/// Current binary format version.
pub const FORMAT_VERSION: u16 = 1;

/// Alias for format version for backward compatibility.
pub const FILE_VERSION: u16 = FORMAT_VERSION;

/// Fixed file header size (exactly 64 bytes, L1 cache line aligned).
pub const HEADER_SIZE: usize = 64;

/// Header of a `.snap` database room snapshot file.
///
/// Layout:
/// - `magic`: 4 bytes (`b"ZEM1"`)
/// - `version`: 2 bytes (`u16`, little-endian)
/// - `flags`: 2 bytes (`u16`, little-endian)
/// - `snapshot_seq`: 8 bytes (`u64`, little-endian)
/// - `head_seq`: 8 bytes (`u64`, little-endian)
/// - `snapshot_compressed_len`: 8 bytes (`u64`, little-endian)
/// - `snapshot_payload_crc32`: 4 bytes (`u32`, little-endian CRC32 of compressed payload)
/// - `header_crc`: 4 bytes (`u32`, little-endian CRC32 over preceding 36 bytes)
/// - `reserved`: 24 bytes padding (zeros)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub flags: u16,
    pub snapshot_seq: u64,
    pub head_seq: u64,
    pub snapshot_compressed_len: u64,
    pub snapshot_payload_crc32: u32,
    pub header_crc: u32,
    pub reserved: [u8; 24],
}

impl FileHeader {
    /// Creates a new `FileHeader` with computed CRC32 checksums and zeroed padding.
    pub fn new(
        snapshot_seq: u64,
        head_seq: u64,
        snapshot_compressed_len: u64,
        snapshot_payload_crc32: u32,
    ) -> Self {
        let mut header = Self {
            magic: MAGIC,
            version: FORMAT_VERSION,
            flags: 0,
            snapshot_seq,
            head_seq,
            snapshot_compressed_len,
            snapshot_payload_crc32,
            header_crc: 0,
            reserved: [0u8; 24],
        };
        header.header_crc = header.compute_crc();
        header
    }

    /// Computes the CRC32 checksum over the first 36 bytes of the header.
    fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; 36];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.snapshot_seq.to_le_bytes());
        buf[16..24].copy_from_slice(&self.head_seq.to_le_bytes());
        buf[24..32].copy_from_slice(&self.snapshot_compressed_len.to_le_bytes());
        buf[32..36].copy_from_slice(&self.snapshot_payload_crc32.to_le_bytes());
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
        bytes[32..36].copy_from_slice(&self.snapshot_payload_crc32.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.header_crc.to_le_bytes());
        bytes[40..64].copy_from_slice(&self.reserved);
        bytes
    }

    /// Decodes and validates a `FileHeader` from a 64-byte buffer.
    pub fn decode(bytes: &[u8; HEADER_SIZE]) -> Result<Self, StorageError> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if magic != MAGIC {
            return Err(StorageError::WalCorruption(format!(
                "Invalid magic bytes in file header: expected ZEM1, got {:?}",
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
        let snapshot_payload_crc32 = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
        let header_crc = u32::from_le_bytes(bytes[36..40].try_into().unwrap());

        let mut reserved = [0u8; 24];
        reserved.copy_from_slice(&bytes[40..64]);

        let header = Self {
            magic,
            version,
            flags,
            snapshot_seq,
            head_seq,
            snapshot_compressed_len,
            snapshot_payload_crc32,
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

pub use zemdb_core::protocol::wal_frame::{
    WalBatchDecodeResult, WalDecodeResult, WalFrameError, BATCH_HEADER_SIZE, BATCH_MAGIC,
};

/// Encodes a batch of `SequencedOperation` into an append-only, atomically framed WAL batch.
pub fn encode_wal_batch(
    ops: &[SequencedOperation],
    mutation_id: Option<MutationId>,
) -> Result<Vec<u8>, StorageError> {
    zemdb_core::protocol::wal_frame::encode_wal_batch(ops, mutation_id).map_err(Into::into)
}

/// Encodes a single `SequencedOperation` into an append-only WAL batch.
pub fn encode_wal_record(
    op: &SequencedOperation,
    mutation_id: Option<MutationId>,
) -> Result<Vec<u8>, StorageError> {
    zemdb_core::protocol::wal_frame::encode_wal_record(op, mutation_id).map_err(Into::into)
}

/// Decodes the next framed WAL batch from a byte slice.
pub fn decode_wal_batch_from_slice(slice: &[u8]) -> Result<WalBatchDecodeResult, StorageError> {
    zemdb_core::protocol::wal_frame::decode_wal_batch_from_slice(slice).map_err(Into::into)
}

/// Decodes the next WAL record from a byte slice.
pub fn decode_wal_record_from_slice(slice: &[u8]) -> Result<WalDecodeResult, StorageError> {
    zemdb_core::protocol::wal_frame::decode_wal_record_from_slice(slice).map_err(Into::into)
}

/// Helper to read all valid WAL records from a byte buffer.
pub fn replay_wal_records(
    wal_bytes: &[u8],
) -> Result<(Vec<SequencedOperation>, usize, Option<String>), StorageError> {
    zemdb_core::protocol::wal_frame::replay_wal_records(wal_bytes).map_err(Into::into)
}
