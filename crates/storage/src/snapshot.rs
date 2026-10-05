//! Encoding and decoding of complete `ZMSN` snapshot envelopes.
//!
//! The header format and its validation live in `zemdb-core`
//! ([`zemdb_core::protocol::snapshot_envelope`]), shared with the server's snapshot relay; this
//! module adds compression and decompression of the body.

use crate::error::StorageError;
pub use zemdb_core::protocol::snapshot_envelope::{
    SnapshotCompression, SnapshotEnvelopeHeader, SNAPSHOT_HEADER_LEN, SNAPSHOT_MAGIC,
    SNAPSHOT_VERSION,
};

/// Compression flag indicating uncompressed raw Bincode payload.
pub const COMPRESSION_RAW: u8 = SnapshotCompression::Raw.flag();

/// Compression flag indicating Zstandard-compressed payload.
pub const COMPRESSION_ZSTD: u8 = SnapshotCompression::Zstd.flag();

/// Default upper bound for the decompressed payload of a snapshot (2 GiB).
///
/// The declared length comes from the envelope, which may come from another peer, so it is
/// never trusted to size an allocation or to bound the decompression on its own.
pub const DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Largest buffer reserved up front while decompressing; beyond it the buffer grows with the
/// data actually produced.
#[cfg(not(target_arch = "wasm32"))]
const MAX_DECOMPRESSION_PREALLOCATION: usize = 64 * 1024 * 1024;

/// Encapsulates a serialized room snapshot into the universal canonical container.
pub fn encode_snapshot_envelope(
    payload_bytes: &[u8],
    compress_zstd: bool,
    #[allow(unused_variables)] zstd_level: i32,
) -> Result<Vec<u8>, StorageError> {
    let uncompressed_len = u32::try_from(payload_bytes.len()).map_err(|_| {
        StorageError::Serialization(format!(
            "Snapshot payload of {} bytes exceeds the envelope's 4 GiB limit",
            payload_bytes.len()
        ))
    })?;

    #[cfg(not(target_arch = "wasm32"))]
    let (body_bytes, compression) = if compress_zstd {
        let compressed = zstd::encode_all(payload_bytes, zstd_level)
            .map_err(|e| StorageError::Serialization(format!("Zstd compression failed: {e}")))?;
        (compressed, SnapshotCompression::Zstd)
    } else {
        (payload_bytes.to_vec(), SnapshotCompression::Raw)
    };

    #[cfg(target_arch = "wasm32")]
    let (body_bytes, compression) = {
        let _ = compress_zstd;
        (payload_bytes.to_vec(), SnapshotCompression::Raw)
    };

    let header = SnapshotEnvelopeHeader::for_body(compression, uncompressed_len, &body_bytes);

    let mut envelope = Vec::with_capacity(SNAPSHOT_HEADER_LEN + body_bytes.len());
    envelope.extend_from_slice(&header.to_bytes());
    envelope.extend_from_slice(&body_bytes);

    Ok(envelope)
}

/// Decodes and validates a universal snapshot envelope, returning the uncompressed Bincode
/// payload. The payload may not exceed [`DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES`]; see
/// [`decode_snapshot_envelope_with_limit`].
pub fn decode_snapshot_envelope(envelope_bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
    decode_snapshot_envelope_with_limit(envelope_bytes, DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES)
}

/// Decodes and validates a snapshot envelope whose decompressed payload may not exceed
/// `max_uncompressed_bytes`.
///
/// An envelope declaring a larger payload is rejected before decompressing. Decompression
/// itself stops one byte past the declared length, so a header that understates the payload
/// cannot make it produce more than that; the result must match the declared length exactly.
pub fn decode_snapshot_envelope_with_limit(
    envelope_bytes: &[u8],
    max_uncompressed_bytes: u64,
) -> Result<Vec<u8>, StorageError> {
    let header = SnapshotEnvelopeHeader::parse(envelope_bytes)
        .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;
    let body = &envelope_bytes[SNAPSHOT_HEADER_LEN..];
    header
        .verify_body(body)
        .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

    let declared_len = u64::from(header.uncompressed_len());
    if declared_len > max_uncompressed_bytes {
        return Err(StorageError::SnapshotCorruption(format!(
            "Declared snapshot size {declared_len} exceeds the maximum of {max_uncompressed_bytes} bytes"
        )));
    }

    match header.compression() {
        // `verify_body` already checked that a raw body has the declared length.
        SnapshotCompression::Raw => Ok(body.to_vec()),
        SnapshotCompression::Zstd => decompress_bounded(body, declared_len),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn decompress_bounded(body: &[u8], declared_len: u64) -> Result<Vec<u8>, StorageError> {
    use std::io::Read;

    let decoder = zstd::stream::read::Decoder::new(body)
        .map_err(|e| StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}")))?;
    let capacity = usize::try_from(declared_len)
        .unwrap_or(usize::MAX)
        .min(MAX_DECOMPRESSION_PREALLOCATION);
    let mut decompressed = Vec::with_capacity(capacity);
    decoder
        .take(declared_len.saturating_add(1))
        .read_to_end(&mut decompressed)
        .map_err(|e| StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}")))?;

    let actual_len = decompressed.len() as u64;
    if actual_len > declared_len {
        return Err(StorageError::SnapshotCorruption(format!(
            "Decompressed snapshot is larger than declared ({declared_len} bytes)"
        )));
    }
    if actual_len < declared_len {
        return Err(StorageError::SnapshotCorruption(format!(
            "Decompressed snapshot length mismatch: expected {declared_len}, actual {actual_len}"
        )));
    }
    Ok(decompressed)
}

#[cfg(target_arch = "wasm32")]
fn decompress_bounded(_body: &[u8], _declared_len: u64) -> Result<Vec<u8>, StorageError> {
    Err(StorageError::SnapshotCorruption(
        "Zstandard-compressed snapshots are not supported in wasm32 build".to_string(),
    ))
}
