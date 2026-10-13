//! Encoding and decoding of complete `ZMSN` snapshot envelopes.
//!
//! The header format and its validation live in `zemdb-core`
//! ([`zemdb_core::protocol::snapshot_envelope`]), shared with the server's snapshot relay; this
//! module adds compression and decompression of the body.

use crate::error::StorageError;
use bincode::Options;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
#[cfg(not(target_arch = "wasm32"))]
use std::io::{BufReader, Read};
pub use zemdb_core::protocol::snapshot_envelope::{
    SnapshotCompression, SnapshotEnvelopeHeader, SNAPSHOT_HEADER_LEN, SNAPSHOT_MAGIC,
    SNAPSHOT_VERSION,
};

/// Default upper bound for the decompressed payload of a snapshot (2 GiB).
///
/// The declared length comes from the envelope, which may come from another peer, so it is
/// never trusted to size an allocation or to bound the decompression on its own.
pub const DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Largest buffer reserved up front while decompressing; beyond it the buffer grows with the
/// data actually produced.
#[cfg(not(target_arch = "wasm32"))]
const MAX_DECOMPRESSION_PREALLOCATION: usize = 64 * 1024 * 1024;

/// Wraps `payload_bytes` in a `ZMSN` envelope, compressing it with Zstandard if `compress_zstd`.
pub fn encode_snapshot_envelope(
    payload_bytes: &[u8],
    compress_zstd: bool,
    zstd_level: i32,
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
        (Cow::Owned(compressed), SnapshotCompression::Zstd)
    } else {
        (Cow::Borrowed(payload_bytes), SnapshotCompression::Raw)
    };

    #[cfg(target_arch = "wasm32")]
    let (body_bytes, compression) = {
        let _ = (compress_zstd, zstd_level);
        (Cow::Borrowed(payload_bytes), SnapshotCompression::Raw)
    };

    let header = SnapshotEnvelopeHeader::for_body(compression, uncompressed_len, &body_bytes);

    let mut envelope = Vec::with_capacity(SNAPSHOT_HEADER_LEN + body_bytes.len());
    envelope.extend_from_slice(&header.to_bytes());
    envelope.extend_from_slice(&body_bytes);

    Ok(envelope)
}

/// Decodes and validates a `ZMSN` snapshot envelope, returning the uncompressed Bincode
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
    let (header, body) = checked_envelope(envelope_bytes, max_uncompressed_bytes)?;
    let declared_len = u64::from(header.uncompressed_len());
    match header.compression() {
        // `verify_body` already checked that a raw body has the declared length.
        SnapshotCompression::Raw => Ok(body.to_vec()),
        SnapshotCompression::Zstd => decompress_bounded(body, declared_len),
    }
}

/// Decodes a snapshot envelope straight into `T`, with the checks of
/// [`decode_snapshot_envelope_with_limit`].
///
/// A compressed payload is deserialized while it is decompressed, so its decompressed bytes are
/// never held in memory next to the decoded value. Neither a raw nor a decompressed payload may
/// hold anything after the value.
pub(crate) fn decode_snapshot_payload<T: DeserializeOwned>(
    envelope_bytes: &[u8],
    max_uncompressed_bytes: u64,
) -> Result<T, StorageError> {
    let (header, body) = checked_envelope(envelope_bytes, max_uncompressed_bytes)?;
    let declared_len = u64::from(header.uncompressed_len());
    match header.compression() {
        SnapshotCompression::Raw => payload_options(declared_len)
            .reject_trailing_bytes()
            .deserialize(body)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string())),
        SnapshotCompression::Zstd => deserialize_zstd(body, declared_len, Some(declared_len)),
    }
}

/// Parses the header of `envelope_bytes`, verifies its body and checks the declared payload
/// size against `max_uncompressed_bytes`.
fn checked_envelope(
    envelope_bytes: &[u8],
    max_uncompressed_bytes: u64,
) -> Result<(SnapshotEnvelopeHeader, &[u8]), StorageError> {
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
    Ok((header, body))
}

/// The Bincode options snapshots are written with (those of `bincode::serialize`), limited to
/// `limit` bytes.
///
/// Bincode fails past `limit` bytes. Nothing a payload declares reserves memory beyond what it
/// holds: a raw payload is a slice that Bincode checks every length against, a stream is read
/// through a [`PayloadReader`], and the tables of a snapshot are inserted as they are read (see
/// `RoomSnapshotPayload`).
fn payload_options(limit: u64) -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(limit)
}

/// Size of the buffer between the Zstandard decoder and the Bincode deserializer, which reads
/// a few bytes at a time.
#[cfg(not(target_arch = "wasm32"))]
const STREAM_BUFFER: usize = 64 * 1024;

/// Deserializes `T` from the Zstandard stream `compressed` as it decompresses it, reading at
/// most `limit` decompressed bytes. The stream must end right after the value and, if
/// `exact_len` is given, be exactly that long.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn deserialize_zstd<T: DeserializeOwned>(
    compressed: &[u8],
    limit: u64,
    exact_len: Option<u64>,
) -> Result<T, StorageError> {
    let corruption = |e: &dyn std::fmt::Display| {
        StorageError::SnapshotCorruption(format!("Snapshot payload decode failed: {e}"))
    };
    let decoder = zstd::stream::read::Decoder::new(compressed).map_err(|e| corruption(&e))?;
    let mut reader = PayloadReader {
        inner: BufReader::with_capacity(STREAM_BUFFER, decoder.take(limit.saturating_add(1))),
        count: 0,
    };
    let value: T = payload_options(limit)
        .deserialize_from_custom(&mut reader)
        .map_err(|e| corruption(&e))?;
    let consumed = reader.count;

    let mut probe = [0u8; 1];
    if reader.read(&mut probe).map_err(|e| corruption(&e))? > 0 {
        return Err(corruption(&"data after the end of the payload"));
    }
    if let Some(expected) = exact_len {
        if consumed != expected {
            return Err(corruption(&format!(
                "payload of {consumed} bytes, expected {expected}"
            )));
        }
    }
    Ok(value)
}

#[cfg(target_arch = "wasm32")]
fn deserialize_zstd<T: DeserializeOwned>(
    _compressed: &[u8],
    _limit: u64,
    _exact_len: Option<u64>,
) -> Result<T, StorageError> {
    Err(StorageError::SnapshotCorruption(
        "Zstandard-compressed snapshots are not supported in wasm32 build".to_string(),
    ))
}

/// Reads a payload stream for Bincode, counting the bytes read.
///
/// Bincode's own stream reader fills a buffer of a string's declared length with zeros before
/// reading it, so a few compressed bytes declaring a long string would reserve up to the
/// payload's declared size. This one reads strings and byte arrays into a buffer that grows with
/// the bytes actually read.
#[cfg(not(target_arch = "wasm32"))]
struct PayloadReader<R> {
    inner: R,
    count: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl<R: std::io::Read> PayloadReader<R> {
    /// Reads the next `length` bytes, failing if the stream ends before them. At most
    /// `STREAM_BUFFER` bytes are reserved up front; beyond that the buffer grows with the bytes
    /// read.
    fn read_exactly(&mut self, length: usize) -> bincode::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(length.min(STREAM_BUFFER));
        self.by_ref().take(length as u64).read_to_end(&mut bytes)?;
        if bytes.len() != length {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        Ok(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<R: std::io::Read> std::io::Read for PayloadReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.count += read as u64;
        Ok(read)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'de, R: std::io::Read> bincode::BincodeRead<'de> for &mut PayloadReader<R> {
    fn forward_read_str<V: serde::de::Visitor<'de>>(
        &mut self,
        length: usize,
        visitor: V,
    ) -> bincode::Result<V::Value> {
        let bytes = self.read_exactly(length)?;
        let string =
            std::str::from_utf8(&bytes).map_err(bincode::ErrorKind::InvalidUtf8Encoding)?;
        visitor.visit_str(string)
    }

    fn get_byte_buffer(&mut self, length: usize) -> bincode::Result<Vec<u8>> {
        self.read_exactly(length)
    }

    fn forward_read_bytes<V: serde::de::Visitor<'de>>(
        &mut self,
        length: usize,
        visitor: V,
    ) -> bincode::Result<V::Value> {
        let bytes = self.read_exactly(length)?;
        visitor.visit_bytes(&bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn decompress_bounded(body: &[u8], declared_len: u64) -> Result<Vec<u8>, StorageError> {
    let failed = |e: std::io::Error| {
        StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}"))
    };
    let decoder = zstd::stream::read::Decoder::new(body).map_err(failed)?;
    let capacity = usize::try_from(declared_len)
        .unwrap_or(usize::MAX)
        .min(MAX_DECOMPRESSION_PREALLOCATION);
    let mut decompressed = Vec::with_capacity(capacity);
    decoder
        .take(declared_len.saturating_add(1))
        .read_to_end(&mut decompressed)
        .map_err(failed)?;

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

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
