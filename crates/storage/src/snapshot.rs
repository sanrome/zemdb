//! Encoding and decoding of complete `ZMSN` snapshot envelopes.
//!
//! The header format and its validation live in `zemdb-core`
//! ([`zemdb_core::protocol::snapshot_envelope`]), shared with the server's snapshot relay; this
//! module adds compression and decompression of the body, with `zstd` natively and `ruzstd`
//! in WebAssembly.

use crate::error::StorageError;
use bincode::Options;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
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
const MAX_DECOMPRESSION_PREALLOCATION: usize = 64 * 1024 * 1024;

/// Wraps `payload_bytes` in a `ZMSN` envelope, compressing it with Zstandard if `compress_zstd`:
/// natively with `zstd` at `zstd_level`, and in WebAssembly with `ruzstd` at its `Fastest`
/// level, where `zstd_level` is ignored.
pub fn encode_snapshot_envelope(
    payload_bytes: &[u8],
    compress_zstd: bool,
    zstd_level: i32,
) -> Result<Vec<u8>, StorageError> {
    encode_envelope_with::<PlatformZstd>(payload_bytes, compress_zstd, zstd_level)
}

/// [`encode_snapshot_envelope`], compressing with `Z`.
fn encode_envelope_with<Z: ZstdCodec>(
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

    let (body_bytes, compression) = if compress_zstd {
        let compressed = Z::compress(payload_bytes, zstd_level)?;
        (Cow::Owned(compressed), SnapshotCompression::Zstd)
    } else {
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
    decode_envelope_with::<PlatformZstd>(envelope_bytes, max_uncompressed_bytes)
}

/// [`decode_snapshot_envelope_with_limit`], decompressing with `Z`.
fn decode_envelope_with<Z: ZstdCodec>(
    envelope_bytes: &[u8],
    max_uncompressed_bytes: u64,
) -> Result<Vec<u8>, StorageError> {
    let (header, body) = checked_envelope(envelope_bytes, max_uncompressed_bytes)?;
    let declared_len = u64::from(header.uncompressed_len());
    match header.compression() {
        // `verify_body` already checked that a raw body has the declared length.
        SnapshotCompression::Raw => Ok(body.to_vec()),
        SnapshotCompression::Zstd => decompress_bounded::<Z>(body, declared_len),
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
    decode_payload_with::<PlatformZstd, T>(envelope_bytes, max_uncompressed_bytes)
}

/// [`decode_snapshot_payload`], decompressing with `Z`.
fn decode_payload_with<Z: ZstdCodec, T: DeserializeOwned>(
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
        SnapshotCompression::Zstd => {
            deserialize_zstd_with::<Z, T>(body, declared_len, Some(declared_len))
        }
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
    deserialize_zstd_with::<PlatformZstd, T>(compressed, limit, exact_len)
}

/// [`deserialize_zstd`], decompressing with `Z`.
fn deserialize_zstd_with<Z: ZstdCodec, T: DeserializeOwned>(
    compressed: &[u8],
    limit: u64,
    exact_len: Option<u64>,
) -> Result<T, StorageError> {
    let corruption = |e: &dyn std::fmt::Display| {
        StorageError::SnapshotCorruption(format!("Snapshot payload decode failed: {e}"))
    };
    let decoder = Z::decoder(compressed).map_err(|e| corruption(&e))?;
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

/// Reads a payload stream for Bincode, counting the bytes read.
///
/// Bincode's own stream reader fills a buffer of a string's declared length with zeros before
/// reading it, so a few compressed bytes declaring a long string would reserve up to the
/// payload's declared size. This one reads strings and byte arrays into a buffer that grows with
/// the bytes actually read.
struct PayloadReader<R> {
    inner: R,
    count: u64,
}

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

impl<R: std::io::Read> std::io::Read for PayloadReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.count += read as u64;
        Ok(read)
    }
}

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

/// Decompresses `body` with `Z`, stopping one byte past `declared_len`, and fails unless it
/// produces exactly `declared_len` bytes.
fn decompress_bounded<Z: ZstdCodec>(
    body: &[u8],
    declared_len: u64,
) -> Result<Vec<u8>, StorageError> {
    let failed = |e: std::io::Error| {
        StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}"))
    };
    let decoder = Z::decoder(body).map_err(failed)?;
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

/// A Zstandard implementation: `Libzstd` natively and `Ruzstd`, in pure Rust, in
/// WebAssembly (and in this crate's unit tests, which check it against `Libzstd`).
///
/// Both decode a body only if it is one Zstandard frame that passes [`check_frame_header`], with
/// nothing after it, and if it declares them, of its declared size and with its checksum. On a
/// frame valid under RFC 8878 they give the same bytes; on one damaged on purpose they may not
/// agree (`zstd` rejects some reserved fields and oversized blocks of literals that `ruzstd`
/// accepts, `ruzstd` some inconsistent literal sections that `zstd` accepts, and without a
/// checksum both may accept it with different bytes), which is a false snapshot either way.
trait ZstdCodec {
    /// Compresses `payload` into one frame.
    fn compress(payload: &[u8], level: i32) -> Result<Vec<u8>, StorageError>;

    /// A reader of what the frame `body` decompresses to. It fails, at the latest when the frame
    /// ends, if `body` is not a frame that decodes as described above.
    fn decoder(body: &[u8]) -> std::io::Result<impl Read + '_>;
}

#[cfg(not(target_arch = "wasm32"))]
type PlatformZstd = Libzstd;
#[cfg(target_arch = "wasm32")]
type PlatformZstd = Ruzstd;

/// The `zstd` crate, over libzstd.
#[cfg(not(target_arch = "wasm32"))]
struct Libzstd;

#[cfg(not(target_arch = "wasm32"))]
impl ZstdCodec for Libzstd {
    fn compress(payload: &[u8], level: i32) -> Result<Vec<u8>, StorageError> {
        zstd::encode_all(payload, level)
            .map_err(|e| StorageError::Serialization(format!("Zstd compression failed: {e}")))
    }

    fn decoder(body: &[u8]) -> std::io::Result<impl Read + '_> {
        check_frame_header(body)?;
        // libzstd checks the declared size and the checksum itself.
        let decoder = zstd::stream::read::Decoder::with_buffer(body)?.single_frame();
        Ok(LibzstdFrame { decoder })
    }
}

/// libzstd's decoder of one frame, which reads its input straight from the body, so what it
/// leaves is what follows the frame.
#[cfg(not(target_arch = "wasm32"))]
struct LibzstdFrame<'a> {
    decoder: zstd::stream::read::Decoder<'static, &'a [u8]>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Read for LibzstdFrame<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.decoder.read(buf)?;
        if read == 0 && !buf.is_empty() && !self.decoder.get_ref().is_empty() {
            return Err(invalid_frame("data after the end of the Zstandard frame"));
        }
        Ok(read)
    }
}

/// `ruzstd`, in pure Rust.
#[cfg(any(target_arch = "wasm32", test))]
struct Ruzstd;

#[cfg(any(target_arch = "wasm32", test))]
impl ZstdCodec for Ruzstd {
    /// `ruzstd`'s `Fastest` level, similar to `zstd`'s level 1; `level` is ignored. The frame
    /// ends with a checksum of its content.
    fn compress(payload: &[u8], _level: i32) -> Result<Vec<u8>, StorageError> {
        Ok(ruzstd::encoding::compress_to_vec(
            payload,
            ruzstd::encoding::CompressionLevel::Fastest,
        ))
    }

    fn decoder(body: &[u8]) -> std::io::Result<impl Read + '_> {
        Ruzstd::frame(body)
    }
}

#[cfg(any(target_arch = "wasm32", test))]
impl Ruzstd {
    /// The decoder of the frame `body`.
    fn frame(body: &[u8]) -> std::io::Result<RuzstdFrame<'_>> {
        let header = check_frame_header(body)?;
        let decoder =
            ruzstd::decoding::StreamingDecoder::new_with_max_window_size(body, MAX_ZSTD_WINDOW)
                .map_err(|e| invalid_frame(&e.to_string()))?;
        Ok(RuzstdFrame {
            decoder,
            header,
            produced: 0,
        })
    }
}

/// `ruzstd`'s decoder of one frame, with the checks it does not do itself: once the frame ends,
/// its declared size, its checksum and that nothing follows it.
///
/// It decodes a block at a time, and only until it has output past the last window, so it holds
/// at most a window and a block: `ruzstd` limits the window to [`MAX_ZSTD_WINDOW`], what the
/// sequences of a block produce to the smaller of the window and 128 KiB (it rejects the block
/// after the sequence that passes the limit); the format limits the literals after them to
/// 1 MiB.
#[cfg(any(target_arch = "wasm32", test))]
struct RuzstdFrame<'a> {
    decoder: ruzstd::decoding::StreamingDecoder<&'a [u8], ruzstd::decoding::FrameDecoder>,
    header: FrameHeader,
    produced: u64,
}

#[cfg(any(target_arch = "wasm32", test))]
impl RuzstdFrame<'_> {
    fn check_end(&self) -> std::io::Result<()> {
        let frame = &self.decoder.decoder;
        if self.header.checksum && frame.get_checksum_from_data() != frame.get_calculated_checksum()
        {
            return Err(invalid_frame("Zstandard frame checksum mismatch"));
        }
        if self
            .header
            .content_size
            .is_some_and(|size| size != self.produced)
        {
            return Err(invalid_frame(
                "Zstandard frame content differs from its declared size",
            ));
        }
        if !self.decoder.get_ref().is_empty() {
            return Err(invalid_frame("data after the end of the Zstandard frame"));
        }
        Ok(())
    }
}

#[cfg(any(target_arch = "wasm32", test))]
impl Read for RuzstdFrame<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.decoder.read(buf)?;
        self.produced += read as u64;
        if read == 0 && !buf.is_empty() {
            self.check_end()?;
        }
        Ok(read)
    }
}

/// What the header of a Zstandard frame declares.
#[cfg_attr(not(any(target_arch = "wasm32", test)), allow(dead_code))]
struct FrameHeader {
    /// The frame's decompressed size, if it declares it.
    content_size: Option<u64>,
    /// Whether the frame ends with a checksum of its content.
    checksum: bool,
}

/// The magic number a Zstandard frame starts with, little-endian.
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// The largest window a frame may declare: libzstd's default limit when decoding (2^27 bytes,
/// plus one), also given to `ruzstd`. A decoder keeps up to a window of output, so it bounds
/// what a few bytes of header make it reserve.
const MAX_ZSTD_WINDOW: u64 = (1 << 27) + 1;

/// Checks the header of the frame that `body` starts with, before either codec decodes it: a
/// Zstandard frame (not a skippable one), with no reserved bit, no dictionary, and a window of at
/// most [`MAX_ZSTD_WINDOW`] (for a single-segment frame, its declared size).
fn check_frame_header(body: &[u8]) -> std::io::Result<FrameHeader> {
    let truncated = || invalid_frame("truncated Zstandard frame header");
    let rest = body
        .strip_prefix(&ZSTD_FRAME_MAGIC)
        .ok_or_else(|| invalid_frame("not a Zstandard frame"))?;
    let (&descriptor, mut rest) = rest.split_first().ok_or_else(truncated)?;
    if descriptor & 0x08 != 0 {
        return Err(invalid_frame(
            "Zstandard frame header has its reserved bit set",
        ));
    }
    if descriptor & 0x03 != 0 {
        return Err(invalid_frame("Zstandard frame needs a dictionary"));
    }
    let single_segment = descriptor & 0x20 != 0;
    let mut window = 0;
    if !single_segment {
        let (&window_descriptor, after) = rest.split_first().ok_or_else(truncated)?;
        rest = after;
        let base = 1u64 << (10 + (window_descriptor >> 3));
        window = base + base / 8 * u64::from(window_descriptor & 0x07);
    }
    let size_len = match descriptor >> 6 {
        0 => usize::from(single_segment),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let size_field = rest.get(..size_len).ok_or_else(truncated)?;
    let content_size = (size_len > 0).then(|| {
        let mut bytes = [0; 8];
        bytes[..size_len].copy_from_slice(size_field);
        let size = u64::from_le_bytes(bytes);
        if size_len == 2 {
            size + 256
        } else {
            size
        }
    });
    if single_segment {
        window = content_size.unwrap_or(0);
    }
    if window > MAX_ZSTD_WINDOW {
        return Err(invalid_frame(&format!(
            "Zstandard frame window of {window} bytes exceeds the maximum of {MAX_ZSTD_WINDOW}"
        )));
    }
    Ok(FrameHeader {
        content_size,
        checksum: descriptor & 0x04 != 0,
    })
}

fn invalid_frame(reason: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, reason.to_string())
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
