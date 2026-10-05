//! Header of the `ZMSN` snapshot envelope, the container every room snapshot travels in.
//!
//! Layout (big-endian integers):
//!
//! | Offset | Size | Field |
//! |---|---|---|
//! | 0 | 4 | magic `ZMSN` |
//! | 4 | 1 | format version |
//! | 5 | 1 | compression flag (`0` raw, `1` Zstandard) |
//! | 6 | 4 | length of the payload once decompressed |
//! | 10 | 4 | CRC32 of the body (the bytes after the header, as stored) |
//! | 14 | … | body |
//!
//! This module only parses and checks the header and the body checksum, which needs no
//! decompression. `zemdb-storage` encodes and decodes complete envelopes on top of it, and the
//! server's snapshot relay uses it to reject uploads that are not snapshot envelopes.

use thiserror::Error;

/// Canonical 4-byte magic identifier for ZemDB snapshot envelopes ("ZMSN").
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"ZMSN";

/// Current snapshot envelope container format version.
pub const SNAPSHOT_VERSION: u8 = 0x01;

/// Fixed length of the snapshot envelope header:
/// 4B magic + 1B version + 1B compression + 4B uncompressed length + 4B body CRC32 = 14 bytes.
pub const SNAPSHOT_HEADER_LEN: usize = 14;

/// Compression applied to the body of a snapshot envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnapshotCompression {
    /// The body is the payload itself.
    Raw,
    /// The body is a Zstandard frame of the payload.
    Zstd,
}

impl SnapshotCompression {
    /// Wire value of the compression flag.
    pub const fn flag(self) -> u8 {
        match self {
            SnapshotCompression::Raw => 0x00,
            SnapshotCompression::Zstd => 0x01,
        }
    }

    /// Parses a compression flag; `None` for an unknown value.
    pub const fn from_flag(flag: u8) -> Option<Self> {
        match flag {
            0x00 => Some(SnapshotCompression::Raw),
            0x01 => Some(SnapshotCompression::Zstd),
            _ => None,
        }
    }
}

/// Reasons a byte sequence is not a valid snapshot envelope.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SnapshotEnvelopeError {
    #[error("snapshot envelope is shorter than its {SNAPSHOT_HEADER_LEN}-byte header")]
    TooShort,

    #[error("invalid snapshot magic: expected {SNAPSHOT_MAGIC:?}, got {0:?}")]
    InvalidMagic([u8; 4]),

    #[error("unsupported snapshot version: expected {SNAPSHOT_VERSION}, got {0}")]
    UnsupportedVersion(u8),

    #[error("unknown snapshot compression flag: 0x{0:02X}")]
    UnknownCompression(u8),

    #[error("snapshot body CRC32 mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("raw snapshot length mismatch: declared {declared}, actual {actual}")]
    RawLengthMismatch { declared: u64, actual: u64 },
}

/// Parsed header of a snapshot envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotEnvelopeHeader {
    compression: SnapshotCompression,
    uncompressed_len: u32,
    body_crc: u32,
}

impl SnapshotEnvelopeHeader {
    /// Builds the header for `body`, computing its checksum.
    ///
    /// `uncompressed_len` is the length of the payload once decompressed (for a raw body, the
    /// length of the body itself).
    pub fn for_body(compression: SnapshotCompression, uncompressed_len: u32, body: &[u8]) -> Self {
        Self {
            compression,
            uncompressed_len,
            body_crc: crc32fast::hash(body),
        }
    }

    /// Parses the header at the start of `bytes`. Only the first [`SNAPSHOT_HEADER_LEN`] bytes
    /// are read; the body is not checked (see [`SnapshotEnvelopeHeader::verify_body`]).
    pub fn parse(bytes: &[u8]) -> Result<Self, SnapshotEnvelopeError> {
        let header: &[u8; SNAPSHOT_HEADER_LEN] = bytes
            .get(..SNAPSHOT_HEADER_LEN)
            .and_then(|h| h.try_into().ok())
            .ok_or(SnapshotEnvelopeError::TooShort)?;

        let magic = [header[0], header[1], header[2], header[3]];
        if magic != SNAPSHOT_MAGIC {
            return Err(SnapshotEnvelopeError::InvalidMagic(magic));
        }
        if header[4] != SNAPSHOT_VERSION {
            return Err(SnapshotEnvelopeError::UnsupportedVersion(header[4]));
        }
        let compression = SnapshotCompression::from_flag(header[5])
            .ok_or(SnapshotEnvelopeError::UnknownCompression(header[5]))?;
        let uncompressed_len = u32::from_be_bytes([header[6], header[7], header[8], header[9]]);
        let body_crc = u32::from_be_bytes([header[10], header[11], header[12], header[13]]);

        Ok(Self {
            compression,
            uncompressed_len,
            body_crc,
        })
    }

    /// Serializes the header.
    pub fn to_bytes(&self) -> [u8; SNAPSHOT_HEADER_LEN] {
        let mut out = [0u8; SNAPSHOT_HEADER_LEN];
        out[..4].copy_from_slice(&SNAPSHOT_MAGIC);
        out[4] = SNAPSHOT_VERSION;
        out[5] = self.compression.flag();
        out[6..10].copy_from_slice(&self.uncompressed_len.to_be_bytes());
        out[10..14].copy_from_slice(&self.body_crc.to_be_bytes());
        out
    }

    /// Compression applied to the body.
    pub fn compression(&self) -> SnapshotCompression {
        self.compression
    }

    /// Declared length of the payload once decompressed.
    pub fn uncompressed_len(&self) -> u32 {
        self.uncompressed_len
    }

    /// Declared CRC32 of the body.
    pub fn body_crc(&self) -> u32 {
        self.body_crc
    }

    /// Checks a body against this header: its CRC32 and, for a raw body, its length.
    pub fn verify_body(&self, body: &[u8]) -> Result<(), SnapshotEnvelopeError> {
        self.verify_digest(crc32fast::hash(body), body.len() as u64)
    }

    /// Checks a body, given its CRC32 and length, against this header.
    fn verify_digest(&self, actual_crc: u32, body_len: u64) -> Result<(), SnapshotEnvelopeError> {
        if actual_crc != self.body_crc {
            return Err(SnapshotEnvelopeError::ChecksumMismatch {
                expected: self.body_crc,
                actual: actual_crc,
            });
        }
        if self.compression == SnapshotCompression::Raw
            && body_len != u64::from(self.uncompressed_len)
        {
            return Err(SnapshotEnvelopeError::RawLengthMismatch {
                declared: u64::from(self.uncompressed_len),
                actual: body_len,
            });
        }
        Ok(())
    }
}

/// Validates a complete envelope held in memory: its header and its body checksum, without
/// decompressing. Returns the header.
pub fn validate_snapshot_envelope(
    envelope: &[u8],
) -> Result<SnapshotEnvelopeHeader, SnapshotEnvelopeError> {
    let header = SnapshotEnvelopeHeader::parse(envelope)?;
    header.verify_body(&envelope[SNAPSHOT_HEADER_LEN..])?;
    Ok(header)
}

/// Incremental version of [`validate_snapshot_envelope`], for envelopes read in pieces (for
/// example from a file too large to hold in memory). Feed the bytes in order with
/// [`update`](Self::update), then call [`finish`](Self::finish).
#[derive(Debug, Default, Clone)]
pub struct SnapshotEnvelopeValidator {
    header_bytes: Vec<u8>,
    body_crc: crc32fast::Hasher,
    body_len: u64,
}

impl SnapshotEnvelopeValidator {
    /// Creates a validator that has seen no bytes yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds the next bytes of the envelope.
    pub fn update(&mut self, mut bytes: &[u8]) {
        if self.header_bytes.len() < SNAPSHOT_HEADER_LEN {
            let take = (SNAPSHOT_HEADER_LEN - self.header_bytes.len()).min(bytes.len());
            self.header_bytes.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
        }
        self.body_crc.update(bytes);
        self.body_len += bytes.len() as u64;
    }

    /// Validates everything fed so far as one complete envelope and returns its header.
    pub fn finish(self) -> Result<SnapshotEnvelopeHeader, SnapshotEnvelopeError> {
        let header = SnapshotEnvelopeHeader::parse(&self.header_bytes)?;
        header.verify_digest(self.body_crc.finalize(), self.body_len)?;
        Ok(header)
    }
}
