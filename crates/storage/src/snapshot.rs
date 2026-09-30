use crate::error::StorageError;

/// Canonical 4-byte magic identifier for RimDB snapshot envelopes ("RMSN").
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"RMSN";

/// Current snapshot envelope container format version.
pub const SNAPSHOT_VERSION: u8 = 0x01;

/// Compression flag indicating uncompressed raw Bincode payload.
pub const COMPRESSION_RAW: u8 = 0x00;

/// Compression flag indicating Zstandard-compressed payload.
pub const COMPRESSION_ZSTD: u8 = 0x01;

/// Fixed length of the snapshot envelope header:
/// 4B magic + 1B version + 1B compression + 4B uncompressed length + 4B payload CRC32 = 14 bytes.
pub const SNAPSHOT_HEADER_LEN: usize = 14;

/// Encapsulates a serialized room snapshot into the universal canonical container.
pub fn encode_snapshot_envelope(
    payload_bytes: &[u8],
    compress_zstd: bool,
    #[allow(unused_variables)] zstd_level: i32,
) -> Result<Vec<u8>, StorageError> {
    let uncompressed_len = payload_bytes.len() as u32;

    #[cfg(not(target_arch = "wasm32"))]
    let (body_bytes, compression_flag) = if compress_zstd {
        let compressed = zstd::encode_all(payload_bytes, zstd_level)
            .map_err(|e| StorageError::Serialization(format!("Zstd compression failed: {e}")))?;
        (compressed, COMPRESSION_ZSTD)
    } else {
        (payload_bytes.to_vec(), COMPRESSION_RAW)
    };

    #[cfg(target_arch = "wasm32")]
    let (body_bytes, compression_flag) = (payload_bytes.to_vec(), COMPRESSION_RAW);

    let crc = crc32fast::hash(&body_bytes);

    let mut envelope = Vec::with_capacity(SNAPSHOT_HEADER_LEN + body_bytes.len());
    envelope.extend_from_slice(&SNAPSHOT_MAGIC);
    envelope.push(SNAPSHOT_VERSION);
    envelope.push(compression_flag);
    envelope.extend_from_slice(&uncompressed_len.to_be_bytes());
    envelope.extend_from_slice(&crc.to_be_bytes());
    envelope.extend_from_slice(&body_bytes);

    Ok(envelope)
}

/// Decodes and validates a universal snapshot envelope, returning the uncompressed Bincode payload.
pub fn decode_snapshot_envelope(envelope_bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
    if envelope_bytes.len() < SNAPSHOT_HEADER_LEN {
        return Err(StorageError::SnapshotCorruption(
            "Snapshot buffer too short for canonical header".to_string(),
        ));
    }

    if envelope_bytes[0..4] != SNAPSHOT_MAGIC {
        return Err(StorageError::SnapshotCorruption(format!(
            "Invalid snapshot magic: expected {:?}, got {:?}",
            SNAPSHOT_MAGIC,
            &envelope_bytes[0..4]
        )));
    }

    let version = envelope_bytes[4];
    if version != SNAPSHOT_VERSION {
        return Err(StorageError::SnapshotCorruption(format!(
            "Unsupported snapshot version: expected {}, got {}",
            SNAPSHOT_VERSION, version
        )));
    }

    let compression_flag = envelope_bytes[5];
    let uncompressed_len = u32::from_be_bytes(envelope_bytes[6..10].try_into().map_err(|_| {
        StorageError::SnapshotCorruption("Invalid uncompressed_len field".to_string())
    })?) as usize;

    let expected_crc = u32::from_be_bytes(
        envelope_bytes[10..14]
            .try_into()
            .map_err(|_| StorageError::SnapshotCorruption("Invalid crc field".to_string()))?,
    );

    let body = &envelope_bytes[SNAPSHOT_HEADER_LEN..];
    let actual_crc = crc32fast::hash(body);
    if actual_crc != expected_crc {
        return Err(StorageError::SnapshotCorruption(format!(
            "Snapshot payload CRC32 mismatch: expected 0x{:08X}, got 0x{:08X}",
            expected_crc, actual_crc
        )));
    }

    match compression_flag {
        COMPRESSION_RAW => {
            if body.len() != uncompressed_len {
                return Err(StorageError::SnapshotCorruption(format!(
                    "Raw snapshot length mismatch: declared {}, actual {}",
                    uncompressed_len,
                    body.len()
                )));
            }
            Ok(body.to_vec())
        }
        COMPRESSION_ZSTD => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let decompressed = zstd::decode_all(body).map_err(|e| {
                    StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}"))
                })?;
                if decompressed.len() != uncompressed_len {
                    return Err(StorageError::SnapshotCorruption(format!(
                        "Decompressed snapshot length mismatch: expected {}, actual {}",
                        uncompressed_len,
                        decompressed.len()
                    )));
                }
                Ok(decompressed)
            }
            #[cfg(target_arch = "wasm32")]
            {
                Err(StorageError::SnapshotCorruption(
                    "Zstandard-compressed snapshots are not supported in wasm32 build".to_string(),
                ))
            }
        }
        other => Err(StorageError::SnapshotCorruption(format!(
            "Unknown snapshot compression flag: 0x{:02X}",
            other
        ))),
    }
}
