use bincode::Options;
use serde::{Deserialize, Serialize};

/// Canonical 2-byte magic identifier for ZemDB wire protocol messages ("ZM").
pub const PROTOCOL_MAGIC: [u8; 2] = [0x5A, 0x4D];

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u8 = 0x01;

/// Fixed length of wire protocol header (2B magic + 1B version + 1B flags).
pub const PROTOCOL_HEADER_LEN: usize = 4;

/// Default maximum payload limit (16 MB) to protect against allocation exhaustion (DoS).
pub const MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// Serialize any protocol message to binary using bincode preceded by the canonical wire header.
#[tracing::instrument(level = "trace", skip(msg))]
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, bincode::Error> {
    let mut buffer = Vec::with_capacity(128);
    buffer.extend_from_slice(&PROTOCOL_MAGIC);
    buffer.push(PROTOCOL_VERSION);
    buffer.push(0x00); // reserved flags
    bincode::DefaultOptions::new()
        .with_limit(MAX_MESSAGE_SIZE)
        .reject_trailing_bytes()
        .serialize_into(&mut buffer, msg)?;
    Ok(buffer)
}

/// Deserialize any protocol message from binary validating wire header and using bincode limits against DoS.
#[tracing::instrument(level = "trace", skip(bytes))]
pub fn decode_message<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    if bytes.len() as u64 > MAX_MESSAGE_SIZE {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }
    if bytes.len() < PROTOCOL_HEADER_LEN {
        return Err(Box::new(bincode::ErrorKind::Custom(
            "Message payload too short to contain wire protocol header".to_string(),
        )));
    }
    if bytes[0..2] != PROTOCOL_MAGIC {
        return Err(Box::new(bincode::ErrorKind::Custom(format!(
            "Invalid protocol magic bytes: expected {:?}, got {:?}",
            PROTOCOL_MAGIC,
            &bytes[0..2]
        ))));
    }
    if bytes[2] != PROTOCOL_VERSION {
        return Err(Box::new(bincode::ErrorKind::Custom(format!(
            "Unsupported protocol version: expected {}, got {}",
            PROTOCOL_VERSION, bytes[2]
        ))));
    }
    bincode::DefaultOptions::new()
        .with_limit(MAX_MESSAGE_SIZE)
        .reject_trailing_bytes()
        .deserialize(&bytes[PROTOCOL_HEADER_LEN..])
}
