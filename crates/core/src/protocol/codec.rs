use bincode::Options;
use serde::{Deserialize, Serialize};

/// Default maximum payload limit (16 MB) to protect against allocation exhaustion (DoS).
pub const MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// Serialize any protocol message to binary using bincode with defensive limits.
#[tracing::instrument(level = "trace", skip(msg))]
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::DefaultOptions::new()
        .with_limit(MAX_MESSAGE_SIZE)
        .reject_trailing_bytes()
        .serialize(msg)
}

/// Deserialize any protocol message from binary using bincode with defensive limits against DoS.
#[tracing::instrument(level = "trace", skip(bytes))]
pub fn decode_message<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    if bytes.len() as u64 > MAX_MESSAGE_SIZE {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }
    bincode::DefaultOptions::new()
        .with_limit(MAX_MESSAGE_SIZE)
        .reject_trailing_bytes()
        .deserialize(bytes)
}
