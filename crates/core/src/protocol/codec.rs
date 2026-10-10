use bincode::Options;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::protocol::limits::MAX_MESSAGE_VALUES;
use crate::protocol::messages::{ClientMessage, ClientMessageKind};
use crate::value::ValueBudget;

/// Canonical 2-byte magic identifier for ZemDB wire protocol messages ("ZM").
pub const PROTOCOL_MAGIC: [u8; 2] = [0x5A, 0x4D];

/// Current wire protocol version. It stays at 1 until the first release; after that it changes
/// with every incompatible wire change between published releases.
pub const PROTOCOL_VERSION: u8 = 0x01;

/// Fixed length of wire protocol header (2B magic + 1B version + 1B flags).
pub const PROTOCOL_HEADER_LEN: usize = 4;

/// Default maximum payload limit (16 MB) to protect against allocation exhaustion (DoS).
pub const MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// Largest frame the codec produces and accepts: a payload of `MAX_MESSAGE_SIZE` bytes plus
/// the wire header. Transports that carry frames (the server's request body limit) use it too.
pub const MAX_FRAME_SIZE: usize = MAX_MESSAGE_SIZE as usize + PROTOCOL_HEADER_LEN;

/// Why a frame could not be decoded.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// The frame is longer than [`MAX_FRAME_SIZE`].
    #[error("Frame of {len} bytes exceeds the maximum of {max} bytes", max = MAX_FRAME_SIZE)]
    TooLarge { len: usize },
    /// The frame is shorter than the wire header.
    #[error("Frame of {len} bytes is too short to contain the wire protocol header")]
    TooShort { len: usize },
    /// The frame does not start with [`PROTOCOL_MAGIC`]: it is not a ZemDB frame.
    #[error("Invalid protocol magic bytes: expected {expected:?}, got {got:?}", expected = PROTOCOL_MAGIC)]
    InvalidMagic { got: [u8; 2] },
    /// A ZemDB frame of another protocol version.
    #[error("Unsupported protocol version: expected {expected}, got {got}")]
    UnsupportedVersion { expected: u8, got: u8 },
    /// The header is valid but the payload is not a valid message of the expected type
    /// (including invalid values, such as an invalid identifier, and trailing bytes).
    #[error("Malformed message payload: {0}")]
    Malformed(#[source] bincode::Error),
    /// The message carries more values than a client message may (see
    /// [`decode_client_message`]).
    #[error("Message carries more than {max} values")]
    TooManyValues { max: u64 },
}

/// Size of `value` encoded with the wire options, without the frame header: what it adds to a
/// frame's payload. Fails only if the value cannot be serialized.
pub fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<u64, bincode::Error> {
    bincode::DefaultOptions::new().serialized_size(value)
}

/// Serialize any protocol message to binary using bincode preceded by the canonical wire header.
///
/// Fails only if the message cannot be serialized or its payload exceeds `MAX_MESSAGE_SIZE`.
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

/// Protocol version of a frame, read from its header without decoding the payload, or `None`
/// if the bytes do not start with a ZemDB header (too short or wrong magic).
///
/// A peer of another version cannot decode the frames of this one, error frames included. So
/// a response that carries the ZemDB magic and another version means a protocol version
/// mismatch, whatever its HTTP status and body.
pub fn peek_version(bytes: &[u8]) -> Option<u8> {
    match bytes {
        [m0, m1, version, _, ..] if [*m0, *m1] == PROTOCOL_MAGIC => Some(*version),
        _ => None,
    }
}

/// Deserialize any protocol message from binary validating wire header and using bincode limits against DoS.
///
/// The header is checked before the size, so a frame of another protocol version is always
/// reported as [`DecodeError::UnsupportedVersion`], whatever its length or payload.
#[tracing::instrument(level = "trace", skip(bytes))]
pub fn decode_message<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, DecodeError> {
    bincode::DefaultOptions::new()
        .with_limit(MAX_MESSAGE_SIZE)
        .reject_trailing_bytes()
        .deserialize(frame_payload(bytes)?)
        .map_err(DecodeError::Malformed)
}

/// Decodes a message sent by a client, as [`decode_message`] does, materializing at most
/// [`MAX_MESSAGE_VALUES`] values: a message with more is [`DecodeError::TooManyValues`]. It is
/// what the server decodes requests with, so that no request reserves more memory for its
/// values than the budget, whatever their encoded size.
#[tracing::instrument(level = "trace", skip(bytes))]
pub fn decode_client_message(bytes: &[u8]) -> Result<ClientMessage, DecodeError> {
    decode_with_value_budget(bytes, MAX_MESSAGE_VALUES)
}

/// [`decode_message`] with at most `max_values` values materialized.
fn decode_with_value_budget<'a, T: Deserialize<'a>>(
    bytes: &'a [u8],
    max_values: u64,
) -> Result<T, DecodeError> {
    // Decoding is synchronous, so the budget covers exactly the values of this message.
    let budget = ValueBudget::start(max_values);
    decode_message(bytes).map_err(|err| {
        if budget.exhausted() {
            DecodeError::TooManyValues { max: max_values }
        } else {
            err
        }
    })
}

/// Kind of the client message in a frame, read from its variant tag without decoding the
/// rest of the payload, which can then be rejected unread. The header is checked as in
/// [`decode_message`]; an unknown variant is [`DecodeError::Malformed`]. A frame whose kind
/// was read can still fail to decode.
pub fn peek_client_message_kind(bytes: &[u8]) -> Result<ClientMessageKind, DecodeError> {
    // An enum is encoded as its variant index (a `u32`, varint with the wire options),
    // followed by the variant's fields.
    let index: u32 = bincode::DefaultOptions::new()
        .with_limit(MAX_MESSAGE_SIZE)
        .allow_trailing_bytes()
        .deserialize(frame_payload(bytes)?)
        .map_err(DecodeError::Malformed)?;
    ClientMessageKind::from_variant_index(index).ok_or_else(|| {
        DecodeError::Malformed(bincode::Error::custom(format!(
            "unknown client message variant index {index}"
        )))
    })
}

/// The payload of a frame, after checking its header and size.
fn frame_payload(bytes: &[u8]) -> Result<&[u8], DecodeError> {
    if bytes.len() < PROTOCOL_HEADER_LEN {
        return Err(DecodeError::TooShort { len: bytes.len() });
    }
    if bytes[0..2] != PROTOCOL_MAGIC {
        return Err(DecodeError::InvalidMagic {
            got: [bytes[0], bytes[1]],
        });
    }
    if bytes[2] != PROTOCOL_VERSION {
        return Err(DecodeError::UnsupportedVersion {
            expected: PROTOCOL_VERSION,
            got: bytes[2],
        });
    }
    if bytes.len() > MAX_FRAME_SIZE {
        return Err(DecodeError::TooLarge { len: bytes.len() });
    }
    Ok(&bytes[PROTOCOL_HEADER_LEN..])
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
