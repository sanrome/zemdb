//! Size limit of a single operation.
//!
//! The server stores each operation as one WAL record (bincode with fixed-width integers,
//! payload up to `MAX_MESSAGE_SIZE`) and returns it in `CommitAck` and `SyncBatch` frames
//! (wire options, payload up to `MAX_MESSAGE_SIZE`). The invariant is that **every accepted
//! operation fits alone both in a WAL record and in a response frame**, so a catch-up or sync
//! batch can always make progress by returning at least one operation. The server rejects a
//! commit whose operation breaks it before assigning a sequence number; clients can run the
//! same check before sending.

use thiserror::Error;

use crate::id::{MutationId, MAX_PATH_ID_LEN};
use crate::protocol::codec::{encoded_len, MAX_MESSAGE_SIZE};
use crate::protocol::messages::SequencedOperation;
use crate::protocol::wal_frame::wal_record_payload_len;

/// Largest wire encoding of a `u64` (bincode varint: a tag byte plus 8 bytes).
const MAX_VARINT_U64_LEN: u64 = 9;

/// Largest wire encoding of an enum variant tag (a varint `u32`).
const MAX_VARIANT_TAG_LEN: u64 = 5;

/// Encoded size of a `MutationId` (16 raw bytes).
const MUTATION_ID_LEN: u64 = 16;

/// Largest wire encoding of everything in a `CommitAck` or `SyncBatch` payload except its
/// operations, with every variable-length field at its maximum. `CommitAck` is the larger:
/// variant tag, correlation id, room id (length and bytes), mutation id, assigned sequence,
/// operation count and the two flags. `SyncBatch` has the same fields minus the mutation id.
pub const RESPONSE_ENVELOPE_ALLOWANCE: u64 = MAX_VARIANT_TAG_LEN
    + MAX_VARINT_U64_LEN
    + (MAX_VARINT_U64_LEN + MAX_PATH_ID_LEN as u64)
    + MUTATION_ID_LEN
    + MAX_VARINT_U64_LEN
    + MAX_VARINT_U64_LEN
    + 2;

/// Byte budget for the operations of one response (`CommitAck::catchup_ops`,
/// `SyncBatch::ops`), as measured by [`encoded_len`] of each `SequencedOperation`.
pub const MAX_RESPONSE_OPS_BYTES: u64 = MAX_MESSAGE_SIZE - RESPONSE_ENVELOPE_ALLOWANCE;

/// Why an operation cannot be accepted.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OperationSizeError {
    /// Its WAL record payload would exceed `MAX_MESSAGE_SIZE`.
    #[error("Operation needs a log record of {size} bytes; the maximum is {max} bytes")]
    LogRecord { size: u64, max: u64 },
    /// It would not fit alone in a response frame.
    #[error("Operation encodes to {size} bytes; at most {max} bytes fit in one response")]
    Response { size: u64, max: u64 },
    /// It cannot be serialized at all.
    #[error("Operation cannot be encoded: {0}")]
    Encoding(String),
}

/// Checks that `op` fits alone in a WAL record (with `mutation_id`) and in a response frame.
///
/// Both sizes depend on the operation's sequence number only slightly (fixed width in the
/// log, varint on the wire); a client checking before the server assigns one can use
/// `SequenceNumber::new(u64::MAX)` for the worst case.
pub fn check_operation_size(
    op: &SequencedOperation,
    mutation_id: Option<MutationId>,
) -> Result<(), OperationSizeError> {
    let log_size = wal_record_payload_len(op, mutation_id)
        .map_err(|e| OperationSizeError::Encoding(e.to_string()))?;
    if log_size > MAX_MESSAGE_SIZE {
        return Err(OperationSizeError::LogRecord {
            size: log_size,
            max: MAX_MESSAGE_SIZE,
        });
    }
    let wire_size = encoded_len(op).map_err(|e| OperationSizeError::Encoding(e.to_string()))?;
    if wire_size > MAX_RESPONSE_OPS_BYTES {
        return Err(OperationSizeError::Response {
            size: wire_size,
            max: MAX_RESPONSE_OPS_BYTES,
        });
    }
    Ok(())
}
