#![forbid(unsafe_code)]

pub mod crypto;
pub mod id;
pub mod mutation;
pub mod protocol;
pub mod schema;
pub mod value;

/// Compatibility alias module for legacy imports.
pub mod operation {
    pub use crate::mutation::*;
    pub use crate::protocol::SequencedOperation;
}

pub use crypto::{CryptoConcurrencyBounds, CryptoEngine, CryptoError, NoOpCryptoEngine};
pub use id::{
    ClientId, CorrelationId, InvalidIdError, MutationId, RoomId, SchemaId, SequenceNumber,
};
pub use mutation::{
    client_squash_operations, merge_sorted_column_updates, squash_operations, BufferError,
    ColumnUpdate, Operation, OperationKind, SquashOutcome, TableBuffer, UpdateBuilder,
};
pub use protocol::{
    check_operation_size, classify_checksum_mismatch, classify_zeroed_header, decode_batch_payload,
    decode_message, decode_wal_batch_from_slice, encode_message, encode_wal_batch,
    encode_wal_record, parse_batch_header, peek_version, replay_wal_records,
    validate_snapshot_envelope, BatchHeader, BatchPayloadDecode, ClientMessage, DecodeError,
    ErrorCode, OperationSizeError, SequencedOperation, ServerMessage, SnapshotCompression,
    SnapshotEnvelopeError, SnapshotEnvelopeHeader, SnapshotEnvelopeValidator, WalBatchDecodeResult,
    WalFrameError, BATCH_HEADER_SIZE, BATCH_MAGIC, MAX_FRAME_SIZE, MAX_MESSAGE_SIZE,
    MAX_RESPONSE_OPS_BYTES, PROTOCOL_HEADER_LEN, PROTOCOL_VERSION, SNAPSHOT_HEADER_LEN,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION,
};
pub use schema::{
    ColumnDef, Schema, SchemaBuilder, SchemaUpdateBuilder, TableBuilder, TableSchema,
    ValidationError,
};
pub use value::{CompactRow, DataType, PrimaryKey, Row, RowBuilder, Value};
