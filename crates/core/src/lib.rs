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
pub use id::{ClientId, CorrelationId, MutationId, RoomId, SchemaId, SequenceNumber};
pub use mutation::{
    client_squash_operations, merge_sorted_column_updates, squash_operations, BufferError,
    ColumnUpdate, Operation, OperationKind, SquashOutcome, TableBuffer, UpdateBuilder,
};
pub use protocol::{
    decode_message, decode_wal_batch_from_slice, decode_wal_record_from_slice, encode_message,
    encode_wal_batch, encode_wal_record, replay_wal_records, ClientMessage, ErrorCode,
    SequencedOperation, ServerMessage, WalBatchDecodeResult, WalDecodeResult, WalFrameError,
    BATCH_HEADER_SIZE, BATCH_MAGIC, MAX_MESSAGE_SIZE,
};
pub use schema::{
    ColumnDef, Schema, SchemaBuilder, SchemaUpdateBuilder, TableBuilder, TableSchema,
    ValidationError,
};
pub use value::{CompactRow, DataType, PrimaryKey, Row, RowBuilder, Value};
