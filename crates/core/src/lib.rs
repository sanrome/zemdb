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

pub use crypto::{CryptoEngine, CryptoError, NoOpCryptoEngine};
pub use id::{ClientId, CorrelationId, MutationId, RoomId, SequenceNumber};
pub use mutation::{
    client_squash_operations, client_squash_table_operations, merge_sorted_column_updates,
    server_squash_operations, server_squash_table_operations, BufferError, ColumnUpdate, Operation,
    OperationKind, SquashOutcome, TableBuffer, TableOperation, UpdateBuilder,
};
pub use protocol::{
    decode_message, encode_message, ClientMessage, ErrorCode, ServerMessage, SequencedOperation,
    MAX_MESSAGE_SIZE,
};
pub use schema::{
    ColumnDef, Schema, SchemaBuilder, SchemaUpdateBuilder, TableBuilder, TableSchema,
    ValidationError,
};
pub use value::{CompactRow, DataType, PrimaryKey, Row, RowBuilder, Value};
