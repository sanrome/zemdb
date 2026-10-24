use thiserror::Error;
use zemdb_core::{RoomId, SequenceNumber, ValidationError, WalFrameError};

/// Storage errors that can occur during storage engine operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Room '{0}' not found or not opened")]
    RoomNotFound(RoomId),

    #[error("Room '{0}' is already opened")]
    RoomAlreadyOpen(RoomId),

    #[error("Room '{0}' file is locked by another process")]
    RoomLocked(RoomId),

    #[error("Table '{table}' not found in room '{room_id}'")]
    TableNotFound { room_id: RoomId, table: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("WAL corruption detected: {0}")]
    WalCorruption(String),

    #[error("Snapshot corruption detected: {0}")]
    SnapshotCorruption(String),

    /// A snapshot, received from another peer or opened from disk, whose rows break a
    /// constraint of the schema it is applied or opened with: a `UNIQUE` key that another row
    /// repeats, or a foreign key without its parent row. Its bytes are not damaged: the rows
    /// were written under other constraints.
    #[error("Snapshot rows of table id {table_id} break the constraint '{constraint}'")]
    SnapshotConstraints { table_id: u16, constraint: String },

    #[error("Sequence number mismatch: expected {expected}, actual {actual}")]
    SequenceMismatch {
        expected: SequenceNumber,
        actual: SequenceNumber,
    },

    /// A snapshot older than the room's state. Applying it would move the room back in time,
    /// and the records between the two sequence numbers would no longer continue the sequence.
    #[error("Snapshot at sequence {snapshot} is behind the room's head sequence {current}")]
    SnapshotBehind {
        current: SequenceNumber,
        snapshot: SequenceNumber,
    },

    /// A snapshot whose head sequence is not the one announced for it. Applying it could skip
    /// the operations between the two.
    #[error("Snapshot head sequence {actual} differs from the expected {expected}")]
    SnapshotHeadMismatch {
        expected: SequenceNumber,
        actual: SequenceNumber,
    },

    #[error("Schema validation failed: {0}")]
    SchemaValidation(#[from] ValidationError),

    /// The room's files hold data of a table that the schema it was opened with does not have.
    /// Opening it anyway would drop that table's operations.
    #[error("Room '{room_id}' holds data of table id {table_id}, which its schema does not have")]
    SchemaMismatch { room_id: RoomId, table_id: u16 },

    /// Engine options that cannot work, such as a compaction ratio that is not a finite number
    /// above 0.
    #[error("Invalid storage options: {0}")]
    InvalidOptions(String),

    /// An I/O failure left the room's files in a state that only recovery can resolve.
    /// Writes and compactions are refused until the room is closed and reopened.
    #[error("Room '{room_id}' must be reopened after an unrecoverable I/O failure: {reason}")]
    RoomFailed { room_id: RoomId, reason: String },

    #[error("Storage engine error: {0}")]
    Other(String),
}

impl From<bincode::Error> for StorageError {
    fn from(err: bincode::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<WalFrameError> for StorageError {
    fn from(err: WalFrameError) -> Self {
        match err {
            WalFrameError::Serialization(s) => StorageError::Serialization(s),
            WalFrameError::Corruption(s) => StorageError::WalCorruption(s),
        }
    }
}
