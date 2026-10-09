use thiserror::Error;
use zemdb_core::{RoomId, SequenceNumber, ValidationError};

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

    #[error("Schema validation failed: {0}")]
    SchemaValidation(#[from] ValidationError),

    #[error("Storage engine is closed")]
    EngineClosed,

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

impl From<zemdb_core::WalFrameError> for StorageError {
    fn from(err: zemdb_core::WalFrameError) -> Self {
        match err {
            zemdb_core::WalFrameError::Serialization(s) => StorageError::Serialization(s),
            zemdb_core::WalFrameError::Corruption(s) => StorageError::WalCorruption(s),
        }
    }
}
