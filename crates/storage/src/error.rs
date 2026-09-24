use rimdb_core::{RoomId, SequenceNumber, ValidationError};
use thiserror::Error;

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

    #[error("Schema validation failed: {0}")]
    SchemaValidation(#[from] ValidationError),

    #[error("Storage engine is closed")]
    EngineClosed,

    #[error("Storage engine error: {0}")]
    Other(String),
}

impl From<bincode::Error> for StorageError {
    fn from(err: bincode::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<rimdb_core::WalFrameError> for StorageError {
    fn from(err: rimdb_core::WalFrameError) -> Self {
        match err {
            rimdb_core::WalFrameError::Serialization(s) => StorageError::Serialization(s),
            rimdb_core::WalFrameError::Corruption(s) => StorageError::WalCorruption(s),
        }
    }
}
