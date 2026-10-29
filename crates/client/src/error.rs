use serde::{Deserialize, Serialize};
use std::io;
use zemdb_core::RoomId;
use zemdb_storage::StorageError;

/// The error of a call to the client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum ClientError {
    /// The configuration cannot work: a person that is empty, holds `#` or does not make a
    /// valid client id.
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// The local storage of the client failed: its metadata or its storage engine could not be
    /// read or written, or another client holds them.
    #[error("Storage error: {0}")]
    Storage(String),

    /// The room is not open in this client, or the client is shut down.
    #[error("Room '{0}' is not open")]
    RoomClosed(RoomId),

    /// An error of the network or of the server: no valid reply came, or the server refused
    /// the request.
    #[error("Network error: {0}")]
    Network(String),

    /// The room fails closed: the server's schema for it does not evolve from the replica's, or
    /// the server sent an operation that does not validate against the schema of the room. The
    /// replica stays open, to be read; closing the room and opening it again tries again.
    #[error("Room '{0}' fails closed")]
    FailedClosed(RoomId),
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<StorageError> for ClientError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err.to_string())
    }
}
