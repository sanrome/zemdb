use serde::{Deserialize, Serialize};
use zemdb_core::RoomId;

/// The error of a call to the client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum ClientError {
    /// The configuration cannot work: a person that is empty, holds `#` or does not make a
    /// valid client id.
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// The local storage of the client failed: its metadata could not be read or written, or
    /// another client holds them.
    #[error("Storage error: {0}")]
    Storage(String),

    /// The room is not open in this client.
    #[error("Room '{0}' is not open")]
    RoomClosed(RoomId),
}
