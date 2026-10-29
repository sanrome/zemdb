use serde::{Deserialize, Serialize};
use std::io;
use zemdb_core::RoomId;
use zemdb_storage::{RejectReason, StorageError};

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

    /// The replica rejected the write: the row would break a constraint of the schema.
    #[error("The write breaks a constraint: {0:?}")]
    ConstraintViolation(Violation),

    /// The replica rejected the write: an update of a row that does not exist.
    #[error("The row to update does not exist")]
    RowMissing,

    /// The write may or may not have been applied: an attempt got no valid reply and no
    /// `CommitAck` came, or the replica never applied its sequence with a report. Its outcome
    /// reaches the replica with the log.
    #[error("The outcome of the write is unknown")]
    OutcomeUnknown,

    /// The server refused the write for good: the operation is invalid for the schema of the
    /// room, or the token does not grant it.
    #[error("The server refused the write: {0}")]
    Validation(String),
}

/// The constraint that a write rejected by the replica would break.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Violation {
    /// The row would have the same key for this `UNIQUE` constraint as another row.
    Unique { constraint: String },
    /// The row would reference, through this foreign key, a row that does not exist.
    ForeignKey { constraint: String },
    /// A delete of a row that other rows reference through this foreign key.
    Restrict { constraint: String },
    /// An insert into an immutable table of a primary key that already has a row.
    Duplicate,
    /// An operation on an existing row whose owner is not the owner the operation declares.
    NotOwner,
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<RejectReason> for ClientError {
    fn from(reason: RejectReason) -> Self {
        let violation = match reason {
            RejectReason::MissingRow => return Self::RowMissing,
            RejectReason::Duplicate => Violation::Duplicate,
            RejectReason::NotOwner => Violation::NotOwner,
            RejectReason::Unique { constraint } => Violation::Unique { constraint },
            RejectReason::ForeignKey { constraint } => Violation::ForeignKey { constraint },
            RejectReason::Restrict { constraint } => Violation::Restrict { constraint },
        };
        Self::ConstraintViolation(violation)
    }
}

impl From<StorageError> for ClientError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err.to_string())
    }
}
