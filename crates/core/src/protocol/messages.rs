use crate::id::{ClientId, CorrelationId, MutationId, RoomId, SequenceNumber};
use crate::mutation::Operation;
use serde::{Deserialize, Serialize};

/// Error codes returned by the coordination server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    SchemaViolation,
    RoomNotFound,
    ClientDeregistered,
    /// The client's cursor is older than the oldest retained log entry;
    /// client must request a full snapshot.
    BehindCompaction,
    RateLimited,
    RoomLocked,
    Internal,
}

/// An operation ordered by the coordination server with an assigned sequence ID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequencedOperation {
    pub seq: SequenceNumber,
    pub op: Operation,
}

impl SequencedOperation {
    pub fn new(seq: impl Into<SequenceNumber>, op: Operation) -> Self {
        Self {
            seq: seq.into(),
            op,
        }
    }
}

/// Unified message sent from Client to Server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Commit a new validated mutation into a room with idempotency key.
    Commit {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        mutation_id: MutationId,
        op: Operation,
    },
    /// Request delta operations starting after `last_ack_seq` with flow control limit.
    Sync {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        last_ack_seq: SequenceNumber,
        max_batch_size: u32,
    },
    /// Periodically inform server that client is active and report current cursor.
    Heartbeat {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        last_ack_seq: SequenceNumber,
    },
    /// Join a room as an active client.
    RegisterClient {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
    },
}

/// Unified message sent from Server to Client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Confirmation of an accepted commit with its assigned sequence number.
    CommitAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        mutation_id: MutationId,
        assigned_seq: SequenceNumber,
    },
    /// Batch of sequenced operations to be applied on the client with pagination flag.
    SyncBatch {
        correlation_id: CorrelationId,
        room_id: RoomId,
        head_seq: SequenceNumber,
        ops: Vec<SequencedOperation>,
        has_more: bool,
    },
    /// Acknowledgment of a heartbeat.
    HeartbeatAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        current_head_seq: SequenceNumber,
    },
    /// Confirmation of client registration.
    Registered {
        correlation_id: CorrelationId,
        room_id: RoomId,
        head_seq: SequenceNumber,
    },
    /// Error notification.
    Error {
        correlation_id: Option<CorrelationId>,
        room_id: Option<RoomId>,
        code: ErrorCode,
        message: String,
    },
}
