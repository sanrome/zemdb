use crate::id::{ClientId, CorrelationId, MutationId, RoomId, SchemaId, SequenceNumber};
use crate::mutation::Operation;
use crate::schema::Schema;
use serde::{Deserialize, Serialize};

/// Error codes returned by the coordination server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Unauthorized,
    RoomAlreadyExists,
    TableAlreadyExists,
    SchemaNotFound,
    InvalidSequence,
    ProtocolVersionMismatch,
}

/// An operation ordered by the coordination server with assigned sequence ID.
///
/// Bounded strictly to 96 bytes (8B SequenceNumber + 88B Operation).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    /// Ergonomic alias for tests.
    pub fn with_default_origin(seq: impl Into<SequenceNumber>, op: Operation) -> Self {
        Self::new(seq, op)
    }
}

/// Unified message sent from Client to Server.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Commit a new validated mutation into a room with idempotency key and client cursor for 1-RTT catch-up.
    Commit {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: Operation,
    },
    /// Explicitly acknowledge receipt and local persistence of operations up to `ack_seq`.
    Ack {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        ack_seq: SequenceNumber,
    },
    /// Request delta operations starting after `from_seq` with flow control limit.
    Sync {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
    },
    /// Periodically inform server that client is active to maintain lease.
    Heartbeat {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
    },
    /// Join a room as an active client with a signed backend authorization token.
    RegisterClient {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
        auth_token: String,
        /// Current local sequence cursor (None if new client, Some(seq) if reconnecting or restoring).
        #[serde(default)]
        current_seq: Option<SequenceNumber>,
    },
    /// Request current schema for a room under active evolution.
    GetSchema {
        correlation_id: CorrelationId,
        room_id: RoomId,
    },
    /// Explicitly deregister a client from a room to advance retention immediately.
    DeregisterClient {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
    },
    /// Request a specific chunk of the room base snapshot for bootstrapping datasets > 16 MB.
    RequestSnapshotChunk {
        correlation_id: CorrelationId,
        room_id: RoomId,
        chunk_index: u32,
        chunk_size: u32,
    },
    /// Upload a chunk of the room base snapshot for multipart staging of large datasets.
    UploadSnapshotChunk {
        correlation_id: CorrelationId,
        room_id: RoomId,
        snapshot_head_seq: SequenceNumber,
        chunk_index: u32,
        total_chunks: u32,
        total_bytes: u64,
        snapshot_hash: [u8; 32],
        data: bytes::Bytes,
    },
}

/// Unified message sent from Server to Client.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Confirmation of an accepted commit with its assigned sequence number, accumulated catchup deltas and pagination flag.
    CommitAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        mutation_id: MutationId,
        assigned_seq: SequenceNumber,
        catchup_ops: Vec<SequencedOperation>,
        has_more: bool,
    },
    /// Confirmation of client acknowledgment and cursor persistence.
    AckConfirmed {
        correlation_id: CorrelationId,
        room_id: RoomId,
        ack_seq: SequenceNumber,
        head_seq: SequenceNumber,
    },
    /// Batch of sequenced operations to be applied on the client with pagination flag.
    SyncBatch {
        correlation_id: CorrelationId,
        room_id: RoomId,
        head_seq: SequenceNumber,
        ops: Vec<SequencedOperation>,
        has_more: bool,
    },
    /// Chunk of the base room snapshot during multipart bootstrapping.
    SnapshotChunk {
        correlation_id: CorrelationId,
        room_id: RoomId,
        snapshot_head_seq: SequenceNumber,
        chunk_index: u32,
        total_chunks: u32,
        total_bytes: u64,
        /// BLAKE3 256-bit cryptographic digest of the complete concatenated snapshot payload.
        snapshot_hash: [u8; 32],
        data: bytes::Bytes,
    },
    /// Acknowledgment of an uploaded snapshot chunk.
    SnapshotUploadChunkAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        chunk_index: u32,
        total_chunks: u32,
        staged: bool,
    },
    /// Acknowledgment of a heartbeat.
    HeartbeatAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        current_head_seq: SequenceNumber,
    },
    /// Confirmation of client deregistration from the room.
    DeregisterAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
    },
    /// Confirmation of client registration, delivering the room schema, current head, retention tail, and active snapshot info.
    Registered {
        correlation_id: CorrelationId,
        room_id: RoomId,
        head_seq: SequenceNumber,
        tail_seq: SequenceNumber,
        schema_id: SchemaId,
        schema: Schema,
        /// Sequence number of the currently active snapshot available in relay, if any.
        #[serde(default)]
        active_snapshot_seq: Option<SequenceNumber>,
    },
    /// Room schema definition delivered in response to `GetSchema`.
    Schema {
        correlation_id: CorrelationId,
        room_id: RoomId,
        schema_id: SchemaId,
        schema: Schema,
    },
    /// Error notification.
    Error {
        correlation_id: Option<CorrelationId>,
        room_id: Option<RoomId>,
        code: ErrorCode,
        message: String,
    },
}

impl ServerMessage {
    /// Computes the BLAKE3 256-bit cryptographic digest of an entire assembled snapshot.
    pub fn compute_snapshot_hash(snapshot_bytes: &[u8]) -> [u8; 32] {
        *blake3::hash(snapshot_bytes).as_bytes()
    }
}
