use crate::id::{ClientId, CorrelationId, MutationId, RoomId, SchemaId, SequenceNumber};
use crate::mutation::Operation;
use crate::schema::Schema;
use serde::{Deserialize, Serialize};

/// Error codes returned by the coordination server.
///
/// Clients decide how to react from the code: `Unauthorized` asks for a new token,
/// `Forbidden` is permanent, `ClientNotRegistered` asks to register again, and `Unavailable`,
/// `Timeout` and `RequestTimeout` may be retried (commits are deduplicated by `MutationId`).
///
/// Encoded by variant index, so new codes are appended at the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCode {
    SchemaViolation,
    RoomNotFound,
    /// The client is not in the room's roster (never registered, or deregistered); it must
    /// register again.
    ClientNotRegistered,
    /// The client's cursor is older than the oldest retained log entry;
    /// client must request a full snapshot.
    BehindCompaction,
    RateLimited,
    RoomLocked,
    /// An unexpected server failure. Not retried automatically.
    Internal,
    /// The client token is missing, malformed, wrongly signed or expired; the client needs a
    /// new token.
    Unauthorized,
    RoomAlreadyExists,
    TableAlreadyExists,
    SchemaNotFound,
    InvalidSequence,
    /// The request frame has another protocol version than the server's.
    ProtocolVersionMismatch,
    /// The request is malformed or carries an invalid value (for example an invalid ID).
    BadRequest,
    /// The snapshot the request refers to is no longer the room's active snapshot (a newer one
    /// replaced it, or it expired), or an upload targets a sequence that is not newer than the
    /// active snapshot or the upload in progress. A download restarts from chunk 0 without
    /// `snapshot_hash`.
    SnapshotSuperseded,
    /// The token is valid but does not grant the request: it was issued for another room, or
    /// the message names another client than the token. Permanent for that token.
    Forbidden,
    /// The room is temporarily unable to answer (it is restarting after a failure, or its
    /// actor stopped); retry after the delay in the `Retry-After` header.
    Unavailable,
    /// The room did not answer in time. Retrying is safe.
    Timeout,
    /// A schema with this id is already registered. A schema is declared once; it evolves
    /// only by appending nullable columns.
    SchemaAlreadyExists,
    /// The request body did not arrive in time (HTTP 408). Transient: retrying with backoff is
    /// safe (commits are deduplicated by `MutationId`).
    RequestTimeout,
    /// The client's cursor belongs to a room deleted and created again with the same id, or
    /// came without the room incarnation: the client must discard its local state and register
    /// without a cursor (HTTP 409). Permanent for that state.
    RoomReset,
}

/// An operation ordered by the coordination server with assigned sequence ID.
///
/// Occupies 96 bytes in memory (8B SequenceNumber + 88B Operation).
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
}

/// Unified message sent from Client to Server.
///
/// Encoded by variant index, so variants are never reordered and new ones go at the end.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Commit a validated mutation, with its idempotency key and the client cursor for catch-up.
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
        /// Local sequence cursor: `None` for a new client, `Some` when reconnecting or restoring.
        #[serde(default)]
        current_seq: Option<SequenceNumber>,
        /// The room incarnation the cursor belongs to (from `Registered`). Required with a
        /// cursor: a missing or different one is `RoomReset`.
        #[serde(default)]
        known_incarnation: Option<u128>,
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
    ///
    /// The server clamps `chunk_size` to its allowed range; the reply's `total_chunks` reflects
    /// the size actually used. The first request of a download leaves `snapshot_hash` empty and
    /// gets the active snapshot; every following request carries the `snapshot_hash` of that
    /// reply, so that all chunks come from the same snapshot. If it is no longer the active
    /// snapshot the server answers `ErrorCode::SnapshotSuperseded`.
    RequestSnapshotChunk {
        correlation_id: CorrelationId,
        room_id: RoomId,
        chunk_index: u32,
        chunk_size: u32,
        snapshot_hash: Option<[u8; 32]>,
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

impl ClientMessage {
    /// The kind of this message.
    pub fn kind(&self) -> ClientMessageKind {
        match self {
            ClientMessage::Commit { .. } => ClientMessageKind::Commit,
            ClientMessage::Ack { .. } => ClientMessageKind::Ack,
            ClientMessage::Sync { .. } => ClientMessageKind::Sync,
            ClientMessage::Heartbeat { .. } => ClientMessageKind::Heartbeat,
            ClientMessage::RegisterClient { .. } => ClientMessageKind::RegisterClient,
            ClientMessage::GetSchema { .. } => ClientMessageKind::GetSchema,
            ClientMessage::DeregisterClient { .. } => ClientMessageKind::DeregisterClient,
            ClientMessage::RequestSnapshotChunk { .. } => ClientMessageKind::RequestSnapshotChunk,
            ClientMessage::UploadSnapshotChunk { .. } => ClientMessageKind::UploadSnapshotChunk,
        }
    }
}

/// The variant of a [`ClientMessage`], without its fields.
///
/// The discriminants are the variant indexes of `ClientMessage`, which is how its encoding
/// starts, so the kind of a frame can be read without decoding the rest (see
/// [`peek_client_message_kind`](crate::protocol::peek_client_message_kind)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientMessageKind {
    Commit = 0,
    Ack = 1,
    Sync = 2,
    Heartbeat = 3,
    RegisterClient = 4,
    GetSchema = 5,
    DeregisterClient = 6,
    RequestSnapshotChunk = 7,
    UploadSnapshotChunk = 8,
}

impl ClientMessageKind {
    /// Every kind, indexed by its discriminant.
    const ALL: [ClientMessageKind; 9] = [
        ClientMessageKind::Commit,
        ClientMessageKind::Ack,
        ClientMessageKind::Sync,
        ClientMessageKind::Heartbeat,
        ClientMessageKind::RegisterClient,
        ClientMessageKind::GetSchema,
        ClientMessageKind::DeregisterClient,
        ClientMessageKind::RequestSnapshotChunk,
        ClientMessageKind::UploadSnapshotChunk,
    ];

    /// The kind whose `ClientMessage` variant has index `index`, if any.
    pub(crate) fn from_variant_index(index: u32) -> Option<Self> {
        Self::ALL.get(usize::try_from(index).ok()?).copied()
    }
}

/// Unified message sent from Server to Client.
///
/// Encoded by variant index, so variants are never reordered and new ones go at the end.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Accepted commit: its assigned sequence number and the catch-up deltas, paginated.
    CommitAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        mutation_id: MutationId,
        assigned_seq: SequenceNumber,
        catchup_ops: Vec<SequencedOperation>,
        has_more: bool,
        /// Whether the server chose this client to upload a room snapshot.
        snapshot_wanted: bool,
        /// Version of the room's schema. Versions count per schema, so the client compares it
        /// together with `schema_id`.
        schema_version: u64,
        /// Id of the room's schema. An id or a version other than the client's is a schema
        /// change: the client asks for the schema (`GetSchema`) before applying operations.
        schema_id: SchemaId,
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
        /// Whether the server chose this client to upload a room snapshot.
        snapshot_wanted: bool,
        /// Version of the room's schema (see `CommitAck`).
        schema_version: u64,
        /// Id of the room's schema (see `CommitAck`).
        schema_id: SchemaId,
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
    /// Acknowledgment of a snapshot uploaded in a single request
    /// (`POST /rooms/:room_id/snapshot/upload`): the snapshot is staged and active.
    SnapshotStaged {
        room_id: RoomId,
        snapshot_head_seq: SequenceNumber,
        /// BLAKE3 256-bit digest of the uploaded snapshot.
        snapshot_hash: [u8; 32],
    },
    /// Acknowledgment of a heartbeat. A heartbeat never fails because the client fell
    /// behind the retained log; the client learns it from its next sync or commit.
    HeartbeatAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        current_head_seq: SequenceNumber,
        /// Whether the server chose this client to upload a room snapshot.
        snapshot_wanted: bool,
        /// Sequence number of the relay's active snapshot, if a client can catch up from it
        /// with the retained log.
        active_snapshot_seq: Option<SequenceNumber>,
        /// Version of the room's schema (see `CommitAck`).
        schema_version: u64,
        /// Id of the room's schema (see `CommitAck`).
        schema_id: SchemaId,
    },
    /// Confirmation of client deregistration from the room.
    DeregisterAck {
        correlation_id: CorrelationId,
        room_id: RoomId,
        client_id: ClientId,
    },
    /// Registration accepted: the room schema, head and retention tail, and the active snapshot.
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
        /// Identifies this room among the rooms ever created with its id; the client sends it
        /// back with its cursor when it registers again.
        room_incarnation: u128,
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

#[cfg(test)]
#[path = "tests/messages.rs"]
mod tests;
