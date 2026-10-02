use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};
use zemdb_core::id::{ClientId, MutationId, RoomId, SchemaId, SequenceNumber};
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::schema::Schema;

use crate::error::ServerError;

/// Response returned to a client upon successful room registration.
#[derive(Debug, Clone)]
pub struct RegisterResponse {
    pub head_seq: SequenceNumber,
    pub tail_seq: SequenceNumber,
    pub schema_id: SchemaId,
    pub schema: Arc<Schema>,
    pub active_snapshot_seq: Option<SequenceNumber>,
}

/// Response returned to a client upon committing an operation.
#[derive(Debug, Clone)]
pub struct CommitResponse {
    pub assigned_seq: SequenceNumber,
    pub catchup_ops: Vec<SequencedOperation>,
    pub has_more: bool,
}

/// Response returned upon requesting delta operations via Sync.
#[derive(Debug, Clone)]
pub struct SyncBatchResponse {
    pub head_seq: SequenceNumber,
    pub ops: Vec<SequencedOperation>,
    pub has_more: bool,
}

/// Metrics snapshot for a room actor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMetrics {
    pub room_id: RoomId,
    pub schema_id: SchemaId,
    pub head_seq: SequenceNumber,
    pub tail_seq: SequenceNumber,
    pub bootstrapping_clients: usize,
    pub connected_clients: usize,
    pub disconnected_clients: usize,
    pub dormant_clients: usize,
    pub total_clients: usize,
}

/// Real-time notifications emitted by a RoomActor for SSE subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEvent {
    HeadAdvanced(SequenceNumber),
    SchemaReloaded(SchemaId),
}

/// Commands dispatched to a RoomActor.
#[derive(Debug)]
pub enum RoomCommand {
    /// Register or re-activate a client in the room.
    RegisterClient {
        client_id: ClientId,
        current_seq: Option<SequenceNumber>,
        reply: oneshot::Sender<Result<RegisterResponse, ServerError>>,
    },

    /// Explicitly deregister a client from the room.
    DeregisterClient {
        client_id: ClientId,
        reply: oneshot::Sender<Result<(), ServerError>>,
    },

    /// Request the current active schema of the room.
    GetSchema {
        reply: oneshot::Sender<Result<(SchemaId, Arc<Schema>), ServerError>>,
    },

    /// Commit a single mutation into the room log.
    Commit {
        client_id: ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: Operation,
        reply: oneshot::Sender<Result<CommitResponse, ServerError>>,
    },

    /// Fetch a batch of sequenced operations starting after `from_seq`.
    Sync {
        client_id: ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
        reply: oneshot::Sender<Result<SyncBatchResponse, ServerError>>,
    },

    /// Explicitly acknowledge receipt and local persistence of operations up to `ack_seq`.
    Ack {
        client_id: ClientId,
        ack_seq: SequenceNumber,
        reply: oneshot::Sender<Result<SequenceNumber, ServerError>>,
    },

    /// Inform the actor that the client is active to maintain lease (liveness ping).
    Heartbeat {
        client_id: ClientId,
        reply: oneshot::Sender<Result<SequenceNumber, ServerError>>,
    },

    /// Subscribe to the room's signal-only SSE broadcast channel.
    SubscribeEvents {
        reply: oneshot::Sender<broadcast::Receiver<RoomEvent>>,
    },

    /// Reload the active schema in this room (e.g. after schema evolution).
    ReloadSchema {
        schema: Arc<Schema>,
        reply: oneshot::Sender<Result<(), ServerError>>,
    },

    /// Retrieve operational metrics for this room.
    GetMetrics { reply: oneshot::Sender<RoomMetrics> },

    /// Query the confirmed cursor (last_ack_seq) for a registered client.
    GetClientCursor {
        client_id: ClientId,
        reply: oneshot::Sender<Option<SequenceNumber>>,
    },

    /// Gracefully shutdown the room actor loop and release all file resources.
    Shutdown { reply: oneshot::Sender<()> },
}
