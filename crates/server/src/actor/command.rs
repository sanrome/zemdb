use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};
use zemdb_core::id::{ClientId, MutationId, RoomId, SchemaId, SequenceNumber};
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::schema::Schema;

use crate::error::ServerError;
use crate::log::RoomLifecyclePolicy;

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
    /// Whether this client is the one designated to upload a room snapshot.
    pub snapshot_wanted: bool,
}

/// Response returned upon requesting delta operations via Sync.
#[derive(Debug, Clone)]
pub struct SyncBatchResponse {
    pub head_seq: SequenceNumber,
    pub ops: Vec<SequencedOperation>,
    pub has_more: bool,
    /// Whether this client is the one designated to upload a room snapshot.
    pub snapshot_wanted: bool,
}

/// Response returned to a client's heartbeat.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub head_seq: SequenceNumber,
    /// Whether this client is the one designated to upload a room snapshot.
    pub snapshot_wanted: bool,
    /// The relay's active snapshot, only if a client can catch up from it with the retained log.
    pub active_snapshot_seq: Option<SequenceNumber>,
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
    /// The effective lifecycle policy the room runs with.
    pub lifecycle: RoomLifecyclePolicy,
}

/// Real-time notifications emitted by a RoomActor for SSE subscribers.
///
/// Events only speed clients up: the replies to heartbeats, syncs and commits are the source
/// of truth, since SSE is optional and a slow subscriber can miss events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEvent {
    HeadAdvanced(SequenceNumber),
    SchemaReloaded(SchemaId),
    /// A client was designated to upload a snapshot. Carries no data: a client learns whether
    /// it is the designee from its next heartbeat.
    SnapshotWanted,
    /// A new snapshot that clients can catch up from is available in the relay.
    SnapshotAvailable(SequenceNumber),
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
        reply: oneshot::Sender<Result<HeartbeatResponse, ServerError>>,
    },

    /// Subscribe to the room's signal-only SSE broadcast channel.
    SubscribeEvents {
        reply: oneshot::Sender<Result<broadcast::Receiver<RoomEvent>, ServerError>>,
    },

    /// Reload the active schema in this room (e.g. after schema evolution).
    ReloadSchema {
        schema: Arc<Schema>,
        reply: oneshot::Sender<Result<(), ServerError>>,
    },

    /// Retrieve operational metrics for this room.
    GetMetrics {
        reply: oneshot::Sender<Result<RoomMetrics, ServerError>>,
    },

    /// Query the retained log range as `(tail_seq, head_seq)`: the oldest retained sequence
    /// and the highest committed one. The snapshot relay uses it to accept only snapshots a
    /// client can catch up from.
    GetLogBounds {
        reply: oneshot::Sender<Result<(SequenceNumber, SequenceNumber), ServerError>>,
    },

    /// Query the confirmed cursor (last_ack_seq) for a registered client.
    GetClientCursor {
        client_id: ClientId,
        reply: oneshot::Sender<Result<Option<SequenceNumber>, ServerError>>,
    },

    /// Gracefully shutdown the room actor loop and release all file resources.
    Shutdown { reply: oneshot::Sender<()> },
}

impl RoomCommand {
    /// Answers the command with `err` without executing it. `Shutdown` is answered as done:
    /// it is only rejected once the actor has stopped.
    pub(crate) fn reject(self, err: ServerError) {
        match self {
            RoomCommand::RegisterClient { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::DeregisterClient { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::GetSchema { reply } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::Commit { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::Sync { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::Ack { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::Heartbeat { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::SubscribeEvents { reply } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::ReloadSchema { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::GetMetrics { reply } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::GetLogBounds { reply } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::GetClientCursor { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            RoomCommand::Shutdown { reply } => {
                let _ = reply.send(());
            }
        }
    }
}
