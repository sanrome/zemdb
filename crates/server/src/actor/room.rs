use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use zemdb_core::id::{MutationId, RoomId, SchemaId, SequenceNumber};
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::schema::Schema;

use crate::actor::command::{
    CommitResponse, RegisterResponse, RoomCommand, RoomEvent, RoomMetrics, SyncBatchResponse,
};
use crate::actor::lease::ClientLeaseTracker;
use crate::config::ServerConfig;
use crate::dedup::DedupLruCache;
use crate::durable;
use crate::error::ServerError;
use crate::log::{RoomLifecyclePolicy, TieredLog};
use crate::relay::SnapshotRelay;

/// Interval between maintenance passes (lease timeouts, log compaction, roster persistence).
const MAINTENANCE_PERIOD: Duration = Duration::from_millis(500);

/// Dedicated single-writer Tokio actor managing state, sequencing, durability, and synchronization for a single room.
pub struct RoomActor {
    room_id: RoomId,
    schema_id: SchemaId,
    schema: Arc<Schema>,
    dedup_cache: DedupLruCache,
    tiered_log: TieredLog,
    lease_tracker: ClientLeaseTracker,
    head_seq: SequenceNumber,
    events_tx: broadcast::Sender<RoomEvent>,
    receiver: mpsc::Receiver<RoomCommand>,
    lease_timeout: Duration,
    snapshot_relay: Arc<SnapshotRelay>,
}

impl RoomActor {
    /// Spawns a new RoomActor task, recovering state and initializing durability from disk.
    pub fn spawn(
        room_id: RoomId,
        schema_id: SchemaId,
        schema: Arc<Schema>,
        data_dir: impl AsRef<Path>,
        config: Arc<ServerConfig>,
        lifecycle_policy: RoomLifecyclePolicy,
        snapshot_relay: Arc<SnapshotRelay>,
    ) -> Result<(mpsc::Sender<RoomCommand>, JoinHandle<()>), ServerError> {
        let room_dir = data_dir.as_ref().join("rooms").join(room_id.as_str());
        durable::create_dir_all_synced(&room_dir)?;

        let clients_path = room_dir.join(format!("meta_clients_{}.json", room_id.as_str()));

        // 1. Open or recover 4-tier delta log and recovered mutations
        let (tiered_log, recovered_mutations) =
            TieredLog::open_or_create(&room_dir, lifecycle_policy.clone())?;

        // 2. Hydrate deduplication LRU cache from log
        let mut dedup_cache = DedupLruCache::new(config.dedup_lru_capacity);
        dedup_cache.hydrate(recovered_mutations);

        // 3. Monotonic head sequence derived directly from TieredLog
        let head_seq = tiered_log.head_seq();

        // 4. Open client lease tracker
        let lease_tracker = ClientLeaseTracker::open_or_create(&clients_path)?;

        // 5. Channels
        let (command_tx, command_rx) = mpsc::channel(1024);
        let (events_tx, _) = broadcast::channel(256);

        let lease_timeout = Duration::from_secs(config.lease_timeout_secs);

        let actor = Self {
            room_id: room_id.clone(),
            schema_id,
            schema,
            dedup_cache,
            tiered_log,
            lease_tracker,
            head_seq,
            events_tx,
            receiver: command_rx,
            lease_timeout,
            snapshot_relay,
        };

        info!(
            room = %room_id,
            head_seq = %head_seq,
            "Spawned RoomActor task"
        );

        let handle = tokio::spawn(async move {
            actor.run().await;
        });

        Ok((command_tx, handle))
    }

    /// Primary actor loop running sequentially on Tokio.
    pub async fn run(mut self) {
        // The first tick fires one period after start, not immediately.
        let mut maintenance_timer = tokio::time::interval_at(
            tokio::time::Instant::now() + MAINTENANCE_PERIOD,
            MAINTENANCE_PERIOD,
        );

        loop {
            tokio::select! {
                cmd = self.receiver.recv() => {
                    match cmd {
                        Some(RoomCommand::Shutdown { reply }) => {
                            debug!(room = %self.room_id, "Shutdown command received, terminating actor loop");
                            self.persist_roster();
                            let _ = reply.send(());
                            break;
                        }
                        Some(command) => {
                            if self.handle_command(command).is_break() {
                                break;
                            }
                        }
                        None => {
                            debug!(room = %self.room_id, "RoomCommand channel closed, shutting down actor loop");
                            break;
                        }
                    }
                }
                _ = maintenance_timer.tick() => {
                    self.run_periodic_maintenance().await;
                }
            }
        }

        // Every exit flushes pending cursor changes. Dropping the actor afterwards releases
        // the log's file lock and drops any command still queued, whose callers observe a
        // closed reply channel instead of waiting forever.
        self.persist_roster();
    }

    /// Handles one command. `Break` means the room can no longer operate safely and the
    /// actor must stop so that the next request reopens it from disk.
    fn handle_command(&mut self, command: RoomCommand) -> ControlFlow<()> {
        match command {
            RoomCommand::RegisterClient {
                client_id,
                current_seq,
                reply,
            } => {
                let tail_seq = self.tiered_log.tail_seq();
                let active_snapshot_seq = self.snapshot_relay.active_snapshot_seq(&self.room_id);
                let res = self
                    .lease_tracker
                    .register_client(&client_id, current_seq, tail_seq)
                    .map(|_| RegisterResponse {
                        head_seq: self.head_seq,
                        tail_seq,
                        schema_id: self.schema_id.clone(),
                        schema: Arc::clone(&self.schema),
                        active_snapshot_seq,
                    });
                let _ = reply.send(res);
            }

            RoomCommand::DeregisterClient { client_id, reply } => {
                let res = self.lease_tracker.deregister_client(&client_id).map(|_| ());
                let _ = reply.send(res);
            }

            RoomCommand::GetSchema { reply } => {
                let _ = reply.send(Ok((self.schema_id.clone(), Arc::clone(&self.schema))));
            }

            RoomCommand::ReloadSchema { schema, reply } => {
                self.schema = schema;
                let _ = self
                    .events_tx
                    .send(RoomEvent::SchemaReloaded(self.schema_id.clone()));
                let _ = reply.send(Ok(()));
            }

            RoomCommand::Commit {
                client_id,
                mutation_id,
                last_ack_seq,
                op,
                reply,
            } => {
                return self.handle_commit(client_id, mutation_id, last_ack_seq, op, reply);
            }

            RoomCommand::Sync {
                client_id,
                from_seq,
                max_batch_size,
                reply,
            } => {
                self.handle_sync(client_id, from_seq, max_batch_size, reply);
            }

            RoomCommand::Ack {
                client_id,
                ack_seq,
                reply,
            } => {
                self.handle_ack(client_id, ack_seq, reply);
            }

            RoomCommand::Heartbeat { client_id, reply } => {
                if !self.lease_tracker.is_registered(&client_id) {
                    let _ = reply.send(Err(ServerError::Unauthorized(format!(
                        "Client {} is not registered in room {}",
                        client_id, self.room_id
                    ))));
                    return ControlFlow::Continue(());
                }

                if self.lease_tracker.is_dormant(&client_id) {
                    let _ = reply.send(Err(ServerError::BehindCompaction));
                    return ControlFlow::Continue(());
                }

                let res = self
                    .lease_tracker
                    .record_heartbeat(&client_id)
                    .map(|_| self.head_seq);
                let _ = reply.send(res);
            }

            RoomCommand::SubscribeEvents { reply } => {
                let _ = reply.send(self.events_tx.subscribe());
            }

            RoomCommand::GetMetrics { reply } => {
                let (boot, conn, disc, dorm, total) = self.lease_tracker.client_counts();
                let metrics = RoomMetrics {
                    room_id: self.room_id.clone(),
                    schema_id: self.schema_id.clone(),
                    head_seq: self.head_seq,
                    tail_seq: self.tiered_log.tail_seq(),
                    bootstrapping_clients: boot,
                    connected_clients: conn,
                    disconnected_clients: disc,
                    dormant_clients: dorm,
                    total_clients: total,
                };
                let _ = reply.send(metrics);
            }

            RoomCommand::GetClientCursor { client_id, reply } => {
                let cursor = self
                    .lease_tracker
                    .get_client(&client_id)
                    .map(|c| c.last_ack_seq);
                let _ = reply.send(cursor);
            }

            RoomCommand::Shutdown { reply } => {
                let _ = reply.send(());
            }
        }
        ControlFlow::Continue(())
    }

    fn handle_commit(
        &mut self,
        client_id: zemdb_core::id::ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: zemdb_core::mutation::Operation,
        reply: tokio::sync::oneshot::Sender<Result<CommitResponse, ServerError>>,
    ) -> ControlFlow<()> {
        // 0. Check if client is registered in the room roster
        if !self.lease_tracker.is_registered(&client_id) {
            let _ = reply.send(Err(ServerError::Unauthorized(format!(
                "Client {} is not registered in room {}",
                client_id, self.room_id
            ))));
            return ControlFlow::Continue(());
        }

        // 1. Validate that client last_ack_seq does not exceed server head_seq
        if last_ack_seq > self.head_seq {
            let _ = reply.send(Err(ServerError::InvalidSequence {
                expected: self.head_seq,
                actual: last_ack_seq,
            }));
            return ControlFlow::Continue(());
        }

        // 2. Exactly-once idempotency. Checked before any rejection based on client state:
        // a retried mutation was already committed and replicated, so it must always be
        // acknowledged with its original sequence, never reported as a failure.
        if let Some(existing_seq) = self.dedup_cache.is_duplicate(&mutation_id) {
            // The retry is accepted, so the cursor it reports (already validated against
            // head_seq) is recorded. Lifecycle state is left untouched.
            self.lease_tracker.advance_cursor(&client_id, last_ack_seq);
            let (catchup_ops, has_more) = self.catchup_after_commit(last_ack_seq);
            let _ = reply.send(Ok(CommitResponse {
                assigned_seq: existing_seq,
                catchup_ops,
                has_more,
            }));
            return ControlFlow::Continue(());
        }

        // 3. Reject clients behind the compaction boundary before sequencing anything,
        // so that a rejected commit leaves no trace in the log, the head or the SSE stream.
        if self.lease_tracker.is_dormant(&client_id)
            || self.lease_tracker.is_bootstrapping(&client_id)
            || self.tiered_log.is_behind_retention(last_ack_seq)
        {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return ControlFlow::Continue(());
        }

        // 4. Schema validation in O(C)
        if let Err(err) = self.schema.validate_operation(&op) {
            let _ = reply.send(Err(ServerError::SchemaViolation(err.to_string())));
            return ControlFlow::Continue(());
        }

        // 5. Assign strictly monotonic sequence number
        let new_seq = self.head_seq.next();
        let seq_op = SequencedOperation::new(new_seq, op);

        // 6. Write-Through append to TieredLog (Hot Buffer RAM + synchronous active.wal disk sync with mutation_id)
        // A failed write or fsync leaves the log in an unknown state: the record may or may
        // not be on disk, and a failed fsync cannot be retried safely. The room stops and is
        // recovered from disk by the next request; if the record survived, deduplication
        // acknowledges a retry of the same mutation with the sequence that reached disk.
        let outcome = match self.tiered_log.append(seq_op.clone(), Some(mutation_id)) {
            Ok(outcome) => outcome,
            Err(err) => {
                error!(
                    room = %self.room_id,
                    error = %err,
                    "Log append failed; stopping room actor for recovery from disk"
                );
                // Closing the mailbox before replying guarantees that a caller retrying after
                // this reply already sees the room as stopped and gets it respawned.
                self.receiver.close();
                let _ = reply.send(Err(ServerError::Internal(format!(
                    "Room {} could not persist the commit and is restarting; retry the request",
                    self.room_id
                ))));
                return ControlFlow::Break(());
            }
        };

        // From here on the mutation is durable: the reply must acknowledge it.

        // 7. Record in DedupLruCache
        self.dedup_cache.record(mutation_id, new_seq);

        // 8. Advance local head sequence
        self.head_seq = new_seq;

        // 9. Refresh the client's lease and record the cursor reported with the commit.
        // Persisted on the next maintenance tick.
        self.lease_tracker.record_activity(&client_id);
        self.lease_tracker.advance_cursor(&client_id, last_ack_seq);

        // 10. Broadcast signal-only SSE event to active watchers
        let _ = self.events_tx.send(RoomEvent::HeadAdvanced(new_seq));

        // 11. Compute catch-up deltas for 1-RTT synchronization
        let (catchup_ops, has_more) = if last_ack_seq.get() + 1 == new_seq.get() {
            (vec![seq_op], false)
        } else {
            self.catchup_after_commit(last_ack_seq)
        };

        // A failed segment rotation after a durable append leaves the segment files in an
        // uncertain state. The commit is still acknowledged; then the room stops and is
        // recovered from disk by the next request.
        let flow = match outcome.rotation_error {
            Some(err) => {
                error!(
                    room = %self.room_id,
                    error = %err,
                    "Segment rotation failed after a durable commit; stopping room actor for recovery from disk"
                );
                self.receiver.close();
                ControlFlow::Break(())
            }
            None => ControlFlow::Continue(()),
        };

        let _ = reply.send(Ok(CommitResponse {
            assigned_seq: new_seq,
            catchup_ops,
            has_more,
        }));
        flow
    }

    /// Builds the catch-up batch returned with an acknowledged commit, starting after the
    /// client's cursor.
    ///
    /// The commit itself is already durable, so a failure here must not turn into an error
    /// reply. It degrades to an empty batch flagged `has_more`, which sends the client to
    /// `/sync`, where the underlying error (for example `BehindCompaction`) is reported.
    fn catchup_after_commit(
        &self,
        last_ack_seq: SequenceNumber,
    ) -> (Vec<SequencedOperation>, bool) {
        match self.tiered_log.fetch_deltas(last_ack_seq, 100) {
            Ok(batch) => batch,
            Err(err) => {
                warn!(
                    room = %self.room_id,
                    error = %err,
                    "Catch-up unavailable for committed mutation; deferring to sync"
                );
                (Vec::new(), true)
            }
        }
    }

    fn handle_sync(
        &mut self,
        client_id: zemdb_core::id::ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
        reply: tokio::sync::oneshot::Sender<Result<SyncBatchResponse, ServerError>>,
    ) {
        // 0. Check if client is registered in the room roster
        if !self.lease_tracker.is_registered(&client_id) {
            let _ = reply.send(Err(ServerError::Unauthorized(format!(
                "Client {} is not registered in room {}",
                client_id, self.room_id
            ))));
            return;
        }

        // 1. Check if client is Dormant
        if self.lease_tracker.is_dormant(&client_id) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 2. Check if from_seq is behind retained log tail
        if self.tiered_log.is_behind_retention(from_seq) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 3. Fetch continuous multi-tier delta batch
        let bounded_batch_size = max_batch_size.clamp(1, 1000);
        match self.tiered_log.fetch_deltas(from_seq, bounded_batch_size) {
            Ok((ops, has_more)) => {
                // If client was bootstrapping and synced a valid range, promote to Connected
                if self.lease_tracker.is_bootstrapping(&client_id) {
                    self.lease_tracker.record_ack(&client_id, from_seq);
                } else {
                    self.lease_tracker.record_activity(&client_id);
                }

                let _ = reply.send(Ok(SyncBatchResponse {
                    head_seq: self.head_seq,
                    ops,
                    has_more,
                }));
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    fn handle_ack(
        &mut self,
        client_id: zemdb_core::id::ClientId,
        ack_seq: SequenceNumber,
        reply: tokio::sync::oneshot::Sender<Result<SequenceNumber, ServerError>>,
    ) {
        // 0. Check if client is registered in the room roster
        if !self.lease_tracker.is_registered(&client_id) {
            let _ = reply.send(Err(ServerError::Unauthorized(format!(
                "Client {} is not registered in room {}",
                client_id, self.room_id
            ))));
            return;
        }

        // 1. Check if client is Dormant
        if self.lease_tracker.is_dormant(&client_id) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 2. Validate that ack_seq <= head_seq to prevent catastrophic log truncation
        if ack_seq > self.head_seq {
            let _ = reply.send(Err(ServerError::InvalidSequence {
                expected: self.head_seq,
                actual: ack_seq,
            }));
            return;
        }

        // 3. Record explicit Ack, advancing cursor, refreshing lease, and promoting Bootstrapping.
        // The cursor is persisted on the next maintenance tick.
        self.lease_tracker.record_ack(&client_id, ack_seq);

        // 4. Trigger proactive log pruning if all connected clients are past the sequence
        self.prune_to_retention_floor();

        let _ = reply.send(Ok(self.head_seq));
    }

    /// Prunes the log up to the retention floor when every non-dormant client is connected:
    /// the lowest connected cursor, bounded by the active snapshot (Retention Anchor).
    ///
    /// The cursors that justify the prune are persisted before any segment is deleted, so the
    /// roster on disk never falls behind the retained log; otherwise a restart would treat
    /// those clients as fallen behind and force them to a snapshot.
    fn prune_to_retention_floor(&mut self) {
        let Some(min_ack) = self.lease_tracker.min_connected_ack_seq() else {
            return;
        };
        let active_snap = self.snapshot_relay.active_snapshot_seq(&self.room_id);
        let retention_floor = match active_snap {
            Some(snap_seq) => min_ack.min(snap_seq),
            None => min_ack,
        };

        // Segments are deleted only when they end below the floor, and none ends below the
        // current tail, so a floor at or below the tail deletes nothing.
        if retention_floor <= self.tiered_log.tail_seq() {
            return;
        }

        if let Err(err) = self.lease_tracker.persist_if_dirty() {
            warn!(
                room = %self.room_id,
                error = %err,
                "Failed to persist clients roster; skipping proactive log pruning"
            );
            return;
        }
        if let Err(err) = self.tiered_log.prune_older_than(retention_floor) {
            warn!(room = %self.room_id, error = %err, "Proactive log pruning failed");
        }
    }

    /// Writes pending cursor and lease changes to the roster file. A failure is logged and the
    /// changes stay pending for the next attempt; it only delays how far the log can be pruned.
    fn persist_roster(&mut self) {
        if let Err(err) = self.lease_tracker.persist_if_dirty() {
            warn!(room = %self.room_id, error = %err, "Failed to persist clients roster");
        }
    }

    async fn run_periodic_maintenance(&mut self) {
        // 1. Check client timeouts (Connected/Bootstrapping -> Disconnected -> Dormant)
        self.lease_tracker
            .check_timeouts(self.lease_timeout, self.tiered_log.tail_seq());

        // 2. Run TieredLog TTL and size compaction (non-blocking via spawn_blocking)
        if let Err(e) = self.tiered_log.run_maintenance().await {
            warn!(room = %self.room_id, error = %e, "TieredLog maintenance error");
        }

        // 3. Trigger proactive cursor-driven pruning if all non-dormant clients are Connected,
        // bounded by active_snapshot_seq (Retention Anchor)
        self.prune_to_retention_floor();

        // 4. Persist cursor and lease changes accumulated since the previous tick
        self.persist_roster();
    }
}

#[cfg(test)]
#[path = "tests/room.rs"]
mod tests;
