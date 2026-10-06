use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use zemdb_core::id::{ClientId, MutationId, RoomId, SchemaId, SequenceNumber};
use zemdb_core::protocol::codec::encoded_len;
use zemdb_core::protocol::limits::{check_operation_size, MAX_RESPONSE_OPS_BYTES};
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::schema::Schema;

use crate::actor::command::{
    CommitResponse, HeartbeatResponse, RegisterResponse, RoomCommand, RoomEvent, RoomMetrics,
    SyncBatchResponse,
};
use crate::actor::lease::{ClientLeaseTracker, ClientState};
use crate::actor::snapshot_demand::{persistence_granularity, SnapshotDemand, DESIGNATION_TIMEOUT};
use crate::config::ServerConfig;
use crate::dedup::DedupLruCache;
use crate::durable;
use crate::error::ServerError;
use crate::log::{retention, RoomLifecyclePolicy, TieredLog};
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
    dormant_after: Option<Duration>,
    snapshot_relay: Arc<SnapshotRelay>,
    snapshot_demand: SnapshotDemand,
    /// How stale the persisted renewal time of the snapshot demand may get.
    demand_persistence_granularity: Duration,
    /// The last usable snapshot announced to SSE subscribers.
    announced_snapshot: Option<SequenceNumber>,
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
        Self::spawn_with_designation_timeout(
            room_id,
            schema_id,
            schema,
            data_dir,
            config,
            lifecycle_policy,
            snapshot_relay,
            DESIGNATION_TIMEOUT,
        )
    }

    /// Like [`spawn`](Self::spawn), with the time a client designated to upload a snapshot has
    /// to start the upload. Production always uses [`DESIGNATION_TIMEOUT`]; tests shorten it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_with_designation_timeout(
        room_id: RoomId,
        schema_id: SchemaId,
        schema: Arc<Schema>,
        data_dir: impl AsRef<Path>,
        config: Arc<ServerConfig>,
        lifecycle_policy: RoomLifecyclePolicy,
        snapshot_relay: Arc<SnapshotRelay>,
        designation_timeout: Duration,
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
        let dormant_after = config.dormant_after_secs.map(Duration::from_secs);
        let demand_ttl = Duration::from_secs(config.snapshot_demand_ttl_secs);
        let snapshot_demand = SnapshotDemand::new(demand_ttl, designation_timeout);

        let mut actor = Self {
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
            dormant_after,
            snapshot_relay,
            snapshot_demand,
            demand_persistence_granularity: persistence_granularity(demand_ttl),
            announced_snapshot: None,
        };
        // A snapshot that was already usable before this actor started is not news.
        actor.announced_snapshot = actor.usable_snapshot_seq();
        actor.restore_snapshot_demand();

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
                // A cursor beyond the head was never delivered by this room. Accepting it
                // would make the client the preferred snapshot uploader and could raise the
                // pruning floor above the head.
                if let Some(seq) = current_seq.filter(|seq| *seq > self.head_seq) {
                    let _ = reply.send(Err(ServerError::InvalidSequence {
                        expected: self.head_seq,
                        actual: seq,
                    }));
                    return ControlFlow::Continue(());
                }
                let tail_seq = self.tiered_log.tail_seq();
                let res = self
                    .lease_tracker
                    .register_client(&client_id, current_seq, tail_seq);
                if matches!(res, Ok(ClientState::Bootstrapping)) {
                    self.request_snapshot();
                }
                let res = res.map(|_| RegisterResponse {
                    head_seq: self.head_seq,
                    tail_seq,
                    schema_id: self.schema_id.clone(),
                    schema: Arc::clone(&self.schema),
                    active_snapshot_seq: self.usable_snapshot_seq(),
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
                self.handle_heartbeat(client_id, reply);
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

            RoomCommand::GetLogBounds { reply } => {
                let _ = reply.send((self.tiered_log.tail_seq(), self.head_seq));
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
        client_id: ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: zemdb_core::mutation::Operation,
        reply: tokio::sync::oneshot::Sender<Result<CommitResponse, ServerError>>,
    ) -> ControlFlow<()> {
        // 0. Check if client is registered in the room roster
        if let Err(err) = self.ensure_registered(&client_id) {
            let _ = reply.send(Err(err));
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
            // head_seq) is recorded and the client's state recomputed from it.
            self.observe(&client_id, Some(last_ack_seq));
            let (catchup_ops, has_more) = self.catchup_after_commit(last_ack_seq);
            let _ = reply.send(Ok(CommitResponse {
                assigned_seq: existing_seq,
                catchup_ops,
                has_more,
                snapshot_wanted: self.snapshot_wanted_for(&client_id),
            }));
            return ControlFlow::Continue(());
        }

        // 3. Reject a cursor behind the compaction boundary before sequencing anything, so
        // that a rejected commit leaves no trace in the log, the head or the SSE stream. The
        // client's lifecycle state is not a reason to reject: a client that was bootstrapping
        // or dormant commits as soon as its cursor is inside the log.
        if self.tiered_log.is_behind_retention(last_ack_seq) {
            self.reject_behind_log(&client_id, last_ack_seq);
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return ControlFlow::Continue(());
        }

        // 4. Schema validation in O(C)
        if let Err(err) = self.schema.validate_operation(&op) {
            let _ = reply.send(Err(ServerError::SchemaViolation(err.to_string())));
            return ControlFlow::Continue(());
        }

        // 5. Assign strictly monotonic sequence number (local until the append succeeds)
        let new_seq = self.head_seq.next();
        let seq_op = SequencedOperation::new(new_seq, op);

        // 5b. The operation must fit alone in a log record and in a response frame. A
        // mutation that does not is the client's error: it is rejected here, before any I/O,
        // and the room keeps running unchanged.
        if let Err(err) = check_operation_size(&seq_op, Some(mutation_id)) {
            let _ = reply.send(Err(ServerError::BadRequest(err.to_string())));
            return ControlFlow::Continue(());
        }

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
                let _ = reply.send(Err(ServerError::Unavailable(format!(
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

        // 9. Refresh the client's lease, record the cursor reported with the commit and make
        // the client Connected. Persisted on the next maintenance tick.
        self.observe(&client_id, Some(last_ack_seq));

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
            snapshot_wanted: self.snapshot_wanted_for(&client_id),
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
            Ok((ops, has_more)) => fit_in_response(ops, has_more),
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
        client_id: ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
        reply: tokio::sync::oneshot::Sender<Result<SyncBatchResponse, ServerError>>,
    ) {
        // 0. Check if client is registered in the room roster
        if let Err(err) = self.ensure_registered(&client_id) {
            let _ = reply.send(Err(err));
            return;
        }

        // 1. A cursor beyond the head was never delivered by this room
        if from_seq > self.head_seq {
            let _ = reply.send(Err(ServerError::InvalidSequence {
                expected: self.head_seq,
                actual: from_seq,
            }));
            return;
        }

        // 2. Check if from_seq is behind retained log tail
        if self.tiered_log.is_behind_retention(from_seq) {
            self.reject_behind_log(&client_id, from_seq);
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 3. Fetch continuous multi-tier delta batch
        let bounded_batch_size = max_batch_size.clamp(1, 1000);
        match self.tiered_log.fetch_deltas(from_seq, bounded_batch_size) {
            Ok((ops, has_more)) => {
                let (ops, has_more) = fit_in_response(ops, has_more);
                // Asking for the operations after from_seq confirms everything up to it:
                // the cursor advances and the client becomes Connected.
                self.observe(&client_id, Some(from_seq));
                let _ = reply.send(Ok(SyncBatchResponse {
                    head_seq: self.head_seq,
                    ops,
                    has_more,
                    snapshot_wanted: self.snapshot_wanted_for(&client_id),
                }));
            }
            Err(e) => {
                // The range starts inside the retained log, yet the log cannot serve it (a
                // gap). No client can catch up through it, so the client this answer sends
                // to bootstrap needs a snapshot whatever its cursor says.
                if matches!(e, ServerError::BehindCompaction) {
                    self.request_snapshot();
                }
                let _ = reply.send(Err(e));
            }
        }
    }

    fn handle_ack(
        &mut self,
        client_id: ClientId,
        ack_seq: SequenceNumber,
        reply: tokio::sync::oneshot::Sender<Result<SequenceNumber, ServerError>>,
    ) {
        // 0. Check if client is registered in the room roster
        let stored_cursor = match self.ensure_registered(&client_id) {
            Ok(cursor) => cursor,
            Err(err) => {
                let _ = reply.send(Err(err));
                return;
            }
        };

        // 1. Validate that ack_seq <= head_seq to prevent catastrophic log truncation
        if ack_seq > self.head_seq {
            let _ = reply.send(Err(ServerError::InvalidSequence {
                expected: self.head_seq,
                actual: ack_seq,
            }));
            return;
        }

        // 2. The cursor never moves backwards, so an ack below the stored cursor (stale or
        // out of order) changes nothing and succeeds. Retention is checked against the
        // cursor the client ends up with.
        if self
            .tiered_log
            .is_behind_retention(ack_seq.max(stored_cursor))
        {
            self.reject_behind_log(&client_id, ack_seq);
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 3. Advance the cursor, refresh the lease and make the client Connected.
        // The cursor is persisted on the next maintenance tick.
        self.observe(&client_id, Some(ack_seq));

        // 4. Trigger proactive log pruning if all connected clients are past the sequence
        self.prune_to_retention_floor();

        let _ = reply.send(Ok(self.head_seq));
    }

    /// A heartbeat refreshes the lease and recomputes the client's state from its stored
    /// cursor. It never fails because the client fell behind the log: the reply tells the
    /// client whether it must upload a snapshot and which usable snapshot exists.
    fn handle_heartbeat(
        &mut self,
        client_id: ClientId,
        reply: tokio::sync::oneshot::Sender<Result<HeartbeatResponse, ServerError>>,
    ) {
        if let Err(err) = self.ensure_registered(&client_id) {
            let _ = reply.send(Err(err));
            return;
        }

        self.observe(&client_id, None);

        let _ = reply.send(Ok(HeartbeatResponse {
            head_seq: self.head_seq,
            snapshot_wanted: self.snapshot_wanted_for(&client_id),
            active_snapshot_seq: self.usable_snapshot_seq(),
        }));
    }

    /// Returns the client's stored cursor, or `ClientNotRegistered` if it is not in the roster.
    fn ensure_registered(&self, client_id: &ClientId) -> Result<SequenceNumber, ServerError> {
        self.lease_tracker
            .get_client(client_id)
            .map(|entry| entry.last_ack_seq)
            .ok_or_else(|| {
                ServerError::ClientNotRegistered(format!(
                    "Client {} is not registered in room {}",
                    client_id, self.room_id
                ))
            })
    }

    /// Records activity of a registered client (see [`ClientLeaseTracker::observe`]). A client
    /// whose cursor is behind the log becomes `Bootstrapping`, and the room asks for a snapshot.
    fn observe(
        &mut self,
        client_id: &ClientId,
        reported_cursor: Option<SequenceNumber>,
    ) -> Option<ClientState> {
        let state =
            self.lease_tracker
                .observe(client_id, reported_cursor, self.tiered_log.tail_seq());
        if state == Some(ClientState::Bootstrapping) {
            self.request_snapshot();
        }
        state
    }

    /// Bookkeeping for an operation rejected with `BehindCompaction` because the cursor it
    /// carries is behind the log: the client's state is recomputed from its effective cursor.
    /// Only a client that ends up `Bootstrapping` makes the room ask for a snapshot; one whose
    /// stored cursor is still inside the log (for example a request sent before an ack that
    /// pruned the log) stays `Connected` and needs none. The cursor does not move: a cursor
    /// behind the log is never stored, and one inside it is never lowered.
    fn reject_behind_log(&mut self, client_id: &ClientId, reported_cursor: SequenceNumber) {
        self.observe(client_id, Some(reported_cursor));
    }

    /// The relay's active snapshot, only if a client restoring it can catch up from the
    /// retained log (`tail_seq - 1 <= seq <= head_seq`). The relay checks the range when it
    /// accepts a snapshot, but TTL and size compaction move the range on afterwards.
    fn usable_snapshot_seq(&self) -> Option<SequenceNumber> {
        let tail_seq = self.tiered_log.tail_seq();
        self.snapshot_relay
            .active_snapshot_seq(&self.room_id)
            .filter(|seq| retention::is_usable_snapshot(*seq, tail_seq, self.head_seq))
    }

    /// Some client needs a snapshot: unless a usable one exists, turns the snapshot demand on
    /// (or renews it) and makes sure a client is designated to upload one. The renewal is
    /// persisted with the roster on the next maintenance tick, but only once the stored
    /// renewal time is [`persistence_granularity`] old, so a client waiting for a snapshot
    /// does not cause a roster write per heartbeat.
    fn request_snapshot(&mut self) {
        if self.usable_snapshot_seq().is_some() {
            return;
        }
        self.snapshot_demand.renew(Instant::now());
        self.lease_tracker
            .renew_snapshot_demand(SystemTime::now(), self.demand_persistence_granularity);
        self.designate_uploader();
    }

    /// Restores the snapshot demand persisted in the roster when the room reopens, unless it
    /// expired meanwhile or a usable snapshot exists, and designates an uploader among the
    /// clients known now. A renewal time in the future (the wall clock moved backwards) is
    /// stored as now, so that it cannot keep the demand alive across restarts.
    fn restore_snapshot_demand(&mut self) {
        let Some(renewed_at) = self.lease_tracker.snapshot_demand() else {
            return;
        };
        let wall_now = SystemTime::now();
        let restored = self.usable_snapshot_seq().is_none()
            && self
                .snapshot_demand
                .restore(renewed_at, wall_now, Instant::now());
        if restored {
            self.lease_tracker
                .set_snapshot_demand(Some(renewed_at.min(wall_now)));
            self.designate_uploader();
        } else {
            self.lease_tracker.set_snapshot_demand(None);
        }
    }

    /// Keeps a `Connected` client designated to upload while the demand is on, replacing a
    /// designee that timed out or left. Every new designation is announced to SSE subscribers,
    /// who check with a heartbeat whether they are the one.
    fn designate_uploader(&mut self) {
        let upload_in_progress = self.snapshot_relay.has_upload_in_progress(&self.room_id);
        let tracker = &self.lease_tracker;
        let designated = self.snapshot_demand.designate(
            Instant::now(),
            upload_in_progress,
            |client_id| tracker.is_connected(client_id),
            |excluded| tracker.pick_uploader(excluded),
        );
        if let Some(client_id) = designated {
            info!(room = %self.room_id, client = %client_id, "Client designated to upload a snapshot");
            let _ = self.events_tx.send(RoomEvent::SnapshotWanted);
        }
    }

    /// Whether `client_id` is the client currently designated to upload a snapshot.
    fn snapshot_wanted_for(&self, client_id: &ClientId) -> bool {
        self.snapshot_demand.designee() == Some(client_id) && self.usable_snapshot_seq().is_none()
    }

    /// Maintenance of the snapshot signals: announces a new usable snapshot, which also ends
    /// the demand; otherwise lets an unrenewed demand expire and keeps an uploader designated.
    fn update_snapshot_signals(&mut self) {
        if let Some(seq) = self.usable_snapshot_seq() {
            if self.announced_snapshot != Some(seq) {
                self.announced_snapshot = Some(seq);
                let _ = self.events_tx.send(RoomEvent::SnapshotAvailable(seq));
            }
            if self.snapshot_demand.is_active() {
                debug!(room = %self.room_id, snapshot_seq = %seq, "Snapshot demand satisfied");
                self.snapshot_demand.satisfy();
                self.lease_tracker.set_snapshot_demand(None);
            }
            return;
        }
        if self.snapshot_demand.expire(Instant::now()) {
            info!(room = %self.room_id, "Snapshot demand expired without a snapshot");
            self.lease_tracker.set_snapshot_demand(None);
            return;
        }
        self.designate_uploader();
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
        // The relay only accepts snapshots inside the retained range, but the range moves on
        // (TTL and size compaction ignore the anchor), so a snapshot that fell out of it can no
        // longer anchor anything and is ignored.
        let retention_floor = match self.usable_snapshot_seq() {
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
        self.lease_tracker.check_timeouts(
            self.lease_timeout,
            self.dormant_after,
            self.tiered_log.tail_seq(),
        );

        // 2. Run TieredLog TTL and size compaction (non-blocking via spawn_blocking)
        if let Err(e) = self.tiered_log.run_maintenance().await {
            warn!(room = %self.room_id, error = %e, "TieredLog maintenance error");
        }

        // 3. Trigger proactive cursor-driven pruning if all non-dormant clients are Connected,
        // bounded by active_snapshot_seq (Retention Anchor)
        self.prune_to_retention_floor();

        // 4. Snapshot demand, uploader designation and snapshot announcements
        self.update_snapshot_signals();

        // 5. Persist cursor and lease changes accumulated since the previous tick
        self.persist_roster();
    }
}

/// Keeps the longest prefix of `ops` that fits in one response frame
/// ([`MAX_RESPONSE_OPS_BYTES`]), and at least one operation (every accepted operation fits
/// alone). If any operation is left out the batch is flagged `has_more`, so the client asks
/// for the rest.
fn fit_in_response(
    mut ops: Vec<SequencedOperation>,
    has_more: bool,
) -> (Vec<SequencedOperation>, bool) {
    let mut total: u64 = 0;
    let fitting = ops
        .iter()
        .position(|op| {
            total = total.saturating_add(encoded_len(op).unwrap_or(u64::MAX));
            total > MAX_RESPONSE_OPS_BYTES
        })
        .unwrap_or(ops.len())
        .max(1);
    if fitting < ops.len() {
        ops.truncate(fitting);
        return (ops, true);
    }
    (ops, has_more)
}

#[cfg(test)]
#[path = "tests/room.rs"]
mod tests;
