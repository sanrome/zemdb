use futures::FutureExt;
use std::ops::ControlFlow;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};
use zemdb_core::id::{ClientId, MutationId, RoomId, SchemaId, SequenceNumber};
use zemdb_core::mutation::Operation;
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
use crate::dedup::{dedup_key, DedupLruCache};
use crate::durable;
use crate::error::ServerError;
use crate::log::{RoomLifecyclePolicy, TieredLog};
use crate::relay::{LogBounds, SnapshotRelay, Uploader};
use crate::response_budget::{no_room, InflightBudget, Reservation};

/// Period of the fast maintenance tick, which only touches memory (and the roster file when
/// it changed): lease timeouts and state transitions, snapshot signals, roster persistence and
/// the inactivity check.
const FAST_TICK_PERIOD: Duration = Duration::from_secs(1);

/// Period of the slow maintenance tick, the disk maintenance of the log: compression of aged
/// warm segments, cold TTL, disk quota, the RAM TTL window and a backup pass of the
/// cursor-driven pruning (which also runs on every Ack).
const SLOW_TICK_PERIOD: Duration = Duration::from_secs(30);

/// Commands a room's mailbox queues before senders have to wait.
const MAILBOX_CAPACITY: usize = 1024;

/// Events an SSE subscriber may fall behind by before it misses some and is told it lagged.
const EVENTS_CAPACITY: usize = 256;

/// Most operations returned as catch-up with an acknowledged commit.
const COMMIT_CATCHUP_BATCH: u32 = 100;

/// Largest batch a sync is served with, whatever `max_batch_size` it asks for.
const MAX_SYNC_BATCH: u32 = 1000;

/// The timings of a room actor. Production always uses [`ActorTimings::default`]; tests
/// shorten them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActorTimings {
    /// Time a client designated to upload a snapshot has to start the upload.
    pub(crate) designation_timeout: Duration,
    /// Period of the fast (memory) maintenance tick.
    pub(crate) fast_tick: Duration,
    /// Period of the slow (disk) maintenance tick.
    pub(crate) slow_tick: Duration,
}

impl Default for ActorTimings {
    fn default() -> Self {
        Self {
            designation_timeout: DESIGNATION_TIMEOUT,
            fast_tick: FAST_TICK_PERIOD,
            slow_tick: SLOW_TICK_PERIOD,
        }
    }
}

/// How a room actor ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorExit {
    /// The actor stopped on purpose: shutdown, inactivity, a closed mailbox, or a failure
    /// that requires reopening the room from disk.
    Stopped,
    /// The actor panicked. Commands still queued were answered with `Unavailable`.
    Panicked,
}

/// Single-writer actor of one room: sequencing, durability, client leases and snapshot signals.
pub struct RoomActor {
    room_id: RoomId,
    /// Tells this room apart from earlier rooms created with the same id.
    incarnation: u128,
    schema_id: SchemaId,
    schema: Arc<Schema>,
    dedup_cache: DedupLruCache,
    tiered_log: TieredLog,
    lease_tracker: ClientLeaseTracker,
    head_seq: SequenceNumber,
    events_tx: broadcast::Sender<RoomEvent>,
    receiver: mpsc::Receiver<RoomCommand>,
    /// Effective lifecycle policy: the server defaults overlaid with the room's overrides.
    policy: RoomLifecyclePolicy,
    /// Directory holding the room's files; keys the fail points of this room in tests.
    #[cfg(test)]
    room_dir: PathBuf,
    /// When the last command arrived; drives the shutdown for inactivity.
    last_command_at: Instant,
    /// Set once the actor closed its mailbox to shut down for inactivity.
    stopping_idle: bool,
    snapshot_relay: Arc<SnapshotRelay>,
    response_budget: Arc<InflightBudget>,
    snapshot_demand: SnapshotDemand,
    /// How stale the persisted renewal time of the snapshot demand may get.
    demand_persistence_granularity: Duration,
    /// The last usable snapshot announced to SSE subscribers.
    announced_snapshot: Option<SequenceNumber>,
    /// The highest cursor from which a sync found a gap of the log: a snapshot at or below it
    /// cannot be restored, so it is not usable and a new upload replaces it.
    unrestorable_through: Option<SequenceNumber>,
    timings: ActorTimings,
}

impl RoomActor {
    /// Recovers the room from disk and builds its actor without starting it, with the given
    /// `timings`. `incarnation` tells this room apart from earlier rooms created with its id,
    /// and `lifecycle_policy` is its effective policy.
    ///
    /// Reads the room's files: async callers run it through
    /// [`blocking_io`](crate::blocking::blocking_io).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        room_id: RoomId,
        incarnation: u128,
        schema_id: SchemaId,
        schema: Arc<Schema>,
        data_dir: impl AsRef<Path>,
        config: Arc<ServerConfig>,
        lifecycle_policy: RoomLifecyclePolicy,
        snapshot_relay: Arc<SnapshotRelay>,
        response_budget: Arc<InflightBudget>,
        timings: ActorTimings,
    ) -> Result<(mpsc::Sender<RoomCommand>, Self), ServerError> {
        let room_dir = room_dir(data_dir.as_ref(), &room_id);
        durable::create_dir_all_synced(&room_dir)?;

        let clients_path = room_dir.join(format!("meta_clients_{}.json", room_id.as_str()));

        // Fills RAM from active.wal and reads back only the mutation IDs the deduplication
        // cache holds.
        let mut dedup_cache = DedupLruCache::new(config.dedup_lru_capacity);
        let (tiered_log, recovered_mutations) = TieredLog::open_or_create_with_dedup_window(
            &room_dir,
            lifecycle_policy.clone(),
            dedup_cache.capacity(),
        )?;

        // Oldest first, so that the window evicts in log order, as it did before the reopen.
        dedup_cache.hydrate(recovered_mutations);

        let head_seq = tiered_log.head_seq();

        let lease_tracker = ClientLeaseTracker::open_or_create(&clients_path)?;

        let (command_tx, command_rx) = mpsc::channel(MAILBOX_CAPACITY);
        let (events_tx, _) = broadcast::channel(EVENTS_CAPACITY);

        let demand_ttl = lifecycle_policy.snapshot_demand_ttl;
        let snapshot_demand = SnapshotDemand::new(demand_ttl, timings.designation_timeout);

        let mut actor = Self {
            room_id: room_id.clone(),
            incarnation,
            schema_id,
            schema,
            dedup_cache,
            tiered_log,
            lease_tracker,
            head_seq,
            events_tx,
            receiver: command_rx,
            policy: lifecycle_policy,
            #[cfg(test)]
            room_dir,
            last_command_at: Instant::now(),
            stopping_idle: false,
            snapshot_relay,
            response_budget,
            snapshot_demand,
            demand_persistence_granularity: persistence_granularity(demand_ttl),
            announced_snapshot: None,
            unrestorable_through: None,
            timings,
        };
        // A snapshot that was already usable before this actor started is not news.
        actor.announced_snapshot = actor.usable_snapshot_seq();
        actor.restore_snapshot_demand();
        actor.publish_snapshot_floor();

        info!(room = %room_id, head_seq = %head_seq, "Opened RoomActor");

        Ok((command_tx, actor))
    }

    /// Runs the actor until it stops, and reports how it ended.
    ///
    /// A panic is caught here instead of ending the task: the room is reported as terminated
    /// by a panic and every command still queued is answered with `Unavailable`, so those
    /// callers can retry against the room reopened from disk. The command being handled when
    /// the panic occurred loses its reply; `RoomManager` answers it with `Internal`, since
    /// retrying it could panic again. Dropping the actor afterwards releases the room's files.
    pub async fn run(mut self) -> ActorExit {
        let outcome = AssertUnwindSafe(self.run_loop()).catch_unwind().await;
        match outcome {
            Ok(()) => ActorExit::Stopped,
            Err(panic) => {
                error!(
                    room = %self.room_id,
                    panic = panic_message(panic.as_ref()),
                    "RoomActor panicked; the next request reopens the room from disk"
                );
                self.receiver.close();
                // After `close`, `recv` yields the queued commands and then `None`.
                while let Some(command) = self.receiver.recv().await {
                    command.reject(ServerError::Unavailable(format!(
                        "Room {} stopped after an internal error; retry the request",
                        self.room_id
                    )));
                }
                ActorExit::Panicked
            }
        }
    }

    /// Primary actor loop running sequentially on Tokio.
    async fn run_loop(&mut self) {
        // The first ticks fire one period after start, not immediately.
        let start = Instant::now();
        let mut fast_timer =
            tokio::time::interval_at(start + self.timings.fast_tick, self.timings.fast_tick);
        let mut slow_timer =
            tokio::time::interval_at(start + self.timings.slow_tick, self.timings.slow_tick);

        loop {
            tokio::select! {
                cmd = self.receiver.recv() => {
                    self.last_command_at = Instant::now();
                    match cmd {
                        Some(RoomCommand::Shutdown { reply }) => {
                            debug!(room = %self.room_id, "Shutdown command received, terminating actor loop");
                            #[cfg(test)]
                            crate::fail_point::hook("room_shutdown", &self.room_dir);
                            self.persist_roster_at_stop();
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
                _ = fast_timer.tick() => {
                    self.run_fast_maintenance();
                    if self.is_idle() {
                        // A room whose idle timeout is shorter than the slow tick would never
                        // reach its first slow tick, so it runs the disk maintenance on the way
                        // out instead.
                        if self.drain_for_idle_shutdown().await.is_continue() {
                            self.run_disk_maintenance().await;
                        }
                        break;
                    }
                }
                _ = slow_timer.tick() => {
                    self.run_disk_maintenance().await;
                }
            }
        }

        // Every exit flushes pending cursor changes and the clients' last activity. Dropping
        // the actor afterwards releases the log's file lock and drops any command still
        // queued, whose callers observe a closed reply channel instead of waiting forever.
        self.persist_roster_at_stop();
    }

    /// Whether the room has been inactive for its `idle_timeout`: no command in that time,
    /// no SSE subscriber and no snapshot upload in progress.
    fn is_idle(&self) -> bool {
        let Some(idle_timeout) = self.policy.idle_timeout else {
            return false;
        };
        self.last_command_at.elapsed() >= idle_timeout
            && self.events_tx.receiver_count() == 0
            && self
                .snapshot_relay
                .upload_in_progress(&self.room_id)
                .is_none()
    }

    /// Starts the shutdown for inactivity. Closing the mailbox makes `RoomManager` respawn the
    /// room for the next request, and a request that finds the mailbox closed is sent to the
    /// new actor; commands that were already queued are handled normally first, so no request
    /// is lost or refused because of the shutdown. The roster is persisted when the loop ends.
    /// `Break` means a queued command left the room unable to operate safely.
    async fn drain_for_idle_shutdown(&mut self) -> ControlFlow<()> {
        info!(room = %self.room_id, "Room inactive; shutting down its actor");
        self.receiver.close();
        self.stopping_idle = true;
        // After `close`, `recv` yields the queued commands and then `None`.
        while let Some(command) = self.receiver.recv().await {
            self.handle_command(command)?;
        }
        ControlFlow::Continue(())
    }

    /// Handles one command. `Break` means the room can no longer operate safely and the
    /// actor must stop so that the next request reopens it from disk.
    fn handle_command(&mut self, command: RoomCommand) -> ControlFlow<()> {
        #[cfg(test)]
        if crate::fail_point::check("room_command_panic", &self.room_dir).is_err() {
            panic!("injected panic while handling a room command");
        }

        match command {
            RoomCommand::RegisterClient {
                client_id,
                current_seq,
                known_incarnation,
                reply,
            } => {
                let _ = reply.send(self.register(&client_id, current_seq, known_incarnation));
            }

            RoomCommand::DeregisterClient { client_id, reply } => {
                self.lease_tracker.deregister_client(&client_id);
                let _ = reply.send(Ok(()));
            }

            RoomCommand::GetSchema { reply } => {
                let _ = reply.send(Ok((self.schema_id.clone(), Arc::clone(&self.schema))));
            }

            RoomCommand::ReloadSchema {
                schema_id,
                schema,
                reply,
            } => {
                // A reload of another schema is not for this room: it was sent to a room that
                // used it before moving to its current one.
                if schema_id == self.schema_id {
                    self.schema = schema;
                    let _ = self
                        .events_tx
                        .send(RoomEvent::SchemaReloaded(self.schema_id.clone()));
                }
                let _ = reply.send(Ok(()));
            }

            RoomCommand::Commit {
                client_id,
                mutation_id,
                last_ack_seq,
                op,
                reply,
            } => {
                let mut flow = ControlFlow::Continue(());
                let res = self.commit(&client_id, mutation_id, last_ack_seq, op, &mut flow);
                let _ = reply.send(res);
                return flow;
            }

            RoomCommand::Sync {
                client_id,
                from_seq,
                max_batch_size,
                reply,
            } => {
                let _ = reply.send(self.sync(&client_id, from_seq, max_batch_size));
            }

            RoomCommand::Ack {
                client_id,
                ack_seq,
                reply,
            } => {
                let _ = reply.send(self.ack(&client_id, ack_seq));
            }

            RoomCommand::Heartbeat { client_id, reply } => {
                let _ = reply.send(self.heartbeat(&client_id));
            }

            RoomCommand::SubscribeEvents { reply } => {
                // A subscription to an actor that is about to stop would end at once.
                let res = if self.stopping_idle {
                    Err(ServerError::Unavailable(format!(
                        "Room {} is shutting down for inactivity; retry the request",
                        self.room_id
                    )))
                } else {
                    Ok(self.events_tx.subscribe())
                };
                let _ = reply.send(res);
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
                    lifecycle: self.policy.clone(),
                };
                let _ = reply.send(Ok(metrics));
            }

            RoomCommand::GetLogBounds { reply } => {
                let _ = reply.send(Ok(self.log_bounds()));
            }

            RoomCommand::GetClientCursor { client_id, reply } => {
                let cursor = self
                    .lease_tracker
                    .get_client(&client_id)
                    .map(|c| c.last_ack_seq);
                let _ = reply.send(Ok(cursor));
            }

            RoomCommand::Shutdown { reply } => {
                let _ = reply.send(());
            }
        }
        ControlFlow::Continue(())
    }

    fn register(
        &mut self,
        client_id: &ClientId,
        current_seq: Option<SequenceNumber>,
        known_incarnation: Option<u128>,
    ) -> Result<RegisterResponse, ServerError> {
        if let Some(seq) = current_seq {
            // A cursor of an earlier room with this id would mix that room's rows with this
            // one's.
            if known_incarnation != Some(self.incarnation) {
                return Err(ServerError::RoomReset(self.room_id.to_string()));
            }
            // Accepting a cursor beyond the head would make the client the preferred snapshot
            // uploader and could raise the pruning floor above the head.
            self.check_within_head(seq)?;
        }
        let tail_seq = self.tiered_log.tail_seq();
        let state = self
            .lease_tracker
            .register_client(client_id, current_seq, tail_seq);
        if state == ClientState::Bootstrapping {
            self.request_snapshot();
        }
        Ok(RegisterResponse {
            head_seq: self.head_seq,
            tail_seq,
            schema_id: self.schema_id.clone(),
            schema: Arc::clone(&self.schema),
            active_snapshot_seq: self.usable_snapshot_seq(),
            room_incarnation: self.incarnation,
        })
    }

    /// Commits `op`. Sets `flow` to `Break`, with the mailbox already closed, when the room
    /// must stop after replying.
    fn commit(
        &mut self,
        client_id: &ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: Operation,
        flow: &mut ControlFlow<()>,
    ) -> Result<CommitResponse, ServerError> {
        self.ensure_registered(client_id)?;
        self.check_within_head(last_ack_seq)?;
        let key = dedup_key(client_id, &mutation_id);

        // Exactly-once idempotency. Checked before any rejection based on client state:
        // a retried mutation was already committed and replicated, so it must always be
        // acknowledged with its original sequence, never reported as a failure.
        if let Some(existing_seq) = self.dedup_cache.is_duplicate(&key) {
            // The retry is accepted, so the cursor it reports (already validated against
            // head_seq) is recorded and the client's state recomputed from it.
            self.observe(client_id, Some(last_ack_seq));
            let reservation = self.reserve_frame(client_id);
            let (catchup_ops, has_more) =
                self.catchup_after_commit(last_ack_seq, reservation.is_some());
            return Ok(CommitResponse {
                assigned_seq: existing_seq,
                catchup_ops,
                has_more,
                snapshot_wanted: self.snapshot_wanted_for(client_id),
                schema_version: self.schema.version(),
                schema_id: self.schema_id.clone(),
                reservation,
            });
        }

        // Reject a cursor behind the compaction boundary before sequencing anything, so
        // that a rejected commit leaves no trace in the log, the head or the SSE stream. The
        // client's lifecycle state is not a reason to reject: a client that was bootstrapping
        // or dormant commits as soon as its cursor is inside the log.
        if self.tiered_log.is_behind_retention(last_ack_seq) {
            self.reject_behind_log(client_id, last_ack_seq);
            return Err(ServerError::BehindCompaction);
        }

        // The owner an operation declares must be the owner of the client sending it. Checked
        // against the validated operation, before anything is sequenced.
        self.schema
            .validate_operation(&op)
            .and_then(|()| self.schema.check_owner(&op, client_id))
            .map_err(|err| ServerError::SchemaViolation(err.to_string()))?;

        // The room never assigns u64::MAX, so that `head_seq + 1` always exists.
        if self.head_seq.get() >= u64::MAX - 1 {
            return Err(ServerError::Internal(format!(
                "Room {} has no sequence numbers left",
                self.room_id
            )));
        }
        // The new sequence stays local until the append succeeds.
        let new_seq = self.head_seq.next();
        let seq_op = SequencedOperation::new(new_seq, op);

        // The operation must fit alone in a log record and in a response frame. A
        // mutation that does not is the client's error: it is rejected here, before any I/O,
        // and the room keeps running unchanged.
        check_operation_size(&seq_op, Some(key))
            .map_err(|err| ServerError::BadRequest(err.to_string()))?;

        // A failed write or fsync leaves the log in an unknown state: the record may or may
        // not be on disk, and a failed fsync cannot be retried safely. The room stops and is
        // recovered from disk by the next request; if the record survived, deduplication
        // acknowledges a retry of the same mutation with the sequence that reached disk.
        let outcome = match self.tiered_log.append(seq_op.clone(), Some(key)) {
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
                *flow = ControlFlow::Break(());
                return Err(ServerError::Unavailable(format!(
                    "Room {} could not persist the commit and is restarting; retry the request",
                    self.room_id
                )));
            }
        };

        // From here on the mutation is durable: the reply must acknowledge it, so nothing
        // below may return an error.

        self.dedup_cache.record(key, new_seq);
        self.head_seq = new_seq;

        // Refresh the client's lease, record the cursor reported with the commit and make
        // the client Connected. Persisted on the next maintenance tick.
        self.observe(client_id, Some(last_ack_seq));

        let _ = self.events_tx.send(RoomEvent::HeadAdvanced(new_seq));

        // Catch-up for 1-RTT synchronization.
        let reservation = self.reserve_frame(client_id);
        let with_catchup = reservation.is_some();
        let (catchup_ops, has_more) = if with_catchup && last_ack_seq.get() + 1 == new_seq.get() {
            (vec![seq_op], false)
        } else {
            self.catchup_after_commit(last_ack_seq, with_catchup)
        };

        // A failed segment rotation after a durable append leaves the segment files in an
        // uncertain state. The commit is still acknowledged; then the room stops and is
        // recovered from disk by the next request.
        if let Some(err) = outcome.rotation_error {
            error!(
                room = %self.room_id,
                error = %err,
                "Segment rotation failed after a durable commit; stopping room actor for recovery from disk"
            );
            self.receiver.close();
            *flow = ControlFlow::Break(());
        }

        Ok(CommitResponse {
            assigned_seq: new_seq,
            catchup_ops,
            has_more,
            snapshot_wanted: self.snapshot_wanted_for(client_id),
            schema_version: self.schema.version(),
            schema_id: self.schema_id.clone(),
            reservation,
        })
    }

    /// A frame of `client_id`'s part of the budget of response bytes in flight, reserved right
    /// before a response is read from the log, so that a command waiting in the mailbox holds
    /// none. The response carries it to its encoding.
    fn reserve_frame(&self, client_id: &ClientId) -> Option<Reservation> {
        let holder = (self.room_id.clone(), Uploader::Client(client_id.clone()));
        self.response_budget.reserve_frame(holder)
    }

    /// Builds the catch-up batch returned with an acknowledged commit, starting after the
    /// client's cursor, unless the budget had no room for it (`with_catchup` unset).
    ///
    /// The commit itself is already durable, so a failure here must not turn into an error
    /// reply. Without a catch-up, the batch is empty and flagged `has_more`, which sends the
    /// client to `/sync`, where any underlying error (for example `BehindCompaction`) is
    /// reported.
    fn catchup_after_commit(
        &self,
        last_ack_seq: SequenceNumber,
        with_catchup: bool,
    ) -> (Vec<SequencedOperation>, bool) {
        if !with_catchup {
            return (Vec::new(), true);
        }
        match self
            .tiered_log
            .fetch_deltas(last_ack_seq, COMMIT_CATCHUP_BATCH)
        {
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

    fn sync(
        &mut self,
        client_id: &ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
    ) -> Result<SyncBatchResponse, ServerError> {
        self.ensure_registered(client_id)?;
        self.check_within_head(from_seq)?;

        if self.tiered_log.is_behind_retention(from_seq) {
            self.reject_behind_log(client_id, from_seq);
            return Err(ServerError::BehindCompaction);
        }

        let reservation = self.reserve_frame(client_id).ok_or_else(no_room)?;
        let bounded_batch_size = max_batch_size.clamp(1, MAX_SYNC_BATCH);
        let (ops, has_more) = match self.tiered_log.fetch_deltas(from_seq, bounded_batch_size) {
            Ok((ops, has_more)) => fit_in_response(ops, has_more),
            Err(e) => {
                // The range starts inside the retained log, yet the log cannot serve it (a
                // gap). No client can catch up through it, so the client this answer sends
                // to bootstrap needs a snapshot whatever its cursor says, and a snapshot at
                // or below the cursor, restored, would run into the same gap.
                if matches!(e, ServerError::BehindCompaction) {
                    self.unrestorable_through = self.unrestorable_through.max(Some(from_seq));
                    self.publish_snapshot_floor();
                    self.request_snapshot();
                }
                return Err(e);
            }
        };
        // Asking for the operations after from_seq confirms everything up to it: the cursor
        // advances and the client becomes Connected.
        self.observe(client_id, Some(from_seq));
        Ok(SyncBatchResponse {
            head_seq: self.head_seq,
            ops,
            has_more,
            snapshot_wanted: self.snapshot_wanted_for(client_id),
            schema_version: self.schema.version(),
            schema_id: self.schema_id.clone(),
            reservation,
        })
    }

    fn ack(
        &mut self,
        client_id: &ClientId,
        ack_seq: SequenceNumber,
    ) -> Result<SequenceNumber, ServerError> {
        let stored_cursor = self.ensure_registered(client_id)?;
        self.check_within_head(ack_seq)?;

        // The cursor never moves backwards, so an ack below the stored cursor (stale or
        // out of order) changes nothing and succeeds. Retention is checked against the
        // cursor the client ends up with.
        if self
            .tiered_log
            .is_behind_retention(ack_seq.max(stored_cursor))
        {
            self.reject_behind_log(client_id, ack_seq);
            return Err(ServerError::BehindCompaction);
        }

        // Advance the cursor, refresh the lease and make the client Connected.
        // The cursor is persisted on the next maintenance tick.
        self.observe(client_id, Some(ack_seq));

        self.prune_to_retention_floor();

        Ok(self.head_seq)
    }

    /// A heartbeat refreshes the lease and recomputes the client's state from its stored
    /// cursor. It never fails because the client fell behind the log: the reply tells the
    /// client whether it must upload a snapshot and which usable snapshot exists.
    fn heartbeat(&mut self, client_id: &ClientId) -> Result<HeartbeatResponse, ServerError> {
        self.ensure_registered(client_id)?;
        self.observe(client_id, None);
        Ok(HeartbeatResponse {
            head_seq: self.head_seq,
            snapshot_wanted: self.snapshot_wanted_for(client_id),
            active_snapshot_seq: self.usable_snapshot_seq(),
            schema_version: self.schema.version(),
            schema_id: self.schema_id.clone(),
        })
    }

    /// A cursor beyond the head was never delivered by this room.
    fn check_within_head(&self, seq: SequenceNumber) -> Result<(), ServerError> {
        if seq > self.head_seq {
            return Err(ServerError::InvalidSequence {
                expected: self.head_seq,
                actual: seq,
            });
        }
        Ok(())
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
    /// retained log (`tail_seq - 1 <= seq <= head_seq`) and no gap was found after it. The
    /// relay checks the range when it accepts a snapshot, but TTL and size compaction move the
    /// range on afterwards.
    fn usable_snapshot_seq(&self) -> Option<SequenceNumber> {
        let bounds = self.log_bounds();
        self.snapshot_relay
            .active_snapshot_seq(&self.room_id)
            .filter(|seq| bounds.admits(*seq))
    }

    /// The retained range of the log, and the highest cursor from which a sync found a gap.
    fn log_bounds(&self) -> LogBounds {
        LogBounds {
            tail_seq: self.tiered_log.tail_seq(),
            head_seq: self.head_seq,
            unrestorable_through: self.unrestorable_through,
        }
    }

    /// Tells the relay the lowest sequence of a usable snapshot when the actor opens, after the
    /// log moved and when a gap is found, so that it does not install an upload checked against
    /// an older range.
    fn publish_snapshot_floor(&self) {
        let floor = self.log_bounds().snapshot_floor();
        self.snapshot_relay.set_snapshot_floor(&self.room_id, floor);
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
    /// designee that timed out or left; an upload by another member does not keep a designee
    /// that does not upload. Every new designation is announced to SSE subscribers, who check
    /// with a heartbeat whether they are the one.
    fn designate_uploader(&mut self) {
        let designee_uploading = self
            .snapshot_relay
            .upload_in_progress_by(&self.room_id, self.snapshot_demand.designee());
        let tracker = &self.lease_tracker;
        let designated = self.snapshot_demand.designate(
            Instant::now(),
            designee_uploading,
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
    /// the lowest connected cursor, bounded by the active snapshot and by a snapshot upload in
    /// progress (Retention Anchor).
    ///
    /// The cursors that justify the prune are persisted before any segment is deleted, so the
    /// roster on disk never falls behind the retained log; otherwise a restart would treat
    /// those clients as fallen behind and force them to a snapshot. A prune that deletes no
    /// segment only trims RAM and leaves the roster for the fast tick.
    fn prune_to_retention_floor(&mut self) {
        let Some(min_ack) = self.lease_tracker.min_connected_ack_seq() else {
            return;
        };
        // The relay only accepts snapshots inside the retained range, but the range moves on
        // (TTL and size compaction ignore the anchor), so a snapshot that fell out of it can no
        // longer anchor anything and is ignored. An upload that fell out of it holds pruning
        // back only until it completes (and fails) or goes idle.
        let retention_floor = [
            self.usable_snapshot_seq(),
            self.snapshot_relay.upload_in_progress(&self.room_id),
        ]
        .into_iter()
        .flatten()
        .fold(min_ack, SequenceNumber::min);

        // Segments are deleted only when they end below the floor, and none ends below the
        // current tail, so a floor at or below the tail deletes nothing.
        if retention_floor <= self.tiered_log.tail_seq() {
            return;
        }

        if self.tiered_log.has_prunable_below(retention_floor) {
            if let Err(err) = self.lease_tracker.persist_if_dirty() {
                warn!(
                    room = %self.room_id,
                    error = %err,
                    "Failed to persist clients roster; skipping proactive log pruning"
                );
                return;
            }
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

    /// Like [`persist_roster`](Self::persist_roster), but also writes the roster when only the
    /// clients' activity changed, so that their inactivity keeps counting after the room
    /// reopens (for example after a shutdown for inactivity).
    fn persist_roster_at_stop(&mut self) {
        if let Err(err) = self.lease_tracker.persist_if_dirty_or_active() {
            warn!(room = %self.room_id, error = %err, "Failed to persist clients roster");
        }
    }

    /// The fast tick: memory-only maintenance, plus writing the roster if it changed.
    fn run_fast_maintenance(&mut self) {
        // Connected/Bootstrapping -> Disconnected -> Dormant.
        self.lease_tracker.check_timeouts(
            self.policy.lease_timeout,
            self.policy.dormant_after,
            self.policy.forget_dormant_after,
            self.tiered_log.tail_seq(),
        );

        self.update_snapshot_signals();
        // Cursor and lease changes accumulated since the previous tick.
        self.persist_roster();
    }

    /// The slow tick: disk maintenance of the log, served from its segment index.
    async fn run_disk_maintenance(&mut self) {
        // Compression (in spawn_blocking), RAM TTL window, cold TTL and quota.
        if let Err(e) = self.tiered_log.run_maintenance().await {
            warn!(room = %self.room_id, error = %e, "TieredLog maintenance error");
        }

        // Backup pass of the cursor-driven pruning, which normally runs on every Ack: a floor
        // can also move without an Ack (cursors reported by commits, clients becoming Dormant).
        self.prune_to_retention_floor();
        self.publish_snapshot_floor();
    }
}

/// Directory of the room `room_id` under the server's `data_dir`.
pub(crate) fn room_dir(data_dir: &Path, room_id: &RoomId) -> PathBuf {
    data_dir.join("rooms").join(room_id.as_str())
}

/// Text of a caught panic payload, when it is a string.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
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
