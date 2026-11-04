use futures::FutureExt;
use std::ops::ControlFlow;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, mpsc, oneshot};
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
use crate::actor::ops_event::OpsEventBudget;
use crate::actor::snapshot_demand::{persistence_granularity, SnapshotDemand, DESIGNATION_TIMEOUT};
use crate::config::ServerConfig;
use crate::dedup::{dedup_key, DedupLruCache};
use crate::durable;
use crate::error::ServerError;
use crate::io_pool::{run_holding, IoThreads};
use crate::log::budget::OpSize;
use crate::log::tiered_log::{AppendOutcome, MaintenanceClock};
use crate::log::{RoomLifecyclePolicy, TieredLog};
use crate::relay::{LogBounds, SnapshotRelay, Uploader};
use crate::response_budget::{no_room, InflightBudget, Reservation, RESPONSE_OVERHEAD};

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

/// Most commits a group takes from the mailbox. Below [`COMMIT_CATCHUP_BATCH`], so that the
/// catch-up of a client that was up to date before the group reaches the group's end.
const GROUP_MAX_COMMITS: usize = 64;

/// Bytes of new operations (as the RAM window counts them) after which a group takes no
/// more commits, so that one group writes at most this much plus one operation before its
/// `fsync`.
const GROUP_MAX_BYTES: u64 = 1024 * 1024;

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
    /// The room's log; away only while the actor waits for an operation on it in the file I/O
    /// threads ([`log_io`](Self::log_io)).
    tiered_log: Option<TieredLog>,
    /// The threads that run the file I/O of the room.
    io: Arc<IoThreads>,
    lease_tracker: ClientLeaseTracker,
    head_seq: SequenceNumber,
    events_tx: broadcast::Sender<RoomEvent>,
    mailbox: Mailbox,
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
    /// The server's total of the bytes retained by `ops` events, which this room's events take
    /// from.
    ops_events: Arc<OpsEventBudget>,
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
    /// Reads the room's files: async callers run it on the file I/O threads, `io`, which run
    /// the actor's file I/O as well.
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
        ops_events: Arc<OpsEventBudget>,
        timings: ActorTimings,
        io: Arc<IoThreads>,
    ) -> Result<(mpsc::Sender<RoomCommand>, Self), ServerError> {
        let room_dir = room_dir(data_dir.as_ref(), &room_id);
        crate::fail_point::hook("room_open", &room_dir);
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
            tiered_log: Some(tiered_log),
            io,
            lease_tracker,
            head_seq,
            events_tx,
            mailbox: Mailbox {
                receiver: command_rx,
                held: None,
            },
            policy: lifecycle_policy,
            #[cfg(test)]
            room_dir,
            last_command_at: Instant::now(),
            stopping_idle: false,
            snapshot_relay,
            response_budget,
            ops_events,
            snapshot_demand,
            demand_persistence_granularity: persistence_granularity(demand_ttl),
            announced_snapshot: None,
            unrestorable_through: None,
            timings,
        };
        // A snapshot that was already usable before this actor started is not news.
        actor.announced_snapshot = actor.usable_snapshot_seq();
        actor.restore_snapshot_demand();

        info!(room = %room_id, head_seq = %head_seq, "Opened RoomActor");

        Ok((command_tx, actor))
    }

    /// Runs the actor until it stops, and reports how it ended.
    ///
    /// A panic is caught here instead of ending the task: the room is reported as terminated
    /// by a panic and every command still queued is answered with `Unavailable`, so those
    /// callers can retry against the room reopened from disk. The command being handled when
    /// the panic occurred (with a group of commits, every commit of the group) loses its reply;
    /// `RoomManager` answers it with `Internal`, since retrying it could panic again. Dropping
    /// the actor afterwards releases the room's files.
    pub async fn run(mut self) -> ActorExit {
        // Published once the actor runs, so that an actor opened for a spawn that was dropped
        // leaves none: the task that runs the actor clears it when the actor ends.
        self.publish_snapshot_floor();
        let outcome = AssertUnwindSafe(self.run_loop()).catch_unwind().await;
        match outcome {
            Ok(()) => ActorExit::Stopped,
            Err(panic) => {
                error!(
                    room = %self.room_id,
                    panic = panic_message(panic.as_ref()),
                    "RoomActor panicked; the next request reopens the room from disk"
                );
                self.mailbox.close();
                // After `close`, `recv` yields the queued commands and then `None`.
                while let Some(command) = self.mailbox.recv().await {
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
                cmd = self.mailbox.recv() => {
                    self.last_command_at = Instant::now();
                    match cmd {
                        Some(RoomCommand::Shutdown { reply }) => {
                            debug!(room = %self.room_id, "Shutdown command received, terminating actor loop");
                            #[cfg(test)]
                            crate::fail_point::hook("room_shutdown", &self.room_dir);
                            self.persist_roster_at_stop().await;
                            let _ = reply.send(());
                            break;
                        }
                        Some(command) => {
                            if self.handle_command(command).await.is_break() {
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
                    self.run_fast_maintenance().await;
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
        self.persist_roster_at_stop().await;
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
        self.mailbox.close();
        self.stopping_idle = true;
        // After `close`, `recv` yields the queued commands and then `None`.
        while let Some(command) = self.mailbox.recv().await {
            self.handle_command(command).await?;
        }
        ControlFlow::Continue(())
    }

    /// Handles one command. `Break` means the room can no longer operate safely and the
    /// actor must stop so that the next request reopens it from disk.
    async fn handle_command(&mut self, command: RoomCommand) -> ControlFlow<()> {
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
                let first = CommitRequest {
                    client_id,
                    mutation_id,
                    last_ack_seq,
                    op,
                    reply,
                };
                return self.commit_group(first).await;
            }

            RoomCommand::Sync {
                client_id,
                from_seq,
                max_batch_size,
                reply,
            } => {
                let _ = reply.send(self.sync(&client_id, from_seq, max_batch_size).await);
            }

            RoomCommand::Ack {
                client_id,
                ack_seq,
                reply,
            } => {
                let _ = reply.send(self.ack(&client_id, ack_seq).await);
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
                    tail_seq: self.log().tail_seq(),
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
        let tail_seq = self.log().tail_seq();
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

    /// Commits `first` together with the commits that wait behind it in the mailbox, as one
    /// group: up to the first command that is not a commit, and to [`GROUP_MAX_COMMITS`] commits
    /// or [`GROUP_MAX_BYTES`] of new operations. Each commit is checked and sequenced in order,
    /// as if it came alone, and the new mutations are written to the log and made durable with
    /// one `fsync`. Only then does the group become visible: the deduplication window and the
    /// head take it, it is announced once, and every commit is answered, in order.
    ///
    /// `Break`, with the mailbox already closed, means the room must stop.
    async fn commit_group(&mut self, first: CommitRequest) -> ControlFlow<()> {
        let mut group = Group::after(self.head_seq);
        let mut next = Some(first);
        while let Some(request) = next {
            self.sequence(request, &mut group);
            next = if group.is_full() {
                None
            } else {
                self.mailbox.next_commit()
            };
        }

        // A failed write or fsync leaves the log in an unknown state: any record of the group
        // may or may not be on disk, and a failed fsync cannot be retried safely. The room
        // stops and is recovered from disk by the next request; deduplication acknowledges a
        // retry of a mutation whose record survived with the sequence that reached disk.
        let records = std::mem::take(&mut group.records);
        let written = if records.is_empty() {
            Ok(AppendOutcome::default())
        } else {
            self.log_io(move |log| log.append_group(records)).await
        };
        let outcome = match written {
            Ok(outcome) => outcome,
            Err(err) => {
                error!(
                    room = %self.room_id,
                    error = %err,
                    "Log append failed; stopping room actor for recovery from disk"
                );
                // Closing the mailbox before replying guarantees that a caller retrying after
                // this reply already sees the room as stopped and gets it respawned.
                self.mailbox.close();
                for commit in group.commits {
                    let res = match commit.decision {
                        Decision::Rejected(err) => Err(err),
                        Decision::BehindLog => {
                            self.reject_behind_log(&commit.client_id, commit.last_ack_seq);
                            Err(ServerError::BehindCompaction)
                        }
                        Decision::Retry(_) | Decision::New(_) => {
                            Err(ServerError::Unavailable(format!(
                                "Room {} could not persist the commit and is restarting; \
                                 retry the request",
                                self.room_id
                            )))
                        }
                    };
                    let _ = commit.reply.send(res);
                }
                return ControlFlow::Break(());
            }
        };

        // From here on the new mutations are durable: their replies must acknowledge them, so
        // nothing below may turn them into an error.
        for (key, seq) in &group.keys {
            self.dedup_cache.record(*key, *seq);
        }
        if group.head > self.head_seq {
            self.head_seq = group.head;
            let _ = self.events_tx.send(self.announcement(&group.commits));
        }
        // A failed segment rotation after a durable append leaves the segment files in an
        // uncertain state. The commits are still acknowledged; then the room stops and is
        // recovered from disk by the next request. The mailbox is closed before the first reply,
        // as when the write fails, so that a request sent after it reaches the reopened room.
        let rotation_failed = outcome.rotation_error.is_some();
        if let Some(err) = outcome.rotation_error {
            error!(
                room = %self.room_id,
                error = %err,
                "Segment rotation failed after a durable commit; stopping room actor for recovery from disk"
            );
            self.mailbox.close();
        }
        for commit in group.commits {
            let res = self
                .answer(&commit.client_id, commit.last_ack_seq, commit.decision)
                .await;
            let _ = commit.reply.send(res);
        }
        if rotation_failed {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    /// The event that announces the new mutations of `commits`, a group whose records are
    /// durable and whose head is now the room's: `ops`, with their operations, if a stream
    /// listens and the server's total of retained events has room for them; otherwise
    /// `head_advanced`. The operations were checked with the room's schema, which a group
    /// never changes: a schema reload in the mailbox ends the group.
    fn announcement(&self, commits: &[PendingCommit]) -> RoomEvent {
        let new_ops = commits.iter().filter_map(|commit| match &commit.decision {
            Decision::New(seq_op) => Some(seq_op),
            _ => None,
        });
        let event = (self.events_tx.receiver_count() > 0)
            .then(|| {
                self.ops_events.event(
                    &self.room_id,
                    self.incarnation,
                    &self.schema_id,
                    self.schema.version(),
                    new_ops,
                )
            })
            .flatten();
        event.map_or(RoomEvent::HeadAdvanced(self.head_seq), RoomEvent::Ops)
    }

    /// Checks `request` against the room and the commits of `group` before it, and adds it to
    /// the group with what was decided: a rejection, a retry of a mutation already in the log
    /// or in the group, or a new mutation, sequenced after the group's head. Changes nothing
    /// outside the group.
    fn sequence(&self, request: CommitRequest, group: &mut Group) {
        let CommitRequest {
            client_id,
            mutation_id,
            last_ack_seq,
            op,
            reply,
        } = request;
        let decision = self
            .decide(&client_id, mutation_id, last_ack_seq, op, group)
            .unwrap_or_else(Decision::Rejected);
        group.commits.push(PendingCommit {
            client_id,
            last_ack_seq,
            reply,
            decision,
        });
    }

    /// What [`sequence`](Self::sequence) decides for one commit; an `Err` is a rejection.
    fn decide(
        &self,
        client_id: &ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: Operation,
        group: &mut Group,
    ) -> Result<Decision, ServerError> {
        self.ensure_registered(client_id)?;
        // Against the head that is visible: a commit never learns of a sequence before it is
        // durable, the group's included.
        self.check_within_head(last_ack_seq)?;
        let key = dedup_key(client_id, &mutation_id);

        // Exactly-once idempotency. Checked before any rejection based on client state:
        // a retried mutation was already committed and replicated, so it must always be
        // acknowledged with its original sequence, never reported as a failure. A retry of a
        // mutation earlier in the group gets that mutation's sequence.
        if let Some(existing_seq) = self.dedup_cache.is_duplicate(&key).or_else(|| {
            group
                .keys
                .iter()
                .find_map(|(k, seq)| (*k == key).then_some(*seq))
        }) {
            return Ok(Decision::Retry(existing_seq));
        }

        // Reject a cursor behind the compaction boundary before sequencing anything, so
        // that a rejected commit leaves no trace in the log, the head or the SSE stream. The
        // client's lifecycle state is not a reason to reject: a client that was bootstrapping
        // or dormant commits as soon as its cursor is inside the log.
        if self.log().is_behind_retention(last_ack_seq) {
            return Ok(Decision::BehindLog);
        }

        // The owner an operation declares must be the owner of the client sending it. Checked
        // against the validated operation, before anything is sequenced.
        self.schema
            .validate_operation(&op)
            .and_then(|()| self.schema.check_owner(&op, client_id))
            .map_err(|err| ServerError::SchemaViolation(err.to_string()))?;

        // The room never assigns u64::MAX, so that `head_seq + 1` always exists.
        if group.head.get() >= u64::MAX - 1 {
            return Err(ServerError::Internal(format!(
                "Room {} has no sequence numbers left",
                self.room_id
            )));
        }
        // The new sequence stays in the group until the group is durable.
        let seq_op = SequencedOperation::new(group.head.next(), op);

        // The operation must fit alone in a log record and in a response frame. A
        // mutation that does not is the client's error: it is rejected here, before any I/O,
        // and the room keeps running unchanged.
        check_operation_size(&seq_op, Some(key))
            .map_err(|err| ServerError::BadRequest(err.to_string()))?;

        group.head = seq_op.seq;
        group.bytes = group.bytes.saturating_add(OpSize::of(&seq_op).bytes);
        group.keys.push((key, seq_op.seq));
        group.records.push((seq_op.clone(), Some(key)));
        Ok(Decision::New(seq_op))
    }

    /// The reply to a commit of a group whose new mutations are durable, given what was decided
    /// for it. An accepted commit records the cursor it reports, and carries the catch-up from
    /// that cursor.
    async fn answer(
        &mut self,
        client_id: &ClientId,
        last_ack_seq: SequenceNumber,
        decision: Decision,
    ) -> Result<CommitResponse, ServerError> {
        let assigned_seq = match decision {
            Decision::Rejected(err) => return Err(err),
            Decision::BehindLog => {
                self.reject_behind_log(client_id, last_ack_seq);
                return Err(ServerError::BehindCompaction);
            }
            Decision::Retry(seq) => seq,
            Decision::New(ref seq_op) => seq_op.seq,
        };

        // Refresh the client's lease, record the cursor reported with the commit (already
        // validated against the head) and make the client Connected. Persisted on the next
        // maintenance tick.
        self.observe(client_id, Some(last_ack_seq));

        // Catch-up for 1-RTT synchronization. A client that was up to date gets the operation
        // it sent, without reading the log, if it is the head and the response has room for it.
        let mut reservation = self.reserve_response(client_id);
        let (catchup_ops, has_more) = match (decision, reservation.as_mut()) {
            (Decision::New(seq_op), Some(reservation))
                if last_ack_seq.get() + 1 == seq_op.seq.get() && seq_op.seq == self.head_seq =>
            {
                if reservation.grow(OpSize::of(&seq_op).in_response()) {
                    (vec![seq_op], false)
                } else {
                    (Vec::new(), true)
                }
            }
            _ => {
                self.catchup_after_commit(last_ack_seq, &mut reservation)
                    .await
            }
        };

        Ok(CommitResponse {
            assigned_seq,
            catchup_ops,
            has_more,
            snapshot_wanted: self.snapshot_wanted_for(client_id),
            schema_version: self.schema.version(),
            schema_id: self.schema_id.clone(),
            reservation,
        })
    }

    /// What a response to `client_id` takes besides its operations ([`RESPONSE_OVERHEAD`]),
    /// reserved from the client's part of the budget of response bytes in flight right before
    /// the response is read from the log, so that a command waiting in the mailbox holds none.
    /// Reading the operations grows it, and the response carries it to its encoding.
    fn reserve_response(&self, client_id: &ClientId) -> Option<Reservation> {
        let holder = (self.room_id.clone(), Uploader::Client(client_id.clone()));
        self.response_budget.reserve(holder, RESPONSE_OVERHEAD)
    }

    /// Builds the catch-up batch returned with an acknowledged commit, starting after the
    /// client's cursor, with the operations its response's `reservation` has room for (none
    /// without a reservation).
    ///
    /// The commit itself is already durable, so a failure here must not turn into an error
    /// reply. Without a catch-up, the batch is empty and flagged `has_more`, which sends the
    /// client to `/sync`, where any underlying error (for example `BehindCompaction`) is
    /// reported.
    async fn catchup_after_commit(
        &mut self,
        last_ack_seq: SequenceNumber,
        reservation: &mut Option<Reservation>,
    ) -> (Vec<SequencedOperation>, bool) {
        let Some(held) = reservation.take() else {
            return (Vec::new(), true);
        };
        let (read, held) = self
            .read_log(last_ack_seq, COMMIT_CATCHUP_BATCH, held)
            .await;
        *reservation = Some(held);
        match read {
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

    async fn sync(
        &mut self,
        client_id: &ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
    ) -> Result<SyncBatchResponse, ServerError> {
        self.ensure_registered(client_id)?;
        self.check_within_head(from_seq)?;

        if self.log().is_behind_retention(from_seq) {
            self.reject_behind_log(client_id, from_seq);
            return Err(ServerError::BehindCompaction);
        }

        let reservation = self.reserve_response(client_id).ok_or_else(no_room)?;
        let bounded_batch_size = max_batch_size.clamp(1, MAX_SYNC_BATCH);
        let (read, reservation) = self
            .read_log(from_seq, bounded_batch_size, reservation)
            .await;
        let (ops, has_more) = match read {
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
        // Not even the first operation fits in the budget of response bytes in flight: the
        // request is answered `Unavailable` without changing anything, and is retried.
        if ops.is_empty() && has_more {
            return Err(no_room());
        }
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

    async fn ack(
        &mut self,
        client_id: &ClientId,
        ack_seq: SequenceNumber,
    ) -> Result<SequenceNumber, ServerError> {
        let stored_cursor = self.ensure_registered(client_id)?;
        self.check_within_head(ack_seq)?;

        // The cursor never moves backwards, so an ack below the stored cursor (stale or
        // out of order) changes nothing and succeeds. Retention is checked against the
        // cursor the client ends up with.
        if self.log().is_behind_retention(ack_seq.max(stored_cursor)) {
            self.reject_behind_log(client_id, ack_seq);
            return Err(ServerError::BehindCompaction);
        }

        // Advance the cursor, refresh the lease and make the client Connected.
        // The cursor is persisted on the next maintenance tick.
        self.observe(client_id, Some(ack_seq));

        self.prune_to_retention_floor().await;

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
        let state = self
            .lease_tracker
            .observe(client_id, reported_cursor, self.log().tail_seq());
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

    /// The retained range of the log, the highest cursor from which a sync found a gap, and the
    /// room's incarnation.
    fn log_bounds(&self) -> LogBounds {
        LogBounds {
            tail_seq: self.log().tail_seq(),
            head_seq: self.head_seq,
            unrestorable_through: self.unrestorable_through,
            room_incarnation: self.incarnation,
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
    async fn prune_to_retention_floor(&mut self) {
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
        if retention_floor <= self.log().tail_seq() {
            return;
        }

        // A prune that deletes no segment only trims RAM, without touching a file.
        let pruned = if self.log().has_prunable_below(retention_floor) {
            if let Err(err) = self.save_roster(false).await {
                warn!(
                    room = %self.room_id,
                    error = %err,
                    "Failed to persist clients roster; skipping proactive log pruning"
                );
                return;
            }
            self.log_io(move |log| log.prune_older_than(retention_floor))
                .await
        } else {
            self.log_mut().prune_older_than(retention_floor)
        };
        if let Err(err) = pruned {
            warn!(room = %self.room_id, error = %err, "Proactive log pruning failed");
        }
    }

    /// Writes pending cursor and lease changes to the roster file. A failure is logged and the
    /// changes stay pending for the next attempt; it only delays how far the log can be pruned.
    async fn persist_roster(&mut self) {
        if let Err(err) = self.save_roster(false).await {
            warn!(room = %self.room_id, error = %err, "Failed to persist clients roster");
        }
    }

    /// Like [`persist_roster`](Self::persist_roster), but also writes the roster when only the
    /// clients' activity changed, so that their inactivity keeps counting after the room
    /// reopens (for example after a shutdown for inactivity).
    async fn persist_roster_at_stop(&mut self) {
        if let Err(err) = self.save_roster(true).await {
            warn!(room = %self.room_id, error = %err, "Failed to persist clients roster");
        }
    }

    /// Writes the roster file if it changed (with `with_activity`, also if only the clients'
    /// activity did), on the file I/O threads, carrying the log like any other operation on
    /// the room's files.
    async fn save_roster(&mut self, with_activity: bool) -> Result<(), ServerError> {
        let Some((path, json)) = self.lease_tracker.unsaved(with_activity)? else {
            return Ok(());
        };
        self.log_io(move |_| durable::write_checked(&path, &json))
            .await?;
        self.lease_tracker.saved();
        Ok(())
    }

    /// The log, between its operations on the file I/O threads.
    fn log(&self) -> &TieredLog {
        self.tiered_log.as_ref().expect(LOG_IN_PLACE)
    }

    /// The log, between its operations on the file I/O threads, for what touches no file.
    fn log_mut(&mut self) -> &mut TieredLog {
        self.tiered_log.as_mut().expect(LOG_IN_PLACE)
    }

    /// Runs `work`, which reads or writes the room's files, on the file I/O threads of the
    /// rooms, and waits for it without blocking the worker.
    async fn log_io<T: Send + 'static>(
        &mut self,
        work: impl FnOnce(&mut TieredLog) -> T + Send + 'static,
    ) -> T {
        let permit = self.io.io().permit().await;
        self.log_io_holding(permit, work).await
    }

    /// Runs `work` on a file I/O thread, holding `permit`, a permit of the pool it counts
    /// against, and waits for it without blocking the worker. The log travels to that thread and
    /// back, so nothing else touches it meanwhile. If the actor is dropped while it waits, the
    /// log is dropped on that thread once `work` ends, and only then are the room's files
    /// released.
    async fn log_io_holding<T: Send + 'static>(
        &mut self,
        permit: tokio::sync::OwnedSemaphorePermit,
        work: impl FnOnce(&mut TieredLog) -> T + Send + 'static,
    ) -> T {
        let mut log = self.tiered_log.take().expect(LOG_IN_PLACE);
        let (log, out) = run_holding(permit, move || {
            let out = work(&mut log);
            (log, out)
        })
        .await;
        self.tiered_log = Some(log);
        out
    }

    /// The operations after `from_seq` for a response holding `reservation`, as
    /// [`TieredLog::fetch_deltas`] returns them: from memory when the read needs no file, and
    /// otherwise on the file I/O threads, with the reservation.
    async fn read_log(
        &mut self,
        from_seq: SequenceNumber,
        max_batch_size: u32,
        mut reservation: Reservation,
    ) -> (
        Result<(Vec<SequencedOperation>, bool), ServerError>,
        Reservation,
    ) {
        if let Some(read) =
            self.log()
                .fetch_from_memory(from_seq, max_batch_size, Some(&mut reservation))
        {
            return (read, reservation);
        }
        self.log_io(move |log| {
            let read = log.fetch_deltas(from_seq, max_batch_size, Some(&mut reservation));
            (read, reservation)
        })
        .await
    }

    /// The fast tick: memory-only maintenance, plus writing the roster if it changed.
    async fn run_fast_maintenance(&mut self) {
        // Connected/Bootstrapping -> Disconnected -> Dormant.
        let tail_seq = self.log().tail_seq();
        self.lease_tracker.check_timeouts(
            self.policy.lease_timeout,
            self.policy.dormant_after,
            self.policy.forget_dormant_after,
            tail_seq,
        );

        self.update_snapshot_signals();
        // Cursor and lease changes accumulated since the previous tick.
        self.persist_roster().await;
    }

    /// The slow tick: disk maintenance of the log, served from its segment index.
    async fn run_disk_maintenance(&mut self) {
        // Compression, RAM TTL window, cold TTL and quota. A pass that compresses runs on a
        // compression thread, so that it never keeps the other rooms' file I/O waiting; with
        // none free (other rooms compressing), it leaves the warm segments for the next pass
        // rather than stop serving the room to wait for one.
        let compression = if self.log().has_compression_due(SystemTime::now()) {
            self.io.compression().try_permit()
        } else {
            None
        };
        let compress = compression.is_some();
        let permit = match compression {
            Some(permit) => permit,
            None => self.io.io().permit().await,
        };
        if let Err(e) = self
            .log_io_holding(permit, move |log| {
                log.run_maintenance_pass(MaintenanceClock::now(), compress)
            })
            .await
        {
            warn!(room = %self.room_id, error = %e, "TieredLog maintenance error");
        }

        // Backup pass of the cursor-driven pruning, which normally runs on every Ack: a floor
        // can also move without an Ack (cursors reported by commits, clients becoming Dormant).
        self.prune_to_retention_floor().await;
        self.publish_snapshot_floor();
    }
}

/// The room's mailbox. A group of commits ends at the first command that is not a commit,
/// which the mailbox then holds back for the next [`recv`](Self::recv), so that commands are
/// still handled in the order they came.
struct Mailbox {
    receiver: mpsc::Receiver<RoomCommand>,
    held: Option<RoomCommand>,
}

impl Mailbox {
    /// The next command, as [`mpsc::Receiver::recv`] returns it. Cancel safe: a command is
    /// only taken by a call that returns it.
    async fn recv(&mut self) -> Option<RoomCommand> {
        if let Some(command) = self.held.take() {
            return Some(command);
        }
        self.receiver.recv().await
    }

    /// The next command, if it is a commit already in the mailbox. Any other command is held
    /// back for the next [`recv`](Self::recv), and ends the group: no command is taken past it.
    fn next_commit(&mut self) -> Option<CommitRequest> {
        debug_assert!(self.held.is_none(), "a group goes on past a held command");
        match self.receiver.try_recv().ok()? {
            RoomCommand::Commit {
                client_id,
                mutation_id,
                last_ack_seq,
                op,
                reply,
            } => Some(CommitRequest {
                client_id,
                mutation_id,
                last_ack_seq,
                op,
                reply,
            }),
            other => {
                self.held = Some(other);
                None
            }
        }
    }

    /// Closes the mailbox: senders fail, and [`recv`](Self::recv) yields the commands already
    /// in it, then `None`.
    fn close(&mut self) {
        self.receiver.close();
    }
}

/// A commit taken from the mailbox.
struct CommitRequest {
    client_id: ClientId,
    mutation_id: MutationId,
    last_ack_seq: SequenceNumber,
    op: Operation,
    reply: oneshot::Sender<Result<CommitResponse, ServerError>>,
}

/// A commit of a group, waiting for the group to be written to be answered.
struct PendingCommit {
    client_id: ClientId,
    last_ack_seq: SequenceNumber,
    reply: oneshot::Sender<Result<CommitResponse, ServerError>>,
    decision: Decision,
}

/// What a group decided for one of its commits before writing anything.
enum Decision {
    /// Rejected, with the error it is answered: it leaves no trace.
    Rejected(ServerError),
    /// Rejected with `BehindCompaction`, because the cursor it carries is behind the log; the
    /// client's state is recomputed when it is answered.
    BehindLog,
    /// A retry of a mutation in the log or earlier in the group, with that mutation's
    /// sequence.
    Retry(SequenceNumber),
    /// A new mutation, with the sequence it was given.
    New(SequencedOperation),
}

/// The commits of a group as they are sequenced, before their `fsync`.
struct Group {
    commits: Vec<PendingCommit>,
    /// The records of the new mutations, in order, to be written to the log.
    records: Vec<(SequencedOperation, Option<MutationId>)>,
    /// The deduplication keys of the new mutations, in order, with their sequences.
    keys: Vec<(MutationId, SequenceNumber)>,
    /// The head once the new mutations are durable.
    head: SequenceNumber,
    /// The bytes of the new operations, as the RAM window counts them.
    bytes: u64,
}

impl Group {
    /// An empty group, whose first new mutation follows `head_seq`.
    fn after(head_seq: SequenceNumber) -> Self {
        Self {
            commits: Vec::new(),
            records: Vec::new(),
            keys: Vec::new(),
            head: head_seq,
            bytes: 0,
        }
    }

    /// Whether the group takes no more commits.
    fn is_full(&self) -> bool {
        self.commits.len() >= GROUP_MAX_COMMITS || self.bytes >= GROUP_MAX_BYTES
    }
}

/// Why the log is in place outside [`RoomActor::log_io`]: the actor waits for every operation
/// it sends, and a panic in one ends the actor.
const LOG_IN_PLACE: &str = "the log is back after every file operation";

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
