use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use rimdb_core::id::{MutationId, RoomId, SchemaId, SequenceNumber};
use rimdb_core::protocol::messages::SequencedOperation;
use rimdb_core::schema::Schema;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::actor::command::{
    CommitResponse, RegisterResponse, RoomCommand, RoomMetrics, SyncBatchResponse,
};
use crate::actor::lease::ClientLeaseTracker;
use crate::config::ServerConfig;
use crate::dedup::DedupLruCache;
use crate::error::ServerError;
use crate::log::{RoomLifecyclePolicy, TieredLog};
use crate::micro_wal::MicroWal;

/// Dedicated single-writer Tokio actor managing state, sequencing, durability, and synchronization for a single room.
pub struct RoomActor {
    room_id: RoomId,
    schema_id: SchemaId,
    schema: Arc<Schema>,
    micro_wal: MicroWal,
    dedup_cache: DedupLruCache,
    tiered_log: TieredLog,
    lease_tracker: ClientLeaseTracker,
    head_seq: SequenceNumber,
    events_tx: broadcast::Sender<SequenceNumber>,
    receiver: mpsc::Receiver<RoomCommand>,
    lease_timeout: Duration,
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
    ) -> Result<(mpsc::Sender<RoomCommand>, JoinHandle<()>), ServerError> {
        let room_dir = data_dir.as_ref().join("rooms").join(room_id.as_str());
        fs::create_dir_all(&room_dir)?;

        let wal_path = room_dir.join(format!("meta_{}.wal", room_id.as_str()));
        let clients_path = room_dir.join(format!("meta_clients_{}.json", room_id.as_str()));

        // 1. Recover Micro-WAL
        let (micro_wal, wal_recovery) = MicroWal::open_or_create(&wal_path)?;

        // 2. Hydrate deduplication LRU cache
        let mut dedup_cache = DedupLruCache::new(config.dedup_lru_capacity);
        dedup_cache.hydrate(wal_recovery.entries);

        // 3. Open or recover 4-tier delta log
        let tiered_log = TieredLog::open_or_create(&room_dir, lifecycle_policy.clone())?;

        // Reconcile monotonic head sequence
        let head_seq = std::cmp::max(wal_recovery.head_seq, tiered_log.head_seq());

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
            micro_wal,
            dedup_cache,
            tiered_log,
            lease_tracker,
            head_seq,
            events_tx,
            receiver: command_rx,
            lease_timeout,
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
        let mut maintenance_timer = tokio::time::interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                cmd = self.receiver.recv() => {
                    match cmd {
                        Some(command) => self.handle_command(command),
                        None => {
                            debug!(room = %self.room_id, "RoomCommand channel closed, shutting down actor loop");
                            break;
                        }
                    }
                }
                _ = maintenance_timer.tick() => {
                    self.run_periodic_maintenance();
                }
            }
        }
    }

    fn handle_command(&mut self, command: RoomCommand) {
        match command {
            RoomCommand::RegisterClient { client_id, reply } => {
                let res = self.lease_tracker.register_client(&client_id, self.head_seq).map(|_| {
                    RegisterResponse {
                        head_seq: self.head_seq,
                        schema_id: self.schema_id.clone(),
                        schema: Arc::clone(&self.schema),
                    }
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
                let _ = reply.send(Ok(()));
            }

            RoomCommand::Commit {
                client_id,
                mutation_id,
                last_ack_seq,
                op,
                reply,
            } => {
                self.handle_commit(client_id, mutation_id, last_ack_seq, op, reply);
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
                if self.lease_tracker.is_dormant(&client_id) {
                    let _ = reply.send(Err(ServerError::BehindCompaction));
                    return;
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
                let (conn, disc, dorm, total) = self.lease_tracker.client_counts();
                let metrics = RoomMetrics {
                    room_id: self.room_id.clone(),
                    schema_id: self.schema_id.clone(),
                    head_seq: self.head_seq,
                    tail_seq: self.tiered_log.tail_seq(),
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
        }
    }

    fn handle_commit(
        &mut self,
        client_id: rimdb_core::id::ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: rimdb_core::mutation::Operation,
        reply: tokio::sync::oneshot::Sender<Result<CommitResponse, ServerError>>,
    ) {
        // 1. Check if client is Dormant (behind compaction boundary)
        if self.lease_tracker.is_dormant(&client_id) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 2. Exactly-Once Idempotency Check via DedupLruCache
        if let Some(existing_seq) = self.dedup_cache.is_duplicate(&mutation_id) {
            let from_seq = if last_ack_seq < existing_seq {
                SequenceNumber::new(last_ack_seq.get() + 1)
            } else {
                existing_seq
            };
            let (catchup_ops, has_more) = self
                .tiered_log
                .fetch_deltas(from_seq, 100)
                .unwrap_or_default();

            let _ = reply.send(Ok(CommitResponse {
                assigned_seq: existing_seq,
                catchup_ops,
                has_more,
            }));
            return;
        }

        // 3. Schema validation in O(C)
        if let Err(err) = self.schema.validate_operation(&op) {
            let _ = reply.send(Err(ServerError::SchemaViolation(err.to_string())));
            return;
        }

        // 4. Assign strictly monotonic sequence number
        let new_seq = self.head_seq.next();

        // 5. Durable synchronous Micro-WAL append (torn write and crash protection)
        if let Err(err) = self.micro_wal.append(new_seq, &mutation_id, &client_id) {
            error!(room = %self.room_id, error = %err, "MicroWal append failure");
            let _ = reply.send(Err(err));
            return;
        }

        // 6. Record in DedupLruCache
        self.dedup_cache.record(mutation_id, new_seq);

        // 7. Write-Through append to TieredLog (Hot Buffer RAM + synchronous active.wal disk sync)
        let seq_op = SequencedOperation::new(new_seq, op);
        if let Err(err) = self.tiered_log.append(seq_op.clone()) {
            error!(room = %self.room_id, error = %err, "TieredLog append failure");
            let _ = reply.send(Err(err));
            return;
        }

        // 8. Advance local head sequence
        self.head_seq = new_seq;

        // 9. Update client lease activity (cursor advances exclusively via explicit Ack)
        self.lease_tracker.record_activity(&client_id);

        // 10. Broadcast signal-only SSE event to active watchers
        let _ = self.events_tx.send(new_seq);

        // 11. Compute catch-up deltas for 1-RTT synchronization
        let (catchup_ops, has_more) = if last_ack_seq.get() < new_seq.get().saturating_sub(1) {
            let from_seq = SequenceNumber::new(last_ack_seq.get() + 1);
            self.tiered_log
                .fetch_deltas(from_seq, 100)
                .unwrap_or_else(|_| (vec![seq_op], false))
        } else {
            (vec![seq_op], false)
        };

        let _ = reply.send(Ok(CommitResponse {
            assigned_seq: new_seq,
            catchup_ops,
            has_more,
        }));
    }

    fn handle_sync(
        &mut self,
        client_id: rimdb_core::id::ClientId,
        from_seq: SequenceNumber,
        max_batch_size: u32,
        reply: tokio::sync::oneshot::Sender<Result<SyncBatchResponse, ServerError>>,
    ) {
        // 1. Check if client is Dormant
        if self.lease_tracker.is_dormant(&client_id) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 2. Check if from_seq is behind retained log tail
        let tail_seq = self.tiered_log.tail_seq();
        if tail_seq.get() > 0 && from_seq.get() < tail_seq.get().saturating_sub(1) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 3. Fetch continuous multi-tier delta batch
        match self
            .tiered_log
            .fetch_deltas(from_seq, max_batch_size)
        {
            Ok((ops, has_more)) => {
                // Record activity only - cursor is NOT advanced until explicit Ack!
                self.lease_tracker.record_activity(&client_id);

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
        client_id: rimdb_core::id::ClientId,
        ack_seq: SequenceNumber,
        reply: tokio::sync::oneshot::Sender<Result<SequenceNumber, ServerError>>,
    ) {
        // 1. Check if client is Dormant
        if self.lease_tracker.is_dormant(&client_id) {
            let _ = reply.send(Err(ServerError::BehindCompaction));
            return;
        }

        // 2. Record explicit Ack, advancing cursor and refreshing lease
        let res = self
            .lease_tracker
            .record_ack(&client_id, ack_seq)
            .map(|_| {
                // 3. Trigger proactive log pruning if all connected clients are past the sequence
                if let Some(min_ack) = self.lease_tracker.min_connected_ack_seq() {
                    let _ = self.tiered_log.prune_older_than(min_ack);
                }
                self.head_seq
            });

        let _ = reply.send(res);
    }

    fn run_periodic_maintenance(&mut self) {
        // 1. Check client timeouts (Connected -> Disconnected -> Dormant)
        self.lease_tracker
            .check_timeouts(self.lease_timeout, self.tiered_log.tail_seq());

        // 2. Run TieredLog TTL and size compaction
        if let Err(e) = self.tiered_log.run_maintenance() {
            warn!(room = %self.room_id, error = %e, "TieredLog maintenance error");
        }

        // 3. Trigger proactive cursor-driven pruning if all non-dormant clients are Connected
        if let Some(min_ack) = self.lease_tracker.min_connected_ack_seq() {
            let _ = self.tiered_log.prune_older_than(min_ack);
        }
    }
}
