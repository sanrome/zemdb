use super::*;
use crate::actor::lease::ClientEntry;
use crate::actor::manager::RoomManager;
use crate::fail_point;
use crate::log::RoomLifecycleOverrides;
use crate::schema_registry::SchemaRegistry;
use std::fs;
use std::path::PathBuf;
use tempfile::{tempdir, TempDir};
use tokio::sync::oneshot;
use zemdb_core::id::ClientId;
use zemdb_core::mutation::Operation;
use zemdb_core::schema::TableSchema;
use zemdb_core::value::{DataType, PrimaryKey, RowBuilder, Value};

const POLL_CEILING: Duration = Duration::from_secs(10);

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .build()
        .unwrap();
    Schema::from_tables(vec![table])
}

fn insert_op(id: i64) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", "task").build();
    test_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// An operation on a table the schema does not define, rejected by validation.
fn invalid_op() -> Operation {
    Operation::delete(999, PrimaryKey::single(Value::Int(1)), 1000)
}

fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

fn mutation(n: u8) -> MutationId {
    MutationId::new([n; 16])
}

struct Fixture {
    dir: TempDir,
    manager: RoomManager,
    relay: Arc<SnapshotRelay>,
    room_id: RoomId,
}

impl Fixture {
    async fn new(policy: RoomLifecycleOverrides) -> Self {
        Self::with_options(policy, |_| {}, None).await
    }

    /// A fixture whose room has the lifecycle overrides `policy`, whose server configuration
    /// is adjusted by `configure` and, if given, whose designated snapshot uploaders time out
    /// after `designation_timeout`.
    async fn with_options(
        policy: RoomLifecycleOverrides,
        configure: impl FnOnce(&mut ServerConfig),
        designation_timeout: Option<Duration>,
    ) -> Self {
        let dir = tempdir().unwrap();
        let (mut manager, relay) = new_manager_with(&dir, configure);
        if let Some(timeout) = designation_timeout {
            manager.set_designation_timeout(timeout);
        }
        let room_id = RoomId::new("room").unwrap();
        manager
            .create_room(
                room_id.clone(),
                SchemaId::new("todo").unwrap(),
                Some(policy),
            )
            .await
            .unwrap();
        Self {
            dir,
            manager,
            relay,
            room_id,
        }
    }

    fn room_dir(&self) -> PathBuf {
        self.dir.path().join("rooms").join(self.room_id.as_str())
    }

    fn roster_path(&self) -> PathBuf {
        self.room_dir()
            .join(format!("meta_clients_{}.json", self.room_id.as_str()))
    }

    fn persisted_cursor(&self, client_id: &ClientId) -> Option<SequenceNumber> {
        let roster: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(self.roster_path()).unwrap()).unwrap();
        let entries: Vec<ClientEntry> = serde_json::from_value(roster["clients"].clone()).unwrap();
        entries
            .into_iter()
            .find(|e| &e.client_id == client_id)
            .map(|e| e.last_ack_seq)
    }

    async fn sender(&self) -> mpsc::Sender<RoomCommand> {
        self.manager
            .get_or_spawn(&self.room_id, None)
            .await
            .unwrap()
    }
}

fn new_manager_with(
    dir: &TempDir,
    configure: impl FnOnce(&mut ServerConfig),
) -> (RoomManager, Arc<SnapshotRelay>) {
    let mut config = ServerConfig {
        data_dir: dir.path().to_path_buf(),
        // Rooms under test stay open unless a test enables the shutdown for inactivity.
        idle_timeout_secs: 0,
        ..ServerConfig::default()
    };
    configure(&mut config);
    let config = Arc::new(config);
    let registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    registry
        .register_schema(SchemaId::new("todo").unwrap(), test_schema())
        .unwrap();
    let relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
        )
        .unwrap(),
    );
    (
        RoomManager::new(config, registry, Arc::clone(&relay)),
        relay,
    )
}

async fn register(sender: &mpsc::Sender<RoomCommand>, client_id: &ClientId) {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq: None,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

async fn commit(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    mutation_id: MutationId,
    last_ack_seq: SequenceNumber,
    op: Operation,
) -> Result<CommitResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client_id.clone(),
            mutation_id,
            last_ack_seq,
            op,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

async fn cursor(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
) -> Option<SequenceNumber> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetClientCursor {
            client_id: client_id.clone(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap()
}

async fn metrics(sender: &mpsc::Sender<RoomCommand>) -> RoomMetrics {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetMetrics { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap().unwrap()
}

async fn sync_all(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
) -> Result<SyncBatchResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: client_id.clone(),
            from_seq: seq(0),
            max_batch_size: 100,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

#[tokio::test]
async fn accepted_commit_advances_client_cursor() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();

    assert_eq!(cursor(&sender, &client).await, Some(seq(1)));
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn retried_commit_advances_client_cursor() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &client, mutation(2), seq(0), insert_op(2))
        .await
        .unwrap();
    let retry = commit(&sender, &client, mutation(1), seq(2), insert_op(1))
        .await
        .unwrap();

    assert_eq!(retry.assigned_seq, seq(1));
    assert_eq!(cursor(&sender, &client).await, Some(seq(2)));
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn rejected_commit_does_not_advance_client_cursor() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    let invalid = commit(&sender, &client, mutation(2), seq(1), invalid_op()).await;
    assert!(matches!(invalid, Err(ServerError::SchemaViolation(_))));
    let ahead = commit(&sender, &client, mutation(3), seq(5), insert_op(3)).await;
    assert!(matches!(ahead, Err(ServerError::InvalidSequence { .. })));

    assert_eq!(cursor(&sender, &client).await, Some(seq(0)));
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn commit_only_client_advances_retention_floor() {
    let policy = RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        ..RoomLifecycleOverrides::default()
    };
    let fx = Fixture::new(policy).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    // Each commit reports the previous head as the client's cursor; no explicit Ack is sent.
    for n in 1..=6u8 {
        commit(
            &sender,
            &client,
            mutation(n),
            seq(u64::from(n) - 1),
            insert_op(n.into()),
        )
        .await
        .unwrap();
    }

    let deadline = tokio::time::Instant::now() + POLL_CEILING;
    loop {
        let tail = metrics(&sender).await.tail_seq;
        if tail > seq(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retention floor never advanced (tail_seq {tail})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn cursor_advance_is_persisted_by_maintenance_tick() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + POLL_CEILING;
    while fx.persisted_cursor(&client) != Some(seq(1)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cursor was never persisted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    fx.manager.shutdown_all().await;
}

// Paused time: the maintenance tick cannot fire on its own, so only Shutdown can flush.
#[tokio::test(start_paused = true)]
async fn shutdown_persists_pending_roster_changes() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();

    assert_eq!(
        fx.persisted_cursor(&client),
        Some(seq(0)),
        "the cursor must still be pending before Shutdown"
    );
    assert!(fx.manager.shutdown_room(&fx.room_id).await);

    assert_eq!(fx.persisted_cursor(&client), Some(seq(1)));
}

#[tokio::test]
async fn failed_commit_sync_stops_room_and_recovers_from_disk() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    // The record reaches the file, but its sync_data fails.
    fail_point::arm(
        "warm_append_sync",
        &fx.room_dir().join("segments").join("active.wal"),
    );
    let (commit_tx, commit_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client.clone(),
            mutation_id: mutation(2),
            last_ack_seq: seq(1),
            op: insert_op(2),
            reply: commit_tx,
        })
        .await
        .unwrap();
    let (sync_tx, sync_rx) = oneshot::channel();
    let queued = sender
        .send(RoomCommand::Sync {
            client_id: client.clone(),
            from_seq: seq(0),
            max_batch_size: 100,
            reply: sync_tx,
        })
        .await;

    let failed = commit_rx.await.unwrap();
    assert!(
        sender.is_closed(),
        "the room actor must stop after a failed sync"
    );
    // Depending on scheduling the command is either refused by the closed mailbox or
    // dropped unanswered; it must never be answered by the failed actor.
    if queued.is_ok() {
        assert!(
            sync_rx.await.is_err(),
            "a command queued behind the failure must be dropped, not answered"
        );
    }
    assert!(matches!(failed, Err(ServerError::Unavailable(_))));

    // The next request respawns the room from disk.
    let recovered = fx.sender().await;
    assert!(!recovered.same_channel(&sender));
    let retry = commit(&recovered, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();
    assert_eq!(retry.assigned_seq, seq(2));
    let next = commit(&recovered, &client, mutation(3), seq(2), insert_op(3))
        .await
        .unwrap();
    assert_eq!(next.assigned_seq, seq(3));

    let synced = sync_all(&recovered, &client).await.unwrap();
    let seqs: Vec<u64> = synced.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    fx.manager.shutdown_all().await;
}

async fn ack(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    ack_seq: SequenceNumber,
) -> Result<SequenceNumber, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::Ack {
            client_id: client_id.clone(),
            ack_seq,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

// Paused time: no maintenance tick runs, so only the ack path can persist the cursor.
#[tokio::test(start_paused = true)]
async fn pruning_on_ack_persists_cursor_before_deleting_segments() {
    let policy = RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        ..RoomLifecycleOverrides::default()
    };
    let fx = Fixture::new(policy).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for n in 1..=4u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }

    ack(&sender, &client, seq(4)).await.unwrap();

    let tail = metrics(&sender).await.tail_seq;
    assert!(tail > seq(1), "the ack must have pruned a segment");
    let persisted = fx.persisted_cursor(&client).unwrap();
    assert!(
        persisted.get() + 1 >= tail.get(),
        "persisted cursor {persisted} is behind the pruned tail {tail}"
    );
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn rotation_failure_after_durable_commit_is_acknowledged_and_restarts_room() {
    let policy = RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        ..RoomLifecycleOverrides::default()
    };
    let fx = Fixture::new(policy).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    // The second record is durable; sealing the segment afterwards fails.
    fail_point::arm("sync_dir", &fx.room_dir().join("segments"));
    let acked = commit(&sender, &client, mutation(2), seq(1), insert_op(2))
        .await
        .expect("a durable commit must be acknowledged");
    assert_eq!(acked.assigned_seq, seq(2));
    assert!(
        sender.is_closed(),
        "the room must restart after a failed rotation"
    );

    let recovered = fx.sender().await;
    let retry = commit(&recovered, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();
    assert_eq!(retry.assigned_seq, seq(2));
    let next = commit(&recovered, &client, mutation(3), seq(2), insert_op(3))
        .await
        .unwrap();
    assert_eq!(next.assigned_seq, seq(3));
    let synced = sync_all(&recovered, &client).await.unwrap();
    let seqs: Vec<u64> = synced.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn failed_commit_write_restarts_room_and_retry_gets_next_sequence() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    // The write fails before anything reaches the file.
    fail_point::arm(
        "warm_append_write",
        &fx.room_dir().join("segments").join("active.wal"),
    );
    let failed = commit(&sender, &client, mutation(2), seq(1), insert_op(2)).await;
    assert!(matches!(failed, Err(ServerError::Unavailable(_))));
    assert!(sender.is_closed());

    let recovered = fx.sender().await;
    let retry = commit(&recovered, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();
    assert_eq!(retry.assigned_seq, seq(2));
    let synced = sync_all(&recovered, &client).await.unwrap();
    let seqs: Vec<u64> = synced.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2]);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn log_bounds_report_the_retained_range() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for i in 1..=3u8 {
        commit(
            &sender,
            &client,
            mutation(i),
            seq(u64::from(i) - 1),
            insert_op(i64::from(i)),
        )
        .await
        .unwrap();
    }

    let (tail_seq, head_seq) = fx.manager.log_bounds(&fx.room_id).await.unwrap();
    assert_eq!(head_seq, seq(3));
    assert_eq!(tail_seq, metrics(&sender).await.tail_seq);

    let missing = RoomId::new("missing").unwrap();
    assert!(matches!(
        fx.manager.log_bounds(&missing).await,
        Err(ServerError::RoomNotFound(_))
    ));
    fx.manager.shutdown_all().await;
}

/// A valid raw snapshot envelope.
fn snapshot_envelope() -> bytes::Bytes {
    use zemdb_core::protocol::snapshot_envelope::{SnapshotCompression, SnapshotEnvelopeHeader};
    let body = b"snapshot";
    let header = SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, 8, body);
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(body);
    bytes::Bytes::from(out)
}

// Paused time: no maintenance tick runs, so only the ack path prunes.
#[tokio::test(start_paused = true)]
async fn snapshot_below_the_retained_range_does_not_anchor_pruning() {
    let policy = RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        ..RoomLifecycleOverrides::default()
    };
    let fx = Fixture::new(policy).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for n in 1..=8u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(&sender, &client, seq(8)).await.unwrap();
    let tail_before = metrics(&sender).await.tail_seq;
    assert!(
        tail_before >= seq(3),
        "the ack must have pruned (tail {tail_before})"
    );

    // A snapshot at seq 1 is below tail - 1: the relay would no longer accept it, and it must
    // not hold back pruning. (Staged with bounds that admit it, to simulate a snapshot that
    // fell out of the range after it was accepted.)
    fx.relay
        .stage_snapshot(
            &fx.room_id,
            seq(1),
            snapshot_envelope(),
            std::future::ready(Ok(crate::relay::LogBounds {
                tail_seq: seq(0),
                head_seq: seq(100),
            })),
        )
        .await
        .unwrap();

    for n in 9..=12u8 {
        commit(&sender, &client, mutation(n), seq(8), insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(&sender, &client, seq(12)).await.unwrap();
    let tail_after = metrics(&sender).await.tail_seq;
    assert!(
        tail_after > tail_before,
        "a stale snapshot held back pruning (tail {tail_before} -> {tail_after})"
    );
    fx.manager.shutdown_all().await;
}

async fn heartbeat(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
) -> Result<HeartbeatResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::Heartbeat {
            client_id: client_id.clone(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

async fn sync_from(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    from_seq: SequenceNumber,
) -> Result<SyncBatchResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: client_id.clone(),
            from_seq,
            max_batch_size: 100,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// Lifecycle state counts as `(bootstrapping, connected, disconnected, dormant)`.
async fn states(sender: &mpsc::Sender<RoomCommand>) -> (usize, usize, usize, usize) {
    let m = metrics(sender).await;
    (
        m.bootstrapping_clients,
        m.connected_clients,
        m.disconnected_clients,
        m.dormant_clients,
    )
}

impl Fixture {
    /// Stops the room, replaces its client roster with `entries` (client, state, cursor),
    /// written in the array format that predates the persisted snapshot demand, and reopens
    /// the room from disk.
    async fn restart_with_roster(
        &self,
        entries: &[(&ClientId, &str, u64)],
    ) -> mpsc::Sender<RoomCommand> {
        self.manager.shutdown_room(&self.room_id).await;
        let roster = roster_entries(entries);
        fs::write(self.roster_path(), serde_json::to_string(&roster).unwrap()).unwrap();
        self.sender().await
    }

    /// Like [`restart_with_roster`](Self::restart_with_roster), with a persisted snapshot
    /// demand last renewed at `renewed_at`.
    async fn restart_with_demand(
        &self,
        entries: &[(&ClientId, &str, u64)],
        renewed_at: std::time::SystemTime,
    ) -> mpsc::Sender<RoomCommand> {
        self.manager.shutdown_room(&self.room_id).await;
        let renewed_at_ms = renewed_at
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let roster = serde_json::json!({
            "clients": roster_entries(entries),
            "snapshot_demand": { "renewed_at_unix_ms": renewed_at_ms },
        });
        fs::write(self.roster_path(), serde_json::to_string(&roster).unwrap()).unwrap();
        self.sender().await
    }
}

fn roster_entries(entries: &[(&ClientId, &str, u64)]) -> Vec<serde_json::Value> {
    entries
        .iter()
        .map(|(client_id, state, cursor)| {
            serde_json::json!({
                "client_id": client_id.as_str(),
                "state": state,
                "last_ack_seq": cursor,
            })
        })
        .collect()
}

/// Two operations per segment, so that a few commits leave segments to prune.
fn small_segments() -> RoomLifecycleOverrides {
    RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        ..RoomLifecycleOverrides::default()
    }
}

/// A room whose log was pruned by the cursor of its only client, `writer`, which committed
/// 1..=8 and acknowledged 8. Returns the fixture and the retained tail.
async fn pruned_room() -> (Fixture, ClientId, SequenceNumber) {
    let fx = Fixture::new(small_segments()).await;
    let (writer, tail) = prune_with_writer(&fx).await;
    (fx, writer, tail)
}

/// Registers `writer`, which commits 1..=8 and acknowledges 8, pruning the log of a room with
/// [`small_segments`]. Returns the writer and the retained tail.
async fn prune_with_writer(fx: &Fixture) -> (ClientId, SequenceNumber) {
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    register(&sender, &writer).await;
    for n in 1..=8u8 {
        commit(&sender, &writer, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(&sender, &writer, seq(8)).await.unwrap();
    let tail = metrics(&sender).await.tail_seq;
    assert!(tail >= seq(5), "the ack must have pruned (tail {tail})");
    (writer, tail)
}

// Paused time: no maintenance tick changes the lifecycle states under test.
#[tokio::test(start_paused = true)]
async fn dormant_client_with_a_valid_cursor_syncs_acks_commits_and_becomes_connected() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    register(&sender, &writer).await;
    for n in 1..=3u8 {
        commit(&sender, &writer, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    let sleeper = ClientId::new("sleeper").unwrap();

    // Sync.
    let sender = fx
        .restart_with_roster(&[(&writer, "Connected", 3), (&sleeper, "Dormant", 1)])
        .await;
    let batch = sync_from(&sender, &sleeper, seq(1)).await.unwrap();
    assert_eq!(batch.ops.len(), 2);
    assert_eq!(states(&sender).await, (0, 2, 0, 0));

    // Ack.
    let sender = fx
        .restart_with_roster(&[(&writer, "Connected", 3), (&sleeper, "Dormant", 1)])
        .await;
    ack(&sender, &sleeper, seq(3)).await.unwrap();
    assert_eq!(cursor(&sender, &sleeper).await, Some(seq(3)));
    assert_eq!(states(&sender).await, (0, 2, 0, 0));

    // Commit.
    let sender = fx
        .restart_with_roster(&[(&writer, "Connected", 3), (&sleeper, "Dormant", 1)])
        .await;
    let acked = commit(&sender, &sleeper, mutation(9), seq(2), insert_op(9))
        .await
        .unwrap();
    assert_eq!(acked.assigned_seq, seq(4));
    assert_eq!(cursor(&sender, &sleeper).await, Some(seq(2)));
    assert_eq!(states(&sender).await, (0, 2, 0, 0));
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_never_fails_for_retention() {
    let (fx, writer, _tail) = pruned_room().await;
    let sleeper = ClientId::new("sleeper").unwrap();

    // A Dormant client whose cursor is still inside the log becomes Connected.
    let sender = fx
        .restart_with_roster(&[(&writer, "Connected", 8), (&sleeper, "Dormant", 8)])
        .await;
    heartbeat(&sender, &sleeper)
        .await
        .expect("a heartbeat of a Dormant client inside the log must succeed");
    assert_eq!(states(&sender).await, (0, 2, 0, 0));

    // A Dormant client whose cursor fell behind the log becomes Bootstrapping.
    let sender = fx
        .restart_with_roster(&[(&writer, "Connected", 8), (&sleeper, "Dormant", 1)])
        .await;
    heartbeat(&sender, &sleeper)
        .await
        .expect("a heartbeat of a client behind the log must succeed");
    assert_eq!(states(&sender).await, (1, 1, 0, 0));
    assert_eq!(cursor(&sender, &sleeper).await, Some(seq(1)));
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn operations_with_a_cursor_behind_the_log_are_rejected_and_make_the_client_bootstrap() {
    let (fx, writer, tail) = pruned_room().await;
    let behind = seq(tail.get() - 2);
    let client = ClientId::new("client").unwrap();

    for state in ["Connected", "Disconnected", "Dormant"] {
        // Sync.
        let sender = fx
            .restart_with_roster(&[(&writer, "Connected", 8), (&client, state, behind.get())])
            .await;
        let res = sync_from(&sender, &client, behind).await;
        assert!(
            matches!(res, Err(ServerError::BehindCompaction)),
            "{state}: {res:?}"
        );
        assert_eq!(states(&sender).await, (1, 1, 0, 0), "{state}");

        // Ack.
        let sender = fx
            .restart_with_roster(&[(&writer, "Connected", 8), (&client, state, behind.get())])
            .await;
        let res = ack(&sender, &client, behind).await;
        assert!(
            matches!(res, Err(ServerError::BehindCompaction)),
            "{state}: {res:?}"
        );
        assert_eq!(states(&sender).await, (1, 1, 0, 0), "{state}");

        // Commit: rejected before sequencing.
        let sender = fx
            .restart_with_roster(&[(&writer, "Connected", 8), (&client, state, behind.get())])
            .await;
        let res = commit(&sender, &client, mutation(20), behind, insert_op(20)).await;
        assert!(
            matches!(res, Err(ServerError::BehindCompaction)),
            "{state}: {res:?}"
        );
        assert_eq!(states(&sender).await, (1, 1, 0, 0), "{state}");
        assert_eq!(metrics(&sender).await.head_seq, seq(8));
        assert_eq!(cursor(&sender, &client).await, Some(behind));
    }
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn bootstrapping_client_with_a_valid_cursor_can_commit() {
    let (fx, writer, _tail) = pruned_room().await;
    let newcomer = ClientId::new("newcomer").unwrap();
    // Registered while behind the log, then restored a snapshot at seq 8.
    let sender = fx
        .restart_with_roster(&[(&writer, "Connected", 8), (&newcomer, "Bootstrapping", 0)])
        .await;

    let acked = commit(&sender, &newcomer, mutation(30), seq(8), insert_op(30))
        .await
        .expect("a Bootstrapping client with a valid cursor must be able to commit");
    assert_eq!(acked.assigned_seq, seq(9));
    assert_eq!(cursor(&sender, &newcomer).await, Some(seq(8)));
    assert_eq!(states(&sender).await, (0, 2, 0, 0));
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn ack_below_the_stored_cursor_is_a_successful_no_op() {
    let (fx, writer, tail) = pruned_room().await;
    let sender = fx.sender().await;

    // An out-of-order ack, even one behind the log, does not move the cursor back.
    let stale = seq(tail.get() - 2);
    assert_eq!(ack(&sender, &writer, stale).await.unwrap(), seq(8));
    assert_eq!(cursor(&sender, &writer).await, Some(seq(8)));
    assert_eq!(states(&sender).await, (0, 1, 0, 0));
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn sync_from_beyond_the_head_is_an_invalid_sequence() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("client").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    let res = sync_from(&sender, &client, seq(5)).await;
    assert!(
        matches!(res, Err(ServerError::InvalidSequence { .. })),
        "{res:?}"
    );
    assert_eq!(cursor(&sender, &client).await, Some(seq(0)));
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn sync_advances_the_cursor_of_any_client() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("client").unwrap();
    register(&sender, &client).await;
    for n in 1..=3u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }

    sync_from(&sender, &client, seq(2)).await.unwrap();
    assert_eq!(cursor(&sender, &client).await, Some(seq(2)));
    fx.manager.shutdown_all().await;
}

async fn register_at(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    current_seq: Option<SequenceNumber>,
) {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

async fn deregister(sender: &mpsc::Sender<RoomCommand>, client_id: &ClientId) {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::DeregisterClient {
            client_id: client_id.clone(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

async fn subscribe(sender: &mpsc::Sender<RoomCommand>) -> broadcast::Receiver<RoomEvent> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::SubscribeEvents { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap().unwrap()
}

fn is_snapshot_event(event: &RoomEvent) -> bool {
    matches!(
        event,
        RoomEvent::SnapshotWanted | RoomEvent::SnapshotAvailable(_)
    )
}

/// Waits for the next snapshot signal, skipping other events.
async fn next_snapshot_event(events: &mut broadcast::Receiver<RoomEvent>) -> RoomEvent {
    tokio::time::timeout(POLL_CEILING, async {
        loop {
            let event = events.recv().await.unwrap();
            if is_snapshot_event(&event) {
                return event;
            }
        }
    })
    .await
    .expect("no snapshot event was broadcast")
}

/// Snapshot signals broadcast so far and not yet received.
fn pending_snapshot_events(events: &mut broadcast::Receiver<RoomEvent>) -> Vec<RoomEvent> {
    let mut pending = Vec::new();
    while let Ok(event) = events.try_recv() {
        if is_snapshot_event(&event) {
            pending.push(event);
        }
    }
    pending
}

async fn snapshot_wanted(sender: &mpsc::Sender<RoomCommand>, client_id: &ClientId) -> bool {
    heartbeat(sender, client_id).await.unwrap().snapshot_wanted
}

/// Stages a snapshot at `n` in the relay, accepted against the room's current log range.
async fn stage_snapshot_at(fx: &Fixture, n: u64) {
    fx.relay
        .stage_snapshot(&fx.room_id, seq(n), snapshot_envelope(), async {
            let (tail_seq, head_seq) = fx.manager.log_bounds(&fx.room_id).await?;
            Ok(crate::relay::LogBounds { tail_seq, head_seq })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn client_registering_behind_the_log_gets_one_designated_uploader() {
    let (fx, writer, tail) = pruned_room().await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some(tail)).await;
    let mut events = subscribe(&sender).await;

    let newcomer = ClientId::new("newcomer").unwrap();
    register_at(&sender, &newcomer, None).await;
    assert_eq!(states(&sender).await, (1, 2, 0, 0));
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );

    // Only the Connected client with the highest cursor is asked, in every kind of reply.
    let hb = heartbeat(&sender, &writer).await.unwrap();
    assert!(hb.snapshot_wanted);
    assert_eq!(hb.active_snapshot_seq, None);
    assert!(!snapshot_wanted(&sender, &peer).await);
    assert!(!snapshot_wanted(&sender, &newcomer).await);
    let committed = commit(&sender, &writer, mutation(40), seq(8), insert_op(40))
        .await
        .unwrap();
    assert!(committed.snapshot_wanted);
    let committed = commit(&sender, &peer, mutation(41), tail, insert_op(41))
        .await
        .unwrap();
    assert!(!committed.snapshot_wanted);
    assert!(
        sync_from(&sender, &writer, seq(8))
            .await
            .unwrap()
            .snapshot_wanted
    );
    assert!(
        !sync_from(&sender, &peer, tail)
            .await
            .unwrap()
            .snapshot_wanted
    );

    // Further requests renew the demand without designating anyone else.
    assert!(!snapshot_wanted(&sender, &newcomer).await);
    assert!(pending_snapshot_events(&mut events).is_empty());
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn usable_snapshot_ends_the_demand_and_is_announced() {
    let (fx, writer, _tail) = pruned_room().await;
    let sender = fx.sender().await;
    let mut events = subscribe(&sender).await;
    let newcomer = ClientId::new("newcomer").unwrap();
    register_at(&sender, &newcomer, None).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);

    stage_snapshot_at(&fx, 8).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotAvailable(seq(8))
    );
    let hb = heartbeat(&sender, &writer).await.unwrap();
    assert!(!hb.snapshot_wanted);
    assert_eq!(hb.active_snapshot_seq, Some(seq(8)));

    // With a usable snapshot, a client behind the log does not turn the demand on again.
    let hb = heartbeat(&sender, &newcomer).await.unwrap();
    assert!(!hb.snapshot_wanted);
    assert_eq!(hb.active_snapshot_seq, Some(seq(8)));
    let other = ClientId::new("other").unwrap();
    register_at(&sender, &other, None).await;
    assert!(!snapshot_wanted(&sender, &writer).await);
    assert!(pending_snapshot_events(&mut events).is_empty());
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn behind_compaction_answers_request_a_snapshot() {
    let (fx, writer, tail) = pruned_room().await;
    let behind = seq(tail.get() - 2);
    let client = ClientId::new("client").unwrap();

    // A rejected sync.
    let sender = fx
        .restart_with_roster(&[
            (&writer, "Connected", 8),
            (&client, "Connected", behind.get()),
        ])
        .await;
    let mut events = subscribe(&sender).await;
    assert!(!snapshot_wanted(&sender, &writer).await);
    let res = sync_from(&sender, &client, behind).await;
    assert!(matches!(res, Err(ServerError::BehindCompaction)), "{res:?}");
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);

    // The heartbeat of a client whose stored cursor fell behind (the demand lives in memory,
    // so a restarted room starts without one).
    let sender = fx
        .restart_with_roster(&[
            (&writer, "Connected", 8),
            (&client, "Dormant", behind.get()),
        ])
        .await;
    let mut events = subscribe(&sender).await;
    assert!(!snapshot_wanted(&sender, &writer).await);
    heartbeat(&sender, &client).await.unwrap();
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn designee_that_leaves_is_replaced() {
    let (fx, writer, tail) = pruned_room().await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some(tail)).await;
    let mut events = subscribe(&sender).await;
    register_at(&sender, &ClientId::new("newcomer").unwrap(), None).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);

    deregister(&sender, &writer).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &peer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn demand_without_candidates_waits_for_a_connected_client() {
    let (fx, writer, _tail) = pruned_room().await;
    let sender = fx
        .restart_with_roster(&[(&writer, "Disconnected", 8)])
        .await;
    let mut events = subscribe(&sender).await;
    register_at(&sender, &ClientId::new("newcomer").unwrap(), None).await;
    // Let a few maintenance ticks pass: nobody can be designated.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(pending_snapshot_events(&mut events).is_empty());

    // The writer comes back: the next tick designates it.
    heartbeat(&sender, &writer).await.unwrap();
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

/// Designation timeout for tests that exercise it. Between a designation and the start of
/// the upload the tests make a few round trips and a disk write, which must all fit in it.
const DESIGNATION_TIMEOUT_IN_TESTS: Duration = Duration::from_secs(1);

/// A valid snapshot envelope of 200 KiB, uploaded in three chunks.
fn three_chunk_snapshot() -> (Vec<u8>, [u8; 32], usize) {
    use zemdb_core::protocol::messages::ServerMessage;
    use zemdb_core::protocol::snapshot_envelope::{
        SnapshotCompression, SnapshotEnvelopeHeader, SNAPSHOT_HEADER_LEN,
    };
    let body = vec![7u8; 200 * 1024 - SNAPSHOT_HEADER_LEN];
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
    let mut data = header.to_bytes().to_vec();
    data.extend_from_slice(&body);
    let hash = ServerMessage::compute_snapshot_hash(&data);
    let chunk_len = data.len().div_ceil(3);
    (data, hash, chunk_len)
}

async fn upload_chunk(fx: &Fixture, n: u64, index: u32) -> bool {
    let (data, hash, chunk_len) = three_chunk_snapshot();
    let start = index as usize * chunk_len;
    let end = (start + chunk_len).min(data.len());
    fx.relay
        .stage_chunk(
            crate::relay::SnapshotChunkUpload {
                room_id: fx.room_id.clone(),
                uploader: crate::relay::Uploader::Client(ClientId::new("writer").unwrap()),
                head_seq: seq(n),
                chunk_index: index,
                total_chunks: 3,
                total_bytes: data.len() as u64,
                snapshot_hash: hash,
                data: bytes::Bytes::copy_from_slice(&data[start..end]),
            },
            async {
                let (tail_seq, head_seq) = fx.manager.log_bounds(&fx.room_id).await?;
                Ok(crate::relay::LogBounds { tail_seq, head_seq })
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn designee_that_does_not_upload_in_time_is_replaced_but_never_during_an_upload() {
    let fx =
        Fixture::with_options(small_segments(), |_| {}, Some(DESIGNATION_TIMEOUT_IN_TESTS)).await;
    let (writer, tail) = prune_with_writer(&fx).await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some(tail)).await;
    let mut events = subscribe(&sender).await;
    register_at(&sender, &ClientId::new("newcomer").unwrap(), None).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);

    // The writer does not upload: the role moves to the peer.
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &peer).await);
    assert!(!snapshot_wanted(&sender, &writer).await);

    // An upload starts: however long it takes, the role stays put.
    assert!(!upload_chunk(&fx, 8, 0).await);
    let _ = pending_snapshot_events(&mut events);
    // Well past the designation timeout, with several maintenance ticks.
    tokio::time::sleep(DESIGNATION_TIMEOUT_IN_TESTS * 5 / 2).await;
    let during_upload = pending_snapshot_events(&mut events);
    assert!(during_upload.is_empty(), "{during_upload:?}");
    assert!(snapshot_wanted(&sender, &peer).await);

    // The upload completes: the demand ends.
    assert!(!upload_chunk(&fx, 8, 1).await);
    assert!(upload_chunk(&fx, 8, 2).await);
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotAvailable(seq(8))
    );
    assert!(!snapshot_wanted(&sender, &peer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn demand_that_is_not_renewed_expires() {
    let fx = Fixture::with_options(
        small_segments(),
        |config| config.snapshot_demand_ttl_secs = 1,
        None,
    )
    .await;
    let (writer, _tail) = prune_with_writer(&fx).await;
    let sender = fx.sender().await;
    let newcomer = ClientId::new("newcomer").unwrap();
    let started = std::time::Instant::now();
    register_at(&sender, &newcomer, None).await;
    assert!(snapshot_wanted(&sender, &writer).await);

    // Heartbeats of the designee do not renew the demand.
    let deadline = tokio::time::Instant::now() + POLL_CEILING;
    while snapshot_wanted(&sender, &writer).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the demand never expired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(started.elapsed() >= Duration::from_secs(1));

    // The bootstrapping client asks again.
    heartbeat(&sender, &newcomer).await.unwrap();
    assert!(snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn snapshot_demand_survives_a_room_restart() {
    let (fx, writer, tail) = pruned_room().await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some(tail)).await;
    register_at(&sender, &ClientId::new("newcomer").unwrap(), None).await;
    assert!(snapshot_wanted(&sender, &writer).await);

    // The designation is recomputed when the room reopens, without new requests.
    assert!(fx.manager.shutdown_room(&fx.room_id).await);
    let sender = fx.sender().await;
    assert!(snapshot_wanted(&sender, &writer).await);
    assert!(!snapshot_wanted(&sender, &peer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn persisted_snapshot_demand_is_restored_unless_expired() {
    let (fx, writer, _tail) = pruned_room().await;
    let now = std::time::SystemTime::now();
    let day = Duration::from_secs(24 * 60 * 60);
    let roster = [(&writer, "Connected", 8)];

    // Renewed an hour ago: still on, within the default 7-day TTL.
    let sender = fx
        .restart_with_demand(&roster, now - Duration::from_secs(3600))
        .await;
    assert!(snapshot_wanted(&sender, &writer).await);

    // Renewed 8 days ago: expired while the room was closed.
    let sender = fx.restart_with_demand(&roster, now - 8 * day).await;
    assert!(!snapshot_wanted(&sender, &writer).await);

    // Renewed "in the future" (the clock moved backwards): on, as if renewed now.
    let sender = fx.restart_with_demand(&roster, now + 30 * day).await;
    assert!(snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn snapshot_demand_ended_by_a_snapshot_is_not_restored() {
    let (fx, writer, _tail) = pruned_room().await;
    let sender = fx.sender().await;
    let mut events = subscribe(&sender).await;
    register_at(&sender, &ClientId::new("newcomer").unwrap(), None).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);
    stage_snapshot_at(&fx, 8).await;
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotAvailable(seq(8))
    );

    // Without the snapshot, a restored demand would designate the writer again.
    assert!(fx.manager.shutdown_room(&fx.room_id).await);
    fx.relay.purge_room(&fx.room_id).await.unwrap();
    let sender = fx.sender().await;
    assert!(!snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

async fn register_result(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    current_seq: Option<SequenceNumber>,
) -> Result<RegisterResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

#[tokio::test]
async fn stale_cursor_of_a_client_inside_the_log_does_not_request_a_snapshot() {
    let (fx, writer, _tail) = pruned_room().await;
    let sender = fx.sender().await;
    let mut events = subscribe(&sender).await;

    // A commit and a sync sent before the ack that pruned the log carry an old cursor. They
    // are rejected, but the writer's stored cursor (8) is inside the log: it does not need a
    // snapshot.
    let res = commit(&sender, &writer, mutation(50), seq(0), insert_op(50)).await;
    assert!(matches!(res, Err(ServerError::BehindCompaction)), "{res:?}");
    let res = sync_from(&sender, &writer, seq(0)).await;
    assert!(matches!(res, Err(ServerError::BehindCompaction)), "{res:?}");

    assert_eq!(states(&sender).await, (0, 1, 0, 0));
    assert!(!snapshot_wanted(&sender, &writer).await);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(pending_snapshot_events(&mut events).is_empty());
    assert!(!snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn register_reports_only_a_usable_snapshot() {
    let (fx, _writer, tail) = pruned_room().await;
    // A snapshot that fell behind the retained range (staged with bounds that admit it).
    fx.relay
        .stage_snapshot(
            &fx.room_id,
            seq(1),
            snapshot_envelope(),
            std::future::ready(Ok(crate::relay::LogBounds {
                tail_seq: seq(0),
                head_seq: seq(100),
            })),
        )
        .await
        .unwrap();
    assert!(seq(1).get() + 1 < tail.get());

    let sender = fx.sender().await;
    let reg = register_result(&sender, &ClientId::new("newcomer").unwrap(), None)
        .await
        .unwrap();
    assert_eq!(reg.active_snapshot_seq, None);
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn register_with_a_cursor_beyond_the_head_is_rejected_without_touching_the_roster() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("client").unwrap();
    register(&sender, &client).await;
    for n in 1..=3u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(&sender, &client, seq(2)).await.unwrap();

    let res = register_result(&sender, &client, Some(seq(500))).await;
    match res {
        Err(ServerError::InvalidSequence { expected, actual }) => {
            assert_eq!(expected, seq(3));
            assert_eq!(actual, seq(500));
        }
        other => panic!("expected InvalidSequence, got {other:?}"),
    }
    assert_eq!(cursor(&sender, &client).await, Some(seq(2)));

    let stranger = ClientId::new("stranger").unwrap();
    let res = register_result(&sender, &stranger, Some(seq(4))).await;
    assert!(
        matches!(res, Err(ServerError::InvalidSequence { .. })),
        "{res:?}"
    );
    assert_eq!(cursor(&sender, &stranger).await, None);
    assert_eq!(metrics(&sender).await.total_clients, 1);

    // The head itself is a valid cursor.
    register_result(&sender, &stranger, Some(seq(3)))
        .await
        .unwrap();
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn room_with_a_snapshot_upload_in_progress_stays_open() {
    let fx = Fixture::with_options(
        RoomLifecycleOverrides {
            idle_timeout_secs: Some(1),
            ..RoomLifecycleOverrides::default()
        },
        |_| {},
        None,
    )
    .await;
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    register(&sender, &writer).await;
    commit(&sender, &writer, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    // The first of three chunks of a snapshot at the head: the upload stays in progress.
    assert!(!upload_chunk(&fx, 1, 0).await);

    // Well past the idle timeout, with several maintenance ticks and no command.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        fx.manager.get_room(&fx.room_id).is_some(),
        "the room shut down during an upload"
    );
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn commands_queued_when_the_room_goes_idle_are_handled() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    fx.manager.shutdown_all().await;
    let config = Arc::new(ServerConfig {
        data_dir: fx.dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    let (sender, mut actor) = RoomActor::open(
        fx.room_id.clone(),
        SchemaId::new("todo").unwrap(),
        Arc::new(test_schema()),
        fx.dir.path(),
        config,
        RoomLifecyclePolicy::default(),
        Arc::clone(&fx.relay),
        DESIGNATION_TIMEOUT,
    )
    .unwrap();

    // Requests that reached the mailbox before the actor decided to shut down.
    let client = ClientId::new("writer").unwrap();
    let (register_tx, register_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client.clone(),
            current_seq: None,
            reply: register_tx,
        })
        .await
        .unwrap();
    let (commit_tx, commit_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client.clone(),
            mutation_id: mutation(1),
            last_ack_seq: seq(0),
            op: insert_op(1),
            reply: commit_tx,
        })
        .await
        .unwrap();
    let (subscribe_tx, subscribe_rx) = oneshot::channel();
    sender
        .send(RoomCommand::SubscribeEvents {
            reply: subscribe_tx,
        })
        .await
        .unwrap();

    actor.drain_for_idle_shutdown().await;

    // Nothing more is accepted; what was queued was handled normally.
    assert!(sender.is_closed());
    register_rx.await.unwrap().unwrap();
    assert_eq!(commit_rx.await.unwrap().unwrap().assigned_seq, seq(1));
    // A subscription would end with the actor: it is refused so that the caller retries
    // against the reopened room.
    assert!(matches!(
        subscribe_rx.await.unwrap(),
        Err(ServerError::Unavailable(_))
    ));
    drop(actor);

    // The commit is durable and the client registered in the reopened room.
    let sender = fx.sender().await;
    assert_eq!(cursor(&sender, &client).await, Some(seq(0)));
    assert_eq!(metrics(&sender).await.head_seq, seq(1));
    fx.manager.shutdown_all().await;
}
