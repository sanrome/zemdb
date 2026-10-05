use super::*;
use crate::actor::lease::ClientEntry;
use crate::actor::manager::RoomManager;
use crate::fail_point;
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
    room_id: RoomId,
}

impl Fixture {
    async fn new(policy: RoomLifecyclePolicy) -> Self {
        let dir = tempdir().unwrap();
        let manager = new_manager(&dir);
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
        let entries: Vec<ClientEntry> =
            serde_json::from_str(&fs::read_to_string(self.roster_path()).unwrap()).unwrap();
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

fn new_manager(dir: &TempDir) -> RoomManager {
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    let registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    registry
        .register_schema(SchemaId::new("todo").unwrap(), test_schema())
        .unwrap();
    let relay = Arc::new(SnapshotRelay::new_in_memory(Duration::from_secs(60)));
    RoomManager::new(config, registry, relay)
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
    rx.await.unwrap()
}

async fn metrics(sender: &mpsc::Sender<RoomCommand>) -> RoomMetrics {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetMetrics { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap()
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ..RoomLifecyclePolicy::default()
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    assert!(matches!(failed, Err(ServerError::Internal(_))));

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
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ..RoomLifecyclePolicy::default()
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
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ..RoomLifecyclePolicy::default()
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
    let fx = Fixture::new(RoomLifecyclePolicy::default()).await;
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
    assert!(matches!(failed, Err(ServerError::Internal(_))));
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
