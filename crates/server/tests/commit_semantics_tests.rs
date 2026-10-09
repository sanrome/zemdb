use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::sync::{mpsc, oneshot};
use zemdb_core::*;
use zemdb_server::{
    CommitResponse, RoomCommand, RoomLifecyclePolicy, RoomManager, RoomMetrics, SchemaRegistry,
    ServerConfig, ServerError, SnapshotRelay,
};

struct TestRoom {
    _dir: TempDir,
    _manager: RoomManager,
    sender: mpsc::Sender<RoomCommand>,
    schema: Schema,
    segments_dir: PathBuf,
}

fn create_test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .column("completed", DataType::Bool)
        .build()
        .expect("valid table schema");
    Schema::from_tables(vec![table])
}

/// Five operations per segment, and segments that are compressed and pruned on the next
/// maintenance tick once sealed.
fn fast_prune_policy() -> RoomLifecyclePolicy {
    RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::ZERO,
        cold_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    }
}

/// Five operations per segment, with sealed segments retained on disk for the whole test.
fn long_retention_policy() -> RoomLifecyclePolicy {
    RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    }
}

async fn spawn_room(policy: RoomLifecyclePolicy) -> TestRoom {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("tasks-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let data_dir = dir.path().join("data");
    let config = Arc::new(ServerConfig {
        data_dir: data_dir.clone(),
        ..Default::default()
    });
    let relay = Arc::new(
        SnapshotRelay::new(
            data_dir.join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
        )
        .unwrap(),
    );
    let manager = RoomManager::new(config, schema_registry, relay).with_default_policy(policy);
    let room_id = RoomId::new("commit-room").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .unwrap();

    TestRoom {
        segments_dir: data_dir.join("rooms").join("commit-room").join("segments"),
        _dir: dir,
        _manager: manager,
        sender,
        schema,
    }
}

impl TestRoom {
    async fn register(&self, client: &str) {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RoomCommand::RegisterClient {
                client_id: ClientId::new(client).unwrap(),
                current_seq: None,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    async fn commit(
        &self,
        client: &str,
        mutation: u8,
        last_ack_seq: u64,
    ) -> Result<CommitResponse, ServerError> {
        let row = RowBuilder::new()
            .set("id", mutation as i64)
            .set("title", "task")
            .set("completed", false)
            .build();
        let op = self
            .schema
            .to_operation_insert("tasks", &row, 1000)
            .unwrap();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RoomCommand::Commit {
                client_id: ClientId::new(client).unwrap(),
                mutation_id: MutationId::new([mutation; 16]),
                last_ack_seq: SequenceNumber::new(last_ack_seq),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// Commits `count` new mutations from `client`, each acknowledging the previous head.
    async fn commit_many(&self, client: &str, first_mutation: u8, count: u8) {
        for mutation in first_mutation..first_mutation + count {
            let head = self.metrics().await.head_seq.get();
            self.commit(client, mutation, head).await.unwrap();
        }
    }

    async fn metrics(&self) -> RoomMetrics {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RoomCommand::GetMetrics { reply: tx })
            .await
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    /// Waits for the actor's maintenance tick to advance the retention floor past `seq`.
    async fn wait_for_tail_above(&self, seq: u64) {
        for _ in 0..60 {
            if self.metrics().await.tail_seq.get() > seq {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("retention floor never advanced past {}", seq);
    }
}

fn seqs(resp: &CommitResponse) -> Vec<u64> {
    resp.catchup_ops.iter().map(|op| op.seq.get()).collect()
}

#[tokio::test]
async fn commit_behind_retention_is_rejected_without_side_effects() {
    let room = spawn_room(fast_prune_policy()).await;
    room.register("stale").await;
    room.register("writer").await;

    room.commit_many("writer", 1, 5).await;
    room.wait_for_tail_above(1).await;

    let result = room.commit("stale", 100, 0).await;

    assert!(matches!(result, Err(ServerError::BehindCompaction)));
    assert_eq!(room.metrics().await.head_seq.get(), 5);
}

#[tokio::test]
async fn retried_commit_from_client_now_behind_retention_returns_original_seq() {
    let room = spawn_room(fast_prune_policy()).await;
    room.register("stale").await;
    room.register("writer").await;

    let first = room.commit("stale", 1, 0).await.unwrap();
    assert_eq!(first.assigned_seq.get(), 1);
    room.commit_many("writer", 2, 4).await;
    room.wait_for_tail_above(1).await;

    // The retry carries the same mutation id; it was already committed, so it must be
    // acknowledged with its original sequence even though the client fell behind meanwhile.
    let retry = room.commit("stale", 1, 0).await.unwrap();

    assert_eq!(retry.assigned_seq.get(), 1);
    assert!(retry.catchup_ops.is_empty());
    assert!(
        retry.has_more,
        "a client behind retention must not be told it is up to date"
    );
    assert_eq!(room.metrics().await.head_seq.get(), 5);
}

#[tokio::test]
async fn retried_commit_catches_up_from_client_cursor() {
    let room = spawn_room(long_retention_policy()).await;
    room.register("client").await;
    room.register("writer").await;

    room.commit("client", 1, 0).await.unwrap();
    room.commit_many("writer", 2, 2).await;

    let retry_up_to_date = room.commit("client", 1, 3).await.unwrap();
    assert_eq!(retry_up_to_date.assigned_seq.get(), 1);
    assert!(seqs(&retry_up_to_date).is_empty());
    assert!(!retry_up_to_date.has_more);

    let retry_behind = room.commit("client", 1, 0).await.unwrap();
    assert_eq!(retry_behind.assigned_seq.get(), 1);
    assert_eq!(seqs(&retry_behind), vec![1, 2, 3]);
    assert!(!retry_behind.has_more);
}

#[tokio::test]
async fn durable_commit_with_unreadable_catchup_is_acknowledged_without_gap() {
    let room = spawn_room(long_retention_policy()).await;
    room.register("reader").await;
    room.register("writer").await;

    // Ops 1..=5 are sealed into a segment; 6..=7 remain in the active segment.
    room.commit_many("writer", 1, 7).await;
    let sealed = room
        .segments_dir
        .join(format!("segment_{:016}_{:016}.wal", 1, 5));
    assert!(sealed.exists());
    std::fs::write(&sealed, b"corrupted segment contents").unwrap();

    let resp = room.commit("reader", 50, 0).await.unwrap();

    assert_eq!(resp.assigned_seq.get(), 8);
    assert!(resp.catchup_ops.is_empty());
    assert!(resp.has_more);
    assert_eq!(room.metrics().await.head_seq.get(), 8);
}
