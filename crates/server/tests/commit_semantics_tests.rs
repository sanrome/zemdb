mod common;

use common::{commit, make_test_op, metrics, register, seqs, TestRoom};
use std::time::Duration;
use zemdb_core::*;
use zemdb_server::{CommitResponse, RoomLifecyclePolicy, ServerError};

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
    TestRoom::spawn(Some(policy)).await
}

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

impl TestRoom {
    async fn register(&self, client_id: &str) {
        register(&self.sender, &client(client_id)).await;
    }

    async fn commit(
        &self,
        client_id: &str,
        mutation: u8,
        last_ack_seq: u64,
    ) -> Result<CommitResponse, ServerError> {
        commit(
            &self.sender,
            &client(client_id),
            MutationId::new([mutation; 16]),
            last_ack_seq,
            make_test_op(mutation.into()),
        )
        .await
    }

    /// Commits `count` new mutations from `client`, each acknowledging the previous head.
    async fn commit_many(&self, client: &str, first_mutation: u8, count: u8) {
        for mutation in first_mutation..first_mutation + count {
            let head = self.metrics().await.head_seq.get();
            self.commit(client, mutation, head).await.unwrap();
        }
    }

    async fn metrics(&self) -> zemdb_server::RoomMetrics {
        metrics(&self.sender).await
    }

    /// Waits for the actor's disk maintenance tick to advance the retention floor past `seq`.
    /// Tests calling it run with paused time, so waiting for the tick costs nothing.
    async fn wait_for_tail_above(&self, seq: u64) {
        for _ in 0..120 {
            if self.metrics().await.tail_seq.get() > seq {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("retention floor never advanced past {}", seq);
    }
}

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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
    assert!(seqs(&retry_up_to_date.catchup_ops).is_empty());
    assert!(!retry_up_to_date.has_more);

    let retry_behind = room.commit("client", 1, 0).await.unwrap();
    assert_eq!(retry_behind.assigned_seq.get(), 1);
    assert_eq!(seqs(&retry_behind.catchup_ops), vec![1, 2, 3]);
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
        .segments_dir()
        .join(format!("segment_{:016}_{:016}.wal", 1, 5));
    assert!(sealed.exists());
    std::fs::write(&sealed, b"corrupted segment contents").unwrap();

    let resp = room.commit("reader", 50, 0).await.unwrap();

    assert_eq!(resp.assigned_seq.get(), 8);
    assert!(resp.catchup_ops.is_empty());
    assert!(resp.has_more);
    assert_eq!(room.metrics().await.head_seq.get(), 8);
}
