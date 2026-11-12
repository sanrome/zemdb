use super::*;
use crate::actor::lease::ClientEntry;
use crate::actor::manager::RoomManager;
use crate::actor::test_support::{
    seq, snapshot_envelope, test_manager, test_schema, wait_until, wait_until_async, POLL_CEILING,
    TEST_TIMINGS,
};
use crate::durable;
use crate::fail_point;
use crate::log::io_probe::{self, IoEvent};
use crate::log::segment_index::{segment_path, Tier};
use crate::log::RoomLifecycleOverrides;
use crate::test_support::TestRng;
use std::fs;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tempfile::{tempdir, TempDir};
use tokio::sync::oneshot;
use zemdb_core::id::ClientId;
use zemdb_core::mutation::Operation;
use zemdb_core::value::{PrimaryKey, RowBuilder, Value};

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

fn mutation(n: u8) -> MutationId {
    MutationId::new([n; 16])
}

struct Fixture {
    dir: TempDir,
    manager: RoomManager,
    relay: Arc<SnapshotRelay>,
    room_id: RoomId,
    incarnation: u128,
}

impl Fixture {
    async fn new(policy: RoomLifecycleOverrides) -> Self {
        Self::with_options(policy, |_| {}, None).await
    }

    /// A fixture whose room has the lifecycle overrides `policy`, whose server configuration
    /// is adjusted by `configure` and whose actor runs with `timings` ([`TEST_TIMINGS`] if not
    /// given).
    async fn with_options(
        policy: RoomLifecycleOverrides,
        configure: impl FnOnce(&mut ServerConfig),
        timings: Option<ActorTimings>,
    ) -> Self {
        let dir = tempdir().unwrap();
        let (mut manager, relay) = new_manager_with(&dir, configure);
        if let Some(timings) = timings {
            manager.set_timings(timings);
        }
        let room_id = RoomId::new("room").unwrap();
        let incarnation = manager
            .create_room(
                room_id.clone(),
                SchemaId::new("todo").unwrap(),
                Some(policy),
            )
            .await
            .unwrap()
            .room_incarnation;
        Self {
            dir,
            manager,
            relay,
            room_id,
            incarnation,
        }
    }

    fn room_dir(&self) -> PathBuf {
        self.dir.path().join("rooms").join(self.room_id.as_str())
    }

    fn roster_path(&self) -> PathBuf {
        self.room_dir()
            .join(format!("meta_clients_{}.json", self.room_id.as_str()))
    }

    /// The client's cursor in the roster file, if the file holds the client.
    fn persisted_cursor(&self, client_id: &ClientId) -> Option<SequenceNumber> {
        cursor_in_roster(&self.roster_path(), client_id)
    }

    async fn sender(&self) -> mpsc::Sender<RoomCommand> {
        self.manager
            .get_or_spawn(&self.room_id, None)
            .await
            .unwrap()
    }
}

/// The client's cursor in the roster file at `path`, if the file holds the client.
fn cursor_in_roster(path: &Path, client_id: &ClientId) -> Option<SequenceNumber> {
    let roster: serde_json::Value =
        serde_json::from_str(durable::verified_json(&fs::read(path).ok()?)?).unwrap();
    let entries: Vec<ClientEntry> = serde_json::from_value(roster["clients"].clone()).unwrap();
    entries
        .into_iter()
        .find(|e| &e.client_id == client_id)
        .map(|e| e.last_ack_seq)
}

fn new_manager_with(
    dir: &TempDir,
    configure: impl FnOnce(&mut ServerConfig),
) -> (RoomManager, Arc<SnapshotRelay>) {
    test_manager(dir.path(), &["todo"], |config| {
        // Rooms under test stay open unless a test enables the shutdown for inactivity.
        config.idle_timeout_secs = 0;
        configure(config);
    })
}

async fn register(sender: &mpsc::Sender<RoomCommand>, client_id: &ClientId) {
    register_at(sender, client_id, None).await;
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

    let sender_ref = &sender;
    wait_until_async("the retention floor advances", move || async move {
        metrics(sender_ref).await.tail_seq > seq(1)
    })
    .await;
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

    wait_until("the cursor is persisted", || {
        fx.persisted_cursor(&client) == Some(seq(1))
    })
    .await;
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
        None,
        "the roster must still be pending before Shutdown"
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

    // The record may be only in the page cache: the room reopens once this process synced it.
    let active = fx.room_dir().join("segments").join("active.wal");
    fail_point::arm("warm_open_sync", &active);
    assert!(fx.manager.get_or_spawn(&fx.room_id, None).await.is_err());

    // The next request respawns the room from disk.
    let recovered = fx.sender().await;
    assert!(!recovered.same_channel(&sender));
    let retry = commit(&recovered, &client, mutation(2), seq(1), insert_op(2)).await;
    assert_eq!(retry.unwrap().assigned_seq, seq(2));
    let next = commit(&recovered, &client, mutation(3), seq(2), insert_op(3)).await;
    assert_eq!(next.unwrap().assigned_seq, seq(3));

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

// Paused time: no maintenance tick writes the roster.
#[tokio::test(start_paused = true)]
async fn acks_that_prune_no_segment_do_not_write_the_roster() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for n in 1..=50u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
        ack(&sender, &client, seq(n.into())).await.unwrap();
    }
    assert!(!fx.roster_path().exists());
    fx.manager.shutdown_all().await;
}

// Paused time: no maintenance tick writes the registration before the crash.
#[tokio::test(start_paused = true)]
async fn registration_lost_in_a_crash_is_made_again() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let client = ClientId::new("writer").unwrap();
    register(&fx.sender().await, &client).await;

    // A panic stops the room without saving its roster, as a crash would.
    fail_point::arm("room_command_panic", &fx.room_dir());
    let crashed = fx
        .manager
        .ask(&fx.room_id, |reply| RoomCommand::Heartbeat {
            client_id: client.clone(),
            reply,
        })
        .await;
    assert!(matches!(crashed, Err(ServerError::Internal(_))));

    let sender = fx.sender().await;
    assert!(matches!(
        heartbeat(&sender, &client).await,
        Err(ServerError::ClientNotRegistered(_))
    ));
    register(&sender, &client).await;
    heartbeat(&sender, &client).await.unwrap();
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
    let retry = commit(&recovered, &client, mutation(2), seq(1), insert_op(2)).await;
    assert_eq!(retry.unwrap().assigned_seq, seq(2));
    let next = commit(&recovered, &client, mutation(3), seq(2), insert_op(3)).await;
    assert_eq!(next.unwrap().assigned_seq, seq(3));
    let synced = sync_all(&recovered, &client).await.unwrap();
    let seqs: Vec<u64> = synced.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    fx.manager.shutdown_all().await;
}

/// A rotation that recorded its end and stopped before the rename, then damage to the last
/// frame of `active.wal` (commit 3): the respawn truncates it, and the commit acknowledged
/// after it (4) is still served after another reopen.
// Paused time: no maintenance tick runs.
#[tokio::test(start_paused = true)]
async fn commit_acknowledged_after_a_truncated_frame_below_the_sealed_end_is_served() {
    let policy = RoomLifecycleOverrides {
        ram_max_ops: Some(3),
        ..RoomLifecycleOverrides::default()
    };
    let fx = Fixture::new(policy).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for i in 1..=2u8 {
        let last_ack = seq(u64::from(i) - 1);
        commit(
            &sender,
            &client,
            mutation(i),
            last_ack,
            insert_op(i64::from(i)),
        )
        .await
        .unwrap();
    }
    let active = fx.room_dir().join("segments").join("active.wal");
    fail_point::arm("exists", &active);
    let acked = commit(&sender, &client, mutation(3), seq(2), insert_op(3)).await;
    assert_eq!(acked.unwrap().assigned_seq, seq(3));
    fx.manager.shutdown_all().await;
    let mut bytes = fs::read(&active).unwrap();
    *bytes.last_mut().unwrap() ^= 0xFF;
    fs::write(&active, bytes).unwrap();

    let recovered = fx.sender().await;
    assert_eq!(metrics(&recovered).await.head_seq, seq(3));
    let acked = commit(&recovered, &client, mutation(4), seq(3), insert_op(4)).await;
    assert_eq!(acked.unwrap().assigned_seq, seq(4));
    fx.manager.shutdown_all().await;

    let reopened = fx.sender().await;
    let (tx, rx) = oneshot::channel();
    let sync = RoomCommand::Sync {
        client_id: client.clone(),
        from_seq: seq(3),
        max_batch_size: 100,
        reply: tx,
    };
    reopened.send(sync).await.unwrap();
    let synced = rx.await.unwrap().unwrap();
    let seqs: Vec<u64> = synced.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, vec![4]);
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
async fn commit_that_would_take_the_last_sequence_number_is_refused() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    fx.manager.shutdown_all().await;
    let last = u64::MAX - 1;
    let meta = format!(r#"{{"pruned_through_seq": {last}, "sealed_through_seq": 0}}"#);
    durable::write_checked(&fx.room_dir().join("log_meta.json"), meta.as_bytes()).unwrap();

    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let res = commit(&sender, &client, mutation(1), seq(last), insert_op(1)).await;
    assert!(matches!(res, Err(ServerError::Internal(_))), "{res:?}");
    assert_eq!(metrics(&sender).await.head_seq, seq(last));
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

    let bounds = fx.manager.log_bounds(&fx.room_id).await.unwrap();
    assert_eq!(bounds.head_seq, seq(3));
    assert_eq!(bounds.tail_seq, metrics(&sender).await.tail_seq);

    let missing = RoomId::new("missing").unwrap();
    assert!(matches!(
        fx.manager.log_bounds(&missing).await,
        Err(ServerError::RoomNotFound(_))
    ));
    fx.manager.shutdown_all().await;
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
            &crate::relay::Uploader::Admin,
            seq(1),
            snapshot_envelope(),
            std::future::ready(Ok(crate::relay::LogBounds {
                tail_seq: seq(0),
                head_seq: seq(100),
                unrestorable_through: None,
                room_incarnation: 0,
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
        durable::write_checked(&self.roster_path(), &serde_json::to_vec(&roster).unwrap()).unwrap();
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
        durable::write_checked(&self.roster_path(), &serde_json::to_vec(&roster).unwrap()).unwrap();
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
async fn a_bootstrapping_client_does_not_hold_back_pruning() {
    let fx = Fixture::new(small_segments()).await;
    let (writer, tail) = prune_with_writer(&fx).await;
    let sender = fx.sender().await;
    let newbie = ClientId::new("newbie").unwrap();
    register(&sender, &newbie).await;
    assert_eq!(states(&sender).await.0, 1, "newbie must be Bootstrapping");

    for n in 9..=12u8 {
        commit(&sender, &writer, mutation(n), seq(8), insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(&sender, &writer, seq(12)).await.unwrap();
    assert!(metrics(&sender).await.tail_seq > tail);
    fx.manager.shutdown_all().await;
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

/// A commit's catch-up and a sync return the operations of the RAM buffer themselves: the
/// operation of a client up to date, the catch-up of a client behind and a sync share each
/// operation, without copies (DS6).
#[tokio::test]
async fn catch_ups_and_syncs_share_the_operations_of_the_ram_buffer() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    let behind = ClientId::new("behind").unwrap();
    register(&sender, &writer).await;
    register(&sender, &behind).await;
    let own = commit(&sender, &writer, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &writer, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();
    let catch_up = commit(&sender, &behind, mutation(3), seq(0), insert_op(3))
        .await
        .unwrap();
    let synced = sync_all(&sender, &writer).await.unwrap().ops;

    let seqs = |ops: &[Arc<SequencedOperation>]| -> Vec<u64> {
        ops.iter().map(|op| op.seq.get()).collect()
    };
    assert_eq!(seqs(&own.catchup_ops), [1]);
    assert_eq!(seqs(&catch_up.catchup_ops), [1, 2, 3]);
    assert_eq!(seqs(&synced), [1, 2, 3]);
    assert!(
        Arc::ptr_eq(&own.catchup_ops[0], &synced[0]),
        "the operation of the client up to date is a copy"
    );
    for (op, same) in catch_up.catchup_ops.iter().zip(&synced) {
        assert!(Arc::ptr_eq(op, same), "operation {} is a copy", op.seq);
    }
    fx.manager.shutdown_all().await;
}

/// A sync and the catch-up of a commit carry what the read of the log returns, which fits in a
/// response once encoded without measuring it again: two of three operations of 6 MiB, flagged
/// `has_more`.
#[tokio::test]
async fn syncs_and_catch_ups_carry_what_fits_in_a_response() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    let behind = ClientId::new("behind").unwrap();
    register(&sender, &writer).await;
    register(&sender, &behind).await;
    for n in 1..=3u8 {
        let op = insert_op_with_title(n.into(), 6 << 20);
        commit(&sender, &writer, mutation(n), seq(u64::from(n) - 1), op)
            .await
            .unwrap();
    }
    let catch_up = commit(&sender, &behind, mutation(4), seq(0), insert_op(4))
        .await
        .unwrap();
    let synced = sync_all(&sender, &writer).await.unwrap();

    for (ops, has_more) in [
        (&catch_up.catchup_ops, catch_up.has_more),
        (&synced.ops, synced.has_more),
    ] {
        let seqs: Vec<u64> = ops.iter().map(|op| op.seq.get()).collect();
        assert_eq!(seqs, [1, 2]);
        assert!(has_more);
        let encoded: u64 = ops
            .iter()
            .map(|op| zemdb_core::protocol::codec::encoded_len(op).unwrap())
            .sum();
        assert!(
            encoded <= zemdb_core::protocol::limits::MAX_RESPONSE_OPS_BYTES,
            "{encoded} bytes"
        );
    }
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

/// Registers `client_id` with `cursor`: its sequence and the room incarnation it belongs to.
async fn register_at(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    cursor: Option<(SequenceNumber, u128)>,
) {
    register_result(sender, client_id, cursor).await.unwrap();
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
        .stage_snapshot(
            &fx.room_id,
            &crate::relay::Uploader::Admin,
            seq(n),
            snapshot_envelope(),
            fx.manager.log_bounds(&fx.room_id),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn client_registering_behind_the_log_gets_one_designated_uploader() {
    let (fx, writer, tail) = pruned_room().await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some((tail, fx.incarnation))).await;
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
async fn snapshot_below_a_gap_stops_being_usable() {
    let fx = Fixture::new(small_segments()).await;
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    register(&sender, &writer).await;
    for n in 1..=6u8 {
        commit(&sender, &writer, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    // The snapshot anchors the log, so the writer's ack prunes nothing.
    stage_snapshot_at(&fx, 2).await;
    ack(&sender, &writer, seq(6)).await.unwrap();
    let segments = fx.room_dir().join("segments");
    fs::remove_file(segment_path(&segments, seq(3), seq(4), Tier::Warm)).unwrap();

    // A client past the snapshot finds the gap after its cursor: the snapshot is no longer
    // offered, and the writer is asked for a new one.
    let restorer = ClientId::new("restorer").unwrap();
    register_at(&sender, &restorer, Some((seq(3), fx.incarnation))).await;
    let before_gap = fx.manager.log_bounds(&fx.room_id).await.unwrap();
    let gap = async || {
        let res = sync_from(&sender, &restorer, seq(3)).await;
        assert!(matches!(res, Err(ServerError::BehindCompaction)), "{res:?}");
        heartbeat(&sender, &writer).await.unwrap()
    };
    let hb = gap().await;
    assert_eq!(hb.active_snapshot_seq, None);
    assert!(hb.snapshot_wanted);
    // Nor is it served to a client that saw it announced and starts a download now.
    let first_chunk = async || {
        let bounds = fx.manager.log_bounds(&fx.room_id);
        let correlation_id = zemdb_core::id::CorrelationId::new(1);
        fx.relay
            .get_chunk(correlation_id, &fx.room_id, 0, 0, None, bounds)
            .await
    };
    let res = first_chunk().await;
    assert!(matches!(res, Err(ServerError::RoomNotFound(_))), "{res:?}");
    assert_eq!(fx.relay.active_snapshot_seq(&fx.room_id), None);
    // Nor is a new upload at its sequence, or at any other up to the cursor: restored, it
    // would run into the same gap. A sync from further down does not lower that bound.
    let res = sync_from(&sender, &restorer, seq(2)).await;
    assert!(matches!(res, Err(ServerError::BehindCompaction)), "{res:?}");
    for n in [2, 3] {
        let res = fx
            .relay
            .stage_snapshot(
                &fx.room_id,
                &crate::relay::Uploader::Admin,
                seq(n),
                snapshot_envelope(),
                fx.manager.log_bounds(&fx.room_id),
            )
            .await;
        assert!(
            matches!(res, Err(ServerError::BadRequest(_))),
            "{n}: {res:?}"
        );
    }
    // Nor one whose range was read before the gap was found.
    let res = fx
        .relay
        .stage_snapshot(
            &fx.room_id,
            &crate::relay::Uploader::Admin,
            seq(3),
            snapshot_envelope(),
            std::future::ready(Ok(before_gap)),
        )
        .await;
    assert!(
        matches!(res, Err(ServerError::SnapshotSuperseded(_))),
        "{res:?}"
    );

    // A snapshot above the gap is offered and served, also after another client runs into it.
    stage_snapshot_at(&fx, 6).await;
    assert_eq!(gap().await.active_snapshot_seq, Some(seq(6)));
    first_chunk().await.unwrap();
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn designee_that_leaves_is_replaced() {
    let (fx, writer, tail) = pruned_room().await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some((tail, fx.incarnation))).await;
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
/// Four maintenance ticks: a designee is replaced on the first tick past it, and the steps a
/// test takes between two designations have that long even on a slow machine.
const DESIGNATION_TIMEOUT_IN_TESTS: Duration = Duration::from_secs(2);

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
            fx.manager.log_bounds(&fx.room_id),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn designee_that_does_not_upload_in_time_is_replaced_but_never_during_its_upload() {
    let fx = Fixture::with_options(
        small_segments(),
        |_| {},
        Some(ActorTimings {
            designation_timeout: DESIGNATION_TIMEOUT_IN_TESTS,
            ..TEST_TIMINGS
        }),
    )
    .await;
    let (writer, tail) = prune_with_writer(&fx).await;
    let sender = fx.sender().await;
    let peer = ClientId::new("peer").unwrap();
    register_at(&sender, &peer, Some((tail, fx.incarnation))).await;
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

    // The writer's upload does not keep the peer, which does not upload: every candidate had
    // its turn, so the role goes back to the writer.
    assert!(!upload_chunk(&fx, 8, 0).await);
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotWanted
    );
    assert!(snapshot_wanted(&sender, &writer).await);

    // The designee's upload does: however long it takes, the role stays put. Well past the
    // designation timeout, with several maintenance ticks.
    tokio::time::sleep(DESIGNATION_TIMEOUT_IN_TESTS * 5 / 2).await;
    let during_upload = pending_snapshot_events(&mut events);
    assert!(during_upload.is_empty(), "{during_upload:?}");
    assert!(snapshot_wanted(&sender, &writer).await);

    // The upload completes: the demand ends.
    assert!(!upload_chunk(&fx, 8, 1).await);
    assert!(upload_chunk(&fx, 8, 2).await);
    assert_eq!(
        next_snapshot_event(&mut events).await,
        RoomEvent::SnapshotAvailable(seq(8))
    );
    assert!(!snapshot_wanted(&sender, &writer).await);
    fx.manager.shutdown_all().await;
}

/// Commits `ops` as `writer`, which acknowledged everything before them, and acknowledges
/// the last one.
async fn commit_and_ack(
    sender: &mpsc::Sender<RoomCommand>,
    writer: &ClientId,
    ops: RangeInclusive<u8>,
) {
    let (acked, last) = (seq(u64::from(*ops.start()) - 1), *ops.end());
    for n in ops {
        commit(sender, writer, mutation(n), acked, insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(sender, writer, seq(last.into())).await.unwrap();
}

// Paused time: only the ack path prunes, and the clock moves only when the test advances it.
#[tokio::test(start_paused = true)]
async fn snapshot_upload_in_progress_anchors_pruning_until_it_goes_idle() {
    let fx = Fixture::new(RoomLifecycleOverrides {
        lease_timeout_secs: Some(600),
        ..small_segments()
    })
    .await;
    let sender = fx.sender().await;
    let writer = ClientId::new("writer").unwrap();
    register(&sender, &writer).await;

    // An upload that stops receiving chunks no longer holds back pruning.
    commit_and_ack(&sender, &writer, 1..=4).await;
    assert!(!upload_chunk(&fx, 4, 0).await);
    tokio::time::advance(crate::relay::UPLOAD_IDLE_TIMEOUT).await;
    commit_and_ack(&sender, &writer, 5..=10).await;
    let tail = metrics(&sender).await.tail_seq;
    assert!(
        tail > seq(5),
        "an idle upload held back pruning (tail {tail})"
    );

    // An active upload does: the room moves on and acknowledges, and the upload completes.
    assert!(!upload_chunk(&fx, 10, 0).await);
    commit_and_ack(&sender, &writer, 11..=16).await;
    let tail = metrics(&sender).await.tail_seq;
    assert!(
        tail <= seq(11),
        "pruning passed the upload at 10 (tail {tail})"
    );
    assert!(!upload_chunk(&fx, 10, 1).await);
    assert!(upload_chunk(&fx, 10, 2).await);
    assert_eq!(fx.relay.active_snapshot_seq(&fx.room_id), Some(seq(10)));
    fx.manager.shutdown_all().await;
}

/// A relay for the rooms of [`stopped_actor`] on `dir`.
fn relay_on(dir: &TempDir) -> Arc<SnapshotRelay> {
    Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
            Duration::from_secs(ServerConfig::default().snapshot_upload_max_secs),
        )
        .unwrap(),
    )
}

/// The room `room` on `dir`, opened with `policy` and `timings` but without running its loop:
/// the test runs its ticks itself, at the instants it chooses.
fn stopped_actor(
    dir: &TempDir,
    relay: &Arc<SnapshotRelay>,
    policy: RoomLifecyclePolicy,
    timings: ActorTimings,
) -> RoomActor {
    stopped_actor_with(dir, relay, policy, timings, 1, |_| {})
}

/// [`stopped_actor`], with the room's `incarnation` and the server configuration adjusted by
/// `configure`.
fn stopped_actor_with(
    dir: &TempDir,
    relay: &Arc<SnapshotRelay>,
    policy: RoomLifecyclePolicy,
    timings: ActorTimings,
    incarnation: u128,
    configure: impl FnOnce(&mut ServerConfig),
) -> RoomActor {
    let mut config = ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    };
    configure(&mut config);
    let config = Arc::new(config);
    let (_sender, actor) = RoomActor::open(
        RoomId::new("room").unwrap(),
        incarnation,
        SchemaId::new("todo").unwrap(),
        Arc::new(test_schema()),
        dir.path(),
        config,
        policy,
        Arc::clone(relay),
        Arc::new(InflightBudget::new(u64::MAX, u64::MAX)),
        Arc::new(OpsEventBudget::new(u64::MAX)),
        timings,
        IoThreads::new(2),
    )
    .unwrap();
    actor
}

/// Registers `writer` and `peer` in `actor`, whose policy seals a segment every two
/// operations; `writer` commits 1..=8 and acknowledges 8, which prunes the log, and `peer`
/// registers at 8. Both are `Connected`.
async fn prune_stopped_actor(actor: &mut RoomActor, writer: &ClientId, peer: &ClientId) {
    actor.register(writer, None, None).unwrap();
    let mut flow = ControlFlow::Continue(());
    for n in 1..=8u8 {
        actor
            .commit(writer, mutation(n), seq(0), insert_op(n.into()), &mut flow)
            .await
            .unwrap();
    }
    actor.ack(writer, seq(8)).await.unwrap();
    assert!(actor.log().tail_seq() > seq(1), "the ack must prune");
    actor.register(peer, Some(seq(8)), Some(1)).unwrap();
}

impl RoomActor {
    /// Commits `op` as a group of its own, and returns its reply; `flow` is what the group
    /// returned.
    async fn commit(
        &mut self,
        client_id: &ClientId,
        mutation_id: MutationId,
        last_ack_seq: SequenceNumber,
        op: Operation,
        flow: &mut ControlFlow<()>,
    ) -> Result<CommitResponse, ServerError> {
        let (reply, rx) = oneshot::channel();
        let request = CommitRequest {
            client_id: client_id.clone(),
            mutation_id,
            last_ack_seq,
            op,
            reply,
        };
        *flow = self.commit_group(request).await;
        rx.await.unwrap()
    }
}

fn state_of(actor: &RoomActor, client: &ClientId) -> ClientState {
    actor.lease_tracker.get_client(client).unwrap().state
}

/// How far the tests of the deadlines move the Tokio clock before they start, so that it runs
/// ahead of the system's monotonic clock: a deadline measured on the system clock would then
/// expire at once or never.
const CLOCK_AHEAD: Duration = Duration::from_secs(3600);

/// Least time between two writes of the roster by the fast tick in the tests of that period.
const ROSTER_PERSIST: Duration = Duration::from_secs(4);

/// The room of the tests of the roster's period: [`stopped_actor`] with `policy`, whose fast
/// tick writes the roster at most once every [`ROSTER_PERSIST`], and whose clients' leases
/// outlast the tests. Its incarnation, 1, puts its first write 1 ms into the first period: in
/// practice, on the first tick.
fn roster_actor(
    dir: &TempDir,
    relay: &Arc<SnapshotRelay>,
    policy: RoomLifecyclePolicy,
) -> RoomActor {
    roster_actor_of(dir, relay, policy, 1)
}

/// [`roster_actor`], with the room's `incarnation`.
fn roster_actor_of(
    dir: &TempDir,
    relay: &Arc<SnapshotRelay>,
    policy: RoomLifecyclePolicy,
    incarnation: u128,
) -> RoomActor {
    let policy = RoomLifecyclePolicy {
        lease_timeout: Duration::from_secs(3600),
        ..policy
    };
    stopped_actor_with(dir, relay, policy, TEST_TIMINGS, incarnation, |config| {
        config.roster_persist_secs = ROSTER_PERSIST.as_secs();
    })
}

/// The client's cursor in the roster file of the room of [`stopped_actor`] on `dir`.
fn cursor_on_disk(dir: &TempDir, client_id: &ClientId) -> Option<SequenceNumber> {
    let room_id = RoomId::new("room").unwrap();
    let path =
        room_dir(dir.path(), &room_id).join(format!("meta_clients_{}.json", room_id.as_str()));
    cursor_in_roster(&path, client_id)
}

/// Registers `client` in `actor` and commits two operations from it, with its cursor at 0.
async fn register_and_commit_two(actor: &mut RoomActor, client: &ClientId) {
    actor.register(client, None, None).unwrap();
    let mut flow = ControlFlow::Continue(());
    for n in 1..=2u8 {
        actor
            .commit(client, mutation(n), seq(0), insert_op(n.into()), &mut flow)
            .await
            .unwrap();
    }
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn the_fast_tick_writes_a_changed_roster_at_most_once_every_roster_persist_secs() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let mut actor = roster_actor(&dir, &relay_on(&dir), RoomLifecyclePolicy::default());
    let client = ClientId::new("writer").unwrap();
    register_and_commit_two(&mut actor, &client).await;
    // The first tick that finds the roster changed writes it.
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));

    // A change waits for the tick of the grid's next point, a period after that write.
    actor.ack(&client, seq(1)).await.unwrap();
    for _ in 0..ROSTER_PERSIST.as_secs() - 1 {
        tokio::time::advance(Duration::from_secs(1)).await;
        actor.run_fast_maintenance().await;
        assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));

    // And the next change, for the point after that.
    actor.ack(&client, seq(2)).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_room_first_writes_its_roster_at_the_point_of_the_period_its_incarnation_gives() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    // 2.5 s into the period of 4 s.
    let incarnation = 4_000 * 1_000_000 + 2_500;
    let mut actor = roster_actor_of(
        &dir,
        &relay_on(&dir),
        RoomLifecyclePolicy::default(),
        incarnation,
    );
    let client = ClientId::new("writer").unwrap();
    register_and_commit_two(&mut actor, &client).await;

    let tick = TEST_TIMINGS.fast_tick;
    for _ in 0..4 {
        actor.run_fast_maintenance().await;
        assert_eq!(cursor_on_disk(&dir, &client), None);
        tokio::time::advance(tick).await;
    }
    // 2 s in, a tick before the phase, it does not write yet; 2.5 s in, it does.
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), None);
    tokio::time::advance(tick).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));

    // From then on, a period after each write.
    actor.ack(&client, seq(1)).await.unwrap();
    tokio::time::advance(ROSTER_PERSIST - tick).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));
    tokio::time::advance(tick).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn the_tick_of_the_period_writes_the_roster_even_if_it_runs_a_little_early() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let mut actor = roster_actor(&dir, &relay_on(&dir), RoomLifecyclePolicy::default());
    let client = ClientId::new("writer").unwrap();
    register_and_commit_two(&mut actor, &client).await;
    actor.run_fast_maintenance().await;
    actor.ack(&client, seq(1)).await.unwrap();

    // The tick before the period's does not write it, nor one 0.6 of a tick early.
    let tick = TEST_TIMINGS.fast_tick;
    tokio::time::advance(ROSTER_PERSIST - tick).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));
    tokio::time::advance(tick * 2 / 5).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));
    // The period's tick does, though it comes 0.4 of a tick early: half a tick of slack.
    tokio::time::advance(tick / 5).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_write_off_the_grid_does_not_move_the_grid_of_a_room() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    // The grid: 2.5 s, 6.5 s, 10.5 s, 14.5 s... after the room opens.
    let incarnation = 4_000 * 1_000_000 + 2_500;
    let mut actor = roster_actor_of(
        &dir,
        &relay_on(&dir),
        RoomLifecyclePolicy::default(),
        incarnation,
    );
    let client = ClientId::new("writer").unwrap();
    register_and_commit_two(&mut actor, &client).await;
    let tick = TEST_TIMINGS.fast_tick;
    tokio::time::advance(tick * 5).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));

    // Unchanged until 12 s: the change of then goes with the next tick, off the grid.
    tokio::time::advance(Duration::from_millis(9_500)).await;
    actor.ack(&client, seq(1)).await.unwrap();
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));

    // The next change waits for the grid's next point, 14.5 s, not for 12 s plus a period.
    actor.ack(&client, seq(2)).await.unwrap();
    for _ in 0..4 {
        tokio::time::advance(tick).await;
        actor.run_fast_maintenance().await;
        assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));
    }
    tokio::time::advance(tick).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(2)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn the_next_point_counts_from_when_a_write_was_asked_for_not_from_when_the_io_threads_ran_it()
{
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let mut actor = roster_actor(&dir, &relay_on(&dir), RoomLifecyclePolicy::default());
    let client = ClientId::new("writer").unwrap();
    register_and_commit_two(&mut actor, &client).await;

    // The file I/O threads are busy for longer than a period: the write waits for them.
    let pool = Arc::clone(&actor.io);
    let busy = [
        pool.io().try_permit().unwrap(),
        pool.io().try_permit().unwrap(),
    ];
    let wait = ROSTER_PERSIST + TEST_TIMINGS.fast_tick;
    tokio::join!(actor.run_fast_maintenance(), async move {
        tokio::time::advance(wait).await;
        drop(busy);
    });
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));

    // The write was asked for at the grid's first point, so the next one is already due:
    // the wait for the threads does not add to what a crash may lose.
    actor.ack(&client, seq(1)).await.unwrap();
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_change_to_a_roster_not_written_for_roster_persist_secs_goes_with_the_next_tick() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let mut actor = roster_actor(&dir, &relay_on(&dir), RoomLifecyclePolicy::default());
    let client = ClientId::new("writer").unwrap();
    register_and_commit_two(&mut actor, &client).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));

    // Ticks that find nothing to write do not start the period again.
    for _ in 0..3 * ROSTER_PERSIST.as_secs() {
        tokio::time::advance(Duration::from_secs(1)).await;
        actor.run_fast_maintenance().await;
    }
    actor.ack(&client, seq(1)).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(1)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_crash_loses_only_the_roster_changes_since_its_last_write() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let relay = relay_on(&dir);
    let mut actor = roster_actor(&dir, &relay, RoomLifecyclePolicy::default());
    let (writer, peer) = (
        ClientId::new("writer").unwrap(),
        ClientId::new("peer").unwrap(),
    );
    register_and_commit_two(&mut actor, &writer).await;
    actor.run_fast_maintenance().await;
    actor.ack(&writer, seq(2)).await.unwrap();
    actor.register(&peer, None, None).unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    actor.run_fast_maintenance().await;
    // A crash: the room's memory is lost, its files stay as they are.
    drop(actor);

    let mut reopened = roster_actor(&dir, &relay, RoomLifecyclePolicy::default());
    // The lost progress of a cursor only makes the room keep more of its log.
    let writer_entry = reopened.lease_tracker.get_client(&writer).unwrap();
    assert_eq!(writer_entry.last_ack_seq, seq(0));
    assert_eq!(reopened.log().tail_seq(), seq(1));
    // A lost registration makes the client register again.
    assert!(matches!(
        reopened.heartbeat(&peer),
        Err(ServerError::ClientNotRegistered(_))
    ));
    reopened.register(&peer, None, None).unwrap();
    reopened.heartbeat(&peer).unwrap();
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_prune_that_deletes_writes_the_roster_before_its_period_ends() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = roster_actor(&dir, &relay_on(&dir), policy);
    let client = ClientId::new("writer").unwrap();
    actor.register(&client, None, None).unwrap();
    let mut flow = ControlFlow::Continue(());
    for n in 1..=4u8 {
        actor
            .commit(&client, mutation(n), seq(0), insert_op(n.into()), &mut flow)
            .await
            .unwrap();
    }
    actor.run_fast_maintenance().await;
    assert_eq!(cursor_on_disk(&dir, &client), Some(seq(0)));

    tokio::time::advance(Duration::from_secs(1)).await;
    actor.ack(&client, seq(4)).await.unwrap();
    let tail = actor.log().tail_seq();
    assert!(tail > seq(1), "the ack must prune a segment");
    let on_disk = cursor_on_disk(&dir, &client).unwrap();
    assert!(
        on_disk.get() + 1 >= tail.get(),
        "the cursor on disk {on_disk} is behind the pruned tail {tail}"
    );
}

#[tokio::test]
async fn shutdown_writes_the_roster_changes_that_wait_for_their_period() {
    // The room first writes its roster within a period of opening, and then once a period.
    let fx = Fixture::with_options(
        RoomLifecycleOverrides::default(),
        |config| config.roster_persist_secs = ROSTER_PERSIST.as_secs(),
        None,
    )
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    wait_until("the roster is written", || {
        fx.persisted_cursor(&client) == Some(seq(0))
    })
    .await;
    commit(&sender, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();
    tokio::time::sleep(TEST_TIMINGS.fast_tick * 2 + Duration::from_millis(100)).await;
    assert_eq!(
        fx.persisted_cursor(&client),
        Some(seq(0)),
        "the ticks within the period must not write the roster"
    );

    assert!(fx.manager.shutdown_room(&fx.room_id).await);
    assert_eq!(fx.persisted_cursor(&client), Some(seq(1)));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_client_without_heartbeats_disconnects_when_its_lease_expires_on_the_tokio_clock() {
    const LEASE: Duration = Duration::from_secs(90);
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        lease_timeout: LEASE,
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = stopped_actor(&dir, &relay_on(&dir), policy, TEST_TIMINGS);
    let client = ClientId::new("client").unwrap();
    actor.register(&client, None, None).unwrap();
    tokio::time::advance(LEASE / 2).await;
    actor.heartbeat(&client).unwrap();

    // The lease counts from the heartbeat, and expires once it has passed whole.
    tokio::time::advance(LEASE).await;
    actor.run_fast_maintenance().await;
    assert_eq!(state_of(&actor, &client), ClientState::Connected);
    tokio::time::advance(Duration::from_millis(1)).await;
    actor.run_fast_maintenance().await;
    assert_eq!(state_of(&actor, &client), ClientState::Disconnected);
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_designation_expires_after_60_seconds_on_the_tokio_clock_and_passes_to_another_client() {
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        lease_timeout: Duration::from_secs(3600),
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = stopped_actor(&dir, &relay_on(&dir), policy, ActorTimings::default());
    let (writer, peer) = (
        ClientId::new("writer").unwrap(),
        ClientId::new("peer").unwrap(),
    );
    prune_stopped_actor(&mut actor, &writer, &peer).await;
    actor
        .register(&ClientId::new("newcomer").unwrap(), None, None)
        .unwrap();
    let first = actor.snapshot_demand.designee().cloned().unwrap();

    tokio::time::advance(DESIGNATION_TIMEOUT - Duration::from_millis(1)).await;
    actor.run_fast_maintenance().await;
    assert_eq!(actor.snapshot_demand.designee(), Some(&first));
    tokio::time::advance(Duration::from_millis(1)).await;
    actor.run_fast_maintenance().await;
    let second = actor.snapshot_demand.designee().cloned().unwrap();
    assert_ne!(second, first, "the designation must pass to another client");
    assert!([&writer, &peer].contains(&&second));
}

// Paused time: the test moves the Tokio clock and runs the fast tick itself.
#[tokio::test(start_paused = true)]
async fn a_snapshot_demand_expires_on_the_tokio_clock_also_after_the_room_reopens() {
    const TTL: Duration = Duration::from_secs(600);
    tokio::time::advance(CLOCK_AHEAD).await;
    let dir = tempdir().unwrap();
    let relay = relay_on(&dir);
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        lease_timeout: Duration::from_secs(3600),
        snapshot_demand_ttl: TTL,
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = stopped_actor(&dir, &relay, policy.clone(), ActorTimings::default());
    let (writer, peer) = (
        ClientId::new("writer").unwrap(),
        ClientId::new("peer").unwrap(),
    );
    prune_stopped_actor(&mut actor, &writer, &peer).await;
    let newcomer = ClientId::new("newcomer").unwrap();
    actor.register(&newcomer, None, None).unwrap();

    tokio::time::advance(TTL - Duration::from_millis(1)).await;
    actor.run_fast_maintenance().await;
    assert!(actor.snapshot_demand.is_active());
    tokio::time::advance(Duration::from_millis(1)).await;
    actor.run_fast_maintenance().await;
    assert!(!actor.snapshot_demand.is_active());

    // Renewed, and restored when the room reopens. The wall clock hardly moved, so it has
    // almost a whole TTL left.
    actor.heartbeat(&newcomer).unwrap();
    actor.persist_roster_at_stop().await;
    drop(actor);
    let mut actor = stopped_actor(&dir, &relay, policy, ActorTimings::default());
    assert!(actor.snapshot_demand.is_active());
    tokio::time::advance(TTL - Duration::from_secs(1)).await;
    actor.run_fast_maintenance().await;
    assert!(actor.snapshot_demand.is_active());
    tokio::time::advance(Duration::from_secs(1)).await;
    actor.run_fast_maintenance().await;
    assert!(!actor.snapshot_demand.is_active());
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
    let (sender_ref, writer_ref) = (&sender, &writer);
    wait_until_async("the demand expires", move || async move {
        !snapshot_wanted(sender_ref, writer_ref).await
    })
    .await;
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
    register_at(&sender, &peer, Some((tail, fx.incarnation))).await;
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
    cursor: Option<(SequenceNumber, u128)>,
) -> Result<RegisterResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq: cursor.map(|(seq, _)| seq),
            known_incarnation: cursor.map(|(_, incarnation)| incarnation),
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

// Paused time: the slow tick would publish the room's snapshot floor and reject the staging.
#[tokio::test(start_paused = true)]
async fn register_reports_only_a_usable_snapshot() {
    let (fx, _writer, tail) = pruned_room().await;
    // A snapshot that fell behind the retained range (staged with bounds that admit it).
    fx.relay
        .stage_snapshot(
            &fx.room_id,
            &crate::relay::Uploader::Admin,
            seq(1),
            snapshot_envelope(),
            std::future::ready(Ok(crate::relay::LogBounds {
                tail_seq: seq(0),
                head_seq: seq(100),
                unrestorable_through: None,
                room_incarnation: 0,
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

    let res = register_result(&sender, &client, Some((seq(500), fx.incarnation))).await;
    match res {
        Err(ServerError::InvalidSequence { expected, actual }) => {
            assert_eq!(expected, seq(3));
            assert_eq!(actual, seq(500));
        }
        other => panic!("expected InvalidSequence, got {other:?}"),
    }
    assert_eq!(cursor(&sender, &client).await, Some(seq(2)));

    let stranger = ClientId::new("stranger").unwrap();
    let res = register_result(&sender, &stranger, Some((seq(4), fx.incarnation))).await;
    assert!(
        matches!(res, Err(ServerError::InvalidSequence { .. })),
        "{res:?}"
    );
    assert_eq!(cursor(&sender, &stranger).await, None);
    assert_eq!(metrics(&sender).await.total_clients, 1);

    // The head itself is a valid cursor.
    register_result(&sender, &stranger, Some((seq(3), fx.incarnation)))
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
        fx.incarnation,
        SchemaId::new("todo").unwrap(),
        Arc::new(test_schema()),
        fx.dir.path(),
        config,
        RoomLifecyclePolicy::default(),
        Arc::clone(&fx.relay),
        Arc::new(InflightBudget::new(u64::MAX, u64::MAX)),
        Arc::new(OpsEventBudget::new(u64::MAX)),
        ActorTimings::default(),
        IoThreads::new(2),
    )
    .unwrap();

    // Requests that reached the mailbox before the actor decided to shut down.
    let client = ClientId::new("writer").unwrap();
    let (register_tx, register_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client.clone(),
            current_seq: None,
            known_incarnation: None,
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

    assert!(actor.drain_for_idle_shutdown().await.is_continue());
    // As the actor loop does on every exit.
    actor.persist_roster_at_stop().await;

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

/// On a runtime of a single thread, no file operation of a room runs on that thread: not the
/// commits and the sealing of segments, a sync from the disk tiers, an ack that prunes, the
/// roster or the disk maintenance.
#[tokio::test]
async fn no_file_operation_of_a_room_runs_on_the_runtime_thread() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        lease_timeout: Duration::from_secs(3600),
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = stopped_actor(&dir, &relay_on(&dir), policy, TEST_TIMINGS);
    let room = room_dir(dir.path(), &actor.room_id);
    fail_point::watch_threads(&room);
    let (writer, peer) = (
        ClientId::new("writer").unwrap(),
        ClientId::new("peer").unwrap(),
    );
    actor.register(&writer, None, None).unwrap();
    actor.register(&peer, Some(seq(0)), Some(1)).unwrap();
    // A segment sealed every two commits; RAM keeps the last two operations.
    let mut flow = ControlFlow::Continue(());
    for n in 1..=8u8 {
        actor
            .commit(&writer, mutation(n), seq(0), insert_op(n.into()), &mut flow)
            .await
            .unwrap();
    }
    let batch = actor.sync(&peer, seq(0), 100).await.unwrap();
    assert_eq!(batch.ops.first().map(|op| op.seq), Some(seq(1)));
    for client in [&peer, &writer] {
        actor.ack(client, seq(8)).await.unwrap();
    }
    assert!(actor.log().tail_seq() > seq(1), "the acks must prune");
    actor.run_fast_maintenance().await;
    actor.run_disk_maintenance().await;
    actor.persist_roster_at_stop().await;

    let threads = fail_point::watched_threads(&room);
    assert!(!threads.is_empty());
    assert!(
        !threads.contains(&std::thread::current().id()),
        "a file operation of the room ran on the runtime's thread"
    );
}

/// A roster write that fails, now on the file I/O threads, leaves the changes pending: the
/// next tick writes them.
#[tokio::test]
async fn a_failed_roster_write_is_retried_by_the_next_tick() {
    let dir = tempdir().unwrap();
    let mut actor = stopped_actor(
        &dir,
        &relay_on(&dir),
        RoomLifecyclePolicy::default(),
        TEST_TIMINGS,
    );
    let client = ClientId::new("client").unwrap();
    actor.register(&client, None, None).unwrap();
    let roster = room_dir(dir.path(), &actor.room_id).join("meta_clients_room.json");

    fail_point::arm("write_atomic_before_rename", &roster);
    actor.run_fast_maintenance().await;
    assert!(!roster.exists());
    actor.run_fast_maintenance().await;
    assert!(roster.exists(), "the failed write was not retried");
}

/// An ack whose prune needs the cursors on disk first prunes nothing when the roster write
/// fails, and the next one prunes once it succeeds.
#[tokio::test]
async fn an_ack_whose_roster_write_fails_prunes_nothing() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = stopped_actor(&dir, &relay_on(&dir), policy, TEST_TIMINGS);
    let writer = ClientId::new("writer").unwrap();
    actor.register(&writer, None, None).unwrap();
    let mut flow = ControlFlow::Continue(());
    for n in 1..=8u8 {
        actor
            .commit(&writer, mutation(n), seq(0), insert_op(n.into()), &mut flow)
            .await
            .unwrap();
    }
    let roster = room_dir(dir.path(), &actor.room_id).join("meta_clients_room.json");

    fail_point::arm("write_atomic_before_rename", &roster);
    actor.ack(&writer, seq(8)).await.unwrap();
    assert_eq!(
        actor.log().tail_seq(),
        seq(1),
        "pruned without the cursors on disk"
    );
    actor.ack(&writer, seq(8)).await.unwrap();
    assert!(actor.log().tail_seq() > seq(1));
}

/// The disk maintenance compresses off the runtime's thread, on the compression threads.
#[tokio::test]
async fn compression_runs_off_the_runtime_thread() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        warm_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    };
    let mut actor = stopped_actor(&dir, &relay_on(&dir), policy, TEST_TIMINGS);
    let writer = ClientId::new("writer").unwrap();
    actor.register(&writer, None, None).unwrap();
    let mut flow = ControlFlow::Continue(());
    for n in 1..=2u8 {
        actor
            .commit(&writer, mutation(n), seq(0), insert_op(n.into()), &mut flow)
            .await
            .unwrap();
    }
    let segments = room_dir(dir.path(), &actor.room_id).join("segments");
    let warm = segment_path(&segments, seq(1), seq(2), Tier::Warm);
    let (tx, rx) = std::sync::mpsc::channel();
    fail_point::arm_hook("compress_warm_segment", &warm, move || {
        tx.send(std::thread::current().id()).unwrap();
    });

    // A runtime of a single thread, this one: compressing in place would run here.
    actor.run_disk_maintenance().await;
    assert_ne!(rx.try_recv().unwrap(), std::thread::current().id());
}

#[tokio::test(start_paused = true)]
async fn disk_maintenance_waits_for_the_slow_tick() {
    let fx = Fixture::with_options(
        RoomLifecycleOverrides {
            ram_max_ops: Some(2),
            warm_disk_ttl_secs: Some(0),
            ..RoomLifecycleOverrides::default()
        },
        |_| {},
        Some(ActorTimings::default()),
    )
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    // Seals segment 1..=2, which may be compressed at once.
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &client, mutation(2), seq(1), insert_op(2))
        .await
        .unwrap();
    let segments = fx.room_dir().join("segments");
    let warm = segments.join("segment_0000000000000001_0000000000000002.wal");
    let cold = segments.join("segment_0000000000000001_0000000000000002.wal.zst");

    // The fast tick persists the cursor reported by the commit, and leaves the disk alone.
    tokio::time::sleep(FAST_TICK_PERIOD + Duration::from_millis(100)).await;
    assert_eq!(fx.persisted_cursor(&client), Some(seq(1)));
    assert!(warm.exists() && !cold.exists());

    tokio::time::sleep(SLOW_TICK_PERIOD).await;
    assert!(!warm.exists() && cold.exists());
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn reopening_hydrates_dedup_without_reading_segments() {
    let fx = Fixture::with_options(
        small_segments(),
        |config| config.dedup_lru_capacity = 3,
        None,
    )
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    // Five sealed segments of two operations; active.wal is empty.
    for n in 1..=10u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    fx.manager.shutdown_all().await;
    let segments = fx.room_dir().join("segments");
    let reads_before = io_probe::count(IoEvent::SegmentRead, &segments);

    let sender = fx.sender().await;

    // The last three mutation ids are in the sidecars of the two newest segments, which are
    // not decoded.
    assert_eq!(
        io_probe::count(IoEvent::SegmentRead, &segments) - reads_before,
        0
    );
    // Hydrated oldest first: 8 is the oldest and the first to be evicted.
    let new = commit(&sender, &client, mutation(11), seq(0), insert_op(11))
        .await
        .unwrap();
    assert_eq!(new.assigned_seq, seq(11));
    for n in [9u8, 10, 11] {
        let retry = commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
        assert_eq!(retry.assigned_seq, seq(n.into()), "retry of mutation {n}");
    }
    let evicted = commit(&sender, &client, mutation(8), seq(0), insert_op(8))
        .await
        .unwrap();
    assert_eq!(evicted.assigned_seq, seq(12));
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn disconnected_client_behind_ttl_compaction_becomes_dormant_and_is_behind_the_log() {
    let dir = tempdir().unwrap();
    let (manager, _relay) = new_manager_with(&dir, |_| {});
    // Segments of five operations, compressed after 100 ms and pruned 200 ms later.
    let manager = manager.with_default_policy(RoomLifecyclePolicy {
        lease_timeout: Duration::from_secs(1),
        ..RoomLifecyclePolicy::test_policy()
    });
    let room_id = RoomId::new("lifecycle-room").unwrap();
    manager
        .create_room(room_id.clone(), SchemaId::new("todo").unwrap(), None)
        .await
        .unwrap();
    let sender = manager.get_or_spawn(&room_id, None).await.unwrap();
    let alice = ClientId::new("alice").unwrap();
    let bob = ClientId::new("bob").unwrap();
    register(&sender, &alice).await;
    register(&sender, &bob).await;
    for n in 1..=10u8 {
        commit(
            &sender,
            &alice,
            mutation(n),
            seq(u64::from(n) - 1),
            insert_op(n.into()),
        )
        .await
        .unwrap();
    }
    ack(&sender, &alice, seq(10)).await.unwrap();

    // Bob stays at cursor 0 and goes silent. His lease expires, and TTL compaction prunes the
    // whole log regardless of his cursor, so he ends up Dormant; Alice is only Disconnected.
    let started = std::time::Instant::now();
    loop {
        let (states, tail) = (states(&sender).await, metrics(&sender).await.tail_seq);
        if states == (0, 0, 1, 1) && tail == seq(11) {
            break;
        }
        assert!(
            started.elapsed() < POLL_CEILING,
            "states {states:?}, tail {tail}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(matches!(
        sync_from(&sender, &bob, seq(0)).await,
        Err(ServerError::BehindCompaction)
    ));
    manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn rooms_that_go_idle_before_the_slow_tick_still_get_disk_maintenance() {
    let fx = Fixture::with_options(
        RoomLifecycleOverrides {
            ram_max_ops: Some(2),
            warm_disk_ttl_secs: Some(0),
            idle_timeout_secs: Some(1),
            ..RoomLifecycleOverrides::default()
        },
        |_| {},
        Some(ActorTimings::default()),
    )
    .await;
    let segments = fx.room_dir().join("segments");
    let client = ClientId::new("writer").unwrap();
    for cycle in 0..3u8 {
        let sender = fx.sender().await;
        if cycle == 0 {
            register(&sender, &client).await;
        }
        let (first, second) = (cycle * 2 + 1, cycle * 2 + 2);
        commit(
            &sender,
            &client,
            mutation(first),
            seq(0),
            insert_op(first.into()),
        )
        .await
        .unwrap();
        commit(
            &sender,
            &client,
            mutation(second),
            seq(0),
            insert_op(second.into()),
        )
        .await
        .unwrap();
        // Idle well before the first slow tick (30 s).
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(fx.manager.get_room(&fx.room_id).is_none(), "cycle {cycle}");
        let name = format!("segment_{:016}_{:016}", first, second);
        assert!(
            segments.join(format!("{name}.wal.zst")).exists(),
            "cycle {cycle}"
        );
        assert!(
            !segments.join(format!("{name}.wal")).exists(),
            "cycle {cycle}"
        );
    }
}

#[tokio::test]
async fn retries_are_recognised_after_reopening_with_a_zero_dedup_capacity() {
    let fx = Fixture::with_options(
        RoomLifecycleOverrides::default(),
        |config| config.dedup_lru_capacity = 0,
        None,
    )
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    fx.manager.shutdown_all().await;

    let sender = fx.sender().await;
    let retry = commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    assert_eq!(retry.assigned_seq, seq(1));
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn the_same_mutation_id_from_two_clients_is_two_mutations() {
    let fx = Fixture::new(small_segments()).await;
    let sender = fx.sender().await;
    let (alice, bob) = (
        ClientId::new("alice").unwrap(),
        ClientId::new("bob").unwrap(),
    );
    register(&sender, &alice).await;
    register(&sender, &bob).await;
    // (client, mutation, row and expected sequence): 1 and 2 end in a sealed segment, with
    // their ids in its sidecar, and 3 stays in active.wal.
    let commits = [(&alice, 1, 1), (&bob, 1, 2), (&alice, 3, 3)];
    for (client, n, row) in commits {
        let ack = commit(&sender, client, mutation(n), seq(0), insert_op(row))
            .await
            .unwrap();
        assert_eq!(ack.assigned_seq, seq(row as u64));
    }
    let synced = sync_all(&sender, &alice).await.unwrap();
    let ops: Vec<Operation> = synced.ops.iter().map(|op| op.op.clone()).collect();
    assert_eq!(ops, [insert_op(1), insert_op(2), insert_op(3)]);

    // Retries get their original sequences, also once the room is reopened from disk.
    for reopened in [false, true] {
        if reopened {
            fx.manager.shutdown_all().await;
        }
        let sender = fx.sender().await;
        for (client, n, row) in commits {
            let retry = commit(&sender, client, mutation(n), seq(0), insert_op(row))
                .await
                .unwrap();
            assert_eq!(retry.assigned_seq, seq(row as u64), "reopened: {reopened}");
        }
    }
    fx.manager.shutdown_all().await;
}

#[tokio::test(start_paused = true)]
async fn dormant_client_is_forgotten_after_its_ttl_and_registers_again() {
    let fx = Fixture::new(RoomLifecycleOverrides {
        forget_dormant_after_secs: Some(60),
        ..RoomLifecycleOverrides::default()
    })
    .await;
    let (gone, kept) = (
        ClientId::new("gone").unwrap(),
        ClientId::new("kept").unwrap(),
    );
    // A Dormant roster entry last seen `secs_ago` seconds ago.
    let dormant = |id: &str, secs_ago: u64| {
        let seen = std::time::UNIX_EPOCH.elapsed().unwrap() - Duration::from_secs(secs_ago);
        serde_json::json!({
            "client_id": id, "state": "Dormant", "last_ack_seq": 0,
            "last_seen_unix_ms": seen.as_millis() as u64,
        })
    };
    fx.manager.shutdown_room(&fx.room_id).await;
    let roster = serde_json::json!({ "clients": [dormant("gone", 61), dormant("kept", 30)] });
    durable::write_checked(&fx.roster_path(), roster.to_string().as_bytes()).unwrap();
    let sender = fx.sender().await;
    // A fast tick forgets the client inactive for longer than its TTL.
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(matches!(
        heartbeat(&sender, &gone).await,
        Err(ServerError::ClientNotRegistered(_))
    ));
    heartbeat(&sender, &kept).await.unwrap();
    register(&sender, &gone).await;
    fx.manager.shutdown_all().await;
}

/// Commits from three clients that report random cursors, each only moving forward as an
/// honest client's does: every catch-up is a prefix of the operations after the reported
/// cursor, and all of them when it has no more. Afterwards a `/sync` chain with random batch
/// sizes from any retained cursor returns its exact suffix, and one from behind the log gets
/// `BehindCompaction`.
#[tokio::test(start_paused = true)]
async fn commit_catchups_and_sync_chains_deliver_the_exact_suffix() {
    let fx = Fixture::new(RoomLifecycleOverrides {
        ram_max_ops: Some(3),
        warm_disk_ttl_secs: Some(0),
        ..RoomLifecycleOverrides::default()
    })
    .await;
    let sender = fx.sender().await;
    let clients: Vec<ClientId> = ["a", "b", "c"]
        .iter()
        .map(|c| ClientId::new(*c).unwrap())
        .collect();
    for c in &clients {
        register(&sender, c).await;
    }
    let mut rng = TestRng::new(0x1234_5678);
    let mut accepted: Vec<Operation> = Vec::new();
    let mut cursors = [0u64; 3];
    for n in 1..=150u64 {
        let i = rng.below(3) as usize;
        let head = accepted.len() as u64;
        let last_ack = cursors[i] + rng.below(head - cursors[i] + 1);
        cursors[i] = last_ack;
        let op = insert_op(n as i64);
        let resp = commit(
            &sender,
            &clients[i],
            MutationId::from_u128(u128::from(n)),
            seq(last_ack),
            op.clone(),
        )
        .await
        .unwrap();
        accepted.push(op);
        assert_eq!(resp.assigned_seq, seq(n));
        let expected: Vec<(u64, Operation)> = (last_ack + 1..=n)
            .map(|s| (s, accepted[(s - 1) as usize].clone()))
            .collect();
        let got: Vec<(u64, Operation)> = resp
            .catchup_ops
            .iter()
            .map(|op| (op.seq.get(), op.op.clone()))
            .collect();
        assert!(expected.starts_with(&got), "commit {n} from {last_ack}");
        if !resp.has_more {
            assert_eq!(got, expected, "commit {n}: complete catch-up");
        }
        if n % 50 == 0 {
            // Lets the slow tick compress sealed segments and prune up to the lowest cursor.
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
    }

    let reader = &clients[0];
    for c in [0u64, 1, 2, 3, 50, 99, 100, 148, 149, 150] {
        let tail = metrics(&sender).await.tail_seq.get();
        if c + 1 < tail {
            assert!(matches!(
                sync_from(&sender, reader, seq(c)).await,
                Err(ServerError::BehindCompaction)
            ));
            continue;
        }
        let mut at = c;
        let mut got = Vec::new();
        loop {
            let (tx, rx) = oneshot::channel();
            sender
                .send(RoomCommand::Sync {
                    client_id: reader.clone(),
                    from_seq: seq(at),
                    max_batch_size: 1 + rng.below(9) as u32,
                    reply: tx,
                })
                .await
                .unwrap();
            let batch = rx.await.unwrap().unwrap();
            if let Some(last) = batch.ops.last() {
                at = last.seq.get();
            }
            got.extend(batch.ops.iter().map(|op| (op.seq.get(), op.op.clone())));
            if !batch.has_more {
                break;
            }
        }
        let expected: Vec<(u64, Operation)> = (c + 1..=150)
            .map(|s| (s, accepted[(s - 1) as usize].clone()))
            .collect();
        assert_eq!(got, expected, "sync chain from {c}");
    }
    fx.manager.shutdown_all().await;
}

/// With a window of two mutations, mutations 2 and 3 are the last ones: a retry of 2 is
/// recognised before and after a respawn, although the recognised retry of 1 came later.
#[tokio::test]
async fn a_retry_inside_the_dedup_window_is_recognised_before_and_after_a_respawn() {
    let fx = Fixture::with_options(
        RoomLifecycleOverrides::default(),
        |config| config.dedup_lru_capacity = 2,
        None,
    )
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for n in 1..=2u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    // A lost acknowledgement: the client retries mutation 1.
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(&sender, &client, mutation(3), seq(0), insert_op(3))
        .await
        .unwrap();

    for respawned in [false, true] {
        if respawned {
            fx.manager.shutdown_all().await;
        }
        let sender = fx.sender().await;
        let retry = commit(&sender, &client, mutation(2), seq(0), insert_op(2))
            .await
            .unwrap();
        assert_eq!(retry.assigned_seq, seq(2), "respawned: {respawned}");
        assert_eq!(metrics(&sender).await.head_seq, seq(3));
    }
    fx.manager.shutdown_all().await;
}

/// A commit rejected with `BehindCompaction` changes neither the log, the head, the client's
/// cursor nor the deduplication window, and never announces a new head. It may only make the
/// room ask for a snapshot.
#[tokio::test(start_paused = true)]
async fn commit_rejected_behind_the_log_leaves_no_trace_but_may_ask_for_a_snapshot() {
    let (fx, writer, tail) = pruned_room().await;
    let behind = seq(tail.get() - 2);
    let stale = ClientId::new("stale").unwrap();
    let sender = fx
        .restart_with_roster(&[
            (&writer, "Connected", 8),
            (&stale, "Connected", behind.get()),
        ])
        .await;
    let mut events = subscribe(&sender).await;
    let before = metrics(&sender).await;
    let retained = sync_from(&sender, &writer, seq(tail.get() - 1))
        .await
        .unwrap()
        .ops;

    let res = commit(&sender, &stale, mutation(42), behind, insert_op(42)).await;
    assert!(matches!(res, Err(ServerError::BehindCompaction)), "{res:?}");

    let after = metrics(&sender).await;
    assert_eq!(
        (after.tail_seq, after.head_seq),
        (before.tail_seq, before.head_seq)
    );
    let still_retained = sync_from(&sender, &writer, seq(tail.get() - 1))
        .await
        .unwrap()
        .ops;
    assert_eq!(still_retained, retained);
    assert_eq!(cursor(&sender, &stale).await, Some(behind));
    let mut emitted = Vec::new();
    while let Ok(event) = events.try_recv() {
        emitted.push(event);
    }
    assert!(
        emitted
            .iter()
            .all(|event| matches!(event, RoomEvent::SnapshotWanted)),
        "{emitted:?}"
    );

    // The rejected mutation id was not recorded: once another commit took the next sequence,
    // the same id sent again with a cursor inside the log is a new mutation.
    let next = commit(&sender, &writer, mutation(43), seq(8), insert_op(43))
        .await
        .unwrap();
    assert_eq!(next.assigned_seq, before.head_seq.next());
    let accepted = commit(&sender, &stale, mutation(42), seq(8), insert_op(42))
        .await
        .unwrap();
    assert_eq!(accepted.assigned_seq, next.assigned_seq.next());
    fx.manager.shutdown_all().await;
}

/// Between two registrations a client's cursor never moves back, whatever lower cursor an ack,
/// a sync, a commit or a deduplicated retry reports, and a heartbeat does not move it. A
/// registration redefines it: to the cursor it reports, or to 0 without one.
#[tokio::test(start_paused = true)]
async fn cursor_never_moves_back_between_registrations_and_a_registration_redefines_it() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    for n in 1..=5u8 {
        commit(&sender, &client, mutation(n), seq(0), insert_op(n.into()))
            .await
            .unwrap();
    }
    ack(&sender, &client, seq(4)).await.unwrap();
    assert_eq!(cursor(&sender, &client).await, Some(seq(4)));

    ack(&sender, &client, seq(2)).await.unwrap();
    sync_from(&sender, &client, seq(1)).await.unwrap();
    commit(&sender, &client, mutation(6), seq(3), insert_op(6))
        .await
        .unwrap();
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    heartbeat(&sender, &client).await.unwrap();
    assert_eq!(cursor(&sender, &client).await, Some(seq(4)));

    register_at(&sender, &client, Some((seq(1), fx.incarnation))).await;
    assert_eq!(cursor(&sender, &client).await, Some(seq(1)));
    register(&sender, &client).await;
    assert_eq!(cursor(&sender, &client).await, Some(seq(0)));

    ack(&sender, &client, seq(3)).await.unwrap();
    ack(&sender, &client, seq(2)).await.unwrap();
    assert_eq!(cursor(&sender, &client).await, Some(seq(3)));
    fx.manager.shutdown_all().await;
}

/// Onboarding under disruption: writers commit, sync and ack; newcomers register behind the
/// log and wait for a snapshot; designees upload snapshots in one request or in chunks (some
/// corrupt, abandoned or with a wrong hash) or go away; rooms restart, crash and lose their
/// relay; relay snapshots expire. Optionally, sealed segments disappear (gaps) and TTL or quota
/// compaction prunes a prefix regardless of the retention anchors.
///
/// Checked throughout: the tail never moves back, cursor-driven pruning never deletes what a
/// connected or disconnected client, a usable snapshot or an upload in progress needs, only
/// usable snapshots are announced, a download assembles exactly one uploaded snapshot, and
/// every sync continues its cursor exactly. Liveness: no client waits for a snapshot longer
/// than a bounded number of ticks, and once disruptions stop every client catches up.
mod onboarding_sim {
    use super::*;
    use crate::relay::{LogBounds, SnapshotChunkUpload, Uploader, MIN_CHUNK_BYTES};
    use std::collections::HashMap;
    use zemdb_core::id::CorrelationId;
    use zemdb_core::protocol::messages::ServerMessage;
    use zemdb_core::protocol::snapshot_envelope::{
        SnapshotCompression, SnapshotEnvelopeHeader, SNAPSHOT_HEADER_LEN,
    };

    #[derive(Debug)]
    enum Phase {
        Live,
        NeedSnapshot {
            since: u64,
        },
        Downloading {
            since: u64,
            seq: u64,
            hash: [u8; 32],
            total: u32,
            next: u32,
            bytes: Vec<u8>,
        },
    }

    #[derive(Debug)]
    struct ChunkedUpload {
        seq: u64,
        data: Vec<u8>,
        hash: [u8; 32],
        next: u32,
        abandon_at: Option<u32>,
    }

    struct Client {
        id: ClientId,
        cursor: u64,
        phase: Phase,
        upload: Option<ChunkedUpload>,
        away_until: u64,
        writer: bool,
    }

    /// What a run went through, reported when an assertion fails.
    #[derive(Default, Debug)]
    struct Stats {
        ticks: u64,
        commits: u64,
        uploads_ok: u64,
        uploads_failed: u64,
        uploads_abandoned: u64,
        downloads_ok: u64,
        downloads_superseded: u64,
        restore_behind: u64,
        catch_ups: u64,
        max_catch_up_ticks: u64,
        restarts: u64,
        crashes: u64,
        server_restarts: u64,
        ttl_events: u64,
        gaps: u64,
        designee_away: u64,
        newcomer_away: u64,
        prune_checks: u64,
        prunes: u64,
        demand_expired_while_waiting: u64,
    }

    const SEEDS: RangeInclusive<u64> = 1..=3;
    /// Ticks of disruptions per seed.
    const TICKS: u64 = 600;
    const LEASE: Duration = Duration::from_secs(30);
    const TOKIO_STEP: Duration = Duration::from_secs(4);
    /// The TTL of the snapshot demand and the time a designee has to start uploading: ten
    /// ticks.
    const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(40);
    /// Longest a client may wait for a snapshot while disruptions go on.
    const MAX_WAIT_TICKS: u64 = 400;
    /// Ticks without disruptions that every client gets to catch up at the end.
    const QUIET_TICKS: u64 = 300;

    struct Sim {
        dir: TempDir,
        room_id: RoomId,
        incarnation: u128,
        relay: Option<Arc<SnapshotRelay>>,
        actor: Option<RoomActor>,
        policy: RoomLifecyclePolicy,
        rng: TestRng,
        clients: Vec<Client>,
        uploaded: HashMap<[u8; 32], Vec<u8>>,
        next_mutation: u64,
        tick: u64,
        max_tail: u64,
        /// The ranges of the segments deleted to inject gaps.
        lost: Vec<(u64, u64)>,
        stats: Stats,
        gap_injection: bool,
        ttl_injection: bool,
    }

    fn envelope(body: Vec<u8>) -> Vec<u8> {
        let header =
            SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
        let mut data = header.to_bytes().to_vec();
        data.extend_from_slice(&body);
        data
    }

    /// The range of operations of the segment file `name`
    /// (`segment_{first}_{last}.wal`, or `.wal.zst`).
    fn segment_range(name: &str) -> (u64, u64) {
        let stem = name.strip_prefix("segment_").unwrap();
        let (first, rest) = stem.split_once('_').unwrap();
        let last = rest.split_once('.').unwrap().0;
        (first.parse().unwrap(), last.parse().unwrap())
    }

    impl Sim {
        fn new(seed: u64, gap_injection: bool, ttl_injection: bool) -> Self {
            let dir = tempdir().unwrap();
            let relay = Self::new_relay(dir.path());
            let policy = RoomLifecyclePolicy {
                ram_max_ops: 3,
                warm_disk_ttl: Duration::ZERO,
                cold_disk_ttl: Duration::from_secs(10 * 365 * 24 * 3600),
                lease_timeout: LEASE,
                snapshot_demand_ttl: SNAPSHOT_DEADLINE,
                ..RoomLifecyclePolicy::default()
            };
            let mut sim = Self {
                dir,
                room_id: RoomId::new("simroom").unwrap(),
                incarnation: 42,
                relay: Some(relay),
                actor: None,
                policy,
                rng: TestRng::new(seed),
                clients: Vec::new(),
                uploaded: HashMap::new(),
                next_mutation: 1,
                tick: 0,
                max_tail: 0,
                lost: Vec::new(),
                stats: Stats::default(),
                gap_injection,
                ttl_injection,
            };
            sim.open_actor();
            sim
        }

        fn new_relay(dir: &std::path::Path) -> Arc<SnapshotRelay> {
            Arc::new(
                SnapshotRelay::new(
                    dir.join("snapshots"),
                    Duration::from_secs(2000),
                    ServerConfig::default().max_snapshot_bytes,
                    Duration::from_secs(ServerConfig::default().snapshot_upload_max_secs),
                )
                .unwrap(),
            )
        }

        fn open_actor(&mut self) {
            let config = Arc::new(ServerConfig {
                data_dir: self.dir.path().to_path_buf(),
                ..ServerConfig::default()
            });
            let (_sender, actor) = RoomActor::open(
                self.room_id.clone(),
                self.incarnation,
                SchemaId::new("todo").unwrap(),
                Arc::new(test_schema()),
                self.dir.path(),
                config,
                self.policy.clone(),
                Arc::clone(self.relay.as_ref().unwrap()),
                Arc::new(InflightBudget::new(u64::MAX, u64::MAX)),
                Arc::new(OpsEventBudget::new(u64::MAX)),
                ActorTimings {
                    designation_timeout: SNAPSHOT_DEADLINE,
                    ..TEST_TIMINGS
                },
                IoThreads::new(2),
            )
            .unwrap();
            self.actor = Some(actor);
            self.check_tail();
        }

        fn a(&mut self) -> &mut RoomActor {
            self.actor.as_mut().unwrap()
        }

        fn relay(&self) -> Arc<SnapshotRelay> {
            Arc::clone(self.relay.as_ref().unwrap())
        }

        fn bounds(&mut self) -> LogBounds {
            let a = self.a();
            LogBounds {
                tail_seq: a.log().tail_seq(),
                head_seq: a.head_seq,
                unrestorable_through: a.unrestorable_through,
                room_incarnation: a.incarnation,
            }
        }

        /// The tail never moves back, not even across a reopen.
        fn check_tail(&mut self) {
            let tail = self.actor.as_ref().unwrap().log().tail_seq().get();
            assert!(
                tail >= self.max_tail,
                "the tail moved back from {} to {tail} (tick {})",
                self.max_tail,
                self.tick
            );
            self.max_tail = tail;
        }

        /// The tail and the usable snapshot before a call that may prune the log.
        fn before_prune(&mut self) -> (u64, Option<u64>) {
            let a = self.a();
            let snapshot = a.usable_snapshot_seq().map(|s| s.get());
            (a.log().tail_seq().get(), snapshot)
        }

        /// The lowest sequence that cursor-driven pruning must keep: the cursors of connected
        /// and disconnected clients inside the log after the call (the new cursor of a client
        /// that acked included), the snapshot that was usable before it and an upload in
        /// progress.
        fn protected(&mut self, tail_before: u64, snapshot_before: Option<u64>) -> Option<u64> {
            let ids: Vec<ClientId> = self.clients.iter().map(|c| c.id.clone()).collect();
            let a = self.actor.as_ref().unwrap();
            let mut floor = snapshot_before;
            let mut take = |v: u64| floor = Some(floor.map_or(v, |f: u64| f.min(v)));
            for id in &ids {
                if let Some(entry) = a.lease_tracker.get_client(id) {
                    let c = entry.last_ack_seq.get();
                    let inside = c + 1 >= tail_before;
                    if inside
                        && matches!(
                            entry.state,
                            ClientState::Connected | ClientState::Disconnected
                        )
                    {
                        take(c);
                    }
                }
            }
            if let Some(u) = a.snapshot_relay.upload_in_progress(&a.room_id) {
                if u.get() + 1 >= tail_before {
                    take(u.get());
                }
            }
            floor
        }

        /// A prune by `what`, which started from `before` ([`before_prune`](Self::before_prune)),
        /// deleted nothing past what someone needs. The tail may still move past it over
        /// operations whose segment was lost before the prune.
        fn check_prune(&mut self, before: (u64, Option<u64>), what: &str) {
            let (tail_before, snapshot_before) = before;
            self.stats.prune_checks += 1;
            let tail_after = self.a().log().tail_seq().get();
            if tail_after == tail_before {
                return;
            }
            self.stats.prunes += 1;
            if let Some(p) = self.protected(tail_before, snapshot_before) {
                let deleted = (tail_before.max(p + 1)..tail_after).find(|s| {
                    !self
                        .lost
                        .iter()
                        .any(|&(start, end)| (start..=end).contains(s))
                });
                assert!(
                    deleted.is_none(),
                    "{what} pruned to tail {tail_after}, deleting {deleted:?} past the protected \
                     {p} (tick {})",
                    self.tick
                );
            }
            self.check_tail();
        }

        /// An announced snapshot is inside the retained range and not below a known gap.
        fn check_announced(&mut self, s: Option<SequenceNumber>, what: &str) {
            if let Some(s) = s {
                let b = self.bounds();
                assert!(
                    b.tail_seq.get() <= s.get() + 1 && s <= b.head_seq,
                    "{what} announced the unusable snapshot {s} (tail {}, head {})",
                    b.tail_seq,
                    b.head_seq
                );
                assert!(
                    self.a().unrestorable_through < Some(s),
                    "{what} announced a snapshot below a known gap"
                );
            }
        }

        fn mutation(&mut self) -> MutationId {
            let n = self.next_mutation;
            self.next_mutation += 1;
            MutationId::from_u128(u128::from(n))
        }

        async fn add_client(&mut self, name: String, writer: bool) {
            let id = ClientId::new(name).unwrap();
            let inc = self.incarnation;
            let reg = self.a().register(&id, None, Some(inc)).unwrap();
            self.check_announced(reg.active_snapshot_seq, "Registered");
            let state = self.a().lease_tracker.get_client(&id).unwrap().state;
            let tick = self.tick;
            let phase = if state == ClientState::Connected {
                Phase::Live
            } else {
                Phase::NeedSnapshot { since: tick }
            };
            self.clients.push(Client {
                id,
                cursor: 0,
                phase,
                upload: None,
                away_until: 0,
                writer,
            });
            let i = self.clients.len() - 1;
            if matches!(self.clients[i].phase, Phase::Live) {
                self.catch_up(i).await;
            }
        }

        /// Syncs client `i` from its cursor to the head. Returns whether it got there (false if
        /// it fell behind the log) and whether some reply asked it for a snapshot.
        async fn catch_up(&mut self, i: usize) -> (bool, bool) {
            let mut wanted = false;
            for _ in 0..10_000 {
                let (id, cursor) = (self.clients[i].id.clone(), self.clients[i].cursor);
                match self.a().sync(&id, seq(cursor), 100).await {
                    Ok(batch) => {
                        wanted |= batch.snapshot_wanted;
                        for (k, op) in batch.ops.iter().enumerate() {
                            assert_eq!(op.seq.get(), cursor + 1 + k as u64, "gap in a sync");
                        }
                        if let Some(last) = batch.ops.last() {
                            self.clients[i].cursor = last.seq.get();
                        }
                        if !batch.has_more {
                            return (true, wanted);
                        }
                    }
                    Err(ServerError::BehindCompaction) => return (false, wanted),
                    Err(ServerError::ClientNotRegistered(_)) => {
                        self.reregister(i);
                        return (matches!(self.clients[i].phase, Phase::Live), wanted);
                    }
                    Err(e) => panic!("sync failed: {e:?}"),
                }
            }
            panic!("sync did not converge");
        }

        fn reregister(&mut self, i: usize) {
            let id = self.clients[i].id.clone();
            let inc = self.incarnation;
            let cursor = match self.clients[i].phase {
                Phase::Live => Some(seq(self.clients[i].cursor)),
                _ => None,
            };
            let reg = self.a().register(&id, cursor, cursor.map(|_| inc)).unwrap();
            self.check_announced(reg.active_snapshot_seq, "Registered");
            let state = self.a().lease_tracker.get_client(&id).unwrap().state;
            if state == ClientState::Bootstrapping && matches!(self.clients[i].phase, Phase::Live) {
                let tick = self.tick;
                self.clients[i].phase = Phase::NeedSnapshot { since: tick };
            }
        }

        fn fall_behind(&mut self, i: usize) {
            let tick = self.tick;
            self.clients[i].phase = Phase::NeedSnapshot { since: tick };
            self.clients[i].upload = None;
        }

        async fn step_live(&mut self, i: usize) {
            let id = self.clients[i].id.clone();
            let mut wanted = false;
            match self.a().heartbeat(&id) {
                Ok(hb) => {
                    wanted |= hb.snapshot_wanted;
                    self.check_announced(hb.active_snapshot_seq, "HeartbeatAck");
                }
                Err(ServerError::ClientNotRegistered(_)) => {
                    self.reregister(i);
                    if !matches!(self.clients[i].phase, Phase::Live) {
                        return;
                    }
                }
                Err(e) => panic!("heartbeat: {e:?}"),
            }
            // Writers commit more often than readers.
            let commit_p = if self.clients[i].writer { 300 } else { 50 };
            if self.rng.permille(commit_p) {
                let mid = self.mutation();
                let cursor = self.clients[i].cursor;
                let op = insert_op((self.next_mutation % 50) as i64);
                let mut flow = ControlFlow::Continue(());
                match self.a().commit(&id, mid, seq(cursor), op, &mut flow).await {
                    Ok(resp) => {
                        self.stats.commits += 1;
                        wanted |= resp.snapshot_wanted;
                        for (k, op) in resp.catchup_ops.iter().enumerate() {
                            assert_eq!(op.seq.get(), cursor + 1 + k as u64, "gap in a catch-up");
                        }
                        if let Some(last) = resp.catchup_ops.last() {
                            self.clients[i].cursor = last.seq.get();
                        }
                    }
                    Err(ServerError::BehindCompaction) => {}
                    Err(ServerError::ClientNotRegistered(_)) => {
                        self.reregister(i);
                        return;
                    }
                    Err(e) => panic!("commit: {e:?}"),
                }
                assert!(flow.is_continue());
            }
            let (ok, w) = self.catch_up(i).await;
            wanted |= w;
            if !ok {
                self.fall_behind(i);
                return;
            }
            // The ack prunes the log proactively.
            let before = self.before_prune();
            let cursor = self.clients[i].cursor;
            match self.a().ack(&id, seq(cursor)).await {
                Ok(_) => self.check_prune(before, "an ack"),
                Err(ServerError::BehindCompaction) => {
                    self.fall_behind(i);
                    return;
                }
                Err(ServerError::ClientNotRegistered(_)) => self.reregister(i),
                Err(e) => panic!("ack: {e:?}"),
            }
            // A designee may go away instead of uploading.
            if self.clients[i].upload.is_some() {
                self.continue_upload(i).await;
            } else if wanted {
                let id = self.clients[i].id.clone();
                if self.rng.permille(200) && self.live_writers_present(&id) {
                    self.stats.designee_away += 1;
                    let t = 10 + self.rng.below(50);
                    self.send_away(i, t);
                    return;
                }
                self.start_upload(i).await;
            }
        }

        async fn start_upload(&mut self, i: usize) {
            let s = self.clients[i].cursor;
            if s == 0 {
                return;
            }
            let tag = self.rng.next_u64();
            if self.rng.permille(500) {
                // In a single request, one in five corrupt.
                let mut data = envelope(format!("{s}:{tag}").into_bytes());
                if self.rng.permille(200) {
                    let last = data.len() - 1;
                    data[last] ^= 0xff;
                }
                let bounds = self.bounds();
                let room = self.room_id.clone();
                let uploader = Uploader::Client(self.clients[i].id.clone());
                let res = self
                    .relay()
                    .stage_snapshot(
                        &room,
                        &uploader,
                        seq(s),
                        bytes::Bytes::from(data.clone()),
                        async { Ok(bounds) },
                    )
                    .await;
                match res {
                    Ok(hash) => {
                        self.stats.uploads_ok += 1;
                        self.uploaded.insert(hash, data);
                    }
                    Err(_) => self.stats.uploads_failed += 1,
                }
            } else {
                let mut body = vec![0u8; 200 * 1024 - SNAPSHOT_HEADER_LEN];
                body[..16].copy_from_slice(&[s.to_le_bytes(), tag.to_le_bytes()].concat());
                let data = envelope(body);
                let mut hash = ServerMessage::compute_snapshot_hash(&data);
                if self.rng.permille(200) {
                    // A wrong hash: rejected when the upload completes.
                    hash[0] ^= 1;
                }
                let abandon_at = self.rng.permille(250).then(|| 1 + self.rng.below(2) as u32);
                self.clients[i].upload = Some(ChunkedUpload {
                    seq: s,
                    data,
                    hash,
                    next: 0,
                    abandon_at,
                });
                self.continue_upload(i).await;
            }
        }

        async fn continue_upload(&mut self, i: usize) {
            let id = self.clients[i].id.clone();
            let Some(up) = self.clients[i].upload.as_mut() else {
                return;
            };
            if up.next >= 3 {
                // Every chunk was sent, but the session that held the first ones was lost
                // (expired or replaced): the last chunk did not complete it.
                self.stats.uploads_failed += 1;
                self.clients[i].upload = None;
                return;
            }
            if up.abandon_at == Some(up.next) {
                // The uploader keeps running but never sends the rest.
                self.stats.uploads_abandoned += 1;
                self.clients[i].upload = None;
                return;
            }
            let chunk_len = up.data.len().div_ceil(3);
            let start = up.next as usize * chunk_len;
            let end = (start + chunk_len).min(up.data.len());
            let chunk = SnapshotChunkUpload {
                room_id: self.room_id.clone(),
                uploader: Uploader::Client(id),
                head_seq: seq(up.seq),
                chunk_index: up.next,
                total_chunks: 3,
                total_bytes: up.data.len() as u64,
                snapshot_hash: up.hash,
                data: bytes::Bytes::copy_from_slice(&up.data[start..end]),
            };
            let bounds = self.bounds();
            let res = self.relay().stage_chunk(chunk, async { Ok(bounds) }).await;
            let up = self.clients[i].upload.as_mut().unwrap();
            match res {
                Ok(true) => {
                    self.stats.uploads_ok += 1;
                    let up = self.clients[i].upload.take().unwrap();
                    self.uploaded.insert(up.hash, up.data);
                }
                Ok(false) => up.next += 1,
                Err(_) => {
                    self.stats.uploads_failed += 1;
                    self.clients[i].upload = None;
                }
            }
        }

        async fn step_bootstrapping(&mut self, i: usize) {
            let id = self.clients[i].id.clone();
            if let Phase::NeedSnapshot { .. } = self.clients[i].phase {
                let hb = match self.a().heartbeat(&id) {
                    Ok(hb) => hb,
                    Err(ServerError::ClientNotRegistered(_)) => {
                        self.reregister(i);
                        return;
                    }
                    Err(e) => panic!("heartbeat: {e:?}"),
                };
                self.check_announced(hb.active_snapshot_seq, "HeartbeatAck");
                if hb.active_snapshot_seq.is_none() {
                    return;
                }
            }
            let since = match self.clients[i].phase {
                Phase::NeedSnapshot { since } | Phase::Downloading { since, .. } => since,
                Phase::Live => unreachable!(),
            };
            let (index, anchor) = match &self.clients[i].phase {
                Phase::Downloading { next, hash, .. } => (*next, Some(*hash)),
                _ => (0, None),
            };
            let bounds = self.bounds();
            let room = self.room_id.clone();
            let res = self
                .relay()
                .get_chunk(
                    CorrelationId::new(1),
                    &room,
                    index,
                    MIN_CHUNK_BYTES,
                    anchor,
                    async { Ok(bounds) },
                )
                .await;
            match res {
                Ok(ServerMessage::SnapshotChunk {
                    snapshot_head_seq,
                    total_chunks,
                    snapshot_hash,
                    data,
                    ..
                }) => {
                    if index == 0 {
                        let b = self.bounds();
                        assert!(
                            b.tail_seq.get() <= snapshot_head_seq.get() + 1
                                && snapshot_head_seq <= b.head_seq,
                            "chunk 0 served a snapshot outside the retained range"
                        );
                        assert!(
                            self.a().unrestorable_through < Some(snapshot_head_seq),
                            "chunk 0 served a snapshot below a known gap"
                        );
                        self.clients[i].phase = Phase::Downloading {
                            since,
                            seq: snapshot_head_seq.get(),
                            hash: snapshot_hash,
                            total: total_chunks,
                            next: 0,
                            bytes: Vec::new(),
                        };
                    }
                    let Phase::Downloading {
                        seq: s,
                        hash,
                        total,
                        next,
                        bytes,
                        ..
                    } = &mut self.clients[i].phase
                    else {
                        unreachable!()
                    };
                    assert_eq!(snapshot_hash, *hash, "a chunk of another snapshot");
                    assert_eq!(snapshot_head_seq.get(), *s);
                    bytes.extend_from_slice(&data);
                    *next += 1;
                    if *next == *total {
                        // The assembled bytes are exactly one uploaded snapshot.
                        assert_eq!(&ServerMessage::compute_snapshot_hash(bytes), hash);
                        assert_eq!(
                            self.uploaded.get(hash),
                            Some(&*bytes),
                            "a download mixed the bytes of two snapshots"
                        );
                        let s = *s;
                        self.stats.downloads_ok += 1;
                        self.clients[i].cursor = s;
                        self.clients[i].phase = Phase::Live;
                        let (ok, _) = self.catch_up(i).await;
                        if ok {
                            let took = self.tick - since;
                            self.stats.catch_ups += 1;
                            self.stats.max_catch_up_ticks = self.stats.max_catch_up_ticks.max(took);
                        } else {
                            self.stats.restore_behind += 1;
                            self.clients[i].phase = Phase::NeedSnapshot { since };
                        }
                    }
                }
                Ok(other) => panic!("unexpected {other:?}"),
                Err(ServerError::SnapshotSuperseded(_)) | Err(ServerError::RoomNotFound(_)) => {
                    if anchor.is_some() {
                        self.stats.downloads_superseded += 1;
                    }
                    self.clients[i].phase = Phase::NeedSnapshot { since };
                }
                Err(e) => panic!("get_chunk: {e:?}"),
            }
        }

        fn live_writers_present(&self, except: &ClientId) -> bool {
            self.clients.iter().any(|c| {
                &c.id != except && matches!(c.phase, Phase::Live) && c.away_until <= self.tick
            })
        }

        /// Client `i` stops for `ticks` ticks, its lease already expired.
        fn send_away(&mut self, i: usize, ticks: u64) {
            self.clients[i].away_until = self.tick + ticks;
            self.clients[i].upload = None;
            let id = self.clients[i].id.clone();
            if let Some(entry) = self.a().lease_tracker.get_client_mut(&id) {
                entry.last_heartbeat = Instant::now()
                    .checked_sub(LEASE + Duration::from_secs(1))
                    .unwrap();
            }
        }

        async fn events(&mut self, quiet: bool) {
            if quiet {
                return;
            }
            if self.rng.permille(20) {
                self.stats.restarts += 1;
                let mut a = self.actor.take().unwrap();
                a.persist_roster_at_stop().await;
                drop(a);
                self.open_actor();
            }
            if self.rng.permille(10) {
                self.stats.crashes += 1;
                drop(self.actor.take());
                self.open_actor();
            }
            if self.rng.permille(5) {
                self.stats.server_restarts += 1;
                drop(self.actor.take());
                self.relay = None;
                self.relay = Some(Self::new_relay(self.dir.path()));
                // Uploads in progress were lost.
                for c in &mut self.clients {
                    c.upload = None;
                }
                self.open_actor();
            }
            if self.rng.permille(25) {
                // The relay snapshot expires, and with it every deadline of the room: the
                // snapshot demand, the designation, the RAM window and the leases.
                tokio::time::advance(Duration::from_secs(2000)).await;
            }
            if self.ttl_injection && self.rng.permille(15) {
                // TTL or quota compaction: prunes a prefix regardless of the retention anchors.
                self.stats.ttl_events += 1;
                let head = self.a().head_seq.get();
                let target = seq(head.saturating_sub(2));
                let _ = self.a().log_mut().prune_older_than(target);
                self.check_tail();
            }
            if self.gap_injection && self.rng.permille(15) {
                // A sealed segment other than the oldest and the newest disappears.
                let segments = self.dir.path().join("rooms/simroom/segments");
                let mut names: Vec<String> = fs::read_dir(&segments)
                    .unwrap()
                    .filter_map(|e| e.ok()?.file_name().into_string().ok())
                    .filter(|n| {
                        n.ends_with(".wal") && n.starts_with("segment_") || n.ends_with(".wal.zst")
                    })
                    .collect();
                names.sort();
                if names.len() >= 3 {
                    let victim = &names[1 + self.rng.below(names.len() as u64 - 2) as usize];
                    if fs::remove_file(segments.join(victim)).is_ok() {
                        self.lost.push(segment_range(victim));
                    }
                    self.stats.gaps += 1;
                }
            }
            // The designee goes away.
            if self.rng.permille(30) {
                if let Some(d) = self.a().snapshot_demand.designee().cloned() {
                    if self.live_writers_present(&d) {
                        if let Some(i) = self.clients.iter().position(|c| c.id == d) {
                            self.stats.designee_away += 1;
                            let t = 10 + self.rng.below(50);
                            self.send_away(i, t);
                        }
                    }
                }
            }
            // A waiting newcomer goes away; its demand can expire meanwhile.
            if self.rng.permille(60) {
                let waiting: Vec<usize> = (0..self.clients.len())
                    .filter(|&i| {
                        !self.clients[i].writer
                            && !matches!(self.clients[i].phase, Phase::Live)
                            && self.clients[i].away_until <= self.tick
                    })
                    .collect();
                if !waiting.is_empty() {
                    let i = waiting[self.rng.below(waiting.len() as u64) as usize];
                    self.stats.newcomer_away += 1;
                    let t = 30 + self.rng.below(50);
                    self.send_away(i, t);
                }
            }
            if self.rng.permille(25) && self.clients.len() < 16 {
                let n = self.clients.len();
                self.add_client(format!("newcomer-{n}"), false).await;
            }
        }

        async fn run_tick(&mut self, quiet: bool) {
            self.tick += 1;
            self.stats.ticks += 1;
            self.events(quiet).await;
            let mut order: Vec<usize> = (0..self.clients.len()).collect();
            for k in (1..order.len()).rev() {
                let j = self.rng.below(k as u64 + 1) as usize;
                order.swap(k, j);
            }
            for i in order {
                if self.clients[i].away_until > self.tick {
                    continue;
                }
                match self.clients[i].phase {
                    Phase::Live => self.step_live(i).await,
                    _ => self.step_bootstrapping(i).await,
                }
            }
            self.a().run_fast_maintenance().await;
            if !self.waiting().is_empty()
                && !self.a().snapshot_demand.is_active()
                && self.a().usable_snapshot_seq().is_none()
            {
                self.stats.demand_expired_while_waiting += 1;
            }
            if self.tick % 3 == 0 {
                let before = self.before_prune();
                self.a().run_disk_maintenance().await;
                self.check_prune(before, "the slow tick");
            }
            self.check_tail();
            tokio::time::advance(TOKIO_STEP).await;
        }

        /// The clients waiting for a snapshot, with how many ticks they have waited.
        fn waiting(&self) -> Vec<(String, u64)> {
            self.clients
                .iter()
                .filter_map(|c| match c.phase {
                    Phase::NeedSnapshot { since } | Phase::Downloading { since, .. } => {
                        Some((c.id.to_string(), self.tick - since))
                    }
                    Phase::Live => None,
                })
                .collect()
        }
    }

    /// Runs [`TICKS`] ticks of disruptions and then a quiet phase for each of [`SEEDS`]. The
    /// runs together must complete snapshot downloads and inject the enabled disruptions, so
    /// that a change in the trajectories cannot leave the simulation checking nothing.
    ///
    /// The seeds fix every random choice, and the deadlines of the room run on the Tokio
    /// clock, which only the ticks and the events move. Only what a reopen restores (the
    /// clients' inactivity and the snapshot demand) also counts the few milliseconds of wall
    /// time the run took, far below a tick.
    async fn run(gaps: bool, ttl: bool) {
        let mut exercised = [0; 3];
        for seed in SEEDS {
            let mut sim = Sim::new(seed, gaps, ttl);
            sim.add_client("writer-a".into(), true).await;
            sim.add_client("writer-b".into(), true).await;
            for _ in 0..TICKS {
                sim.run_tick(false).await;
                for (id, waited) in sim.waiting() {
                    assert!(
                        waited <= MAX_WAIT_TICKS,
                        "{id} waited {waited} ticks for a snapshot (seed {seed}, tick {}): {:?}",
                        sim.tick,
                        sim.stats
                    );
                }
            }
            for _ in 0..QUIET_TICKS {
                for c in &mut sim.clients {
                    c.away_until = c.away_until.min(sim.tick);
                }
                sim.run_tick(true).await;
                if sim.waiting().is_empty() {
                    break;
                }
            }
            assert!(
                sim.waiting().is_empty(),
                "clients never caught up (seed {seed}): {:?} {:?}",
                sim.waiting(),
                sim.stats
            );
            exercised[0] += sim.stats.downloads_ok;
            exercised[1] += sim.stats.gaps;
            exercised[2] += sim.stats.ttl_events;
        }
        let [downloads, lost_segments, ttl_prunes] = exercised;
        assert!(
            downloads > 0 && (!gaps || lost_segments > 0) && (!ttl || ttl_prunes > 0),
            "the simulation exercised too little: {downloads} downloads, {lost_segments} lost \
             segments, {ttl_prunes} TTL prunes"
        );
    }

    // Every tick syncs files to disk, so even a short run takes several seconds: these tests
    // only run on demand.

    #[tokio::test(start_paused = true)]
    #[ignore = "long-running randomized test; run with --ignored in release"]
    async fn clients_that_fall_behind_catch_up_through_restarts_and_crashes() {
        run(false, false).await;
    }

    #[tokio::test(start_paused = true)]
    #[ignore = "long-running randomized test; run with --ignored in release"]
    async fn clients_that_fall_behind_catch_up_despite_lost_segments() {
        run(true, false).await;
    }

    #[tokio::test(start_paused = true)]
    #[ignore = "long-running randomized test; run with --ignored in release"]
    async fn clients_that_fall_behind_catch_up_despite_ttl_pruning() {
        run(false, true).await;
    }

    #[tokio::test(start_paused = true)]
    #[ignore = "long-running randomized test; run with --ignored in release"]
    async fn clients_that_fall_behind_catch_up_despite_lost_segments_and_ttl_pruning() {
        run(true, true).await;
    }
}

async fn reload(sender: &mpsc::Sender<RoomCommand>, schema_id: &str, schema: Schema) {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::ReloadSchema {
            schema_id: SchemaId::new(schema_id).unwrap(),
            schema: Arc::new(schema),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

async fn schema_of(sender: &mpsc::Sender<RoomCommand>) -> (SchemaId, Arc<Schema>) {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetSchema { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap().unwrap()
}

#[tokio::test]
async fn a_room_reloads_only_its_own_schema() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let (_, original) = schema_of(&sender).await;
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::SubscribeEvents { reply: tx })
        .await
        .unwrap();
    let mut events = rx.await.unwrap().unwrap();
    let mut evolved = test_schema();
    evolved
        .add_column(
            "tasks",
            zemdb_core::schema::ColumnDef::new("tag", zemdb_core::value::DataType::String)
                .nullable(true),
        )
        .unwrap();

    // A reload of another schema is ignored: the room keeps the one it was opened with, and
    // tells no one of a reload.
    reload(&sender, "other", evolved.clone()).await;
    let (id, schema) = schema_of(&sender).await;
    assert_eq!(id, SchemaId::new("todo").unwrap());
    assert!(Arc::ptr_eq(&schema, &original));
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    // A reload of its own schema takes effect.
    reload(&sender, "todo", evolved.clone()).await;
    let (id, schema) = schema_of(&sender).await;
    assert_eq!(id, SchemaId::new("todo").unwrap());
    assert_eq!(*schema, evolved);
    assert_eq!(
        events.try_recv().unwrap(),
        RoomEvent::SchemaReloaded(SchemaId::new("todo").unwrap())
    );
    fx.manager.shutdown_all().await;
}

// The group commit: the commits waiting in the mailbox share one fsync.

impl Fixture {
    fn active_wal(&self) -> PathBuf {
        self.room_dir().join("segments").join("active.wal")
    }

    /// How many times the room synced `active.wal` after appending records.
    fn fsyncs(&self) -> usize {
        io_probe::count(IoEvent::ActiveWalSync, &self.room_dir().join("segments"))
    }
}

type CommitReply = oneshot::Receiver<Result<CommitResponse, ServerError>>;

/// Sends a commit to the room without waiting for its reply.
async fn send_commit(
    sender: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    mutation_id: MutationId,
    last_ack_seq: SequenceNumber,
    op: Operation,
) -> CommitReply {
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
    rx
}

/// Holds the next fsync of `active` (a room's `active.wal`) until the returned sender sends or
/// is dropped; the returned receiver tells when it is held.
fn hold_next_fsync(active: &Path) -> (std::sync::mpsc::Sender<()>, oneshot::Receiver<()>) {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (held_tx, held_rx) = oneshot::channel();
    fail_point::arm_hook("warm_append_sync", active, move || {
        let _ = held_tx.send(());
        let _ = release_rx.recv_timeout(POLL_CEILING);
    });
    (release_tx, held_rx)
}

/// Runs `check` right before the next fsync of `active`, on the thread that runs it.
fn before_next_fsync(active: &Path, check: impl FnOnce() + Send + 'static) {
    fail_point::arm_hook("warm_append_sync", active, check);
}

/// How many of `replies` already have an answer; any answer found is lost to the test.
fn answered(replies: &Mutex<Vec<CommitReply>>) -> usize {
    replies
        .lock()
        .unwrap()
        .iter_mut()
        .map(|reply| reply.try_recv())
        .filter(|answer| !matches!(answer, Err(oneshot::error::TryRecvError::Empty)))
        .count()
}

fn insert_op_with_title(id: i64, title_len: usize) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "t".repeat(title_len))
        .build();
    test_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// The commits that arrive while an fsync runs wait in the mailbox, and the next fsync covers
/// all of them: fewer fsyncs than commits. None is answered before that fsync, each gets the
/// sequence of its place in the mailbox, and the group is announced once, after that fsync,
/// with its operations.
#[tokio::test]
async fn commits_waiting_behind_an_fsync_share_the_next_one() {
    const WAITING: u8 = 10;
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let events = subscribe(&sender).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let replies = Arc::new(Mutex::new(Vec::new()));
    for n in 2..=WAITING + 1 {
        let reply = send_commit(&sender, &client, mutation(n), seq(0), insert_op(n.into())).await;
        replies.lock().unwrap().push(reply);
    }
    let early = Arc::new(Mutex::new(None));
    let events = Arc::new(Mutex::new(events));
    let announced_early = Arc::new(Mutex::new(Vec::new()));
    before_next_fsync(&fx.active_wal(), {
        let (replies, early) = (Arc::clone(&replies), Arc::clone(&early));
        let (events, announced) = (Arc::clone(&events), Arc::clone(&announced_early));
        move || {
            *early.lock().unwrap() = Some(answered(&replies));
            let mut events = events.lock().unwrap();
            while let Ok(event) = events.try_recv() {
                announced.lock().unwrap().push(event);
            }
        }
    });
    release.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap().assigned_seq, seq(1));
    let replies = std::mem::take(&mut *replies.lock().unwrap());
    for (n, reply) in (2..).zip(replies) {
        assert_eq!(reply.await.unwrap().unwrap().assigned_seq, seq(n));
    }
    assert_eq!(
        fx.fsyncs(),
        2,
        "one fsync for the first commit and one for the rest"
    );
    assert_eq!(
        *early.lock().unwrap(),
        Some(0),
        "a commit of the group was answered before the fsync that covers it"
    );
    let early = std::mem::take(&mut *announced_early.lock().unwrap());
    assert_eq!(
        early.iter().map(seqs_in).collect::<Vec<_>>(),
        vec![vec![1]],
        "only the first commit may be announced before the fsync of the group"
    );
    let head = u64::from(WAITING) + 1;
    let after_fsync: Vec<RoomEvent> =
        std::iter::from_fn(|| events.lock().unwrap().try_recv().ok()).collect();
    assert_eq!(
        after_fsync.iter().map(seqs_in).collect::<Vec<_>>(),
        vec![(2..=head).collect::<Vec<_>>()],
        "the group must be announced once, with its operations"
    );
    let synced = sync_all(&sender, &client).await.unwrap();
    let seqs: Vec<u64> = synced.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, (1..=head).collect::<Vec<_>>());
    fx.manager.shutdown_all().await;
}

/// The fsync of a group fails: every commit of the group is answered `Unavailable` and the
/// room stops. The records reached the file, so once the room reopens a retry of each, in any
/// order, is acknowledged with the sequence it had in the group.
#[tokio::test]
async fn a_failed_fsync_fails_every_commit_of_its_group_and_retries_get_their_sequences() {
    const WAITING: u8 = 5;
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let mut replies = Vec::new();
    for n in 2..=WAITING + 1 {
        replies.push(send_commit(&sender, &client, mutation(n), seq(0), insert_op(n.into())).await);
    }
    // A commit of the group that is rejected keeps its own answer.
    let invalid = send_commit(&sender, &client, mutation(99), seq(0), invalid_op()).await;
    let active = fx.active_wal();
    before_next_fsync(&fx.active_wal(), move || {
        fail_point::arm("warm_append_sync", &active)
    });
    release.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap().assigned_seq, seq(1));
    assert!(matches!(
        invalid.await,
        Ok(Err(ServerError::SchemaViolation(_)))
    ));
    for reply in replies {
        assert!(
            matches!(reply.await, Ok(Err(ServerError::Unavailable(_)))),
            "every commit of the failed group must be answered Unavailable"
        );
    }
    assert!(
        sender.is_closed(),
        "the room must stop after a failed fsync"
    );

    let recovered = fx.sender().await;
    for n in (2..=WAITING + 1).rev() {
        let retry = commit(
            &recovered,
            &client,
            mutation(n),
            seq(0),
            insert_op(n.into()),
        )
        .await;
        assert_eq!(retry.unwrap().assigned_seq, seq(n.into()));
    }
    let synced = sync_all(&recovered, &client).await.unwrap();
    assert_eq!(synced.ops.len(), usize::from(WAITING) + 1);
    fx.manager.shutdown_all().await;
}

/// In the middle of a group, a commit that is rejected leaves no trace and does not hold back
/// the others, and a retry of a mutation earlier in the group gets its sequence.
#[tokio::test]
async fn a_rejected_commit_in_a_group_leaves_no_trace_and_a_repeated_one_gets_its_sequence() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let (writer, other) = (
        ClientId::new("writer").unwrap(),
        ClientId::new("other").unwrap(),
    );
    register(&sender, &writer).await;
    register(&sender, &other).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &writer, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let a = send_commit(&sender, &writer, mutation(2), seq(0), insert_op(2)).await;
    let invalid = send_commit(&sender, &other, mutation(3), seq(0), invalid_op()).await;
    let ahead = send_commit(&sender, &other, mutation(4), seq(5), insert_op(4)).await;
    let c = send_commit(&sender, &writer, mutation(5), seq(0), insert_op(5)).await;
    let a_again = send_commit(&sender, &writer, mutation(2), seq(0), insert_op(2)).await;
    // The same mutation id from another client is another mutation.
    let d = send_commit(&sender, &other, mutation(2), seq(0), insert_op(6)).await;
    // No client has seen a sequence of the group before the group is durable.
    let pending = send_commit(&sender, &other, mutation(7), seq(2), insert_op(7)).await;
    release.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap().assigned_seq, seq(1));
    assert_eq!(a.await.unwrap().unwrap().assigned_seq, seq(2));
    assert!(matches!(
        invalid.await.unwrap(),
        Err(ServerError::SchemaViolation(_))
    ));
    assert!(matches!(
        ahead.await.unwrap(),
        Err(ServerError::InvalidSequence { .. })
    ));
    assert_eq!(c.await.unwrap().unwrap().assigned_seq, seq(3));
    assert_eq!(a_again.await.unwrap().unwrap().assigned_seq, seq(2));
    assert_eq!(d.await.unwrap().unwrap().assigned_seq, seq(4));
    assert!(matches!(
        pending.await.unwrap(),
        Err(ServerError::InvalidSequence { .. })
    ));
    assert_eq!(
        fx.fsyncs(),
        2,
        "the rejected commits must not hold back the group"
    );

    let synced = sync_all(&sender, &writer).await.unwrap();
    let ids: Vec<i64> = synced
        .ops
        .iter()
        .map(|op| match op.op.pk().values()[0] {
            Value::Int(id) => id,
            ref other => panic!("unexpected key {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 5, 6]);
    assert_eq!(metrics(&sender).await.head_seq, seq(4));
    fx.manager.shutdown_all().await;
}

/// A command that is not a commit ends the group in front of it: it is handled after that
/// group's fsync, and before the commits behind it. A `/sync` sees the group before it, durable,
/// and not the commit behind it; an `Ack` answers with the head of the group before it.
#[tokio::test]
async fn a_command_that_is_not_a_commit_ends_the_group_and_sees_it_durable() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let a = send_commit(&sender, &client, mutation(2), seq(0), insert_op(2)).await;
    let (ack_tx, ack_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Ack {
            client_id: client.clone(),
            ack_seq: seq(1),
            reply: ack_tx,
        })
        .await
        .unwrap();
    let (sync_tx, sync_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: client.clone(),
            from_seq: seq(0),
            max_batch_size: 100,
            reply: sync_tx,
        })
        .await
        .unwrap();
    let b = send_commit(&sender, &client, mutation(3), seq(0), insert_op(3)).await;

    // The sync is not answered yet at the fsync of the group of `a`, and is at that of `b`.
    let sync_reply = Arc::new(Mutex::new(sync_rx));
    let (at_fsync_of_a, at_fsync_of_b) = (Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)));
    before_next_fsync(&fx.active_wal(), {
        let (sync_reply, at_a, at_b, active) = (
            Arc::clone(&sync_reply),
            Arc::clone(&at_fsync_of_a),
            Arc::clone(&at_fsync_of_b),
            fx.active_wal(),
        );
        move || {
            *at_a.lock().unwrap() = Some(sync_reply.lock().unwrap().try_recv().is_ok());
            before_next_fsync(&active, move || {
                *at_b.lock().unwrap() = Some(sync_reply.lock().unwrap().try_recv());
            });
        }
    });
    release.send(()).unwrap();

    let within = |reply| tokio::time::timeout(POLL_CEILING, reply);
    let first = within(first)
        .await
        .expect("the first commit was never answered");
    assert_eq!(first.unwrap().unwrap().assigned_seq, seq(1));
    let a = within(a)
        .await
        .expect("the commit in front of the sync was never answered");
    assert_eq!(a.unwrap().unwrap().assigned_seq, seq(2));
    let acked = tokio::time::timeout(POLL_CEILING, ack_rx).await;
    assert_eq!(
        acked.expect("the ack was never answered").unwrap().unwrap(),
        seq(2)
    );
    let b = within(b)
        .await
        .expect("the commit behind the sync was never answered");
    assert_eq!(b.unwrap().unwrap().assigned_seq, seq(3));
    assert_eq!(
        *at_fsync_of_a.lock().unwrap(),
        Some(false),
        "the sync was answered before the fsync of the group in front of it"
    );
    let sync = at_fsync_of_b
        .lock()
        .unwrap()
        .take()
        .expect("the commit behind the sync was never synced")
        .expect("the sync must be answered before the fsync of the commit behind it")
        .unwrap();
    let seqs: Vec<u64> = sync.ops.iter().map(|op| op.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2]);
    assert_eq!(sync.head_seq, seq(2));
    assert_eq!(fx.fsyncs(), 3);
    fx.manager.shutdown_all().await;
}

/// A group whose commits take `active.wal` past `ram_max_ops` seals it once, at the group's
/// end, into a segment that holds the whole group.
#[tokio::test]
async fn a_group_that_reaches_the_segment_limit_seals_active_wal_at_its_end() {
    let fx = Fixture::new(RoomLifecycleOverrides {
        ram_max_ops: Some(3),
        ..RoomLifecycleOverrides::default()
    })
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let mut replies = Vec::new();
    for n in 2..=6u8 {
        replies.push(send_commit(&sender, &client, mutation(n), seq(0), insert_op(n.into())).await);
    }
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    for reply in replies {
        reply.await.unwrap().unwrap();
    }

    let segments = fx.room_dir().join("segments");
    assert!(
        segment_path(&segments, seq(1), seq(6), Tier::Warm).exists(),
        "the group must be sealed whole, at its end"
    );
    assert!(!segment_path(&segments, seq(1), seq(3), Tier::Warm).exists());
    assert!(!fx.active_wal().exists());
    let synced = sync_all(&sender, &client).await.unwrap();
    assert_eq!(synced.ops.len(), 6);
    fx.manager.shutdown_all().await;
}

/// Sealing `active.wal` at the end of a group fails after the group is durable: every commit
/// of the group is acknowledged, and then the room stops.
#[tokio::test]
async fn a_seal_that_fails_after_a_group_acknowledges_every_commit_of_it_and_stops_the_room() {
    let fx = Fixture::new(RoomLifecycleOverrides {
        ram_max_ops: Some(3),
        ..RoomLifecycleOverrides::default()
    })
    .await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let mut replies = Vec::new();
    for n in 2..=5u8 {
        replies.push(send_commit(&sender, &client, mutation(n), seq(0), insert_op(n.into())).await);
    }
    let segments = fx.room_dir().join("segments");
    before_next_fsync(&fx.active_wal(), move || {
        fail_point::arm("sync_dir", &segments)
    });
    release.send(()).unwrap();

    first.await.unwrap().unwrap();
    for (n, reply) in (2..).zip(replies) {
        let acked = reply
            .await
            .unwrap()
            .expect("a durable commit must be acknowledged");
        assert_eq!(acked.assigned_seq, seq(n));
    }
    assert!(
        sender.is_closed(),
        "the room must restart after a failed seal"
    );
    let recovered = fx.sender().await;
    let synced = sync_all(&recovered, &client).await.unwrap();
    assert_eq!(synced.ops.len(), 5);
    fx.manager.shutdown_all().await;
}

/// A group takes at most its cap of commits, and at most its cap of bytes of new operations
/// (plus the operation that reaches it): the rest wait for the next group.
#[tokio::test]
async fn a_group_takes_at_most_its_cap_of_commits_and_of_bytes() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let waiting = GROUP_MAX_COMMITS + 3;
    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(0), seq(0), insert_op(0)).await;
    held.await.unwrap();
    let mut replies = Vec::new();
    for n in 1..=waiting {
        let id = MutationId::from_u128(n as u128);
        replies.push(send_commit(&sender, &client, id, seq(0), insert_op(n as i64)).await);
    }
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    for reply in replies {
        reply.await.unwrap().unwrap();
    }
    assert_eq!(
        fx.fsyncs(),
        3,
        "the first commit, a full group and the rest"
    );

    // Operations of a quarter of the cap of bytes, and a little more: the fourth reaches it.
    let title_len = (GROUP_MAX_BYTES / 4) as usize;
    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(200), seq(0), insert_op(500)).await;
    held.await.unwrap();
    let mut replies = Vec::new();
    for n in 1..=6u8 {
        let op = insert_op_with_title(500 + i64::from(n), title_len);
        replies.push(send_commit(&sender, &client, mutation(200 + n), seq(0), op).await);
    }
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    for reply in replies {
        reply.await.unwrap().unwrap();
    }
    assert_eq!(
        fx.fsyncs(),
        3 + 3,
        "the first commit, a group of four and one of two"
    );
    fx.manager.shutdown_all().await;
}

/// The catch-up of each commit of a group reaches the head of the group, also for a client
/// that was up to date before it, whose own operation is not the last of the group.
#[tokio::test]
async fn the_catchup_of_every_commit_of_a_group_reaches_the_head_of_the_group() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let (x, y) = (ClientId::new("x").unwrap(), ClientId::new("y").unwrap());
    register(&sender, &x).await;
    register(&sender, &y).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &x, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let from_x = send_commit(&sender, &x, mutation(2), seq(1), insert_op(2)).await;
    let from_y = send_commit(&sender, &y, mutation(3), seq(1), insert_op(3)).await;
    release.send(()).unwrap();
    first.await.unwrap().unwrap();

    for reply in [from_x, from_y] {
        let reply = reply.await.unwrap().unwrap();
        let seqs: Vec<u64> = reply.catchup_ops.iter().map(|op| op.seq.get()).collect();
        assert_eq!(
            seqs,
            vec![2, 3],
            "the catch-up must reach the head of the group"
        );
        assert!(!reply.has_more);
    }
    fx.manager.shutdown_all().await;
}

/// Sealing `active.wal` at the end of a group fails: as before the group commit, the room
/// closes its mailbox before it acknowledges any commit of the group, so that a request sent
/// once one of them is acknowledged is never taken by the stopping actor (`RoomManager` sends
/// it to the room reopened from disk). The actor answers the second commit of the group with a
/// catch-up read from disk, which waits for the only permit of its file I/O pool, held by the
/// test.
#[tokio::test]
async fn a_group_whose_seal_fails_closes_the_mailbox_before_its_first_ack() {
    let dir = tempdir().unwrap();
    let io = IoThreads::new(1);
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    // The RAM window keeps the last 4 operations, and 4 operations seal `active.wal`.
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 4,
        ..RoomLifecyclePolicy::default()
    };
    let (sender, actor) = RoomActor::open(
        RoomId::new("room").unwrap(),
        1,
        SchemaId::new("todo").unwrap(),
        Arc::new(test_schema()),
        dir.path(),
        config,
        policy,
        relay_on(&dir),
        Arc::new(InflightBudget::new(u64::MAX, u64::MAX)),
        Arc::new(OpsEventBudget::new(u64::MAX)),
        TEST_TIMINGS,
        Arc::clone(&io),
    )
    .unwrap();
    let segments = room_dir(dir.path(), &actor.room_id).join("segments");
    let active = segments.join("active.wal");
    let running = tokio::spawn(actor.run());
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&active);
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    // The catch-up of the first commit of the group comes from the RAM window (cursor 1), that
    // of the second from disk (cursor 0).
    let from_ram = send_commit(&sender, &client, mutation(2), seq(1), insert_op(2)).await;
    let from_disk = send_commit(&sender, &client, mutation(3), seq(0), insert_op(3)).await;
    let mut rest = Vec::new();
    for n in 4..=5u8 {
        rest.push(send_commit(&sender, &client, mutation(n), seq(0), insert_op(n.into())).await);
    }
    // At the fsync of the group the test queues for the pool's permit, ahead of the catch-up
    // read of `from_disk`, and the seal after the fsync fails.
    let (at_fsync_tx, at_fsync_rx) = oneshot::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    before_next_fsync(&active, {
        let segments = segments.clone();
        move || {
            fail_point::arm("sync_dir", &segments);
            let _ = at_fsync_tx.send(());
            let _ = go_rx.recv_timeout(POLL_CEILING);
        }
    });
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    at_fsync_rx.await.unwrap();
    let permit = io.io().permit();
    tokio::pin!(permit);
    assert!(futures::poll!(&mut permit).is_pending());
    go_tx.send(()).unwrap();
    let permit = permit.await;

    let acked = from_ram
        .await
        .unwrap()
        .expect("a durable commit must be acknowledged");
    assert_eq!(acked.assigned_seq, seq(2));
    let closed = sender.is_closed();
    drop(permit);
    from_disk.await.unwrap().unwrap();
    for reply in rest {
        reply.await.unwrap().unwrap();
    }
    assert_eq!(running.await.unwrap(), ActorExit::Stopped);
    assert!(
        closed,
        "the mailbox was still open once a commit of the group was acknowledged: a request sent \
         then is taken by the stopping actor and dropped, instead of reaching the reopened room"
    );
}

/// The fsync of a group fails with a retry of one of its own mutations in it: the retry is not
/// acknowledged either, since the record it would acknowledge may not be on disk.
#[tokio::test]
async fn a_retry_in_a_group_whose_fsync_fails_is_not_acknowledged() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let original = send_commit(&sender, &client, mutation(2), seq(0), insert_op(2)).await;
    let retry = send_commit(&sender, &client, mutation(2), seq(0), insert_op(2)).await;
    let active = fx.active_wal();
    before_next_fsync(&fx.active_wal(), move || {
        fail_point::arm("warm_append_sync", &active)
    });
    release.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap().assigned_seq, seq(1));
    for reply in [original, retry] {
        assert!(
            matches!(reply.await, Ok(Err(ServerError::Unavailable(_)))),
            "no commit of a group whose fsync failed may be acknowledged, retries included"
        );
    }
    let recovered = fx.sender().await;
    let retry = commit(&recovered, &client, mutation(2), seq(0), insert_op(2)).await;
    assert_eq!(retry.unwrap().assigned_seq, seq(2));
    fx.manager.shutdown_all().await;
}

/// A panic while a group is written: the commits of the group lose their replies (the manager
/// answers them `Internal`), and the command that ended the group, held back in the mailbox,
/// is answered `Unavailable`, as are the commands still queued behind it.
#[tokio::test]
async fn a_panic_during_a_group_answers_the_command_held_behind_it_unavailable() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let grouped = send_commit(&sender, &client, mutation(2), seq(0), insert_op(2)).await;
    let (heartbeat_tx, heartbeat_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Heartbeat {
            client_id: client.clone(),
            reply: heartbeat_tx,
        })
        .await
        .unwrap();
    let queued = send_commit(&sender, &client, mutation(3), seq(0), insert_op(3)).await;
    before_next_fsync(&fx.active_wal(), || {
        panic!("injected panic in the fsync of a group")
    });
    release.send(()).unwrap();

    first.await.unwrap().unwrap();
    assert!(
        grouped.await.is_err(),
        "a commit of the group that panicked loses its reply"
    );
    assert!(
        matches!(heartbeat_rx.await, Ok(Err(ServerError::Unavailable(_)))),
        "the command held behind the group must be answered Unavailable"
    );
    assert!(matches!(queued.await, Ok(Err(ServerError::Unavailable(_)))));
    fx.manager.shutdown_all().await;
}

/// A group never assigns `u64::MAX`, also when the head that is visible is further back than
/// the head of the group.
#[tokio::test]
async fn a_group_never_assigns_the_last_sequence_number() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    fx.manager.shutdown_all().await;
    let floor = u64::MAX - 3;
    let meta = format!(r#"{{"pruned_through_seq": {floor}, "sealed_through_seq": 0}}"#);
    durable::write_checked(&fx.room_dir().join("log_meta.json"), meta.as_bytes()).unwrap();

    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(floor), insert_op(1)).await;
    held.await.unwrap();
    let second = send_commit(&sender, &client, mutation(2), seq(floor), insert_op(2)).await;
    let third = send_commit(&sender, &client, mutation(3), seq(floor), insert_op(3)).await;
    release.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap().assigned_seq, seq(floor + 1));
    assert_eq!(second.await.unwrap().unwrap().assigned_seq, seq(floor + 2));
    let third = third.await.unwrap();
    assert!(matches!(third, Err(ServerError::Internal(_))), "{third:?}");
    assert_eq!(metrics(&sender).await.head_seq, seq(floor + 2));
    fx.manager.shutdown_all().await;
}

// The SSE event `ops`: the operations of a durable commit or group, for the room's streams.

/// The operations an `ops` event carries, decoded, with the id and version of the schema it
/// announces for them. Checks that its head is that of its last operation; panics on any other
/// event.
fn ops_in(event: &RoomEvent) -> (Vec<SequencedOperation>, SchemaId, u64) {
    use base64::Engine;
    let RoomEvent::Ops(event) = event else {
        panic!("expected an ops event, got {event:?}");
    };
    let frame = base64::engine::general_purpose::STANDARD
        .decode(event.data())
        .unwrap();
    match zemdb_core::protocol::codec::decode_message(&frame).unwrap() {
        zemdb_core::protocol::messages::ServerMessage::OpsEvent {
            head_seq,
            ops,
            schema_version,
            schema_id,
            ..
        } => {
            assert_eq!(head_seq, event.head_seq());
            assert_eq!(Some(head_seq), ops.last().map(|op| op.seq));
            (ops, schema_id, schema_version)
        }
        other => panic!("expected an OpsEvent frame, got {other:?}"),
    }
}

/// The sequences of the operations an `ops` event carries.
fn seqs_in(event: &RoomEvent) -> Vec<u64> {
    ops_in(event).0.iter().map(|op| op.seq.get()).collect()
}

/// The events broadcast so far and not yet received.
fn pending_events(events: &mut broadcast::Receiver<RoomEvent>) -> Vec<RoomEvent> {
    std::iter::from_fn(|| events.try_recv().ok()).collect()
}

/// A group with commits rejected and repeated in the middle announces, in one event, exactly
/// its new operations: consecutive, after the head before the group, and the same ones a sync
/// returns.
#[tokio::test]
async fn the_ops_event_of_a_group_carries_exactly_its_new_operations() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let (writer, other) = (
        ClientId::new("writer").unwrap(),
        ClientId::new("other").unwrap(),
    );
    register(&sender, &writer).await;
    register(&sender, &other).await;
    let mut events = subscribe(&sender).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &writer, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let mut replies = vec![
        send_commit(&sender, &writer, mutation(2), seq(0), insert_op(2)).await,
        send_commit(&sender, &other, mutation(3), seq(0), invalid_op()).await,
        send_commit(&sender, &writer, mutation(2), seq(0), insert_op(2)).await,
        send_commit(&sender, &other, mutation(4), seq(9), insert_op(4)).await,
        send_commit(&sender, &other, mutation(5), seq(0), insert_op(5)).await,
    ];
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    for reply in replies.drain(..) {
        let _ = reply.await.unwrap();
    }

    let announced = pending_events(&mut events);
    assert_eq!(
        announced.iter().map(seqs_in).collect::<Vec<_>>(),
        vec![vec![1], vec![2, 3]]
    );
    let delivered: Vec<SequencedOperation> =
        announced.iter().flat_map(|event| ops_in(event).0).collect();
    let synced = sync_all(&sender, &writer).await.unwrap().ops;
    assert_eq!(
        delivered,
        synced.iter().map(|op| (**op).clone()).collect::<Vec<_>>()
    );
    for event in &announced {
        let (_, schema_id, version) = ops_in(event);
        assert_eq!((schema_id.as_str(), version), ("todo", 1));
    }
    fx.manager.shutdown_all().await;
}

/// A schema reload waiting between two commits ends the group in front of it: each `ops`
/// event announces the schema its operations were checked with, and the reload is announced
/// between them.
#[tokio::test]
async fn a_schema_reload_between_waiting_commits_is_announced_between_their_events() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let mut events = subscribe(&sender).await;
    let mut evolved = test_schema();
    evolved
        .add_column(
            "tasks",
            zemdb_core::schema::ColumnDef::new("tag", zemdb_core::value::DataType::String)
                .nullable(true),
        )
        .unwrap();
    let row = RowBuilder::new().set("id", 3).set("title", "new").build();
    let evolved_op = evolved.to_operation_insert("tasks", &row, 1000).unwrap();

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let before = send_commit(&sender, &client, mutation(2), seq(0), insert_op(2)).await;
    let (reload_tx, reload_rx) = oneshot::channel();
    sender
        .send(RoomCommand::ReloadSchema {
            schema_id: SchemaId::new("todo").unwrap(),
            schema: Arc::new(evolved),
            reply: reload_tx,
        })
        .await
        .unwrap();
    let after = send_commit(&sender, &client, mutation(3), seq(0), evolved_op).await;
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    before.await.unwrap().unwrap();
    reload_rx.await.unwrap().unwrap();
    after.await.unwrap().unwrap();

    let announced = pending_events(&mut events);
    assert_eq!(announced.len(), 4, "{announced:?}");
    assert_eq!(
        announced[2],
        RoomEvent::SchemaReloaded(SchemaId::new("todo").unwrap())
    );
    let ops_events: Vec<(Vec<u64>, u64)> = [&announced[0], &announced[1], &announced[3]]
        .into_iter()
        .map(|event| {
            let (ops, _, version) = ops_in(event);
            (ops.iter().map(|op| op.seq.get()).collect(), version)
        })
        .collect();
    assert_eq!(
        ops_events,
        vec![(vec![1], 1), (vec![2], 1), (vec![3], 2)],
        "each event announces the version its operations were checked with"
    );
    fx.manager.shutdown_all().await;
}

/// Operations whose frame does not fit in an `ops` event are announced with `head_advanced`,
/// and clients read them with a sync.
#[tokio::test]
async fn an_operation_too_large_for_an_event_is_announced_with_head_advanced() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let mut events = subscribe(&sender).await;

    commit(
        &sender,
        &client,
        mutation(1),
        seq(0),
        insert_op_with_title(1, 5000),
    )
    .await
    .unwrap();
    commit(&sender, &client, mutation(2), seq(0), insert_op(2))
        .await
        .unwrap();

    let announced = pending_events(&mut events);
    assert_eq!(announced[0], RoomEvent::HeadAdvanced(seq(1)));
    assert_eq!(seqs_in(&announced[1]), vec![2]);
    assert_eq!(announced.len(), 2);
    fx.manager.shutdown_all().await;
}

/// A stream that falls more than the channel's capacity behind is told it lagged, and then
/// receives the events still retained, whose gap from its cursor sends the client to a sync.
#[tokio::test]
async fn a_stream_that_falls_behind_is_told_it_lagged() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let mut events = subscribe(&sender).await;

    let commits = EVENTS_CAPACITY as u64 + 3;
    for n in 1..=commits {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&n.to_be_bytes());
        commit(
            &sender,
            &client,
            MutationId::new(id),
            seq(0),
            insert_op(n as i64),
        )
        .await
        .unwrap();
    }

    assert!(matches!(
        events.recv().await,
        Err(broadcast::error::RecvError::Lagged(3))
    ));
    assert_eq!(seqs_in(&events.recv().await.unwrap()), vec![4]);
    fx.manager.shutdown_all().await;
}

/// The `ops` events of all rooms retain at most `max_sse_ops_bytes`, counted by their base64:
/// with streams that do not read in several rooms, an event that does not fit is sent as
/// `head_advanced`. The bytes come back when the channel drops an event, when its streams read
/// it, and when they close.
#[tokio::test]
async fn retained_ops_events_stay_within_their_total_and_come_back_when_dropped() {
    let rooms = ["room-a", "room-b", "room-c"].map(|id| RoomId::new(id).unwrap());
    let schema_id = SchemaId::new("todo").unwrap();
    // Every room's first event is the same size: rooms of the same length, the same operation,
    // and incarnations of the same encoded length (random 128-bit values: checked below).
    let first_op = SequencedOperation::new(1u64, insert_op(1));
    let first_event = |room_id: &RoomId, incarnation: u128| {
        OpsEventBudget::new(u64::MAX)
            .event(room_id, incarnation, &schema_id, 1, [&first_op].into_iter())
            .unwrap()
            .retained()
    };
    let len = first_event(&rooms[0], u128::MAX);
    let max = len * 2 + len / 2;
    let dir = tempdir().unwrap();
    let (manager, _relay) = new_manager_with(&dir, |config| {
        config.max_sse_ops_bytes = max as u64;
    });
    let held = || manager.ops_events().held();
    let client = ClientId::new("writer").unwrap();
    let mut senders = Vec::new();
    let mut streams = Vec::new();
    for room_id in &rooms {
        let incarnation = manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await
            .unwrap()
            .room_incarnation;
        assert_eq!(first_event(room_id, incarnation), len);
        let sender = manager.get_or_spawn(room_id, None).await.unwrap();
        register(&sender, &client).await;
        streams.push(subscribe(&sender).await);
        senders.push(sender);
    }
    let [a, b, c] = [&senders[0], &senders[1], &senders[2]];

    // Two events fill the total: the third room's is sent as head_advanced.
    commit(a, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(b, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    commit(c, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();
    assert_eq!(held(), 2 * len);
    assert_eq!(
        streams[2].try_recv().unwrap(),
        RoomEvent::HeadAdvanced(seq(1))
    );

    // The channel of the first room drops its event once it is overwritten, unread: until then
    // every event of the room is head_advanced, and the total is never passed.
    for n in 2..=EVENTS_CAPACITY as u64 + 1 {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&n.to_be_bytes());
        commit(a, &client, MutationId::new(id), seq(0), insert_op(n as i64))
            .await
            .unwrap();
        assert!(held() <= max);
    }
    assert_eq!(held(), len, "the overwritten event gave its bytes back");
    assert!(matches!(
        streams[0].try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(1))
    ));

    // Read by the only stream of its room, an event gives its bytes back once dropped.
    let read = streams[1].try_recv().unwrap();
    assert_eq!(seqs_in(&read), vec![1]);
    drop(read);
    assert_eq!(held(), 0);

    // A stream that closes gives back the bytes of what it did not read.
    commit(b, &client, mutation(2), seq(0), insert_op(2))
        .await
        .unwrap();
    assert!(held() > 0);
    drop(streams.remove(1));
    assert_eq!(held(), 0);
    manager.shutdown_all().await;
}

/// The events a stream receives until its room stops and the stream ends.
async fn events_until_the_room_stops(
    events: &mut broadcast::Receiver<RoomEvent>,
) -> Vec<RoomEvent> {
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(POLL_CEILING, events.recv())
            .await
            .expect("the stream of a stopped room ends")
        {
            Ok(event) => received.push(event),
            Err(broadcast::error::RecvError::Closed) => return received,
            Err(err) => panic!("{err:?}"),
        }
    }
}

/// A group whose fsync fails is never announced: its commits are answered `Unavailable`, the
/// room stops, and its streams end with only the events of what was durable. An `ops` event
/// announced anyway would hand clients operations that may never be durable (INV-D7).
#[tokio::test]
async fn a_group_whose_fsync_fails_is_never_announced() {
    const WAITING: u8 = 4;
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let mut events = subscribe(&sender).await;

    let (release, held) = hold_next_fsync(&fx.active_wal());
    let first = send_commit(&sender, &client, mutation(1), seq(0), insert_op(1)).await;
    held.await.unwrap();
    let mut replies = Vec::new();
    for n in 2..=WAITING + 1 {
        replies.push(send_commit(&sender, &client, mutation(n), seq(0), insert_op(n.into())).await);
    }
    let active = fx.active_wal();
    before_next_fsync(&fx.active_wal(), move || {
        fail_point::arm("warm_append_sync", &active)
    });
    release.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap().assigned_seq, seq(1));
    for reply in replies {
        assert!(matches!(reply.await, Ok(Err(ServerError::Unavailable(_)))));
    }
    let announced = events_until_the_room_stops(&mut events).await;
    assert_eq!(
        announced.iter().map(seqs_in).collect::<Vec<_>>(),
        vec![vec![1]],
        "the failed group was announced"
    );
    fx.manager.shutdown_all().await;
}

/// A commit whose write fails is never announced either.
#[tokio::test]
async fn a_commit_whose_write_fails_is_never_announced() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let mut events = subscribe(&sender).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    fail_point::arm("warm_append_write", &fx.active_wal());
    let failed = commit(&sender, &client, mutation(2), seq(1), insert_op(2)).await;
    assert!(
        matches!(failed, Err(ServerError::Unavailable(_))),
        "{failed:?}"
    );

    let announced = events_until_the_room_stops(&mut events).await;
    assert_eq!(
        announced.iter().map(seqs_in).collect::<Vec<_>>(),
        vec![vec![1]],
        "the failed commit was announced"
    );
    fx.manager.shutdown_all().await;
}

/// An `ops` event says the incarnation of its room, so that a client of an earlier room with
/// the same id does not apply it.
#[tokio::test]
async fn an_ops_event_carries_the_incarnation_of_its_room() {
    let fx = Fixture::new(RoomLifecycleOverrides::default()).await;
    let sender = fx.sender().await;
    let client = ClientId::new("writer").unwrap();
    register(&sender, &client).await;
    let mut events = subscribe(&sender).await;
    commit(&sender, &client, mutation(1), seq(0), insert_op(1))
        .await
        .unwrap();

    let RoomEvent::Ops(event) = events.try_recv().unwrap() else {
        panic!("expected an ops event");
    };
    use base64::Engine;
    let frame = base64::engine::general_purpose::STANDARD
        .decode(event.data())
        .unwrap();
    let message: zemdb_core::protocol::messages::ServerMessage =
        zemdb_core::protocol::codec::decode_message(&frame).unwrap();
    assert!(matches!(
        message,
        zemdb_core::protocol::messages::ServerMessage::OpsEvent { room_incarnation, .. }
            if room_incarnation == fx.incarnation
    ));
    fx.manager.shutdown_all().await;
}
