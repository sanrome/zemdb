mod common;

use common::{
    ack, ask, build, commit, cursor, envelope, make_test_op, metrics, register, seqs, sync,
    tasks_schema, TestRoom,
};
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::*;
use zemdb_server::{RoomCommand, RoomEvent, RoomLifecyclePolicy, SchemaRegistry, ServerError};

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

#[tokio::test]
async fn schema_registry_adds_only_nullable_columns_and_persists_them() {
    let dir = tempdir().unwrap();
    let registry = SchemaRegistry::new(dir.path()).unwrap();

    let schema_id = SchemaId::new("todo-schema").unwrap();
    registry
        .register_schema(schema_id.clone(), tasks_schema())
        .unwrap();
    assert!(registry.get_schema(&schema_id).is_some());

    let evolved = registry
        .add_column(
            &schema_id,
            "tasks",
            ColumnDef::new("priority", DataType::Int).nullable(true),
        )
        .unwrap();
    assert!(evolved
        .get_table_by_name("tasks")
        .unwrap()
        .get_column("priority")
        .is_some());

    let err = registry
        .add_column(
            &schema_id,
            "tasks",
            ColumnDef::new("deadline", DataType::Int).nullable(false),
        )
        .unwrap_err();
    assert!(matches!(err, ServerError::SchemaViolation(_)));

    let reloaded = SchemaRegistry::new(dir.path()).unwrap();
    let loaded_schema = reloaded.get_schema(&schema_id).expect("schema persisted");
    assert!(loaded_schema
        .get_table_by_name("tasks")
        .unwrap()
        .get_column("priority")
        .is_some());
}

#[tokio::test]
async fn registration_returns_the_head_and_the_room_schema() {
    let room = TestRoom::spawn(None).await;

    let res = register(&room.sender, &client("client-alice")).await;
    assert_eq!(res.head_seq, SequenceNumber::new(0));
    assert_eq!(res.schema_id, room.schema_id);
    assert!(res.schema.has_table_by_name("tasks"));
}

#[tokio::test]
async fn commits_are_validated_sequenced_and_deduplicated() {
    let room = TestRoom::spawn(None).await;
    let sender = &room.sender;
    let client_id = client("client-1");

    // A client that is not registered cannot commit.
    let res0 = commit(
        sender,
        &client_id,
        MutationId::new([0; 16]),
        0,
        make_test_op(0),
    )
    .await;
    assert!(matches!(res0, Err(ServerError::ClientNotRegistered(_))));

    register(sender, &client_id).await;

    let res1 = commit(
        sender,
        &client_id,
        MutationId::new([1; 16]),
        0,
        make_test_op(1),
    )
    .await
    .unwrap();
    assert_eq!(res1.assigned_seq, SequenceNumber::new(1));
    assert_eq!(res1.catchup_ops.len(), 1);

    let res2 = commit(
        sender,
        &client_id,
        MutationId::new([2; 16]),
        1,
        make_test_op(2),
    )
    .await
    .unwrap();
    assert_eq!(res2.assigned_seq, SequenceNumber::new(2));

    // Re-sending mutation 1 returns its assigned_seq without advancing the head.
    let res3 = commit(
        sender,
        &client_id,
        MutationId::new([1; 16]),
        0,
        make_test_op(1),
    )
    .await
    .unwrap();
    assert_eq!(res3.assigned_seq, SequenceNumber::new(1));

    // An operation on a table the schema does not have.
    let invalid_op = Operation::delete(999, PrimaryKey::single(Value::Int(3)), 1000);
    let res4 = commit(sender, &client_id, MutationId::new([4; 16]), 2, invalid_op).await;
    assert!(matches!(res4, Err(ServerError::SchemaViolation(_))));
}

#[tokio::test]
async fn concurrent_commits_are_all_sequenced_and_signalled() {
    let room = TestRoom::spawn(None).await;
    let sender = &room.sender;

    // Registered first so that its cursor holds back proactive pruning.
    let reader = client("reader");
    register(sender, &reader).await;

    let mut sse_rx = ask(sender, |reply| RoomCommand::SubscribeEvents { reply })
        .await
        .unwrap();

    // Concurrent commits from 5 clients (20 commits each = 100 total)
    let mut tasks = vec![];
    for client_idx in 0..5 {
        let cmd_tx = sender.clone();
        tasks.push(tokio::spawn(async move {
            let client_id = ClientId::new(format!("worker-{}", client_idx)).unwrap();
            register(&cmd_tx, &client_id).await;
            for op_idx in 0..20 {
                let mut mutation_bytes = [0u8; 16];
                mutation_bytes[0] = client_idx as u8;
                mutation_bytes[1] = op_idx as u8;
                let op = make_test_op(client_idx * 100 + op_idx);
                commit(&cmd_tx, &client_id, MutationId::new(mutation_bytes), 0, op)
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(metrics(sender).await.head_seq, SequenceNumber::new(100));

    let mut last_event_seq = SequenceNumber::new(0);
    while let Ok(event) = sse_rx.try_recv() {
        if let RoomEvent::HeadAdvanced(seq) = event {
            last_event_seq = seq;
        }
    }
    assert_eq!(last_event_seq, SequenceNumber::new(100));

    // Sync returns all 100 operations in two batches of 50.
    let sync_res1 = sync(sender, &reader, 0, 50).await.unwrap();
    assert_eq!(sync_res1.ops.len(), 50);
    assert!(sync_res1.has_more);

    let sync_res2 = sync(sender, &reader, 50, 50).await.unwrap();
    assert_eq!(sync_res2.ops.len(), 50);
    assert!(!sync_res2.has_more);
}

#[tokio::test]
async fn restarted_room_recovers_its_head_and_deduplication() {
    let parts = build(|_| {});
    let schema_id = parts.register_schema("recovery-schema", tasks_schema());
    let room_id = RoomId::new("crash-test-room").unwrap();
    let c1 = client("c1");

    // Phase 1: spawn, commit 5 ops, then shut the room actor down.
    {
        let sender = parts
            .room_manager
            .get_or_spawn(&room_id, Some(&schema_id))
            .await
            .unwrap();
        register(&sender, &c1).await;
        for i in 1..=5 {
            commit(
                &sender,
                &c1,
                MutationId::new([i as u8; 16]),
                i as u64 - 1,
                make_test_op(i),
            )
            .await
            .unwrap();
        }
        parts.room_manager.shutdown_room(&room_id).await;
    }

    // Phase 2: respawn the actor from the same directory without passing schema_id.
    {
        let manager2 = parts.restarted_manager();
        let sender2 = manager2
            .get_or_spawn(&room_id, None)
            .await
            .expect("should recover room metadata");

        assert_eq!(metrics(&sender2).await.head_seq, SequenceNumber::new(5));

        // The deduplication cache was rehydrated: mutation 5 is a duplicate.
        let res = commit(&sender2, &c1, MutationId::new([5; 16]), 4, make_test_op(5))
            .await
            .unwrap();
        assert_eq!(res.assigned_seq, SequenceNumber::new(5));

        let res6 = commit(&sender2, &c1, MutationId::new([6; 16]), 5, make_test_op(6))
            .await
            .unwrap();
        assert_eq!(res6.assigned_seq, SequenceNumber::new(6));
    }
}

#[tokio::test]
async fn cursor_advances_only_on_client_ack() {
    let room = TestRoom::spawn(None).await;
    let sender = &room.sender;
    let reader = client("c-reader");
    register(sender, &reader).await;

    let producer = client("producer");
    register(sender, &producer).await;
    for i in 1..=5 {
        commit(
            sender,
            &producer,
            MutationId::new([i as u8; 16]),
            i as u64 - 1,
            make_test_op(i),
        )
        .await
        .unwrap();
    }

    let sync_res = sync(sender, &reader, 0, 10).await.unwrap();
    assert_eq!(sync_res.ops.len(), 5);
    assert_eq!(
        cursor(sender, &reader).await,
        Some(SequenceNumber::new(0)),
        "Cursor must NOT advance upon delivery of SyncResponse, only upon client ACK"
    );

    ask(sender, |reply| RoomCommand::Heartbeat {
        client_id: reader.clone(),
        reply,
    })
    .await
    .unwrap();
    assert_eq!(
        cursor(sender, &reader).await,
        Some(SequenceNumber::new(0)),
        "Heartbeat is pure liveness and must NOT advance cursor"
    );

    ack(sender, &reader, 5).await.unwrap();
    assert_eq!(
        cursor(sender, &reader).await,
        Some(SequenceNumber::new(5)),
        "Cursor must advance after explicit client ACK"
    );
}

#[tokio::test]
async fn retention_anchor_protects_the_deltas_after_a_snapshot() {
    // Small segments so proactive pruning has something to delete, but default TTLs: with
    // the short test TTLs, a slow run could prune the cold segments by age, which is not the
    // behavior under test.
    let lifecycle_policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        lease_timeout: Duration::from_secs(60),
        ..RoomLifecyclePolicy::default()
    };
    let room = TestRoom::spawn_with(
        |config| config.snapshot_ttl_secs = 300,
        Some(lifecycle_policy),
    )
    .await;
    let sender = &room.sender;

    // 1. Alice registers and commits 20 operations (1..=20). With 5 operations per segment
    // the log holds several sealed segments, so a floor above the snapshot can delete the
    // deltas that follow it. Her commits report cursor 0, so no maintenance tick can prune
    // anything before the snapshot is staged.
    let alice = client("alice");
    register(sender, &alice).await;
    for i in 1..=20 {
        commit(
            sender,
            &alice,
            MutationId::new([i as u8; 16]),
            0,
            make_test_op(i),
        )
        .await
        .unwrap();
    }

    // 2. Stage an active snapshot at seq 5 in the relay (it must lie inside the log range)
    room.relay
        .stage_snapshot(
            &room.room_id,
            SequenceNumber::new(5),
            bytes::Bytes::from(envelope(1000, 5)),
            async {
                let (tail_seq, head_seq) = room.room_manager.log_bounds(&room.room_id).await?;
                Ok(zemdb_server::relay::LogBounds { tail_seq, head_seq })
            },
        )
        .await
        .unwrap();

    // 3. Alice, the only registered client, acknowledges seq 20. Her cursor alone would put
    // the pruning floor at 20 and delete every sealed segment up to seq 15; the Retention
    // Anchor at seq 5 must keep deltas 6..=20.
    ack(sender, &alice, 20).await.unwrap();

    // 4. Bob (onboarding client) registers with current_seq = None
    let bob = client("bob");
    let bob_reg = register(sender, &bob).await;
    assert_eq!(bob_reg.head_seq, SequenceNumber::new(20));
    assert_eq!(bob_reg.active_snapshot_seq, Some(SequenceNumber::new(5)));
    assert!(
        bob_reg.tail_seq <= SequenceNumber::new(6),
        "the anchor must keep the deltas after the snapshot (tail {})",
        bob_reg.tail_seq
    );

    // 5. Bob applies the snapshot at 5 and requests the deltas after it: Sync { from_seq: 5 }.
    // These deltas MUST be present thanks to the Retention Anchor.
    let sync_res = sync(sender, &bob, 5, 50)
        .await
        .expect("Sync after snapshot must succeed");
    assert_eq!(seqs(&sync_res.ops), (6..=20).collect::<Vec<_>>());
}

#[tokio::test]
async fn future_ack_and_commit_sequences_are_rejected() {
    let room = TestRoom::spawn(None).await;
    let sender = &room.sender;

    let alice = client("alice");
    let alice_reg = register(sender, &alice).await;
    assert_eq!(alice_reg.head_seq, SequenceNumber::new(0));

    for i in 1u64..=3u64 {
        let commit_res = commit(
            sender,
            &alice,
            MutationId::new([i as u8; 16]),
            i - 1,
            make_test_op(i as i64),
        )
        .await
        .unwrap();
        assert_eq!(commit_res.assigned_seq, SequenceNumber::new(i));
    }

    // An Ack far beyond the head.
    let bad_ack_res = ack(sender, &alice, 999).await;
    assert!(
        matches!(
            bad_ack_res,
            Err(ServerError::InvalidSequence { expected, actual })
                if expected == SequenceNumber::new(3) && actual == SequenceNumber::new(999)
        ),
        "{bad_ack_res:?}"
    );

    // Alice's cursor did not advance to 999: it stays at the cursor reported by her last
    // accepted commit.
    assert_eq!(cursor(sender, &alice).await, Some(SequenceNumber::new(2)));

    // Bob registers and syncs from sequence 0: the deltas were not pruned.
    let bob = client("bob");
    register(sender, &bob).await;
    let sync_res = sync(sender, &bob, 0, 50)
        .await
        .expect("Sync from 0 must succeed");
    assert_eq!(seqs(&sync_res.ops), vec![1, 2, 3]);

    // A commit whose last_ack_seq is beyond the head (50 > 3).
    let invalid_commit_res = commit(
        sender,
        &alice,
        MutationId::new([0xFE; 16]),
        50,
        make_test_op(100),
    )
    .await;
    assert!(
        matches!(
            invalid_commit_res,
            Err(ServerError::InvalidSequence { expected, actual })
                if expected == SequenceNumber::new(3) && actual == SequenceNumber::new(50)
        ),
        "{invalid_commit_res:?}"
    );
}
