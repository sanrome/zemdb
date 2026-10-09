use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;
use zemdb_core::*;
use zemdb_server::{
    CommitResponse, RegisterResponse, RoomCommand, RoomEvent, RoomLifecyclePolicy, RoomManager,
    SchemaRegistry, ServerConfig, ServerError, SnapshotRelay, SyncBatchResponse,
};

fn create_test_relay(dir: &std::path::Path) -> Arc<SnapshotRelay> {
    Arc::new(
        SnapshotRelay::new(
            dir.join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
        )
        .unwrap(),
    )
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

fn create_insert_op(schema: &Schema, id: i64, title: &str) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", title)
        .set("completed", false)
        .build();
    schema
        .to_operation_insert("tasks", &row, 1000)
        .expect("valid insert op")
}

async fn register_client_helper(
    sender: &tokio::sync::mpsc::Sender<RoomCommand>,
    client_id: ClientId,
) -> Result<RegisterResponse, ServerError> {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id,
            current_seq: None,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

#[tokio::test]
async fn test_schema_registry_crud_and_evolution() {
    let dir = tempdir().unwrap();
    let registry = SchemaRegistry::new(dir.path()).unwrap();

    let schema_id = SchemaId::new("todo-schema").unwrap();
    let schema = create_test_schema();

    // 1. Register schema
    let _registered = registry.register_schema(schema_id.clone(), schema).unwrap();
    assert!(registry.get_schema(&schema_id).is_some());

    // 2. Add nullable column evolution
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

    // 3. Reject non-nullable column evolution
    let err = registry
        .add_column(
            &schema_id,
            "tasks",
            ColumnDef::new("deadline", DataType::Int).nullable(false),
        )
        .unwrap_err();
    assert!(matches!(err, ServerError::SchemaViolation(_)));

    // 4. Persistence reload check
    let reloaded = SchemaRegistry::new(dir.path()).unwrap();
    let loaded_schema = reloaded.get_schema(&schema_id).expect("schema persisted");
    assert!(loaded_schema
        .get_table_by_name("tasks")
        .unwrap()
        .get_column("priority")
        .is_some());
}

#[tokio::test]
async fn test_room_actor_registration_and_get_schema() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("test-schema").unwrap();
    schema_registry
        .register_schema(schema_id.clone(), create_test_schema())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let manager = RoomManager::new(config, schema_registry, create_test_relay(dir.path()));
    let room_id = RoomId::new("room-1").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .expect("spawn room");

    // Register client
    let (reply_tx, reply_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: ClientId::new("client-alice").unwrap(),
            current_seq: None,
            reply: reply_tx,
        })
        .await
        .unwrap();

    let res: RegisterResponse = reply_rx.await.unwrap().unwrap();
    assert_eq!(res.head_seq, SequenceNumber::new(0));
    assert_eq!(res.schema_id, schema_id);
    assert!(res.schema.has_table_by_name("tasks"));
}

#[tokio::test]
async fn test_room_actor_commit_validation_and_monotonic_sequencing() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("tasks-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let manager = RoomManager::new(config, schema_registry, create_test_relay(dir.path()));
    let room_id = RoomId::new("tasks-room").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .unwrap();

    let client_id = ClientId::new("client-1").unwrap();

    // 0. Commit from unregistered client rejected with Unauthorized
    let op0 = create_insert_op(&schema, 0, "Unregistered attempt");
    let (tx0, rx0) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client_id.clone(),
            mutation_id: MutationId::new([0; 16]),
            last_ack_seq: SequenceNumber::new(0),
            op: op0,
            reply: tx0,
        })
        .await
        .unwrap();
    assert!(matches!(
        rx0.await.unwrap(),
        Err(ServerError::ClientNotRegistered(_))
    ));

    // Register client
    register_client_helper(&sender, client_id.clone())
        .await
        .unwrap();

    // 1. Commit valid mutation
    let op1 = create_insert_op(&schema, 1, "Buy groceries");
    let (tx1, rx1) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client_id.clone(),
            mutation_id: MutationId::new([1; 16]),
            last_ack_seq: SequenceNumber::new(0),
            op: op1,
            reply: tx1,
        })
        .await
        .unwrap();
    let res1: CommitResponse = rx1.await.unwrap().unwrap();
    assert_eq!(res1.assigned_seq, SequenceNumber::new(1));
    assert_eq!(res1.catchup_ops.len(), 1);

    // 2. Commit second valid mutation
    let op2 = create_insert_op(&schema, 2, "Walk the dog");
    let (tx2, rx2) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client_id.clone(),
            mutation_id: MutationId::new([2; 16]),
            last_ack_seq: SequenceNumber::new(1),
            op: op2,
            reply: tx2,
        })
        .await
        .unwrap();
    let res2: CommitResponse = rx2.await.unwrap().unwrap();
    assert_eq!(res2.assigned_seq, SequenceNumber::new(2));

    // 3. Exactly-Once Idempotency test (re-sending mutation 1)
    let op1_dup = create_insert_op(&schema, 1, "Buy groceries");
    let (tx3, rx3) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client_id.clone(),
            mutation_id: MutationId::new([1; 16]),
            last_ack_seq: SequenceNumber::new(0),
            op: op1_dup,
            reply: tx3,
        })
        .await
        .unwrap();
    let res3: CommitResponse = rx3.await.unwrap().unwrap();
    // Returns existing assigned_seq 1 without advancing head
    assert_eq!(res3.assigned_seq, SequenceNumber::new(1));

    // 4. Schema violation test (invalid table_id)
    let invalid_op = Operation::delete(999, PrimaryKey::single(Value::Int(3)), 1000);
    let (tx4, rx4) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id,
            mutation_id: MutationId::new([4; 16]),
            last_ack_seq: SequenceNumber::new(2),
            op: invalid_op,
            reply: tx4,
        })
        .await
        .unwrap();
    let res4 = rx4.await.unwrap();
    assert!(matches!(res4, Err(ServerError::SchemaViolation(_))));
}

#[tokio::test]
async fn test_room_actor_multi_client_concurrency_and_sse_events() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("concurrent-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let manager = RoomManager::new(config, schema_registry, create_test_relay(dir.path()));
    let room_id = RoomId::new("concurrent-room").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .unwrap();

    // Register reader client initially so its cursor holds back proactive pruning
    let (reg_tx, reg_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: ClientId::new("reader").unwrap(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();

    // Subscribe to SSE events
    let (sub_tx, sub_rx) = oneshot::channel();
    sender
        .send(RoomCommand::SubscribeEvents { reply: sub_tx })
        .await
        .unwrap();
    let mut sse_rx = sub_rx.await.unwrap().unwrap();

    // Concurrent commits from 5 clients (20 commits each = 100 total)
    let mut tasks = vec![];
    for client_idx in 0..5 {
        let cmd_tx = sender.clone();
        let schema_clone = schema.clone();
        tasks.push(tokio::spawn(async move {
            let client_id = ClientId::new(format!("worker-{}", client_idx)).unwrap();
            register_client_helper(&cmd_tx, client_id.clone())
                .await
                .unwrap();
            for op_idx in 0..20 {
                let id = client_idx * 100 + op_idx;
                let op = create_insert_op(&schema_clone, id, &format!("task-{}", id));
                let mut mutation_bytes = [0u8; 16];
                mutation_bytes[0] = client_idx as u8;
                mutation_bytes[1] = op_idx as u8;
                let mutation_id = MutationId::new(mutation_bytes);

                let (tx, rx) = oneshot::channel();
                cmd_tx
                    .send(RoomCommand::Commit {
                        client_id: client_id.clone(),
                        mutation_id,
                        last_ack_seq: SequenceNumber::new(0),
                        op,
                        reply: tx,
                    })
                    .await
                    .unwrap();
                rx.await.unwrap().unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    // Verify metrics
    let (metrics_tx, metrics_rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetMetrics { reply: metrics_tx })
        .await
        .unwrap();
    let metrics = metrics_rx.await.unwrap().unwrap();
    assert_eq!(metrics.head_seq, SequenceNumber::new(100));

    // Verify SSE receiver got events
    let mut last_event_seq = SequenceNumber::new(0);
    while let Ok(event) = sse_rx.try_recv() {
        if let RoomEvent::HeadAdvanced(seq) = event {
            last_event_seq = seq;
        }
    }
    assert_eq!(last_event_seq, SequenceNumber::new(100));

    // Verify sync returns all 100 operations in two batches of 50
    let (sync_tx1, sync_rx1) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: ClientId::new("reader").unwrap(),
            from_seq: SequenceNumber::new(0),
            max_batch_size: 50,
            reply: sync_tx1,
        })
        .await
        .unwrap();
    let sync_res1: SyncBatchResponse = sync_rx1.await.unwrap().unwrap();
    assert_eq!(sync_res1.ops.len(), 50);
    assert!(sync_res1.has_more);

    let (sync_tx2, sync_rx2) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: ClientId::new("reader").unwrap(),
            from_seq: SequenceNumber::new(50),
            max_batch_size: 50,
            reply: sync_tx2,
        })
        .await
        .unwrap();
    let sync_res2: SyncBatchResponse = sync_rx2.await.unwrap().unwrap();
    assert_eq!(sync_res2.ops.len(), 50);
    assert!(!sync_res2.has_more);
}

#[tokio::test]
async fn test_room_actor_client_lifecycle_and_dormant_behind_compaction() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("lifecycle-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    // Aggressive test policy for rapid compaction, with a 1 second lease timeout
    let lifecycle_policy = RoomLifecyclePolicy {
        lease_timeout: Duration::from_secs(1),
        ..RoomLifecyclePolicy::test_policy()
    };

    let manager = RoomManager::new(config, schema_registry, create_test_relay(dir.path()))
        .with_default_policy(lifecycle_policy);
    let room_id = RoomId::new("lifecycle-room").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .unwrap();

    let alice = ClientId::new("alice").unwrap();
    let bob = ClientId::new("bob").unwrap();

    // Register alice and bob
    let (reg_tx, reg_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: alice.clone(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();

    let (reg_tx2, reg_rx2) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: bob.clone(),
            current_seq: None,
            reply: reg_tx2,
        })
        .await
        .unwrap();
    reg_rx2.await.unwrap().unwrap();

    // Produce 10 commits
    for i in 1..=10 {
        let op = create_insert_op(&schema, i, &format!("task-{}", i));
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: alice.clone(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(i as u64 - 1),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    // Alice acknowledges seq 10 via explicit Ack
    let (ack_tx, ack_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Ack {
            client_id: alice.clone(),
            ack_seq: SequenceNumber::new(10),
            reply: ack_tx,
        })
        .await
        .unwrap();
    ack_rx.await.unwrap().unwrap();

    // Bob stays offline at seq 0. Wait 1.2s for lease timeout -> Bob becomes Disconnected
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Trigger maintenance by committing more ops to rotate and compress
    for i in 11..=20 {
        let op = create_insert_op(&schema, i, &format!("task-{}", i));
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: alice.clone(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(i as u64 - 1),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    // Wait for test policy cold TTL to prune older segments
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Check Bob's state
    let (metrics_tx, metrics_rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetMetrics { reply: metrics_tx })
        .await
        .unwrap();
    let metrics = metrics_rx.await.unwrap().unwrap();

    // If tail_seq advanced beyond 0, Bob's sync from 0 should be BehindCompaction
    if metrics.tail_seq > SequenceNumber::new(1) {
        let (sync_tx, sync_rx) = oneshot::channel();
        sender
            .send(RoomCommand::Sync {
                client_id: bob.clone(),
                from_seq: SequenceNumber::new(0),
                max_batch_size: 50,
                reply: sync_tx,
            })
            .await
            .unwrap();
        let res = sync_rx.await.unwrap();
        assert!(
            matches!(res, Err(ServerError::BehindCompaction)),
            "Expected BehindCompaction for bob but got: {:?}",
            res
        );
    }
}

#[tokio::test]
async fn test_room_actor_recovery_retains_state_and_head_seq() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("recovery-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let room_id = RoomId::new("crash-test-room").unwrap();

    // Phase 1: Spawn, commit 5 ops, then close room actor
    {
        let manager = RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&schema_registry),
            create_test_relay(dir.path()),
        );
        let sender = manager
            .get_or_spawn(&room_id, Some(&schema_id))
            .await
            .unwrap();
        register_client_helper(&sender, ClientId::new("c1").unwrap())
            .await
            .unwrap();

        for i in 1..=5 {
            let op = create_insert_op(&schema, i, &format!("task-{}", i));
            let (tx, rx) = oneshot::channel();
            sender
                .send(RoomCommand::Commit {
                    client_id: ClientId::new("c1").unwrap(),
                    mutation_id: MutationId::new([i as u8; 16]),
                    last_ack_seq: SequenceNumber::new(i as u64 - 1),
                    op,
                    reply: tx,
                })
                .await
                .unwrap();
            rx.await.unwrap().unwrap();
        }

        // Gracefully shutdown room actor
        manager.shutdown_room(&room_id).await;
    }

    // Phase 2: Respawn actor from same directory without passing schema_id explicitly
    {
        let manager2 = RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&schema_registry),
            create_test_relay(dir.path()),
        );
        let sender2 = manager2
            .get_or_spawn(&room_id, None)
            .await
            .expect("should recover room metadata");

        // Verify head_seq recovered as 5
        let (metrics_tx, metrics_rx) = oneshot::channel();
        sender2
            .send(RoomCommand::GetMetrics { reply: metrics_tx })
            .await
            .unwrap();
        let metrics = metrics_rx.await.unwrap().unwrap();
        assert_eq!(metrics.head_seq, SequenceNumber::new(5));

        // Verify DedupLruCache rehydrated (mutation 5 is duplicate)
        let op5_dup = create_insert_op(&schema, 5, "task-5");
        let (tx, rx) = oneshot::channel();
        sender2
            .send(RoomCommand::Commit {
                client_id: ClientId::new("c1").unwrap(),
                mutation_id: MutationId::new([5; 16]),
                last_ack_seq: SequenceNumber::new(4),
                op: op5_dup,
                reply: tx,
            })
            .await
            .unwrap();
        let res = rx.await.unwrap().unwrap();
        assert_eq!(res.assigned_seq, SequenceNumber::new(5));

        // Commit op 6 successfully
        let op6 = create_insert_op(&schema, 6, "task-6");
        let (tx6, rx6) = oneshot::channel();
        sender2
            .send(RoomCommand::Commit {
                client_id: ClientId::new("c1").unwrap(),
                mutation_id: MutationId::new([6; 16]),
                last_ack_seq: SequenceNumber::new(5),
                op: op6,
                reply: tx6,
            })
            .await
            .unwrap();
        let res6 = rx6.await.unwrap().unwrap();
        assert_eq!(res6.assigned_seq, SequenceNumber::new(6));
    }
}

#[tokio::test]
async fn test_room_actor_cursor_advances_only_on_client_ack() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("ack-test-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let manager = RoomManager::new(config, schema_registry, create_test_relay(dir.path()));
    let room_id = RoomId::new("ack-test-room").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .unwrap();

    let client = ClientId::new("c-reader").unwrap();

    // 1. Register client: cursor is at 0
    let (reg_tx, reg_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client.clone(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();

    // 2. Producer commits 5 operations (seq 1..=5)
    register_client_helper(&sender, ClientId::new("producer").unwrap())
        .await
        .unwrap();
    for i in 1..=5 {
        let op = create_insert_op(&schema, i, &format!("task-{}", i));
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: ClientId::new("producer").unwrap(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(i as u64 - 1),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    // 3. Client calls Sync from 0, receiving operations 1..=5
    let (sync_tx, sync_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: client.clone(),
            from_seq: SequenceNumber::new(0),
            max_batch_size: 10,
            reply: sync_tx,
        })
        .await
        .unwrap();
    let sync_res = sync_rx.await.unwrap().unwrap();
    assert_eq!(sync_res.ops.len(), 5);

    // 4. CRITICAL CHECK: The server delivered ops 1..=5, but client's recorded cursor MUST STILL BE 0!
    // Because the client has NOT sent its ACK yet!
    let (cur_tx, cur_rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetClientCursor {
            client_id: client.clone(),
            reply: cur_tx,
        })
        .await
        .unwrap();
    let cursor_before_ack = cur_rx.await.unwrap().unwrap();
    assert_eq!(
        cursor_before_ack,
        Some(SequenceNumber::new(0)),
        "Cursor must NOT advance upon delivery of SyncResponse, only upon client ACK"
    );

    // 5. Client sends Heartbeat (liveness only) - cursor MUST STILL BE 0!
    let (hb_tx, hb_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Heartbeat {
            client_id: client.clone(),
            reply: hb_tx,
        })
        .await
        .unwrap();
    hb_rx.await.unwrap().unwrap();

    let (cur_tx_hb, cur_rx_hb) = oneshot::channel();
    sender
        .send(RoomCommand::GetClientCursor {
            client_id: client.clone(),
            reply: cur_tx_hb,
        })
        .await
        .unwrap();
    assert_eq!(
        cur_rx_hb.await.unwrap().unwrap(),
        Some(SequenceNumber::new(0)),
        "Heartbeat is pure liveness and must NOT advance cursor"
    );

    // 6. Client confirms having applied the batch via explicit Ack
    let (ack_tx, ack_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Ack {
            client_id: client.clone(),
            ack_seq: SequenceNumber::new(5),
            reply: ack_tx,
        })
        .await
        .unwrap();
    ack_rx.await.unwrap().unwrap();

    // 7. NOW the client's confirmed cursor in the server is 5!
    let (cur_tx2, cur_rx2) = oneshot::channel();
    sender
        .send(RoomCommand::GetClientCursor {
            client_id: client.clone(),
            reply: cur_tx2,
        })
        .await
        .unwrap();
    let cursor_after_ack = cur_rx2.await.unwrap().unwrap();
    assert_eq!(
        cursor_after_ack,
        Some(SequenceNumber::new(5)),
        "Cursor must advance after explicit client ACK"
    );
}

#[tokio::test]
async fn test_room_actor_retention_anchor_protects_deltas_during_snapshot() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("anchor-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(300),
            ServerConfig::default().max_snapshot_bytes,
        )
        .unwrap(),
    );
    // Small segments so proactive pruning has something to delete, but default TTLs: with
    // the short test TTLs, a slow run could prune the cold segments by age, which is not the
    // behavior under test.
    let lifecycle_policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        lease_timeout: Duration::from_secs(60),
        ..RoomLifecyclePolicy::default()
    };
    let manager = RoomManager::new(config, schema_registry, Arc::clone(&relay))
        .with_default_policy(lifecycle_policy);
    let room_id = RoomId::new("anchor-room").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .unwrap();

    // 1. Alice registers and commits 20 operations (1..=20). With 5 operations per segment
    // the log holds several sealed segments, so a floor above the snapshot can delete the
    // deltas that follow it. Her commits report cursor 0, so no maintenance tick can prune
    // anything before the snapshot is staged.
    let alice = ClientId::new("alice").unwrap();
    let (reg_tx, reg_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: alice.clone(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();

    for i in 1..=20 {
        let op = create_insert_op(&schema, i, &format!("task-{}", i));
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: alice.clone(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    // 2. Stage an active snapshot at seq 5 in the relay (it must lie inside the log range)
    let body = b"fake-snapshot-data-seq-5";
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, body);
    let mut envelope = header.to_bytes().to_vec();
    envelope.extend_from_slice(body);
    relay
        .stage_snapshot(
            &room_id,
            SequenceNumber::new(5),
            bytes::Bytes::from(envelope),
            async {
                let (tail_seq, head_seq) = manager.log_bounds(&room_id).await?;
                Ok(zemdb_server::relay::LogBounds { tail_seq, head_seq })
            },
        )
        .await
        .unwrap();

    // 3. Alice, the only registered client, acknowledges seq 20. Her cursor alone would put
    // the pruning floor at 20 and delete every sealed segment up to seq 15; the Retention
    // Anchor at seq 5 must keep deltas 6..=20.
    let (ack_tx, ack_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Ack {
            client_id: alice.clone(),
            ack_seq: SequenceNumber::new(20),
            reply: ack_tx,
        })
        .await
        .unwrap();
    ack_rx.await.unwrap().unwrap();

    // 4. Bob (onboarding client) registers with current_seq = None
    let bob = ClientId::new("bob").unwrap();
    let (reg_bob_tx, reg_bob_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: bob.clone(),
            current_seq: None,
            reply: reg_bob_tx,
        })
        .await
        .unwrap();
    let bob_reg = reg_bob_rx.await.unwrap().unwrap();
    assert_eq!(bob_reg.head_seq, SequenceNumber::new(20));
    assert_eq!(bob_reg.active_snapshot_seq, Some(SequenceNumber::new(5)));
    assert!(
        bob_reg.tail_seq <= SequenceNumber::new(6),
        "the anchor must keep the deltas after the snapshot (tail {})",
        bob_reg.tail_seq
    );

    // 5. Bob applies the snapshot at 5 and requests the deltas after it: Sync { from_seq: 5 }.
    // These deltas MUST be present thanks to the Retention Anchor.
    let (sync_tx, sync_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: bob.clone(),
            from_seq: SequenceNumber::new(5),
            max_batch_size: 50,
            reply: sync_tx,
        })
        .await
        .unwrap();
    let sync_res = sync_rx
        .await
        .unwrap()
        .expect("Sync after snapshot must succeed");
    assert_eq!(sync_res.ops.len(), 15);
    assert_eq!(sync_res.ops[0].seq, SequenceNumber::new(6));
    assert_eq!(sync_res.ops[14].seq, SequenceNumber::new(20));
}

#[tokio::test]
async fn test_room_actor_rejects_future_ack_and_commit_sequences() {
    let dir = tempdir().unwrap();
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("test-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let config = Arc::new(ServerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    });

    let manager = RoomManager::new(config, schema_registry, create_test_relay(dir.path()));
    let room_id = RoomId::new("room-seq-safety").unwrap();
    let sender = manager
        .get_or_spawn(&room_id, Some(&schema_id))
        .await
        .expect("spawn room");

    let alice = ClientId::new("alice").unwrap();
    let alice_reg = register_client_helper(&sender, alice.clone())
        .await
        .unwrap();
    assert_eq!(alice_reg.head_seq, SequenceNumber::new(0));

    // 1. Commit 3 operations (seq 1, 2, 3)
    for i in 1u64..=3u64 {
        let op = create_insert_op(&schema, i as i64, &format!("Task {i}"));
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: alice.clone(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(i - 1),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        let commit_res = rx.await.unwrap().unwrap();
        assert_eq!(commit_res.assigned_seq, SequenceNumber::new(i));
    }

    // 2. Alice sends an invalid Ack far into the future (e.g. u64::MAX or 999)
    let (bad_ack_tx, bad_ack_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Ack {
            client_id: alice.clone(),
            ack_seq: SequenceNumber::new(999),
            reply: bad_ack_tx,
        })
        .await
        .unwrap();
    let bad_ack_res = bad_ack_rx.await.unwrap();
    match bad_ack_res {
        Err(ServerError::InvalidSequence { expected, actual }) => {
            assert_eq!(expected, SequenceNumber::new(3));
            assert_eq!(actual, SequenceNumber::new(999));
        }
        other => panic!("Expected ServerError::InvalidSequence, got {:?}", other),
    }

    // 3. Verify Alice's cursor did NOT advance to 999: it stays at the cursor reported by
    // her last accepted commit
    let (cursor_tx, cursor_rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetClientCursor {
            client_id: alice.clone(),
            reply: cursor_tx,
        })
        .await
        .unwrap();
    let cursor = cursor_rx.await.unwrap().unwrap();
    assert_eq!(cursor, Some(SequenceNumber::new(2)));

    // 4. Bob registers and syncs from sequence 0; deltas must NOT have been pruned
    let bob = ClientId::new("bob").unwrap();
    register_client_helper(&sender, bob.clone()).await.unwrap();

    let (sync_tx, sync_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: bob.clone(),
            from_seq: SequenceNumber::new(0),
            max_batch_size: 50,
            reply: sync_tx,
        })
        .await
        .unwrap();
    let sync_res = sync_rx.await.unwrap().expect("Sync from 0 must succeed");
    assert_eq!(sync_res.ops.len(), 3);
    assert_eq!(sync_res.ops[0].seq, SequenceNumber::new(1));
    assert_eq!(sync_res.ops[2].seq, SequenceNumber::new(3));

    // 5. Alice attempts to commit with last_ack_seq > head_seq (e.g. 50 > 3)
    let invalid_commit_op = create_insert_op(&schema, 100, "Invalid Commit");
    let (invalid_commit_tx, invalid_commit_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: alice.clone(),
            mutation_id: MutationId::new([0xFE; 16]),
            last_ack_seq: SequenceNumber::new(50),
            op: invalid_commit_op,
            reply: invalid_commit_tx,
        })
        .await
        .unwrap();
    let invalid_commit_res = invalid_commit_rx.await.unwrap();
    match invalid_commit_res {
        Err(ServerError::InvalidSequence { expected, actual }) => {
            assert_eq!(expected, SequenceNumber::new(3));
            assert_eq!(actual, SequenceNumber::new(50));
        }
        other => panic!("Expected ServerError::InvalidSequence, got {:?}", other),
    }
}
