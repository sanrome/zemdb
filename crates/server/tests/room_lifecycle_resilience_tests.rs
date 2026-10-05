use axum::http::StatusCode;
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;
use zemdb_core::*;
use zemdb_server::actor::command::RoomCommand;
use zemdb_server::actor::manager::RoomManager;
use zemdb_server::api::auth::generate_client_token;
use zemdb_server::api::router::{build_router, AppState};
use zemdb_server::config::ServerConfig;
use zemdb_server::error::ServerError;
use zemdb_server::log::WarmDiskLog;
use zemdb_server::relay::SnapshotRelay;
use zemdb_server::schema_registry::SchemaRegistry;

struct LifecycleTestServer {
    pub base_url: String,
    pub config: Arc<ServerConfig>,
    pub schema_registry: Arc<SchemaRegistry>,
    pub room_manager: Arc<RoomManager>,
    pub client: reqwest::Client,
}

impl LifecycleTestServer {
    async fn start_with_dir(data_dir: std::path::PathBuf) -> Self {
        let config = Arc::new(ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            data_dir: data_dir.clone(),
            auth_secret: "lifecycle_cluster_secret_key_12345678".to_string(),
            admin_secret: "lifecycle_admin_secret_key_123456789".to_string(),
            lease_timeout_secs: 60,
            dedup_lru_capacity: 1000,
            snapshot_ttl_secs: 60,
        });

        let schemas_dir = data_dir.join("schemas");
        let schema_registry = Arc::new(SchemaRegistry::new(schemas_dir).unwrap());
        let snapshots_dir = data_dir.join("snapshots");
        let snapshot_relay = Arc::new(
            SnapshotRelay::new(snapshots_dir, Duration::from_secs(config.snapshot_ttl_secs))
                .unwrap(),
        );
        let room_manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&schema_registry),
            Arc::clone(&snapshot_relay),
        ));

        let state = AppState::new(
            Arc::clone(&config),
            Arc::clone(&schema_registry),
            Arc::clone(&room_manager),
            snapshot_relay,
        );
        let app = build_router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let base_url = format!("http://{}", addr);
        let client = reqwest::Client::new();

        Self {
            base_url,
            config,
            schema_registry,
            room_manager,
            client,
        }
    }

    pub async fn shutdown(&self) {
        self.room_manager.shutdown_all().await;
    }
}

fn create_test_schema() -> Schema {
    let table = TableSchema::builder("documents")
        .primary_key("doc_id", DataType::Int)
        .column("content", DataType::String)
        .build()
        .expect("valid table schema");
    Schema::from_tables(vec![table])
}

fn create_test_op(schema: &Schema, id: i64, content: &str) -> Operation {
    let row = RowBuilder::new()
        .set("doc_id", id)
        .set("content", content)
        .build();
    schema
        .to_operation_insert("documents", &row, 1000)
        .expect("valid insert op")
}

#[tokio::test]
async fn test_concurrent_get_or_spawn_elimination_of_race_condition() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let config = Arc::new(ServerConfig {
        data_dir: data_dir.clone(),
        ..Default::default()
    });

    let schemas_dir = data_dir.join("schemas");
    let schema_registry = Arc::new(SchemaRegistry::new(schemas_dir).unwrap());
    let schema_id = SchemaId::new("doc-schema").unwrap();
    schema_registry
        .register_schema(schema_id.clone(), create_test_schema())
        .unwrap();

    let relay = Arc::new(SnapshotRelay::new_in_memory(Duration::from_secs(60)));
    let manager = Arc::new(RoomManager::new(config, schema_registry, relay));
    let room_id = RoomId::new("race-condition-room").unwrap();

    let mut handles = Vec::new();
    for _ in 0..50 {
        let mgr = Arc::clone(&manager);
        let rid = room_id.clone();
        let sid = schema_id.clone();
        let handle = tokio::spawn(async move { mgr.get_or_spawn(&rid, Some(&sid)).await });
        handles.push(handle);
    }

    let mut senders = Vec::new();
    for handle in handles {
        let res = handle.await.expect("join task");
        let sender = res.expect("get_or_spawn succeeded");
        senders.push(sender);
    }

    assert_eq!(senders.len(), 50);

    // Verify all senders communicate with the exact same active room actor
    let test_client = ClientId::new("probe-client").unwrap();
    let (reg_tx, reg_rx) = oneshot::channel();
    senders[0]
        .send(RoomCommand::RegisterClient {
            client_id: test_client.clone(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    let reg_result = reg_rx.await.unwrap().unwrap();
    assert_eq!(reg_result.head_seq, SequenceNumber::new(0));

    // Verify through the 50th sender that the registered client is visible
    let (cur_tx, cur_rx) = oneshot::channel();
    senders[49]
        .send(RoomCommand::GetClientCursor {
            client_id: test_client,
            reply: cur_tx,
        })
        .await
        .unwrap();
    let cursor = cur_rx.await.unwrap().unwrap();
    assert_eq!(cursor, SequenceNumber::new(0));
}

#[tokio::test]
async fn test_graceful_room_deletion_and_directory_cleanup() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let config = Arc::new(ServerConfig {
        data_dir: data_dir.clone(),
        ..Default::default()
    });

    let schemas_dir = data_dir.join("schemas");
    let schema_registry = Arc::new(SchemaRegistry::new(schemas_dir).unwrap());
    let schema_id = SchemaId::new("doc-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let relay = Arc::new(SnapshotRelay::new_in_memory(Duration::from_secs(60)));
    let manager = Arc::new(RoomManager::new(config, schema_registry, relay));
    let room_id = RoomId::new("deletion-target-room").unwrap();

    // Create room and append an operation to disk
    manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .await
        .unwrap();
    let sender = manager.get_room(&room_id).unwrap();

    let client_id = ClientId::new("deleter").unwrap();
    let (reg_tx, reg_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();

    let op = create_test_op(&schema, 1, "before delete");
    let (commit_tx, commit_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id,
            mutation_id: MutationId::new([1u8; 16]),
            last_ack_seq: SequenceNumber::new(0),
            op,
            reply: commit_tx,
        })
        .await
        .unwrap();
    commit_rx.await.unwrap().unwrap();

    let room_dir = data_dir.join("rooms").join(room_id.as_str());
    assert!(room_dir.exists(), "Room directory must exist on disk");

    // Perform graceful deletion: shuts down actor, awaits loop termination, and cleans up directory
    manager.delete_room(&room_id).await.unwrap();

    assert!(
        !room_dir.exists(),
        "Room directory must be completely removed from disk after graceful deletion"
    );

    // Verify room is no longer cached or accessible
    let lookup_res = manager.get_room(&room_id);
    assert!(lookup_res.is_none());
}

#[tokio::test]
async fn test_data_plane_lazy_reactivation_after_restart() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let schema_id = SchemaId::new("doc-schema").unwrap();
    let schema = create_test_schema();
    let room_id = RoomId::new("lazy-reactivation-room").unwrap();

    let client_id = ClientId::new("reactivating-client").unwrap();

    // Server Phase 1: Create room, register client, commit 2 operations
    {
        let server1 = LifecycleTestServer::start_with_dir(data_dir.clone()).await;
        server1
            .schema_registry
            .register_schema(schema_id.clone(), schema.clone())
            .unwrap();

        server1
            .room_manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await
            .unwrap();

        let token = generate_client_token(
            &client_id,
            &room_id,
            Duration::from_secs(300),
            &server1.config.auth_secret,
        );

        let reg_msg = ClientMessage::RegisterClient {
            correlation_id: CorrelationId::new(1),
            room_id: room_id.clone(),
            client_id: client_id.clone(),
            auth_token: token.clone(),
            current_seq: None,
        };
        let reg_resp = server1
            .client
            .post(format!("{}/rooms/{}/register", server1.base_url, room_id))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(encode_message(&reg_msg).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(reg_resp.status(), StatusCode::OK);

        for i in 1..=2 {
            let commit_msg = ClientMessage::Commit {
                correlation_id: CorrelationId::new(10 + i),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(i - 1),
                op: create_test_op(&schema, i as i64, &format!("doc-{i}")),
            };
            let commit_resp = server1
                .client
                .post(format!("{}/rooms/{}/commit", server1.base_url, room_id))
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(encode_message(&commit_msg).unwrap())
                .send()
                .await
                .unwrap();
            assert_eq!(commit_resp.status(), StatusCode::OK);
        }

        server1.shutdown().await;
    }

    // Server Phase 2: Start a brand new server instance on the same data directory.
    // The in-memory cache `rooms` is empty. The control plane `/admin/rooms` is NOT invoked.
    {
        let server2 = LifecycleTestServer::start_with_dir(data_dir.clone()).await;

        let token = generate_client_token(
            &client_id,
            &room_id,
            Duration::from_secs(300),
            &server2.config.auth_secret,
        );

        // Send a commit operation directly to the data plane endpoint.
        // The data plane must lazily reactivate and rehydrate the room actor from disk.
        let commit_msg3 = ClientMessage::Commit {
            correlation_id: CorrelationId::new(30),
            room_id: room_id.clone(),
            client_id: client_id.clone(),
            mutation_id: MutationId::new([3u8; 16]),
            last_ack_seq: SequenceNumber::new(2),
            op: create_test_op(&schema, 3, "doc-3-reactivated"),
        };
        let commit_resp = server2
            .client
            .post(format!("{}/rooms/{}/commit", server2.base_url, room_id))
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(encode_message(&commit_msg3).unwrap())
            .send()
            .await
            .unwrap();

        assert_eq!(
            commit_resp.status(),
            StatusCode::OK,
            "Data plane should lazily reactivate room without control plane pre-warming"
        );

        let bytes = commit_resp.bytes().await.unwrap();
        let server_msg: ServerMessage = decode_message(&bytes).unwrap();
        match server_msg {
            ServerMessage::CommitAck { assigned_seq, .. } => {
                assert_eq!(assigned_seq, SequenceNumber::new(3));
            }
            other => panic!("Expected CommitAck, got: {:?}", other),
        }
    }
}

#[test]
fn test_multi_process_file_lock_collision_on_active_wal() {
    let dir = tempdir().unwrap();
    let mut log1 = WarmDiskLog::open_or_create(dir.path()).unwrap();

    let pk = PrimaryKey::single(Value::Int(1));
    let op = Operation::delete(1, pk, 1000);
    let seq_op = SequencedOperation::new(SequenceNumber::new(1), op);

    // Appending a record acquires and retains exclusive flock on active.wal
    log1.append_record(&seq_op, None).unwrap();

    // Opening a second WarmDiskLog instance against the same directory must fail with RoomLocked
    let log2_res = WarmDiskLog::open_or_create(dir.path());
    match log2_res {
        Err(ServerError::RoomLocked(msg)) => {
            assert!(
                msg.contains("active.wal locked by another process"),
                "Expected RoomLocked error message, got: {}",
                msg
            );
        }
        Ok(_) => panic!("Expected RoomLocked error, but second open succeeded"),
        Err(other) => panic!("Expected RoomLocked error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_max_batch_size_clamped_in_sync() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let config = Arc::new(ServerConfig {
        data_dir: data_dir.clone(),
        ..Default::default()
    });

    let schemas_dir = data_dir.join("schemas");
    let schema_registry = Arc::new(SchemaRegistry::new(schemas_dir).unwrap());
    let schema_id = SchemaId::new("doc-schema").unwrap();
    let schema = create_test_schema();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let relay = Arc::new(SnapshotRelay::new_in_memory(Duration::from_secs(60)));
    let manager = Arc::new(RoomManager::new(config, schema_registry, relay));
    let room_id = RoomId::new("clamp-batch-size-room").unwrap();

    manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .await
        .unwrap();
    let sender = manager.get_room(&room_id).unwrap();

    let reader_client = ClientId::new("sync-reader").unwrap();
    let (reg_tx, reg_rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: reader_client.clone(),
            current_seq: None,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();

    // Commit 1005 operations to room actor. The client keeps reporting cursor 0: a cursor
    // reported by an accepted commit advances the retention floor, and this test needs the
    // whole log retained to sync it from the start.
    for i in 1..=1005u64 {
        let op = create_test_op(&schema, i as i64, "item");
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: reader_client.clone(),
                mutation_id: MutationId::from_u128(i as u128),
                last_ack_seq: SequenceNumber::new(0),
                op,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    // Client requests sync with excessively large batch size (e.g. 50,000)
    let (sync_tx, sync_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: reader_client.clone(),
            from_seq: SequenceNumber::new(0),
            max_batch_size: 50_000,
            reply: sync_tx,
        })
        .await
        .unwrap();

    let sync_res = sync_rx.await.unwrap().unwrap();
    assert_eq!(
        sync_res.ops.len(),
        1000,
        "max_batch_size must be clamped to the maximum allowed limit of 1000"
    );

    // Client requests sync with batch size of 0, which must be clamped to 1
    let (sync0_tx, sync0_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Sync {
            client_id: reader_client,
            from_seq: SequenceNumber::new(0),
            max_batch_size: 0,
            reply: sync0_tx,
        })
        .await
        .unwrap();

    let sync0_res = sync0_rx.await.unwrap().unwrap();
    assert_eq!(
        sync0_res.ops.len(),
        1,
        "max_batch_size 0 must be clamped to minimum allowed limit of 1"
    );
}

#[tokio::test]
async fn test_snapshot_multipart_chunk_upload_and_blake3_verification() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let server = LifecycleTestServer::start_with_dir(data_dir).await;

    let room_id = RoomId::new("multipart-snapshot-room").unwrap();
    let client_id = ClientId::new("snapshot-uploader").unwrap();

    let client_token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );

    // 1. Prepare 300 KB synthetic snapshot data
    let snapshot_data = vec![0x42u8; 300 * 1024];
    let snapshot_hash = ServerMessage::compute_snapshot_hash(&snapshot_data);
    let total_bytes = snapshot_data.len() as u64;
    let chunk_size = 100 * 1024;
    let total_chunks = 3;

    // 2. Unauthenticated upload chunk request -> 401 Unauthorized
    let unauth_msg = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(10),
        chunk_index: 0,
        total_chunks,
        total_bytes,
        snapshot_hash,
        data: Bytes::copy_from_slice(&snapshot_data[0..chunk_size]),
    };
    let unauth_resp = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/upload-chunk",
            server.base_url, room_id
        ))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&unauth_msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

    // 3. Upload chunk 0
    let chunk0_msg = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(2),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(10),
        chunk_index: 0,
        total_chunks,
        total_bytes,
        snapshot_hash,
        data: Bytes::copy_from_slice(&snapshot_data[0..chunk_size]),
    };
    let resp0 = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/upload-chunk",
            server.base_url, room_id
        ))
        .header(AUTHORIZATION, format!("Bearer {}", client_token))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&chunk0_msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp0.status(), StatusCode::OK);
    let ack0: ServerMessage = decode_message(&resp0.bytes().await.unwrap()).unwrap();
    match ack0 {
        ServerMessage::SnapshotUploadChunkAck {
            chunk_index,
            total_chunks: tc,
            staged,
            ..
        } => {
            assert_eq!(chunk_index, 0);
            assert_eq!(tc, 3);
            assert!(!staged);
        }
        other => panic!("Expected SnapshotUploadChunkAck, got: {:?}", other),
    }

    // 4. Upload chunk 1
    let chunk1_msg = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(3),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(10),
        chunk_index: 1,
        total_chunks,
        total_bytes,
        snapshot_hash,
        data: Bytes::copy_from_slice(&snapshot_data[chunk_size..2 * chunk_size]),
    };
    let resp1 = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/upload-chunk",
            server.base_url, room_id
        ))
        .header(AUTHORIZATION, format!("Bearer {}", client_token))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&chunk1_msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let ack1: ServerMessage = decode_message(&resp1.bytes().await.unwrap()).unwrap();
    match ack1 {
        ServerMessage::SnapshotUploadChunkAck {
            chunk_index,
            staged,
            ..
        } => {
            assert_eq!(chunk_index, 1);
            assert!(!staged);
        }
        other => panic!("Expected SnapshotUploadChunkAck, got: {:?}", other),
    }

    // 5. Upload final chunk 2: must consolidate and stage the full snapshot
    let chunk2_msg = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(4),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(10),
        chunk_index: 2,
        total_chunks,
        total_bytes,
        snapshot_hash,
        data: Bytes::copy_from_slice(&snapshot_data[2 * chunk_size..]),
    };
    let resp2 = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/upload-chunk",
            server.base_url, room_id
        ))
        .header(AUTHORIZATION, format!("Bearer {}", client_token))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&chunk2_msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let ack2: ServerMessage = decode_message(&resp2.bytes().await.unwrap()).unwrap();
    match ack2 {
        ServerMessage::SnapshotUploadChunkAck {
            chunk_index,
            staged,
            ..
        } => {
            assert_eq!(chunk_index, 2);
            assert!(staged, "Final chunk must trigger snapshot staging");
        }
        other => panic!("Expected SnapshotUploadChunkAck, got: {:?}", other),
    }

    // 6. Request a chunk of the staged snapshot to confirm availability and BLAKE3 integrity
    let req_chunk = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(5),
        room_id: room_id.clone(),
        chunk_index: 0,
        chunk_size: 100 * 1024,
    };
    let download_resp = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/chunk",
            server.base_url, room_id
        ))
        .header(AUTHORIZATION, format!("Bearer {}", client_token))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&req_chunk).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(download_resp.status(), StatusCode::OK);
    let chunk_msg: ServerMessage = decode_message(&download_resp.bytes().await.unwrap()).unwrap();
    match chunk_msg {
        ServerMessage::SnapshotChunk {
            snapshot_hash: returned_hash,
            total_bytes: returned_bytes,
            ..
        } => {
            assert_eq!(returned_hash, snapshot_hash);
            assert_eq!(returned_bytes, total_bytes);
        }
        other => panic!("Expected SnapshotChunk, got: {:?}", other),
    }

    // 7. Verify corrupted upload detection (tampered data causing BLAKE3 digest mismatch)
    let corrupt_room = RoomId::new("corrupt-snapshot-room").unwrap();
    let corrupt_token = generate_client_token(
        &client_id,
        &corrupt_room,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );
    let single_chunk_tampered = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(6),
        room_id: corrupt_room.clone(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: 4,
        snapshot_hash: [0xFF; 32], // incorrect hash
        data: Bytes::from_static(b"test"),
    };
    let corrupt_resp = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/upload-chunk",
            server.base_url, corrupt_room
        ))
        .header(AUTHORIZATION, format!("Bearer {}", corrupt_token))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&single_chunk_tampered).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(corrupt_resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn test_multi_thread_warm_disk_log_flock_concurrency_stress() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // 1. Initial log opens and appends a record, acquiring and holding kernel flock on active.wal
    let mut initial_log = WarmDiskLog::open_or_create(&path).unwrap();
    let pk0 = PrimaryKey::single(Value::Int(0));
    let op0 = Operation::delete(1, pk0, 100);
    let seq_op0 = SequencedOperation::new(SequenceNumber::new(1), op0);
    initial_log.append_record(&seq_op0, None).unwrap();

    // 2. Spawn 12 concurrent threads contending to open_or_create against the locked directory
    const NUM_THREADS: usize = 12;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(NUM_THREADS));
    let mut handles = Vec::new();

    for _ in 0..NUM_THREADS {
        let b = std::sync::Arc::clone(&barrier);
        let p = path.clone();

        handles.push(std::thread::spawn(move || {
            b.wait();
            WarmDiskLog::open_or_create(&p)
        }));
    }

    let mut locked_errors = 0;
    for handle in handles {
        let res = handle.join().unwrap();
        match res {
            Err(ServerError::RoomLocked(msg)) => {
                assert!(msg.contains("active.wal locked by another process"));
                locked_errors += 1;
            }
            Ok(_) => {
                panic!("Concurrent open_or_create must not succeed while active.wal is locked")
            }
            Err(other) => panic!("Expected RoomLocked, got: {other:?}"),
        }
    }

    assert_eq!(
        locked_errors, NUM_THREADS,
        "All 12 concurrent threads must be rejected with RoomLocked"
    );

    // 3. Drop initial_log, releasing the kernel flock
    drop(initial_log);

    // 4. Now a subsequent instance can successfully acquire the lock and append records
    let mut next_log = WarmDiskLog::open_or_create(&path).unwrap();
    let pk = PrimaryKey::single(Value::Int(99));
    let op = Operation::delete(1, pk, 9999);
    let seq_op = SequencedOperation::new(SequenceNumber::new(2), op);
    next_log.append_record(&seq_op, None).unwrap();
    assert_eq!(next_log.active_end_seq().unwrap().get(), 2);
}
