use std::sync::Arc;
use std::time::Duration;
use axum::http::StatusCode;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use rimdb_core::*;
use rimdb_server::actor::command::RoomMetrics;
use rimdb_server::actor::manager::{RoomManager, RoomMetadata};
use rimdb_server::api::auth::generate_client_token;
use rimdb_server::api::control_plane::{AddColumnRequest, CreateRoomRequest, CreateSchemaRequest};
use rimdb_server::api::router::{build_router, AppState};
use rimdb_server::config::ServerConfig;
use rimdb_server::relay::SnapshotRelay;
use rimdb_server::schema_registry::SchemaRegistry;
use tempfile::tempdir;

struct TestServer {
    pub base_url: String,
    pub config: Arc<ServerConfig>,
    pub schema_registry: Arc<SchemaRegistry>,
    pub room_manager: Arc<RoomManager>,
    pub _temp_dir: tempfile::TempDir,
    pub client: reqwest::Client,
}

impl TestServer {
    async fn start() -> Self {
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let config = Arc::new(ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            data_dir: data_dir.clone(),
            auth_secret: "test_cluster_secret_key_12345678".to_string(),
            admin_secret: "test_admin_secret_key_123456789".to_string(),
            lease_timeout_secs: 60,
            dedup_lru_capacity: 1000,
        });

        let schemas_dir = data_dir.join("schemas");
        let schema_registry = Arc::new(SchemaRegistry::new(schemas_dir).unwrap());
        let room_manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&schema_registry),
        ));
        let snapshot_relay = Arc::new(SnapshotRelay::new(Duration::from_secs(60)));

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
            _temp_dir: temp_dir,
            client,
        }
    }
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

#[tokio::test]
async fn test_control_plane_schema_crud_and_auth() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("todo-schema");
    let schema = create_test_schema();

    // 1. Missing Authorization header -> 401 Unauthorized
    let resp = server
        .client
        .post(format!("{}/admin/schemas", server.base_url))
        .json(&CreateSchemaRequest {
            schema_id: schema_id.clone(),
            schema: schema.clone(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 2. Invalid Bearer token -> 401 Unauthorized
    let resp = server
        .client
        .post(format!("{}/admin/schemas", server.base_url))
        .header(AUTHORIZATION, "Bearer invalid_secret_token")
        .json(&CreateSchemaRequest {
            schema_id: schema_id.clone(),
            schema: schema.clone(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 3. Valid Bearer token -> 201 Created
    let auth_header = format!("Bearer {}", server.config.admin_secret);
    let resp = server
        .client
        .post(format!("{}/admin/schemas", server.base_url))
        .header(AUTHORIZATION, &auth_header)
        .json(&CreateSchemaRequest {
            schema_id: schema_id.clone(),
            schema: schema.clone(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 4. Retrieve schema via GET /admin/schemas/:id
    let resp = server
        .client
        .get(format!("{}/admin/schemas/{}", server.base_url, schema_id))
        .header(AUTHORIZATION, &auth_header)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let retrieved_schema: Schema = resp.json().await.unwrap();
    assert_eq!(retrieved_schema.tables_by_id.len(), 1);

    // 5. Schema evolution: add nullable column via POST /admin/schemas/:id/columns
    let resp = server
        .client
        .post(format!(
            "{}/admin/schemas/{}/columns",
            server.base_url, schema_id
        ))
        .header(AUTHORIZATION, &auth_header)
        .json(&AddColumnRequest {
            table_name: "tasks".to_string(),
            column: ColumnDef::new("priority", DataType::Int).nullable(true),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let evolved_schema: Schema = resp.json().await.unwrap();
    assert!(evolved_schema
        .get_table_by_name("tasks")
        .unwrap()
        .get_column("priority")
        .is_some());
}

#[tokio::test]
async fn test_control_plane_room_lifecycle() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("room-schema");
    let schema = create_test_schema();
    let auth_header = format!("Bearer {}", server.config.admin_secret);

    // Pre-register schema
    server
        .schema_registry
        .register_schema(schema_id.clone(), schema)
        .unwrap();

    let room_id = RoomId::new("room-lifecycle-1");

    // 1. Provision room via POST /admin/rooms -> 201 Created
    let resp = server
        .client
        .post(format!("{}/admin/rooms", server.base_url))
        .header(AUTHORIZATION, &auth_header)
        .json(&CreateRoomRequest {
            room_id: room_id.clone(),
            schema_id: schema_id.clone(),
            lifecycle: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let meta: RoomMetadata = resp.json().await.unwrap();
    assert_eq!(meta.room_id, room_id);

    // 2. Duplicate room creation -> 409 Conflict
    let resp = server
        .client
        .post(format!("{}/admin/rooms", server.base_url))
        .header(AUTHORIZATION, &auth_header)
        .json(&CreateRoomRequest {
            room_id: room_id.clone(),
            schema_id: schema_id.clone(),
            lifecycle: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // 3. Inspect room metrics via GET /admin/rooms/:id
    let resp = server
        .client
        .get(format!("{}/admin/rooms/{}", server.base_url, room_id))
        .header(AUTHORIZATION, &auth_header)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let metrics: RoomMetrics = resp.json().await.unwrap();
    assert_eq!(metrics.head_seq, SequenceNumber::new(0));

    // 4. Delete room via DELETE /admin/rooms/:id -> 204 No Content
    let resp = server
        .client
        .delete(format!("{}/admin/rooms/{}", server.base_url, room_id))
        .header(AUTHORIZATION, &auth_header)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // 5. Subsequent delete returns 404 Not Found
    let resp = server
        .client
        .delete(format!("{}/admin/rooms/{}", server.base_url, room_id))
        .header(AUTHORIZATION, &auth_header)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_data_plane_handshake_and_1rtt_commit() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("todo-schema");
    let schema = create_test_schema();
    server
        .schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let room_id = RoomId::new("room-dataplane-1");
    server
        .room_manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .unwrap();

    let client_id = ClientId::new("alice");

    // 1. Invalid auth_token registration attempt -> 401 Unauthorized
    let reg_msg_bad = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        auth_token: "invalid.token.signature.123".to_string(),
    };
    let body_bad = encode_message(&reg_msg_bad).unwrap();
    let resp = server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body_bad)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 2. Valid token registration handshake -> 200 OK with ServerMessage::Registered
    let valid_token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );
    let reg_msg = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(2),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        auth_token: valid_token,
    };
    let body_reg = encode_message(&reg_msg).unwrap();
    let resp = server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body_reg)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.bytes().await.unwrap();
    let server_msg: ServerMessage = decode_message(&bytes).unwrap();
    match server_msg {
        ServerMessage::Registered {
            head_seq,
            schema_id: sid,
            ..
        } => {
            assert_eq!(head_seq, SequenceNumber::new(0));
            assert_eq!(sid, schema_id);
        }
        other => panic!("Expected Registered message, got {:?}", other),
    }

    // 3. Commit mutation 1 -> 200 OK with ServerMessage::CommitAck
    let op1 = create_insert_op(&schema, 1, "Buy milk");
    let commit_msg1 = ClientMessage::Commit {
        correlation_id: CorrelationId::new(3),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        mutation_id: MutationId::new([1; 16]),
        last_ack_seq: SequenceNumber::new(0),
        op: op1,
    };
    let body_commit1 = encode_message(&commit_msg1).unwrap();
    let resp1 = server
        .client
        .post(format!("{}/rooms/{}/commit", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body_commit1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let bytes1 = resp1.bytes().await.unwrap();
    let ack1: ServerMessage = decode_message(&bytes1).unwrap();
    match ack1 {
        ServerMessage::CommitAck { assigned_seq, .. } => {
            assert_eq!(assigned_seq, SequenceNumber::new(1));
        }
        other => panic!("Expected CommitAck, got {:?}", other),
    }

    // 4. Commit mutation 2 -> assigned_seq 2
    let op2 = create_insert_op(&schema, 2, "Read book");
    let commit_msg2 = ClientMessage::Commit {
        correlation_id: CorrelationId::new(4),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        mutation_id: MutationId::new([2; 16]),
        last_ack_seq: SequenceNumber::new(1),
        op: op2,
    };
    let body_commit2 = encode_message(&commit_msg2).unwrap();
    let resp2 = server
        .client
        .post(format!("{}/rooms/{}/commit", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body_commit2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let bytes2 = resp2.bytes().await.unwrap();
    let ack2: ServerMessage = decode_message(&bytes2).unwrap();
    match ack2 {
        ServerMessage::CommitAck { assigned_seq, .. } => {
            assert_eq!(assigned_seq, SequenceNumber::new(2));
        }
        other => panic!("Expected CommitAck, got {:?}", other),
    }
}

#[tokio::test]
async fn test_data_plane_sync_and_explicit_ack_pruning() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("todo-schema");
    let schema = create_test_schema();
    server
        .schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let room_id = RoomId::new("room-sync-ack-1");
    server
        .room_manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .unwrap();

    let writer = ClientId::new("writer");
    let reader = ClientId::new("reader");

    // Pre-register reader so its cursor participates in retention tracking
    let token_reader = generate_client_token(
        &reader,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );
    let reg_reader = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: reader.clone(),
        auth_token: token_reader,
    };
    server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&reg_reader).unwrap())
        .send()
        .await
        .unwrap();

    // Writer registers and commits 3 ops
    let token_writer = generate_client_token(
        &writer,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );
    server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::RegisterClient {
                correlation_id: CorrelationId::new(2),
                room_id: room_id.clone(),
                client_id: writer.clone(),
                auth_token: token_writer,
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    for i in 1..=3 {
        let op = create_insert_op(&schema, i, &format!("task-{}", i));
        let mut mut_id = [0u8; 16];
        mut_id[0] = i as u8;
        let commit_msg = ClientMessage::Commit {
            correlation_id: CorrelationId::new(10 + i as u64),
            room_id: room_id.clone(),
            client_id: writer.clone(),
            mutation_id: MutationId::new(mut_id),
            last_ack_seq: SequenceNumber::new(i as u64 - 1),
            op,
        };
        server
            .client
            .post(format!("{}/rooms/{}/commit", server.base_url, room_id))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(encode_message(&commit_msg).unwrap())
            .send()
            .await
            .unwrap();
    }

    // Writer acknowledges its own ops up to 3
    server
        .client
        .post(format!("{}/rooms/{}/ack", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::Ack {
                correlation_id: CorrelationId::new(30),
                room_id: room_id.clone(),
                client_id: writer.clone(),
                ack_seq: SequenceNumber::new(3),
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Reader syncs from 0
    let sync_msg = ClientMessage::Sync {
        correlation_id: CorrelationId::new(40),
        room_id: room_id.clone(),
        client_id: reader.clone(),
        from_seq: SequenceNumber::new(0),
        max_batch_size: 10,
    };
    let resp = server
        .client
        .post(format!("{}/rooms/{}/sync", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&sync_msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.bytes().await.unwrap();
    let sync_batch: ServerMessage = decode_message(&bytes).unwrap();
    match sync_batch {
        ServerMessage::SyncBatch { ops, head_seq, .. } => {
            assert_eq!(ops.len(), 3);
            assert_eq!(head_seq, SequenceNumber::new(3));
        }
        other => panic!("Expected SyncBatch, got {:?}", other),
    }

    // Explicit check: Reader's cursor MUST NOT have moved yet (still 0)
    let room_sender = server.room_manager.get_room(&room_id).unwrap();
    let (cur_tx, cur_rx) = tokio::sync::oneshot::channel();
    room_sender
        .send(rimdb_server::RoomCommand::GetClientCursor {
            client_id: reader.clone(),
            reply: cur_tx,
        })
        .await
        .unwrap();
    assert_eq!(cur_rx.await.unwrap(), Some(SequenceNumber::new(0)));

    // Reader sends explicit Ack for sequence 3
    let ack_msg = ClientMessage::Ack {
        correlation_id: CorrelationId::new(50),
        room_id: room_id.clone(),
        client_id: reader.clone(),
        ack_seq: SequenceNumber::new(3),
    };
    let ack_resp = server
        .client
        .post(format!("{}/rooms/{}/ack", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(encode_message(&ack_msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(ack_resp.status(), StatusCode::OK);
    let ack_bytes = ack_resp.bytes().await.unwrap();
    let ack_confirmed: ServerMessage = decode_message(&ack_bytes).unwrap();
    match ack_confirmed {
        ServerMessage::AckConfirmed { ack_seq, head_seq, .. } => {
            assert_eq!(ack_seq, SequenceNumber::new(3));
            assert_eq!(head_seq, SequenceNumber::new(3));
        }
        other => panic!("Expected AckConfirmed, got {:?}", other),
    }

    // Now reader's recorded cursor in server is 3!
    let (cur_tx2, cur_rx2) = tokio::sync::oneshot::channel();
    room_sender
        .send(rimdb_server::RoomCommand::GetClientCursor {
            client_id: reader.clone(),
            reply: cur_tx2,
        })
        .await
        .unwrap();
    assert_eq!(cur_rx2.await.unwrap(), Some(SequenceNumber::new(3)));
}

#[tokio::test]
async fn test_data_plane_heartbeat_and_deregister() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("todo-schema");
    let schema = create_test_schema();
    server
        .schema_registry
        .register_schema(schema_id.clone(), schema)
        .unwrap();

    let room_id = RoomId::new("room-hb-dereg");
    server
        .room_manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .unwrap();

    let client_id = ClientId::new("hb-client");
    let token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );

    // Register
    server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::RegisterClient {
                correlation_id: CorrelationId::new(1),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                auth_token: token,
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Heartbeat
    let hb_resp = server
        .client
        .post(format!("{}/rooms/{}/heartbeat", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::Heartbeat {
                correlation_id: CorrelationId::new(2),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(hb_resp.status(), StatusCode::OK);
    let hb_bytes = hb_resp.bytes().await.unwrap();
    let hb_ack: ServerMessage = decode_message(&hb_bytes).unwrap();
    match hb_ack {
        ServerMessage::HeartbeatAck {
            current_head_seq, ..
        } => {
            assert_eq!(current_head_seq, SequenceNumber::new(0));
        }
        other => panic!("Expected HeartbeatAck, got {:?}", other),
    }

    // Deregister
    let dereg_resp = server
        .client
        .post(format!("{}/rooms/{}/deregister", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::DeregisterClient {
                correlation_id: CorrelationId::new(3),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(dereg_resp.status(), StatusCode::NO_CONTENT);

    // Query cursor shows client removed
    let room_sender = server.room_manager.get_room(&room_id).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    room_sender
        .send(rimdb_server::RoomCommand::GetClientCursor {
            client_id,
            reply: tx,
        })
        .await
        .unwrap();
    assert_eq!(rx.await.unwrap(), None);
}

#[tokio::test]
async fn test_sse_realtime_head_advanced_events() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("todo-schema");
    let schema = create_test_schema();
    server
        .schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();

    let room_id = RoomId::new("room-sse-1");
    server
        .room_manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .unwrap();

    let client_id = ClientId::new("writer-sse");
    let token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );

    // 1. Establish SSE subscription stream
    let sse_resp = server
        .client
        .get(format!("{}/rooms/{}/events", server.base_url, room_id))
        .send()
        .await
        .unwrap();
    assert_eq!(sse_resp.status(), StatusCode::OK);
    let mut stream = sse_resp.bytes_stream();

    // 2. Commit a mutation via Data Plane
    server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::RegisterClient {
                correlation_id: CorrelationId::new(1),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                auth_token: token,
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let op = create_insert_op(&schema, 1, "SSE Trigger");
    server
        .client
        .post(format!("{}/rooms/{}/commit", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::Commit {
                correlation_id: CorrelationId::new(2),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                mutation_id: MutationId::new([42; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op,
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // 3. Receive signal event from SSE stream within 2 seconds
    let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("SSE event timeout")
        .expect("Stream closed unexpectedly")
        .unwrap();

    let text = String::from_utf8_lossy(&chunk);
    assert!(
        text.contains("event: head_advanced"),
        "Expected SSE head_advanced event, got: {}",
        text
    );
    assert!(text.contains("data: 1"), "Expected sequence 1, got: {}", text);
}

#[tokio::test]
async fn test_snapshot_relay_chunked_transfer_and_blake3() {
    let server = TestServer::start().await;
    let room_id = RoomId::new("room-relay-1");

    // 1. Prepare 512 KB synthetic snapshot payload
    let snapshot_bytes = vec![0xABu8; 512 * 1024];
    let expected_hash = ServerMessage::compute_snapshot_hash(&snapshot_bytes);

    // 2. Upload snapshot staging via POST /rooms/:id/snapshot/upload
    let upload_resp = server
        .client
        .post(format!(
            "{}/rooms/{}/snapshot/upload",
            server.base_url, room_id
        ))
        .header("x-snapshot-head-seq", "100")
        .body(snapshot_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::OK);

    // 3. Request chunks in 128 KB fragments (4 chunks total)
    let chunk_size = 128 * 1024;
    let mut assembled_data = Vec::new();

    for chunk_idx in 0..4 {
        let req_msg = ClientMessage::RequestSnapshotChunk {
            correlation_id: CorrelationId::new(100 + chunk_idx as u64),
            room_id: room_id.clone(),
            chunk_index: chunk_idx,
            chunk_size,
        };

        let chunk_resp = server
            .client
            .post(format!(
                "{}/rooms/{}/snapshot/chunk",
                server.base_url, room_id
            ))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(encode_message(&req_msg).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(chunk_resp.status(), StatusCode::OK);

        let bytes = chunk_resp.bytes().await.unwrap();
        let chunk_msg: ServerMessage = decode_message(&bytes).unwrap();
        match chunk_msg {
            ServerMessage::SnapshotChunk {
                chunk_index,
                total_chunks,
                total_bytes,
                snapshot_hash,
                data,
                snapshot_head_seq,
                ..
            } => {
                assert_eq!(chunk_index, chunk_idx);
                assert_eq!(total_chunks, 4);
                assert_eq!(total_bytes, 512 * 1024);
                assert_eq!(snapshot_hash, expected_hash);
                assert_eq!(snapshot_head_seq, SequenceNumber::new(100));
                assembled_data.extend_from_slice(&data);
            }
            other => panic!("Expected SnapshotChunk, got {:?}", other),
        }
    }

    // 4. Validate cryptographic integrity of assembled stream
    assert_eq!(assembled_data.len(), 512 * 1024);
    assert_eq!(assembled_data, snapshot_bytes);
    let computed_hash = ServerMessage::compute_snapshot_hash(&assembled_data);
    assert_eq!(computed_hash, expected_hash);
}

#[tokio::test]
async fn test_schema_evolution_cascades_to_active_room() {
    let server = TestServer::start().await;
    let schema_id = SchemaId::new("evolving-schema");
    let schema = create_test_schema();
    server
        .schema_registry
        .register_schema(schema_id.clone(), schema)
        .unwrap();

    let room_id = RoomId::new("room-evolution-cascade");
    server
        .room_manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .unwrap();

    let client_id = ClientId::new("writer-evo");
    let token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(300),
        &server.config.auth_secret,
    );

    // Register writer
    server
        .client
        .post(format!("{}/rooms/{}/register", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::RegisterClient {
                correlation_id: CorrelationId::new(1),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                auth_token: token,
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Admin adds "notes" column via Control Plane
    let auth_header = format!("Bearer {}", server.config.admin_secret);
    let add_resp = server
        .client
        .post(format!(
            "{}/admin/schemas/{}/columns",
            server.base_url, schema_id
        ))
        .header(AUTHORIZATION, &auth_header)
        .json(&AddColumnRequest {
            table_name: "tasks".to_string(),
            column: ColumnDef::new("notes", DataType::String).nullable(true),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(add_resp.status(), StatusCode::OK);
    let updated_schema: Schema = add_resp.json().await.unwrap();

    // Client commits an operation containing the new "notes" column
    let row_with_notes = RowBuilder::new()
        .set("id", 100i64)
        .set("title", "Evolution task")
        .set("completed", false)
        .set("notes", "Hot reloaded without restart!")
        .build();
    let op_with_notes = updated_schema
        .to_operation_insert("tasks", &row_with_notes, 2000)
        .unwrap();

    let commit_resp = server
        .client
        .post(format!("{}/rooms/{}/commit", server.base_url, room_id))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(
            encode_message(&ClientMessage::Commit {
                correlation_id: CorrelationId::new(2),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                mutation_id: MutationId::new([99; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op: op_with_notes,
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(commit_resp.status(), StatusCode::OK);
    let bytes = commit_resp.bytes().await.unwrap();
    let ack: ServerMessage = decode_message(&bytes).unwrap();
    match ack {
        ServerMessage::CommitAck { assigned_seq, .. } => {
            assert_eq!(assigned_seq, SequenceNumber::new(1));
        }
        other => panic!("Expected CommitAck, got {:?}", other),
    }
}
