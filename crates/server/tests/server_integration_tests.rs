mod common;

use axum::http::StatusCode;
use common::{
    commit, cursor, envelope, make_test_op, read_sse_until, register, register_msg, seqs,
    tasks_schema, token, TestServer,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::Method;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::*;
use zemdb_server::actor::command::RoomMetrics;
use zemdb_server::actor::manager::RoomMetadata;
use zemdb_server::api::auth::generate_client_token;
use zemdb_server::api::control_plane::{AddColumnRequest, CreateRoomRequest, CreateSchemaRequest};

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

fn commit_msg(
    room_id: &RoomId,
    client_id: &ClientId,
    mutation: u8,
    last_ack_seq: u64,
    op: Operation,
) -> ClientMessage {
    ClientMessage::Commit {
        correlation_id: CorrelationId::new(u64::from(mutation)),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        mutation_id: MutationId::new([mutation; 16]),
        last_ack_seq: SequenceNumber::new(last_ack_seq),
        op,
    }
}

fn ack_msg(room_id: &RoomId, client_id: &ClientId, ack_seq: u64) -> ClientMessage {
    ClientMessage::Ack {
        correlation_id: CorrelationId::new(30),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        ack_seq: SequenceNumber::new(ack_seq),
    }
}

impl TestServer {
    async fn heartbeat_ack(
        &self,
        room_id: &RoomId,
        client_id: &ClientId,
        token: &str,
    ) -> (bool, Option<SequenceNumber>) {
        let msg = ClientMessage::Heartbeat {
            correlation_id: CorrelationId::new(90),
            room_id: room_id.clone(),
            client_id: client_id.clone(),
        };
        match self.post_message(room_id, "heartbeat", token, &msg).await {
            (
                StatusCode::OK,
                ServerMessage::HeartbeatAck {
                    snapshot_wanted,
                    active_snapshot_seq,
                    ..
                },
            ) => (snapshot_wanted, active_snapshot_seq),
            other => panic!("Expected HeartbeatAck, got {:?}", other),
        }
    }
}

#[tokio::test]
async fn control_plane_schema_crud_and_auth() {
    let server = TestServer::start(|_| {}).await;
    let schema_id = SchemaId::new("todo-schema").unwrap();
    let create = CreateSchemaRequest {
        schema_id: schema_id.clone(),
        schema: tasks_schema(),
    };

    // No Authorization header, then an invalid bearer token.
    for auth in [None, Some("invalid_secret_token")] {
        let mut req = server
            .client
            .post(server.url("/admin/schemas"))
            .json(&create);
        if let Some(token) = auth {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{auth:?}");
    }

    let resp = server
        .admin(Method::POST, "/admin/schemas")
        .json(&create)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = server
        .admin(Method::GET, &format!("/admin/schemas/{schema_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let retrieved_schema: Schema = resp.json().await.unwrap();
    assert_eq!(retrieved_schema.table_count(), 1);

    let resp = server
        .admin(Method::POST, &format!("/admin/schemas/{schema_id}/columns"))
        .json(&AddColumnRequest {
            table_name: "tasks".to_string(),
            column: ColumnDef::new("priority", DataType::Int).nullable(true),
            unique: Vec::new(),
            foreign_keys: Vec::new(),
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
async fn control_plane_room_lifecycle() {
    let server = TestServer::start(|_| {}).await;
    let schema_id = server.register_schema("room-schema", tasks_schema());
    let room_id = RoomId::new("room-lifecycle-1").unwrap();
    let create = CreateRoomRequest {
        room_id: room_id.clone(),
        schema_id,
        lifecycle: None,
    };
    let room_path = format!("/admin/rooms/{room_id}");

    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .json(&create)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let meta: RoomMetadata = resp.json().await.unwrap();
    assert_eq!(meta.room_id, room_id);

    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .json(&create)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    let resp = server.admin(Method::GET, &room_path).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let metrics: RoomMetrics = resp.json().await.unwrap();
    assert_eq!(metrics.head_seq, SequenceNumber::new(0));

    // The first delete removes the room; the second one finds nothing.
    for expected in [StatusCode::NO_CONTENT, StatusCode::NOT_FOUND] {
        let resp = server
            .admin(Method::DELETE, &room_path)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), expected);
    }
}

#[tokio::test]
async fn control_plane_room_lifecycle_overrides() {
    // Server defaults that differ from those of the policy, so the test tells them apart.
    let server = TestServer::start(|config| {
        config.lease_timeout_secs = 60;
        config.snapshot_demand_ttl_secs = 60;
    })
    .await;
    server.register_schema("policy-schema", tasks_schema());
    let create = |body: String| {
        server
            .admin(Method::POST, "/admin/rooms")
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
    };

    // Invalid overrides are JSON BadRequest and create nothing: out of range, unknown fields
    // (for example a duration without the `_secs` suffix) and values of the wrong type.
    for lifecycle in [
        r#"{"ram_max_ops": 0}"#,
        r#"{"snapshot_demand_ttl_secs": 59}"#,
        r#"{"ram_ttl": 5}"#,
        r#"{"lease_timeout_secs": "90"}"#,
        r#"{"cold_disk_ttl_secs": -1}"#,
    ] {
        let resp = create(format!(
            r#"{{"room_id": "room-policy", "schema_id": "policy-schema", "lifecycle": {lifecycle}}}"#
        ))
        .await
        .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{lifecycle}");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["code"], "BadRequest", "{lifecycle}: {body}");
    }
    assert!(!server
        .room_manager
        .room_exists(&RoomId::new("room-policy").unwrap())
        .unwrap());

    let resp = create(
        r#"{"room_id": "room-policy", "schema_id": "policy-schema",
            "lifecycle": {"ram_max_ops": 7, "idle_timeout_secs": 0}}"#
            .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let meta: serde_json::Value = resp.json().await.unwrap();
    let overrides = serde_json::json!({"ram_max_ops": 7, "idle_timeout_secs": 0});
    assert_eq!(meta["lifecycle"], overrides);

    // The room details show the stored overrides and the effective policy: the overrides on
    // top of the server defaults, durations in seconds.
    let resp = server
        .admin(Method::GET, "/admin/rooms/room-policy")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let details: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(details["lifecycle_overrides"], overrides);
    let effective = &details["lifecycle"];
    assert_eq!(effective["ram_max_ops"], 7);
    assert_eq!(effective["idle_timeout_secs"], 0);
    assert_eq!(effective["lease_timeout_secs"], 60);
    assert_eq!(effective["snapshot_demand_ttl_secs"], 60);
    assert_eq!(effective["dormant_after_secs"], serde_json::Value::Null);
    assert_eq!(details["head_seq"], 0);
}

#[tokio::test]
async fn data_plane_handshake_and_1rtt_commit() {
    let server = TestServer::start(|_| {}).await;
    let (schema_id, room_id) = server
        .room_with_schema("todo-schema", "room-dataplane-1")
        .await;
    let client_id = client("alice");

    let bad_register = register_msg(&room_id, &client_id, "invalid.token.signature.123");
    let resp = server
        .post_frame(&format!("{room_id}/register"), None, &bad_register)
        .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let (token, registered) = server.register(&room_id, &client_id).await;
    assert!(
        matches!(
            &registered,
            ServerMessage::Registered { head_seq, schema_id: sid, .. }
                if *head_seq == SequenceNumber::new(0) && *sid == schema_id
        ),
        "{registered:?}"
    );

    for (mutation, last_ack_seq) in [(1u8, 0), (2, 1)] {
        let msg = commit_msg(
            &room_id,
            &client_id,
            mutation,
            last_ack_seq,
            make_test_op(mutation.into()),
        );
        let (status, ack) = server.post_message(&room_id, "commit", &token, &msg).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            matches!(ack, ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(mutation.into())),
            "{ack:?}"
        );
    }
}

#[tokio::test]
async fn data_plane_sync_and_explicit_ack() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-sync-ack-1")
        .await;
    let writer = client("writer");
    let reader = client("reader");

    // The reader registers first so its cursor participates in retention tracking.
    let (token_reader, _) = server.register(&room_id, &reader).await;
    let (token_writer, _) = server.register(&room_id, &writer).await;

    for i in 1..=3u8 {
        let msg = commit_msg(
            &room_id,
            &writer,
            i,
            u64::from(i) - 1,
            make_test_op(i.into()),
        );
        let (status, _) = server
            .post_message(&room_id, "commit", &token_writer, &msg)
            .await;
        assert_eq!(status, StatusCode::OK);
    }

    // The writer acknowledges its own ops up to 3.
    let (status, _) = server
        .post_message(
            &room_id,
            "ack",
            &token_writer,
            &ack_msg(&room_id, &writer, 3),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let sync_msg = ClientMessage::Sync {
        correlation_id: CorrelationId::new(40),
        room_id: room_id.clone(),
        client_id: reader.clone(),
        from_seq: SequenceNumber::new(0),
        max_batch_size: 10,
    };
    let (status, sync_batch) = server
        .post_message(&room_id, "sync", &token_reader, &sync_msg)
        .await;
    assert_eq!(status, StatusCode::OK);
    match sync_batch {
        ServerMessage::SyncBatch { ops, head_seq, .. } => {
            assert_eq!(ops.len(), 3);
            assert_eq!(head_seq, SequenceNumber::new(3));
        }
        other => panic!("Expected SyncBatch, got {:?}", other),
    }

    // Delivering the batch does not move the reader's cursor; its Ack does.
    let room_sender = server.room_manager.get_room(&room_id).unwrap();
    assert_eq!(
        cursor(&room_sender, &reader).await,
        Some(SequenceNumber::new(0))
    );

    let (status, ack_confirmed) = server
        .post_message(
            &room_id,
            "ack",
            &token_reader,
            &ack_msg(&room_id, &reader, 3),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    match ack_confirmed {
        ServerMessage::AckConfirmed {
            ack_seq, head_seq, ..
        } => {
            assert_eq!(ack_seq, SequenceNumber::new(3));
            assert_eq!(head_seq, SequenceNumber::new(3));
        }
        other => panic!("Expected AckConfirmed, got {:?}", other),
    }

    assert_eq!(
        cursor(&room_sender, &reader).await,
        Some(SequenceNumber::new(3))
    );
}

#[tokio::test]
async fn data_plane_heartbeat_and_deregister() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-hb-dereg")
        .await;
    let client_id = client("hb-client");
    let (token, _) = server.register(&room_id, &client_id).await;

    let heartbeat = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(2),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
    };
    let (status, hb_ack) = server
        .post_message(&room_id, "heartbeat", &token, &heartbeat)
        .await;
    assert_eq!(status, StatusCode::OK);
    match hb_ack {
        ServerMessage::HeartbeatAck {
            current_head_seq,
            snapshot_wanted,
            active_snapshot_seq,
            ..
        } => {
            assert_eq!(current_head_seq, SequenceNumber::new(0));
            assert!(!snapshot_wanted);
            assert_eq!(active_snapshot_seq, None);
        }
        other => panic!("Expected HeartbeatAck, got {:?}", other),
    }

    let deregister = ClientMessage::DeregisterClient {
        correlation_id: CorrelationId::new(3),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
    };
    let (status, ack_msg) = server
        .post_message(&room_id, "deregister", &token, &deregister)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ack_msg,
        ServerMessage::DeregisterAck {
            correlation_id: CorrelationId::new(3),
            room_id: room_id.clone(),
            client_id: client_id.clone(),
        }
    );

    // The deregistered client has no cursor any more.
    let room_sender = server.room_manager.get_room(&room_id).unwrap();
    assert_eq!(cursor(&room_sender, &client_id).await, None);
}

#[tokio::test]
async fn sse_signals_head_advanced() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server.room_with_schema("todo-schema", "room-sse-1").await;
    let client_id = client("writer-sse");

    let sse_resp = server.events(&room_id, &token(&client_id, &room_id)).await;
    assert_eq!(sse_resp.status(), StatusCode::OK);
    let mut stream = sse_resp.bytes_stream();

    let (token, _) = server.register(&room_id, &client_id).await;
    let msg = commit_msg(&room_id, &client_id, 42, 0, make_test_op(1));
    let (status, _) = server.post_message(&room_id, "commit", &token, &msg).await;
    assert_eq!(status, StatusCode::OK);

    let text = read_sse_until(&mut stream, "data: 1").await;
    assert!(
        text.contains("event: head_advanced"),
        "Expected SSE head_advanced event, got: {}",
        text
    );
}

#[tokio::test]
async fn snapshot_relay_chunked_transfer_and_blake3() {
    let server = TestServer::start(|_| {}).await;

    // The relay only accepts snapshots inside the room's log range: create the room and
    // commit one operation so that the snapshot can sit at seq 1.
    let (_, room_id) = server
        .room_with_schema("relay-schema", "room-relay-1")
        .await;
    let sender = server.room_manager.get_room(&room_id).unwrap();
    let writer = client("writer");
    register(&sender, &writer).await;
    commit(
        &sender,
        &writer,
        MutationId::new([1; 16]),
        0,
        make_test_op(1),
    )
    .await
    .unwrap();

    let snapshot_bytes = envelope(512 * 1024, 0xAB);
    let expected_hash = ServerMessage::compute_snapshot_hash(&snapshot_bytes);

    let upload_resp = server
        .admin(Method::POST, &format!("/rooms/{room_id}/snapshot/upload"))
        .header("x-snapshot-head-seq", "1")
        .body(snapshot_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::OK);

    // Download in chunks of 128 KB (4 chunks in all).
    let chunk_size = 128 * 1024;
    let mut assembled_data = Vec::new();
    for chunk_idx in 0..4 {
        let req_msg = ClientMessage::RequestSnapshotChunk {
            correlation_id: CorrelationId::new(100 + chunk_idx as u64),
            room_id: room_id.clone(),
            chunk_index: chunk_idx,
            chunk_size,
            snapshot_hash: (chunk_idx > 0).then_some(expected_hash),
        };
        let (status, chunk_msg) = server
            .post_message(
                &room_id,
                "snapshot/chunk",
                &server.config.admin_secret,
                &req_msg,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
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
                assert_eq!(snapshot_head_seq, SequenceNumber::new(1));
                assembled_data.extend_from_slice(&data);
            }
            other => panic!("Expected SnapshotChunk, got {:?}", other),
        }
    }

    assert_eq!(assembled_data.len(), 512 * 1024);
    assert_eq!(assembled_data, snapshot_bytes);
    let computed_hash = ServerMessage::compute_snapshot_hash(&assembled_data);
    assert_eq!(computed_hash, expected_hash);
}

#[tokio::test]
async fn schema_evolution_cascades_to_active_room() {
    let server = TestServer::start(|_| {}).await;
    let (schema_id, room_id) = server
        .room_with_schema("evolving-schema", "room-evolution-cascade")
        .await;
    let client_id = client("writer-evo");
    let (token, registered) = server.register(&room_id, &client_id).await;
    assert!(
        matches!(&registered, ServerMessage::Registered { schema_id: id, schema, .. } if *id == schema_id && schema.version() == 1),
        "{registered:?}"
    );

    let add_resp = server
        .admin(Method::POST, &format!("/admin/schemas/{schema_id}/columns"))
        .json(&AddColumnRequest {
            table_name: "tasks".to_string(),
            column: ColumnDef::new("notes", DataType::String).nullable(true),
            unique: Vec::new(),
            foreign_keys: Vec::new(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(add_resp.status(), StatusCode::OK);
    let updated_schema: Schema = add_resp.json().await.unwrap();
    assert_eq!(updated_schema.version(), 2);

    // Responses carry the new version of the same schema id, which tells a client without SSE
    // to fetch the schema.
    let heartbeat = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(97),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
    };
    let sync = ClientMessage::Sync {
        correlation_id: CorrelationId::new(98),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        from_seq: SequenceNumber::new(0),
        max_batch_size: 10,
    };
    let get_schema = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(96),
        room_id: room_id.clone(),
    };
    let (_, reply) = server
        .post_message(&room_id, "schema", &token, &get_schema)
        .await;
    assert!(
        matches!(&reply, ServerMessage::Schema { schema_id: id, schema, .. } if *id == schema_id && schema.version() == 2),
        "{reply:?}"
    );
    for (endpoint, msg) in [("heartbeat", heartbeat), ("sync", sync)] {
        let (_, reply) = server.post_message(&room_id, endpoint, &token, &msg).await;
        assert!(
            matches!(
                &reply,
                ServerMessage::HeartbeatAck {
                    schema_version: 2,
                    schema_id: id,
                    ..
                } | ServerMessage::SyncBatch {
                    schema_version: 2,
                    schema_id: id,
                    ..
                } if *id == schema_id
            ),
            "{reply:?}"
        );
    }

    // The client commits an operation containing the new "notes" column.
    let row_with_notes = RowBuilder::new()
        .set("id", 100i64)
        .set("title", "Evolution task")
        .set("completed", false)
        .set("notes", "Hot reloaded without restart!")
        .build();
    let op_with_notes = updated_schema
        .to_operation_insert("tasks", &row_with_notes, 2000)
        .unwrap();
    let msg = commit_msg(&room_id, &client_id, 99, 0, op_with_notes);
    let (status, ack) = server.post_message(&room_id, "commit", &token, &msg).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        matches!(
            &ack,
            ServerMessage::CommitAck { assigned_seq, schema_version: 2, schema_id: id, .. }
                if *assigned_seq == SequenceNumber::new(1) && *id == schema_id
        ),
        "{ack:?}"
    );
}

#[tokio::test]
async fn data_plane_auth_enforcement_rejected_without_bearer() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-auth-test")
        .await;
    let client_id = client("anonymous");

    let requests = [
        (
            "commit",
            commit_msg(&room_id, &client_id, 1, 0, make_test_op(1)),
        ),
        (
            "sync",
            ClientMessage::Sync {
                correlation_id: CorrelationId::new(2),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                from_seq: SequenceNumber::new(0),
                max_batch_size: 10,
            },
        ),
        ("ack", ack_msg(&room_id, &client_id, 1)),
        (
            "heartbeat",
            ClientMessage::Heartbeat {
                correlation_id: CorrelationId::new(4),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
            },
        ),
        (
            "deregister",
            ClientMessage::DeregisterClient {
                correlation_id: CorrelationId::new(5),
                room_id: room_id.clone(),
                client_id: client_id.clone(),
            },
        ),
    ];
    for (endpoint, msg) in &requests {
        let resp = server
            .post_frame(&format!("{room_id}/{endpoint}"), None, msg)
            .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{endpoint}");
    }

    // Neither an Authorization header nor a token in the query.
    let sse_resp = server
        .client
        .get(server.url(&format!("/rooms/{room_id}/events")))
        .send()
        .await
        .unwrap();
    assert_eq!(sse_resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn data_plane_auth_token_tampered_or_expired() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-tamper-test")
        .await;
    let client_id = client("alice");

    let expired_token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(0),
        &server.config.auth_secret,
    );
    let valid_token = token(&client_id, &room_id);
    let tampered_token = format!("{}deadbeef", &valid_token[..valid_token.len() - 8]);
    let msg = commit_msg(&room_id, &client_id, 1, 0, make_test_op(1));
    for bad_token in [expired_token, tampered_token] {
        let resp = server
            .post_frame(&format!("{room_id}/commit"), Some(&bad_token), &msg)
            .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{bad_token}");
    }
}

#[tokio::test]
async fn data_plane_room_path_token_and_payload_mismatch_rejected() {
    let server = TestServer::start(|_| {}).await;
    let schema_id = server.register_schema("todo-schema", tasks_schema());
    let room_a = server.create_room("room-a", &schema_id).await;
    let room_b = server.create_room("room-b", &schema_id).await;

    let alice = client("alice");
    let bob = client("bob");
    let token_a = token(&alice, &room_a);

    for (path_room, payload_room, payload_client, expected) in [
        // A token for room_a on the endpoint of room_b.
        (&room_b, &room_b, &alice, StatusCode::FORBIDDEN),
        // The path and the token are for room_a, but the payload says room_b.
        (&room_a, &room_b, &alice, StatusCode::BAD_REQUEST),
        // The token is alice's, but the payload says bob: impersonation.
        (&room_a, &room_a, &bob, StatusCode::FORBIDDEN),
    ] {
        let msg = commit_msg(payload_room, payload_client, 1, 0, make_test_op(1));
        let resp = server
            .post_frame(&format!("{path_room}/commit"), Some(&token_a), &msg)
            .await;
        assert_eq!(
            resp.status(),
            expected,
            "path {path_room}, payload {payload_room}/{payload_client}"
        );
    }
}

#[tokio::test]
async fn dev_token_is_not_accepted() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-dev-token")
        .await;
    let client_id = client("hacker");

    let register = register_msg(&room_id, &client_id, "dev-token");
    let resp = server
        .post_frame(&format!("{room_id}/register"), None, &register)
        .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let commit = commit_msg(&room_id, &client_id, 1, 0, make_test_op(1));
    let resp = server
        .post_frame(&format!("{room_id}/commit"), Some("dev-token"), &commit)
        .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn sse_events_auth_header_and_query_param() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-sse-auth")
        .await;
    let valid_token = token(&client("listener"), &room_id);

    let resp_header = server.events(&room_id, &valid_token).await;
    assert_eq!(resp_header.status(), StatusCode::OK);

    // A token in the query also works, for EventSource compatibility; an invalid one does not.
    for (query_token, expected) in [
        (valid_token.as_str(), StatusCode::OK),
        ("invalid.token.123", StatusCode::UNAUTHORIZED),
    ] {
        let resp = server
            .client
            .get(server.url(&format!("/rooms/{room_id}/events?token={query_token}")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), expected, "{query_token}");
    }
}

#[tokio::test]
async fn admin_get_room_non_existent_returns_404_without_spawning() {
    let server = TestServer::start(|_| {}).await;
    let ghost_id = "ghost-room-unspawned";

    let resp = server
        .admin(Method::GET, &format!("/admin/rooms/{ghost_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let room_dir = server.config.data_dir.join("rooms").join(ghost_id);
    assert!(
        !room_dir.exists(),
        "Room directory should not have been created for non-existent room query"
    );
    assert!(
        !server
            .room_manager
            .room_exists(&RoomId::new(ghost_id).unwrap())
            .unwrap(),
        "Room should not exist in room manager"
    );
}

#[tokio::test]
async fn sse_signals_schema_reloaded() {
    let server = TestServer::start(|_| {}).await;
    let schema_id = SchemaId::new("schema-reload-sse").unwrap();
    let room_id = RoomId::new("room-schema-sse").unwrap();

    let resp = server
        .admin(Method::POST, "/admin/schemas")
        .json(&CreateSchemaRequest {
            schema_id: schema_id.clone(),
            schema: tasks_schema(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .json(&CreateRoomRequest {
            room_id: room_id.clone(),
            schema_id: schema_id.clone(),
            lifecycle: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let sse_resp = server
        .events(&room_id, &token(&client("sse-schema-listener"), &room_id))
        .await;
    assert_eq!(sse_resp.status(), StatusCode::OK);
    let mut stream = sse_resp.bytes_stream();

    let add_col_resp = server
        .admin(Method::POST, &format!("/admin/schemas/{schema_id}/columns"))
        .json(&AddColumnRequest {
            table_name: "tasks".to_string(),
            column: ColumnDef::new("priority", DataType::Int).nullable(true),
            unique: Vec::new(),
            foreign_keys: Vec::new(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(add_col_resp.status(), StatusCode::OK);

    let text = read_sse_until(&mut stream, "data: schema-reload-sse").await;
    assert!(
        text.contains("event: schema_reloaded"),
        "Expected SSE schema_reloaded event, got: {}",
        text
    );
}

#[test]
fn client_lease_disconnected_to_dormant_timeout() {
    use std::time::Instant;
    use zemdb_server::actor::lease::{ClientLeaseTracker, ClientState};

    let dir = tempdir().unwrap();
    let roster_path = dir.path().join("clients.json");
    let mut tracker = ClientLeaseTracker::open_or_create(&roster_path).unwrap();

    let alice = client("alice");
    let bob = client("bob");
    let lease_timeout = Duration::from_secs(5);
    let tail_seq = SequenceNumber::new(0);

    // Register alice and bob as connected
    let state_alice = tracker.register_client(&alice, Some(SequenceNumber::new(10)), tail_seq);
    let state_bob = tracker.register_client(&bob, Some(SequenceNumber::new(10)), tail_seq);
    assert_eq!(state_alice, ClientState::Connected);
    assert_eq!(state_bob, ClientState::Connected);
    assert_eq!(
        tracker.min_connected_ack_seq(),
        Some(SequenceNumber::new(10))
    );

    // 1. Simulate alice lease expiry (> 5s) -> transitions to Disconnected
    tracker.get_client_mut(&alice).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(6);
    let modified = tracker.check_timeouts(lease_timeout, None, Duration::MAX, tail_seq);
    assert!(modified);
    assert_eq!(
        tracker.get_client(&alice).unwrap().state,
        ClientState::Disconnected
    );

    // Disconnected alice blocks proactive pruning
    assert_eq!(
        tracker.min_connected_ack_seq(),
        None,
        "Disconnected client must block proactive pruning"
    );

    // 2. A prolonged disconnection alone keeps alice Disconnected: her cursor is still
    // inside the retained log, so she can catch up from it.
    tracker.get_client_mut(&alice).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(95);
    assert!(!tracker.check_timeouts(lease_timeout, None, Duration::MAX, tail_seq));
    assert_eq!(
        tracker.get_client(&alice).unwrap().state,
        ClientState::Disconnected
    );

    // 3. With a dormancy timeout configured, the same disconnection makes her Dormant.
    let modified2 = tracker.check_timeouts(
        lease_timeout,
        Some(Duration::from_secs(90)),
        Duration::MAX,
        tail_seq,
    );
    assert!(modified2);
    assert_eq!(
        tracker.get_client(&alice).unwrap().state,
        ClientState::Dormant,
        "Alice should transition to Dormant after the configured dormancy timeout"
    );

    // Dormant alice no longer blocks proactive pruning; bob's cursor is returned
    assert_eq!(
        tracker.min_connected_ack_seq(),
        Some(SequenceNumber::new(10)),
        "Dormant client must not block log compaction"
    );
}

#[tokio::test]
async fn commit_ack_catchup_ops_content_ordering_and_contiguity() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema-catchup", "room-catchup-contiguity-1")
        .await;
    let client_alpha = client("writer-alpha");
    let client_beta = client("writer-beta");
    let client_gamma = client("writer-gamma");

    // The room is still empty, so the clients register without a cursor: a cursor can only be
    // reported once it exists. The cursor that matters below is the one each commit carries.
    let (token_alpha, _) = server.register(&room_id, &client_alpha).await;
    let (token_beta, _) = server.register(&room_id, &client_beta).await;
    let (token_gamma, _) = server.register(&room_id, &client_gamma).await;

    let commit = |token: &str, msg: ClientMessage| {
        let server = &server;
        let room_id = &room_id;
        let token = token.to_string();
        async move {
            match server.post_message(room_id, "commit", &token, &msg).await {
                (
                    StatusCode::OK,
                    ServerMessage::CommitAck {
                        assigned_seq,
                        catchup_ops,
                        ..
                    },
                ) => (assigned_seq, catchup_ops),
                other => panic!("Expected CommitAck, got {:?}", other),
            }
        }
    };
    let ops_of = |catchup_ops: &[SequencedOperation]| -> Vec<Operation> {
        catchup_ops.iter().map(|o| o.op.clone()).collect()
    };

    // Client alpha commits 5 operations sequentially (assigned_seq 1..=5)
    let mut sent_ops = Vec::new();
    for i in 1..=5u8 {
        let op = make_test_op(i.into());
        sent_ops.push(op.clone());
        let msg = commit_msg(&room_id, &client_alpha, i, u64::from(i) - 1, op);
        let (assigned_seq, catchup_ops) = commit(&token_alpha, msg).await;
        assert_eq!(assigned_seq, SequenceNumber::new(i.into()));
        assert_eq!(
            seqs(&catchup_ops),
            vec![u64::from(i)],
            "Alpha was at last_ack_seq i-1, catchup_ops must contain its own sequenced op"
        );
        assert_eq!(catchup_ops[0].op.pk(), &PrimaryKey::single(i64::from(i)));
    }

    // Client beta, whose local cursor is only at sequence 2, commits mutation 6: its catchup
    // holds alpha's ops 3, 4 and 5 and its own op 6, contiguous and in order.
    let op_beta = make_test_op(6);
    let commit_beta = commit_msg(&room_id, &client_beta, 66, 2, op_beta.clone());
    let (assigned_seq, catchup_ops) = commit(&token_beta, commit_beta.clone()).await;
    assert_eq!(assigned_seq, SequenceNumber::new(6));
    assert_eq!(
        seqs(&catchup_ops),
        vec![3, 4, 5, 6],
        "Catchup sequence must be strictly contiguous from the cursor lag of 2 up to 6"
    );
    let mut expected_ops = sent_ops[2..].to_vec();
    expected_ops.push(op_beta.clone());
    assert_eq!(ops_of(&catchup_ops), expected_ops);

    // Now client_gamma commits mutation 7 while fully up-to-date (last_ack_seq: 6)
    let op_gamma = make_test_op(7);
    let commit_gamma = commit_msg(&room_id, &client_gamma, 77, 6, op_gamma.clone());
    let (assigned_seq, catchup_ops) = commit(&token_gamma, commit_gamma).await;
    assert_eq!(assigned_seq, SequenceNumber::new(7));
    assert_eq!(
        seqs(&catchup_ops),
        vec![7],
        "Gamma was up-to-date at seq 6, catchup_ops contains op 7"
    );
    assert_eq!(ops_of(&catchup_ops), vec![op_gamma]);

    // Idempotent commit retry: client_beta resends the exact same commit_beta (same
    // mutation_id) with last_ack_seq = 2, and catches up from its cursor to the head.
    let (assigned_seq, catchup_ops) = commit(&token_beta, commit_beta).await;
    assert_eq!(assigned_seq, SequenceNumber::new(6));
    assert_eq!(
        seqs(&catchup_ops),
        vec![3, 4, 5, 6, 7],
        "Catchup on duplicate commit must return deltas from client cursor up to head"
    );
    assert_eq!(catchup_ops[3].op, op_beta);
}

#[tokio::test]
async fn snapshot_demand_signals_over_http_and_sse() {
    let server = TestServer::start(|_| {}).await;
    let schema_id = server.register_schema("todo-schema", tasks_schema());
    let room_id = RoomId::new("room-snapshot-signals").unwrap();
    let small_segments = zemdb_server::RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        ..Default::default()
    };
    server
        .room_manager
        .create_room(room_id.clone(), schema_id, Some(small_segments))
        .await
        .unwrap();

    // 1. The writer commits 1..=8 and acknowledges 8, which prunes the log.
    let writer = client("writer");
    let (writer_token, _) = server.register(&room_id, &writer).await;
    for i in 1..=8u8 {
        let msg = commit_msg(&room_id, &writer, i, 0, make_test_op(i.into()));
        let (status, _) = server
            .post_message(&room_id, "commit", &writer_token, &msg)
            .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _) = server
        .post_message(
            &room_id,
            "ack",
            &writer_token,
            &ack_msg(&room_id, &writer, 8),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let sse_resp = server.events(&room_id, &writer_token).await;
    assert_eq!(sse_resp.status(), StatusCode::OK);
    let mut stream = sse_resp.bytes_stream();

    // 2. A new client registers in the pruned room: it must bootstrap from a snapshot, and
    // there is none, so the writer is asked for one.
    let newcomer = client("newcomer");
    let (newcomer_token, registered) = server.register(&room_id, &newcomer).await;
    match registered {
        ServerMessage::Registered { tail_seq, .. } => {
            assert!(tail_seq > SequenceNumber::new(1), "tail {tail_seq}")
        }
        other => panic!("Expected Registered, got {:?}", other),
    }
    let text = read_sse_until(&mut stream, "event: snapshot_wanted").await;
    assert!(
        text.contains("event: snapshot_wanted\ndata: \n"),
        "snapshot_wanted carries an empty data line: {text:?}"
    );

    // The bootstrapping client's heartbeat succeeds; only the writer is asked to upload.
    assert_eq!(
        server
            .heartbeat_ack(&room_id, &newcomer, &newcomer_token)
            .await,
        (false, None)
    );
    assert_eq!(
        server.heartbeat_ack(&room_id, &writer, &writer_token).await,
        (true, None)
    );

    // 3. The writer uploads a snapshot at 8: it is announced and the request ends.
    let upload = server
        .client
        .post(server.url(&format!("/rooms/{room_id}/snapshot/upload")))
        .bearer_auth(&writer_token)
        .header("x-snapshot-head-seq", "8")
        .body(envelope(1000, 8))
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::OK);

    read_sse_until(&mut stream, "event: snapshot_available\ndata: 8\n").await;
    assert_eq!(
        server.heartbeat_ack(&room_id, &writer, &writer_token).await,
        (false, Some(SequenceNumber::new(8)))
    );
    assert_eq!(
        server
            .heartbeat_ack(&room_id, &newcomer, &newcomer_token)
            .await,
        (false, Some(SequenceNumber::new(8)))
    );
}

#[tokio::test]
async fn recreated_room_resets_the_cursors_of_the_deleted_one() {
    let server = TestServer::start(|_| {}).await;
    let (schema_id, room_id) = server
        .room_with_schema("todo-schema", "room-recreated")
        .await;
    let register_incarnation =
        async |client_id: &ClientId| match server.register(&room_id, client_id).await {
            (
                token,
                ServerMessage::Registered {
                    room_incarnation, ..
                },
            ) => (token, room_incarnation),
            other => panic!("Expected Registered, got {other:?}"),
        };
    let commit_all = async |client_id: &ClientId, token: &str, count: u8| {
        for n in 1..=count {
            let msg = commit_msg(&room_id, client_id, n, 0, make_test_op(n.into()));
            let (status, _) = server.post_message(&room_id, "commit", token, &msg).await;
            assert_eq!(status, StatusCode::OK);
        }
    };
    let old = client("old");
    let (old_token, old_incarnation) = register_incarnation(&old).await;
    commit_all(&old, &old_token, 5).await;

    // The admin deletes the room and creates it again; another client commits 3 operations.
    let resp = server
        .admin(Method::DELETE, &format!("/admin/rooms/{room_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    server.create_room(room_id.as_str(), &schema_id).await;
    let other = client("other");
    let (other_token, incarnation) = register_incarnation(&other).await;
    assert_ne!(incarnation, old_incarnation);
    commit_all(&other, &other_token, 3).await;

    // `old` comes back with its cursor of the deleted room, or without saying which room it
    // belongs to: it must start over, although the cursor is also above the new room's head.
    let register = |current_seq, known_incarnation| ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: old.clone(),
        auth_token: old_token.clone(),
        current_seq,
        known_incarnation,
    };
    let old_cursor = async || {
        let room = server.room_manager.get_room(&room_id).unwrap();
        cursor(&room, &old).await
    };
    for known in [Some(old_incarnation), None] {
        let msg = register(Some(SequenceNumber::new(5)), known);
        let (status, reply) = server
            .post_message(&room_id, "register", &old_token, &msg)
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            matches!(
                reply,
                ServerMessage::Error {
                    code: ErrorCode::RoomReset,
                    ..
                }
            ),
            "{reply:?}"
        );
        assert_eq!(old_cursor().await, None);
    }

    // Without a cursor it starts over; with this room's incarnation, its cursor is accepted.
    for (seq, known) in [(0, None), (3, Some(incarnation))] {
        let current_seq = (seq > 0).then(|| SequenceNumber::new(seq));
        let (status, reply) = server
            .post_message(
                &room_id,
                "register",
                &old_token,
                &register(current_seq, known),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
        assert_eq!(old_cursor().await, Some(SequenceNumber::new(seq)));
    }
}

#[tokio::test]
async fn register_with_a_cursor_beyond_the_head_is_an_invalid_sequence() {
    let server = TestServer::start(|_| {}).await;
    let (_, room_id) = server
        .room_with_schema("todo-schema", "room-register-ahead")
        .await;
    let client_id = client("ahead");
    let token = token(&client_id, &room_id);
    let (
        _,
        ServerMessage::Registered {
            room_incarnation, ..
        },
    ) = server.register(&room_id, &client("peer")).await
    else {
        panic!("Expected Registered");
    };

    let register = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(7),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        auth_token: token.clone(),
        current_seq: Some(SequenceNumber::new(999)),
        known_incarnation: Some(room_incarnation),
    };
    match server
        .post_message(&room_id, "register", &token, &register)
        .await
    {
        (
            StatusCode::BAD_REQUEST,
            ServerMessage::Error {
                correlation_id,
                code,
                ..
            },
        ) => {
            assert_eq!(code, ErrorCode::InvalidSequence);
            assert_eq!(correlation_id, Some(CorrelationId::new(7)));
        }
        other => panic!("Expected an InvalidSequence error, got {:?}", other),
    }

    // The client was not registered: its heartbeat asks it to register.
    let heartbeat = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(8),
        room_id: room_id.clone(),
        client_id,
    };
    let (status, _) = server
        .post_message(&room_id, "heartbeat", &token, &heartbeat)
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
}
