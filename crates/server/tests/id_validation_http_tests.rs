use reqwest::StatusCode;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use zemdb_core::protocol::codec::{decode_message, encode_message};
use zemdb_core::protocol::messages::{ClientMessage, ErrorCode, ServerMessage};
use zemdb_core::*;
use zemdb_server::api::auth::generate_client_token;
use zemdb_server::api::router::{build_router, AppState};
use zemdb_server::config::ServerConfig;
use zemdb_server::relay::SnapshotRelay;
use zemdb_server::schema_registry::SchemaRegistry;
use zemdb_server::RoomManager;

const ADMIN_SECRET: &str = "test_admin_secret_key_123456789";
const AUTH_SECRET: &str = "test_cluster_secret_key_12345678";

struct TestServer {
    base_url: String,
    temp_dir: TempDir,
    data_dir: PathBuf,
    client: reqwest::Client,
}

async fn start_server() -> TestServer {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path().join("data");
    let config = Arc::new(ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        data_dir: data_dir.clone(),
        auth_secret: AUTH_SECRET.to_string(),
        admin_secret: ADMIN_SECRET.to_string(),
        lease_timeout_secs: 60,
        dormant_after_secs: None,
        dedup_lru_capacity: 1000,
        snapshot_ttl_secs: 60,
        snapshot_demand_ttl_secs: 60,
        max_snapshot_bytes: 16 * 1024 * 1024,
        ..ServerConfig::default()
    });
    let schema_registry = Arc::new(SchemaRegistry::new(data_dir.join("schemas")).unwrap());
    let table = TableSchema::builder("tasks").primary_key("id", DataType::Int);
    schema_registry
        .register_schema(
            SchemaId::new("tasks-schema").unwrap(),
            Schema::builder().table(table).build(),
        )
        .unwrap();
    let relay = Arc::new(
        SnapshotRelay::new(
            data_dir.join("snapshots"),
            Duration::from_secs(60),
            config.max_snapshot_bytes,
        )
        .unwrap(),
    );
    let room_manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&schema_registry),
        Arc::clone(&relay),
    ));
    let app = build_router(AppState::new(config, schema_registry, room_manager, relay));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestServer {
        base_url: format!("http://{}", addr),
        temp_dir,
        data_dir,
        client: reqwest::Client::new(),
    }
}

#[tokio::test]
async fn creating_a_room_with_a_traversal_id_is_rejected_and_writes_nothing_outside() {
    let server = start_server().await;

    for room_id in ["../escape", "../../escape", "Escape"] {
        let resp = server
            .client
            .post(format!("{}/admin/rooms", server.base_url))
            .bearer_auth(ADMIN_SECRET)
            .header("content-type", "application/json")
            .body(format!(
                r#"{{"room_id":"{room_id}","schema_id":"tasks-schema"}}"#
            ))
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "room id {room_id:?}"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["code"], "BadRequest", "room id {room_id:?}: {body}");
    }

    assert!(!server.data_dir.join("escape").exists());
    assert!(!server.temp_dir.path().join("escape").exists());
    assert!(!server.data_dir.join("rooms").join("Escape").exists());
}

#[tokio::test]
async fn data_plane_request_with_invalid_room_id_gets_binary_bad_request() {
    let server = start_server().await;

    // `%2F` is an encoded "/": it reaches the server inside the room id segment, which the
    // router decodes to "x/../../escape". (An encoded ".." alone is normalized away by the
    // HTTP client before the request is sent.)
    for room_id in ["x%2F..%2F..%2Fescape", "UPPER"] {
        let resp = server
            .client
            .post(format!("{}/rooms/{room_id}/register", server.base_url))
            .body(Vec::<u8>::new())
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "room id {room_id:?}"
        );
        let body = resp.bytes().await.unwrap();
        match decode_message::<ServerMessage>(&body).unwrap() {
            ServerMessage::Error { code, .. } => assert_eq!(code, ErrorCode::BadRequest),
            other => panic!("expected an error message, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn admin_request_with_invalid_room_id_gets_bad_request() {
    let server = start_server().await;

    let resp = server
        .client
        .get(format!("{}/admin/rooms/UPPER", server.base_url))
        .bearer_auth(ADMIN_SECRET)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

fn client_token(client_id: &str, room_id: &str) -> String {
    generate_client_token(
        &ClientId::new(client_id).unwrap(),
        &RoomId::new(room_id).unwrap(),
        Duration::from_secs(300),
        AUTH_SECRET,
    )
}

async fn create_room(server: &TestServer, room_id: &str) {
    let resp = server
        .client
        .post(format!("{}/admin/rooms", server.base_url))
        .bearer_auth(ADMIN_SECRET)
        .json(&serde_json::json!({"room_id": room_id, "schema_id": "tasks-schema"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

/// Asserts a binary `ServerMessage::Error` response with the given status and code.
async fn assert_binary_error(resp: reqwest::Response, status: StatusCode, code: ErrorCode) {
    assert_eq!(resp.status(), status);
    assert_eq!(
        resp.headers()["content-type"],
        "application/octet-stream",
        "error responses of the binary API are binary frames"
    );
    let body = resp.bytes().await.unwrap();
    match decode_message::<ServerMessage>(&body) {
        Ok(ServerMessage::Error { code: actual, .. }) => assert_eq!(actual, code),
        other => panic!("expected a binary error frame, got {other:?}"),
    }
}

/// Lists every file below `dir`, recursively.
fn files_under(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(files_under(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

#[tokio::test]
async fn creating_a_schema_with_a_traversal_id_is_rejected_and_writes_nothing_outside() {
    let server = start_server().await;
    let schemas_dir = server.data_dir.join("schemas");
    let before = files_under(server.temp_dir.path());

    let resp = server
        .client
        .post(format!("{}/admin/schemas", server.base_url))
        .bearer_auth(ADMIN_SECRET)
        .json(&serde_json::json!({
            "schema_id": "../x",
            "schema": {"tables": []}
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "BadRequest", "{body}");
    assert!(!server.data_dir.join("x.json").exists());
    let after = files_under(server.temp_dir.path());
    assert_eq!(before, after, "no file may be written anywhere");
    assert!(after.iter().all(|p| p.starts_with(&schemas_dir)));
}

#[tokio::test]
async fn snapshot_relay_with_a_traversal_room_id_is_rejected_and_writes_nothing() {
    let server = start_server().await;
    let before = files_under(server.temp_dir.path());

    let upload = server
        .client
        .post(format!(
            "{}/rooms/x%2F..%2Fy/snapshot/upload",
            server.base_url
        ))
        .bearer_auth(ADMIN_SECRET)
        .header("x-snapshot-head-seq", "1")
        .body(vec![1u8; 64])
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::BAD_REQUEST);

    let chunk = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("y").unwrap(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: 64,
        snapshot_hash: ServerMessage::compute_snapshot_hash(&[1u8; 64]),
        data: vec![1u8; 64].into(),
    };
    let upload_chunk = server
        .client
        .post(format!(
            "{}/rooms/x%2F..%2Fy/snapshot/upload-chunk",
            server.base_url
        ))
        .bearer_auth(ADMIN_SECRET)
        .body(encode_message(&chunk).unwrap())
        .send()
        .await
        .unwrap();
    assert_binary_error(upload_chunk, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;

    assert_eq!(before, files_under(server.temp_dir.path()));
    assert!(!server.data_dir.join("y_1.snap.zst").exists());
}

#[tokio::test]
async fn invalid_id_inside_a_binary_body_gets_binary_bad_request() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    let token = client_token("alice", "room-a");

    let msg = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room-a").unwrap(),
        client_id: ClientId::new("alice").unwrap(),
    };
    let mut body = encode_message(&msg).unwrap();
    // Same length, so the frame stays well formed: only the room id becomes invalid.
    let at = body
        .windows(b"room-a".len())
        .position(|w| w == b"room-a")
        .unwrap();
    body[at..at + 6].copy_from_slice(b"ROOM-A");

    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .bearer_auth(&token)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn token_for_another_room_gets_binary_forbidden() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    create_room(&server, "room-b").await;
    let token_a = client_token("alice", "room-a");

    let msg = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room-b").unwrap(),
        client_id: ClientId::new("alice").unwrap(),
    };
    let resp = server
        .client
        .post(format!("{}/rooms/room-b/heartbeat", server.base_url))
        .bearer_auth(&token_a)
        .body(encode_message(&msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
}

#[tokio::test]
async fn missing_token_gets_binary_unauthorized() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    let msg = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room-a").unwrap(),
        client_id: ClientId::new("alice").unwrap(),
    };
    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .body(encode_message(&msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
}

#[tokio::test]
async fn sse_with_a_token_for_another_room_is_forbidden() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    create_room(&server, "room-b").await;
    let token_a = client_token("alice", "room-a");

    let resp = server
        .client
        .get(format!(
            "{}/rooms/room-b/events?token={token_a}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
}

#[tokio::test]
async fn payload_identity_must_match_the_authenticated_request() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    create_room(&server, "room-b").await;
    let token = client_token("alice", "room-a");

    // Payload names another room than the path: malformed request.
    let other_room = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room-b").unwrap(),
        client_id: ClientId::new("alice").unwrap(),
    };
    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .bearer_auth(&token)
        .body(encode_message(&other_room).unwrap())
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;

    // Payload names another client than the token: impersonation.
    let other_client = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(2),
        room_id: RoomId::new("room-a").unwrap(),
        client_id: ClientId::new("mallory").unwrap(),
    };
    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .bearer_auth(&token)
        .body(encode_message(&other_client).unwrap())
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // A message of another kind than the endpoint expects.
    let wrong_kind = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(3),
        room_id: RoomId::new("room-a").unwrap(),
    };
    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .bearer_auth(&token)
        .body(encode_message(&wrong_kind).unwrap())
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn admin_path_with_invalid_schema_id_gets_json_bad_request() {
    let server = start_server().await;

    let resp = server
        .client
        .get(format!("{}/admin/schemas/UPPER", server.base_url))
        .bearer_auth(ADMIN_SECRET)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "BadRequest", "{body}");
}

async fn register_client(server: &TestServer, client_id: &str, room_id: &str) {
    let msg = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new(room_id).unwrap(),
        client_id: ClientId::new(client_id).unwrap(),
        auth_token: client_token(client_id, room_id),
        current_seq: None,
    };
    let resp = server
        .client
        .post(format!("{}/rooms/{room_id}/register", server.base_url))
        .body(encode_message(&msg).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

fn heartbeat_body(client_id: &str, room_id: &str) -> Vec<u8> {
    encode_message(&ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(2),
        room_id: RoomId::new(room_id).unwrap(),
        client_id: ClientId::new(client_id).unwrap(),
    })
    .unwrap()
}

#[tokio::test]
async fn relay_rejects_a_client_token_for_another_room_as_forbidden() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    create_room(&server, "room-b").await;

    let resp = server
        .client
        .post(format!("{}/rooms/room-b/snapshot/upload", server.base_url))
        .bearer_auth(client_token("alice", "room-a"))
        .header("x-snapshot-head-seq", "1")
        .body(vec![1u8, 2, 3])
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
}

#[tokio::test]
async fn admin_secret_is_not_accepted_as_a_client_token() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .bearer_auth(ADMIN_SECRET)
        .body(heartbeat_body("alice", "room-a"))
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
}

#[tokio::test]
async fn snapshot_upload_header_errors_are_binary_bad_request() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    for head_seq in [None, Some("0"), Some("abc")] {
        let mut req = server
            .client
            .post(format!("{}/rooms/room-a/snapshot/upload", server.base_url))
            .bearer_auth(ADMIN_SECRET)
            .body(vec![1u8, 2, 3]);
        if let Some(value) = head_seq {
            req = req.header("x-snapshot-head-seq", value);
        }
        let resp = req.send().await.unwrap();
        assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
    }
}

#[tokio::test]
async fn token_in_query_string_is_only_accepted_by_the_event_stream() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    register_client(&server, "alice", "room-a").await;
    let token = client_token("alice", "room-a");

    // A registered client with a valid token: accepted from the header...
    let resp = server
        .client
        .post(format!("{}/rooms/room-a/heartbeat", server.base_url))
        .bearer_auth(&token)
        .body(heartbeat_body("alice", "room-a"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // ...but not from the URL, where it would end up in access logs.
    let resp = server
        .client
        .post(format!(
            "{}/rooms/room-a/heartbeat?token={token}",
            server.base_url
        ))
        .body(heartbeat_body("alice", "room-a"))
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;

    // The SSE endpoint still accepts it, since browser EventSource cannot set headers.
    let resp = server
        .client
        .get(format!(
            "{}/rooms/room-a/events?token={token}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_json_without_json_content_type_gets_415_json_error() {
    let server = start_server().await;

    let resp = server
        .client
        .post(format!("{}/admin/rooms", server.base_url))
        .bearer_auth(ADMIN_SECRET)
        .body(r#"{"room_id":"room-a","schema_id":"tasks-schema"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "BadRequest");
}

#[tokio::test]
async fn undecodable_body_error_frame_carries_the_path_room_id() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    let resp = server
        .client
        .post(format!("{}/rooms/room-a/register", server.base_url))
        .body(vec![0xFFu8; 8])
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.bytes().await.unwrap();
    match decode_message::<ServerMessage>(&body).unwrap() {
        ServerMessage::Error { code, room_id, .. } => {
            assert_eq!(code, ErrorCode::BadRequest);
            assert_eq!(room_id, Some(RoomId::new("room-a").unwrap()));
        }
        other => panic!("expected an error frame, got {other:?}"),
    }
}
