//! Error codes and statuses of the Data Plane over HTTP: protocol version mismatches,
//! authentication (401) versus authorization (403), unregistered clients (409), and the
//! binary format of every Data Plane response, including SSE errors after authentication.

use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use zemdb_core::*;
use zemdb_server::api::auth::generate_client_token;
use zemdb_server::api::router::{build_router, AppState};
use zemdb_server::config::ServerConfig;
use zemdb_server::relay::SnapshotRelay;
use zemdb_server::schema_registry::SchemaRegistry;
use zemdb_server::RoomManager;

const ADMIN_SECRET: &str = "errors_admin_secret_key_123456789";
const AUTH_SECRET: &str = "errors_cluster_secret_key_12345678";

struct TestServer {
    base_url: String,
    client: reqwest::Client,
    _dir: TempDir,
}

/// Starts a server with rooms `room-a` and `room-b`.
async fn start_server() -> TestServer {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let config = Arc::new(ServerConfig {
        data_dir: data_dir.clone(),
        auth_secret: AUTH_SECRET.to_string(),
        admin_secret: ADMIN_SECRET.to_string(),
        ..ServerConfig::default()
    });
    let schema_registry = Arc::new(SchemaRegistry::new(data_dir.join("schemas")).unwrap());
    let schema_id = SchemaId::new("tasks").unwrap();
    let table = TableSchema::builder("tasks").primary_key("id", DataType::Int);
    schema_registry
        .register_schema(schema_id.clone(), Schema::builder().table(table).build())
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
    for room in ["room-a", "room-b"] {
        room_manager
            .create_room(RoomId::new(room).unwrap(), schema_id.clone(), None)
            .await
            .unwrap();
    }
    let app = build_router(AppState::new(config, schema_registry, room_manager, relay));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestServer {
        base_url: format!("http://{addr}"),
        client: reqwest::Client::new(),
        _dir: dir,
    }
}

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn token(client_id: &str, room_id: &str, ttl: Duration) -> String {
    generate_client_token(&client(client_id), &room(room_id), ttl, AUTH_SECRET)
}

fn valid_token(client_id: &str, room_id: &str) -> String {
    token(client_id, room_id, Duration::from_secs(300))
}

impl TestServer {
    async fn post(&self, path: &str, bearer: Option<&str>, body: Vec<u8>) -> reqwest::Response {
        let mut req = self
            .client
            .post(format!("{}/rooms/{path}", self.base_url))
            .body(body);
        if let Some(token) = bearer {
            req = req.bearer_auth(token);
        }
        req.send().await.unwrap()
    }

    async fn register(
        &self,
        client_id: &str,
        room_id: &str,
        auth_token: String,
    ) -> reqwest::Response {
        let msg = ClientMessage::RegisterClient {
            correlation_id: CorrelationId::new(1),
            room_id: room(room_id),
            client_id: client(client_id),
            auth_token,
            current_seq: None,
        };
        self.post(
            &format!("{room_id}/register"),
            None,
            encode_message(&msg).unwrap(),
        )
        .await
    }
}

/// Asserts a binary `ServerMessage::Error` frame with the given status and code.
async fn assert_binary_error(resp: reqwest::Response, status: StatusCode, code: ErrorCode) {
    assert_eq!(resp.status(), status);
    assert_eq!(
        resp.headers()["content-type"],
        "application/octet-stream",
        "Data Plane errors are binary frames"
    );
    let body = resp.bytes().await.unwrap();
    match decode_message::<ServerMessage>(&body) {
        Ok(ServerMessage::Error { code: actual, .. }) => assert_eq!(actual, code),
        other => panic!("expected a binary error frame, got {other:?}"),
    }
}

fn heartbeat(client_id: &str, room_id: &str) -> Vec<u8> {
    encode_message(&ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(2),
        room_id: room(room_id),
        client_id: client(client_id),
    })
    .unwrap()
}

#[tokio::test]
async fn request_frame_of_another_protocol_version_is_a_version_mismatch() {
    let server = start_server().await;
    let token = valid_token("alice", "room-a");

    let mut body = heartbeat("alice", "room-a");
    body[2] = PROTOCOL_VERSION + 1;
    let resp = server.post("room-a/heartbeat", Some(&token), body).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let frame = resp.bytes().await.unwrap();
    // The reply is a frame of the server's own version: a client of the other version can
    // only read its header, which is enough to detect the mismatch.
    assert_eq!(peek_version(&frame), Some(PROTOCOL_VERSION));
    match decode_message::<ServerMessage>(&frame).unwrap() {
        ServerMessage::Error { code, .. } => assert_eq!(code, ErrorCode::ProtocolVersionMismatch),
        other => panic!("expected an error frame, got {other:?}"),
    }

    // Any other undecodable body is a plain bad request.
    let resp = server
        .post("room-a/heartbeat", Some(&token), b"not a frame".to_vec())
        .await;
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn missing_invalid_or_expired_token_is_unauthorized() {
    let server = start_server().await;

    let resp = server
        .post("room-a/heartbeat", None, heartbeat("alice", "room-a"))
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;

    let tampered = format!("{}0", valid_token("alice", "room-a"));
    let resp = server
        .post(
            "room-a/heartbeat",
            Some(&tampered),
            heartbeat("alice", "room-a"),
        )
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;

    let expired = token("alice", "room-a", Duration::ZERO);
    let resp = server
        .post(
            "room-a/heartbeat",
            Some(&expired),
            heartbeat("alice", "room-a"),
        )
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
}

#[tokio::test]
async fn valid_token_for_another_room_is_forbidden_on_every_extractor() {
    let server = start_server().await;
    let token_a = valid_token("alice", "room-a");

    // Client endpoints (`AuthenticatedRoom`).
    let resp = server
        .post(
            "room-b/heartbeat",
            Some(&token_a),
            heartbeat("alice", "room-b"),
        )
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // Snapshot relay (`RelayAuth`).
    let chunk_request = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(3),
        room_id: room("room-b"),
        chunk_index: 0,
        chunk_size: 64 * 1024,
        snapshot_hash: None,
    };
    let resp = server
        .post(
            "room-b/snapshot/chunk",
            Some(&token_a),
            encode_message(&chunk_request).unwrap(),
        )
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // Event stream (`EventStreamAuth`), with the token in the query string.
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
async fn payload_client_other_than_the_token_is_forbidden() {
    let server = start_server().await;
    let token = valid_token("alice", "room-a");

    let resp = server
        .post(
            "room-a/heartbeat",
            Some(&token),
            heartbeat("mallory", "room-a"),
        )
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // A payload room other than the path stays a malformed request.
    let resp = server
        .post(
            "room-a/heartbeat",
            Some(&token),
            heartbeat("alice", "room-b"),
        )
        .await;
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn register_token_checks_tell_authentication_from_authorization() {
    let server = start_server().await;

    // Validly signed, but issued for another client or another room.
    let resp = server
        .register("alice", "room-a", valid_token("bob", "room-a"))
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = server
        .register("alice", "room-a", valid_token("alice", "room-b"))
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // Not a valid token at all, or expired.
    let resp = server
        .register("alice", "room-a", "v1.garbage".to_string())
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
    let resp = server
        .register("alice", "room-a", token("alice", "room-a", Duration::ZERO))
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;

    let resp = server
        .register("alice", "room-a", valid_token("alice", "room-a"))
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unregistered_client_gets_client_not_registered() {
    let server = start_server().await;
    let token = valid_token("alice", "room-a");
    let schema = Schema::builder()
        .table(TableSchema::builder("tasks").primary_key("id", DataType::Int))
        .build();
    let row = RowBuilder::new().set("id", 1i64).build();

    let requests = [
        (
            "commit",
            ClientMessage::Commit {
                correlation_id: CorrelationId::new(1),
                room_id: room("room-a"),
                client_id: client("alice"),
                mutation_id: MutationId::new([1; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op: schema.to_operation_insert("tasks", &row, 1000).unwrap(),
            },
        ),
        (
            "sync",
            ClientMessage::Sync {
                correlation_id: CorrelationId::new(2),
                room_id: room("room-a"),
                client_id: client("alice"),
                from_seq: SequenceNumber::new(0),
                max_batch_size: 10,
            },
        ),
        (
            "ack",
            ClientMessage::Ack {
                correlation_id: CorrelationId::new(3),
                room_id: room("room-a"),
                client_id: client("alice"),
                ack_seq: SequenceNumber::new(0),
            },
        ),
        (
            "heartbeat",
            ClientMessage::Heartbeat {
                correlation_id: CorrelationId::new(4),
                room_id: room("room-a"),
                client_id: client("alice"),
            },
        ),
    ];
    for (endpoint, msg) in requests {
        let resp = server
            .post(
                &format!("room-a/{endpoint}"),
                Some(&token),
                encode_message(&msg).unwrap(),
            )
            .await;
        assert_binary_error(resp, StatusCode::CONFLICT, ErrorCode::ClientNotRegistered).await;
    }
}

#[tokio::test]
async fn event_stream_errors_after_authentication_are_binary_frames() {
    let server = start_server().await;

    // Authenticated for a room that does not exist: the failure comes after authentication,
    // when the stream subscribes to the room.
    let token = valid_token("alice", "room-missing");
    let resp = server
        .client
        .get(format!("{}/rooms/room-missing/events", server.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::NOT_FOUND, ErrorCode::RoomNotFound).await;
}

#[tokio::test]
async fn unknown_data_plane_paths_and_methods_get_binary_frames() {
    let server = start_server().await;
    let token = valid_token("alice", "room-a");

    // No such endpoint: 404, as a frame.
    let resp = server
        .post(
            "room-a/no-such-endpoint",
            Some(&token),
            heartbeat("alice", "room-a"),
        )
        .await;
    assert_binary_error(resp, StatusCode::NOT_FOUND, ErrorCode::BadRequest).await;

    // Known endpoint, wrong method: 405, as a frame.
    let resp = server
        .client
        .get(format!("{}/rooms/room-a/commit", server.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::METHOD_NOT_ALLOWED, ErrorCode::BadRequest).await;

    // Outside the Data Plane the router's default answer is kept.
    let resp = server
        .client
        .get(format!("{}/no-such-path", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert!(resp
        .headers()
        .get("content-type")
        .is_none_or(|ct| ct != "application/octet-stream"));
}
