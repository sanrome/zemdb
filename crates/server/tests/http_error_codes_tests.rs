//! Error codes and statuses of the Data Plane over HTTP: protocol version mismatches,
//! authentication (401) versus authorization (403), unregistered clients (409), and the
//! binary format of every Data Plane response, including SSE errors after authentication.

mod common;

use common::{
    assert_binary_error, heartbeat, make_test_op, register_msg, tasks_schema, TestServer,
    AUTH_SECRET,
};
use reqwest::StatusCode;
use std::time::Duration;
use zemdb_core::*;
use zemdb_server::api::auth::generate_client_token;

/// Starts a server with rooms `room-a` and `room-b`.
async fn start_server() -> TestServer {
    let server = TestServer::start(|_| {}).await;
    let schema_id = server.register_schema("tasks", tasks_schema());
    for room in ["room-a", "room-b"] {
        server.create_room(room, &schema_id).await;
    }
    server
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
    async fn register_as(
        &self,
        client_id: &str,
        room_id: &str,
        auth_token: String,
    ) -> reqwest::Response {
        let msg = register_msg(&room(room_id), &client(client_id), &auth_token);
        self.post(
            &format!("{room_id}/register"),
            None,
            encode_message(&msg).unwrap(),
        )
        .await
    }
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

    // Snapshot relay (`RelayAuth`): chunk download and single-request upload.
    let chunk_request = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(3),
        room_id: room("room-b"),
        chunk_index: 0,
        chunk_size: 64 * 1024,
        snapshot_hash: None,
        room_incarnation: 0,
    };
    let resp = server
        .post(
            "room-b/snapshot/chunk",
            Some(&token_a),
            encode_message(&chunk_request).unwrap(),
        )
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = server
        .client
        .post(server.url("/rooms/room-b/snapshot/upload"))
        .bearer_auth(&token_a)
        .header("x-snapshot-head-seq", "1")
        .body(vec![1u8, 2, 3])
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // Event stream (`EventStreamAuth`), with the token in the query string.
    let resp = server
        .client
        .get(server.url(&format!("/rooms/room-b/events?token={token_a}")))
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

    // So does a message of another kind than the endpoint expects.
    let wrong_kind = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(3),
        room_id: room("room-a"),
    };
    let resp = server
        .post(
            "room-a/heartbeat",
            Some(&token),
            encode_message(&wrong_kind).unwrap(),
        )
        .await;
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn register_token_checks_tell_authentication_from_authorization() {
    let server = start_server().await;

    // Validly signed, but issued for another client or another room.
    let resp = server
        .register_as("alice", "room-a", valid_token("bob", "room-a"))
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = server
        .register_as("alice", "room-a", valid_token("alice", "room-b"))
        .await;
    assert_binary_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // Not a valid token at all, or expired.
    let resp = server
        .register_as("alice", "room-a", "v1.garbage".to_string())
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
    let resp = server
        .register_as("alice", "room-a", token("alice", "room-a", Duration::ZERO))
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;

    let resp = server
        .register_as("alice", "room-a", valid_token("alice", "room-a"))
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unregistered_client_gets_client_not_registered() {
    let server = start_server().await;
    let token = valid_token("alice", "room-a");

    let requests = [
        (
            "commit",
            ClientMessage::Commit {
                correlation_id: CorrelationId::new(1),
                room_id: room("room-a"),
                client_id: client("alice"),
                mutation_id: MutationId::new([1; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op: make_test_op(1),
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
        .get(server.url("/rooms/room-missing/events"))
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
        .get(server.url("/rooms/room-a/commit"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_binary_error(resp, StatusCode::METHOD_NOT_ALLOWED, ErrorCode::BadRequest).await;

    // Outside the Data Plane the router's default answer is kept.
    let resp = server
        .client
        .get(server.url("/no-such-path"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert!(resp
        .headers()
        .get("content-type")
        .is_none_or(|ct| ct != "application/octet-stream"));
}
