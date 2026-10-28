mod common;

use common::{
    assert_binary_error, assert_json_error, heartbeat, register_msg, tasks_schema, TestServer,
    ADMIN_SECRET,
};
use reqwest::{Method, StatusCode};
use std::path::PathBuf;
use zemdb_core::*;

async fn start_server() -> TestServer {
    let server = TestServer::start(|_| {}).await;
    server.register_schema("tasks-schema", tasks_schema());
    server
}

#[tokio::test]
async fn creating_a_room_with_a_traversal_id_is_rejected_and_writes_nothing_outside() {
    let server = start_server().await;

    for room_id in ["../escape", "../../escape", "Escape"] {
        let resp = server
            .admin(Method::POST, "/admin/rooms")
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
        assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    }

    assert!(!server.data_dir.join("escape").exists());
    assert!(!server.dir.path().join("escape").exists());
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
            .post(&format!("{room_id}/register"), None, Vec::new())
            .await;

        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "room id {room_id:?}"
        );
        assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
    }
}

#[tokio::test]
async fn admin_request_with_invalid_room_id_gets_bad_request() {
    let server = start_server().await;

    let resp = server
        .admin(Method::GET, "/admin/rooms/UPPER")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

fn client_token(client_id: &str, room_id: &str) -> String {
    common::token(
        &ClientId::new(client_id).unwrap(),
        &RoomId::new(room_id).unwrap(),
    )
}

async fn create_room(server: &TestServer, room_id: &str) {
    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .json(&serde_json::json!({"room_id": room_id, "schema_id": "tasks-schema"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
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
    let before = files_under(server.dir.path());

    let resp = server
        .admin(Method::POST, "/admin/schemas")
        .json(&serde_json::json!({
            "schema_id": "../x",
            "schema": {"tables": []}
        }))
        .send()
        .await
        .unwrap();

    assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    assert!(!server.data_dir.join("x.json").exists());
    let after = files_under(server.dir.path());
    assert_eq!(before, after, "no file may be written anywhere");
    assert!(after.iter().all(|p| p.starts_with(&schemas_dir)));
}

#[tokio::test]
async fn snapshot_relay_with_a_traversal_room_id_is_rejected_and_writes_nothing() {
    let server = start_server().await;
    let before = files_under(server.dir.path());

    let upload = server
        .admin(Method::POST, "/rooms/x%2F..%2Fy/snapshot/upload")
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
        room_incarnation: 0,
    };
    let upload_chunk = server
        .post(
            "x%2F..%2Fy/snapshot/upload-chunk",
            Some(ADMIN_SECRET),
            encode_message(&chunk).unwrap(),
        )
        .await;
    assert_binary_error(upload_chunk, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;

    assert_eq!(before, files_under(server.dir.path()));
    assert!(!server.data_dir.join("y_1.snap.zst").exists());
}

#[tokio::test]
async fn invalid_id_inside_a_binary_body_gets_binary_bad_request() {
    let server = start_server().await;
    create_room(&server, "room-a").await;
    let token = client_token("alice", "room-a");

    let mut body = heartbeat("alice", "room-a");
    // Same length, so the frame stays well formed: only the room id becomes invalid.
    let at = body
        .windows(b"room-a".len())
        .position(|w| w == b"room-a")
        .unwrap();
    body[at..at + 6].copy_from_slice(b"ROOM-A");

    let resp = server.post("room-a/heartbeat", Some(&token), body).await;
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn admin_path_with_invalid_schema_id_gets_json_bad_request() {
    let server = start_server().await;

    let resp = server
        .admin(Method::GET, "/admin/schemas/UPPER")
        .send()
        .await
        .unwrap();

    assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
}

async fn register_client(server: &TestServer, client_id: &str, room_id: &str) {
    let msg = register_msg(
        &RoomId::new(room_id).unwrap(),
        &ClientId::new(client_id).unwrap(),
        &client_token(client_id, room_id),
    );
    let resp = server
        .post(
            &format!("{room_id}/register"),
            None,
            encode_message(&msg).unwrap(),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_secret_is_not_accepted_as_a_client_token() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    let resp = server
        .post(
            "room-a/heartbeat",
            Some(ADMIN_SECRET),
            heartbeat("alice", "room-a"),
        )
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
}

#[tokio::test]
async fn snapshot_upload_header_errors_are_binary_bad_request() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    for head_seq in [None, Some("0"), Some("abc")] {
        let mut req = server
            .admin(Method::POST, "/rooms/room-a/snapshot/upload")
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
        .post(
            "room-a/heartbeat",
            Some(&token),
            heartbeat("alice", "room-a"),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // ...but not from the URL, where it would end up in access logs.
    let resp = server
        .post(
            &format!("room-a/heartbeat?token={token}"),
            None,
            heartbeat("alice", "room-a"),
        )
        .await;
    assert_binary_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;

    // The SSE endpoint still accepts it, since browser EventSource cannot set headers.
    let resp = server
        .client
        .get(server.url(&format!("/rooms/room-a/events?token={token}")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_json_without_json_content_type_gets_415_json_error() {
    let server = start_server().await;

    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .body(r#"{"room_id":"room-a","schema_id":"tasks-schema"}"#)
        .send()
        .await
        .unwrap();

    assert_json_error(resp, StatusCode::UNSUPPORTED_MEDIA_TYPE, "BadRequest").await;
}

#[tokio::test]
async fn undecodable_body_error_frame_carries_the_path_room_id() {
    let server = start_server().await;
    create_room(&server, "room-a").await;

    let resp = server.post("room-a/register", None, vec![0xFFu8; 8]).await;

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
