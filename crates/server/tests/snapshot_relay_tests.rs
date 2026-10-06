//! Snapshot relay over HTTP: acceptance rules, anchored downloads, error statuses and room
//! deletion.

use reqwest::StatusCode;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::sync::oneshot;
use zemdb_core::*;
use zemdb_server::api::router::{build_router, AppState};
use zemdb_server::config::ServerConfig;
use zemdb_server::relay::SnapshotRelay;
use zemdb_server::schema_registry::SchemaRegistry;
use zemdb_server::{RoomCommand, RoomManager};

const ADMIN_SECRET: &str = "relay_admin_secret_key_1234567890";
const AUTH_SECRET: &str = "relay_cluster_secret_key_123456789";

struct TestServer {
    base_url: String,
    snapshots_dir: PathBuf,
    room_manager: Arc<RoomManager>,
    client: reqwest::Client,
    _dir: TempDir,
}

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .build()
        .unwrap();
    Schema::from_tables(vec![table])
}

/// Starts a server with room `room` created and `ops` operations committed to it.
async fn start_server(ops: u64) -> TestServer {
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
    schema_registry
        .register_schema(schema_id.clone(), test_schema())
        .unwrap();
    let snapshots_dir = data_dir.join("snapshots");
    let relay = Arc::new(
        SnapshotRelay::new(
            &snapshots_dir,
            Duration::from_secs(config.snapshot_ttl_secs),
            config.max_snapshot_bytes,
        )
        .unwrap(),
    );
    let room_manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&schema_registry),
        Arc::clone(&relay),
    ));

    let room_id = room();
    room_manager
        .create_room(room_id.clone(), schema_id, None)
        .await
        .unwrap();
    let sender = room_manager.get_room(&room_id).unwrap();
    let writer = ClientId::new("writer").unwrap();
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: writer.clone(),
            current_seq: None,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
    let schema = test_schema();
    for i in 1..=ops {
        let row = RowBuilder::new()
            .set("id", i as i64)
            .set("title", "task")
            .build();
        let (tx, rx) = oneshot::channel();
        sender
            .send(RoomCommand::Commit {
                client_id: writer.clone(),
                mutation_id: MutationId::new([i as u8; 16]),
                last_ack_seq: SequenceNumber::new(i - 1),
                op: schema.to_operation_insert("tasks", &row, 1000).unwrap(),
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    let app = build_router(AppState::new(
        config,
        schema_registry,
        Arc::clone(&room_manager),
        relay,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestServer {
        base_url: format!("http://{addr}"),
        snapshots_dir,
        room_manager,
        client: reqwest::Client::new(),
        _dir: dir,
    }
}

fn room() -> RoomId {
    RoomId::new("relay-room").unwrap()
}

fn envelope(total_len: usize, fill: u8) -> Vec<u8> {
    let body = vec![fill; total_len - SNAPSHOT_HEADER_LEN];
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

impl TestServer {
    async fn upload(&self, room_id: &RoomId, seq: u64, data: Vec<u8>) -> reqwest::Response {
        self.client
            .post(format!("{}/rooms/{room_id}/snapshot/upload", self.base_url))
            .bearer_auth(ADMIN_SECRET)
            .header("x-snapshot-head-seq", seq.to_string())
            .body(data)
            .send()
            .await
            .unwrap()
    }

    async fn post_binary(&self, path: &str, msg: &ClientMessage) -> (StatusCode, ServerMessage) {
        let resp = self
            .client
            .post(format!("{}/rooms/{}/{path}", self.base_url, room()))
            .bearer_auth(ADMIN_SECRET)
            .body(encode_message(msg).unwrap())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.bytes().await.unwrap();
        (status, decode_message(&body).unwrap())
    }

    async fn request_chunk(
        &self,
        chunk_index: u32,
        snapshot_hash: Option<[u8; 32]>,
    ) -> (StatusCode, ServerMessage) {
        let msg = ClientMessage::RequestSnapshotChunk {
            correlation_id: CorrelationId::new(1),
            room_id: room(),
            chunk_index,
            chunk_size: 64 * 1024,
            snapshot_hash,
        };
        self.post_binary("snapshot/chunk", &msg).await
    }
}

async fn error_code(resp: reqwest::Response) -> ErrorCode {
    let body = resp.bytes().await.unwrap();
    match decode_message::<ServerMessage>(&body).unwrap() {
        ServerMessage::Error { code, .. } => code,
        other => panic!("expected an error frame, got {other:?}"),
    }
}

fn expect_error(reply: (StatusCode, ServerMessage), status: StatusCode, code: ErrorCode) {
    assert_eq!(reply.0, status, "{:?}", reply.1);
    match reply.1 {
        ServerMessage::Error { code: actual, .. } => assert_eq!(actual, code),
        other => panic!("expected an error frame, got {other:?}"),
    }
}

#[tokio::test]
async fn upload_outside_the_log_range_is_rejected() {
    let server = start_server(3).await;

    let resp = server.upload(&room(), 4, envelope(1000, 1)).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(resp).await, ErrorCode::BadRequest);

    let resp = server.upload(&room(), 3, envelope(1000, 1)).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn upload_that_is_not_a_snapshot_envelope_is_rejected() {
    let server = start_server(3).await;
    let resp = server.upload(&room(), 2, vec![0xAB; 1000]).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(resp).await, ErrorCode::BadRequest);
}

#[tokio::test]
async fn upload_not_newer_than_the_active_snapshot_is_superseded() {
    let server = start_server(3).await;
    assert_eq!(
        server.upload(&room(), 2, envelope(1000, 1)).await.status(),
        StatusCode::OK
    );
    let resp = server.upload(&room(), 1, envelope(1000, 2)).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(error_code(resp).await, ErrorCode::SnapshotSuperseded);
}

#[tokio::test]
async fn upload_for_a_room_that_does_not_exist_is_not_found() {
    let server = start_server(0).await;
    let missing = RoomId::new("missing-room").unwrap();
    let resp = server.upload(&missing, 1, envelope(1000, 1)).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(error_code(resp).await, ErrorCode::RoomNotFound);
}

#[tokio::test]
async fn anchored_download_of_a_replaced_snapshot_returns_409() {
    let server = start_server(3).await;
    let first = envelope(200 * 1024, 1);
    assert_eq!(
        server.upload(&room(), 1, first.clone()).await.status(),
        StatusCode::OK
    );

    let (status, reply) = server.request_chunk(0, None).await;
    assert_eq!(status, StatusCode::OK);
    let anchor = match reply {
        ServerMessage::SnapshotChunk {
            snapshot_hash,
            total_chunks,
            data,
            ..
        } => {
            assert_eq!(total_chunks, 4);
            assert_eq!(&data[..], &first[..64 * 1024]);
            snapshot_hash
        }
        other => panic!("expected a snapshot chunk, got {other:?}"),
    };
    assert_eq!(anchor, ServerMessage::compute_snapshot_hash(&first));

    assert_eq!(
        server
            .upload(&room(), 2, envelope(200 * 1024, 2))
            .await
            .status(),
        StatusCode::OK
    );
    expect_error(
        server.request_chunk(1, Some(anchor)).await,
        StatusCode::CONFLICT,
        ErrorCode::SnapshotSuperseded,
    );

    // Restarting from chunk 0 without an anchor serves the new snapshot.
    let (status, reply) = server.request_chunk(0, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(matches!(
        reply,
        ServerMessage::SnapshotChunk { snapshot_head_seq, .. } if snapshot_head_seq == SequenceNumber::new(2)
    ));
}

#[tokio::test]
async fn relay_client_mistakes_are_not_server_errors() {
    let server = start_server(3).await;

    // No snapshot staged yet.
    expect_error(
        server.request_chunk(0, None).await,
        StatusCode::NOT_FOUND,
        ErrorCode::RoomNotFound,
    );

    assert_eq!(
        server.upload(&room(), 1, envelope(1000, 1)).await.status(),
        StatusCode::OK
    );
    // Chunk index beyond the snapshot.
    expect_error(
        server.request_chunk(5, None).await,
        StatusCode::BAD_REQUEST,
        ErrorCode::BadRequest,
    );

    // Multipart chunk with inconsistent parameters.
    let bad_chunk = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(2),
        room_id: room(),
        snapshot_head_seq: SequenceNumber::new(2),
        chunk_index: 0,
        total_chunks: 2,
        total_bytes: 200 * 1024,
        snapshot_hash: [0; 32],
        data: bytes::Bytes::from(vec![0u8; 10]),
    };
    expect_error(
        server
            .post_binary("snapshot/upload-chunk", &bad_chunk)
            .await,
        StatusCode::BAD_REQUEST,
        ErrorCode::BadRequest,
    );

    // Multipart upload above the size limit.
    let oversized = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(3),
        room_id: room(),
        snapshot_head_seq: SequenceNumber::new(2),
        chunk_index: 0,
        total_chunks: 1000,
        total_bytes: 1 << 40,
        snapshot_hash: [0; 32],
        data: bytes::Bytes::from(vec![0u8; 1024]),
    };
    expect_error(
        server
            .post_binary("snapshot/upload-chunk", &oversized)
            .await,
        StatusCode::BAD_REQUEST,
        ErrorCode::BadRequest,
    );
}

#[tokio::test]
async fn deleting_a_room_purges_its_snapshot() {
    let server = start_server(3).await;
    assert_eq!(
        server.upload(&room(), 2, envelope(1000, 1)).await.status(),
        StatusCode::OK
    );
    assert_eq!(std::fs::read_dir(&server.snapshots_dir).unwrap().count(), 2);

    server.room_manager.delete_room(&room()).await.unwrap();

    let leftover: Vec<_> = std::fs::read_dir(&server.snapshots_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|name| name != "uploads")
        .collect();
    assert!(leftover.is_empty(), "snapshot files left: {leftover:?}");
    expect_error(
        server.request_chunk(0, None).await,
        StatusCode::NOT_FOUND,
        ErrorCode::RoomNotFound,
    );
}

#[tokio::test]
async fn single_request_upload_over_the_body_limit_gets_a_binary_413() {
    let server = start_server(3).await;
    let resp = server
        .upload(&room(), 1, vec![0u8; MAX_FRAME_SIZE + 1])
        .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error_code(resp).await, ErrorCode::BadRequest);
}

#[tokio::test]
async fn single_request_upload_is_acknowledged_with_a_binary_frame() {
    let server = start_server(3).await;
    let snapshot = envelope(1000, 1);

    let resp = server.upload(&room(), 2, snapshot.clone()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "application/octet-stream");
    let body = resp.bytes().await.unwrap();
    match decode_message::<ServerMessage>(&body).unwrap() {
        ServerMessage::SnapshotStaged {
            room_id,
            snapshot_head_seq,
            snapshot_hash,
        } => {
            assert_eq!(room_id, room());
            assert_eq!(snapshot_head_seq, SequenceNumber::new(2));
            assert_eq!(
                snapshot_hash,
                ServerMessage::compute_snapshot_hash(&snapshot)
            );
        }
        other => panic!("expected SnapshotStaged, got {other:?}"),
    }
}

/// Request whose body is a valid frame of exactly `frame_len` bytes: an `UploadSnapshotChunk`
/// sent to the chunk download endpoint, which decodes it and then rejects its kind.
fn wrong_kind_frame_of_len(frame_len: usize) -> Vec<u8> {
    let message = |data_len: usize| ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(1),
        room_id: room(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: 1,
        snapshot_hash: [0; 32],
        data: bytes::Bytes::from(vec![0u8; data_len]),
    };
    let probe_len = 1 << 20;
    let overhead = encode_message(&message(probe_len)).unwrap().len() - probe_len;
    let frame = encode_message(&message(frame_len - overhead)).unwrap();
    assert_eq!(frame.len(), frame_len);
    frame
}

#[tokio::test]
async fn frame_of_the_maximum_size_passes_the_body_limit_and_one_byte_more_does_not() {
    let server = start_server(0).await;
    let post = |body: Vec<u8>| {
        server
            .client
            .post(format!(
                "{}/rooms/{}/snapshot/chunk",
                server.base_url,
                room()
            ))
            .bearer_auth(ADMIN_SECRET)
            .body(body)
            .send()
    };

    // The largest frame the codec produces is read and decoded: the handler rejects it only
    // because it is not a chunk request.
    let max_frame = wrong_kind_frame_of_len(MAX_FRAME_SIZE);
    let resp = post(max_frame.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    match decode_message::<ServerMessage>(&resp.bytes().await.unwrap()).unwrap() {
        ServerMessage::Error { code, message, .. } => {
            assert_eq!(code, ErrorCode::BadRequest);
            assert!(
                message.contains("Expected RequestSnapshotChunk"),
                "{message}"
            );
        }
        other => panic!("expected an error frame, got {other:?}"),
    }

    let mut oversized = max_frame;
    oversized.push(0);
    let resp = post(oversized).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error_code(resp).await, ErrorCode::BadRequest);
}
