//! Snapshot relay over HTTP: acceptance rules, anchored downloads, error statuses and room
//! deletion.

mod common;

use common::{
    assert_binary_error, commit, envelope, make_test_op, register, token, TestServer, ADMIN_SECRET,
};
use reqwest::StatusCode;
use zemdb_core::*;

/// Starts a server with room `room` created and `ops` operations committed to it.
async fn start_server(ops: u64) -> TestServer {
    let server = TestServer::start(|_| {}).await;
    server.room_with_schema("tasks", room().as_str()).await;
    commit_ops(&server, ops).await;
    server
}

/// Commits `ops` operations to room `room`.
async fn commit_ops(server: &TestServer, ops: u64) {
    let sender = server.room_manager.get_room(&room()).unwrap();
    let writer = ClientId::new("writer").unwrap();
    register(&sender, &writer).await;
    for i in 1..=ops {
        commit(
            &sender,
            &writer,
            MutationId::new([i as u8; 16]),
            i - 1,
            make_test_op(i as i64),
        )
        .await
        .unwrap();
    }
}

fn room() -> RoomId {
    RoomId::new("relay-room").unwrap()
}

impl TestServer {
    async fn upload(&self, room_id: &RoomId, seq: u64, data: Vec<u8>) -> reqwest::Response {
        self.upload_as(room_id, ADMIN_SECRET, seq, data).await
    }

    async fn upload_as(
        &self,
        room_id: &RoomId,
        bearer: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> reqwest::Response {
        self.client
            .post(self.url(&format!("/rooms/{room_id}/snapshot/upload")))
            .bearer_auth(bearer)
            .header("x-snapshot-head-seq", seq.to_string())
            .body(data)
            .send()
            .await
            .unwrap()
    }

    /// Uploads chunk `index` of `snapshot`, made of three 64 KiB chunks, at sequence 2, with the
    /// room's incarnation.
    async fn upload_chunk_as(&self, bearer: &str, snapshot: &[u8], index: u32) -> StatusCode {
        let incarnation = self.incarnation(&room()).await;
        self.upload_chunk_in(bearer, incarnation, 2, snapshot, index)
            .await
            .0
    }

    /// Uploads chunk `index` of `snapshot`, made of three 64 KiB chunks, at sequence `seq`, as a
    /// chunk of the room's `incarnation`.
    async fn upload_chunk_in(
        &self,
        bearer: &str,
        incarnation: u128,
        seq: u64,
        snapshot: &[u8],
        index: u32,
    ) -> (StatusCode, ServerMessage) {
        let len = 64 * 1024;
        let msg = ClientMessage::UploadSnapshotChunk {
            correlation_id: CorrelationId::new(1),
            room_id: room(),
            snapshot_head_seq: SequenceNumber::new(seq),
            chunk_index: index,
            total_chunks: 3,
            total_bytes: snapshot.len() as u64,
            snapshot_hash: ServerMessage::compute_snapshot_hash(snapshot),
            data: snapshot[index as usize * len..][..len].to_vec().into(),
            room_incarnation: incarnation,
        };
        let path = "snapshot/upload-chunk";
        self.post_message(&room(), path, bearer, &msg).await
    }

    async fn upload_ok(&self, seq: u64, data: Vec<u8>) {
        assert_eq!(
            self.upload(&room(), seq, data).await.status(),
            StatusCode::OK
        );
    }

    async fn post_binary(&self, path: &str, msg: &ClientMessage) -> (StatusCode, ServerMessage) {
        self.post_message(&room(), path, ADMIN_SECRET, msg).await
    }

    /// Requests a chunk as the snapshot worker, with the room's incarnation.
    async fn request_chunk(
        &self,
        chunk_index: u32,
        snapshot_hash: Option<[u8; 32]>,
    ) -> (StatusCode, ServerMessage) {
        let incarnation = self.incarnation(&room()).await;
        self.request_chunk_in(ADMIN_SECRET, incarnation, chunk_index, snapshot_hash)
            .await
    }

    /// Requests a chunk of 64 KiB as a download of the room's `incarnation`.
    async fn request_chunk_in(
        &self,
        bearer: &str,
        incarnation: u128,
        chunk_index: u32,
        snapshot_hash: Option<[u8; 32]>,
    ) -> (StatusCode, ServerMessage) {
        let msg = ClientMessage::RequestSnapshotChunk {
            correlation_id: CorrelationId::new(1),
            room_id: room(),
            chunk_index,
            chunk_size: 64 * 1024,
            snapshot_hash,
            room_incarnation: incarnation,
        };
        self.post_message(&room(), "snapshot/chunk", bearer, &msg)
            .await
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
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;

    server.upload_ok(3, envelope(1000, 1)).await;
}

#[tokio::test]
async fn upload_that_is_not_a_snapshot_envelope_is_rejected() {
    let server = start_server(3).await;
    let resp = server.upload(&room(), 2, vec![0xAB; 1000]).await;
    assert_binary_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;
}

#[tokio::test]
async fn upload_not_newer_than_the_active_snapshot_is_superseded() {
    let server = start_server(3).await;
    server.upload_ok(2, envelope(1000, 1)).await;
    let resp = server.upload(&room(), 1, envelope(1000, 2)).await;
    assert_binary_error(resp, StatusCode::CONFLICT, ErrorCode::SnapshotSuperseded).await;
}

/// A single-request upload replaces an upload in progress at the same sequence only where a
/// chunk of its own could: the same member's, or any for the snapshot worker. Otherwise it is
/// superseded and the upload in progress completes. Any upload at a higher sequence replaces it.
#[tokio::test]
async fn single_request_upload_replaces_an_upload_at_the_same_seq_only_where_a_chunk_could() {
    let member = |id| token(&ClientId::new(id).unwrap(), &room());
    let (alice, mallory) = (member("alice"), member("mallory"));
    let (chunked, single) = (envelope(3 * 64 * 1024, 1), envelope(1000, 2));
    for (chunked_by, single_by, single_seq, replaces) in [
        (alice.as_str(), mallory.as_str(), 2, false),
        (ADMIN_SECRET, mallory.as_str(), 2, false),
        (alice.as_str(), alice.as_str(), 2, true),
        (alice.as_str(), ADMIN_SECRET, 2, true),
        (alice.as_str(), mallory.as_str(), 3, true),
    ] {
        let server = start_server(3).await;
        let status = server.upload_chunk_as(chunked_by, &chunked, 0).await;
        assert_eq!(status, StatusCode::OK);
        let resp = server
            .upload_as(&room(), single_by, single_seq, single.clone())
            .await;
        if replaces {
            assert_eq!(resp.status(), StatusCode::OK);
        } else {
            assert_binary_error(resp, StatusCode::CONFLICT, ErrorCode::SnapshotSuperseded).await;
            for index in [1, 2] {
                let status = server.upload_chunk_as(chunked_by, &chunked, index).await;
                assert_eq!(status, StatusCode::OK);
            }
        }
        let served = if replaces { &single } else { &chunked };
        let (_, reply) = server.request_chunk(0, None).await;
        assert!(
            matches!(reply, ServerMessage::SnapshotChunk { snapshot_hash, .. }
                if snapshot_hash == ServerMessage::compute_snapshot_hash(served)),
            "{reply:?}"
        );
    }
}

#[tokio::test]
async fn upload_for_a_room_that_does_not_exist_is_not_found() {
    let server = start_server(0).await;
    let missing = RoomId::new("missing-room").unwrap();
    let resp = server.upload(&missing, 1, envelope(1000, 1)).await;
    assert_binary_error(resp, StatusCode::NOT_FOUND, ErrorCode::RoomNotFound).await;
}

#[tokio::test]
async fn anchored_download_of_a_replaced_snapshot_returns_409() {
    let server = start_server(3).await;
    let first = envelope(200 * 1024, 1);
    server.upload_ok(1, first.clone()).await;

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

    server.upload_ok(2, envelope(200 * 1024, 2)).await;
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
    let room_incarnation = server.incarnation(&room()).await;

    // No snapshot staged yet.
    expect_error(
        server.request_chunk(0, None).await,
        StatusCode::NOT_FOUND,
        ErrorCode::RoomNotFound,
    );

    server.upload_ok(1, envelope(1000, 1)).await;
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
        room_incarnation,
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
        room_incarnation,
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
    let incarnation = server.incarnation(&room()).await;
    let snapshots_dir = server.data_dir.join("snapshots");
    server.upload_ok(2, envelope(1000, 1)).await;
    assert_eq!(std::fs::read_dir(&snapshots_dir).unwrap().count(), 2);

    server.room_manager.delete_room(&room()).await.unwrap();

    let leftover: Vec<_> = std::fs::read_dir(&snapshots_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|name| name != "uploads")
        .collect();
    assert!(leftover.is_empty(), "snapshot files left: {leftover:?}");
    expect_error(
        server
            .request_chunk_in(ADMIN_SECRET, incarnation, 0, None)
            .await,
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
    assert_binary_error(resp, StatusCode::PAYLOAD_TOO_LARGE, ErrorCode::BadRequest).await;
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
        room_incarnation: 0,
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
    let path = format!("{}/snapshot/chunk", room());
    let post = |body: Vec<u8>| server.post(&path, Some(ADMIN_SECRET), body);

    // The largest frame the codec produces is read and decoded: the handler rejects it only
    // because it is not a chunk request.
    let max_frame = wrong_kind_frame_of_len(MAX_FRAME_SIZE);
    let resp = post(max_frame.clone()).await;
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
    let resp = post(oversized).await;
    assert_binary_error(resp, StatusCode::PAYLOAD_TOO_LARGE, ErrorCode::BadRequest).await;
}

/// The incarnation of `registered`, a `Registered` reply.
fn incarnation_of(registered: ServerMessage) -> u128 {
    match registered {
        ServerMessage::Registered {
            room_incarnation, ..
        } => room_incarnation,
        other => panic!("expected Registered, got {other:?}"),
    }
}

/// The snapshot worker takes the room's incarnation from the room's details, the same that
/// `Registered` gives a member. Declaring another one, the first chunk of a download and every
/// chunk of an upload are `RoomReset` and change nothing: the active snapshot is still served,
/// and the upload in progress completes.
#[tokio::test]
async fn relay_chunks_of_another_incarnation_are_room_reset_and_change_nothing() {
    let server = start_server(3).await;
    let (alice, registered) = server
        .register(&room(), &ClientId::new("alice").unwrap())
        .await;
    let incarnation = incarnation_of(registered);
    assert_eq!(server.incarnation(&room()).await, incarnation);
    let other = incarnation.wrapping_add(1);
    server.upload_ok(1, envelope(1000, 1)).await;
    let chunked = envelope(3 * 64 * 1024, 2);
    assert_eq!(
        server.upload_chunk_as(&alice, &chunked, 0).await,
        StatusCode::OK
    );

    for bearer in [alice.as_str(), ADMIN_SECRET] {
        let reset = |reply| expect_error(reply, StatusCode::CONFLICT, ErrorCode::RoomReset);
        reset(server.request_chunk_in(bearer, other, 0, None).await);
        // A chunk of the upload in progress, and the first one of a newer upload that would
        // replace it.
        reset(server.upload_chunk_in(bearer, other, 2, &chunked, 1).await);
        reset(server.upload_chunk_in(bearer, other, 3, &chunked, 0).await);
    }

    let served_seq = |reply| match reply {
        (
            StatusCode::OK,
            ServerMessage::SnapshotChunk {
                snapshot_head_seq, ..
            },
        ) => snapshot_head_seq.get(),
        other => panic!("expected a snapshot chunk, got {other:?}"),
    };
    assert_eq!(served_seq(server.request_chunk(0, None).await), 1);
    for index in [1, 2] {
        let status = server.upload_chunk_as(&alice, &chunked, index).await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(served_seq(server.request_chunk(0, None).await), 2);
}

/// A member of a room deleted and created again with the same id, whose chunks still declare the
/// old incarnation, gets nothing from the new room and gives it nothing, also once it registered
/// again in it with the same `ClientId`. While the new room has no snapshot, its first chunk is
/// `RoomNotFound`; once it has one, of other content, its first chunk and its upload are
/// `RoomReset`, its anchored chunk `SnapshotSuperseded`, and the new room keeps its snapshot.
#[tokio::test]
async fn a_member_of_a_room_created_again_gets_nothing_from_the_new_one() {
    let server = start_server(3).await;
    let alice = ClientId::new("alice").unwrap();
    let (token, registered) = server.register(&room(), &alice).await;
    let old = incarnation_of(registered);
    let old_snapshot = envelope(1000, 1);
    server.upload_ok(1, old_snapshot.clone()).await;
    let chunked = envelope(3 * 64 * 1024, 2);
    let (status, _) = server.upload_chunk_in(&token, old, 2, &chunked, 0).await;
    assert_eq!(status, StatusCode::OK);

    server.room_manager.delete_room(&room()).await.unwrap();
    let schema_id = SchemaId::new("tasks").unwrap();
    server.create_room(room().as_str(), &schema_id).await;
    commit_ops(&server, 3).await;
    expect_error(
        server.request_chunk_in(&token, old, 0, None).await,
        StatusCode::NOT_FOUND,
        ErrorCode::RoomNotFound,
    );
    let new_snapshot = envelope(1000, 3);
    server.upload_ok(1, new_snapshot.clone()).await;

    for registered_again in [false, true] {
        if registered_again {
            server.register(&room(), &alice).await;
        }
        let old_chunk = |index, anchor| server.request_chunk_in(&token, old, index, anchor);
        let (status, code) = (StatusCode::CONFLICT, ErrorCode::RoomReset);
        expect_error(old_chunk(0, None).await, status, code);
        let anchor = Some(ServerMessage::compute_snapshot_hash(&old_snapshot));
        expect_error(
            old_chunk(0, anchor).await,
            status,
            ErrorCode::SnapshotSuperseded,
        );
        for index in 0..3 {
            let reply = server
                .upload_chunk_in(&token, old, 2, &chunked, index)
                .await;
            expect_error(reply, status, code);
        }
    }
    let (_, reply) = server.request_chunk(0, None).await;
    assert!(
        matches!(reply, ServerMessage::SnapshotChunk { snapshot_hash, .. }
            if snapshot_hash == ServerMessage::compute_snapshot_hash(&new_snapshot)),
        "{reply:?}"
    );
}
