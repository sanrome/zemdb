mod common;

use axum::http::StatusCode;
use bytes::Bytes;
use common::{
    build, commit, cursor, envelope, make_test_op, register, sync, tasks_schema, TestServer,
};
use std::sync::Arc;
use zemdb_core::*;
use zemdb_server::ServerError;

#[tokio::test]
async fn concurrent_get_or_spawn_reaches_a_single_actor() {
    let parts = build(|_| {});
    let schema_id = parts.register_schema("doc-schema", tasks_schema());
    let room_id = RoomId::new("race-condition-room").unwrap();

    let mut handles = Vec::new();
    for _ in 0..50 {
        let mgr = Arc::clone(&parts.room_manager);
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

    // All senders reach the same room actor: a client registered through the first one is
    // visible through the last one.
    let test_client = ClientId::new("probe-client").unwrap();
    let reg_result = register(&senders[0], &test_client).await;
    assert_eq!(reg_result.head_seq, SequenceNumber::new(0));

    let cursor = cursor(&senders[49], &test_client).await.unwrap();
    assert_eq!(cursor, SequenceNumber::new(0));
}

#[tokio::test]
async fn deleting_a_room_removes_its_directory() {
    let parts = build(|_| {});
    let schema_id = parts.register_schema("doc-schema", tasks_schema());
    let room_id = parts.create_room("deletion-target-room", &schema_id).await;
    let sender = parts.room_manager.get_room(&room_id).unwrap();

    let client_id = ClientId::new("deleter").unwrap();
    register(&sender, &client_id).await;
    commit(
        &sender,
        &client_id,
        MutationId::new([1u8; 16]),
        0,
        make_test_op(1),
    )
    .await
    .unwrap();

    let room_dir = parts.data_dir.join("rooms").join(room_id.as_str());
    assert!(room_dir.exists(), "Room directory must exist on disk");

    // Another manager on the same directory, as another process would be, does not own the
    // room's log: it cannot delete the room.
    let foreign = parts.restarted_manager().delete_room(&room_id).await;
    assert!(
        matches!(foreign, Err(ServerError::RoomLocked(_))),
        "{foreign:?}"
    );
    assert!(room_dir.exists());

    // Graceful deletion: shuts down actor, awaits loop termination, and cleans up directory
    parts.room_manager.delete_room(&room_id).await.unwrap();

    assert!(
        !room_dir.exists(),
        "Room directory must be completely removed from disk after graceful deletion"
    );
    assert!(parts.room_manager.get_room(&room_id).is_none());
}

#[tokio::test]
async fn data_plane_reactivates_a_room_after_restart() {
    let client_id = ClientId::new("reactivating-client").unwrap();
    let commit_msg = |room_id: &RoomId, i: u64, last_ack_seq: u64| ClientMessage::Commit {
        correlation_id: CorrelationId::new(10 + i),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        mutation_id: MutationId::new([i as u8; 16]),
        last_ack_seq: SequenceNumber::new(last_ack_seq),
        op: make_test_op(i as i64),
    };

    // Server Phase 1: Create room, register client, commit 2 operations
    let server1 = TestServer::start(|_| {}).await;
    let data_dir = server1.data_dir.clone();
    let (_, room_id) = server1
        .room_with_schema("doc-schema", "lazy-reactivation-room")
        .await;
    let (token, _) = server1.register(&room_id, &client_id).await;
    for i in 1..=2 {
        let resp = server1
            .post_frame(
                &format!("{room_id}/commit"),
                Some(&token),
                &commit_msg(&room_id, i, i - 1),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    server1.room_manager.shutdown_all().await;

    // Server Phase 2: Start a brand new server instance on the same data directory.
    // The in-memory cache `rooms` is empty. The control plane `/admin/rooms` is NOT invoked.
    let server2 = TestServer::start(|config| config.data_dir = data_dir).await;

    // Send a commit operation directly to the data plane endpoint.
    // The data plane must lazily reactivate and rehydrate the room actor from disk.
    let (status, reply) = server2
        .post_message(&room_id, "commit", &token, &commit_msg(&room_id, 3, 2))
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "Data plane should lazily reactivate room without control plane pre-warming"
    );
    assert!(
        matches!(reply, ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(3)),
        "{reply:?}"
    );
}

#[tokio::test]
async fn sync_batch_size_is_clamped() {
    let parts = build(|_| {});
    let schema_id = parts.register_schema("doc-schema", tasks_schema());
    let room_id = parts.create_room("clamp-batch-size-room", &schema_id).await;
    let sender = parts.room_manager.get_room(&room_id).unwrap();

    let reader_client = ClientId::new("sync-reader").unwrap();
    register(&sender, &reader_client).await;

    // Commit 1005 operations to room actor. The client keeps reporting cursor 0: a cursor
    // reported by an accepted commit advances the retention floor, and this test needs the
    // whole log retained to sync it from the start.
    for i in 1..=1005u64 {
        commit(
            &sender,
            &reader_client,
            MutationId::from_u128(i as u128),
            0,
            make_test_op(i as i64),
        )
        .await
        .unwrap();
    }

    let sync_res = sync(&sender, &reader_client, 0, 50_000).await.unwrap();
    assert_eq!(
        sync_res.ops.len(),
        1000,
        "max_batch_size must be clamped to the maximum allowed limit of 1000"
    );

    let sync0_res = sync(&sender, &reader_client, 0, 0).await.unwrap();
    assert_eq!(
        sync0_res.ops.len(),
        1,
        "max_batch_size 0 must be clamped to minimum allowed limit of 1"
    );
}

#[tokio::test]
async fn multipart_snapshot_upload_is_staged_and_verified() {
    let server = TestServer::start(|_| {}).await;
    let client_id = ClientId::new("snapshot-uploader").unwrap();

    // The relay only accepts snapshots inside the room's log range: create the room and
    // commit 11 operations (the snapshot below is at seq 10, the corrupted one at seq 11).
    let (_, room_id) = server
        .room_with_schema("doc-schema", "multipart-snapshot-room")
        .await;
    let client_token = common::token(&client_id, &room_id);
    let sender = server.room_manager.get_room(&room_id).unwrap();
    let room_incarnation = register(&sender, &client_id).await.room_incarnation;
    for i in 1..=11u64 {
        commit(
            &sender,
            &client_id,
            MutationId::new([i as u8; 16]),
            i - 1,
            make_test_op(i as i64),
        )
        .await
        .unwrap();
    }

    // A 300 KB snapshot, uploaded in three chunks of at most 100 KB.
    let snapshot_data = envelope(300 * 1024, 0x42);
    let snapshot_hash = ServerMessage::compute_snapshot_hash(&snapshot_data);
    let total_bytes = snapshot_data.len() as u64;
    let chunk_size = 100 * 1024;
    let chunk = |chunk_index: u32, data: &[u8]| ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(2 + u64::from(chunk_index)),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(10),
        chunk_index,
        total_chunks: 3,
        total_bytes,
        snapshot_hash,
        data: Bytes::copy_from_slice(data),
        room_incarnation,
    };
    let upload_path = format!("{room_id}/snapshot/upload-chunk");

    let unauth_resp = server
        .post_frame(&upload_path, None, &chunk(0, &snapshot_data[..chunk_size]))
        .await;
    assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

    // Only the final chunk consolidates and stages the full snapshot.
    for (i, data, expect_staged) in [
        (0, &snapshot_data[..chunk_size], false),
        (1, &snapshot_data[chunk_size..2 * chunk_size], false),
        (2, &snapshot_data[2 * chunk_size..], true),
    ] {
        let (status, ack) = server
            .post_message(
                &room_id,
                "snapshot/upload-chunk",
                &client_token,
                &chunk(i, data),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            matches!(
                ack,
                ServerMessage::SnapshotUploadChunkAck { chunk_index, total_chunks: 3, staged, .. }
                    if chunk_index == i && staged == expect_staged
            ),
            "chunk {i}: {ack:?}"
        );
    }

    // A chunk of the staged snapshot confirms its availability and BLAKE3 integrity.
    let req_chunk = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(5),
        room_id: room_id.clone(),
        chunk_index: 0,
        chunk_size: 100 * 1024,
        snapshot_hash: None,
        room_incarnation,
    };
    let (status, chunk_msg) = server
        .post_message(&room_id, "snapshot/chunk", &client_token, &req_chunk)
        .await;
    assert_eq!(status, StatusCode::OK);
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

    // Corrupted upload detection (tampered data causing BLAKE3 digest mismatch).
    // The snapshot is a valid envelope at a newer, in-range seq; only its hash is wrong, which
    // is a client error.
    let tampered = envelope(SNAPSHOT_HEADER_LEN + 4, b't');
    let single_chunk_tampered = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(6),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(11),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: tampered.len() as u64,
        snapshot_hash: [0xFF; 32], // incorrect hash
        data: Bytes::from(tampered),
        room_incarnation,
    };
    let corrupt_resp = server
        .post_frame(&upload_path, Some(&client_token), &single_chunk_tampered)
        .await;
    assert_eq!(corrupt_resp.status(), StatusCode::BAD_REQUEST);
}
