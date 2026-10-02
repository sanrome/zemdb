use axum::body::Bytes;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::id::{CorrelationId, RoomId, SequenceNumber};
use zemdb_core::protocol::messages::ServerMessage;
use zemdb_server::error::ServerError;
use zemdb_server::relay::{SnapshotChunkUpload, SnapshotRelay};

#[test]
fn test_snapshot_relay_disk_persistence_and_recovery() {
    let dir = tempdir().unwrap();
    let snapshots_dir = dir.path().join("snapshots");
    let room_id = RoomId::new("room-persist");
    let head_seq = SequenceNumber::new(42);
    let payload = Bytes::from_static(b"snapshot-binary-data-test-payload-bytes");

    // 1. Initialize relay and stage snapshot
    let relay1 = SnapshotRelay::new(&snapshots_dir, Duration::from_secs(60)).unwrap();
    let hash = relay1.stage_snapshot(room_id.clone(), head_seq, payload.clone());

    // File must exist on disk
    let snap_file = snapshots_dir.join(format!("{}_{}.snap.zst", room_id.as_str(), head_seq.get()));
    assert!(
        snap_file.is_file(),
        "Snapshot file should be written to disk"
    );

    // active_snapshot_seq returns 42
    assert_eq!(relay1.active_snapshot_seq(&room_id), Some(head_seq));

    // 2. Simulate server restart: create a new relay pointing to same dir
    let relay2 = SnapshotRelay::new(&snapshots_dir, Duration::from_secs(60)).unwrap();
    assert_eq!(relay2.active_snapshot_seq(&room_id), Some(head_seq));

    // Chunk retrieval works on recovered relay
    let chunk_msg = relay2
        .get_chunk(CorrelationId::new(1), &room_id, 0, 1024)
        .expect("should get chunk from recovered relay");

    match chunk_msg {
        ServerMessage::SnapshotChunk {
            snapshot_head_seq,
            snapshot_hash,
            data,
            ..
        } => {
            assert_eq!(snapshot_head_seq, head_seq);
            assert_eq!(snapshot_hash, hash);
            assert_eq!(data, payload);
        }
        _ => panic!("Expected SnapshotChunk message"),
    }

    // 3. Expiry cleans up disk file
    let relay_expired = SnapshotRelay::new(&snapshots_dir, Duration::from_millis(1)).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    relay_expired.cleanup_expired();

    assert_eq!(relay_expired.active_snapshot_seq(&room_id), None);
    assert!(
        !snap_file.exists(),
        "Expired snapshot file should be deleted from disk"
    );
}

#[test]
fn test_snapshot_relay_multipart_abrupt_disconnect_and_corrupted_chunk() {
    let dir = tempdir().unwrap();
    let snapshots_dir = dir.path().join("snapshots");
    let room_id = RoomId::new("room-relay-disconnect");
    let relay = SnapshotRelay::new(&snapshots_dir, Duration::from_millis(50)).unwrap();

    let full_data =
        Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz-out-of-order-assembly-payload");
    let snapshot_hash = ServerMessage::compute_snapshot_hash(&full_data);
    let total_bytes = full_data.len() as u64;
    let head_seq = SequenceNumber::new(10);

    let chunk0_data = full_data.slice(0..20);
    let chunk1_data = full_data.slice(20..40);
    let chunk2_data = full_data.slice(40..);

    // --- Scenario 1: Abrupt client disconnect after chunk 0 ---
    let upload_chunk0 = SnapshotChunkUpload {
        room_id: room_id.clone(),
        head_seq,
        chunk_index: 0,
        total_chunks: 3,
        total_bytes,
        snapshot_hash,
        data: chunk0_data.clone(),
    };
    let completed = relay.stage_chunk(upload_chunk0).unwrap();
    assert!(
        !completed,
        "Upload must not be marked complete after only 1 of 3 chunks"
    );
    assert_eq!(
        relay.active_snapshot_seq(&room_id),
        None,
        "Partial upload must not become an active snapshot"
    );

    // Wait for TTL to expire, cleanup expired multipart session
    std::thread::sleep(Duration::from_millis(60));
    relay.cleanup_expired();

    // --- Scenario 2: Corrupted chunk upload causing hash verification failure ---
    let corrupt_room = RoomId::new("room-corrupt-upload");
    let corrupt_head = SequenceNumber::new(15);
    let bad_chunk1_data = Bytes::from_static(b"corrupted-data-segment-here!");
    let corrupt_total_bytes = (chunk0_data.len() + bad_chunk1_data.len()) as u64;
    let expected_hash = ServerMessage::compute_snapshot_hash(&Bytes::from_static(
        b"valid-data-segment-here!padding",
    ));

    let corrupt_chunk0 = SnapshotChunkUpload {
        room_id: corrupt_room.clone(),
        head_seq: corrupt_head,
        chunk_index: 0,
        total_chunks: 2,
        total_bytes: corrupt_total_bytes,
        snapshot_hash: expected_hash,
        data: chunk0_data.clone(),
    };
    assert!(!relay.stage_chunk(corrupt_chunk0).unwrap());

    let corrupt_chunk1 = SnapshotChunkUpload {
        room_id: corrupt_room.clone(),
        head_seq: corrupt_head,
        chunk_index: 1,
        total_chunks: 2,
        total_bytes: corrupt_total_bytes,
        snapshot_hash: expected_hash,
        data: bad_chunk1_data,
    };
    let corrupt_res = relay.stage_chunk(corrupt_chunk1);
    match corrupt_res {
        Err(ServerError::Serialization(msg)) => {
            assert!(msg.contains("BLAKE3 digest verification failed"));
        }
        other => panic!("Expected Serialization error on corrupted chunk digest, got: {other:?}"),
    }
    assert_eq!(
        relay.active_snapshot_seq(&corrupt_room),
        None,
        "Corrupted snapshot must not be staged"
    );

    // --- Scenario 3: Out-of-order chunks [2, 0, 1] assembled successfully ---
    let ooo_room = RoomId::new("room-out-of-order");
    let ooo_head = SequenceNumber::new(20);

    let ooo_chunk2 = SnapshotChunkUpload {
        room_id: ooo_room.clone(),
        head_seq: ooo_head,
        chunk_index: 2,
        total_chunks: 3,
        total_bytes,
        snapshot_hash,
        data: chunk2_data.clone(),
    };
    assert!(!relay.stage_chunk(ooo_chunk2).unwrap());

    let ooo_chunk0 = SnapshotChunkUpload {
        room_id: ooo_room.clone(),
        head_seq: ooo_head,
        chunk_index: 0,
        total_chunks: 3,
        total_bytes,
        snapshot_hash,
        data: chunk0_data.clone(),
    };
    assert!(!relay.stage_chunk(ooo_chunk0).unwrap());

    let ooo_chunk1 = SnapshotChunkUpload {
        room_id: ooo_room.clone(),
        head_seq: ooo_head,
        chunk_index: 1,
        total_chunks: 3,
        total_bytes,
        snapshot_hash,
        data: chunk1_data.clone(),
    };
    let finished = relay.stage_chunk(ooo_chunk1).unwrap();
    assert!(
        finished,
        "Final chunk must trigger successful snapshot assembly"
    );

    assert_eq!(relay.active_snapshot_seq(&ooo_room), Some(ooo_head));

    // Verify all chunks retrieved from the assembled snapshot match original data
    let mut assembled = Vec::new();
    let total_chunks = total_bytes.div_ceil(20) as u32;
    for idx in 0..total_chunks {
        let msg = relay
            .get_chunk(CorrelationId::new(100 + idx as u64), &ooo_room, idx, 20)
            .unwrap();
        match msg {
            ServerMessage::SnapshotChunk { data, .. } => {
                assembled.extend_from_slice(&data);
            }
            other => panic!("Expected SnapshotChunk, got: {other:?}"),
        }
    }
    assert_eq!(assembled, full_data.as_ref());
}
