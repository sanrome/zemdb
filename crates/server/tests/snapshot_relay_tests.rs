use axum::body::Bytes;
use rimdb_core::id::{CorrelationId, RoomId, SequenceNumber};
use rimdb_core::protocol::messages::ServerMessage;
use rimdb_server::relay::SnapshotRelay;
use std::time::Duration;
use tempfile::tempdir;

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
    assert!(snap_file.is_file(), "Snapshot file should be written to disk");

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
    assert!(!snap_file.exists(), "Expired snapshot file should be deleted from disk");
}
