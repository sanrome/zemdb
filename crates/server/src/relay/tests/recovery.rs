use super::*;
use crate::relay::test_common::*;

#[tokio::test]
async fn startup_discards_uploads_and_keeps_only_the_newest_valid_snapshot() {
    let dir = tempdir().unwrap();
    let snapshots = snapshots_dir(&dir);
    let uploads = uploads_dir(&dir);
    fs::create_dir_all(&uploads).unwrap();

    let write_snapshot = |n: u64, data: &[u8]| -> String {
        let name = snapshot_file_name(&room(), seq(n), &ServerMessage::compute_snapshot_hash(data));
        fs::write(snapshots.join(&name), data).unwrap();
        name
    };
    let _older = write_snapshot(5, &envelope(100, 5));
    let newest_valid = write_snapshot(10, &envelope(100, 10));
    // Newest by seq, but its content does not match the hash in its name.
    let corrupt_name = snapshot_file_name(
        &room(),
        seq(12),
        &ServerMessage::compute_snapshot_hash(b"other"),
    );
    fs::write(snapshots.join(&corrupt_name), envelope(100, 12)).unwrap();
    // Another room is recovered independently.
    let other_room = RoomId::new("other_room").unwrap();
    let other_data = envelope(100, 1);
    let other_name = snapshot_file_name(
        &other_room,
        seq(3),
        &ServerMessage::compute_snapshot_hash(&other_data),
    );
    fs::write(snapshots.join(&other_name), &other_data).unwrap();
    // Leftovers of interrupted writes and uploads.
    fs::write(uploads.join("relay-room_20.part"), b"partial").unwrap();
    fs::write(uploads.join("relay-room_21.tmp"), b"partial").unwrap();

    let relay = open(&dir);
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(10)));
    assert_eq!(relay.active_snapshot_seq(&other_room), Some(seq(3)));
    let mut expected = vec![newest_valid, other_name];
    expected.sort();
    assert_eq!(files_in(&snapshots), expected);
    assert!(files_in(&uploads).is_empty());

    let (_, _, served) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(&served[..], &envelope(100, 10)[..]);
}

#[tokio::test]
async fn startup_keeps_a_snapshot_it_cannot_read_and_does_not_fall_back_to_an_older_one() {
    let dir = tempdir().unwrap();
    let (older, newer) = {
        let relay = open(&dir);
        let older = stage(&relay, 5, &envelope(100, 1)).await.unwrap();
        // Simulate a newer snapshot promoted to disk: keep both files.
        let newer_data = envelope(100, 2);
        let newer = ServerMessage::compute_snapshot_hash(&newer_data);
        fs::write(
            snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(6), &newer)),
            &newer_data,
        )
        .unwrap();
        (older, newer)
    };
    let newer_path = snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(6), &newer));
    let older_path = snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(5), &older));

    fail_point::arm("relay_verify_snapshot", &newer_path);
    let relay = open(&dir);
    assert!(
        newer_path.exists(),
        "a snapshot that failed to read was deleted"
    );
    assert_eq!(
        relay.active_snapshot_seq(&room()),
        None,
        "recovery must not fall back to an older snapshot than one it could not read"
    );
    assert!(!older_path.exists());
}

#[tokio::test]
async fn startup_survives_a_failure_deleting_a_stale_file() {
    let dir = tempdir().unwrap();
    let older_path = {
        let relay = open(&dir);
        let older = stage(&relay, 5, &envelope(100, 1)).await.unwrap();
        let older_path = snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(5), &older));
        // Crash between promoting seq 6 and recording it: both files are on disk.
        let newer_data = envelope(100, 2);
        let newer = ServerMessage::compute_snapshot_hash(&newer_data);
        fs::write(
            snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(6), &newer)),
            &newer_data,
        )
        .unwrap();
        older_path
    };

    fail_point::arm("relay_remove_file", &older_path);
    let relay = SnapshotRelay::new(snapshots_dir(&dir), TTL, MAX_BYTES)
        .expect("a failed deletion must not abort startup");
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(6)));
    assert!(older_path.exists());
}

#[tokio::test]
async fn crash_between_promotion_and_install_recovers_the_newest_snapshot() {
    let dir = tempdir().unwrap();
    let newer = {
        let relay = open(&dir);
        stage(&relay, 5, &envelope(100, 1)).await.unwrap();
        let newer_data = envelope(100, 2);
        let newer = ServerMessage::compute_snapshot_hash(&newer_data);
        fs::write(
            snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(6), &newer)),
            &newer_data,
        )
        .unwrap();
        newer
    };
    let relay = open(&dir);
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(6)));
    assert_eq!(
        files_in(&snapshots_dir(&dir)),
        vec![snapshot_file_name(&room(), seq(6), &newer)]
    );
}

#[tokio::test]
async fn startup_drops_snapshots_older_than_the_ttl() {
    let dir = tempdir().unwrap();
    let path = {
        let relay = open(&dir);
        let hash = stage(&relay, 5, &envelope(100, 1)).await.unwrap();
        snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(5), &hash))
    };
    let old = SystemTime::now() - TTL - Duration::from_secs(1);
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(old)
        .unwrap();

    let relay = open(&dir);
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(!path.exists());
}
