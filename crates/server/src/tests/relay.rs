//! Tests of the snapshot relay as a whole: staging, serving and restarting.

use crate::relay::test_common::*;

#[tokio::test]
async fn chunks_are_read_from_the_snapshot_file() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let data = envelope(1000, 0x11);
    let hash = stage(&relay, 1, &data).await.unwrap();

    let path = snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(1), &hash));
    assert_eq!(fs::read(&path).unwrap(), data);

    // Rewrite the file behind the relay's back: what is served must be what is on disk.
    let mut on_disk = data.clone();
    on_disk[999] = 0x22;
    fs::write(&path, &on_disk).unwrap();
    let (_, _, served) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(&served[..], &on_disk[..]);
}

#[tokio::test]
async fn staged_snapshot_survives_a_restart() {
    let dir = tempdir().unwrap();
    let hash = {
        let relay = open(&dir);
        stage(&relay, 42, &envelope(1000, 4)).await.unwrap()
    };
    let relay = open(&dir);
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(42)));
    let (_, served_hash, data) = chunk(&relay, 0, 0, Some(hash)).await.unwrap();
    assert_eq!(served_hash, hash);
    assert_eq!(&data[..], &envelope(1000, 4)[..]);
}

#[tokio::test]
async fn replaced_snapshot_file_is_deleted_once_the_new_one_is_staged() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let old = stage(&relay, 5, &envelope(100, 1)).await.unwrap();
    let new = stage(&relay, 6, &envelope(100, 2)).await.unwrap();

    assert_eq!(
        files_in(&snapshots_dir(&dir)),
        vec![snapshot_file_name(&room(), seq(6), &new)]
    );
    assert!(!snapshots_dir(&dir)
        .join(snapshot_file_name(&room(), seq(5), &old))
        .exists());
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}
