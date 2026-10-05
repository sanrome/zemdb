use super::*;
use crate::relay::test_common::*;

#[tokio::test]
async fn download_chunk_size_is_clamped() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let data = envelope(5 * 1024 * 1024, 3);
    stage(&relay, 1, &data).await.unwrap();

    // Zero (and anything below 64 KiB) is raised to 64 KiB.
    let (total, _, first) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(first.len(), MIN_CHUNK_BYTES as usize);
    assert_eq!(total, 80);

    // Anything above 4 MiB is lowered to 4 MiB.
    let (total, hash, first) = chunk(&relay, 0, u32::MAX, None).await.unwrap();
    assert_eq!(first.len(), MAX_CHUNK_BYTES as usize);
    assert_eq!(total, 2);
    let (_, _, last) = chunk(&relay, 1, u32::MAX, Some(hash)).await.unwrap();
    assert_eq!(last.len(), 1024 * 1024);
}

#[tokio::test]
async fn out_of_range_chunk_index_and_missing_snapshot_are_client_errors() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let missing = chunk(&relay, 0, 0, None).await;
    assert!(
        matches!(missing, Err(ServerError::RoomNotFound(_))),
        "{missing:?}"
    );

    let hash = stage(&relay, 1, &envelope(100, 1)).await.unwrap();
    assert_bad_request(chunk(&relay, 1, 0, Some(hash)).await);
}

#[tokio::test]
async fn anchored_download_of_a_replaced_snapshot_is_superseded() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let first = stage(&relay, 1, &envelope(200 * 1024, 1)).await.unwrap();
    let (_, hash, _) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(hash, first);
    chunk(&relay, 1, 0, Some(first)).await.unwrap();

    let second = stage(&relay, 2, &envelope(200 * 1024, 2)).await.unwrap();
    assert_superseded(chunk(&relay, 2, 0, Some(first)).await);

    // Restarting without an anchor gets the new snapshot.
    let (_, hash, _) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(hash, second);
    chunk(&relay, 2, 0, Some(second)).await.unwrap();
}

#[tokio::test]
async fn download_after_the_first_chunk_requires_the_anchor() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    stage(&relay, 1, &envelope(200 * 1024, 1)).await.unwrap();
    assert_bad_request(chunk(&relay, 1, 0, None).await);
}

#[tokio::test]
async fn first_chunk_drops_a_snapshot_below_the_retained_range() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let hash = stage(&relay, 5, &envelope(100, 1)).await.unwrap();
    let path = snapshots_dir(&dir).join(snapshot_file_name(&room(), seq(5), &hash));

    // Still usable while tail - 1 <= 5.
    relay
        .get_chunk(CorrelationId::new(1), &room(), 0, 0, None, bounds(6, 20))
        .await
        .unwrap();

    // The log moved on: tail - 1 = 6 > 5.
    let result = relay
        .get_chunk(CorrelationId::new(1), &room(), 0, 0, None, bounds(7, 20))
        .await;
    assert!(
        matches!(result, Err(ServerError::RoomNotFound(_))),
        "{result:?}"
    );
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(!path.exists());
}

#[tokio::test]
async fn failed_read_of_a_replaced_snapshot_is_superseded() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    stage(&relay, 1, &envelope(100, 1)).await.unwrap();
    let old = relay.active(&room()).unwrap();
    stage(&relay, 2, &envelope(100, 2)).await.unwrap();

    // The old file is gone; put something unreadable as a file in its place so the error is
    // not "not found".
    fs::create_dir(&old.path).unwrap();
    assert_superseded(relay.read_chunk(&room(), &old, 0, 10).await);

    // While it is still the active snapshot, a read error is reported as such.
    let current = relay.active(&room()).unwrap();
    fs::remove_file(&current.path).unwrap();
    fs::create_dir(&current.path).unwrap();
    let result = relay.read_chunk(&room(), &current, 0, 10).await;
    assert!(matches!(result, Err(ServerError::Io(_))), "{result:?}");
}
