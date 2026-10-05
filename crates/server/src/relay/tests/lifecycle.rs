use super::*;
use crate::relay::test_common::*;
use crate::relay::UPLOAD_IDLE_TIMEOUT;

#[tokio::test(start_paused = true)]
async fn idle_upload_expires_and_its_file_is_deleted() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    relay
        .stage_chunk(
            part(10, 0, layout, hash, &data[ranges[0].clone()]),
            bounds(0, 10),
        )
        .await
        .unwrap();

    // Idle time counts from the last chunk, not from the start.
    tokio::time::advance(UPLOAD_IDLE_TIMEOUT - Duration::from_secs(1)).await;
    relay
        .stage_chunk(
            part(10, 1, layout, hash, &data[ranges[1].clone()]),
            bounds(0, 10),
        )
        .await
        .unwrap();
    tokio::time::advance(UPLOAD_IDLE_TIMEOUT - Duration::from_secs(1)).await;
    relay.cleanup_expired().await;
    assert_eq!(files_in(&uploads_dir(&dir)), vec!["relay-room_10.part"]);

    tokio::time::advance(Duration::from_secs(1)).await;
    relay.cleanup_expired().await;
    assert!(files_in(&uploads_dir(&dir)).is_empty());
    assert!(relay.slots.is_empty(), "idle room locks are forgotten");

    // The upload is gone: the last chunk starts a new upload that cannot complete alone.
    assert!(!relay
        .stage_chunk(
            part(10, 2, layout, hash, &data[ranges[2].clone()]),
            bounds(0, 10)
        )
        .await
        .unwrap());
}

#[tokio::test(start_paused = true)]
async fn expired_snapshot_is_dropped_and_its_file_deleted() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    stage(&relay, 1, &envelope(100, 1)).await.unwrap();

    tokio::time::advance(TTL).await;
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    let missing = chunk(&relay, 0, 0, None).await;
    assert!(
        matches!(missing, Err(ServerError::RoomNotFound(_))),
        "{missing:?}"
    );

    relay.cleanup_expired().await;
    assert!(files_in(&snapshots_dir(&dir)).is_empty());
}

#[tokio::test]
async fn purge_removes_the_snapshot_the_upload_and_their_files() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    stage(&relay, 5, &envelope(100, 1)).await.unwrap();
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    relay
        .stage_chunk(
            part(10, 0, layout, hash, &data[ranges[0].clone()]),
            bounds(0, 10),
        )
        .await
        .unwrap();
    // A room whose id extends this one keeps its files.
    let neighbour = RoomId::new("relay-room_2").unwrap();
    relay
        .stage_snapshot(
            &neighbour,
            seq(1),
            Bytes::from(envelope(100, 2)),
            bounds(0, 10),
        )
        .await
        .unwrap();

    relay.purge_room(&room()).await.unwrap();
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert_eq!(relay.active_snapshot_seq(&neighbour), Some(seq(1)));
    assert_eq!(files_in(&snapshots_dir(&dir)).len(), 1);
    assert!(files_in(&uploads_dir(&dir)).is_empty());

    // A recreated room starts from nothing.
    stage(&relay, 1, &envelope(100, 3)).await.unwrap();
}

#[tokio::test]
async fn stage_that_was_under_way_when_the_room_was_deleted_is_dropped() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    // The room is deleted while the upload waits for the log range, after it was tied to the
    // room's state.
    let result = relay
        .stage_snapshot(&room(), seq(1), Bytes::from(envelope(100, 1)), async {
            relay.purge_room(&room()).await.unwrap();
            Ok(LogBounds {
                tail_seq: seq(0),
                head_seq: seq(10),
            })
        })
        .await;
    assert!(
        matches!(result, Err(ServerError::RoomNotFound(_))),
        "{result:?}"
    );
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(files_in(&snapshots_dir(&dir)).is_empty());
}

#[tokio::test(start_paused = true)]
async fn cleanup_keeps_room_locks_that_requests_still_hold() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let held = relay.slot(&room());
    relay.cleanup_expired().await;
    assert!(
        relay.slots.contains_key(&room()),
        "a referenced lock was dropped"
    );
    drop(held);
    relay.cleanup_expired().await;
    assert!(relay.slots.is_empty());
}

#[tokio::test(start_paused = true)]
async fn expiry_sweeper_deletes_expired_snapshots() {
    let dir = tempdir().unwrap();
    let relay = Arc::new(open(&dir));
    stage(&relay, 1, &envelope(100, 1)).await.unwrap();
    let sweeper = relay.spawn_expiry_sweeper();

    tokio::time::sleep(TTL + EXPIRY_SWEEP_PERIOD).await;
    // Let the sweep's blocking file deletion finish.
    for _ in 0..100 {
        if files_in(&snapshots_dir(&dir)).is_empty() {
            break;
        }
        tokio::task::yield_now().await;
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(files_in(&snapshots_dir(&dir)).is_empty());
    sweeper.abort();
}
