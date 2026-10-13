mod common;

use common::*;
use futures::StreamExt;
use zemdb_core::{CompactRow, PrimaryKey, RoomId, Value};
use zemdb_storage::{KeyRange, MemoryStorageEngine, ScanOptions, StorageEngine};

#[tokio::test]
async fn test_range_scans_and_reverse_ordering() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-scan").unwrap();
    engine.open_room(&room_id, users_schema()).await.unwrap();
    engine
        .apply_batch(&room_id, inserts(1..=5, 100))
        .await
        .unwrap();

    let forward = scan_keys(&engine, &room_id, ScanOptions::new()).await;
    assert_eq!(forward, pks(1..=5));

    // ORDER BY pk DESC
    let backward = scan_keys(&engine, &room_id, ScanOptions::new().backward()).await;
    assert_eq!(backward, pks((1..=5).rev()));

    let range = KeyRange::from(PrimaryKey::single(2i64)..=PrimaryKey::single(4i64));
    let in_range = scan_keys(&engine, &room_id, ScanOptions::new().range(range)).await;
    assert_eq!(in_range, pks(2..=4));
}

#[tokio::test]
async fn test_limit_and_projection_pushdowns() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-pushdowns").unwrap();
    engine.open_room(&room_id, users_schema()).await.unwrap();
    engine
        .apply_batch(&room_id, inserts(1..=10, 100))
        .await
        .unwrap();

    let limited = scan_rows(&engine, &room_id, ScanOptions::new().limit(3)).await;
    assert_eq!(limited.len(), 3);

    let top_two = scan_keys(&engine, &room_id, ScanOptions::new().backward().limit(2)).await;
    assert_eq!(top_two, pks([10, 9]));

    // Only column 1 (name) and column 3 (active).
    let mut proj_stream = engine
        .scan(
            &room_id,
            "users",
            ScanOptions::new().limit(1).projection(vec![1, 3]),
        )
        .await
        .unwrap();
    let (pk, proj_row) = proj_stream.next().await.unwrap().unwrap();
    assert_eq!(pk, PrimaryKey::single(1i64));
    assert_eq!(
        proj_row,
        CompactRow::new(vec![Value::String("User 1".into()), Value::Bool(false)])
    );
}

#[tokio::test]
async fn test_disk_query_pushdowns_parity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = disk_engine(temp_dir.path());
    let room_id = RoomId::new("room-disk-pushdowns").unwrap();
    engine.open_room(&room_id, users_schema()).await.unwrap();
    engine
        .apply_batch(&room_id, inserts(1..=10, 100))
        .await
        .unwrap();

    let options = ScanOptions::new().backward().limit(3).projection(vec![1]);
    let names: Vec<_> = scan_rows(&engine, &room_id, options)
        .await
        .into_iter()
        .map(|(pk, row)| (pk, row[0].clone()))
        .collect();

    let expected: Vec<_> = [10i64, 9, 8]
        .into_iter()
        .map(|id| {
            let name = Value::String(format!("User {id}").into());
            (PrimaryKey::single(id), name)
        })
        .collect();
    assert_eq!(names, expected);
}

/// Scans 150 rows, more than two of the batches a scan takes from its table at a time.
async fn scan_streams_across_batches<E: StorageEngine>(engine: &E, room_id: &RoomId) {
    let total = 150;
    engine.open_room(room_id, users_schema()).await.unwrap();
    engine
        .apply_batch(room_id, inserts(1..=total, 100))
        .await
        .unwrap();

    let forward = scan_keys(engine, room_id, ScanOptions::new()).await;
    assert_eq!(forward, pks(1..=total));

    let backward = scan_keys(engine, room_id, ScanOptions::new().backward()).await;
    assert_eq!(backward, pks((1..=total).rev()));

    // A limit of 75 spans two batches.
    let limited = scan_rows(engine, room_id, ScanOptions::new().limit(75)).await;
    assert_eq!(limited.len(), 75);
}

#[tokio::test]
async fn test_multi_batch_lazy_streaming_memory() {
    let room_id = RoomId::new("room-multi-batch-mem").unwrap();
    scan_streams_across_batches(&MemoryStorageEngine::new(), &room_id).await;
}

#[tokio::test]
async fn test_multi_batch_lazy_streaming_disk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let room_id = RoomId::new("room-multi-batch-disk").unwrap();
    scan_streams_across_batches(&disk_engine(temp_dir.path()), &room_id).await;
}

/// Scans whose limit or range ends exactly where a batch of 64 rows does, in both directions.
async fn scan_stops_at_batch_boundaries<E: StorageEngine>(engine: &E, room_id: &RoomId) {
    engine.open_room(room_id, users_schema()).await.unwrap();
    engine
        .apply_batch(room_id, inserts(1..=150, 100))
        .await
        .unwrap();

    for limit in [64, 128] {
        let forward = scan_keys(engine, room_id, ScanOptions::new().limit(limit)).await;
        assert_eq!(forward, pks(1..=limit as i64), "forward, limit {limit}");
        let backward = scan_keys(engine, room_id, ScanOptions::new().backward().limit(limit)).await;
        let expected = pks((150 - limit as i64 + 1..=150).rev());
        assert_eq!(backward, expected, "backward, limit {limit}");
    }

    // Backward from the end of a range, so that every batch after the first starts below the
    // last key of the one before it.
    let range = KeyRange::from(PrimaryKey::single(11i64)..PrimaryKey::single(139i64));
    let backward = scan_keys(engine, room_id, ScanOptions::new().backward().range(range)).await;
    assert_eq!(backward, pks((11..139).rev()));
}

#[tokio::test]
async fn memory_scan_stops_at_batch_boundaries() {
    let room_id = RoomId::new("batch-boundaries-mem").unwrap();
    scan_stops_at_batch_boundaries(&MemoryStorageEngine::new(), &room_id).await;
}

#[tokio::test]
async fn disk_scan_stops_at_batch_boundaries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let room_id = RoomId::new("batch-boundaries-disk").unwrap();
    scan_stops_at_batch_boundaries(&disk_engine(temp_dir.path()), &room_id).await;
}

/// Reads two rows of a scan, then inserts rows 11..=15, updates the scores of 3..=10 and deletes
/// row 5. The rest of the scan must see the table as it was when the scan began.
async fn scan_is_snapshot_isolated<E: StorageEngine>(engine: &E, room_id: &RoomId) {
    engine.open_room(room_id, users_schema()).await.unwrap();
    engine
        .apply_batch(room_id, inserts(1..=10, 100))
        .await
        .unwrap();

    let mut stream = engine
        .scan(room_id, "users", ScanOptions::new())
        .await
        .unwrap();
    let (pk1, row1) = stream.next().await.unwrap().unwrap();
    assert_eq!(pk1, PrimaryKey::single(1i64));
    assert_eq!(row1[2], Value::Int(10));
    let (pk2, row2) = stream.next().await.unwrap().unwrap();
    assert_eq!(pk2, PrimaryKey::single(2i64));
    assert_eq!(row2[2], Value::Int(20));

    let mut seqs = 11u64..;
    let mut mutations: Vec<_> = (11..=15i64)
        .map(|i| {
            let phantom = row(i, &format!("Phantom {i}"), i * 100, false);
            insert(seqs.next().unwrap(), i, phantom, 200)
        })
        .collect();
    mutations
        .extend((3..=10i64).map(|i| update(seqs.next().unwrap(), i, 2, Value::Int(9999), 201)));
    mutations.push(delete(seqs.next().unwrap(), 5, 202));
    engine.apply_batch(room_id, mutations).await.unwrap();

    let rest: Vec<_> = stream.map(Result::unwrap).collect().await;
    let (remaining_pks, remaining_scores): (Vec<_>, Vec<_>) = rest
        .into_iter()
        .map(|(pk, row)| (pk, row[2].clone()))
        .unzip();
    // Exactly rows 3..=10, with row 5, deleted after the scan began, and none of the phantoms.
    assert_eq!(remaining_pks, pks(3..=10));
    // The original scores (30, 40, ..., 100), not 9999.
    assert_eq!(
        remaining_scores,
        (3..=10i64).map(|i| Value::Int(i * 10)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_scan_snapshot_isolation_memory() {
    let room_id = RoomId::new("room-iso-mem").unwrap();
    scan_is_snapshot_isolated(&MemoryStorageEngine::new(), &room_id).await;
}

#[tokio::test]
async fn test_scan_snapshot_isolation_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let room_id = RoomId::new("room-iso-disk").unwrap();
    scan_is_snapshot_isolated(&disk_engine(tmp.path()), &room_id).await;
}
