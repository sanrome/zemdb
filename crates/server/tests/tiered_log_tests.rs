use rimdb_core::id::SequenceNumber;
use rimdb_core::mutation::Operation;
use rimdb_core::protocol::messages::SequencedOperation;
use rimdb_core::value::{PrimaryKey, Value};
use rimdb_server::error::ServerError;
use rimdb_server::log::{RoomLifecyclePolicy, TieredLog};
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;

fn make_test_op(seq: u64) -> SequencedOperation {
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    let op = Operation::delete(1, pk, seq * 1000);
    SequencedOperation::new(SequenceNumber::new(seq), op)
}

#[test]
fn test_tiered_log_write_through_and_crash_recovery() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    // 1. Append 10 operations with Write-Through
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        assert_eq!(log.head_seq().get(), 0);

        for i in 1..=10 {
            log.append(make_test_op(i), None).unwrap();
            assert_eq!(log.head_seq().get(), i);
        }
        // Abruptly drop log without clean shutdown (simulating server crash)
    }

    // 2. Reopen from disk and verify 100% of data survived
    let (log2, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log2.head_seq().get(), 10);

    let (deltas, has_more) = log2.fetch_deltas(SequenceNumber::new(0), 100).unwrap();

    assert_eq!(deltas.len(), 10);
    assert!(!has_more);

    for (idx, op) in deltas.iter().enumerate() {
        let expected_seq = idx as u64 + 1;
        assert_eq!(op.seq.get(), expected_seq);
        assert_eq!(op.op.table_id, 1);
        assert_eq!(
            op.op.pk,
            PrimaryKey::single(Value::Int(expected_seq as i64))
        );
    }
}

#[test]
fn test_tiered_log_hot_buffer_fast_read() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }

    // Read range from cursor 2 with limit 2 (should return 3 and 4 with has_more = true)
    let (batch1, has_more1) = log.fetch_deltas(SequenceNumber::new(2), 2).unwrap();

    assert_eq!(batch1.len(), 2);
    assert_eq!(batch1[0].seq.get(), 3);
    assert_eq!(batch1[1].seq.get(), 4);
    assert!(has_more1);

    // Read remaining from cursor 4 with limit 10 (should return 5 with has_more = false)
    let (batch2, has_more2) = log.fetch_deltas(SequenceNumber::new(4), 10).unwrap();

    assert_eq!(batch2.len(), 1);
    assert_eq!(batch2[0].seq.get(), 5);
    assert!(!has_more2);

    // Read from current head (should return empty)
    let (batch3, has_more3) = log.fetch_deltas(SequenceNumber::new(5), 10).unwrap();

    assert!(batch3.is_empty());
    assert!(!has_more3);
}

#[test]
fn test_tiered_log_warm_segment_rotation() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..Default::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for i in 1..=6 {
        log.append(make_test_op(i), None).unwrap();
    }

    // Verify segments directory contains sealed segment and active.wal
    let segments_dir = dir.path().join("segments");
    let sealed_name = "segment_0000000000000001_0000000000000005.wal";
    assert!(segments_dir.join(sealed_name).exists());
    assert!(segments_dir.join("active.wal").exists());

    // Fetch across the rotation boundary (from 0 to 6)
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10).unwrap();

    assert_eq!(deltas.len(), 6);
    assert!(!has_more);
    for (i, op) in deltas.iter().enumerate() {
        assert_eq!(op.seq.get(), i as u64 + 1);
    }
}

#[tokio::test]
async fn test_tiered_log_cold_compression_and_read() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ram_ttl: Duration::from_secs(60),
        warm_disk_ttl: Duration::from_millis(10), // Fast warm compression
        cold_disk_ttl: Duration::from_secs(3600),
        max_room_disk_bytes: 50 * 1024 * 1024,
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }
    // Force rotate to seal segment 1..5 into .wal
    log.force_rotate_warm().unwrap();

    let sealed_wal = dir
        .path()
        .join("segments")
        .join("segment_0000000000000001_0000000000000005.wal");
    assert!(sealed_wal.exists());

    // Wait past warm_disk_ttl
    sleep(Duration::from_millis(20));

    // Run background maintenance
    let report = log.run_maintenance().await.unwrap();
    assert_eq!(report.warm_compressed_count, 1);

    // Sealed .wal should be removed, and .wal.zst created
    let cold_zst = dir
        .path()
        .join("segments")
        .join("segment_0000000000000001_0000000000000005.wal.zst");
    assert!(!sealed_wal.exists());
    assert!(cold_zst.exists());

    // Fetch deltas directly from Cold Disk (.wal.zst)
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10).unwrap();

    assert_eq!(deltas.len(), 5);
    assert!(!has_more);
    for (i, op) in deltas.iter().enumerate() {
        assert_eq!(op.seq.get(), i as u64 + 1);
    }
}

#[tokio::test]
async fn test_tiered_log_multi_tier_continuous_fetch() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 100,
        ram_ttl: Duration::from_secs(3600),
        warm_disk_ttl: Duration::from_millis(10),
        cold_disk_ttl: Duration::from_secs(3600),
        max_room_disk_bytes: 50 * 1024 * 1024,
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // 1. Operations 1..=5 -> Seal & compress to Cold
    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }
    log.force_rotate_warm().unwrap();
    sleep(Duration::from_millis(20));
    log.run_maintenance().await.unwrap();

    // 2. Operations 6..=10 -> Seal to Warm (without compressing to cold)
    for i in 6..=10 {
        log.append(make_test_op(i), None).unwrap();
    }
    log.force_rotate_warm().unwrap();

    // 3. Operations 11..=15 -> Active in RAM HotBuffer and active.wal
    for i in 11..=15 {
        log.append(make_test_op(i), None).unwrap();
    }

    assert_eq!(log.head_seq().get(), 15);

    // Fetch entire range 0..15 traversing Cold -> Warm -> Hot in a single contiguous batch
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 20).unwrap();

    assert_eq!(deltas.len(), 15);
    assert!(!has_more);

    for (i, op) in deltas.iter().enumerate() {
        assert_eq!(op.seq.get(), i as u64 + 1);
    }

    // Paged fetch crossing from Cold (ends at 5) into Warm (starts at 6)
    let (paged, paged_has_more) = log.fetch_deltas(SequenceNumber::new(3), 5).unwrap();

    assert_eq!(paged.len(), 5);
    assert_eq!(paged[0].seq.get(), 4);
    assert_eq!(paged[1].seq.get(), 5);
    assert_eq!(paged[2].seq.get(), 6);
    assert_eq!(paged[3].seq.get(), 7);
    assert_eq!(paged[4].seq.get(), 8);
    assert!(paged_has_more);
}

#[tokio::test]
async fn test_tiered_log_behind_compaction_eviction() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 100,
        ram_ttl: Duration::from_secs(3600),
        warm_disk_ttl: Duration::from_millis(10),
        cold_disk_ttl: Duration::from_millis(35), // Fast cold pruning
        max_room_disk_bytes: 50 * 1024 * 1024,
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // 1. Create Cold segment 1..=5
    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }
    log.force_rotate_warm().unwrap();
    sleep(Duration::from_millis(15));
    let r1 = log.run_maintenance().await.unwrap();
    assert_eq!(r1.warm_compressed_count, 1);
    assert_eq!(r1.cold_pruned_count, 0);

    // 2. Append 6..=10
    for i in 6..=10 {
        log.append(make_test_op(i), None).unwrap();
    }

    // 3. Wait past cold_disk_ttl and run maintenance to prune Cold segment 1..=5
    sleep(Duration::from_millis(40));
    let report = log.run_maintenance().await.unwrap();
    assert_eq!(report.cold_pruned_count, 1);
    assert_eq!(log.tail_seq().get(), 6);

    // Client requests cursor 2 (which is older than tail_seq 6) -> BehindCompaction!
    let err = log.fetch_deltas(SequenceNumber::new(2), 10).unwrap_err();
    match err {
        ServerError::BehindCompaction => {}
        other => panic!("Expected BehindCompaction, got: {:?}", other),
    }

    // Client requests cursor 5 (immediately before oldest available 6) -> Succeeds!
    let (valid_deltas, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10).unwrap();

    assert_eq!(valid_deltas.len(), 5);
    assert_eq!(valid_deltas[0].seq.get(), 6);
    assert_eq!(valid_deltas[4].seq.get(), 10);
    assert!(!has_more);
}

#[test]
fn test_tiered_log_strict_contiguity_and_monotonicity() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    log.append(make_test_op(1), None).unwrap();

    // Attempting to append sequence 3 directly (gap) must be rejected
    let err = log.append(make_test_op(3), None).unwrap_err();
    match err {
        ServerError::Wal(msg) => {
            assert!(msg.contains("Non-contiguous sequence"));
        }
        other => panic!("Expected ServerError::Wal, got: {:?}", other),
    }
}

#[test]
fn test_tiered_log_proactive_pruning_by_cursor() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // 1. Append 1..=5 and rotate to sealed Warm segment
    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }
    log.force_rotate_warm().unwrap();

    // 2. Append 6..=10 and rotate to sealed Warm segment
    for i in 6..=10 {
        log.append(make_test_op(i), None).unwrap();
    }
    log.force_rotate_warm().unwrap();

    // Verify both sealed segments exist on disk
    let seg1_path = dir
        .path()
        .join("segments")
        .join("segment_0000000000000001_0000000000000005.wal");
    let seg2_path = dir
        .path()
        .join("segments")
        .join("segment_0000000000000006_0000000000000010.wal");
    assert!(seg1_path.exists());
    assert!(seg2_path.exists());

    // 3. Proactively prune all deltas older than sequence 6 (all clients confirmed >= 6)
    let prune_report = log.prune_older_than(SequenceNumber::new(6)).unwrap();
    assert_eq!(prune_report.warm_deleted_count, 1);
    assert_eq!(prune_report.new_tail_seq.get(), 6);

    // Segment 1..5 was deleted from disk, segment 6..10 remains
    assert!(!seg1_path.exists());
    assert!(seg2_path.exists());

    // Cursor 2 is now expired -> BehindCompaction
    let err = log.fetch_deltas(SequenceNumber::new(2), 10).unwrap_err();
    match err {
        ServerError::BehindCompaction => {}
        other => panic!("Expected BehindCompaction, got: {:?}", other),
    }

    // Cursor 5 can fetch 6..=10 smoothly
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10).unwrap();
    assert_eq!(deltas.len(), 5);
    assert_eq!(deltas[0].seq.get(), 6);
    assert_eq!(deltas[4].seq.get(), 10);
    assert!(!has_more);
}

#[test]
fn test_tiered_log_hot_buffer_sliding_window_no_zero_eviction() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..Default::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // 1. Append operations 1..=5 (filling the RAM buffer and reaching the active segment limit)
    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }

    // 2. Append operation 6. This triggers disk rotation of the active WAL segment into segment_1_5.wal.
    // The sliding window must pop the oldest element (1) and retain operations 2..=6 in RAM.
    log.append(make_test_op(6), None).unwrap();

    // Verify that operations 2..=6 can still be fetched directly from the RAM buffer window without zero-eviction
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(1), 10).unwrap();

    assert_eq!(deltas.len(), 5);
    assert_eq!(deltas[0].seq.get(), 2);
    assert_eq!(deltas[4].seq.get(), 6);
    assert!(!has_more);

    // Fetching from cursor 4 returns 5 and 6
    let (recent_deltas, recent_has_more) = log.fetch_deltas(SequenceNumber::new(4), 10).unwrap();
    assert_eq!(recent_deltas.len(), 2);
    assert_eq!(recent_deltas[0].seq.get(), 5);
    assert_eq!(recent_deltas[1].seq.get(), 6);
    assert!(!recent_has_more);
}

#[test]
fn test_tiered_log_hot_buffer_o1_range_query() {
    let mut buffer = rimdb_server::log::HotBuffer::new();

    // Appending contiguous operations 10..=20
    for i in 10..=20 {
        buffer.append(make_test_op(i)).unwrap();
    }

    assert_eq!(buffer.min_seq().unwrap().get(), 10);
    assert_eq!(buffer.max_seq().unwrap().get(), 20);

    // Query 1: from_seq older than min_seq
    let res = buffer.get_range(SequenceNumber::new(5), 5);
    assert_eq!(res.len(), 5);
    assert_eq!(res[0].seq.get(), 10);
    assert_eq!(res[4].seq.get(), 14);

    // Query 2: from_seq exactly at min_seq
    let res = buffer.get_range(SequenceNumber::new(10), 3);
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].seq.get(), 11);
    assert_eq!(res[2].seq.get(), 13);

    // Query 3: from_seq in the middle
    let res = buffer.get_range(SequenceNumber::new(15), 10);
    assert_eq!(res.len(), 5);
    assert_eq!(res[0].seq.get(), 16);
    assert_eq!(res[4].seq.get(), 20);

    // Query 4: from_seq equal to max_seq
    let res = buffer.get_range(SequenceNumber::new(20), 5);
    assert!(res.is_empty());

    // Query 5: from_seq greater than max_seq
    let res = buffer.get_range(SequenceNumber::new(25), 5);
    assert!(res.is_empty());

    // Query 6: limit 0
    let res = buffer.get_range(SequenceNumber::new(10), 0);
    assert!(res.is_empty());
}

#[test]
fn test_tiered_log_hierarchical_cache_inversion() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 10,
        ..Default::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Append 10 operations
    for i in 1..=10 {
        log.append(make_test_op(i), None).unwrap();
    }

    // A query for deltas starting within the hot buffer (e.g. from 5) should resolve directly
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10).unwrap();
    assert_eq!(deltas.len(), 5);
    assert_eq!(deltas[0].seq.get(), 6);
    assert_eq!(deltas[4].seq.get(), 10);
    assert!(!has_more);
}

#[tokio::test]
async fn test_tiered_log_async_maintenance_spawn_blocking() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ram_ttl: Duration::from_secs(60),
        warm_disk_ttl: Duration::from_millis(5),
        cold_disk_ttl: Duration::from_secs(3600),
        max_room_disk_bytes: 50 * 1024 * 1024,
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for i in 1..=5 {
        log.append(make_test_op(i), None).unwrap();
    }
    log.force_rotate_warm().unwrap();

    // Wait past warm_disk_ttl
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Run async maintenance which delegates Zstd compression to spawn_blocking
    let report = log.run_maintenance().await.unwrap();
    assert_eq!(report.warm_compressed_count, 1);

    // Verify Cold Disk file was created and is readable
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10).unwrap();
    assert_eq!(deltas.len(), 5);
    assert_eq!(deltas[0].seq.get(), 1);
    assert_eq!(deltas[4].seq.get(), 5);
    assert!(!has_more);
}
