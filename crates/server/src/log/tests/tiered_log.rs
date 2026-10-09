use super::*;
use crate::log::io_probe::{self, IoEvent};
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::mutation::Operation;
use zemdb_core::value::{PrimaryKey, Value};

fn make_op(seq: u64) -> SequencedOperation {
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    SequencedOperation::new(
        SequenceNumber::new(seq),
        Operation::delete(1, pk, seq * 1000),
    )
}

/// Policy where RAM entries expire quickly but the active segment never rotates by count,
/// so evicted operations remain only in `active.wal`.
fn ttl_eviction_policy() -> RoomLifecyclePolicy {
    RoomLifecyclePolicy {
        ram_max_ops: 1_000,
        ram_ttl: Duration::from_millis(30),
        ..RoomLifecyclePolicy::default()
    }
}

fn assert_contiguous(ops: &[SequencedOperation], first: u64, last: u64) {
    let seqs: Vec<u64> = ops.iter().map(|op| op.seq.get()).collect();
    let expected: Vec<u64> = (first..=last).collect();
    assert_eq!(seqs, expected);
}

#[test]
fn fresh_log_tail_is_next_sequence() {
    let dir = tempdir().unwrap();
    let (log, _) = TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();

    assert_eq!(log.head_seq().get(), 0);
    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn tail_stays_at_first_op_after_appends() {
    let dir = tempdir().unwrap();
    let (mut log, _) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();

    for seq in 1..=3 {
        log.append(make_op(seq), None).unwrap();
    }

    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn tail_includes_active_wal_after_ram_ttl_eviction() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    for seq in 1..=10 {
        log.append(make_op(seq), None).unwrap();
    }
    sleep(Duration::from_millis(60));
    // The sliding window runs on append and evicts everything older than the RAM TTL.
    log.append(make_op(11), None).unwrap();
    assert_eq!(log.hot_buffer.min_seq().map(|s| s.get()), Some(11));

    log.run_maintenance_sync().unwrap();

    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn fetch_after_ram_ttl_eviction_bridges_from_active_wal() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    for seq in 1..=10 {
        log.append(make_op(seq), None).unwrap();
    }
    sleep(Duration::from_millis(60));
    log.append(make_op(11), None).unwrap();
    log.run_maintenance_sync().unwrap();

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();

    assert_contiguous(&ops, 1, 11);
    assert!(!has_more);
}

#[test]
fn fully_pruned_log_rejects_cursor_behind_head() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::ZERO,
        cold_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Five appends rotate the active segment, leaving no `active.wal`.
    for seq in 1..=5 {
        log.append(make_op(seq), None).unwrap();
    }
    // With zero TTLs, one maintenance pass compresses the sealed segment and prunes it.
    log.run_maintenance_sync().unwrap();

    assert_eq!(log.head_seq().get(), 5);
    assert_eq!(log.tail_seq().get(), 6);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(4), 100),
        Err(ServerError::BehindCompaction)
    ));
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(5), 100).unwrap();
    assert!(ops.is_empty());
    assert!(!has_more);
}

#[test]
fn append_after_full_prune_keeps_tail_on_new_op() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::ZERO,
        cold_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for seq in 1..=5 {
        log.append(make_op(seq), None).unwrap();
    }
    log.run_maintenance_sync().unwrap();
    log.append(make_op(6), None).unwrap();

    assert_eq!(log.tail_seq().get(), 6);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100).unwrap();
    assert_contiguous(&ops, 6, 6);
}

#[test]
fn prune_older_than_keeps_tail_on_active_wal() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ram_ttl: Duration::from_millis(30),
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Ops 1..=5 are sealed; ops 6..=8 stay in `active.wal`.
    for seq in 1..=8 {
        log.append(make_op(seq), None).unwrap();
    }
    sleep(Duration::from_millis(60));
    log.append(make_op(9), None).unwrap();

    let report = log.prune_older_than(SequenceNumber::new(6)).unwrap();

    assert_eq!(report.warm_deleted_count, 1);
    assert_eq!(log.tail_seq().get(), 6);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100).unwrap();
    assert_contiguous(&ops, 6, 9);
}

#[test]
fn reopen_after_ram_ttl_eviction_restores_tail_from_disk() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();
        for seq in 1..=4 {
            log.append(make_op(seq), None).unwrap();
        }
    }

    let (log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    assert_eq!(log.head_seq().get(), 4);
    assert_eq!(log.tail_seq().get(), 1);
}

fn full_prune_policy() -> RoomLifecyclePolicy {
    RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::ZERO,
        cold_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    }
}

#[test]
fn reopen_after_full_prune_preserves_head() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();
        for seq in 1..=5 {
            log.append(make_op(seq), None).unwrap();
        }
        log.run_maintenance_sync().unwrap();
        assert_eq!(log.tail_seq().get(), 6);
    }

    let (log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();

    assert_eq!(log.head_seq().get(), 5);
    assert_eq!(log.tail_seq().get(), 6);
}

#[test]
fn reopen_after_full_prune_continues_sequence() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();
        for seq in 1..=5 {
            log.append(make_op(seq), None).unwrap();
        }
        log.run_maintenance_sync().unwrap();
    }

    let (mut log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();

    // Reusing an already delivered sequence number must be rejected.
    assert!(log.append(make_op(1), None).is_err());
    log.append(make_op(6), None).unwrap();
    assert_eq!(log.head_seq().get(), 6);
}

#[test]
fn reopen_with_corrupt_log_meta_fails_instead_of_resetting_head() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();
        for seq in 1..=5 {
            log.append(make_op(seq), None).unwrap();
        }
        log.run_maintenance_sync().unwrap();
    }
    std::fs::write(dir.path().join("log_meta.json"), b"{ not json").unwrap();

    assert!(TieredLog::open_or_create(dir.path(), full_prune_policy()).is_err());
}

#[test]
fn gap_inside_retained_range_reports_behind_compaction() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Sealed segments [1..=5] and [6..=10]; operations 11..=12 stay in `active.wal`.
    for seq in 1..=12 {
        log.append(make_op(seq), None).unwrap();
    }
    let middle = dir
        .path()
        .join("segments")
        .join(format!("segment_{:016}_{:016}.wal", 6, 10));
    std::fs::remove_file(middle).unwrap();

    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(0), 100),
        Err(ServerError::BehindCompaction)
    ));
}

#[test]
fn open_makes_new_segments_directory_durable() {
    let dir = tempdir().unwrap();
    crate::fail_point::arm("sync_dir", dir.path());

    assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());
}

fn segments_dir(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("segments")
}

fn mutation_for(seq: u64) -> MutationId {
    MutationId::from_u128(u128::from(seq))
}

/// Appends `first..=last`, each operation with the mutation id `mutation_for(seq)`.
fn append_range(log: &mut TieredLog, first: u64, last: u64) {
    for seq in first..=last {
        log.append(make_op(seq), Some(mutation_for(seq))).unwrap();
    }
}

/// Five operations per segment; sealed segments are compressed on the next maintenance pass
/// and kept for the whole test.
fn compress_at_once_policy() -> RoomLifecyclePolicy {
    RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    }
}

#[test]
fn open_lists_the_segments_directory_once() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 1, 10);
        log.run_maintenance_sync().unwrap();
        append_range(&mut log, 11, 17);
    }
    let segments = segments_dir(dir.path());
    let before = io_probe::count(IoEvent::Listing, &segments);

    TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();

    assert_eq!(io_probe::count(IoEvent::Listing, &segments) - before, 1);
}

#[test]
fn maintenance_fetch_and_pruning_never_list_the_segments_directory() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    let segments = segments_dir(dir.path());
    let after_open = io_probe::count(IoEvent::Listing, &segments);

    // Cold segments 1..=5 and 6..=10, sealed warm segment 11..=15, ops 16..=17 in active.wal.
    append_range(&mut log, 1, 10);
    log.run_maintenance_sync().unwrap();
    append_range(&mut log, 11, 17);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 17);
    log.prune_older_than(SequenceNumber::new(6)).unwrap();
    log.run_maintenance_sync().unwrap();

    assert_eq!(io_probe::count(IoEvent::Listing, &segments), after_open);
    assert_eq!(log.tail_seq().get(), 6);
}

#[test]
fn reopen_reads_only_the_newest_segments_needed_for_ram_and_dedup() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        // Ten cold segments of five operations each, then 51..=53 in active.wal.
        append_range(&mut log, 1, 50);
        log.run_maintenance_sync().unwrap();
        append_range(&mut log, 51, 53);
    }
    let segments = segments_dir(dir.path());
    let before = io_probe::count(IoEvent::SegmentRead, &segments);

    // The last 7 mutation ids span active.wal and the newest cold segment (46..=50).
    let (log, mutations) =
        TieredLog::open_or_create_with_dedup_window(dir.path(), compress_at_once_policy(), 7)
            .unwrap();

    assert_eq!(io_probe::count(IoEvent::SegmentRead, &segments) - before, 1);
    let recovered: Vec<(MutationId, u64)> =
        mutations.into_iter().map(|(m, s)| (m, s.get())).collect();
    let expected: Vec<(MutationId, u64)> = (47..=53).map(|s| (mutation_for(s), s)).collect();
    assert_eq!(recovered, expected, "the newest mutations, oldest first");
    assert_eq!(log.head_seq().get(), 53);
    assert_eq!(log.tail_seq().get(), 1);
    assert_eq!(log.hot_buffer.min_seq().map(|s| s.get()), Some(49));
    assert_eq!(log.hot_buffer.max_seq().map(|s| s.get()), Some(53));
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 53);
}

#[test]
fn fetch_reads_active_wal_through_the_handle_holding_its_lock() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();
    for seq in 1..=10 {
        log.append(make_op(seq), None).unwrap();
    }
    sleep(Duration::from_millis(60));
    // Evicts 1..=10 from RAM: they are only in active.wal now.
    log.append(make_op(11), None).unwrap();

    // Any reader that opens `active.wal` by path (a second handle, which Windows refuses
    // while the lock is held) no longer finds it; the log's own handle still reads it.
    let segments = segments_dir(dir.path());
    std::fs::rename(segments.join("active.wal"), segments.join("moved.wal")).unwrap();

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 11);
    assert!(!has_more);
}

#[test]
fn reopen_reads_active_wal_only_through_the_handle_holding_its_lock() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();
        append_range(&mut log, 1, 4);
    }
    let segments = segments_dir(dir.path());

    let (log, mutations) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    assert_eq!(io_probe::count(IoEvent::ActiveWalPathRead, &segments), 0);
    assert_eq!(mutations.len(), 4);
    assert_eq!(log.head_seq().get(), 4);
}

#[test]
fn open_removes_leftover_temporary_cold_segments() {
    let dir = tempdir().unwrap();
    let segments = segments_dir(dir.path());
    std::fs::create_dir_all(&segments).unwrap();
    // Compressions interrupted before their rename, with the old and the current naming.
    let leftovers = [
        segments.join("segment_0000000000000001_0000000000000005.wal.tmp"),
        segments.join("segment_0000000000000006_0000000000000010.wal.zst.tmp"),
    ];
    for leftover in &leftovers {
        std::fs::write(leftover, b"partial").unwrap();
    }

    TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();

    for leftover in &leftovers {
        assert!(!leftover.exists(), "{leftover:?} was not removed");
    }
}

#[test]
fn maintenance_applies_the_ram_ttl_window_without_appends() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_ttl: Duration::from_secs(60),
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_range(&mut log, 1, 5);

    log.run_maintenance_sync_at(MaintenanceClock::after(Duration::from_secs(30)))
        .unwrap();
    assert_eq!(log.hot_buffer.len(), 5);

    log.run_maintenance_sync_at(MaintenanceClock::after(Duration::from_secs(61)))
        .unwrap();
    assert!(log.hot_buffer.is_empty());
    assert_eq!(log.tail_seq().get(), 1);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 5);
}

#[test]
fn cold_segment_is_pruned_once_its_ttl_has_passed() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::from_secs(60),
        cold_disk_ttl: Duration::from_secs(3600),
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    // Segment 1..=5 is sealed; 6..=8 stay in active.wal.
    append_range(&mut log, 1, 8);

    let report = log
        .run_maintenance_sync_at(MaintenanceClock::after(Duration::from_secs(59)))
        .unwrap();
    assert_eq!(report.warm_compressed_count, 0);

    // Compressed one minute after sealing; the cold TTL counts from the compression.
    let compressed_at = Duration::from_secs(61);
    let report = log
        .run_maintenance_sync_at(MaintenanceClock::after(compressed_at))
        .unwrap();
    assert_eq!(report.warm_compressed_count, 1);
    assert_eq!(report.cold_pruned_count, 0);

    let just_before = compressed_at + Duration::from_secs(3599);
    let report = log
        .run_maintenance_sync_at(MaintenanceClock::after(just_before))
        .unwrap();
    assert_eq!(report.cold_pruned_count, 0);
    assert_eq!(log.tail_seq().get(), 1);

    let past_ttl = compressed_at + Duration::from_secs(3601);
    let report = log
        .run_maintenance_sync_at(MaintenanceClock::after(past_ttl))
        .unwrap();
    assert_eq!(report.cold_pruned_count, 1);
    assert_eq!(log.tail_seq().get(), 6);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(2), 10),
        Err(ServerError::BehindCompaction)
    ));
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10).unwrap();
    assert_contiguous(&ops, 6, 8);
    assert!(!has_more);
}

#[test]
fn disk_bytes_track_the_files_of_the_log() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    let segments = segments_dir(dir.path());
    let on_disk = || -> u64 {
        std::fs::read_dir(&segments)
            .unwrap()
            // Through each file: on Windows a listing can report a stale size for the open
            // `active.wal`.
            .map(|entry| std::fs::metadata(entry.unwrap().path()).unwrap().len())
            .sum()
    };

    append_range(&mut log, 1, 12);
    assert_eq!(log.disk_bytes(), on_disk());
    log.run_maintenance_sync().unwrap();
    assert_eq!(log.disk_bytes(), on_disk());
    log.prune_older_than(SequenceNumber::new(6)).unwrap();
    assert_eq!(log.disk_bytes(), on_disk());
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    assert_eq!(log.disk_bytes(), on_disk());
}

#[test]
fn quota_prunes_the_oldest_cold_segments_until_the_log_fits() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 15);
    log.run_maintenance_sync().unwrap();
    let total = log.disk_bytes();
    let oldest_cold = log.segments.iter().next().unwrap().bytes;

    // One byte under the current size: dropping the oldest cold segment is enough.
    log.policy.max_room_disk_bytes = total - 1;
    let report = log.run_maintenance_sync().unwrap();

    assert_eq!(report.cold_pruned_count, 1);
    assert_eq!(log.tail_seq().get(), 6);
    assert_eq!(log.disk_bytes(), total - oldest_cold);
}

#[test]
fn open_keeps_the_warm_original_of_an_interrupted_compression() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 1, 5);
    }
    // A crash after the cold segment was renamed into place, before the warm one was removed.
    let segments = segments_dir(dir.path());
    let warm = segments.join("segment_0000000000000001_0000000000000005.wal");
    let cold = segments.join("segment_0000000000000001_0000000000000005.wal.zst");
    std::fs::write(
        &cold,
        zstd::stream::encode_all(&std::fs::read(&warm).unwrap()[..], 3).unwrap(),
    )
    .unwrap();

    let (mut log, mutations) =
        TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();

    assert!(warm.exists());
    assert!(!cold.exists());
    assert_eq!(mutations.len(), 5);
    assert_eq!(log.run_maintenance_sync().unwrap().warm_compressed_count, 1);
    assert!(cold.exists());
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 5);
}

#[test]
fn failed_cleanup_after_compression_keeps_the_index_on_the_cold_segment() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 7);
    let segments = segments_dir(dir.path());
    let warm = segments.join("segment_0000000000000001_0000000000000005.wal");
    let cold = segments.join("segment_0000000000000001_0000000000000005.wal.zst");

    // The cold segment is durably in place and the warm one deleted, but the directory sync
    // that makes the deletion durable fails.
    crate::fail_point::arm("compress_after_warm_removed", &warm);
    let _ = log.run_maintenance_sync();

    assert!(cold.exists() && !warm.exists());
    let report = log.run_maintenance_sync().unwrap();
    assert_eq!(report.warm_compressed_count, 0);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 7);
    assert_eq!(log.disk_bytes(), {
        std::fs::read_dir(&segments)
            .unwrap()
            .map(|entry| std::fs::metadata(entry.unwrap().path()).unwrap().len())
            .sum::<u64>()
    });
}

#[test]
fn failed_compression_does_not_skip_expiry() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ram_ttl: Duration::from_secs(60),
        warm_disk_ttl: Duration::ZERO,
        cold_disk_ttl: Duration::from_secs(3600),
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_range(&mut log, 1, 5);
    log.run_maintenance_sync().unwrap();
    append_range(&mut log, 6, 12);

    // Compressing segment 6..=10 fails, while segment 1..=5 is past its cold TTL and every
    // operation in RAM is past the RAM TTL.
    crate::fail_point::arm("sync_dir", &segments_dir(dir.path()));
    let result = log.run_maintenance_sync_at(MaintenanceClock::after(Duration::from_secs(7200)));

    assert!(result.is_err());
    assert_eq!(log.tail_seq().get(), 6);
    assert!(log.hot_buffer.is_empty());
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100).unwrap();
    assert_contiguous(&ops, 6, 12);
}

/// Files a second opener must not touch: a leftover temporary cold segment, a cold copy of
/// a warm segment and a leftover temporary `log_meta.json`.
fn plant_leftovers(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let segments = segments_dir(dir);
    let planted = vec![
        segments.join("segment_0000000000000001_0000000000000005.wal.zst"),
        segments.join("segment_0000000000000099_0000000000000100.wal.zst.tmp"),
        dir.join("log_meta.json.tmp"),
    ];
    for path in &planted {
        std::fs::write(path, b"planted").unwrap();
    }
    planted
}

#[test]
fn second_open_of_a_live_log_fails_without_touching_its_files() {
    let dir = tempdir().unwrap();
    let (mut first, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    // Sealed segment 1..=5 and ops 6..=7 in active.wal.
    append_range(&mut first, 1, 7);
    let planted = plant_leftovers(dir.path());

    let second = TieredLog::open_or_create(dir.path(), compress_at_once_policy());

    assert!(
        matches!(second, Err(ServerError::RoomLocked(_))),
        "{second:?}"
    );
    for path in &planted {
        assert!(path.exists(), "{path:?} was removed by the second open");
    }
    drop(first);
}

#[test]
fn second_open_fails_even_between_a_rotation_and_the_next_append() {
    let dir = tempdir().unwrap();
    let (mut first, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    // The fifth operation seals the segment: no active.wal, so no lock on it.
    append_range(&mut first, 1, 5);
    assert!(!segments_dir(dir.path()).join("active.wal").exists());

    let second = TieredLog::open_or_create(dir.path(), compress_at_once_policy());

    assert!(
        matches!(second, Err(ServerError::RoomLocked(_))),
        "{second:?}"
    );
    drop(first);
    assert!(TieredLog::open_or_create(dir.path(), compress_at_once_policy()).is_ok());
}

#[test]
fn dedup_window_counts_distinct_mutation_ids() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ..RoomLifecyclePolicy::default()
    };
    let (a, b, c) = (mutation_for(100), mutation_for(200), mutation_for(300));
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        // A mutation id appended again once it left the deduplication window.
        for (seq, id) in [(1, a), (2, b), (3, c), (4, c), (5, c), (6, c)] {
            log.append(make_op(seq), Some(id)).unwrap();
        }
    }

    let (_, mutations) =
        TieredLog::open_or_create_with_dedup_window(dir.path(), policy, 3).unwrap();

    let recovered: Vec<(MutationId, u64)> =
        mutations.into_iter().map(|(m, s)| (m, s.get())).collect();
    assert_eq!(recovered, vec![(a, 1), (b, 2), (c, 6)]);
}

/// Rewrites the warm segment `start..=end` of the log in `dir` so that it only holds
/// `start..=last`, as if its tail had been lost.
fn shorten_warm_segment(dir: &std::path::Path, start: u64, end: u64, last: u64) {
    let path = segments_dir(dir).join(format!("segment_{start:016}_{end:016}.wal"));
    let mut bytes = Vec::new();
    for seq in start..=last {
        bytes.extend(
            zemdb_core::protocol::wal_frame::encode_wal_record(
                &make_op(seq),
                Some(mutation_for(seq)),
            )
            .unwrap(),
        );
    }
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn segment_shorter_than_its_name_leaves_ram_empty_and_commits_continue() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 1, 10);
    }
    shorten_warm_segment(dir.path(), 6, 10, 8);

    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();

    // The name says the log reached 10: that sequence was delivered and is never reused.
    assert_eq!(log.head_seq().get(), 10);
    assert!(log.hot_buffer.is_empty());
    log.append(make_op(11), None).unwrap();
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(10), 100).unwrap();
    assert_contiguous(&ops, 11, 11);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(8), 100),
        Err(ServerError::BehindCompaction)
    ));
}

#[test]
fn missing_segment_at_the_end_of_the_disk_tiers_is_behind_compaction() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 10);
    // Nothing in RAM or active.wal covers the range any more.
    log.run_maintenance_sync_at(MaintenanceClock::after(Duration::from_secs(600)))
        .unwrap();
    assert!(log.hot_buffer.is_empty());
    std::fs::remove_file(
        segments_dir(dir.path()).join("segment_0000000000000006_0000000000000010.wal.zst"),
    )
    .unwrap();

    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(5), 100),
        Err(ServerError::BehindCompaction)
    ));
}

/// Damages the first record of `path` in the middle of its payload: a CRC mismatch that is
/// not at the end of the file, which decodes as corruption rather than a torn write.
fn corrupt_first_record(path: &std::path::Path) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[20] ^= 0xFF;
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn corrupt_segment_inside_the_retained_range_is_behind_compaction() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_range(&mut log, 1, 12);
    corrupt_first_record(
        &segments_dir(dir.path()).join("segment_0000000000000001_0000000000000005.wal"),
    );

    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(0), 100),
        Err(ServerError::BehindCompaction)
    ));
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100).unwrap();
    assert_contiguous(&ops, 6, 12);
}

#[test]
fn open_tolerates_a_corrupt_newest_segment() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        append_range(&mut log, 1, 12);
    }
    corrupt_first_record(
        &segments_dir(dir.path()).join("segment_0000000000000006_0000000000000010.wal"),
    );

    let (mut log, mutations) =
        TieredLog::open_or_create_with_dedup_window(dir.path(), policy, 100).unwrap();

    assert_eq!(log.head_seq().get(), 12);
    // Only what is newer than the damaged segment is hydrated.
    let recovered: Vec<u64> = mutations.iter().map(|(_, s)| s.get()).collect();
    assert_eq!(recovered, vec![11, 12]);
    assert_eq!(log.hot_buffer.min_seq().map(|s| s.get()), Some(11));
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(5), 100),
        Err(ServerError::BehindCompaction)
    ));
    log.append(make_op(13), None).unwrap();
}
