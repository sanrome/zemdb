use super::*;
use crate::dedup::DedupLruCache;
use crate::log::io_probe::{self, IoEvent};
use crate::log::policy::{MAX_LOG_BYTES_LIMIT, MIN_LOG_BYTES_LIMIT};
use crate::log::test_support::{frames, make_op, segment_file, seqs};
use crate::test_support::TestRng;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::mutation::Operation;
use zemdb_core::value::{PrimaryKey, Value};

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
    let expected: Vec<u64> = (first..=last).collect();
    assert_eq!(seqs(ops), expected);
}

fn segments_dir(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("segments")
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// Bytes of the segments and `active.wal` in `segments` (not the mutation ID sidecars, which the
/// quota does not count), read through each file: on Windows a listing can report a stale size
/// for the open `active.wal`.
fn bytes_on_disk(segments: &Path) -> u64 {
    std::fs::read_dir(segments)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_none_or(|ext| ext != "ids"))
        .map(|path| file_len(&path))
        .sum()
}

/// Appends `first..=last` without mutation ids.
fn append_plain(log: &mut TieredLog, first: u64, last: u64) {
    for seq in first..=last {
        log.append(make_op(seq), None).unwrap();
    }
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

/// Writes operations `1..=last` with the default policy, as `append_range` does, closes the
/// log and returns the path of its `active.wal`.
fn write_log(dir: &Path, last: u64) -> std::path::PathBuf {
    let (mut log, _) = TieredLog::open_or_create(dir, RoomLifecyclePolicy::default()).unwrap();
    append_range(&mut log, 1, last);
    segments_dir(dir).join("active.wal")
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

    append_plain(&mut log, 1, 3);

    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn tail_includes_active_wal_after_ram_ttl_eviction() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    append_plain(&mut log, 1, 10);
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

    append_plain(&mut log, 1, 10);
    sleep(Duration::from_millis(60));
    log.append(make_op(11), None).unwrap();
    log.run_maintenance_sync().unwrap();

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();

    assert_contiguous(&ops, 1, 11);
    assert!(!has_more);
}

fn full_prune_policy() -> RoomLifecyclePolicy {
    RoomLifecyclePolicy {
        ram_max_ops: 5,
        warm_disk_ttl: Duration::ZERO,
        cold_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    }
}

/// A log that held operations `1..=5` and pruned them all: five appends rotate the active
/// segment, leaving no `active.wal`, and with zero TTLs one maintenance pass compresses the
/// sealed segment and prunes it.
fn fully_pruned_log(dir: &Path) -> TieredLog {
    let (mut log, _) = TieredLog::open_or_create(dir, full_prune_policy()).unwrap();
    append_plain(&mut log, 1, 5);
    log.run_maintenance_sync().unwrap();
    log
}

#[test]
fn fully_pruned_log_rejects_cursor_behind_head() {
    let dir = tempdir().unwrap();
    let log = fully_pruned_log(dir.path());

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
    let mut log = fully_pruned_log(dir.path());

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
    append_plain(&mut log, 1, 8);
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
        append_plain(&mut log, 1, 4);
    }

    let (log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    assert_eq!(log.head_seq().get(), 4);
    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn reopen_after_full_prune_preserves_head() {
    let dir = tempdir().unwrap();
    let log = fully_pruned_log(dir.path());
    assert_eq!(log.tail_seq().get(), 6);
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();

    assert_eq!(log.head_seq().get(), 5);
    assert_eq!(log.tail_seq().get(), 6);
}

#[test]
fn reopen_after_full_prune_continues_sequence() {
    let dir = tempdir().unwrap();
    drop(fully_pruned_log(dir.path()));

    let (mut log, _) = TieredLog::open_or_create(dir.path(), full_prune_policy()).unwrap();

    // Reusing an already delivered sequence number must be rejected.
    assert!(log.append(make_op(1), None).is_err());
    log.append(make_op(6), None).unwrap();
    assert_eq!(log.head_seq().get(), 6);
}

#[test]
fn failure_to_check_for_active_wal_fails_the_open() {
    let dir = tempdir().unwrap();
    let active = write_log(dir.path(), 3);

    crate::fail_point::arm("exists", &active);
    assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());

    let (log, _) = TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    assert_eq!(log.head_seq().get(), 3);
}

#[test]
fn reopen_with_corrupt_log_meta_fails_instead_of_resetting_head() {
    let dir = tempdir().unwrap();
    drop(fully_pruned_log(dir.path()));
    std::fs::write(dir.path().join("log_meta.json"), b"{ not json").unwrap();

    assert!(TieredLog::open_or_create(dir.path(), full_prune_policy()).is_err());
}

#[test]
fn log_claiming_the_last_sequence_number_fails_to_open() {
    let dir = tempdir().unwrap();
    let (mut log, _) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    append_range(&mut log, 1, 3);
    let segment = log.force_rotate_warm().unwrap().unwrap();
    drop(log);
    let meta = dir.path().join("log_meta.json");
    std::fs::write(&meta, format!(r#"{{"pruned_through_seq": {}}}"#, u64::MAX)).unwrap();
    assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());
    assert!(segment.exists(), "the failed open deleted a segment");

    std::fs::write(
        &meta,
        format!(r#"{{"pruned_through_seq": {}}}"#, u64::MAX - 1),
    )
    .unwrap();
    let (log, _) = TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    assert_eq!(log.tail_seq().get(), u64::MAX);
    drop(log);

    // A segment name can claim it too.
    std::fs::write(
        segment_file(&segments_dir(dir.path()), u64::MAX, u64::MAX, "wal"),
        b"",
    )
    .unwrap();
    assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());
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
    append_plain(&mut log, 1, 12);
    std::fs::remove_file(segment_file(&segments_dir(dir.path()), 6, 10, "wal")).unwrap();

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

/// Ten cold segments of five operations each, then 51..=53 in active.wal.
fn fifty_three_compressed_ops(dir: &Path) {
    let (mut log, _) = TieredLog::open_or_create(dir, compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 50);
    log.run_maintenance_sync().unwrap();
    append_range(&mut log, 51, 53);
}

/// Reopens the log of [`fifty_three_compressed_ops`] in `dir` with a deduplication window of
/// `window` mutation IDs and checks what it recovered. Returns how many sealed segments it
/// read.
fn reopen_with_window(dir: &Path, window: usize) -> usize {
    let segments = segments_dir(dir);
    let before = io_probe::count(IoEvent::SegmentRead, &segments);
    let (log, mutations) =
        TieredLog::open_or_create_with_dedup_window(dir, compress_at_once_policy(), window)
            .unwrap();
    let reads = io_probe::count(IoEvent::SegmentRead, &segments) - before;

    let recovered: Vec<(MutationId, u64)> =
        mutations.into_iter().map(|(m, s)| (m, s.get())).collect();
    let first = 54 - window as u64;
    let expected: Vec<(MutationId, u64)> = (first..=53).map(|s| (mutation_for(s), s)).collect();
    assert_eq!(recovered, expected, "the newest mutations, oldest first");
    assert_eq!(log.head_seq().get(), 53);
    assert_eq!(log.tail_seq().get(), 1);
    // RAM holds only active.wal.
    assert_eq!(log.hot_buffer.min_seq().map(|s| s.get()), Some(51));
    assert_eq!(log.hot_buffer.max_seq().map(|s| s.get()), Some(53));
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 53);
    reads
}

#[test]
fn reopen_reads_the_mutation_ids_of_sealed_segments_without_decoding_them() {
    let dir = tempdir().unwrap();
    // The sidecars were written when the segments were sealed and kept by the compression.
    fifty_three_compressed_ops(dir.path());
    // The window spans active.wal and the newest segment (46..=50).
    assert_eq!(reopen_with_window(dir.path(), 7), 0);
}

#[test]
fn reopen_decodes_a_segment_whose_mutation_ids_are_missing_or_invalid() {
    let dir = tempdir().unwrap();
    fifty_three_compressed_ops(dir.path());
    let segments = segments_dir(dir.path());
    let ids = |first, last| segment_file(&segments, first, last, "ids");
    // Another segment's sidecar, with a valid CRC; four zero bytes; and none at all.
    std::fs::copy(ids(41, 45), ids(46, 50)).unwrap();
    std::fs::write(ids(41, 45), [0u8; 4]).unwrap();
    std::fs::remove_file(ids(36, 40)).unwrap();
    // The window spans active.wal and the segments 46..=50, 41..=45 and 36..=40.
    assert_eq!(reopen_with_window(dir.path(), 18), 3);
}

#[test]
fn ids_written_before_a_reopen_reach_the_sidecar() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 1, 3);
    }
    {
        // Seals 1..=5, three of which were written before this open.
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 4, 5);
    }
    let segments = segments_dir(dir.path());
    let before = io_probe::count(IoEvent::SegmentRead, &segments);
    let (_, mutations) =
        TieredLog::open_or_create_with_dedup_window(dir.path(), compress_at_once_policy(), 5)
            .unwrap();
    assert_eq!(io_probe::count(IoEvent::SegmentRead, &segments), before);
    assert_eq!(mutations.len(), 5);
}

#[test]
fn pruning_a_segment_deletes_its_mutation_ids() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 10);
    let ids = segment_file(&segments_dir(dir.path()), 1, 5, "ids");
    assert!(ids.exists());
    log.prune_older_than(SequenceNumber::new(6)).unwrap();
    assert!(!ids.exists());
}

#[test]
fn fetch_reads_active_wal_through_the_handle_holding_its_lock() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();
    append_plain(&mut log, 1, 10);
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
        segment_file(&segments, 1, 5, "wal.tmp"),
        segment_file(&segments, 6, 10, "wal.zst.tmp"),
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

    append_range(&mut log, 1, 12);
    assert_eq!(log.disk_bytes(), bytes_on_disk(&segments));
    log.run_maintenance_sync().unwrap();
    assert_eq!(log.disk_bytes(), bytes_on_disk(&segments));
    log.prune_older_than(SequenceNumber::new(6)).unwrap();
    assert_eq!(log.disk_bytes(), bytes_on_disk(&segments));
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    assert_eq!(log.disk_bytes(), bytes_on_disk(&segments));
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
    let warm = segment_file(&segments, 1, 5, "wal");
    let cold = segment_file(&segments, 1, 5, "wal.zst");
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
    let warm = segment_file(&segments, 1, 5, "wal");
    let cold = segment_file(&segments, 1, 5, "wal.zst");

    // The cold segment is durably in place and the warm one deleted, but the directory sync
    // that makes the deletion durable fails.
    crate::fail_point::arm("compress_after_warm_removed", &warm);
    let _ = log.run_maintenance_sync();

    assert!(cold.exists() && !warm.exists());
    let report = log.run_maintenance_sync().unwrap();
    assert_eq!(report.warm_compressed_count, 0);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 7);
    assert_eq!(log.disk_bytes(), bytes_on_disk(&segments));
}

#[test]
fn warm_copy_that_could_not_be_removed_is_removed_later() {
    let policy = RoomLifecyclePolicy {
        cold_disk_ttl: Duration::from_secs(3600),
        ..compress_at_once_policy()
    };
    let expired = MaintenanceClock::after(Duration::from_secs(7200));

    // Without a restart, the next pass removes it.
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    append_range(&mut log, 1, 7);
    let warm = segment_file(&segments_dir(dir.path()), 1, 5, "wal");
    // Once after the compression and once in the retry of the same pass.
    crate::fail_point::arm("remove_file", &warm);
    crate::fail_point::arm("remove_file", &warm);
    log.run_maintenance_sync().unwrap();
    assert!(warm.exists());
    log.run_maintenance_sync().unwrap();
    assert!(!warm.exists());

    // If it is still there when its cold copy is pruned, reopening removes it instead of
    // indexing it again, which would move the tail back over a range the log lost.
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    append_range(&mut log, 1, 7);
    let warm = segment_file(&segments_dir(dir.path()), 1, 5, "wal");
    for _ in 0..3 {
        crate::fail_point::arm("remove_file", &warm);
    }
    log.run_maintenance_sync().unwrap();
    log.run_maintenance_sync_at(expired).unwrap();
    assert_eq!(log.tail_seq().get(), 6);
    assert!(warm.exists());
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log.tail_seq().get(), 6);
    assert!(!warm.exists());
}

#[test]
fn quota_pruning_stops_at_the_first_segment_it_keeps() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 12);
    let segments = segments_dir(dir.path());

    // Segment 1..=5 cannot be compressed, so it stays warm in front of the cold 6..=10.
    std::fs::write(segment_file(&segments, 1, 5, "wal"), b"damaged").unwrap();
    let _ = log.run_maintenance_sync();
    log.policy.max_room_disk_bytes = 1;
    let _ = log.run_maintenance_sync();

    assert!(segment_file(&segments, 6, 10, "wal.zst").exists());
    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn compression_runs_off_the_runtime_thread() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 7);
    let warm = segment_file(&segments_dir(dir.path()), 1, 5, "wal");
    let (tx, rx) = std::sync::mpsc::channel();
    crate::fail_point::arm_hook("compress_warm_segment", &warm, move || {
        tx.send(std::thread::current().id()).unwrap();
    });

    // A current-thread runtime on this thread: compressing inline would run here.
    assert_eq!(log.run_maintenance_sync().unwrap().warm_compressed_count, 1);
    assert_ne!(rx.try_recv().unwrap(), std::thread::current().id());
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
        segment_file(&segments, 1, 5, "wal.zst"),
        segment_file(&segments, 99, 100, "wal.zst.tmp"),
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
    let path = segment_file(&segments_dir(dir), start, end, "wal");
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
    std::fs::remove_file(segment_file(&segments_dir(dir.path()), 6, 10, "wal.zst")).unwrap();

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
    corrupt_first_record(&segment_file(&segments_dir(dir.path()), 1, 5, "wal"));

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
    // Without its sidecar, the segment is decoded on open.
    corrupt_first_record(&segment_file(&segments_dir(dir.path()), 6, 10, "wal"));
    std::fs::remove_file(segment_file(&segments_dir(dir.path()), 6, 10, "ids")).unwrap();

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

/// An operation whose value takes `bytes` bytes.
fn large_op(seq: u64, bytes: usize) -> SequencedOperation {
    let row = zemdb_core::value::CompactRow::new(vec![
        Value::Int(seq as i64),
        Value::Bytes(vec![seq as u8; bytes].into()),
    ]);
    SequencedOperation::new(
        SequenceNumber::new(seq),
        Operation::insert(1, PrimaryKey::single(Value::Int(seq as i64)), row, seq),
    )
}

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

fn count_in(log: &TieredLog, tier: Tier) -> usize {
    log.segments.iter().filter(|s| s.tier == tier).count()
}

#[test]
fn hot_buffer_evicts_by_bytes_below_its_operation_limit() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 1_000,
        ram_max_bytes: MIB as u64,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for seq in 1..=10 {
        log.append(large_op(seq, 300 * KIB), None).unwrap();
    }

    assert!(log.hot_buffer.bytes() <= MIB as u64);
    assert_eq!(log.hot_buffer.len(), 3);
    assert_eq!(log.hot_buffer.max_seq(), Some(SequenceNumber::new(10)));
    // Everything evicted is still served, from disk.
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 10);
    assert!(!has_more);
}

#[test]
fn operation_larger_than_ram_is_served_from_disk() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_bytes: MIB as u64,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    log.append(large_op(1, 2 * MIB), None).unwrap();

    assert!(log.hot_buffer.is_empty());
    log.append(make_op(2), None).unwrap();
    assert_eq!(log.hot_buffer.len(), 1);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 2);
}

#[test]
fn active_segment_rotates_at_its_byte_limit() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 1_000,
        segment_max_bytes: MIB as u64,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    for seq in 1..=3 {
        log.append(large_op(seq, 300 * KIB), None).unwrap();
    }
    assert_eq!(count_in(&log, Tier::Warm), 0);
    log.append(large_op(4, 300 * KIB), None).unwrap();

    assert_eq!(count_in(&log, Tier::Warm), 1);
    assert_eq!(log.warm_disk.active_len(), 0);
}

/// Six operations of 6 MiB: two fit in a response (`MAX_RESPONSE_OPS_BYTES` is just under
/// 16 MiB), three do not.
fn large_ops_log(dir: &Path, ram_max_bytes: u64) -> TieredLog {
    let policy = RoomLifecyclePolicy {
        ram_max_bytes,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir, policy).unwrap();
    for seq in 1..=6 {
        log.append(large_op(seq, 6 * MIB), None).unwrap();
    }
    log
}

#[test]
fn fetch_returns_what_fits_in_a_response_from_ram() {
    let dir = tempdir().unwrap();
    let log = large_ops_log(dir.path(), MAX_LOG_BYTES_LIMIT);
    assert_eq!(log.hot_buffer.len(), 6);

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();

    assert_contiguous(&ops, 1, 2);
    assert!(has_more);
}

#[test]
fn fetch_returns_what_fits_in_a_response_from_disk() {
    let dir = tempdir().unwrap();
    let log = large_ops_log(dir.path(), MIN_LOG_BYTES_LIMIT);
    assert!(log.hot_buffer.is_empty());

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(2), 100).unwrap();

    assert_contiguous(&ops, 3, 4);
    assert!(has_more);
}

#[test]
fn cold_segment_of_large_operations_roundtrips() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 4,
        ram_max_bytes: MIN_LOG_BYTES_LIMIT,
        warm_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    let ops: Vec<_> = (1..=4).map(|seq| large_op(seq, 3 * MIB)).collect();
    for op in &ops {
        log.append(op.clone(), None).unwrap();
    }

    log.run_maintenance_sync().unwrap();

    assert_eq!(count_in(&log, Tier::Cold), 1);
    let (read, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_eq!(read, ops);
    assert!(!has_more);
}

#[test]
fn maintenance_compresses_a_bounded_number_of_segments_per_pass() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    // Six sealed segments of five operations.
    append_range(&mut log, 1, 30);
    assert_eq!(count_in(&log, Tier::Warm), 6);

    let report = log.run_maintenance_sync().unwrap();
    assert_eq!(report.warm_compressed_count, MAX_COMPRESSIONS_PER_PASS);
    assert_eq!(count_in(&log, Tier::Warm), 2);

    let report = log.run_maintenance_sync().unwrap();
    assert_eq!(report.warm_compressed_count, 2);
    assert_eq!(count_in(&log, Tier::Cold), 6);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&ops, 1, 30);
}

#[tokio::test]
async fn async_maintenance_compresses_a_bounded_number_of_segments_per_pass() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 30);

    let report = log.run_maintenance().await.unwrap();

    assert_eq!(report.warm_compressed_count, MAX_COMPRESSIONS_PER_PASS);
    assert_eq!(count_in(&log, Tier::Warm), 2);
}

#[test]
fn reopening_loads_into_ram_only_what_fits() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 1_000,
        ram_max_bytes: MIB as u64,
        // active.wal holds more than RAM, which the open has to trim.
        segment_max_bytes: 4 * MIB as u64,
        ..RoomLifecyclePolicy::default()
    };
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        for seq in 1..=10 {
            log.append(large_op(seq, 300 * KIB), Some(mutation_for(seq)))
                .unwrap();
        }
    }

    let (log, mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    assert!(log.hot_buffer.bytes() <= MIB as u64);
    assert_eq!(log.hot_buffer.max_seq(), Some(SequenceNumber::new(10)));
    assert_eq!(mutations.len(), 10);
}

#[test]
fn opening_fills_ram_only_from_active_wal() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 1_000,
        ram_max_bytes: MIB as u64,
        segment_max_bytes: MIB as u64,
        ..RoomLifecyclePolicy::default()
    };
    {
        // Segments 1..=4 and 5..=8, and 9..=10 in active.wal.
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        for seq in 1..=10 {
            log.append(large_op(seq, 300 * KIB), Some(mutation_for(seq)))
                .unwrap();
        }
    }
    let segments = SegmentIndex::scan(&segments_dir(dir.path()), SequenceNumber::new(0)).unwrap();
    let (_warm, active) = WarmDiskLog::open_recovering(segments_dir(dir.path())).unwrap();

    let (ops, mutations) = read_newest(&segments, active, usize::MAX).unwrap();

    // Every mutation ID, but only the operations of active.wal.
    assert_eq!(mutations.len(), 10);
    assert_contiguous(&ops, 9, 10);
}

/// Flips a byte in the payload of the `index`-th frame of the segment open in `file`.
fn damage_payload(mut file: &File, index: usize) {
    use std::io::{Read, Seek, SeekFrom, Write};
    use zemdb_core::protocol::wal_frame::{parse_batch_header, BATCH_HEADER_SIZE};

    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_to_end(&mut bytes).unwrap();
    let mut offset = 0;
    for _ in 0..index {
        let header = parse_batch_header(bytes[offset..].first_chunk().unwrap()).unwrap();
        offset += header.frame_len();
    }
    let at = offset + BATCH_HEADER_SIZE;
    file.seek(SeekFrom::Start(at as u64)).unwrap();
    file.write_all(&[bytes[at] ^ 0xFF]).unwrap();
}

#[test]
fn reads_skip_damaged_frames_before_the_cursor_in_sealed_and_active_segments() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        // Nothing stays in RAM, so every read goes to disk.
        ram_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    // Segments 1..=5 and 6..=10, and 11..=12 in active.wal.
    append_range(&mut log, 1, 12);
    assert!(log.hot_buffer.is_empty());
    let segments = segments_dir(dir.path());

    let sealed = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment_file(&segments, 1, 5, "wal"))
        .unwrap();
    for index in 0..4 {
        damage_payload(&sealed, index);
    }
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(4), 100).unwrap();
    assert_contiguous(&ops, 5, 12);
    assert!(!has_more);

    // Through the handle that holds the lock: Windows refuses any other.
    damage_payload(log.warm_disk.active_file().unwrap(), 0);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(11), 100).unwrap();
    assert_contiguous(&ops, 12, 12);
}

#[test]
fn segments_that_keep_failing_do_not_hold_back_newer_ones() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    // Six sealed segments of five operations; the four oldest cannot be compressed.
    append_range(&mut log, 1, 30);
    let doomed: Vec<_> = log
        .segments
        .iter()
        .take(4)
        .map(|s| s.path.clone())
        .collect();
    for path in &doomed {
        std::fs::remove_file(path).unwrap();
    }

    // The first pass tries the four oldest, which fail; the next one tries the newer ones
    // first.
    assert!(log.run_maintenance_sync().is_err());
    assert_eq!(count_in(&log, Tier::Cold), 0);
    assert!(log.run_maintenance_sync().is_err());
    assert_eq!(count_in(&log, Tier::Cold), 2);
}

#[test]
fn a_pass_tries_a_bounded_number_of_segments_even_when_they_fail() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 30);
    let paths: Vec<_> = log.segments.iter().map(|s| s.path.clone()).collect();
    for path in &paths {
        std::fs::remove_file(path).unwrap();
    }

    assert!(log.run_maintenance_sync().is_err());
    assert_eq!(log.failed_compressions.len(), MAX_COMPRESSIONS_PER_PASS);

    // The two not tried yet go first, then two of those that failed.
    assert!(log.run_maintenance_sync().is_err());
    assert_eq!(log.failed_compressions.len(), 6);
}

#[test]
fn retries_rotate_through_every_failed_segment() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 30);
    let paths: Vec<_> = log.segments.iter().map(|s| s.path.clone()).collect();
    let newest = std::fs::read(&paths[5]).unwrap();
    for path in &paths {
        std::fs::remove_file(path).unwrap();
    }
    // Every segment fails at least once.
    for _ in 0..3 {
        assert!(log.run_maintenance_sync().is_err());
    }
    assert_eq!(log.failed_compressions.len(), 6);

    // The newest segment recovers; the four oldest keep failing.
    std::fs::write(&paths[5], newest).unwrap();
    for _ in 0..2 {
        let _ = log.run_maintenance_sync();
    }

    assert_eq!(count_in(&log, Tier::Cold), 1);
}

#[test]
fn test_tiered_log_write_through_and_crash_recovery() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        assert_eq!(log.head_seq().get(), 0);

        for i in 1..=10 {
            log.append(make_op(i), None).unwrap();
            assert_eq!(log.head_seq().get(), i);
        }
        // Abruptly drop log without clean shutdown (simulating server crash)
    }

    // Reopen from disk: every operation survived, byte for byte.
    let (log2, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log2.head_seq().get(), 10);

    let (deltas, has_more) = log2.fetch_deltas(SequenceNumber::new(0), 100).unwrap();

    assert_eq!(deltas, (1..=10).map(make_op).collect::<Vec<_>>());
    assert!(!has_more);
}

#[test]
fn test_tiered_log_hot_buffer_fast_read() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    append_plain(&mut log, 1, 5);

    let (batch1, has_more1) = log.fetch_deltas(SequenceNumber::new(2), 2).unwrap();
    assert_contiguous(&batch1, 3, 4);
    assert!(has_more1);

    let (batch2, has_more2) = log.fetch_deltas(SequenceNumber::new(4), 10).unwrap();
    assert_contiguous(&batch2, 5, 5);
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

    append_plain(&mut log, 1, 6);

    let segments = segments_dir(dir.path());
    assert!(segment_file(&segments, 1, 5, "wal").exists());
    assert!(segments.join("active.wal").exists());

    // Fetch across the rotation boundary (from 0 to 6)
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10).unwrap();

    assert_contiguous(&deltas, 1, 6);
    assert!(!has_more);
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
        ..RoomLifecyclePolicy::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    append_plain(&mut log, 1, 5);
    // Force rotate to seal segment 1..5 into .wal
    log.force_rotate_warm().unwrap();

    let segments = segments_dir(dir.path());
    let sealed_wal = segment_file(&segments, 1, 5, "wal");
    assert!(sealed_wal.exists());

    // Wait past warm_disk_ttl
    sleep(Duration::from_millis(20));

    // The async pass compresses the segment off the async worker.
    let report = log.run_maintenance().await.unwrap();
    assert_eq!(report.warm_compressed_count, 1);

    // Sealed .wal should be removed, and .wal.zst created
    assert!(!sealed_wal.exists());
    assert!(segment_file(&segments, 1, 5, "wal.zst").exists());

    // Fetch deltas directly from Cold Disk (.wal.zst)
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10).unwrap();

    assert_contiguous(&deltas, 1, 5);
    assert!(!has_more);
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
        ..RoomLifecyclePolicy::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Operations 1..=5 are sealed and compressed to cold.
    append_plain(&mut log, 1, 5);
    log.force_rotate_warm().unwrap();
    sleep(Duration::from_millis(20));
    log.run_maintenance().await.unwrap();

    // Operations 6..=10 are sealed to warm, without compressing them.
    append_plain(&mut log, 6, 10);
    log.force_rotate_warm().unwrap();

    // Operations 11..=15 stay in RAM and active.wal.
    append_plain(&mut log, 11, 15);

    assert_eq!(log.head_seq().get(), 15);

    // Fetch entire range 0..15 traversing Cold -> Warm -> Hot in a single contiguous batch
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 20).unwrap();

    assert_contiguous(&deltas, 1, 15);
    assert!(!has_more);

    // Paged fetch crossing from Cold (ends at 5) into Warm (starts at 6)
    let (paged, paged_has_more) = log.fetch_deltas(SequenceNumber::new(3), 5).unwrap();

    assert_contiguous(&paged, 4, 8);
    assert!(paged_has_more);
}

#[test]
fn test_tiered_log_strict_contiguity_and_monotonicity() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    log.append(make_op(1), None).unwrap();

    // Attempting to append sequence 3 directly (gap) must be rejected
    let err = log.append(make_op(3), None).unwrap_err();
    assert!(
        matches!(&err, ServerError::Wal(msg) if msg.contains("Non-contiguous sequence")),
        "{err:?}"
    );
}

#[test]
fn test_tiered_log_proactive_pruning_by_cursor() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Two sealed warm segments, 1..=5 and 6..=10.
    append_plain(&mut log, 1, 5);
    log.force_rotate_warm().unwrap();
    append_plain(&mut log, 6, 10);
    log.force_rotate_warm().unwrap();

    let segments = segments_dir(dir.path());
    let seg1_path = segment_file(&segments, 1, 5, "wal");
    let seg2_path = segment_file(&segments, 6, 10, "wal");
    assert!(seg1_path.exists());
    assert!(seg2_path.exists());

    // Proactively prune all deltas older than sequence 6 (all clients confirmed >= 6)
    let prune_report = log.prune_older_than(SequenceNumber::new(6)).unwrap();
    assert_eq!(prune_report.warm_deleted_count, 1);
    assert_eq!(prune_report.new_tail_seq.get(), 6);

    // Segment 1..5 was deleted from disk, segment 6..10 remains
    assert!(!seg1_path.exists());
    assert!(seg2_path.exists());

    // Cursor 2 is now expired -> BehindCompaction
    let result = log.fetch_deltas(SequenceNumber::new(2), 10);
    assert!(
        matches!(result, Err(ServerError::BehindCompaction)),
        "{result:?}"
    );

    // Cursor 5 can fetch 6..=10 smoothly
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10).unwrap();
    assert_contiguous(&deltas, 6, 10);
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

    // Operations 1..=5 fill the RAM buffer and reach the active segment limit.
    append_plain(&mut log, 1, 5);

    // Operation 6 rotates the active segment into segment_1_5.wal. The sliding window must
    // pop the oldest element (1) and retain operations 2..=6 in RAM.
    log.append(make_op(6), None).unwrap();

    // Cursor 1 immediately precedes the buffer start (1 + 1 == 2), so 2..=6 come straight
    // from RAM, whether the limit is exactly the five left or larger.
    for limit in [5, 10] {
        let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(1), limit).unwrap();
        assert_contiguous(&deltas, 2, 6);
        assert!(!has_more, "limit {limit}");
    }

    // Fetching from cursor 4 returns 5 and 6
    let (recent_deltas, recent_has_more) = log.fetch_deltas(SequenceNumber::new(4), 10).unwrap();
    assert_contiguous(&recent_deltas, 5, 6);
    assert!(!recent_has_more);
}

#[tokio::test]
async fn test_tiered_log_disk_quota_saturation_pruning() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 100,
        ram_ttl: Duration::from_secs(3600),
        warm_disk_ttl: Duration::from_millis(5),
        cold_disk_ttl: Duration::from_secs(3600), // Very high TTL so time expiration does not trigger
        max_room_disk_bytes: 100, // Very low quota to force quota saturation pruning
        ..RoomLifecyclePolicy::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Two sealed warm segments, 1..=5 and 6..=10.
    append_plain(&mut log, 1, 5);
    log.force_rotate_warm().unwrap();
    append_plain(&mut log, 6, 10);
    log.force_rotate_warm().unwrap();

    // Wait past warm_disk_ttl so maintenance compresses them to Cold (.wal.zst)
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Operations 11..=15 stay in the active segment.
    append_plain(&mut log, 11, 15);

    // Maintenance compresses the warm segments and immediately prunes cold ones over the quota.
    let report = log.run_maintenance().await.unwrap();
    assert_eq!(report.warm_compressed_count, 2);
    assert!(
        report.cold_pruned_count >= 1,
        "Quota saturation must prune at least one cold segment"
    );

    // tail_seq must have advanced past 1 (at least to 6)
    assert!(
        log.tail_seq().get() >= 6,
        "tail_seq must advance after quota pruning, got {}",
        log.tail_seq().get()
    );

    // Any fetch before the new tail_seq must return BehindCompaction
    let result = log.fetch_deltas(SequenceNumber::new(0), 10);
    assert!(
        matches!(result, Err(ServerError::BehindCompaction)),
        "Expected BehindCompaction on quota pruned deltas, got: {result:?}"
    );

    // Fetch from the new tail cursor succeeds
    let cursor = SequenceNumber::new(log.tail_seq().get() - 1);
    let (deltas, _) = log.fetch_deltas(cursor, 10).unwrap();
    assert!(!deltas.is_empty());
    assert_eq!(deltas[0].seq, log.tail_seq());
}

#[test]
fn test_tiered_log_eviction_gap_bridged_from_sealed_segments() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..Default::default()
    };

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    // Append 15 operations. With ram_max_ops = 5, operations 1..=10 are evicted
    // from the RAM sliding window, while 11..=15 remain in RAM.
    // The active segment also rotates every 5 operations, so all 15 operations
    // reside in sealed segments on disk and no `active.wal` remains.
    append_plain(&mut log, 1, 15);

    assert_eq!(log.head_seq().get(), 15);

    // Request range crossing the eviction boundary: from cursor 7 with limit 6 (expecting 8..=13).
    // Operations 8..=10 were evicted from RAM and must be read from the sealed segments.
    // Operations 11..=13 are present in RAM HotBuffer.
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(7), 6).unwrap();

    assert_contiguous(&deltas, 8, 13);
    assert!(has_more);
}

#[test]
fn test_unified_wal_append_and_recovery_roundtrip() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let mutation = |seq: u64| MutationId::from_u128(u128::from(seq) + 1000);

    {
        let (mut log, recovered_mutations) =
            TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        assert_eq!(log.head_seq().get(), 0);
        assert_eq!(recovered_mutations.len(), 0);

        for i in 1..=10 {
            log.append(make_op(i), Some(mutation(i))).unwrap();
            assert_eq!(log.head_seq().get(), i);
        }
    }

    // Reopening recovers head_seq, the deltas and the mutation ids.
    let (log2, recovered_mutations2) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log2.head_seq().get(), 10);
    let expected: Vec<_> = (1..=10)
        .map(|seq| (mutation(seq), SequenceNumber::new(seq)))
        .collect();
    assert_eq!(recovered_mutations2, expected);

    let (deltas, has_more) = log2.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_contiguous(&deltas, 1, 10);
    assert!(!has_more);
}

#[test]
fn test_unified_wal_dedup_idempotency() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    let mut expected_entries = Vec::new();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();

        for i in 1..=5 {
            let mutation_id = MutationId::from_u128(i as u128 * 42);
            log.append(make_op(i), Some(mutation_id)).unwrap();
            expected_entries.push((mutation_id, SequenceNumber::new(i)));
        }
    }

    // Reopen and hydrate DedupLruCache from recovered log entries
    let (_, recovered_mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    let mut dedup_cache = DedupLruCache::new(100);
    dedup_cache.hydrate(recovered_mutations);

    assert_eq!(dedup_cache.len(), 5);

    // Verify all recorded mutations are recognized as duplicates
    for (mut_id, seq) in expected_entries {
        assert_eq!(dedup_cache.is_duplicate(&mut_id), Some(seq));
    }

    // Verify an uncommitted mutation is not recognized as a duplicate
    let unknown_mut = MutationId::from_u128(999999);
    assert_eq!(dedup_cache.is_duplicate(&unknown_mut), None);

    // Record new mutation and verify it becomes duplicate
    dedup_cache.record(unknown_mut, SequenceNumber::new(6));
    assert_eq!(
        dedup_cache.is_duplicate(&unknown_mut),
        Some(SequenceNumber::new(6))
    );
}

#[test]
fn test_unified_wal_crc_tampering_detection() {
    let dir = tempdir().unwrap();
    let active_path = write_log(dir.path(), 3);

    // Corrupt one byte in the middle of the first batch (after batch header)
    let mut data = std::fs::read(&active_path).unwrap();
    data[18] ^= 0xFF;
    std::fs::write(&active_path, data).unwrap();

    let err = TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default())
        .err()
        .unwrap();
    assert!(
        matches!(&err, ServerError::WalCorruption(msg)
            if msg.contains("CRC") || msg.contains("crc") || msg.contains("Corruption")),
        "{err:?}"
    );
}

#[test]
fn test_unified_wal_torn_write_recovery_and_truncation() {
    let dir = tempdir().unwrap();
    let active_path = write_log(dir.path(), 3);
    let clean_len = file_len(&active_path);

    // Simulate a torn write by appending incomplete bytes to active.wal
    {
        let mut file = OpenOptions::new().append(true).open(&active_path).unwrap();
        file.write_all(&[0x52, 0x49, 0x4D, 0x42, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB])
            .unwrap();
    }
    assert_eq!(file_len(&active_path), clean_len + 10);

    // Reopen log: torn write should be detected and automatically truncated to clean_len
    let (mut log2, recovered_mutations) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    assert_eq!(log2.head_seq().get(), 3);
    assert_eq!(recovered_mutations.len(), 3);
    assert_eq!(file_len(&active_path), clean_len);

    // Verify subsequent append proceeds contiguously
    log2.append(make_op(4), Some(mutation_for(4))).unwrap();
    assert_eq!(log2.head_seq().get(), 4);
}

#[test]
fn test_unified_wal_zero_filled_eof_truncation() {
    let dir = tempdir().unwrap();
    let active_path = write_log(dir.path(), 1);
    let clean_len = file_len(&active_path);

    // Simulate zero-filled tail appended due to crash during allocation
    {
        let mut file = OpenOptions::new().append(true).open(&active_path).unwrap();
        file.write_all(&[0u8; 512]).unwrap();
    }

    let (log2, recovered_mutations) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    assert_eq!(log2.head_seq().get(), 1);
    assert_eq!(recovered_mutations.len(), 1);
    assert_eq!(file_len(&active_path), clean_len);
}

// ---- Randomized reads checked against every accepted operation ------------------------------

/// An operation of a random size, from a few bytes to a few KiB, so that the byte limits of
/// RAM and of the active segment cut at varying places.
fn random_size_op(seq: u64, rng: &mut TestRng) -> SequencedOperation {
    let len = match rng.below(10) {
        0 => 3_000 + rng.below(3_000) as usize,
        1..=3 => 200 + rng.below(800) as usize,
        _ => rng.below(40) as usize,
    };
    let row = zemdb_core::value::CompactRow::new(vec![
        Value::Int(seq as i64),
        Value::Bytes(vec![(seq % 251) as u8; len].into()),
    ]);
    SequencedOperation::new(
        SequenceNumber::new(seq),
        Operation::insert(1, PrimaryKey::single(Value::Int(seq as i64)), row, seq),
    )
}

/// Follows `has_more` from `cursor` asking for `batch_size()` operations per page, as a client
/// does, and returns the concatenation of the pages, or the first error. Every page respects
/// its size, and none is empty while the cursor is below the head.
fn read_chain(
    log: &TieredLog,
    cursor: u64,
    mut batch_size: impl FnMut() -> u32,
) -> Result<Vec<SequencedOperation>, ServerError> {
    let mut out: Vec<SequencedOperation> = Vec::new();
    let mut at = cursor;
    loop {
        let batch = batch_size();
        let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(at), batch)?;
        assert!(ops.len() <= batch as usize, "page over its size from {at}");
        if at < log.head_seq().get() {
            assert!(!ops.is_empty(), "empty page below the head from {at}");
        }
        let Some(last) = ops.last() else {
            assert!(!has_more, "has_more without progress from {at}");
            break;
        };
        at = last.seq.get();
        out.extend(ops);
        if !has_more {
            break;
        }
    }
    Ok(out)
}

/// Checks `log` against `accepted`, every operation appended to it so far: the head is the
/// number of operations, the tail is within `[1, head + 1]`, a chain from any cursor of the
/// retained range (the edges of every segment and of RAM among them) returns its exact
/// suffix, and a cursor below the range gets `BehindCompaction`.
fn check_reads(log: &TieredLog, accepted: &[SequencedOperation], rng: &mut TestRng, step: &str) {
    let head = log.head_seq().get();
    let tail = log.tail_seq().get();
    assert_eq!(head, accepted.len() as u64, "{step}: head");
    assert!(
        tail >= 1 && tail <= head + 1,
        "{step}: tail {tail}, head {head}"
    );

    let retained = |c: &&u64| **c + 1 >= tail && **c <= head;
    let mut cursors = vec![tail - 1, head, head.saturating_sub(1).max(tail - 1)];
    for segment in log.segments.iter() {
        let (start, end) = (segment.start_seq.get(), segment.end_seq.get());
        cursors.extend([start - 1, start, end].iter().filter(retained));
    }
    if let Some(min) = log.hot_buffer.min_seq() {
        let min = min.get();
        cursors.extend([min.saturating_sub(1), min].iter().filter(retained));
    }
    for _ in 0..6 {
        cursors.push(tail - 1 + rng.below(head + 2 - tail));
    }
    for c in cursors {
        let got = read_chain(log, c, || 1 + rng.below(7) as u32)
            .unwrap_or_else(|e| panic!("{step}: cursor {c} (tail {tail}, head {head}): {e}"));
        assert!(
            got.as_slice() == &accepted[c as usize..],
            "{step}: cursor {c} (tail {tail}, head {head}) got {:?}",
            seqs(&got)
        );
    }

    if tail >= 2 {
        let c = rng.below(tail - 1);
        assert!(
            matches!(
                log.fetch_deltas(SequenceNumber::new(c), 5),
                Err(ServerError::BehindCompaction)
            ),
            "{step}: cursor {c} below tail {tail}"
        );
    }
}

/// A clock some time from now: none, past the RAM TTL, or past every disk TTL.
fn random_clock(rng: &mut TestRng) -> MaintenanceClock {
    let later = [
        Duration::ZERO,
        Duration::from_secs(120),
        Duration::from_secs(7_200),
    ];
    MaintenanceClock::after(later[rng.below(3) as usize])
}

/// For each seed, `steps` random steps on a log with small random limits (appends, maintenance
/// at different ages, cursor-driven pruning, forced rotation, reopening), checking every read
/// after each step against the accepted operations, and that the tail never moves back.
fn simulate_reads(seeds: std::ops::Range<u64>, steps: usize) {
    for seed in seeds {
        let mut rng = TestRng::new(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) + 1);
        let dir = tempdir().unwrap();
        let policy = RoomLifecyclePolicy {
            ram_max_ops: 1 + rng.below(6) as usize,
            ram_max_bytes: 2_000 + rng.below(10_000),
            segment_max_bytes: 1_500 + rng.below(8_000),
            ram_ttl: Duration::from_secs(60),
            warm_disk_ttl: Duration::from_secs(if rng.below(2) == 0 { 0 } else { 600 }),
            cold_disk_ttl: Duration::from_secs(3_600),
            max_room_disk_bytes: if rng.below(3) == 0 {
                20_000 + rng.below(20_000)
            } else {
                u64::MAX / 4
            },
            ..RoomLifecyclePolicy::default()
        };
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        let mut accepted: Vec<SequencedOperation> = Vec::new();
        let mut last_tail = log.tail_seq().get();
        for step in 0..steps {
            match rng.below(10) {
                0..=4 => {
                    for _ in 0..1 + rng.below(6) {
                        let op = random_size_op(accepted.len() as u64 + 1, &mut rng);
                        let id = mutation_for(op.seq.get());
                        let outcome = log.append(op.clone(), Some(id)).unwrap();
                        assert!(outcome.rotation_error.is_none());
                        accepted.push(op);
                    }
                }
                5 => {
                    log.run_maintenance_sync_at(random_clock(&mut rng)).unwrap();
                }
                6 => {
                    let target = 1 + rng.below(log.head_seq().get().max(1));
                    log.prune_older_than(SequenceNumber::new(target)).unwrap();
                }
                7 => {
                    log.force_rotate_warm().unwrap();
                }
                _ => {
                    drop(log);
                    log = TieredLog::open_or_create(dir.path(), policy.clone())
                        .unwrap()
                        .0;
                }
            }
            let step = format!("seed {seed} step {step}");
            let tail = log.tail_seq().get();
            assert!(tail >= last_tail, "{step}: tail moved back");
            last_tail = tail;
            check_reads(&log, &accepted, &mut rng, &step);
        }
    }
}

#[test]
fn reads_from_any_cursor_return_its_exact_suffix() {
    simulate_reads(0..1, 20);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn reads_from_any_cursor_return_its_exact_suffix_over_many_seeds() {
    simulate_reads(0..24, 45);
}

/// Operations of about a third of a response, and two small ones right after a large one in
/// the next tier: the byte budget cuts pages inside and at the edges of every tier, a page
/// never continues past an operation it refused even when a later one would fit, and a chain
/// from any cursor returns its exact suffix. Encoding and decoding some 100 MiB of operations
/// takes many seconds in a debug build.
#[test]
#[ignore = "long-running test; run with --ignored in release"]
fn byte_budget_cuts_never_skip_an_operation_across_tiers() {
    // A page from 0 takes 1..=3 and refuses 4; one from 1 refuses 4 and one from 2 refuses 6,
    // and 5 and 7, in the next tier, would still fit.
    const SIZES_KIB: [usize; 7] = [5_200, 5_300, 5_100, 6_000, 1, 5_400, 1];
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 2,
        ram_max_bytes: MAX_LOG_BYTES_LIMIT,
        warm_disk_ttl: Duration::ZERO,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    let mut accepted = Vec::new();
    for (seq, kib) in (1..=7).zip(SIZES_KIB) {
        let op = large_op(seq, kib * KIB);
        log.append(op.clone(), None).unwrap();
        accepted.push(op);
        if seq == 4 {
            // 1..=2 and 3..=4 sealed and compressed.
            log.run_maintenance_sync().unwrap();
        }
    }
    // 5..=6 sealed warm, 7 in active.wal and RAM.
    assert_eq!(count_in(&log, Tier::Cold), 2);
    assert_eq!(count_in(&log, Tier::Warm), 1);
    for c in 0..=7u64 {
        let got = read_chain(&log, c, || 100).unwrap();
        assert!(
            got.as_slice() == &accepted[c as usize..],
            "cursor {c}: {:?}",
            seqs(&got)
        );
    }
}

/// For each seed, chains of pages from random cursors while the log changes between pages
/// (appends, maintenance at different ages, cursor-driven pruning, reopening). Every page
/// continues its cursor exactly with the accepted operations, its `has_more` says whether the
/// head was reached, and the only error is `BehindCompaction` for a cursor behind the tail.
fn simulate_pages_while_the_log_changes(seeds: std::ops::Range<u64>, chains: usize) {
    fn append(log: &mut TieredLog, accepted: &mut Vec<SequencedOperation>, rng: &mut TestRng) {
        for _ in 0..1 + rng.below(4) {
            let op = random_size_op(accepted.len() as u64 + 1, rng);
            log.append(op.clone(), None).unwrap();
            accepted.push(op);
        }
    }

    for seed in seeds {
        let mut rng = TestRng::new(seed.wrapping_mul(0x9E37_79B9) + 11);
        let dir = tempdir().unwrap();
        let policy = RoomLifecyclePolicy {
            ram_max_ops: 1 + rng.below(5) as usize,
            ram_max_bytes: 1_500 + rng.below(6_000),
            segment_max_bytes: 1_200 + rng.below(5_000),
            ram_ttl: Duration::from_secs(60),
            warm_disk_ttl: Duration::ZERO,
            cold_disk_ttl: Duration::from_secs(3_600),
            max_room_disk_bytes: 30_000 + rng.below(30_000),
            ..RoomLifecyclePolicy::default()
        };
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        let mut accepted: Vec<SequencedOperation> = Vec::new();
        for _ in 0..10 {
            append(&mut log, &mut accepted, &mut rng);
        }
        for chain in 0..chains {
            let context = format!("seed {seed} chain {chain}");
            let (tail, head) = (log.tail_seq().get(), log.head_seq().get());
            let mut at = tail - 1 + rng.below(head + 2 - tail);
            loop {
                let batch = 1 + rng.below(5) as u32;
                let (tail, head) = (log.tail_seq().get(), log.head_seq().get());
                match log.fetch_deltas(SequenceNumber::new(at), batch) {
                    Ok((ops, has_more)) => {
                        let n = ops.len() as u64;
                        assert!(
                            ops.as_slice() == &accepted[at as usize..(at + n) as usize],
                            "{context}: page from {at}"
                        );
                        assert_eq!(has_more, at + n < head, "{context}: has_more from {at}");
                        assert!(at == head || n > 0, "{context}: empty page below the head");
                        at += n;
                        if !has_more {
                            break;
                        }
                    }
                    Err(ServerError::BehindCompaction) => {
                        assert!(
                            at + 1 < tail,
                            "{context}: BehindCompaction from {at}, tail {tail}"
                        );
                        break;
                    }
                    Err(e) => panic!("{context}: {e}"),
                }
                // The log moves on between pages.
                match rng.below(8) {
                    0 | 1 => append(&mut log, &mut accepted, &mut rng),
                    2 => {
                        log.run_maintenance_sync_at(random_clock(&mut rng)).unwrap();
                    }
                    3 => {
                        let target = 1 + rng.below(log.head_seq().get().max(1));
                        log.prune_older_than(SequenceNumber::new(target)).unwrap();
                    }
                    4 => {
                        drop(log);
                        log = TieredLog::open_or_create(dir.path(), policy.clone())
                            .unwrap()
                            .0;
                    }
                    _ => {}
                }
            }
            append(&mut log, &mut accepted, &mut rng);
        }
    }
}

#[test]
fn pages_stay_exact_while_the_log_changes_between_them() {
    simulate_pages_while_the_log_changes(100..101, 10);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn pages_stay_exact_while_the_log_changes_between_them_over_many_seeds() {
    simulate_pages_while_the_log_changes(100..116, 30);
}

/// A cursor-driven prune whose deletion fails in the middle of its prefix keeps the retained
/// range contiguous, and reopening finishes it without moving the tail back.
#[test]
fn prune_failing_mid_prefix_stays_contiguous_and_reopening_finishes_it() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 3,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    // Segments 1..=3, 4..=6, 7..=9 and 10..=12; 13 in active.wal.
    append_range(&mut log, 1, 13);
    let segments = segments_dir(dir.path());
    crate::fail_point::arm("remove_file", &segment_file(&segments, 4, 6, "wal"));

    assert!(log.prune_older_than(SequenceNumber::new(10)).is_err());
    let tail = log.tail_seq().get();
    assert_eq!(tail, 4, "only the first segment went");
    let (ops, _) = log
        .fetch_deltas(SequenceNumber::new(tail - 1), 100)
        .unwrap();
    assert_contiguous(&ops, tail, 13);
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log.tail_seq().get(), 10);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(9), 100).unwrap();
    assert_contiguous(&ops, 10, 13);
}

/// Opens a three-frame `active.wal` with each byte flipped by each of `masks(position)`. Damage
/// in a frame followed by a valid frame fails the open and leaves the file as it was; damage in
/// the last frame truncates the file exactly where that frame starts. A damaged frame is never
/// decoded into an operation.
fn check_byte_flips_of_active_wal(masks: impl Fn(usize) -> Vec<u8>) {
    let (clean, offsets) = frames(1..=3);
    for position in 0..clean.len() {
        for mask in masks(position) {
            let dir = tempdir().unwrap();
            let segments = segments_dir(dir.path());
            std::fs::create_dir_all(&segments).unwrap();
            let mut bytes = clean.clone();
            bytes[position] ^= mask;
            std::fs::write(segments.join("active.wal"), &bytes).unwrap();
            let frame = offsets.iter().rposition(|&o| o <= position).unwrap();

            let opened = TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default());
            let on_disk = std::fs::read(segments.join("active.wal")).unwrap();
            let flip = format!("flip {mask:#04x} at {position} (frame {frame})");
            if frame < 2 {
                assert!(opened.is_err(), "{flip}: corruption must fail the open");
                assert_eq!(on_disk, bytes, "{flip}: nothing may be truncated");
            } else {
                let (log, _) = opened.unwrap_or_else(|e| panic!("{flip}: {e}"));
                assert_eq!(log.head_seq().get(), 2, "{flip}");
                assert_eq!(
                    on_disk.len(),
                    offsets[2],
                    "{flip}: truncated from the damage"
                );
            }
        }
    }
}

/// Every byte flipped once, by a low bit, a high bit or the whole byte in turn.
#[test]
fn every_byte_flip_of_active_wal_is_corruption_or_a_torn_tail() {
    check_byte_flips_of_active_wal(|position| vec![[0x01, 0x80, 0xFF][position % 3]]);
}

/// Every byte flipped by a low bit, a high bit and the whole byte.
#[test]
#[ignore = "long-running test; run with --ignored in release"]
fn every_bit_and_byte_flip_of_active_wal_is_corruption_or_a_torn_tail() {
    check_byte_flips_of_active_wal(|_| vec![0x01, 0x80, 0xFF]);
}
