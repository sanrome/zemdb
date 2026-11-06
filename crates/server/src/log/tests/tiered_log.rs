use super::*;
use crate::dedup::DedupLruCache;
use crate::log::budget::PEAK;
use crate::log::io_probe::{self, IoEvent};
use crate::log::policy::{MAX_LOG_BYTES_LIMIT, MIN_LOG_BYTES_LIMIT};
use crate::log::test_support::{frames, make_op, owned, segment_file, seqs};
use crate::test_support::TestRng;
use std::fs::OpenOptions;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::wal_frame::{encode_wal_record, BATCH_HEADER_SIZE};
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

fn assert_contiguous<T: std::borrow::Borrow<SequencedOperation>>(ops: &[T], first: u64, last: u64) {
    let expected: Vec<u64> = (first..=last).collect();
    assert_eq!(seqs(ops), expected);
}

fn segments_dir(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("segments")
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// Bytes of the files in `segments` (segments, their mutation ID sidecars and `active.wal`),
/// read through each file: on Windows a listing can report a stale size for the open
/// `active.wal`.
fn bytes_on_disk(segments: &Path) -> u64 {
    std::fs::read_dir(segments)
        .unwrap()
        .map(|entry| file_len(&entry.unwrap().path()))
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

    log.run_maintenance().unwrap();

    assert_eq!(log.tail_seq().get(), 1);
}

#[test]
fn fetch_after_ram_ttl_eviction_bridges_from_active_wal() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), ttl_eviction_policy()).unwrap();

    append_plain(&mut log, 1, 10);
    sleep(Duration::from_millis(60));
    log.append(make_op(11), None).unwrap();
    log.run_maintenance().unwrap();

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();

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
    log.run_maintenance().unwrap();
    log
}

#[test]
fn fully_pruned_log_rejects_cursor_behind_head() {
    let dir = tempdir().unwrap();
    let log = fully_pruned_log(dir.path());

    assert_eq!(log.head_seq().get(), 5);
    assert_eq!(log.tail_seq().get(), 6);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(4), 100, None),
        Err(ServerError::BehindCompaction)
    ));
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(5), 100, None).unwrap();
    assert!(ops.is_empty());
    assert!(!has_more);
}

#[test]
fn append_after_full_prune_keeps_tail_on_new_op() {
    let dir = tempdir().unwrap();
    let mut log = fully_pruned_log(dir.path());

    log.append(make_op(6), None).unwrap();

    assert_eq!(log.tail_seq().get(), 6);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100, None).unwrap();
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
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100, None).unwrap();
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

/// Losing the newest sealed segment right after a rotation (no `active.wal` yet) leaves a gap,
/// never reusable sequences, also once everything before it is pruned.
#[test]
fn losing_the_newest_segment_never_reuses_its_sequences() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    append_range(&mut log, 1, 10);
    drop(log);
    std::fs::remove_file(segment_file(&segments_dir(dir.path()), 6, 10, "wal")).unwrap();

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    assert_eq!(log.head_seq().get(), 10);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(5), 100, None),
        Err(ServerError::BehindCompaction)
    ));
    log.prune_older_than(SequenceNumber::new(11)).unwrap();
    drop(log);

    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log.head_seq().get(), 10);
    assert!(log.append(make_op(6), None).is_err());
    log.append(make_op(11), None).unwrap();
}

/// A rotation that fails to record its end in `log_meta.json` has not renamed anything yet, so
/// losing the segment files afterwards cannot take `head_seq` back.
#[test]
fn rotation_that_fails_to_record_its_end_seals_nothing() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    append_range(&mut log, 1, 4);
    let meta = dir.path().join("log_meta.json");
    crate::fail_point::arm("write_atomic_before_rename", &meta);
    assert!(log
        .append(make_op(5), None)
        .unwrap()
        .rotation_error
        .is_some());
    drop(log);
    for ext in ["wal", "ids"] {
        let _ = std::fs::remove_file(segment_file(&segments_dir(dir.path()), 1, 5, ext));
    }

    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log.head_seq().get(), 5);
}

/// A rotation that recorded its end and stopped before the rename, followed by damage to the
/// last frame of `active.wal`, leaves `active.wal` ending below `head_seq` once the open
/// truncates that frame. The next commit must still be readable after a reopen, whether a
/// rotation by count seals it (`ram_max_ops` 6) or not (7), and the lost operation is a gap.
#[test]
fn commit_after_a_damaged_last_frame_below_the_sealed_end_survives_a_reopen() {
    for ram_max_ops in [6, 7] {
        let dir = tempdir().unwrap();
        let policy = RoomLifecyclePolicy {
            ram_max_ops,
            ..RoomLifecyclePolicy::default()
        };
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        append_range(&mut log, 1, 5);
        let active = segments_dir(dir.path()).join("active.wal");
        crate::fail_point::arm("exists", &active);
        assert!(log.force_rotate_warm().is_err());
        drop(log);
        let active_file = File::options()
            .read(true)
            .write(true)
            .open(&active)
            .unwrap();
        damage_payload(&active_file, 4);
        drop(active_file);

        // An open whose sealing fails seals nothing; the next one seals `active.wal`, without
        // lowering the recorded floor, as the open after it shows.
        crate::fail_point::arm(
            "write_atomic_before_rename",
            &dir.path().join(LOG_META_FILE),
        );
        assert!(TieredLog::open_or_create(dir.path(), policy.clone()).is_err());
        drop(TieredLog::open_or_create(dir.path(), policy.clone()).unwrap());
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        assert_eq!(log.head_seq().get(), 5, "5 is in log_meta.json");
        let outcome = log.append(make_op(6), Some(mutation_for(6))).unwrap();
        assert!(outcome.rotation_error.is_none());
        drop(log);

        let (log, mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();
        assert_eq!(log.head_seq().get(), 6);
        assert!(mutations.contains(&(mutation_for(6), SequenceNumber::new(6))));
        let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 10, None).unwrap();
        assert_eq!(seqs(&ops), vec![6], "ram_max_ops {ram_max_ops}");
        assert!(matches!(
            log.fetch_deltas(SequenceNumber::new(3), 10, None),
            Err(ServerError::BehindCompaction)
        ));
    }
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

/// Writes a log that sealed 1..=20 in segments of five, holds 21..=22 in `active.wal` and pruned
/// everything below 11, so that both fields of `log_meta.json` are set (10 and 20), closes it and
/// returns its policy.
fn write_pruned_log(dir: &Path) -> RoomLifecyclePolicy {
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir, policy.clone()).unwrap();
    append_range(&mut log, 1, 22);
    log.prune_older_than(SequenceNumber::new(11)).unwrap();
    policy
}

/// The names of the files in `segments` (segments, sidecars and `active.wal`), sorted.
fn segment_file_names(segments: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(segments)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// One bit flipped in `log_meta.json` keeps it valid JSON but raises the recorded pruning point:
/// the open fails instead of deleting segments that were never pruned.
#[test]
fn reopen_with_damaged_log_meta_fails_without_deleting_anything() {
    let dir = tempdir().unwrap();
    let policy = write_pruned_log(dir.path());
    let meta = dir.path().join("log_meta.json");
    let mut bytes = std::fs::read(&meta).unwrap();
    let at = bytes.windows(2).position(|w| w == b"10").unwrap();
    bytes[at] ^= 0b10; // "10" -> "30"
    std::fs::write(&meta, bytes).unwrap();

    assert!(TieredLog::open_or_create(dir.path(), policy).is_err());
    assert!(segment_file(&segments_dir(dir.path()), 11, 15, "wal").exists());
}

/// Every single bit flip and every truncation of `log_meta.json` makes the open fail, and the
/// failed open deletes nothing: not even a flip that keeps the JSON valid and raises
/// `pruned_through_seq` above the retained segments.
#[test]
fn every_damage_to_log_meta_fails_the_open_without_deleting_anything() {
    let dir = tempdir().unwrap();
    let policy = write_pruned_log(dir.path());
    let segments = segments_dir(dir.path());
    let meta = dir.path().join("log_meta.json");
    let original = std::fs::read(&meta).unwrap();
    let files = segment_file_names(&segments);
    assert!(files.contains(&"segment_0000000000000011_0000000000000015.wal".to_string()));

    for (damage, bytes) in crate::test_support::every_truncation_and_bit_flip(&original) {
        std::fs::write(&meta, &bytes).unwrap();
        let opened = TieredLog::open_or_create(dir.path(), policy.clone());
        assert!(
            opened.is_err(),
            "a log_meta.json {damage} ({:?}) opened",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(
            segment_file_names(&segments),
            files,
            "an open with a log_meta.json {damage} deleted files"
        );
    }

    // Verified, but missing a field or holding the wrong type: no default fills it in.
    for json in [
        r#"{"pruned_through_seq":10}"#,
        r#"{"sealed_through_seq":20}"#,
        r#"{"pruned_through_seq":"10","sealed_through_seq":20}"#,
        r#"{"pruned_through_seq":-1,"sealed_through_seq":20}"#,
    ] {
        crate::durable::write_checked(&meta, json.as_bytes()).unwrap();
        assert!(
            TieredLog::open_or_create(dir.path(), policy.clone()).is_err(),
            "a log_meta.json holding {json} opened"
        );
        assert_eq!(
            segment_file_names(&segments),
            files,
            "an open with a log_meta.json holding {json} deleted files"
        );
    }

    std::fs::write(&meta, &original).unwrap();
    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!((log.tail_seq().get(), log.head_seq().get()), (11, 22));
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
    let write_meta = |pruned: u64, sealed: u64| {
        let json = format!(r#"{{"pruned_through_seq": {pruned}, "sealed_through_seq": {sealed}}}"#);
        crate::durable::write_checked(&meta, json.as_bytes()).unwrap();
    };
    for (pruned, sealed) in [(u64::MAX, 0), (0, u64::MAX)] {
        write_meta(pruned, sealed);
        assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());
        assert!(segment.exists(), "the failed open deleted a segment");
    }

    write_meta(u64::MAX - 1, 0);
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
        log.fetch_deltas(SequenceNumber::new(0), 100, None),
        Err(ServerError::BehindCompaction)
    ));
}

#[test]
fn open_makes_new_segments_directory_durable() {
    let dir = tempdir().unwrap();
    crate::fail_point::arm("sync_dir", dir.path());

    assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());
}

/// An earlier actor or process may have written `active.wal` and created the directories of the
/// log without a successful sync: opening syncs all of them before anything is served.
#[test]
fn open_syncs_active_wal_and_every_directory_on_its_path() {
    let root = tempdir().unwrap();
    let dir = root.path().join("rooms").join("room");
    let active = write_log(&dir, 3);
    let points = [
        ("warm_open_sync", active),
        ("sync_dir", segments_dir(&dir)),
        ("sync_dir", dir.clone()),
        ("sync_dir", root.path().join("rooms")),
        ("sync_dir", root.path().to_path_buf()),
    ];
    for (point, path) in points {
        crate::fail_point::arm(point, &path);
        assert!(
            TieredLog::open_or_create(&dir, RoomLifecyclePolicy::default()).is_err(),
            "{point} of {path:?}"
        );
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
        log.run_maintenance().unwrap();
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
    log.run_maintenance().unwrap();
    append_range(&mut log, 11, 17);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_contiguous(&ops, 1, 17);
    log.prune_older_than(SequenceNumber::new(6)).unwrap();
    log.run_maintenance().unwrap();

    assert_eq!(io_probe::count(IoEvent::Listing, &segments), after_open);
    assert_eq!(log.tail_seq().get(), 6);
}

/// Ten cold segments of five operations each, then 51..=53 in active.wal.
fn fifty_three_compressed_ops(dir: &Path) {
    let (mut log, _) = TieredLog::open_or_create(dir, compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 50);
    log.run_maintenance().unwrap();
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
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
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

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
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

// Paused time: the Tokio clock runs ahead of the system's monotonic clock once the test
// advances it, so a window measured on the system clock would expire at once or never.
#[tokio::test(start_paused = true)]
async fn the_ram_window_expires_on_the_tokio_clock() {
    let ttl = Duration::from_secs(60);
    let just_before = ttl - Duration::from_millis(1);
    tokio::time::advance(Duration::from_secs(3600)).await;
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_ttl: ttl,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_range(&mut log, 1, 5);

    // Maintenance and appends keep what has not been in RAM for the whole window...
    tokio::time::advance(just_before).await;
    log.run_maintenance().unwrap();
    append_range(&mut log, 6, 6);
    assert_eq!(log.hot_buffer.len(), 6);

    // ...and drop it once it has: an append, then maintenance with no append.
    tokio::time::advance(Duration::from_millis(1)).await;
    append_range(&mut log, 7, 7);
    assert_eq!(log.hot_buffer.min_seq(), Some(SequenceNumber::new(6)));
    tokio::time::advance(just_before).await;
    log.run_maintenance().unwrap();
    assert_eq!(log.hot_buffer.min_seq(), Some(SequenceNumber::new(7)));
    assert_eq!(log.hot_buffer.len(), 1);
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

    log.run_maintenance_at(MaintenanceClock::after(Duration::from_secs(30)))
        .unwrap();
    assert_eq!(log.hot_buffer.len(), 5);

    log.run_maintenance_at(MaintenanceClock::after(Duration::from_secs(61)))
        .unwrap();
    assert!(log.hot_buffer.is_empty());
    assert_eq!(log.tail_seq().get(), 1);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
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
        .run_maintenance_at(MaintenanceClock::after(Duration::from_secs(59)))
        .unwrap();
    assert_eq!(report.warm_compressed_count, 0);

    // Compressed one minute after sealing; the cold TTL counts from the compression.
    let compressed_at = Duration::from_secs(61);
    let report = log
        .run_maintenance_at(MaintenanceClock::after(compressed_at))
        .unwrap();
    assert_eq!(report.warm_compressed_count, 1);
    assert_eq!(report.pruned_count, 0);

    let just_before = compressed_at + Duration::from_secs(3599);
    let report = log
        .run_maintenance_at(MaintenanceClock::after(just_before))
        .unwrap();
    assert_eq!(report.pruned_count, 0);
    assert_eq!(log.tail_seq().get(), 1);

    let past_ttl = compressed_at + Duration::from_secs(3601);
    let report = log
        .run_maintenance_at(MaintenanceClock::after(past_ttl))
        .unwrap();
    assert_eq!(report.pruned_count, 1);
    assert_eq!(log.tail_seq().get(), 6);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(2), 10, None),
        Err(ServerError::BehindCompaction)
    ));
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10, None).unwrap();
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
    log.run_maintenance().unwrap();
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
    log.run_maintenance().unwrap();
    let total = log.disk_bytes();
    let oldest_cold = log.segments.iter().next().unwrap().disk_bytes();

    // One byte under the current size: dropping the oldest cold segment is enough.
    log.policy.max_room_disk_bytes = total - 1;
    let report = log.run_maintenance().unwrap();

    assert_eq!(report.pruned_count, 1);
    assert_eq!(log.tail_seq().get(), 6);
    assert_eq!(log.disk_bytes(), total - oldest_cold);
}

#[test]
fn quota_bounds_the_disk_of_a_sustained_writer() {
    // Warm segments not due for compression yet, and due at once; either way more of them per
    // pass than a pass compresses. Over the quota, a pass compresses as many as it can.
    for warm_disk_ttl in [RoomLifecyclePolicy::default().warm_disk_ttl, Duration::ZERO] {
        let dir = tempdir().unwrap();
        let policy = RoomLifecyclePolicy {
            warm_disk_ttl,
            ..compress_at_once_policy()
        };
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
        append_range(&mut log, 1, 5);
        let quota = 3 * log.disk_bytes();
        log.policy.max_room_disk_bytes = quota;
        for round in 0..5 {
            append_range(&mut log, round * 30 + 6, round * 30 + 35);
            let report = log.run_maintenance().unwrap();
            assert_eq!(report.warm_compressed_count, MAX_COMPRESSIONS_PER_PASS);
            let on_disk = bytes_on_disk(&segments_dir(dir.path()));
            assert!(
                on_disk <= quota + log.warm_disk.active_len(),
                "warm TTL {warm_disk_ttl:?}, round {round}: {on_disk} bytes over a quota of {quota}"
            );
        }
    }
}

#[test]
fn open_keeps_the_warm_original_when_its_cold_copy_does_not_verify() {
    let dir = tempdir().unwrap();
    {
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 1, 5);
    }
    // A cold copy next to its warm original, damaged so that it lacks the last operation.
    let segments = segments_dir(dir.path());
    let warm = segment_file(&segments, 1, 5, "wal");
    let cold = segment_file(&segments, 1, 5, "wal.zst");
    let warm_bytes = std::fs::read(&warm).unwrap();
    let torn = &warm_bytes[..warm_bytes.len() - 1];
    std::fs::write(&cold, zstd::stream::encode_all(torn, 3).unwrap()).unwrap();

    let (mut log, mutations) =
        TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();

    assert!(warm.exists());
    assert!(!cold.exists());
    assert_eq!(mutations.len(), 5);
    assert_eq!(log.run_maintenance().unwrap().warm_compressed_count, 1);
    assert!(cold.exists());
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_contiguous(&ops, 1, 5);
}

/// A compression whose removal of the warm original fails twice leaves both copies; if the
/// warm one is then damaged, opening keeps the verified cold copy and removes the warm one.
#[test]
fn open_never_drops_a_verified_cold_copy_for_a_damaged_warm_one() {
    for damaged_frame in [1, 4] {
        let dir = tempdir().unwrap();
        let (mut log, _) =
            TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        append_range(&mut log, 1, 7);
        let segments = segments_dir(dir.path());
        let warm = segment_file(&segments, 1, 5, "wal");
        let cold = segment_file(&segments, 1, 5, "wal.zst");
        // The removal after the compression, and its retry in the same pass.
        crate::fail_point::arm("remove_file", &warm);
        crate::fail_point::arm("remove_file", &warm);
        log.run_maintenance().unwrap();
        drop(log);
        let warm_file = File::options().read(true).write(true).open(&warm).unwrap();
        damage_payload(&warm_file, damaged_frame);
        drop(warm_file);
        // An I/O error reading the cold copy proves nothing: the open fails and removes neither.
        crate::fail_point::arm("open_segment", &cold);
        assert!(TieredLog::open_or_create(dir.path(), compress_at_once_policy()).is_err());
        assert!(cold.exists() && warm.exists());

        PEAK.set(0);
        let (log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        assert!(cold.exists() && !warm.exists(), "frame {damaged_frame}");
        // Verifying the cold copy keeps no more at once than the open keeps in RAM.
        assert!(PEAK.get() <= log.hot_buffer.bytes());
        let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
        assert_contiguous(&ops, 1, 7);
    }
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
    let _ = log.run_maintenance();

    assert!(cold.exists() && !warm.exists());
    let report = log.run_maintenance().unwrap();
    assert_eq!(report.warm_compressed_count, 0);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
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
    log.run_maintenance().unwrap();
    assert!(warm.exists());
    log.run_maintenance().unwrap();
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
    log.run_maintenance().unwrap();
    log.run_maintenance_at(expired).unwrap();
    assert_eq!(log.tail_seq().get(), 6);
    assert!(warm.exists());
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log.tail_seq().get(), 6);
    assert!(!warm.exists());
}

#[test]
fn quota_prunes_a_prefix_even_with_a_warm_segment_that_cannot_be_compressed() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 12);
    let segments = segments_dir(dir.path());

    // Segment 1..=5 cannot be compressed, so it stays warm in front of the cold 6..=10.
    std::fs::write(segment_file(&segments, 1, 5, "wal"), b"damaged").unwrap();
    let _ = log.run_maintenance();
    log.policy.max_room_disk_bytes = log.disk_bytes() - 1;
    let _ = log.run_maintenance();

    // Pruning the warm segment is enough; the cold one behind it stays.
    assert!(!segment_file(&segments, 1, 5, "wal").exists());
    assert!(segment_file(&segments, 6, 10, "wal.zst").exists());
    assert_eq!(log.tail_seq().get(), 6);
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
    log.run_maintenance().unwrap();
    append_range(&mut log, 6, 12);

    // Compressing segment 6..=10 fails, while segment 1..=5 is past its cold TTL and every
    // operation in RAM is past the RAM TTL.
    crate::fail_point::arm("sync_dir", &segments_dir(dir.path()));
    let result = log.run_maintenance_at(MaintenanceClock::after(Duration::from_secs(7200)));

    assert!(result.is_err());
    assert_eq!(log.tail_seq().get(), 6);
    assert!(log.hot_buffer.is_empty());
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100, None).unwrap();
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
    let (ops, _) = log
        .fetch_deltas(SequenceNumber::new(10), 100, None)
        .unwrap();
    assert_contiguous(&ops, 11, 11);
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(8), 100, None),
        Err(ServerError::BehindCompaction)
    ));

    // A name that claims an absurd range: the size of its sidecar, counted from the range,
    // saturates instead of overflowing. The segment cannot be compressed, and the quota prunes it.
    let dir = tempdir().unwrap();
    drop(TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap());
    shorten_warm_segment(dir.path(), 1, u64::MAX / 2, 2);
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    log.append(large_op(u64::MAX / 2 + 1, 1), None).unwrap();
    let maintenance = log.run_maintenance();
    assert!(
        matches!(maintenance, Err(ServerError::WalCorruption(_))),
        "{maintenance:?}"
    );
    assert_eq!(log.tail_seq().get(), u64::MAX / 2 + 1);
}

#[test]
fn missing_segment_at_the_end_of_the_disk_tiers_is_behind_compaction() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 10);
    // Nothing in RAM or active.wal covers the range any more.
    log.run_maintenance_at(MaintenanceClock::after(Duration::from_secs(600)))
        .unwrap();
    assert!(log.hot_buffer.is_empty());
    std::fs::remove_file(segment_file(&segments_dir(dir.path()), 6, 10, "wal.zst")).unwrap();

    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(5), 100, None),
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
        log.fetch_deltas(SequenceNumber::new(0), 100, None),
        Err(ServerError::BehindCompaction)
    ));
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(5), 100, None).unwrap();
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
    // The damaged segment is skipped; the older ones still hydrate from their sidecars.
    let recovered: Vec<u64> = mutations.iter().map(|(_, s)| s.get()).collect();
    assert_eq!(recovered, vec![1, 2, 3, 4, 5, 11, 12]);
    assert_eq!(log.hot_buffer.min_seq().map(|s| s.get()), Some(11));
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(5), 100, None),
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

/// Values of [`null_row_op`]: its key and a row of 4,096 columns.
const NULL_ROW_VALUES: usize = 1 + 4_096;

/// An operation whose row is all `Null` but its key: 1 byte per value on the wire, 24 in
/// memory.
fn null_row_op(seq: u64) -> SequencedOperation {
    let mut values = vec![Value::Null; NULL_ROW_VALUES - 1];
    values[0] = Value::Int(seq as i64);
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    let row = zemdb_core::value::CompactRow::new(values);
    SequencedOperation::new(SequenceNumber::new(seq), Operation::insert(1, pk, row, seq))
}

/// 300 operations of [`null_row_op`]: they take 1.2 MiB on the wire, and over a million values.
fn null_rows_log(dir: &Path, ram_max_bytes: u64) -> TieredLog {
    let policy = RoomLifecyclePolicy {
        ram_max_bytes,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir, policy).unwrap();
    for seq in 1..=300 {
        log.append(null_row_op(seq), None).unwrap();
    }
    log
}

/// Asserts that `ops`, of [`null_row_op`], hold at most `MAX_MESSAGE_VALUES` values and are
/// not the whole log.
fn assert_within_the_values_of_a_message((ops, has_more): (Vec<Arc<SequencedOperation>>, bool)) {
    let values = (ops.len() * NULL_ROW_VALUES) as u64;
    assert!(
        values <= zemdb_core::protocol::limits::MAX_MESSAGE_VALUES,
        "{values} values"
    );
    assert!(has_more);
}

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
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_contiguous(&ops, 1, 10);
    assert!(!has_more);

    // Rows of `Null`s count what their values take in memory.
    let dir = tempdir().unwrap();
    let log = null_rows_log(dir.path(), MIB as u64);
    assert!(log.hot_buffer.len() * NULL_ROW_VALUES * 24 <= MIB);
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
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
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

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();

    assert_contiguous(&ops, 1, 2);
    assert!(has_more);

    // However little its values take on the wire, a response does not materialize more than
    // a client message could.
    let dir = tempdir().unwrap();
    let log = null_rows_log(dir.path(), MAX_LOG_BYTES_LIMIT);
    assert_eq!(log.hot_buffer.len(), 300);
    assert_within_the_values_of_a_message(
        log.fetch_deltas(SequenceNumber::new(0), 1000, None)
            .unwrap(),
    );
}

#[test]
fn fetch_returns_what_fits_in_a_response_from_disk() {
    let dir = tempdir().unwrap();
    let log = large_ops_log(dir.path(), MIN_LOG_BYTES_LIMIT);
    assert!(log.hot_buffer.is_empty());

    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(2), 100, None).unwrap();

    assert_contiguous(&ops, 3, 4);
    assert!(has_more);

    let dir = tempdir().unwrap();
    let log = null_rows_log(dir.path(), MIN_LOG_BYTES_LIMIT);
    assert!(log.hot_buffer.len() < 300);
    assert_within_the_values_of_a_message(
        log.fetch_deltas(SequenceNumber::new(0), 1000, None)
            .unwrap(),
    );
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

    log.run_maintenance().unwrap();

    assert_eq!(count_in(&log, Tier::Cold), 1);
    let (read, has_more) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_eq!(owned(read), ops);
    assert!(!has_more);
}

#[test]
fn maintenance_compresses_a_bounded_number_of_segments_per_pass() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    // Six sealed segments of five operations.
    append_range(&mut log, 1, 30);
    assert_eq!(count_in(&log, Tier::Warm), 6);

    let report = log.run_maintenance().unwrap();
    assert_eq!(report.warm_compressed_count, MAX_COMPRESSIONS_PER_PASS);
    assert_eq!(count_in(&log, Tier::Warm), 2);

    let report = log.run_maintenance().unwrap();
    assert_eq!(report.warm_compressed_count, 2);
    assert_eq!(count_in(&log, Tier::Cold), 6);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_contiguous(&ops, 1, 30);
}

#[tokio::test]
async fn async_maintenance_compresses_a_bounded_number_of_segments_per_pass() {
    let dir = tempdir().unwrap();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
    append_range(&mut log, 1, 30);

    let report = log.run_maintenance().unwrap();

    assert_eq!(report.warm_compressed_count, MAX_COMPRESSIONS_PER_PASS);
    assert_eq!(count_in(&log, Tier::Warm), 2);
}

/// Reopening keeps at once no more than the RAM window, charged as RAM charges it (a `Null`
/// takes 24 times its wire size): of `active.wal` it keeps only the newest operations, and of a
/// segment whose sidecar is missing, only its mutation IDs.
#[test]
fn reopening_loads_into_ram_only_what_fits() {
    for sidecars in [true, false] {
        let dir = tempdir().unwrap();
        let policy = RoomLifecyclePolicy {
            // Segment 1..=30, and 31..=50 in active.wal: each takes more than RAM.
            ram_max_ops: 30,
            ram_max_bytes: MIB as u64,
            ..RoomLifecyclePolicy::default()
        };
        {
            let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
            for seq in 1..=50 {
                log.append(null_row_op(seq), Some(mutation_for(seq)))
                    .unwrap();
            }
        }
        if !sidecars {
            let (start, end) = (SequenceNumber::new(1), SequenceNumber::new(30));
            std::fs::remove_file(mutation_ids_path(&segments_dir(dir.path()), start, end)).unwrap();
        }

        PEAK.set(0);
        let (log, mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();

        assert!(
            PEAK.get() <= MIB as u64,
            "sidecars {sidecars}: {}",
            PEAK.get()
        );
        assert!(log.hot_buffer.bytes() <= MIB as u64);
        assert_eq!(log.hot_buffer.max_seq(), Some(SequenceNumber::new(50)));
        assert_eq!(
            log.warm_disk.active_start_seq(),
            Some(SequenceNumber::new(31))
        );
        assert_eq!(mutations.len(), 50);
    }
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
    let (_warm, active) = WarmDiskLog::open_recovering(segments_dir(dir.path()), &policy).unwrap();

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
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(4), 100, None).unwrap();
    assert_contiguous(&ops, 5, 12);
    assert!(!has_more);

    // Through the handle that holds the lock: Windows refuses any other.
    damage_payload(log.warm_disk.active_file().unwrap(), 0);
    let (ops, _) = log
        .fetch_deltas(SequenceNumber::new(11), 100, None)
        .unwrap();
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
    assert!(log.run_maintenance().is_err());
    assert_eq!(count_in(&log, Tier::Cold), 0);
    assert!(log.run_maintenance().is_err());
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

    assert!(log.run_maintenance().is_err());
    assert_eq!(log.failed_compressions.len(), MAX_COMPRESSIONS_PER_PASS);

    // The two not tried yet go first, then two of those that failed.
    assert!(log.run_maintenance().is_err());
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
        assert!(log.run_maintenance().is_err());
    }
    assert_eq!(log.failed_compressions.len(), 6);

    // The newest segment recovers; the four oldest keep failing.
    std::fs::write(&paths[5], newest).unwrap();
    for _ in 0..2 {
        let _ = log.run_maintenance();
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

    let (deltas, has_more) = log2
        .fetch_deltas(SequenceNumber::new(0), 100, None)
        .unwrap();

    assert_eq!(owned(deltas), (1..=10).map(make_op).collect::<Vec<_>>());
    assert!(!has_more);
}

#[test]
fn test_tiered_log_hot_buffer_fast_read() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();

    append_plain(&mut log, 1, 5);

    let (batch1, has_more1) = log.fetch_deltas(SequenceNumber::new(2), 2, None).unwrap();
    assert_contiguous(&batch1, 3, 4);
    assert!(has_more1);

    let (batch2, has_more2) = log.fetch_deltas(SequenceNumber::new(4), 10, None).unwrap();
    assert_contiguous(&batch2, 5, 5);
    assert!(!has_more2);

    // Read from current head (should return empty)
    let (batch3, has_more3) = log.fetch_deltas(SequenceNumber::new(5), 10, None).unwrap();
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
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10, None).unwrap();

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
    let report = log.run_maintenance().unwrap();
    assert_eq!(report.warm_compressed_count, 1);

    // Sealed .wal should be removed, and .wal.zst created
    assert!(!sealed_wal.exists());
    assert!(segment_file(&segments, 1, 5, "wal.zst").exists());

    // Fetch deltas directly from Cold Disk (.wal.zst)
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 10, None).unwrap();

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
    log.run_maintenance().unwrap();

    // Operations 6..=10 are sealed to warm, without compressing them.
    append_plain(&mut log, 6, 10);
    log.force_rotate_warm().unwrap();

    // Operations 11..=15 stay in RAM and active.wal.
    append_plain(&mut log, 11, 15);

    assert_eq!(log.head_seq().get(), 15);

    // Fetch entire range 0..15 traversing Cold -> Warm -> Hot in a single contiguous batch
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(0), 20, None).unwrap();

    assert_contiguous(&deltas, 1, 15);
    assert!(!has_more);

    // Paged fetch crossing from Cold (ends at 5) into Warm (starts at 6)
    let (paged, paged_has_more) = log.fetch_deltas(SequenceNumber::new(3), 5, None).unwrap();

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
    let result = log.fetch_deltas(SequenceNumber::new(2), 10, None);
    assert!(
        matches!(result, Err(ServerError::BehindCompaction)),
        "{result:?}"
    );

    // Cursor 5 can fetch 6..=10 smoothly
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(5), 10, None).unwrap();
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
        let (deltas, has_more) = log
            .fetch_deltas(SequenceNumber::new(1), limit, None)
            .unwrap();
        assert_contiguous(&deltas, 2, 6);
        assert!(!has_more, "limit {limit}");
    }

    // Fetching from cursor 4 returns 5 and 6
    let (recent_deltas, recent_has_more) =
        log.fetch_deltas(SequenceNumber::new(4), 10, None).unwrap();
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
    let report = log.run_maintenance().unwrap();
    assert_eq!(report.warm_compressed_count, 2);
    assert!(
        report.pruned_count >= 1,
        "Quota saturation must prune at least one cold segment"
    );

    // tail_seq must have advanced past 1 (at least to 6)
    assert!(
        log.tail_seq().get() >= 6,
        "tail_seq must advance after quota pruning, got {}",
        log.tail_seq().get()
    );

    // Any fetch before the new tail_seq must return BehindCompaction
    let result = log.fetch_deltas(SequenceNumber::new(0), 10, None);
    assert!(
        matches!(result, Err(ServerError::BehindCompaction)),
        "Expected BehindCompaction on quota pruned deltas, got: {result:?}"
    );

    // Fetch from the new tail cursor succeeds
    let cursor = SequenceNumber::new(log.tail_seq().get() - 1);
    let (deltas, _) = log.fetch_deltas(cursor, 10, None).unwrap();
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
    let (deltas, has_more) = log.fetch_deltas(SequenceNumber::new(7), 6, None).unwrap();

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

    let (deltas, has_more) = log2
        .fetch_deltas(SequenceNumber::new(0), 100, None)
        .unwrap();
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
        let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(at), batch, None)?;
        assert!(ops.len() <= batch as usize, "page over its size from {at}");
        if at < log.head_seq().get() {
            assert!(!ops.is_empty(), "empty page below the head from {at}");
        }
        let Some(last) = ops.last() else {
            assert!(!has_more, "has_more without progress from {at}");
            break;
        };
        at = last.seq.get();
        out.extend(owned(ops));
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
                log.fetch_deltas(SequenceNumber::new(c), 5, None),
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
                    log.run_maintenance_at(random_clock(&mut rng)).unwrap();
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

/// A read starts at the right sealed segment at every edge of the index (the segment that holds
/// `cursor + 1`): a cursor at the end of a segment, at the edges of and inside a lost segment,
/// at the tail and at the head. Every read from disk is the exact suffix, or `BehindCompaction`
/// when the suffix crosses the lost segment.
#[test]
fn reads_start_at_the_right_segment_at_every_edge() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        append_range(&mut log, 1, 23);
    }
    let accepted: Vec<SequencedOperation> = (1..=23).map(make_op).collect();
    let starts =
        |log: &TieredLog| -> Vec<u64> { log.segments.iter().map(|s| s.start_seq.get()).collect() };

    // Reopened, RAM holds only `active.wal` (21..=23): every other read goes to disk.
    let (log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
    assert_eq!(starts(&log), vec![1, 6, 11, 16]);
    for c in 0..=23u64 {
        for batch in [1, 3, 100] {
            let got = read_chain(&log, c, || batch).unwrap();
            assert!(
                got.as_slice() == &accepted[c as usize..],
                "a read from cursor {c} in pages of {batch} is not its exact suffix: {:?}",
                seqs(&got)
            );
        }
    }
    drop(log);

    // Segment 11..=15 is lost.
    let segments = segments_dir(dir.path());
    std::fs::remove_file(segment_file(&segments, 11, 15, "wal")).unwrap();
    std::fs::remove_file(segment_file(&segments, 11, 15, "ids")).unwrap();
    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(starts(&log), vec![1, 6, 16]);
    assert_eq!((log.tail_seq().get(), log.head_seq().get()), (1, 23));
    for c in 0..=23u64 {
        for batch in [1, 3, 100] {
            let got = read_chain(&log, c, || batch);
            if c < 15 {
                assert!(
                    matches!(got, Err(ServerError::BehindCompaction)),
                    "a read from cursor {c} in pages of {batch} crosses the lost segment and \
                     must be BehindCompaction: {got:?}"
                );
            } else {
                let got = got.unwrap();
                assert!(
                    got.as_slice() == &accepted[c as usize..],
                    "a read from cursor {c} in pages of {batch} is not its exact suffix: {:?}",
                    seqs(&got)
                );
            }
        }
    }
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
            log.run_maintenance().unwrap();
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
                match log.fetch_deltas(SequenceNumber::new(at), batch, None) {
                    Ok((ops, has_more)) => {
                        let n = ops.len() as u64;
                        assert!(
                            owned(ops) == accepted[at as usize..(at + n) as usize],
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
                        log.run_maintenance_at(random_clock(&mut rng)).unwrap();
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

/// Where a fail point can be armed in the log at `dir` right now: the files every step touches,
/// the files of each indexed segment, and those of the next rotation.
fn fail_points_of(log: &TieredLog, dir: &Path) -> Vec<(&'static str, PathBuf)> {
    let segments = segments_dir(dir);
    let active = segments.join("active.wal");
    let mut points = vec![
        ("warm_append_write", active.clone()),
        ("warm_append_sync", active.clone()),
        ("warm_open_sync", active.clone()),
        ("exists", active.clone()),
        ("sync_dir", segments.clone()),
        ("sync_dir", dir.to_path_buf()),
        ("write_atomic_before_rename", dir.join("log_meta.json")),
    ];
    for segment in log.segments.iter() {
        let (start, end) = (segment.start_seq.get(), segment.end_seq.get());
        points.push(("remove_file", segment.path.clone()));
        points.push(("remove_file", segment_file(&segments, start, end, "ids")));
        if segment.tier == Tier::Warm {
            points.push(("compress_after_warm_removed", segment.path.clone()));
            points.push((
                "remove_file",
                segment_file(&segments, start, end, "wal.zst.tmp"),
            ));
        }
    }
    if let (Some(start), Some(end)) = (
        log.warm_disk.active_start_seq(),
        log.warm_disk.active_end_seq(),
    ) {
        for end in [end.get(), start.get() + log.policy.ram_max_ops as u64 - 1] {
            points.push((
                "write_atomic_before_rename",
                segment_file(&segments, start.get(), end, "ids"),
            ));
        }
    }
    points
}

/// Opens the log at `dir`, retrying while an armed fail point makes the open fail (a crash
/// during the recovery of a crash).
fn open_retrying(
    dir: &Path,
    policy: &RoomLifecyclePolicy,
    step: &str,
) -> (TieredLog, Vec<(MutationId, SequenceNumber)>) {
    let mut last = None;
    for _ in 0..20 {
        match TieredLog::open_or_create(dir, policy.clone()) {
            Ok(opened) => return opened,
            Err(e) => last = Some(e),
        }
    }
    panic!("{step}: the log no longer opens: {last:?}");
}

/// For each seed, `steps` random steps on a log with small random limits (appends, rotations,
/// maintenance at random ages and over the quota, cursor-driven prunes and reopens), with a
/// random fail point armed before about a third of the steps. The room reacts as the actor
/// does: a failed append or rotation stops it (reopen), a failed maintenance pass or prune is
/// only logged. After every step: the head is the number of acknowledged commits (an append
/// whose error came after its record reached the file counts once the reopen synced it), the
/// tail never moves back, every acknowledged commit at or above the tail is read back exactly
/// from any cursor, and after a reopen the mutation IDs are exactly those of the retained
/// commits.
fn simulate_failures(seeds: std::ops::Range<u64>, steps: usize) {
    for seed in seeds {
        let mut rng = TestRng::new(seed.wrapping_mul(0xD1B5_4A32_D192_ED03) + 7);
        let dir = tempdir().unwrap();
        let policy = RoomLifecyclePolicy {
            ram_max_ops: 1 + rng.below(6) as usize,
            ram_max_bytes: 2_000 + rng.below(10_000),
            segment_max_bytes: 1_500 + rng.below(8_000),
            ram_ttl: Duration::from_secs(60),
            warm_disk_ttl: Duration::from_secs(if rng.below(2) == 0 { 0 } else { 600 }),
            cold_disk_ttl: Duration::from_secs(3_600),
            max_room_disk_bytes: if rng.below(2) == 0 {
                15_000 + rng.below(20_000)
            } else {
                u64::MAX / 4
            },
            ..RoomLifecyclePolicy::default()
        };
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        let mut acked: Vec<SequencedOperation> = Vec::new();
        let mut last_tail = log.tail_seq().get();
        for step in 0..steps {
            let context = format!("seed {seed} step {step}");
            if rng.below(3) == 0 {
                let points = fail_points_of(&log, dir.path());
                let (point, path) = &points[rng.index(points.len())];
                crate::fail_point::arm(point, path);
            }
            let mut pending: Option<SequencedOperation> = None;
            let mut stop = false;
            match rng.below(12) {
                0..=5 => {
                    for _ in 0..1 + rng.below(6) {
                        let op = random_size_op(acked.len() as u64 + 1, &mut rng);
                        let id = mutation_for(op.seq.get());
                        match log.append(op.clone(), Some(id)) {
                            Ok(outcome) => {
                                acked.push(op);
                                if outcome.rotation_error.is_some() {
                                    stop = true;
                                    break;
                                }
                            }
                            Err(_) => {
                                pending = Some(op);
                                stop = true;
                                break;
                            }
                        }
                    }
                }
                6 | 7 => {
                    let _ = log.run_maintenance_at(random_clock(&mut rng));
                }
                8 => {
                    let target = 1 + rng.below(log.head_seq().get().max(1));
                    let _ = log.prune_older_than(SequenceNumber::new(target));
                }
                9 => {
                    stop = log.force_rotate_warm().is_err();
                }
                _ => stop = true,
            }

            let mut reopened = None;
            if stop {
                drop(log);
                let (opened, mutations) = open_retrying(dir.path(), &policy, &context);
                log = opened;
                if let Some(op) = pending.take() {
                    if log.head_seq().get() == op.seq.get() {
                        // The record reached the file before the error and the reopen synced
                        // it, so a retry is acknowledged with this sequence.
                        acked.push(op);
                    }
                }
                reopened = Some(mutations);
            }

            let head = log.head_seq().get();
            assert_eq!(
                head,
                acked.len() as u64,
                "{context}: the head is not the number of acknowledged commits"
            );
            let tail = log.tail_seq().get();
            assert!(
                tail >= last_tail,
                "{context}: tail moved back from {last_tail} to {tail}"
            );
            last_tail = tail;
            check_reads(&log, &acked, &mut rng, &context);
            if let Some(mutations) = reopened {
                for (id, seq) in &mutations {
                    assert_eq!(
                        *id,
                        mutation_for(seq.get()),
                        "{context}: the mutation id at {seq} is not the acknowledged one"
                    );
                    assert!(
                        seq.get() >= 1 && seq.get() <= head,
                        "{context}: a mutation id at {seq}, outside 1..={head}"
                    );
                }
                let found: HashSet<u64> = mutations.iter().map(|(_, seq)| seq.get()).collect();
                for seq in tail..=head {
                    assert!(
                        found.contains(&seq),
                        "{context}: the mutation id of retained commit {seq} was lost"
                    );
                }
            }
        }
    }
}

#[test]
fn random_failures_and_reopens_keep_every_acknowledged_commit() {
    simulate_failures(0..2, 25);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_failures_and_reopens_keep_every_acknowledged_commit_over_many_seeds() {
    simulate_failures(0..100, 120);
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
        .fetch_deltas(SequenceNumber::new(tail - 1), 100, None)
        .unwrap();
    assert_contiguous(&ops, tail, 13);
    drop(log);

    let (log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log.tail_seq().get(), 10);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(9), 100, None).unwrap();
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
            let flip = format!("flip {mask:#04x} at {position} (frame {frame})");
            // An open log holds the lock on `active.wal`, which on Windows keeps other handles
            // from reading it, so the file is read once the log is gone.
            let read_active = || std::fs::read(segments.join("active.wal")).unwrap();
            if frame < 2 {
                assert!(opened.is_err(), "{flip}: corruption must fail the open");
                assert_eq!(read_active(), bytes, "{flip}: nothing may be truncated");
            } else {
                let (log, _) = opened.unwrap_or_else(|e| panic!("{flip}: {e}"));
                assert_eq!(log.head_seq().get(), 2, "{flip}");
                drop(log);
                let on_disk = read_active();
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

/// Measurement (release, with `--ignored`): a read near the head costs the same whatever the
/// number of sealed segments before it.
#[test]
#[ignore]
fn fetch_cost_does_not_grow_with_the_number_of_segments() {
    let cost = |segments: u64| {
        let dir = tempdir().unwrap();
        let segs = segments_dir(dir.path());
        std::fs::create_dir_all(&segs).unwrap();
        for start in (1..=segments * 2).step_by(2) {
            let (bytes, _) = frames(start..=start + 1);
            std::fs::write(segment_file(&segs, start, start + 1, "wal"), bytes).unwrap();
        }
        let (log, _) = TieredLog::open_or_create(dir.path(), compress_at_once_policy()).unwrap();
        let cursor = SequenceNumber::new(segments * 2 - 1);
        let started = std::time::Instant::now();
        for _ in 0..1_000 {
            log.fetch_deltas(cursor, 10, None).unwrap();
        }
        started.elapsed()
    };
    let (few, many) = (cost(100), cost(50_000));
    println!("1,000 reads near the head: {few:?} with 100 segments, {many:?} with 50,000");
    assert!(many < 3 * few, "{many:?} against {few:?}");
}

/// Two responses that read the same operations of the RAM buffer share them, and each still
/// reserves all of them, as a response with copies did (INV-B1): what a response reserves does
/// not depend on who else holds its operations.
#[test]
fn each_response_reserves_the_operations_it_shares() {
    use zemdb_core::id::{ClientId, RoomId};
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_plain(&mut log, 1, 12);
    assert_eq!(log.hot_buffer.min_seq(), Some(SequenceNumber::new(8)));

    let (budget, mut first) = reservation_in(u64::from(u32::MAX));
    let holder = (
        RoomId::new("room").unwrap(),
        crate::relay::Uploader::Client(ClientId::new("c").unwrap()),
    );
    let mut second = budget.reserve(holder, 0).unwrap();
    let from = SequenceNumber::new(7);
    let (read, _) = log.fetch_deltas(from, 100, Some(&mut first)).unwrap();
    let (again, _) = log.fetch_deltas(from, 100, Some(&mut second)).unwrap();
    assert_contiguous(&read, 8, 12);
    assert_contiguous(&again, 8, 12);
    assert!(
        read.iter()
            .zip(&again)
            .all(|(op, same)| Arc::ptr_eq(op, same)),
        "the responses copy the operations"
    );
    assert_eq!(budget.reserved().0, 2 * in_response(8, 12));
}

/// A read that starts in the disk tiers and goes on in the RAM buffer, as after a seal that
/// failed once `active.wal` was renamed (the disk tiers then end before the head), shares the
/// operations it takes from the buffer too (DS6).
#[test]
fn a_read_that_goes_on_from_disk_into_ram_shares_the_operations_of_the_buffer() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 3,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_plain(&mut log, 1, 5);
    crate::fail_point::arm("sync_dir", &segments_dir(dir.path()));
    let outcome = log.append(make_op(6), None).unwrap();
    assert!(outcome.rotation_error.is_some(), "the seal of 4..=6 fails");
    assert_eq!(log.hot_buffer.min_seq(), Some(SequenceNumber::new(4)));

    // Segment 1..=3 gives 2 and 3; the buffer, 4 to 6.
    let (ops, has_more) = log.fetch_deltas(SequenceNumber::new(1), 100, None).unwrap();
    assert_contiguous(&ops, 2, 6);
    assert!(!has_more);
    let held = log
        .hot_buffer
        .get_range(SequenceNumber::new(3), &mut ReadBudget::ops(10));
    assert_contiguous(&held, 4, 6);
    for (op, same) in ops[2..].iter().zip(&held) {
        assert!(Arc::ptr_eq(op, same), "operation {} is a copy", op.seq);
    }
}

/// What `make_op(first..=last)` take in a response.
fn in_response(first: u64, last: u64) -> u64 {
    (first..=last)
        .map(|seq| crate::log::budget::OpSize::of(&make_op(seq)).in_response())
        .sum()
}

/// A reservation of nothing in a budget of response bytes in flight of `total` bytes.
fn reservation_in(
    total: u64,
) -> (
    std::sync::Arc<crate::response_budget::InflightBudget>,
    crate::response_budget::Reservation,
) {
    use zemdb_core::id::{ClientId, RoomId};
    let budget = std::sync::Arc::new(crate::response_budget::InflightBudget::new(total, total));
    let holder = (
        RoomId::new("room").unwrap(),
        crate::relay::Uploader::Client(ClientId::new("c").unwrap()),
    );
    let reservation = budget.reserve(holder, 0).unwrap();
    (budget, reservation)
}

/// A read for a response ends at the first operation its reservation has no room for, in the
/// disk tiers and in RAM alike, with `has_more`; one with no room for even the first returns an
/// empty batch with `has_more`, not a gap.
#[test]
fn a_read_ends_where_the_reservation_of_its_response_runs_out() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy {
        ram_max_ops: 5,
        ..RoomLifecyclePolicy::default()
    };
    let (mut log, _) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    append_plain(&mut log, 1, 12);
    assert_eq!(log.hot_buffer.min_seq(), Some(SequenceNumber::new(8)));

    // From the sealed segments, with room for three operations and most of a fourth.
    let room = in_response(1, 4) - 1;
    let (budget, mut reservation) = reservation_in(room);
    let (ops, has_more) = log
        .fetch_deltas(SequenceNumber::new(0), 100, Some(&mut reservation))
        .unwrap();
    assert_contiguous(&ops, 1, 3);
    assert!(has_more);
    assert_eq!(budget.reserved().0, in_response(1, 3));

    // From RAM, the same.
    let room = in_response(9, 12) - 1;
    let (budget, mut reservation) = reservation_in(room);
    let (ops, has_more) = log
        .fetch_deltas(SequenceNumber::new(8), 100, Some(&mut reservation))
        .unwrap();
    assert_contiguous(&ops, 9, 11);
    assert!(has_more);
    assert_eq!(budget.reserved().0, in_response(9, 11));

    // No room for the first operation, from disk and from RAM.
    for (from, first) in [(0, 1), (8, 9)] {
        let (budget, mut reservation) = reservation_in(in_response(first, first) - 1);
        let read = log.fetch_deltas(SequenceNumber::new(from), 100, Some(&mut reservation));
        let (ops, has_more) = read.unwrap();
        assert!(ops.is_empty() && has_more, "from {from}");
        assert_eq!(budget.reserved().0, 0);
    }

    // With room for all, the read is the same as without a reservation.
    let (_budget, mut reservation) = reservation_in(u64::from(u32::MAX));
    let reserved = log
        .fetch_deltas(SequenceNumber::new(0), 100, Some(&mut reservation))
        .unwrap();
    assert_eq!(
        reserved,
        log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap()
    );
}

/// The records `first..=last`, each with the mutation id `mutation_for(seq)`.
fn group_of(first: u64, last: u64) -> Vec<(Arc<SequencedOperation>, Option<MutationId>)> {
    group_of_ops(first, last, make_op)
}

/// A group of records is written to `active.wal` and synced once, and only then is it visible:
/// after a failed fsync the head and the reads are where they were.
#[test]
fn a_group_of_records_is_synced_once_and_is_visible_only_after_its_fsync() {
    let dir = tempdir().unwrap();
    let (mut log, _) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    append_range(&mut log, 1, 2);
    let segments = segments_dir(dir.path());
    let syncs = io_probe::count(IoEvent::ActiveWalSync, &segments);

    log.append_group(group_of(3, 7)).unwrap();
    assert_eq!(
        io_probe::count(IoEvent::ActiveWalSync, &segments) - syncs,
        1
    );
    assert_eq!(log.head_seq().get(), 7);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_contiguous(&ops, 1, 7);

    crate::fail_point::arm("warm_append_sync", &segments.join("active.wal"));
    assert!(log.append_group(group_of(8, 10)).is_err());
    assert_eq!(log.head_seq().get(), 7);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
    assert_contiguous(&ops, 1, 7);
}

/// A crash after the records of a group reached `active.wal` and before their fsync can leave
/// any prefix of them on disk. Every cut reopens with exactly the whole records before it, with
/// their mutation ids, truncates the rest as a torn write, and the next record continues them.
#[test]
fn every_cut_of_a_group_written_before_its_fsync_recovers_the_whole_records_before_it() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();
    let active = segments_dir(dir.path()).join("active.wal");
    let synced_len = {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        append_range(&mut log, 1, 3);
        let synced_len = log.warm_disk.active_len() as usize;
        crate::fail_point::arm("warm_append_sync", &active);
        assert!(log.append_group(group_of(4, 8)).is_err());
        synced_len
    };
    let bytes = std::fs::read(&active).unwrap();
    let ends: Vec<usize> = (1..=8u64)
        .scan(0, |end, seq| {
            *end += encode_wal_record(&make_op(seq), Some(mutation_for(seq)))
                .unwrap()
                .len();
            Some(*end)
        })
        .collect();
    assert_eq!(ends[2], synced_len);
    assert_eq!(
        bytes.len(),
        ends[7],
        "the whole group must reach the file before its fsync"
    );

    for cut in synced_len..=bytes.len() {
        let copy = tempdir().unwrap();
        let copy_segments = segments_dir(copy.path());
        std::fs::create_dir_all(&copy_segments).unwrap();
        std::fs::write(copy_segments.join("active.wal"), &bytes[..cut]).unwrap();
        let whole = ends.iter().filter(|&&end| end <= cut).count() as u64;

        let (mut log, mutations) = TieredLog::open_or_create(copy.path(), policy.clone())
            .unwrap_or_else(|e| panic!("cut at {cut}: {e}"));
        assert_eq!(log.head_seq().get(), whole, "cut at {cut}");
        let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
        assert_contiguous(&ops, 1, whole);
        let expected: Vec<_> = (1..=whole)
            .map(|seq| (mutation_for(seq), SequenceNumber::new(seq)))
            .collect();
        assert_eq!(mutations, expected, "cut at {cut}");
        log.append(make_op(whole + 1), Some(mutation_for(whole + 1)))
            .unwrap();
    }
}

/// The insertion of row `seq` at sequence `seq`, with a title of 200 bytes: a record of about
/// 260 bytes, so a group of them spans several sectors.
fn titled_op(seq: u64) -> SequencedOperation {
    let row = zemdb_core::value::CompactRow::new(vec![
        Value::Int(seq as i64),
        Value::String("t".repeat(200).into()),
    ]);
    SequencedOperation::new(
        SequenceNumber::new(seq),
        Operation::insert(
            1,
            PrimaryKey::single(Value::Int(seq as i64)),
            row,
            seq * 1000,
        ),
    )
}

/// The records `first..=last` of `titled_op`, each with the mutation id `mutation_for(seq)`.
fn titled_group(first: u64, last: u64) -> Vec<(Arc<SequencedOperation>, Option<MutationId>)> {
    group_of_ops(first, last, titled_op)
}

/// The records `first..=last` of `op`, each with the mutation id `mutation_for(seq)`.
fn group_of_ops(
    first: u64,
    last: u64,
    op: fn(u64) -> SequencedOperation,
) -> Vec<(Arc<SequencedOperation>, Option<MutationId>)> {
    (first..=last)
        .map(|seq| (Arc::new(op(seq)), Some(mutation_for(seq))))
        .collect()
}

/// A record of about 1.1 KiB, larger than two sectors of 512 bytes.
fn kib_op(seq: u64) -> SequencedOperation {
    large_op(seq, 1_000)
}

/// A record of about 8.3 KiB, larger than two pages of 4 KiB.
fn eight_kib_op(seq: u64) -> SequencedOperation {
    large_op(seq, 8_300)
}

/// Writes `groups` to a new log in `dir`, each as one group of records of `op`, and returns the
/// bytes of its `active.wal` and where each record ends. With `unsynced`, the fsync of the last
/// group fails, so its records reach the file without being acknowledged.
fn write_groups_of(
    dir: &Path,
    groups: &[(u64, u64)],
    unsynced: bool,
    op: fn(u64) -> SequencedOperation,
) -> (Vec<u8>, Vec<usize>) {
    let active = segments_dir(dir).join("active.wal");
    let (mut log, _) = TieredLog::open_or_create(dir, RoomLifecyclePolicy::default()).unwrap();
    let mut ends = Vec::new();
    for (i, &(first, last)) in groups.iter().enumerate() {
        if unsynced && i == groups.len() - 1 {
            crate::fail_point::arm("warm_append_sync", &active);
            assert!(log.append_group(group_of_ops(first, last, op)).is_err());
        } else {
            log.append_group(group_of_ops(first, last, op)).unwrap();
        }
        for seq in first..=last {
            let len = encode_wal_record(&op(seq), Some(mutation_for(seq)))
                .unwrap()
                .len();
            ends.push(ends.last().copied().unwrap_or(0) + len);
        }
    }
    drop(log);
    let bytes = std::fs::read(&active).unwrap();
    assert_eq!(bytes.len(), *ends.last().unwrap());
    (bytes, ends)
}

/// `write_groups_of` with records of `titled_op`.
fn write_groups(dir: &Path, groups: &[(u64, u64)], unsynced: bool) -> (Vec<u8>, Vec<usize>) {
    write_groups_of(dir, groups, unsynced, titled_op)
}

/// Opens a log whose `active.wal` holds `bytes`, in a new directory, with its mutation ids.
type OpenedImage = Result<(TieredLog, Vec<(MutationId, SequenceNumber)>), ServerError>;
fn open_image(bytes: &[u8]) -> (tempfile::TempDir, OpenedImage) {
    let copy = tempdir().unwrap();
    let segments = segments_dir(copy.path());
    std::fs::create_dir_all(&segments).unwrap();
    std::fs::write(segments.join("active.wal"), bytes).unwrap();
    let opened = TieredLog::open_or_create(copy.path(), RoomLifecyclePolicy::default());
    (copy, opened)
}

/// A group of `records` records of `op` whose fsync failed, after `before` acknowledged ones:
/// the bytes of `active.wal`, where each record ends and how many bytes were synced.
struct UnsyncedGroup {
    bytes: Vec<u8>,
    ends: Vec<usize>,
    synced_len: usize,
    op: fn(u64) -> SequencedOperation,
}

impl UnsyncedGroup {
    fn write(before: u64, records: u64, op: fn(u64) -> SequencedOperation) -> Self {
        let dir = tempdir().unwrap();
        let mut groups = Vec::new();
        if before > 0 {
            groups.push((1, before));
        }
        groups.push((before + 1, before + records));
        let (bytes, ends) = write_groups_of(dir.path(), &groups, true, op);
        let synced_len = if before > 0 {
            ends[before as usize - 1]
        } else {
            0
        };
        Self {
            bytes,
            ends,
            synced_len,
            op,
        }
    }

    /// The sectors of `sector` bytes that the group wrote to, as absolute indexes.
    fn sectors(&self, sector: usize) -> std::ops::RangeInclusive<usize> {
        self.synced_len / sector..=(self.bytes.len() - 1) / sector
    }

    /// The image a power cut leaves when the disk loses the sectors `lost`: zeros where it lost
    /// them, never over the bytes already synced.
    fn losing(&self, sector: usize, lost: impl Fn(usize) -> bool) -> Vec<u8> {
        let mut image = self.bytes.clone();
        for k in self.sectors(sector).filter(|&k| lost(k)) {
            let start = (k * sector).max(self.synced_len);
            let end = ((k + 1) * sector).min(self.bytes.len());
            image[start..end].fill(0);
        }
        image
    }

    /// Opens `image`, which must reopen with exactly the whole records before its first damaged
    /// one and their mutation ids, truncate the rest, and take the next record.
    fn check_reopens(&self, image: &[u8], what: &str) {
        let mut start = 0;
        let whole = self
            .ends
            .iter()
            .take_while(|&&end| {
                let intact = image[start..end] == self.bytes[start..end];
                start = end;
                intact
            })
            .count() as u64;
        let (_copy, opened) = open_image(image);
        let (mut log, mutations) = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
        assert_eq!(log.head_seq().get(), whole, "{what}");
        let (ops, _) = log.fetch_deltas(SequenceNumber::new(0), 100, None).unwrap();
        assert_contiguous(&ops, 1, whole.min(100));
        let expected: Vec<_> = (1..=whole)
            .map(|seq| (mutation_for(seq), SequenceNumber::new(seq)))
            .collect();
        assert_eq!(mutations, expected, "{what}");
        assert_eq!(
            log.warm_disk.active_len(),
            self.ends[..whole as usize].last().copied().unwrap_or(0) as u64,
            "{what}"
        );
        log.append((self.op)(whole + 1), Some(mutation_for(whole + 1)))
            .unwrap();
    }
}

/// Of the images that a power cut during the fsync of a group of `records` records of `op` can
/// leave, after `before` acknowledged records, when the disk loses any subset of the sectors of
/// `sector` bytes that the group wrote, opens all of them if the group spans at most
/// `every_up_to` sectors, and `sample` random ones otherwise (each sector lost or not with even
/// odds). Every image reopens with exactly the whole records before the first damaged one and
/// their mutation ids, truncates the rest, and the next record continues them.
fn check_lost_sectors_of_a_group(
    before: u64,
    records: u64,
    op: fn(u64) -> SequencedOperation,
    sector: usize,
    every_up_to: usize,
    sample: u64,
) {
    let group = UnsyncedGroup::write(before, records, op);
    let first = *group.sectors(sector).start();
    let sectors = group.sectors(sector).count();
    let mut rng = TestRng::new(sectors as u64 * 31 + records);
    let images: Vec<Vec<bool>> = if sectors <= every_up_to {
        (0..1u64 << sectors)
            .map(|mask| (0..sectors).map(|k| mask & (1 << k) == 0).collect())
            .collect()
    } else {
        (0..sample)
            .map(|_| (0..sectors).map(|_| rng.below(2) == 0).collect())
            .collect()
    };
    for lost in images {
        let image = group.losing(sector, |k| lost[k - first]);
        let what = format!(
            "{records} records after {before}, sectors of {sector}, lost {:?}",
            lost.iter().map(|&l| u8::from(l)).collect::<Vec<_>>()
        );
        group.check_reopens(&image, &what);
    }
}

/// The groups the crash images cover: how many records, of what, and the size of the sectors.
type SectorCase = (u64, fn(u64) -> SequencedOperation, usize);
const SECTOR_CASES: [SectorCase; 9] = [
    (2, titled_op, 512),
    (4, titled_op, 512),
    (30, titled_op, 512),
    (64, titled_op, 512),
    (4, titled_op, 4096),
    (30, titled_op, 4096),
    (30, kib_op, 512),
    (64, kib_op, 512),
    (64, eight_kib_op, 4096),
];

/// A power cut during the fsync of a group can persist a later sector of the group and not an
/// earlier one, leaving damage followed by valid frames of the same group. No record of the
/// group was acknowledged, so the open truncates the group from the damage, as it truncates a
/// torn last record. Also when the group is the first thing in `active.wal`, and with records
/// larger than a sector, many of which keep their header and lose part of their payload.
#[test]
fn losing_any_unsynced_sectors_of_a_group_reopens_with_the_whole_records_before_them() {
    for before in [3, 0] {
        for (records, op, sector) in SECTOR_CASES {
            check_lost_sectors_of_a_group(before, records, op, sector, 6, 16);
        }
    }
}

/// `losing_any_unsynced_sectors_of_a_group_reopens_with_the_whole_records_before_them` with every
/// image of the groups of up to 12 sectors, and 1,024 random ones of the larger groups.
#[test]
#[ignore = "long-running test; run with --ignored in release"]
fn losing_any_unsynced_sectors_of_a_group_reopens_with_the_whole_records_before_them_long() {
    for before in [3, 0] {
        for (records, op, sector) in SECTOR_CASES {
            check_lost_sectors_of_a_group(before, records, op, sector, 12, 1_024);
        }
    }
}

/// Every record of a group keeps its header and loses a sector of its payload: each one fails its
/// payload check, more of them than the limit of failed checks of the search after a damaged
/// frame, which must not take the torn group for corruption. Also a full group of 64 small records
/// losing every other sector.
#[test]
fn a_group_whose_records_each_lost_part_of_their_payload_reopens() {
    for (records, op, sector) in [
        (18, kib_op as fn(u64) -> _, 512),
        (18, eight_kib_op, 4096),
        (64, kib_op, 512),
    ] {
        let group = UnsyncedGroup::write(3, records, op);
        let lost: Vec<usize> = group.ends[2..group.ends.len() - 1]
            .iter()
            .map(|&start| (start + BATCH_HEADER_SIZE).div_ceil(sector))
            .collect();
        for (i, &k) in lost.iter().enumerate() {
            assert!(
                (k + 1) * sector <= group.ends[3 + i],
                "record {i} holds a whole sector"
            );
        }
        let image = group.losing(sector, |k| lost.contains(&k));
        group.check_reopens(&image, &format!("{records} records, sectors of {sector}"));
    }
    let group = UnsyncedGroup::write(3, 64, titled_op);
    let first = *group.sectors(512).start();
    for parity in [0, 1] {
        let image = group.losing(512, |k| (k - first) % 2 == parity);
        group.check_reopens(&image, &format!("every other sector from {parity}"));
    }
}

/// A value of `len` bytes that holds, at its start, the header of a frame that says its group
/// starts `delta` bytes before it (where the record that holds the value starts), with no
/// operations, and whose payload is the rest of the value and then the first byte of whatever
/// frame follows the record, always the first byte of the magic: all of it known when the value
/// is written.
fn value_holding_a_frame(len: usize, delta: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&delta.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&0u64.to_le_bytes());
    payload.resize(len - BATCH_HEADER_SIZE, 0x13);
    payload.push(zemdb_core::protocol::wal_frame::BATCH_MAGIC[0]);
    let mut header = [0u8; BATCH_HEADER_SIZE];
    header[0..2].copy_from_slice(&zemdb_core::protocol::wal_frame::BATCH_MAGIC);
    header[2..6].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[6..10].copy_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    let crc = crc32fast::hash(&header[..14]);
    header[14..].copy_from_slice(&crc.to_le_bytes());
    let mut value = header.to_vec();
    value.extend_from_slice(&payload[..payload.len() - 1]);
    value
}

/// A member commits record 4 with a value that holds a frame that says it is of the group of the
/// record, and whose payload reaches into the frame after the record; another member then
/// commits record 5, alone, and gets its acknowledgement. Damage to one bit of the header of
/// record 4 followed by record 5, of a later group, is corruption: the open fails and truncates
/// nothing, instead of taking the stored frame for one of the damaged group, skipping the start
/// of record 5 and truncating it as a torn write.
#[test]
fn a_frame_stored_in_a_value_does_not_hide_a_later_acknowledged_record() {
    let op = |value: Vec<u8>| {
        let row =
            zemdb_core::value::CompactRow::new(vec![Value::Int(4), Value::Bytes(value.into())]);
        SequencedOperation::new(
            SequenceNumber::new(4),
            Operation::insert(1, PrimaryKey::single(Value::Int(4)), row, 4000),
        )
    };
    let len = 200;
    let plain = encode_wal_record(&op(vec![0x55; len]), Some(mutation_for(4))).unwrap();
    let delta = plain.len() - len;
    assert_eq!(
        plain[delta..],
        vec![0x55; len][..],
        "the value ends the record"
    );

    let dir = tempdir().unwrap();
    let active = segments_dir(dir.path()).join("active.wal");
    let (mut log, _) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    log.append_group(titled_group(1, 3)).unwrap();
    let at = log.warm_disk.active_len() as usize;
    log.append_group(vec![(
        Arc::new(op(value_holding_a_frame(len, delta as u64))),
        Some(mutation_for(4)),
    )])
    .unwrap();
    log.append_group(titled_group(5, 5)).unwrap();
    drop(log);

    let mut bytes = std::fs::read(&active).unwrap();
    bytes[at + BATCH_HEADER_SIZE - 1] ^= 0x01;
    std::fs::write(&active, &bytes).unwrap();
    assert!(TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).is_err());
    assert_eq!(
        std::fs::read(&active).unwrap(),
        bytes,
        "nothing may be truncated"
    );
}

/// Damage in a group followed by a frame of a later group is corruption: a group is written only
/// after the group before it was synced, so the damaged records were acknowledged. The open fails
/// and truncates nothing. Also when the damage reaches into the first record of the later group,
/// whose other records are intact, and when it starts at the beginning of `active.wal`.
#[test]
fn damage_in_a_group_followed_by_a_later_group_fails_the_open_and_truncates_nothing() {
    // What the damage is, the groups written, and the damage, given where each record ends.
    type Case = (&'static str, &'static [(u64, u64)], fn(&mut [u8], &[usize]));
    let cases: [Case; 4] = [
        (
            "a payload byte of a record in the middle of a group",
            &[(1, 3), (4, 7), (8, 10)],
            |bytes, ends| bytes[ends[4] - 10] ^= 0xFF,
        ),
        (
            "the last record of a group, before a group of one",
            &[(1, 3), (4, 6), (7, 7)],
            |bytes, ends| bytes[ends[4]..ends[5]].fill(0),
        ),
        (
            "the last record of a group and the first of the next",
            &[(1, 3), (4, 7), (8, 10)],
            |bytes, ends| bytes[ends[5]..ends[7]].fill(0),
        ),
        (
            "the first group of the file, of one record, and the first record of the next",
            &[(1, 1), (2, 4)],
            |bytes, ends| bytes[..ends[1]].fill(0),
        ),
    ];
    for (what, groups, damage) in cases {
        let dir = tempdir().unwrap();
        let (mut bytes, ends) = write_groups(dir.path(), groups, false);
        damage(&mut bytes, &ends);
        let (copy, opened) = open_image(&bytes);
        assert!(opened.is_err(), "{what}: corruption must fail the open");
        let on_disk = std::fs::read(segments_dir(copy.path()).join("active.wal")).unwrap();
        assert_eq!(on_disk, bytes, "{what}: nothing may be truncated");
    }
}

/// Damage in the last group of `active.wal` after its fsync cannot be told from a torn write of
/// it: the group is truncated from the damage, acknowledged records included, and their
/// sequences are assigned again unless another file of the log records them (here
/// `log_meta.json`, after a rotation that recorded its end and stopped before the rename).
#[test]
fn damage_in_the_last_group_after_its_fsync_is_truncated_from_the_damage() {
    let dir = tempdir().unwrap();
    let (mut bytes, ends) = write_groups(dir.path(), &[(1, 3), (4, 7)], false);
    bytes[ends[4] - 10] ^= 0xFF;
    let (copy, opened) = open_image(&bytes);
    let (mut log, _) = opened.unwrap();
    assert_eq!(log.head_seq().get(), 4);
    log.append_group(group_of(5, 6)).unwrap();
    drop(log);
    let (log, mutations) =
        TieredLog::open_or_create(copy.path(), RoomLifecyclePolicy::default()).unwrap();
    assert_eq!(log.head_seq().get(), 6);
    let (ops, _) = log.fetch_deltas(SequenceNumber::new(4), 100, None).unwrap();
    assert_eq!(owned(ops), vec![make_op(5), make_op(6)], "5 is reused");
    assert_eq!(mutations.len(), 6);

    // With the end of the group in `log_meta.json`, its sequences are a gap instead.
    let dir = tempdir().unwrap();
    let active = segments_dir(dir.path()).join("active.wal");
    let (mut log, _) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    log.append_group(titled_group(1, 3)).unwrap();
    log.append_group(titled_group(4, 7)).unwrap();
    crate::fail_point::arm("exists", &active);
    assert!(log.force_rotate_warm().is_err());
    drop(log);
    let mut bytes = std::fs::read(&active).unwrap();
    bytes[ends[4] - 10] ^= 0xFF;
    std::fs::write(&active, &bytes).unwrap();
    let (mut log, _) =
        TieredLog::open_or_create(dir.path(), RoomLifecyclePolicy::default()).unwrap();
    assert_eq!(log.head_seq().get(), 7, "7 is in log_meta.json");
    log.append(make_op(8), Some(mutation_for(8))).unwrap();
    assert!(matches!(
        log.fetch_deltas(SequenceNumber::new(4), 10, None),
        Err(ServerError::BehindCompaction)
    ));
}
