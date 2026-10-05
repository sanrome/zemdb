use super::*;
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
