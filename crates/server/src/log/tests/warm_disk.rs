use super::*;
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

#[test]
fn append_into_empty_existing_active_wal_tracks_start_seq() {
    let dir = tempdir().unwrap();
    // A crash, or a torn write truncated to zero bytes, can leave an empty `active.wal` behind.
    std::fs::write(dir.path().join("active.wal"), b"").unwrap();

    let mut log = WarmDiskLog::open_or_create(dir.path()).unwrap();
    assert_eq!(log.active_start_seq(), None);

    log.append_record(&make_op(1), None).unwrap();
    log.append_record(&make_op(2), None).unwrap();

    assert_eq!(log.active_start_seq().map(|s| s.get()), Some(1));
    assert_eq!(log.active_end_seq().map(|s| s.get()), Some(2));
    assert_eq!(log.active_ops_count(), 2);
}

#[test]
fn empty_existing_active_wal_still_rotates() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("active.wal"), b"").unwrap();

    let mut log = WarmDiskLog::open_or_create(dir.path()).unwrap();
    for seq in 1..=3 {
        log.append_record(&make_op(seq), None).unwrap();
    }

    let sealed = log.rotate_active_segment().unwrap();

    assert!(sealed.is_some());
    let segments = log.list_sealed_segments().unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].start_seq.get(), 1);
    assert_eq!(segments[0].end_seq.get(), 3);
}

#[test]
fn rotation_fails_when_directory_sync_fails() {
    let dir = tempdir().unwrap();
    let mut log = WarmDiskLog::open_or_create(dir.path()).unwrap();
    log.append_record(&make_op(1), None).unwrap();

    crate::fail_point::arm("sync_dir", dir.path());
    assert!(log.rotate_active_segment().is_err());
}

#[test]
fn creating_a_new_active_segment_syncs_the_directory() {
    let dir = tempdir().unwrap();
    let mut log = WarmDiskLog::open_or_create(dir.path()).unwrap();
    log.append_record(&make_op(1), None).unwrap();
    log.rotate_active_segment().unwrap();

    // The next append creates a fresh active.wal, whose directory entry must be durable
    // before the record is acknowledged.
    crate::fail_point::arm("sync_dir", dir.path());
    assert!(log.append_record(&make_op(2), None).is_err());
}
