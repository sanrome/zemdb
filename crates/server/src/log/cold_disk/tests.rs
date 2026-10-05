use super::*;
use crate::fail_point;
use crate::log::warm_disk::WarmDiskLog;
use tempfile::tempdir;
use zemdb_core::mutation::Operation;
use zemdb_core::value::{PrimaryKey, Value};

fn sealed_segment(dir: &Path) -> std::path::PathBuf {
    let mut log = WarmDiskLog::open_or_create(dir).unwrap();
    for seq in 1..=3u64 {
        let op = SequencedOperation::new(
            SequenceNumber::new(seq),
            Operation::delete(1, PrimaryKey::single(Value::Int(seq as i64)), seq),
        );
        log.append_record(&op, None).unwrap();
    }
    log.rotate_active_segment().unwrap().unwrap()
}

#[test]
fn warm_segment_is_kept_when_directory_sync_fails() {
    let dir = tempdir().unwrap();
    let warm = sealed_segment(dir.path());
    let cold = dir
        .path()
        .join("segment_0000000000000001_0000000000000003.wal.zst");

    fail_point::arm("sync_dir", dir.path());
    assert!(ColdDiskLog::compress_warm_segment_sync(&warm, &cold).is_err());
    assert!(
        warm.exists(),
        "the warm segment must survive until the rename is durable"
    );
}

#[tokio::test]
async fn async_compression_propagates_directory_sync_failure() {
    let dir = tempdir().unwrap();
    let warm = sealed_segment(dir.path());
    let cold = dir
        .path()
        .join("segment_0000000000000001_0000000000000003.wal.zst");

    fail_point::arm("sync_dir", dir.path());
    assert!(ColdDiskLog::compress_warm_segment(&warm, &cold)
        .await
        .is_err());
    assert!(warm.exists());
}

#[test]
fn compression_replaces_warm_segment() {
    let dir = tempdir().unwrap();
    let warm = sealed_segment(dir.path());
    let cold = dir
        .path()
        .join("segment_0000000000000001_0000000000000003.wal.zst");

    ColdDiskLog::compress_warm_segment_sync(&warm, &cold).unwrap();

    assert!(!warm.exists());
    let ops = ColdDiskLog::read_range(&cold, SequenceNumber::new(0), usize::MAX).unwrap();
    assert_eq!(ops.len(), 3);
}
