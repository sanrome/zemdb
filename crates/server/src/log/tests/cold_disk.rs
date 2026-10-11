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

fn frames(seqs: std::ops::RangeInclusive<u64>) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for seq in seqs {
        let op = SequencedOperation::new(
            SequenceNumber::new(seq),
            Operation::delete(1, PrimaryKey::single(Value::Int(seq as i64)), seq),
        );
        offsets.push(bytes.len());
        bytes.extend_from_slice(
            &zemdb_core::protocol::wal_frame::encode_wal_record(&op, None).unwrap(),
        );
    }
    (bytes, offsets)
}

fn write_cold(dir: &Path, uncompressed: &[u8]) -> std::path::PathBuf {
    let path = dir.join("segment_0000000000000001_0000000000000004.wal.zst");
    std::fs::write(&path, zstd::stream::encode_all(uncompressed, 3).unwrap()).unwrap();
    path
}

fn read_all(path: &Path) -> Result<RecoveredLogData, ServerError> {
    read_cold_file(
        path,
        Some(SequenceNumber::new(1)),
        SequenceNumber::new(0),
        &mut ReadBudget::ops(usize::MAX),
    )
}

#[test]
fn cold_segment_with_a_torn_tail_returns_the_frames_before_it() {
    let dir = tempdir().unwrap();
    let (bytes, _) = frames(1..=4);
    let path = write_cold(dir.path(), &bytes[..bytes.len() - 3]);

    let (ops, _) = read_all(&path).unwrap();

    assert_eq!(ops.len(), 3);
}

#[test]
fn cold_segment_damaged_before_a_valid_frame_is_corruption() {
    let dir = tempdir().unwrap();
    let (mut bytes, offsets) = frames(1..=4);
    bytes[offsets[1]..offsets[1] + zemdb_core::protocol::wal_frame::BATCH_HEADER_SIZE].fill(0);
    let path = write_cold(dir.path(), &bytes);

    assert!(matches!(
        read_all(&path),
        Err(ServerError::WalCorruption(_))
    ));
}

#[test]
fn cold_segment_that_is_not_zstd_is_corruption() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("segment_0000000000000001_0000000000000004.wal.zst");
    std::fs::write(&path, b"not a zstd stream at all").unwrap();

    assert!(matches!(
        read_all(&path),
        Err(ServerError::WalCorruption(_))
    ));
}

#[test]
fn warm_segment_with_a_damaged_payload_is_not_compressed() {
    let dir = tempdir().unwrap();
    let warm = dir
        .path()
        .join("segment_0000000000000001_0000000000000004.wal");
    let cold = dir
        .path()
        .join("segment_0000000000000001_0000000000000004.wal.zst");
    let (mut bytes, offsets) = frames(1..=4);
    bytes[offsets[2] - 1] ^= 0xFF;
    std::fs::write(&warm, &bytes).unwrap();

    let result = ColdDiskLog::compress_warm_segment_sync(&warm, &cold);

    assert!(matches!(result, Err(ServerError::WalCorruption(_))));
    assert!(!cold.exists());
    assert!(!crate::durable::tmp_path_for(&cold).exists());
    assert_eq!(std::fs::read(&warm).unwrap(), bytes);
}

#[test]
fn warm_segment_whose_operations_do_not_match_its_name_is_not_compressed() {
    let dir = tempdir().unwrap();
    for (name, seqs) in [
        ("segment_0000000000000001_0000000000000005.wal", 1..=4),
        ("segment_0000000000000002_0000000000000005.wal", 1..=5),
    ] {
        let warm = dir.path().join(name);
        let cold = dir.path().join(format!("{name}.zst"));
        let (bytes, _) = frames(seqs);
        std::fs::write(&warm, &bytes).unwrap();

        let result = ColdDiskLog::compress_warm_segment_sync(&warm, &cold);

        assert!(
            matches!(result, Err(ServerError::WalCorruption(_))),
            "{name}: {result:?}"
        );
        assert!(!cold.exists(), "{name}");
        assert!(warm.exists(), "{name}");
    }
}
