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

    assert_eq!(
        sealed,
        Some(
            dir.path()
                .join("segment_0000000000000001_0000000000000003.wal")
        )
    );
    assert!(sealed.unwrap().exists());
    assert!(!dir.path().join("active.wal").exists());
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

/// Writes `active.wal` with one frame per sequence number, and returns its bytes and the offset
/// of each frame.
fn write_active(dir: &Path, seqs: std::ops::RangeInclusive<u64>) -> (Vec<u8>, Vec<usize>) {
    let mut wal = Vec::new();
    let mut offsets = Vec::new();
    for seq in seqs {
        offsets.push(wal.len());
        wal.extend_from_slice(&encode_wal_record(&make_op(seq), None).unwrap());
    }
    std::fs::write(dir.join(ACTIVE_SEGMENT), &wal).unwrap();
    (wal, offsets)
}

/// Bytes a crash can leave where a frame was being written: a header still in zeros followed
/// by data that is not a frame.
fn zeroed_header_and_garbage() -> Vec<u8> {
    let mut bytes = vec![0u8; zemdb_core::protocol::wal_frame::BATCH_HEADER_SIZE];
    bytes.extend_from_slice(&[0x13; 100]);
    bytes.extend_from_slice(&[0xBA, 0x7C, 0x01, 0x02]);
    bytes
}

/// Damage done to the header of a frame, given the bytes from its start.
type HeaderDamage = fn(&mut [u8]);

#[test]
fn damaged_length_in_the_middle_is_corruption_and_leaves_the_segment_intact() {
    let damage: [(&str, HeaderDamage); 2] = [
        ("flipped length", |header| header[2] ^= 0x01),
        ("length past EOF", |header| {
            header[2..6].copy_from_slice(&1_000_000u32.to_le_bytes())
        }),
    ];
    for (name, damage) in damage {
        let dir = tempdir().unwrap();
        let (mut wal, offsets) = write_active(dir.path(), 1..=3);
        damage(&mut wal[offsets[1]..]);
        let active = dir.path().join(ACTIVE_SEGMENT);
        std::fs::write(&active, &wal).unwrap();

        let result = WarmDiskLog::open_recovering(dir.path());

        assert!(
            matches!(result, Err(ServerError::WalCorruption(_))),
            "{name}: unexpected result: {:?}",
            result.map(|(_, data)| data)
        );
        assert_eq!(std::fs::read(&active).unwrap(), wal, "{name}");

        // A sealed segment with the same damage fails to read instead of returning a prefix.
        let sealed = dir
            .path()
            .join("segment_0000000000000001_0000000000000003.wal");
        std::fs::write(&sealed, &wal).unwrap();
        let mut all = ReadBudget::ops(usize::MAX);
        assert!(
            matches!(
                read_segment_file(&sealed, Some(1.into()), 0.into(), &mut all),
                Err(ServerError::WalCorruption(_))
            ),
            "{name}"
        );
    }
}

#[test]
fn zeroed_header_followed_by_garbage_is_a_torn_write() {
    let dir = tempdir().unwrap();
    let (mut wal, _) = write_active(dir.path(), 1..=2);
    let valid_len = wal.len() as u64;
    wal.extend_from_slice(&zeroed_header_and_garbage());
    let active = dir.path().join(ACTIVE_SEGMENT);
    std::fs::write(&active, &wal).unwrap();

    let (log, (ops, _)) = WarmDiskLog::open_recovering(dir.path()).unwrap();

    assert_eq!(ops.len(), 2);
    assert_eq!(log.active_len(), valid_len);
    assert_eq!(std::fs::metadata(&active).unwrap().len(), valid_len);
}

#[test]
fn zeroed_header_followed_by_a_valid_frame_is_corruption() {
    let dir = tempdir().unwrap();
    let (mut wal, _) = write_active(dir.path(), 1..=1);
    wal.extend_from_slice(&zeroed_header_and_garbage());
    wal.extend_from_slice(&encode_wal_record(&make_op(2), None).unwrap());
    let active = dir.path().join(ACTIVE_SEGMENT);
    std::fs::write(&active, &wal).unwrap();

    let result = WarmDiskLog::open_recovering(dir.path());

    assert!(matches!(result, Err(ServerError::WalCorruption(_))));
    assert_eq!(std::fs::read(&active).unwrap(), wal);
}

#[test]
fn test_multi_process_file_lock_collision_on_active_wal() {
    let dir = tempdir().unwrap();
    let mut log1 = WarmDiskLog::open_or_create(dir.path()).unwrap();

    let pk = PrimaryKey::single(Value::Int(1));
    let op = Operation::delete(1, pk, 1000);
    let seq_op = SequencedOperation::new(SequenceNumber::new(1), op);

    // Appending a record acquires and retains exclusive flock on active.wal
    log1.append_record(&seq_op, None).unwrap();

    // Opening a second WarmDiskLog instance against the same directory must fail with RoomLocked
    let log2_res = WarmDiskLog::open_or_create(dir.path());
    match log2_res {
        Err(ServerError::RoomLocked(msg)) => {
            assert!(
                msg.contains("active.wal locked by another process"),
                "Expected RoomLocked error message, got: {}",
                msg
            );
        }
        Ok(_) => panic!("Expected RoomLocked error, but second open succeeded"),
        Err(other) => panic!("Expected RoomLocked error, got: {:?}", other),
    }
}

#[test]
fn test_multi_thread_warm_disk_log_flock_concurrency_stress() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // 1. Initial log opens and appends a record, acquiring and holding kernel flock on active.wal
    let mut initial_log = WarmDiskLog::open_or_create(&path).unwrap();
    let pk0 = PrimaryKey::single(Value::Int(0));
    let op0 = Operation::delete(1, pk0, 100);
    let seq_op0 = SequencedOperation::new(SequenceNumber::new(1), op0);
    initial_log.append_record(&seq_op0, None).unwrap();

    // 2. Spawn 12 concurrent threads contending to open_or_create against the locked directory
    const NUM_THREADS: usize = 12;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(NUM_THREADS));
    let mut handles = Vec::new();

    for _ in 0..NUM_THREADS {
        let b = std::sync::Arc::clone(&barrier);
        let p = path.clone();

        handles.push(std::thread::spawn(move || {
            b.wait();
            WarmDiskLog::open_or_create(&p)
        }));
    }

    let mut locked_errors = 0;
    for handle in handles {
        let res = handle.join().unwrap();
        match res {
            Err(ServerError::RoomLocked(msg)) => {
                assert!(msg.contains("active.wal locked by another process"));
                locked_errors += 1;
            }
            Ok(_) => {
                panic!("Concurrent open_or_create must not succeed while active.wal is locked")
            }
            Err(other) => panic!("Expected RoomLocked, got: {other:?}"),
        }
    }

    assert_eq!(
        locked_errors, NUM_THREADS,
        "All 12 concurrent threads must be rejected with RoomLocked"
    );

    // 3. Drop initial_log, releasing the kernel flock
    drop(initial_log);

    // 4. Now a subsequent instance can successfully acquire the lock and append records
    let mut next_log = WarmDiskLog::open_or_create(&path).unwrap();
    let pk = PrimaryKey::single(Value::Int(99));
    let op = Operation::delete(1, pk, 9999);
    let seq_op = SequencedOperation::new(SequenceNumber::new(2), op);
    next_log.append_record(&seq_op, None).unwrap();
    assert_eq!(next_log.active_end_seq().unwrap().get(), 2);
}
