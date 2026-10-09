use super::*;
use tempfile::tempdir;

fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

#[test]
fn scan_indexes_both_tiers_in_sequence_order_and_ignores_other_files() {
    let dir = tempdir().unwrap();
    std::fs::write(cold_segment_path(dir.path(), seq(1), seq(5)), [0u8; 10]).unwrap();
    std::fs::write(warm_segment_path(dir.path(), seq(6), seq(10)), [0u8; 20]).unwrap();
    std::fs::write(dir.path().join("active.wal"), [0u8; 40]).unwrap();
    std::fs::write(dir.path().join("segment_garbage.wal"), [0u8; 80]).unwrap();

    let index = SegmentIndex::scan(dir.path()).unwrap();

    let segments: Vec<(u64, u64, Tier, u64)> = index
        .iter()
        .map(|s| (s.start_seq.get(), s.end_seq.get(), s.tier, s.bytes))
        .collect();
    assert_eq!(
        segments,
        vec![(1, 5, Tier::Cold, 10), (6, 10, Tier::Warm, 20)]
    );
    assert_eq!(index.total_bytes(), 30);
    assert_eq!(index.first_start(), Some(seq(1)));
    assert_eq!(index.last_end(), Some(seq(10)));
}

#[test]
fn scan_fails_when_removing_a_leftover_is_not_durable() {
    let dir = tempdir().unwrap();
    std::fs::write(
        dir.path()
            .join("segment_0000000000000001_0000000000000005.wal.zst.tmp"),
        b"x",
    )
    .unwrap();

    crate::fail_point::arm("sync_dir", dir.path());
    assert!(SegmentIndex::scan(dir.path()).is_err());
}

#[test]
fn total_bytes_follow_inserts_replacements_and_removals() {
    let mut index = SegmentIndex::default();
    let entry = |tier, bytes| SegmentEntry {
        start_seq: seq(1),
        end_seq: seq(5),
        tier,
        path: PathBuf::from("segment"),
        bytes,
        since: SystemTime::UNIX_EPOCH,
    };

    index.insert(entry(Tier::Warm, 100));
    assert_eq!(index.total_bytes(), 100);
    // Compression replaces the warm entry with its smaller cold copy.
    index.insert(entry(Tier::Cold, 30));
    assert_eq!(index.total_bytes(), 30);
    assert_eq!(index.remove(seq(1)).map(|e| e.tier), Some(Tier::Cold));
    assert_eq!(index.total_bytes(), 0);
    assert!(index.remove(seq(1)).is_none());
}
