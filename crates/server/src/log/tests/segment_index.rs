use super::*;
use crate::log::test_support::{seq, Rng};
use tempfile::tempdir;

#[test]
fn scan_indexes_both_tiers_in_sequence_order_and_ignores_other_files() {
    let dir = tempdir().unwrap();
    std::fs::write(
        segment_path(dir.path(), seq(1), seq(5), Tier::Cold),
        [0u8; 10],
    )
    .unwrap();
    std::fs::write(
        segment_path(dir.path(), seq(6), seq(10), Tier::Warm),
        [0u8; 20],
    )
    .unwrap();
    std::fs::write(dir.path().join("active.wal"), [0u8; 40]).unwrap();
    std::fs::write(dir.path().join("segment_garbage.wal"), [0u8; 80]).unwrap();
    // A name whose range ends before it starts.
    let backwards = segment_path(dir.path(), seq(12), seq(11), Tier::Warm);
    std::fs::write(backwards, [0u8; 80]).unwrap();

    let index = SegmentIndex::scan(dir.path(), SequenceNumber::new(0)).unwrap();

    let segments: Vec<(u64, u64, Tier, u64)> = index
        .iter()
        .map(|s| (s.start_seq.get(), s.end_seq.get(), s.tier, s.bytes))
        .collect();
    assert_eq!(
        segments,
        vec![(1, 5, Tier::Cold, 10), (6, 10, Tier::Warm, 20)]
    );
    // Each segment of 5 operations counts a sidecar of 20 + 5 * 24 bytes.
    assert_eq!(index.total_bytes(), 30 + 2 * 140);
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
    assert!(SegmentIndex::scan(dir.path(), SequenceNumber::new(0)).is_err());
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

    // With the sidecar of 5 operations: 20 + 5 * 24 bytes.
    index.insert(entry(Tier::Warm, 100));
    assert_eq!(index.total_bytes(), 100 + 140);
    // Compression replaces the warm entry with its smaller cold copy.
    index.insert(entry(Tier::Cold, 30));
    assert_eq!(index.total_bytes(), 30 + 140);
    assert_eq!(index.remove(seq(1)).map(|e| e.tier), Some(Tier::Cold));
    assert_eq!(index.total_bytes(), 0);
    assert!(index.remove(seq(1)).is_none());
}

/// Random sidecars round trip, a sidecar of another segment is rejected, and a damaged one
/// never panics the decoder. A flipped bit or a changed byte is always detected: the checksum
/// catches every burst of up to 32 bits.
fn check_sidecars(seed: u64, cases: u64) {
    let mut rng = Rng::new(seed);
    for case in 0..cases {
        let start = if rng.below(8) == 0 {
            rng.next_u64()
        } else {
            rng.below(10_000)
        };
        let n = rng.below(10) as usize;
        let end = start.saturating_add(n as u64);
        let ids: Vec<(MutationId, SequenceNumber)> = (0..n)
            .map(|k| {
                let id = u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64());
                (MutationId::from_u128(id), seq(start.wrapping_add(k as u64)))
            })
            .collect();
        let bytes = encode_mutation_ids(seq(start), seq(end), &ids);
        assert_eq!(bytes.len(), 16 + MUTATION_ID_ENTRY * n + 4);
        assert_eq!(decode_mutation_ids(&bytes, seq(start), seq(end)), Some(ids));
        assert_eq!(decode_mutation_ids(&bytes, seq(start ^ 1), seq(end)), None);
        assert_eq!(decode_mutation_ids(&bytes, seq(start), seq(end ^ 1)), None);

        let mut damaged = bytes.clone();
        let damage = rng.below(5);
        match damage {
            0 => {
                let i = rng.index(damaged.len());
                damaged[i] ^= 1 << rng.below(8);
            }
            1 => {
                let i = rng.index(damaged.len());
                damaged[i] = rng.next_u64() as u8;
            }
            2 => damaged.truncate(rng.index(bytes.len())),
            3 => damaged.extend((0..1 + rng.below(30)).map(|_| rng.next_u64() as u8)),
            _ => damaged = (0..rng.below(80)).map(|_| rng.next_u64() as u8).collect(),
        }
        let decoded = decode_mutation_ids(&damaged, seq(start), seq(end));
        if damage <= 1 && damaged != bytes {
            assert_eq!(decoded, None, "case {case}: {damaged:02x?}");
        }
    }
}

#[test]
fn random_sidecars_round_trip_and_damaged_ones_never_panic() {
    check_sidecars(0x51DE_0001, 20_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_sidecars_round_trip_and_damaged_ones_never_panic_long() {
    check_sidecars(0x51DE_0002, 2_000_000);
}
