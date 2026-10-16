use super::*;
use crate::log::test_support::{frames, make_op, seqs, Rng};
use std::io::Cursor;

fn read(bytes: &[u8], start: Option<u64>, from: u64, budget: &mut ReadBudget) -> FrameRead {
    let mut source = BufReader::new(Cursor::new(bytes));
    read_frames(
        &mut source,
        Some(bytes.len() as u64),
        start.map(SequenceNumber::new),
        SequenceNumber::new(from),
        budget,
    )
    .unwrap()
}

#[test]
fn frames_before_the_cursor_are_skipped_without_decoding() {
    let (mut bytes, offsets) = frames(1..=10);
    let full = read(&bytes, Some(1), 7, &mut ReadBudget::ops(usize::MAX));
    // Damage the payload of every frame up to 7: decoding any of them would fail its checksum.
    for &offset in &offsets[..7] {
        bytes[offset + BATCH_HEADER_SIZE] ^= 0xFF;
    }

    let tail = read(&bytes, Some(1), 7, &mut ReadBudget::ops(usize::MAX));

    assert_eq!(seqs(&tail.ops), vec![8, 9, 10]);
    assert_eq!(tail.ops, full.ops);
    assert!(tail.anomaly.is_none());
}

#[test]
fn without_the_first_sequence_every_frame_is_decoded() {
    let (mut bytes, offsets) = frames(1..=4);
    bytes[offsets[0] + BATCH_HEADER_SIZE] ^= 0xFF;

    let read = read(&bytes, None, 3, &mut ReadBudget::ops(usize::MAX));

    assert!(read.ops.is_empty());
    assert!(matches!(
        read.anomaly,
        Some((0, FrameAnomaly::ChecksumMismatch { .. }))
    ));
}

#[test]
fn reading_stops_where_the_byte_budget_runs_out() {
    let (bytes, _) = frames(1..=10);
    let one = op_bytes(&make_op(1));

    let read = read(&bytes, Some(1), 2, &mut ReadBudget::new(100, 3 * one));

    assert_eq!(seqs(&read.ops), vec![3, 4, 5]);
}

#[test]
fn a_skipped_frame_past_the_end_is_an_anomaly() {
    let (bytes, offsets) = frames(1..=3);
    let cut = &bytes[..bytes.len() - 1];

    let read = read(cut, Some(1), 3, &mut ReadBudget::ops(usize::MAX));

    assert!(read.ops.is_empty());
    assert!(matches!(
        read.anomaly,
        Some((at, FrameAnomaly::TruncatedPayload { .. })) if at == offsets[2] as u64
    ));
}

#[test]
fn a_frame_that_does_not_start_where_the_counts_lead_is_corruption() {
    let (bytes, _) = frames(5..=8);
    let mut source = BufReader::new(Cursor::new(&bytes));

    let result = read_frames(
        &mut source,
        Some(bytes.len() as u64),
        Some(SequenceNumber::new(1)),
        SequenceNumber::new(2),
        &mut ReadBudget::ops(usize::MAX),
    );

    assert!(matches!(result, Err(ServerError::WalCorruption(_))));
}

#[test]
fn a_zstd_stream_skips_frames_too() {
    let (mut bytes, offsets) = frames(1..=10);
    for &offset in &offsets[..7] {
        bytes[offset + BATCH_HEADER_SIZE] ^= 0xFF;
    }
    let compressed = zstd::stream::encode_all(&bytes[..], 3).unwrap();
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed)).unwrap();

    let read = read_frames(
        &mut decoder,
        None,
        Some(SequenceNumber::new(1)),
        SequenceNumber::new(7),
        &mut ReadBudget::ops(usize::MAX),
    )
    .unwrap();

    assert_eq!(seqs(&read.ops), vec![8, 9, 10]);
}

/// Damages `log` with 1 to 3 bit flips, byte changes, truncations, interesting lengths and
/// insertions.
fn damage(rng: &mut Rng, log: &mut Vec<u8>) {
    for _ in 0..1 + rng.below(3) {
        if log.is_empty() {
            return;
        }
        let at = rng.index(log.len());
        match rng.below(5) {
            0 => log[at] ^= 1 << rng.below(8),
            1 => log[at] = rng.next_u64() as u8,
            2 => log.truncate(at),
            3 => {
                let lengths = [0u32, 1, u32::MAX, 16 << 20, (16 << 20) + 1];
                let value = lengths[rng.index(lengths.len())];
                for (k, byte) in value.to_le_bytes().into_iter().enumerate() {
                    if let Some(slot) = log.get_mut(at + k) {
                        *slot = byte;
                    }
                }
            }
            _ => {
                let inserted: Vec<u8> = (0..rng.below(40)).map(|_| rng.next_u64() as u8).collect();
                log.splice(at..at, inserted);
            }
        }
    }
}

/// Rewrites the checksums of the frame at `at` (the payload's, if the length it declares fits,
/// then the header's), so that its damage reaches the payload decoder.
fn seal_frame(log: &mut [u8], at: usize) {
    if log.len() < at + BATCH_HEADER_SIZE {
        return;
    }
    let len = u32::from_le_bytes(log[at + 2..at + 6].try_into().unwrap()) as usize;
    let start = at + BATCH_HEADER_SIZE;
    if start + len <= log.len() {
        let crc = crc32fast::hash(&log[start..start + len]);
        log[at + 6..at + 10].copy_from_slice(&crc.to_le_bytes());
    }
    let header_crc = crc32fast::hash(&log[at..at + 14]);
    log[at + 14..at + 18].copy_from_slice(&header_crc.to_le_bytes());
}

/// Damaged logs never panic the frame reader or the classification of what stopped it, and
/// fail only with `WalCorruption`. A seekable source and a zstd stream of the same bytes (as for
/// cold segments, whose end is unknown) read exactly the same.
fn check_damaged_logs(seed: u64, cases: u64) {
    let mut rng = Rng::new(seed);
    let (log, offsets) = frames(1..=12);
    for case in 0..cases {
        let mut data = log.clone();
        damage(&mut rng, &mut data);
        if rng.below(2) == 0 {
            seal_frame(&mut data, offsets[rng.index(offsets.len())]);
        }
        // The first sequence of the segment: unknown, right, or wrong (which is corruption as
        // soon as a frame is decoded).
        let start = match rng.below(4) {
            0 => None,
            1 => Some(SequenceNumber::new(2 + rng.below(2))),
            _ => Some(SequenceNumber::new(1)),
        };
        let from = SequenceNumber::new(rng.below(16));
        let budget = ReadBudget::new(1 + rng.index(20), 1 + rng.below(4096));

        let mut source = BufReader::new(Cursor::new(&data[..]));
        let seekable = read_frames(
            &mut source,
            Some(data.len() as u64),
            start,
            from,
            &mut budget.clone(),
        );
        match &seekable {
            Ok(read) => {
                assert!(read.ops.iter().all(|op| op.seq > from), "case {case}");
                if let Some((at, anomaly)) = &read.anomaly {
                    let end = data.len() as u64;
                    if let Err(error) = classify_in(&mut source, *at, anomaly.clone(), end) {
                        assert!(matches!(error, ServerError::WalCorruption(_)), "{error:?}");
                    }
                }
            }
            Err(error) => assert!(
                matches!(error, ServerError::WalCorruption(_)),
                "case {case}: {error:?}"
            ),
        }

        let compressed = zstd::encode_all(&data[..], 1).unwrap();
        let mut decoder = zstd::stream::read::Decoder::new(&compressed[..]).unwrap();
        let streamed = read_frames(&mut decoder, None, start, from, &mut budget.clone());
        assert_eq!(
            format!("{streamed:?}"),
            format!("{seekable:?}"),
            "case {case}: {data:02x?}"
        );
    }
}

#[test]
fn damaged_logs_never_panic_the_frame_reader() {
    check_damaged_logs(0xC0FF_EE01, 2_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn damaged_logs_never_panic_the_frame_reader_long() {
    check_damaged_logs(0xC0FF_EE02, 500_000);
}
