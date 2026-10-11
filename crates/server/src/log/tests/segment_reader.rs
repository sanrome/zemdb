use super::*;
use std::io::Cursor;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::wal_frame::encode_wal_record;
use zemdb_core::value::{PrimaryKey, Value};

fn op(seq: u64) -> SequencedOperation {
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    SequencedOperation::new(SequenceNumber::new(seq), Operation::delete(1, pk, seq))
}

/// A segment with one frame per operation `start..=end`, and the offset of each frame.
fn segment(seqs: std::ops::RangeInclusive<u64>) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for seq in seqs {
        offsets.push(bytes.len());
        bytes.extend_from_slice(&encode_wal_record(&op(seq), None).unwrap());
    }
    (bytes, offsets)
}

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

fn seqs(read: &FrameRead) -> Vec<u64> {
    read.ops.iter().map(|op| op.seq.get()).collect()
}

#[test]
fn frames_before_the_cursor_are_skipped_without_decoding() {
    let (mut bytes, offsets) = segment(1..=10);
    let full = read(&bytes, Some(1), 7, &mut ReadBudget::ops(usize::MAX));
    // Damage the payload of every frame up to 7: decoding any of them would fail its checksum.
    for &offset in &offsets[..7] {
        bytes[offset + BATCH_HEADER_SIZE] ^= 0xFF;
    }

    let tail = read(&bytes, Some(1), 7, &mut ReadBudget::ops(usize::MAX));

    assert_eq!(seqs(&tail), vec![8, 9, 10]);
    assert_eq!(tail.ops, full.ops);
    assert!(tail.anomaly.is_none());
}

#[test]
fn without_the_first_sequence_every_frame_is_decoded() {
    let (mut bytes, offsets) = segment(1..=4);
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
    let (bytes, _) = segment(1..=10);
    let one = op_bytes(&op(1));

    let read = read(&bytes, Some(1), 2, &mut ReadBudget::new(100, 3 * one));

    assert_eq!(seqs(&read), vec![3, 4, 5]);
}

#[test]
fn a_skipped_frame_past_the_end_is_an_anomaly() {
    let (bytes, offsets) = segment(1..=3);
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
    let (bytes, _) = segment(5..=8);
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
    let (mut bytes, offsets) = segment(1..=10);
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

    assert_eq!(seqs(&read), vec![8, 9, 10]);
}
