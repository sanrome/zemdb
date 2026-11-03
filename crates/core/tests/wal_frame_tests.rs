use std::fmt::Debug;
use std::io::Cursor;
use zemdb_core::id::MutationId;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::{
    decode_batch_payload, decode_wal_batch_from_slice, encode_wal_batch, encode_wal_batch_in_group,
    find_frame_after_group, find_valid_frame, parse_batch_header, replay_wal_records,
    wal_record_payload_len, BatchPayloadDecode, FrameAnomaly, FrameSearch, FrameSearchStep,
    InvalidHeader, WalBatchDecodeResult, WalFrameError, BATCH_HEADER_SIZE, BATCH_MAGIC,
};
use zemdb_core::value::{CompactRow, PrimaryKey, Value};

fn delete_frame(seq: u64) -> Vec<u8> {
    let op = SequencedOperation::new(seq, Operation::delete(0, PrimaryKey::single(1i64), 100));
    encode_wal_batch(&[op], None).expect("encode ok")
}

fn header_of(frame: &[u8]) -> &[u8; BATCH_HEADER_SIZE] {
    frame.first_chunk().unwrap()
}

fn bytes_frame(seq: u64, bytes: &[u8]) -> Vec<u8> {
    let row = CompactRow::new(vec![Value::Int(1), Value::Bytes(bytes.into())]);
    let op = SequencedOperation::new(
        seq,
        Operation::insert(0, PrimaryKey::single(1i64), row, 100),
    );
    encode_wal_batch(&[op], None).expect("encode ok")
}

/// Writes the checksum of the other header fields into the last 4 bytes of `header`.
fn seal_header(mut header: [u8; BATCH_HEADER_SIZE]) -> [u8; BATCH_HEADER_SIZE] {
    let crc = crc32fast::hash(&header[..BATCH_HEADER_SIZE - 4]);
    header[BATCH_HEADER_SIZE - 4..].copy_from_slice(&crc.to_le_bytes());
    header
}

/// Flips a bit of the payload length in the header of the frame at `offset`.
fn flip_length_bit(log: &mut [u8], offset: usize) {
    log[offset + 2] ^= 0x01;
}

fn assert_corruption<T: Debug>(result: Result<T, WalFrameError>) {
    assert!(
        matches!(result, Err(WalFrameError::Corruption(_))),
        "expected corruption, got {result:?}"
    );
}

fn assert_torn(result: Result<WalBatchDecodeResult, WalFrameError>) {
    assert!(
        matches!(result, Ok(WalBatchDecodeResult::TornWrite { .. })),
        "expected a torn write, got {result:?}"
    );
}

#[test]
fn test_wal_frame_roundtrip() {
    let cases = [
        (
            CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]),
            None,
        ),
        (
            CompactRow::new(vec![Value::Int(42)]),
            Some(MutationId::new([7u8; 16])),
        ),
    ];
    for (row, mutation_id) in cases {
        let op =
            SequencedOperation::new(1, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
        let batch = vec![op];

        let encoded = encode_wal_batch(&batch, mutation_id).expect("encode ok");
        assert_eq!(&encoded[0..2], &BATCH_MAGIC);

        let WalBatchDecodeResult::Ok {
            ops,
            mutation_id: recovered,
            bytes_consumed,
        } = decode_wal_batch_from_slice(&encoded).expect("decode ok")
        else {
            panic!("expected a decoded batch");
        };
        assert_eq!(ops, batch);
        assert_eq!(recovered, mutation_id);
        assert_eq!(bytes_consumed, encoded.len());
    }
}

#[test]
fn a_delete_carrying_its_owner_round_trips_in_a_wal_frame() {
    let batch = vec![
        SequencedOperation::new(
            1,
            Operation::delete_with_owner(0, PrimaryKey::single(1i64), "alice", 100),
        ),
        SequencedOperation::new(2, Operation::delete(0, PrimaryKey::single(2i64), 100)),
    ];
    let encoded = encode_wal_batch(&batch, Some(MutationId::new([3u8; 16]))).expect("encode ok");
    let WalBatchDecodeResult::Ok { ops, .. } =
        decode_wal_batch_from_slice(&encoded).expect("decode ok")
    else {
        panic!("expected a decoded batch");
    };
    assert_eq!(ops, batch);
}

#[test]
fn test_wal_frame_crc_mismatch_at_eof_treated_as_torn_write() {
    let mut frame = delete_frame(1);
    *frame.last_mut().unwrap() ^= 0xFF;
    assert_torn(decode_wal_batch_from_slice(&frame));
}

#[test]
fn test_wal_frame_torn_write_detection() {
    assert_torn(decode_wal_batch_from_slice(&delete_frame(1)[..5]));
}

#[test]
fn test_wal_frame_zero_filled_eof_torn_write_detection() {
    // Zero-filled tail at EOF (e.g., 64 zero bytes from power outage fallocate)
    let zeros = vec![0u8; 64];
    let res = decode_wal_batch_from_slice(&zeros).expect("returns TornWrite for zero tail");
    assert!(matches!(res, WalBatchDecodeResult::TornWrite { .. }));
}

#[test]
fn header_and_payload_decode_a_frame() {
    let frame = delete_frame(1);

    let header = parse_batch_header(header_of(&frame)).unwrap();
    assert_eq!(header.frame_len(), frame.len());
    let decoded = decode_batch_payload(&header, &frame[BATCH_HEADER_SIZE..]).unwrap();

    assert!(matches!(decoded, BatchPayloadDecode::Batch { ops, .. } if ops.len() == 1));
}

#[test]
fn header_with_invalid_magic_is_invalid() {
    let mut frame = delete_frame(1);
    frame[0] = 0;
    assert!(matches!(
        parse_batch_header(header_of(&frame)),
        Err(InvalidHeader::Magic(_))
    ));
    assert!(matches!(
        parse_batch_header(&[0u8; BATCH_HEADER_SIZE]),
        Err(InvalidHeader::Magic([0, 0]))
    ));
}

#[test]
fn header_checksum_covers_every_field() {
    let frame = delete_frame(1);
    for byte in 2..BATCH_HEADER_SIZE {
        let mut damaged = frame.clone();
        damaged[byte] ^= 0x01;
        assert!(
            matches!(
                parse_batch_header(header_of(&damaged)),
                Err(InvalidHeader::Checksum { .. })
            ),
            "a flipped bit in byte {byte} must fail the header checksum"
        );
    }
}

#[test]
fn header_with_checksummed_oversized_length_is_invalid() {
    let mut header = [0u8; BATCH_HEADER_SIZE];
    header[0..2].copy_from_slice(&BATCH_MAGIC);
    header[2..6].copy_from_slice(&u32::MAX.to_le_bytes());
    let header = seal_header(header);

    assert_eq!(
        parse_batch_header(&header),
        Err(InvalidHeader::PayloadTooLong(u32::MAX))
    );
}

#[test]
fn payload_with_wrong_checksum_is_reported_not_decoded() {
    let mut frame = delete_frame(1);
    *frame.last_mut().unwrap() ^= 0xFF;

    let header = parse_batch_header(header_of(&frame)).unwrap();
    let decoded = decode_batch_payload(&header, &frame[BATCH_HEADER_SIZE..]).unwrap();

    assert!(matches!(
        decoded,
        BatchPayloadDecode::ChecksumMismatch { .. }
    ));
}

#[test]
fn anomaly_is_corruption_only_when_a_valid_frame_follows() {
    let anomaly = FrameAnomaly::InvalidHeader(InvalidHeader::Magic([0, 0]));
    assert!(anomaly.clone().classify(None).is_ok());
    assert!(matches!(
        anomaly.classify(Some(40)),
        Err(WalFrameError::Corruption(_))
    ));
}

#[test]
fn search_finds_only_complete_valid_frames() {
    let frame = delete_frame(2);
    // The magic and one more byte: the start of a header that is not complete.
    let mut log = [&BATCH_MAGIC[..], &[0x01]].concat();
    let at = log.len() as u64;
    log.extend_from_slice(&frame);
    let end = log.len() as u64;

    let mut reader = Cursor::new(&log);
    assert_eq!(find_valid_frame(&mut reader, 0, end).unwrap(), Some(at));
    assert_eq!(find_valid_frame(&mut reader, at + 1, end).unwrap(), None);
    // A frame that does not end before `end` is not complete.
    assert_eq!(find_valid_frame(&mut reader, 0, end - 1).unwrap(), None);

    let mut damaged = log.clone();
    *damaged.last_mut().unwrap() ^= 0xFF;
    assert_eq!(
        find_valid_frame(&mut Cursor::new(&damaged), 0, end).unwrap(),
        None
    );
}

#[test]
fn search_finds_frames_across_window_edges() {
    let frame = delete_frame(2);
    // Frames that start just before, at, and after the edge of the 64 KiB scan window.
    for start in [64 * 1024 - 5, 64 * 1024 - 1, 64 * 1024, 64 * 1024 + 3] {
        let mut log = vec![0x55u8; start];
        log.extend_from_slice(&frame);
        let found = find_valid_frame(&mut Cursor::new(&log), 0, log.len() as u64).unwrap();
        assert_eq!(found, Some(start as u64), "frame at {start}");
    }
}

/// Runs a [`FrameSearch`] over `log`, checking that each read it asks for lies within
/// `from..end`, and returns its result and how many reads it asked for.
fn search_by_steps(log: &[u8], from: u64, end: u64) -> (Option<u64>, usize) {
    let mut search = FrameSearch::new(from, end);
    let mut reads = 0;
    loop {
        match search.step() {
            FrameSearchStep::Read { offset, len } => {
                assert!(
                    offset >= from && offset + len as u64 <= end,
                    "{offset}+{len}"
                );
                search.feed(&log[offset as usize..][..len]);
                reads += 1;
            }
            FrameSearchStep::Done(found) => return (found, reads),
        }
    }
}

#[test]
fn search_by_steps_finds_what_the_reader_search_finds() {
    let small = delete_frame(2);
    // A payload of several chunks, valid and damaged.
    let large = bytes_frame(3, &vec![0x33; 200 * 1024]);
    let mut damaged_large = large.clone();
    *damaged_large.last_mut().unwrap() ^= 0xFF;
    let mut logs = Vec::new();
    for start in [0, 7, 64 * 1024 - 5, 64 * 1024, 64 * 1024 + 3, 3 * 64 * 1024] {
        for frame in [&small, &large, &damaged_large] {
            let mut log = vec![0x55u8; start];
            log.extend_from_slice(frame);
            logs.push(log.clone());
            log.extend_from_slice(&small);
            logs.push(log);
        }
    }
    for log in &logs {
        let end = log.len() as u64;
        for from in [
            0,
            1,
            end / 2,
            end.saturating_sub(BATCH_HEADER_SIZE as u64),
            end,
        ] {
            let expected = find_valid_frame(&mut Cursor::new(log), from, end).unwrap();
            let (found, _) = search_by_steps(log, from, end);
            assert_eq!(found, expected, "log of {end} bytes from {from}");
        }
    }

    // The large payload is checksummed a chunk at a time: one window and four chunks.
    let (found, reads) = search_by_steps(&large, 0, large.len() as u64);
    assert_eq!(found, Some(0));
    assert_eq!(reads, 5);
    let (found, _) = search_by_steps(&damaged_large, 0, damaged_large.len() as u64);
    assert_eq!(found, None);
}

#[test]
#[should_panic(expected = "fed a window of the wrong length")]
fn search_by_steps_refuses_bytes_of_another_length() {
    let frame = delete_frame(2);
    let mut search = FrameSearch::new(0, frame.len() as u64);
    assert!(matches!(search.step(), FrameSearchStep::Read { .. }));
    search.feed(&frame[1..]);
}

#[test]
fn flipped_length_in_the_middle_is_corruption() {
    let frames = [delete_frame(1), delete_frame(2), delete_frame(3)];
    let second = frames[0].len();
    let mut log = frames.concat();
    flip_length_bit(&mut log, second);

    assert_corruption(decode_wal_batch_from_slice(&log[second..]));
    assert_corruption(replay_wal_records(&log));
}

#[test]
fn length_pointing_past_the_end_in_the_middle_is_corruption() {
    let frames = [delete_frame(1), delete_frame(2), delete_frame(3)];
    let second = frames[0].len();
    let mut log = frames.concat();
    log[second + 2..second + 6].copy_from_slice(&1_000_000u32.to_le_bytes());

    assert_corruption(decode_wal_batch_from_slice(&log[second..]));
}

#[test]
fn zeroed_header_followed_by_garbage_is_a_torn_write() {
    let mut tail = vec![0u8; BATCH_HEADER_SIZE];
    tail.extend_from_slice(&[0x13; 100]);
    tail.extend_from_slice(&BATCH_MAGIC);
    tail.extend_from_slice(&[0x01, 0x02]);

    assert_torn(decode_wal_batch_from_slice(&tail));
}

#[test]
fn zeroed_header_followed_by_a_valid_frame_is_corruption() {
    let mut log = vec![0u8; BATCH_HEADER_SIZE];
    log.extend_from_slice(&[0x13; 7]);
    log.extend_from_slice(&delete_frame(2));

    assert_corruption(decode_wal_batch_from_slice(&log));
}

#[test]
fn checksum_mismatch_before_a_partial_frame_is_a_torn_write() {
    let mut log = delete_frame(1);
    *log.last_mut().unwrap() ^= 0xFF;
    let next = delete_frame(2);
    log.extend_from_slice(&next[..next.len() - 1]);

    assert_torn(decode_wal_batch_from_slice(&log));
}

#[test]
fn checksum_mismatch_before_a_later_valid_frame_is_corruption() {
    // The valid frame may follow right after the damaged one or after some garbage.
    for gap in [0, 9] {
        let mut log = delete_frame(1);
        *log.last_mut().unwrap() ^= 0xFF;
        log.extend(std::iter::repeat_n(0x13, gap));
        log.extend_from_slice(&delete_frame(2));

        assert_corruption(decode_wal_batch_from_slice(&log));
    }
}

#[test]
fn frame_nested_in_a_damaged_payload_is_not_a_following_frame() {
    // A client can store the bytes of a valid frame as a value.
    let frame = bytes_frame(1, &delete_frame(9));
    let nested = find_valid_frame(&mut Cursor::new(&frame), 1, frame.len() as u64).unwrap();
    assert!(nested.is_some(), "the payload holds a complete valid frame");

    let mut damaged = frame.clone();
    *damaged.last_mut().unwrap() ^= 0xFF;
    assert_torn(decode_wal_batch_from_slice(&damaged));
    assert_torn(decode_wal_batch_from_slice(&frame[..frame.len() - 1]));
}

#[test]
fn frame_nested_after_an_invalid_header_is_found() {
    // Without its header, nothing tells the payload of the damaged frame apart from frames
    // written after it. Reporting corruption truncates nothing.
    let mut log = delete_frame(1);
    let second = log.len();
    log.extend_from_slice(&bytes_frame(2, &delete_frame(9)));
    log[second..second + BATCH_HEADER_SIZE].fill(0);

    assert_corruption(decode_wal_batch_from_slice(&log[second..]));
}

/// Length of a frame written by `fake_headers`: a header and 14 bytes of padding.
const FAKE_FRAME_LEN: usize = BATCH_HEADER_SIZE + 14;

/// A header with a valid checksum declaring a 4-byte payload whose checksum does not match.
fn fake_header() -> [u8; BATCH_HEADER_SIZE] {
    let mut header = [0u8; BATCH_HEADER_SIZE];
    header[0..2].copy_from_slice(&BATCH_MAGIC);
    header[2..6].copy_from_slice(&4u32.to_le_bytes());
    header[6..10].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    header[10..14].copy_from_slice(&1u32.to_le_bytes());
    seal_header(header)
}

fn fake_headers(count: usize) -> Vec<u8> {
    let mut log = Vec::new();
    for _ in 0..count {
        log.extend_from_slice(&fake_header());
        log.extend_from_slice(&[0x13; FAKE_FRAME_LEN - BATCH_HEADER_SIZE]);
    }
    log
}

#[test]
fn search_checks_a_bounded_number_of_payloads() {
    // Up to 16 failed payload checks, the search goes on and finds nothing.
    let log = fake_headers(16);
    assert_eq!(
        find_valid_frame(&mut Cursor::new(&log), 0, log.len() as u64).unwrap(),
        None
    );

    // Past them, the next valid header is taken as a frame without reading its payload.
    let log = fake_headers(17);
    assert_eq!(
        find_valid_frame(&mut Cursor::new(&log), 0, log.len() as u64).unwrap(),
        Some(16 * FAKE_FRAME_LEN as u64)
    );
}

// ---------------------------------------------------------------------------------------------
// Groups of frames
// ---------------------------------------------------------------------------------------------

fn delete_op(seq: u64) -> SequencedOperation {
    SequencedOperation::new(seq, Operation::delete(0, PrimaryKey::single(1i64), 100))
}

/// The frames of `seqs`, one operation each, written as one group, and where each one starts.
fn group(seqs: std::ops::RangeInclusive<u64>) -> (Vec<u8>, Vec<usize>) {
    let mut log = Vec::new();
    let mut starts = Vec::new();
    for seq in seqs {
        starts.push(log.len());
        let frame = encode_wal_batch_in_group(&[delete_op(seq)], None, log.len() as u64).unwrap();
        log.extend_from_slice(&frame);
    }
    (log, starts)
}

/// `log` with the frames at `starts`, as returned by `group`, shifted by `by` bytes.
fn after(by: usize, (log, starts): (Vec<u8>, Vec<usize>)) -> (Vec<u8>, Vec<usize>) {
    (log, starts.into_iter().map(|start| start + by).collect())
}

/// Where the first frame of a later group than the frame at `at` starts, after damage that makes
/// its header invalid, in `log`.
fn after_group(log: &[u8], at: usize) -> Option<u64> {
    let anomaly = FrameAnomaly::InvalidHeader(InvalidHeader::Magic([0, 0]));
    find_frame_after_group(&mut Cursor::new(log), at as u64, &anomaly, log.len() as u64).unwrap()
}

/// Zeroes the header of the frame at `at`.
fn zero_header(log: &mut [u8], at: usize) {
    log[at..at + BATCH_HEADER_SIZE].fill(0);
}

#[test]
fn a_frame_of_a_group_says_where_its_group_starts() {
    let op = delete_op(3);
    for (offset, id) in [
        (0, None),
        (1, Some(MutationId::new([5u8; 16]))),
        (u64::MAX, None),
    ] {
        let frame = encode_wal_batch_in_group(std::slice::from_ref(&op), id, offset).unwrap();
        let header = parse_batch_header(header_of(&frame)).unwrap();
        let decoded = decode_batch_payload(&header, &frame[BATCH_HEADER_SIZE..]).unwrap();
        assert_eq!(
            decoded,
            BatchPayloadDecode::Batch {
                ops: vec![op.clone()],
                mutation_id: id,
                group_offset: offset,
            }
        );
        // Where the group starts does not change the size of the record.
        assert_eq!(
            wal_record_payload_len(&op, id).unwrap() as usize,
            frame.len() - BATCH_HEADER_SIZE
        );
    }
    // A frame written alone starts its group.
    let frame = encode_wal_batch(&[op], None).unwrap();
    let header = parse_batch_header(header_of(&frame)).unwrap();
    assert!(matches!(
        decode_batch_payload(&header, &frame[BATCH_HEADER_SIZE..]).unwrap(),
        BatchPayloadDecode::Batch {
            group_offset: 0,
            ..
        }
    ));
}

#[test]
fn valid_frames_of_the_damaged_group_after_the_damage_are_not_a_later_write() {
    // A group after a frame of its own, damaged in its middle, in its first frame, or in both,
    // with frames of the group after the damage.
    let first = delete_frame(1);
    let (mut log, starts) = after(first.len(), group(2..=6));
    log.splice(0..0, first.iter().copied());
    for damaged in [vec![2], vec![0], vec![0, 2], vec![1, 3]] {
        let mut image = log.clone();
        for &frame in &damaged {
            zero_header(&mut image, starts[frame]);
        }
        let at = starts[damaged[0]];
        assert_eq!(after_group(&image, at), None, "damaged {damaged:?}");
        // The strict search takes the next frame of the group for a later write.
        assert!(
            find_valid_frame(&mut Cursor::new(&image), at as u64 + 1, image.len() as u64)
                .unwrap()
                .is_some()
        );
    }

    // A group at the start of the log, its first frame damaged.
    let (mut log, starts) = group(1..=4);
    zero_header(&mut log, starts[0]);
    assert_eq!(after_group(&log, 0), None);
}

#[test]
fn a_valid_frame_of_a_later_group_after_the_damage_is_found() {
    // Two groups: 1..=3 and 4..=6.
    let (mut log, mut starts) = group(1..=3);
    let (second, second_starts) = after(log.len(), group(4..=6));
    log.extend_from_slice(&second);
    starts.extend(second_starts);

    // Damage in the first group: the second group follows it.
    let mut image = log.clone();
    zero_header(&mut image, starts[1]);
    assert_eq!(after_group(&image, starts[1]), Some(starts[3] as u64));

    // Damage in the last frame of the first group and the first of the second: the rest of the
    // second group still starts after the damage.
    let mut image = log.clone();
    image[starts[2]..starts[4]].fill(0);
    assert_eq!(after_group(&image, starts[2]), Some(starts[4] as u64));

    // The same at the start of the log.
    let mut image = log.clone();
    image[..starts[4]].fill(0);
    assert_eq!(after_group(&image, 0), Some(starts[4] as u64));

    // A frame written alone after a damaged group starts a later group.
    let mut image = log[..starts[3]].to_vec();
    zero_header(&mut image, starts[1]);
    image.extend_from_slice(&delete_frame(4));
    assert_eq!(after_group(&image, starts[1]), Some(starts[3] as u64));

    // A frame that says its group starts before the start of the log is not of the group.
    let mut image = vec![0u8; BATCH_HEADER_SIZE];
    image.extend_from_slice(&encode_wal_batch_in_group(&[delete_op(2)], None, 1_000).unwrap());
    assert_eq!(after_group(&image, 0), Some(BATCH_HEADER_SIZE as u64));
}

#[test]
fn frames_nested_in_a_frame_of_the_damaged_group_are_not_looked_at() {
    // The second frame of the group holds, as a value, a frame that starts a group.
    let first = encode_wal_batch_in_group(&[delete_op(1)], None, 0).unwrap();
    let row = CompactRow::new(vec![Value::Int(1), Value::Bytes(delete_frame(9).into())]);
    let op = SequencedOperation::new(2, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
    let second = encode_wal_batch_in_group(&[op], None, first.len() as u64).unwrap();
    let mut log = [first, second].concat();
    zero_header(&mut log, 0);
    assert_eq!(after_group(&log, 0), None);
}

/// A header with a valid checksum declaring a payload of `len` bytes whose checksum does not
/// match, and padding up to `FAKE_FRAME_LEN`.
fn long_fake_header(len: u32) -> Vec<u8> {
    let mut header = fake_header();
    header[2..6].copy_from_slice(&len.to_le_bytes());
    let mut frame = seal_header(header).to_vec();
    frame.extend_from_slice(&[0x13; FAKE_FRAME_LEN - BATCH_HEADER_SIZE]);
    frame
}

#[test]
fn the_frames_of_a_torn_group_do_not_spend_the_limit_of_failed_checks() {
    // A group of 30 frames whose first header was lost and whose other payloads were each
    // damaged: 29 failed checks, more than the limit, all within the bytes of the group.
    let (mut log, starts) = group(1..=30);
    zero_header(&mut log, 0);
    for &start in &starts[1..] {
        log[start + BATCH_HEADER_SIZE] ^= 0xFF;
    }
    assert_eq!(after_group(&log, 0), None);
}

#[test]
fn the_search_after_a_group_spends_one_limit_of_failed_checks_past_its_allowance() {
    // After the damage: 10 headers whose payloads, of 4 KiB each, overlap and fail their checks,
    // a frame of the group, 10 more such headers, another frame of the group, and room for the
    // payloads. The allowance, the bytes after the damage, takes the first failed check; the
    // limit counts the next 16, across the frame of the group, and the 18th header is returned
    // without checking its frame.
    const PAYLOAD: u32 = 4096;
    let mut log = delete_frame(1);
    zero_header(&mut log, 0);
    let mut fakes = Vec::new();
    let mut fake_run = |log: &mut Vec<u8>| {
        for _ in 0..10 {
            fakes.push(log.len());
            log.extend_from_slice(&long_fake_header(PAYLOAD));
        }
    };
    let frame = |log: &mut Vec<u8>| {
        let frame = encode_wal_batch_in_group(&[delete_op(2)], None, log.len() as u64).unwrap();
        log.extend_from_slice(&frame);
    };
    fake_run(&mut log);
    frame(&mut log);
    fake_run(&mut log);
    frame(&mut log);
    log.extend(std::iter::repeat_n(
        0x13,
        PAYLOAD as usize + BATCH_HEADER_SIZE,
    ));

    // The search starts after the first byte of the damaged header.
    let absorbed = (log.len() as u64 - 1) / u64::from(PAYLOAD);
    assert_eq!(absorbed, 1);
    assert_eq!(after_group(&log, 0), Some(fakes[1 + 16] as u64));
}

/// A value of `len` bytes that holds, at its start, the header of a frame that says its group
/// starts `delta` bytes before it (where the record that holds the value starts), with no
/// operations, and whose payload is the rest of the value and then `next`, the first bytes of
/// the frame written after the record.
fn crafted_value(len: usize, delta: u64, next: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&delta.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&0u64.to_le_bytes());
    payload.resize(len - BATCH_HEADER_SIZE, 0x13);
    payload.extend_from_slice(next);
    let mut header = [0u8; BATCH_HEADER_SIZE];
    header[0..2].copy_from_slice(&BATCH_MAGIC);
    header[2..6].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[6..10].copy_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    let mut value = seal_header(header).to_vec();
    value.extend_from_slice(&payload[..payload.len() - next.len()]);
    value
}

/// A log of a frame, a frame alone whose last value is a frame stored as a value (see
/// `crafted_value`) that covers the first `covered` bytes of the frame after it, and that later
/// frame, alone too; then one bit of the header of the second frame is damaged. Returns the log,
/// where the damage is and where the later frame starts.
fn log_with_a_crafted_value(covered: usize) -> (Vec<u8>, usize, usize) {
    let record = |seq: u64, value: Vec<u8>| {
        let row = CompactRow::new(vec![Value::Int(1), Value::Bytes(value.into())]);
        let op = SequencedOperation::new(
            seq,
            Operation::insert(0, PrimaryKey::single(1i64), row, 100),
        );
        encode_wal_batch(&[op], None).unwrap()
    };
    let first = delete_frame(1);
    let later = delete_frame(3);
    let len = 200;
    let delta = record(2, vec![0x55; len]).len() - len;
    let hostile = record(2, crafted_value(len, delta as u64, &later[..covered]));
    let at = first.len();
    let later_at = at + hostile.len();
    let mut log = [first, hostile, later].concat();
    log[at + BATCH_HEADER_SIZE - 1] ^= 0x01;
    (log, at, later_at)
}

#[test]
fn a_frame_stored_as_a_value_does_not_hide_the_start_of_the_frame_after_it() {
    // It covers the magic of the later frame, which anyone can predict.
    for covered in [1, 2, BATCH_HEADER_SIZE - 1] {
        let (log, at, later_at) = log_with_a_crafted_value(covered);
        assert_eq!(after_group(&log, at), Some(later_at as u64), "{covered}");
    }
    // Without it, the later frame is found too.
    let (mut log, at, later_at) = log_with_a_crafted_value(1);
    log[at + BATCH_HEADER_SIZE..later_at].fill(0x55);
    assert_eq!(after_group(&log, at), Some(later_at as u64));
}

#[test]
fn a_frame_stored_as_a_value_that_predicts_the_whole_header_after_it_hides_that_frame() {
    // The risk accepted with the search after a group: whoever can predict the whole header of
    // the next frame (in practice, the member who writes it too) can hide it after damage to the
    // header of the frame that holds the value.
    let (log, at, _) = log_with_a_crafted_value(BATCH_HEADER_SIZE);
    assert_eq!(after_group(&log, at), None);
}

#[test]
fn a_later_frame_right_after_a_frame_of_the_group_that_crosses_the_window_is_found() {
    // The search reads a new window where the frame of the group ends.
    let first = encode_wal_batch_in_group(&[delete_op(1)], None, 0).unwrap();
    let row = CompactRow::new(vec![Value::Int(1), Value::Bytes(vec![0x13; 70_000].into())]);
    let op = SequencedOperation::new(2, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
    let big = encode_wal_batch_in_group(&[op], None, first.len() as u64).unwrap();
    let later = encode_wal_batch(&[delete_op(3)], None).unwrap();
    let later_at = first.len() + big.len();
    let mut log = [first, big, later].concat();
    zero_header(&mut log, 0);
    assert_eq!(after_group(&log, 0), Some(later_at as u64));
}

#[test]
fn after_a_damaged_frame_with_a_valid_header_the_search_starts_where_it_ends() {
    // A frame stored as a value in its payload, which starts a group of its own, is not looked
    // at, as in the strict search.
    let first = bytes_frame(1, &delete_frame(9));
    let second = encode_wal_batch_in_group(&[delete_op(2)], None, first.len() as u64).unwrap();
    let mut log = [first.clone(), second].concat();
    log[BATCH_HEADER_SIZE] ^= 0xFF;
    let anomaly = FrameAnomaly::ChecksumMismatch {
        expected: 0,
        actual: 1,
        frame_len: first.len(),
    };
    assert_eq!(
        find_frame_after_group(&mut Cursor::new(&log), 0, &anomaly, log.len() as u64).unwrap(),
        None
    );
}

#[test]
fn a_valid_frame_after_the_damage_that_does_not_decode_is_not_of_the_group() {
    // Where its group starts, and then an option tag that is neither `None` nor `Some`.
    let mut payload = vec![0u8; 8];
    payload.push(2);
    payload.extend_from_slice(&[0u8; 8]);
    let mut header = [0u8; BATCH_HEADER_SIZE];
    header[0..2].copy_from_slice(&BATCH_MAGIC);
    header[2..6].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[6..10].copy_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    header[10..14].copy_from_slice(&1u32.to_le_bytes());
    let header = seal_header(header);
    assert!(decode_batch_payload(&parse_batch_header(&header).unwrap(), &payload).is_err());

    let mut log = vec![0u8; BATCH_HEADER_SIZE];
    log.extend_from_slice(&header);
    log.extend_from_slice(&payload);
    assert_eq!(after_group(&log, 0), Some(BATCH_HEADER_SIZE as u64));
}

#[test]
fn a_frame_stored_in_a_torn_frame_of_the_group_is_found() {
    // A frame of the group whose header survived and whose payload did not fails its check, and
    // the search goes on inside its payload, where a frame stored as a value, which starts a
    // group of its own, is taken for a later write: corruption, which truncates nothing (the
    // risk accepted with D79, for any torn frame of the group).
    let first = encode_wal_batch_in_group(&[delete_op(1)], None, 0).unwrap();
    let row = CompactRow::new(vec![Value::Int(1), Value::Bytes(delete_frame(9).into())]);
    let op = SequencedOperation::new(2, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
    let second = encode_wal_batch_in_group(&[op], None, first.len() as u64).unwrap();
    let mut log = [first.clone(), second].concat();
    zero_header(&mut log, 0);
    log[first.len() + BATCH_HEADER_SIZE] ^= 0xFF;
    assert!(after_group(&log, 0).is_some());
}

/// A reader that counts the bytes read through it.
struct Counting<'a> {
    inner: Cursor<&'a [u8]>,
    read: usize,
}

impl std::io::Read for Counting<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n;
        Ok(n)
    }
}

impl std::io::Seek for Counting<'_> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[test]
fn the_search_after_a_group_reads_its_frames_about_once() {
    // A damaged frame followed by a thousand small frames of its group: the search goes on from
    // its window after each one instead of reading a window per frame.
    let (mut log, _) = group(1..=1_000);
    zero_header(&mut log, 0);
    let anomaly = FrameAnomaly::InvalidHeader(InvalidHeader::Magic([0, 0]));
    let mut reader = Counting {
        inner: Cursor::new(&log),
        read: 0,
    };
    let found = find_frame_after_group(&mut reader, 0, &anomaly, log.len() as u64).unwrap();
    assert_eq!(found, None);
    assert!(
        reader.read <= 4 * log.len(),
        "{} bytes read for a log of {}",
        reader.read,
        log.len()
    );
}
