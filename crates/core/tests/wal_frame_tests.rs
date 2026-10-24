use std::fmt::Debug;
use std::io::Cursor;
use zemdb_core::id::MutationId;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::{
    decode_batch_payload, decode_wal_batch_from_slice, encode_wal_batch, find_valid_frame,
    parse_batch_header, replay_wal_records, BatchPayloadDecode, FrameAnomaly, FrameSearch,
    FrameSearchStep, InvalidHeader, WalBatchDecodeResult, WalFrameError, BATCH_HEADER_SIZE,
    BATCH_MAGIC,
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
