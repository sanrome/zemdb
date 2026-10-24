use super::*;
use crate::memory::state::RoomSnapshotPayload;
use crate::{MemoryStorageEngine, StorageEngine};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};

use crate::memory::index::tests::Rng;

fn compressed(bytes: &[u8]) -> Vec<u8> {
    zstd::encode_all(bytes, 3).unwrap()
}

/// Runs `check` once with each codec.
macro_rules! with_each_codec {
    ($check:ident) => {
        $check::<Libzstd>();
        $check::<Ruzstd>();
    };
}

fn corruption_message<T: std::fmt::Debug>(result: Result<T, StorageError>) -> String {
    match result {
        Err(StorageError::SnapshotCorruption(msg)) => msg,
        other => panic!("expected a snapshot corruption error, got {other:?}"),
    }
}

/// An envelope of `body` whose header declares `declared` bytes once decompressed and matches
/// the body's checksum.
fn sealed(compression: SnapshotCompression, body: &[u8], declared: u32) -> Vec<u8> {
    let header = SnapshotEnvelopeHeader::for_body(compression, declared, body);
    let mut envelope = header.to_bytes().to_vec();
    envelope.extend_from_slice(body);
    envelope
}

fn zstd_envelope(body: &[u8], declared: usize) -> Vec<u8> {
    sealed(SnapshotCompression::Zstd, body, declared as u32)
}

/// Decompresses the frame `body` with `Z`, declaring `declared` bytes.
fn decompress_with<Z: ZstdCodec>(body: &[u8], declared: usize) -> Result<Vec<u8>, StorageError> {
    decode_envelope_with::<Z>(&zstd_envelope(body, declared), u64::MAX)
}

// Frames written by hand (RFC 8878), to reach the fields that neither encoder writes.

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// A raw block of `content`.
fn raw_block(content: &[u8], last: bool) -> Vec<u8> {
    let header = (content.len() as u32) << 3 | u32::from(last);
    let mut block = header.to_le_bytes()[..3].to_vec();
    block.extend_from_slice(content);
    block
}

/// A block of `size` copies of `byte`.
fn rle_block(byte: u8, size: u32, last: bool) -> Vec<u8> {
    let header = size << 3 | 1 << 1 | u32::from(last);
    let mut block = header.to_le_bytes()[..3].to_vec();
    block.push(byte);
    block
}

/// A compressed block of `sequences` sequences, each a literal `b'A'` and a match of 65539
/// bytes at offset 1: it decompresses to `sequences * 65540` bytes of `b'A'` from a few bytes.
fn expanding_block(sequences: u8, last: bool) -> Vec<u8> {
    assert!(sequences < 32);
    // Raw literals of one-byte header, then the sequences: their count, every code in RLE mode
    // (literal length 1, offset code 0 for the last offset used, match length code 52), and a
    // bitstream of 16 zero extra bits per sequence before its end mark.
    let mut content = vec![sequences << 3];
    content.extend(std::iter::repeat_n(b'A', usize::from(sequences)));
    content.extend_from_slice(&[sequences, 0x54, 1, 0, 52]);
    content.extend(std::iter::repeat_n(0, 2 * usize::from(sequences)));
    content.push(0x01);
    let header = (content.len() as u32) << 3 | 2 << 1 | u32::from(last);
    let mut block = header.to_le_bytes()[..3].to_vec();
    block.extend_from_slice(&content);
    block
}

/// A compressed block of `size` literals of `b'B'`, in RLE, and no sequences: it decompresses to
/// `size` bytes from a block of eight.
fn literals_block(size: u32, last: bool) -> Vec<u8> {
    assert!(size < 1 << 20);
    // A literals section of RLE type with a three-byte header (20 bits of size), its byte, and
    // a sequences section of zero sequences.
    let literals_header = 1 | 3 << 2 | size << 4;
    let mut content = literals_header.to_le_bytes()[..3].to_vec();
    content.extend_from_slice(&[b'B', 0]);
    let header = (content.len() as u32) << 3 | 2 << 1 | u32::from(last);
    let mut block = header.to_le_bytes()[..3].to_vec();
    block.extend_from_slice(&content);
    block
}

/// A frame of `blocks` whose header is `descriptor` followed by `fields` (the window descriptor,
/// the dictionary id and the content size, as the descriptor says).
fn frame(descriptor: u8, fields: &[u8], blocks: &[Vec<u8>]) -> Vec<u8> {
    let mut frame = ZSTD_MAGIC.to_vec();
    frame.push(descriptor);
    frame.extend_from_slice(fields);
    for block in blocks {
        frame.extend_from_slice(block);
    }
    frame
}

/// A frame with a window of `window_descriptor` and no declared size.
fn windowed_frame(window_descriptor: u8, blocks: &[Vec<u8>]) -> Vec<u8> {
    frame(0x00, &[window_descriptor], blocks)
}

/// A single-segment frame declaring `content_size` (its window).
fn sized_frame(content_size: u64, blocks: &[Vec<u8>]) -> Vec<u8> {
    frame(0xE0, &content_size.to_le_bytes(), blocks)
}

/// The window descriptor of `2^log * (1 + mantissa / 8)` bytes.
fn window_descriptor(log: u8, mantissa: u8) -> u8 {
    (log - 10) << 3 | mantissa
}

/// A frame of 100 bytes in one raw block, with a window of `2^log * (1 + mantissa / 8)`.
fn small_frame_with_window(log: u8, mantissa: u8) -> Vec<u8> {
    windowed_frame(
        window_descriptor(log, mantissa),
        &[raw_block(&[7; 100], true)],
    )
}

/// Payloads around the block size (128 KiB), empty, incompressible and repetitive.
fn sample_payloads() -> Vec<Vec<u8>> {
    let mut rng = Rng::new(0x2257_D000);
    let random =
        |rng: &mut Rng, len: usize| -> Vec<u8> { (0..len).map(|_| rng.next_u64() as u8).collect() };
    let text: Vec<u8> = (0..60_000)
        .flat_map(|i| format!("row {i} of {}\n", i % 97).into_bytes())
        .collect();
    vec![
        Vec::new(),
        vec![7],
        random(&mut rng, 300),
        vec![0; 128 * 1024],
        random(&mut rng, 128 * 1024 + 1),
        random(&mut rng, 1024 * 1024),
        text,
    ]
}

/// A snapshot payload of a room with rows of every kind, as the engines write it.
fn room_payload() -> Vec<u8> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let schema = Schema::builder()
            .table(
                TableSchema::builder("items")
                    .table_id(0)
                    .primary_key("id", DataType::Int)
                    .column("name", DataType::String)
                    .nullable_column("price", DataType::Float)
                    .nullable_column("blob", DataType::Bytes),
            )
            .build();
        let room = RoomId::new("payload").unwrap();
        let engine = MemoryStorageEngine::new();
        engine.open_room(&room, schema).await.unwrap();
        let ops: Vec<SequencedOperation> = (0..200i64)
            .map(|k| {
                let row = CompactRow::new(vec![
                    Value::Int(k),
                    format!("item {k}").into(),
                    Value::Float(k as f64 / 3.0),
                    Value::Bytes(vec![k as u8; (k % 9) as usize].into()),
                ]);
                let op = Operation::insert(0, PrimaryKey::single(k), row, k as u64);
                SequencedOperation::new(k as u64 + 1, op)
            })
            .collect();
        engine.apply_batch(&room, ops).await.unwrap();
        let envelope = engine.create_snapshot(&room).await.unwrap();
        envelope[SNAPSHOT_HEADER_LEN..].to_vec()
    })
}

// What each codec writes, the other decodes to the same bytes.

#[test]
fn frames_of_either_codec_decode_to_the_same_bytes() {
    for payload in sample_payloads() {
        let bodies = [
            Libzstd::compress(&payload, -7).unwrap(),
            Libzstd::compress(&payload, 1).unwrap(),
            Libzstd::compress(&payload, 3).unwrap(),
            Libzstd::compress(&payload, 19).unwrap(),
            Ruzstd::compress(&payload, 3).unwrap(),
        ];
        for body in &bodies {
            assert_eq!(
                decompress_with::<Libzstd>(body, payload.len()).unwrap(),
                payload
            );
            assert_eq!(
                decompress_with::<Ruzstd>(body, payload.len()).unwrap(),
                payload
            );
        }
        // Each library on its own, too, without the checks of this module.
        let ruzstd_body = &bodies[4];
        assert_eq!(zstd::decode_all(&ruzstd_body[..]).unwrap(), payload);
        let mut decoder = ruzstd::decoding::StreamingDecoder::new(&bodies[2][..]).unwrap();
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
    }
}

#[test]
fn envelopes_of_either_codec_decode_to_the_same_bytes() {
    let payload = room_payload();
    let envelopes = [
        encode_envelope_with::<Libzstd>(&payload, true, 3).unwrap(),
        encode_envelope_with::<Ruzstd>(&payload, true, 3).unwrap(),
    ];
    for envelope in &envelopes {
        let header = SnapshotEnvelopeHeader::parse(envelope).unwrap();
        assert_eq!(header.compression(), SnapshotCompression::Zstd);
        for decoded in [
            decode_envelope_with::<Libzstd>(envelope, DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES),
            decode_envelope_with::<Ruzstd>(envelope, DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES),
        ] {
            assert_eq!(decoded.unwrap(), payload);
        }
        let limit = payload.len() as u64;
        let by_libzstd: RoomSnapshotPayload =
            decode_payload_with::<Libzstd, _>(envelope, limit).unwrap();
        let by_ruzstd: RoomSnapshotPayload =
            decode_payload_with::<Ruzstd, _>(envelope, limit).unwrap();
        assert_eq!(by_libzstd.head_seq, by_ruzstd.head_seq);
        assert_eq!(by_libzstd.tables, by_ruzstd.tables);
        assert_eq!(by_ruzstd.tables[&0].len(), 200);
    }
    // Uncompressed, the envelope is the same with either codec.
    assert_eq!(
        encode_envelope_with::<Libzstd>(&payload, false, 3).unwrap(),
        encode_envelope_with::<Ruzstd>(&payload, false, 3).unwrap()
    );
}

// Bodies that either codec rejects, as `SnapshotCorruption`.

fn more_than_declared_is_rejected<Z: ZstdCodec>() {
    let body = Z::compress(&vec![0; 1024 * 1024], 3).unwrap();
    let msg = corruption_message(decompress_with::<Z>(&body, 1024));
    assert!(msg.contains("larger than declared"), "{msg}");
    assert_eq!(
        decompress_with::<Z>(&body, 1024 * 1024).unwrap().len(),
        1024 * 1024
    );
}

#[test]
fn a_body_that_produces_more_than_declared_is_rejected() {
    with_each_codec!(more_than_declared_is_rejected);
}

fn less_than_declared_is_rejected<Z: ZstdCodec>() {
    let body = Z::compress(&[7; 100], 3).unwrap();
    let msg = corruption_message(decompress_with::<Z>(&body, 101));
    assert!(msg.contains("length mismatch"), "{msg}");
    assert_eq!(decompress_with::<Z>(&body, 100).unwrap(), vec![7; 100]);
}

#[test]
fn a_body_that_produces_less_than_declared_is_rejected() {
    with_each_codec!(less_than_declared_is_rejected);
}

fn truncated_bodies_are_rejected<Z: ZstdCodec>() {
    let payload = &sample_payloads()[2];
    let body = Z::compress(payload, 3).unwrap();
    assert_eq!(
        &decompress_with::<Z>(&body, payload.len()).unwrap(),
        payload
    );
    for len in 0..body.len() {
        corruption_message(decompress_with::<Z>(&body[..len], payload.len()));
    }
}

#[test]
fn a_truncated_body_is_rejected() {
    with_each_codec!(truncated_bodies_are_rejected);
}

fn garbage_is_rejected<Z: ZstdCodec>() {
    let mut rng = Rng::new(0x6A4B);
    for len in [0, 3, 4, 16, 1000] {
        let garbage: Vec<u8> = (0..len).map(|_| rng.next_u64() as u8).collect();
        let msg = corruption_message(decompress_with::<Z>(&garbage, 100));
        assert!(msg.contains("not a Zstandard frame"), "{msg}");
    }
}

#[test]
fn a_body_of_garbage_is_rejected() {
    with_each_codec!(garbage_is_rejected);
}

fn declared_size_above_the_limit_is_rejected<Z: ZstdCodec>() {
    let envelope = encode_envelope_with::<Z>(&[3; 4096], true, 3).unwrap();
    assert_eq!(
        decode_envelope_with::<Z>(&envelope, 4096).unwrap(),
        vec![3; 4096]
    );
    let msg = corruption_message(decode_envelope_with::<Z>(&envelope, 4095));
    assert!(msg.contains("exceeds the maximum"), "{msg}");
    let msg = corruption_message(decode_payload_with::<Z, Vec<u8>>(&envelope, 4095));
    assert!(msg.contains("exceeds the maximum"), "{msg}");
}

#[test]
fn a_declared_size_above_the_limit_is_rejected() {
    with_each_codec!(declared_size_above_the_limit_is_rejected);
}

fn data_after_the_frame_is_rejected<Z: ZstdCodec>() {
    let body = Z::compress(&[7; 100], 3).unwrap();
    assert_eq!(decompress_with::<Z>(&body, 100).unwrap(), vec![7; 100]);
    let mut padded = body.clone();
    padded.push(0);
    let mut two_frames = body.clone();
    two_frames.extend_from_slice(&Z::compress(&[], 3).unwrap());
    for body in [padded, two_frames] {
        let msg = corruption_message(decompress_with::<Z>(&body, 100));
        assert!(
            msg.contains("after the end of the Zstandard frame"),
            "{msg}"
        );
    }
}

#[test]
fn data_after_the_frame_is_rejected_by_either_codec() {
    with_each_codec!(data_after_the_frame_is_rejected);
}

fn skippable_frames_are_rejected<Z: ZstdCodec>() {
    let mut body = vec![0x50, 0x2A, 0x4D, 0x18, 4, 0, 0, 0, 1, 2, 3, 4];
    body.extend_from_slice(&Z::compress(&[7; 100], 3).unwrap());
    let msg = corruption_message(decompress_with::<Z>(&body, 100));
    assert!(msg.contains("not a Zstandard frame"), "{msg}");
}

#[test]
fn a_skippable_frame_is_rejected() {
    with_each_codec!(skippable_frames_are_rejected);
}

fn windows_above_the_limit_are_rejected<Z: ZstdCodec>() {
    assert_eq!(
        decompress_with::<Z>(&small_frame_with_window(27, 0), 100).unwrap(),
        vec![7; 100]
    );
    for (log, mantissa) in [(27, 1), (28, 0), (41, 7)] {
        let frame = small_frame_with_window(log, mantissa);
        let msg = corruption_message(decompress_with::<Z>(&frame, 100));
        assert!(msg.contains("window"), "{msg}");
    }
    // A single-segment frame's window is its declared size.
    let blocks = [raw_block(&[7; 100], true)];
    let msg = corruption_message(decompress_with::<Z>(
        &sized_frame(MAX_ZSTD_WINDOW + 1, &blocks),
        100,
    ));
    assert!(msg.contains("window"), "{msg}");
    let msg = corruption_message(decompress_with::<Z>(
        &sized_frame(MAX_ZSTD_WINDOW, &blocks),
        100,
    ));
    assert!(!msg.contains("window"), "{msg}");
}

#[test]
fn a_window_above_the_limit_is_rejected() {
    with_each_codec!(windows_above_the_limit_are_rejected);
}

#[test]
fn the_window_limit_is_libzstds() {
    // libzstd on its own, without the header check, refuses exactly the windows the check
    // refuses.
    let blocks = [raw_block(&[7; 100], true)];
    let mut frames: Vec<Vec<u8>> = (10..=41)
        .flat_map(|log| (0..8).map(move |mantissa| small_frame_with_window(log, mantissa)))
        .collect();
    for size in [
        100,
        MAX_ZSTD_WINDOW - 1,
        MAX_ZSTD_WINDOW,
        MAX_ZSTD_WINDOW + 1,
        u64::MAX,
    ] {
        frames.push(sized_frame(size, &blocks));
    }
    for frame in frames {
        let by_check = check_frame_header(&frame).is_err();
        let mut decoded = Vec::new();
        let by_libzstd = match zstd::stream::read::Decoder::new(&frame[..])
            .and_then(|mut decoder| decoder.read_to_end(&mut decoded))
        {
            Err(e) => e.to_string().contains("too much memory"),
            Ok(_) => false,
        };
        assert_eq!(
            by_check,
            by_libzstd,
            "{:02X?}",
            &frame[..14.min(frame.len())]
        );
    }
}

fn reserved_bit_and_dictionaries_are_rejected<Z: ZstdCodec>() {
    let window = window_descriptor(17, 0);
    let block = [raw_block(&[7; 100], true)];
    assert_eq!(
        decompress_with::<Z>(&frame(0x00, &[window], &block), 100).unwrap(),
        vec![7; 100]
    );
    let msg = corruption_message(decompress_with::<Z>(&frame(0x08, &[window], &block), 100));
    assert!(msg.contains("reserved"), "{msg}");
    // A dictionary id of one, two or four bytes, zero (none) or not.
    for (flag, len) in [(0x01, 1), (0x02, 2), (0x03, 4)] {
        for id in [0, 1] {
            let mut fields = vec![window, id];
            fields.resize(1 + len, 0);
            let msg = corruption_message(decompress_with::<Z>(&frame(flag, &fields, &block), 100));
            assert!(msg.contains("dictionary"), "{msg}");
        }
    }
}

#[test]
fn a_reserved_bit_or_a_dictionary_is_rejected() {
    with_each_codec!(reserved_bit_and_dictionaries_are_rejected);
}

fn declared_content_size_is_checked<Z: ZstdCodec>() {
    let blocks = [raw_block(&[7; 60], false), raw_block(&[8; 40], true)];
    let mut expected = vec![7; 60];
    expected.extend_from_slice(&[8; 40]);
    assert_eq!(
        decompress_with::<Z>(&sized_frame(100, &blocks), 100).unwrap(),
        expected
    );
    for size in [99, 101] {
        corruption_message(decompress_with::<Z>(&sized_frame(size, &blocks), 100));
    }
    // A single-segment frame may declare its size in one byte (here also its window, so a
    // smaller size would fail as a block larger than the window).
    let blocks = [raw_block(&[7; 100], true)];
    assert_eq!(
        decompress_with::<Z>(&frame(0x20, &[100], &blocks), 100).unwrap(),
        vec![7; 100]
    );
    corruption_message(decompress_with::<Z>(&frame(0x20, &[101], &blocks), 100));
    // The two-byte size field counts from 256.
    let blocks = [rle_block(9, 300, true)];
    let frame = frame(0x60, &(300u16 - 256).to_le_bytes(), &blocks);
    assert_eq!(decompress_with::<Z>(&frame, 300).unwrap(), vec![9; 300]);
}

#[test]
fn the_declared_content_size_is_checked() {
    with_each_codec!(declared_content_size_is_checked);
}

fn checksum_is_checked<Z: ZstdCodec>() {
    // `ruzstd` writes a checksum of the content at the end of the frame.
    let body = Ruzstd::compress(&[7; 1000], 3).unwrap();
    assert_ne!(body[4] & 0x04, 0);
    assert_eq!(decompress_with::<Z>(&body, 1000).unwrap(), vec![7; 1000]);
    for bit in 0..8 {
        let mut damaged = body.clone();
        *damaged.last_mut().unwrap() ^= 1 << bit;
        corruption_message(decompress_with::<Z>(&damaged, 1000));
    }
}

#[test]
fn the_checksum_is_checked() {
    with_each_codec!(checksum_is_checked);
}

#[test]
fn ruzstd_frames_are_checked_as_libzstd_checks_its_own() {
    // `ruzstd` checks neither the declared size nor the checksum of a frame, nor what follows
    // it: its decoder does, with its own messages.
    let blocks = [raw_block(&[7; 100], true)];
    let msg = corruption_message(decompress_with::<Ruzstd>(&sized_frame(101, &blocks), 100));
    assert!(msg.contains("declared size"), "{msg}");
    // Also while a payload is deserialized from the frame.
    let raw = bincode::serialize(&vec![7u8; 92]).unwrap();
    let frame = sized_frame(raw.len() as u64 + 1, &[raw_block(&raw, true)]);
    let result = deserialize_zstd_with::<Ruzstd, Vec<u8>>(&frame, u64::MAX, None);
    let msg = corruption_message(result);
    assert!(msg.contains("declared size"), "{msg}");
    let mut body = Ruzstd::compress(&[7; 1000], 3).unwrap();
    *body.last_mut().unwrap() ^= 1;
    let msg = corruption_message(decompress_with::<Ruzstd>(&body, 1000));
    assert!(msg.contains("checksum mismatch"), "{msg}");
}

fn blocks_whose_sequences_pass_the_maximum_are_rejected<Z: ZstdCodec>() {
    // A block may produce at most the smaller of the window and 128 KiB: one sequence (65540
    // bytes) passes, twenty (1.3 MB) do not, though the frame header is valid.
    let window = window_descriptor(20, 0);
    let frame = windowed_frame(window, &[expanding_block(1, true)]);
    assert_eq!(
        decompress_with::<Z>(&frame, 65540).unwrap(),
        vec![b'A'; 65540]
    );
    let frame = windowed_frame(window, &[expanding_block(20, true)]);
    corruption_message(decompress_with::<Z>(&frame, 20 * 65540));
}

#[test]
fn a_block_whose_sequences_pass_the_maximum_is_rejected() {
    with_each_codec!(blocks_whose_sequences_pass_the_maximum_are_rejected);
}

#[test]
fn a_block_of_literals_past_the_maximum_is_accepted_by_ruzstd_alone() {
    // `ruzstd` bounds what the sequences of a block produce, not the literals after them: a
    // block of 2^20 - 1 literals and no sequences passes it, and not libzstd. This pins the
    // difference, so that a version of `ruzstd` that closes it shows.
    let size = (1 << 20) - 1;
    let frame = windowed_frame(window_descriptor(20, 0), &[literals_block(size, true)]);
    let decoded = decompress_with::<Ruzstd>(&frame, size as usize).unwrap();
    assert_eq!(decoded, vec![b'B'; size as usize]);
    corruption_message(decompress_with::<Libzstd>(&frame, size as usize));
    let frame = windowed_frame(window_descriptor(20, 0), &[literals_block(100, true)]);
    assert_eq!(
        decompress_with::<Libzstd>(&frame, 100).unwrap(),
        vec![b'B'; 100]
    );
}

#[test]
fn a_read_of_ruzstd_holds_at_most_a_window_of_output() {
    // 8 MiB in raw blocks of 128 KiB, under a window of 1 MiB. However much the caller asks for,
    // the decoder decodes at most a window and a block past what it returns.
    let window = 1 << 20;
    let mut blocks: Vec<Vec<u8>> = (0..63)
        .map(|i| raw_block(&[i as u8; 128 * 1024], false))
        .collect();
    blocks.push(raw_block(&[63; 128 * 1024], true));
    let body = windowed_frame(window_descriptor(20, 0), &blocks);
    let mut decoder = Ruzstd::frame(&body).unwrap();
    let mut buffer = vec![0; 4 * 1024 * 1024];
    let read = decoder.read(&mut buffer).unwrap();
    assert!(read > 0);
    let consumed = body.len() - decoder.decoder.get_ref().len();
    assert!(
        consumed <= window + 2 * 128 * 1024,
        "consumed {consumed} bytes of input"
    );
    let mut rest = Vec::new();
    decoder.read_to_end(&mut rest).unwrap();
    assert_eq!(read + rest.len(), 8 * 1024 * 1024);
}

// Strings in a compressed payload, with either codec.

fn string_in_a_stream_round_trips<Z: ZstdCodec>() {
    let raw = bincode::serialize("a string").unwrap();
    let body = Z::compress(&raw, 3).unwrap();
    let decoded: String =
        deserialize_zstd_with::<Z, _>(&body, raw.len() as u64, Some(raw.len() as u64)).unwrap();
    assert_eq!(decoded, "a string");
}

#[test]
fn a_string_in_a_stream_round_trips() {
    with_each_codec!(string_in_a_stream_round_trips);
}

fn string_longer_than_its_stream_reserves_nothing<Z: ZstdCodec>() {
    // A string that declares almost `usize::MAX` bytes and holds none. Reserving its declared
    // length up front would panic (capacity overflow) instead of failing as corrupt data; the
    // limit does not stop it, as when a disk room reads its own snapshot.
    let raw = (usize::MAX as u64 - 64).to_le_bytes();
    let result = deserialize_zstd_with::<Z, String>(&Z::compress(&raw, 3).unwrap(), u64::MAX, None);
    assert!(
        matches!(result, Err(StorageError::SnapshotCorruption(_))),
        "{result:?}"
    );
}

#[test]
fn a_string_longer_than_its_stream_reserves_nothing() {
    with_each_codec!(string_longer_than_its_stream_reserves_nothing);
}

fn string_that_is_not_utf8_is_rejected<Z: ZstdCodec>() {
    let mut raw = 2u64.to_le_bytes().to_vec();
    raw.extend_from_slice(&[0xFF, 0xFE]);
    let result =
        deserialize_zstd_with::<Z, String>(&Z::compress(&raw, 3).unwrap(), raw.len() as u64, None);
    assert!(
        matches!(result, Err(StorageError::SnapshotCorruption(_))),
        "{result:?}"
    );
}

#[test]
fn a_string_that_is_not_utf8_is_rejected() {
    with_each_codec!(string_that_is_not_utf8_is_rejected);
}

#[test]
fn the_platform_codec_is_libzstd_natively() {
    let raw = bincode::serialize("a string").unwrap();
    let decoded: String = deserialize_zstd(&compressed(&raw), raw.len() as u64, None).unwrap();
    assert_eq!(decoded, "a string");
    let envelope = encode_snapshot_envelope(&raw, true, 19).unwrap();
    assert_eq!(
        &envelope[SNAPSHOT_HEADER_LEN..],
        &Libzstd::compress(&raw, 19).unwrap()[..]
    );
}

// Damaged snapshots compressed by `ruzstd`.

/// Applies 1 to 4 random mutations to `data`: bit flips, byte changes, insertions, deletions,
/// truncations, duplicated ranges and fixed-width integers of interesting values.
fn mutate(rng: &mut Rng, data: &mut Vec<u8>) {
    const INTERESTING: [u64; 8] = [
        0,
        1,
        0xFF,
        0xFFFF,
        1 << 17,
        0x7FFF_FFFF,
        u64::MAX - 1,
        u64::MAX,
    ];
    for _ in 0..1 + rng.below(4) {
        let len = data.len() as u64;
        match rng.below(7) {
            0 if len > 0 => {
                let i = rng.below(len) as usize;
                data[i] ^= 1 << rng.below(8);
            }
            1 if len > 0 => {
                let i = rng.below(len) as usize;
                data[i] = rng.next_u64() as u8;
            }
            2 => {
                let i = rng.below(len + 1) as usize;
                let inserted: Vec<u8> = (0..1 + rng.below(16))
                    .map(|_| rng.next_u64() as u8)
                    .collect();
                data.splice(i..i, inserted);
            }
            3 if len > 0 => {
                let i = rng.below(len) as usize;
                let n = 1 + rng.below((len - i as u64).min(32)) as usize;
                data.drain(i..i + n);
            }
            4 if len > 0 => {
                let i = rng.below(len) as usize;
                data.truncate(i);
            }
            5 if len > 1 => {
                let from = rng.below(len) as usize;
                let n = 1 + rng.below((len - from as u64).min(64)) as usize;
                let chunk = data[from..from + n].to_vec();
                let at = rng.below(len + 1) as usize;
                data.splice(at..at, chunk);
            }
            _ if len >= 8 => {
                let width = [1usize, 2, 4, 8][rng.below(4) as usize];
                let at = rng.below(len - width as u64 + 1) as usize;
                let value = INTERESTING[rng.below(INTERESTING.len() as u64) as usize];
                data[at..at + width].copy_from_slice(&value.to_le_bytes()[..width]);
            }
            _ => {}
        }
    }
}

/// Decodes envelopes compressed by `ruzstd` and damaged at random with `ruzstd`: nothing
/// panics, and every error is `SnapshotCorruption`. Where `ruzstd` and `zstd` both accept a body
/// that carries a checksum, they give the same bytes.
fn check_damaged_ruzstd_snapshots(seed: u64, cases: u64) {
    let payload = room_payload();
    let body = Ruzstd::compress(&payload, 3).unwrap();
    let envelope = sealed(SnapshotCompression::Zstd, &body, payload.len() as u32);
    let limit = DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES;

    for case in 0..cases {
        let case_seed = Rng::new(seed ^ case.wrapping_mul(0xD1B5_4A32_D192_ED03)).next_u64();
        let mut rng = Rng::new(case_seed);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let declared = |rng: &mut Rng, len: usize| {
                if rng.chance(25) {
                    rng.below(1 << 22) as u32
                } else {
                    len as u32
                }
            };
            let damaged = match rng.below(4) {
                // A damaged envelope: most fail its checksum.
                0 => {
                    let mut damaged = envelope.clone();
                    mutate(&mut rng, &mut damaged);
                    damaged
                }
                // A damaged body, sealed.
                1 => {
                    let mut damaged = body.clone();
                    mutate(&mut rng, &mut damaged);
                    let declared = declared(&mut rng, payload.len());
                    sealed(SnapshotCompression::Zstd, &damaged, declared)
                }
                // A body with a damaged frame header, sealed.
                2 => {
                    let mut head = body[..20].to_vec();
                    mutate(&mut rng, &mut head);
                    head.extend_from_slice(&body[20..]);
                    sealed(SnapshotCompression::Zstd, &head, payload.len() as u32)
                }
                // A damaged payload, compressed and sealed.
                _ => {
                    let mut damaged = payload.clone();
                    mutate(&mut rng, &mut damaged);
                    let declared = declared(&mut rng, damaged.len());
                    let body = Ruzstd::compress(&damaged, 3).unwrap();
                    sealed(SnapshotCompression::Zstd, &body, declared)
                }
            };

            let by_ruzstd = decode_envelope_with::<Ruzstd>(&damaged, limit);
            match &by_ruzstd {
                Err(error) => assert!(
                    matches!(error, StorageError::SnapshotCorruption(_)),
                    "{error:?}"
                ),
                Ok(_) => {
                    let body = &damaged[SNAPSHOT_HEADER_LEN..];
                    if body.len() > 4 && body[4] & 0x04 != 0 {
                        if let Ok(by_libzstd) = decode_envelope_with::<Libzstd>(&damaged, limit) {
                            assert_eq!(&by_libzstd, by_ruzstd.as_ref().unwrap());
                        }
                    }
                }
            }
            let applied = decode_payload_with::<Ruzstd, RoomSnapshotPayload>(&damaged, limit);
            if let Err(error) = &applied {
                assert!(
                    matches!(error, StorageError::SnapshotCorruption(_)),
                    "{error:?}"
                );
            } else {
                // A payload that deserializes was decompressed whole.
                assert!(by_ruzstd.is_ok());
            }
        }));
        if let Err(panic) = outcome {
            eprintln!(
                "case {case} of seed {seed:#x} failed; replay it with Rng::new({case_seed:#x})"
            );
            resume_unwind(panic);
        }
    }
}

#[test]
fn damaged_ruzstd_snapshots_are_rejected_without_panicking() {
    check_damaged_ruzstd_snapshots(0x2257_0001, 1_500);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn damaged_ruzstd_snapshots_are_rejected_without_panicking_long() {
    check_damaged_ruzstd_snapshots(0x2257_0002, 200_000);
}

#[test]
fn every_libzstd_level_writes_a_window_within_the_limit() {
    // The disk engine accepts any level of `zstd::compression_level_range()`. Without a known
    // size, the highest levels declare the largest windows (2^27 bytes at level 22).
    let payload = &sample_payloads()[6][..300_000];
    for level in [*zstd::compression_level_range().start(), 1, 19, 20, 21, 22] {
        let body = Libzstd::compress(payload, level).unwrap();
        check_frame_header(&body).unwrap();
        assert_eq!(
            decompress_with::<Ruzstd>(&body, payload.len()).unwrap(),
            payload
        );
    }
    let body = Libzstd::compress(payload, 22).unwrap();
    assert_eq!(body[4] & 0x20, 0, "not a single-segment frame");
    assert_eq!(body[5], window_descriptor(27, 0));
}
