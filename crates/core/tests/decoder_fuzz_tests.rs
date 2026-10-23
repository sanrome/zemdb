//! Damaged inputs for every decoder of bytes that come from a peer or from disk: wire frames,
//! WAL frames, snapshot envelope headers and schema JSON. The random inputs are mutations of
//! valid samples, so that they reach deep into each format. No decoder may panic, and each one
//! fails only with the errors its contract allows.
//!
//! The ignored variants run many more cases: `cargo test --release -- --ignored`.

mod common;

use common::*;
use std::io::Cursor;
use zemdb_core::protocol::PROTOCOL_MAGIC;
use zemdb_core::*;

// ---------------------------------------------------------------------------------------------
// Wire frames
// ---------------------------------------------------------------------------------------------

fn frames_of<M: serde::Serialize>(messages: &[M]) -> Vec<Vec<u8>> {
    messages
        .iter()
        .map(|m| encode_message(m).unwrap())
        .collect()
}

/// A mutated sample of `pool`, or now and then a valid header followed by random bytes. The
/// header is mutated only now and then, so that most inputs reach the payload decoder.
fn mutated_frame(rng: &mut Rng, pool: &[Vec<u8>]) -> Vec<u8> {
    let mut frame = if rng.chance(1, 10) {
        let mut frame = PROTOCOL_MAGIC.to_vec();
        frame.extend_from_slice(&[PROTOCOL_VERSION, 0]);
        frame.extend(rng.bytes_below(96));
        frame
    } else {
        rng.pick(pool).clone()
    };
    mutate(rng, &mut frame, PROTOCOL_HEADER_LEN, pool);
    frame
}

/// Checks that `error` is the one the header of `frame` calls for: the header error if it is
/// not a header of this protocol version, a payload error otherwise. `TooManyValues` is a
/// payload error of the client decoder only.
fn assert_expected_error(frame: &[u8], error: &DecodeError, with_value_budget: bool) {
    match frame {
        [m0, m1, version, _, ..]
            if [*m0, *m1] == PROTOCOL_MAGIC && *version == PROTOCOL_VERSION =>
        {
            assert!(
                matches!(error, DecodeError::Malformed(_))
                    || (with_value_budget && matches!(error, DecodeError::TooManyValues { .. })),
                "{error:?}"
            )
        }
        [m0, m1, version, _, ..] if [*m0, *m1] == PROTOCOL_MAGIC => assert!(
            matches!(error, DecodeError::UnsupportedVersion { got, .. } if got == version),
            "{error:?}"
        ),
        [m0, m1, _, _, ..] => assert!(
            matches!(error, DecodeError::InvalidMagic { got } if *got == [*m0, *m1]),
            "{error:?}"
        ),
        _ => assert!(
            matches!(error, DecodeError::TooShort { len } if *len == frame.len()),
            "{error:?}"
        ),
    }
}

fn check_client_frames(seed: u64, cases: u64) {
    let pool = frames_of(&sample_client_messages());
    for_each_case(seed, cases, |rng| {
        let frame = mutated_frame(rng, &pool);
        let decoded = decode_client_message(&frame);
        let unbudgeted = decode_message::<ClientMessage>(&frame);
        let kind = peek_client_message_kind(&frame);

        match &decoded {
            Ok(message) => {
                assert_eq!(unbudgeted.as_ref().ok(), Some(message));
                assert_eq!(kind.as_ref().ok(), Some(&message.kind()));
                if let ClientMessage::Commit { op, .. } = message {
                    assert!(within_column_limit(op), "{op:?}");
                }
                // Whatever decodes encodes back to a frame of the same message.
                let again = encode_message(message).unwrap();
                assert_eq!(&decode_client_message(&again).unwrap(), message);
            }
            Err(error) => assert_expected_error(&frame, error, true),
        }
        // The value budget only adds limits, and the kind is read from the same variant tag.
        if let Err(error) = &unbudgeted {
            assert_expected_error(&frame, error, false);
            assert!(decoded.is_err());
        }
        if let Err(error) = &kind {
            assert_expected_error(&frame, error, false);
            assert!(decoded.is_err());
        }
    });
}

#[test]
fn random_client_frames_never_panic_the_decoders() {
    check_client_frames(0xC11E_0001, 8_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_client_frames_never_panic_the_decoders_long() {
    check_client_frames(0xC11E_0002, 2_000_000);
}

fn check_server_frames(seed: u64, cases: u64) {
    let pool = frames_of(&sample_server_messages());
    for_each_case(seed, cases, |rng| {
        let frame = mutated_frame(rng, &pool);
        match decode_message::<ServerMessage>(&frame) {
            Ok(message) => {
                let again = encode_message(&message).unwrap();
                assert_eq!(decode_message::<ServerMessage>(&again).unwrap(), message);
            }
            Err(error) => assert_expected_error(&frame, &error, false),
        }
    });
}

#[test]
fn random_server_frames_never_panic_the_decoder() {
    check_server_frames(0x5E4E_0001, 8_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_server_frames_never_panic_the_decoder_long() {
    check_server_frames(0x5E4E_0002, 2_000_000);
}

/// The frame at the start of `frame` with a length of 0 at `at` (a one-byte variable-length
/// integer) declaring `u64::MAX` elements instead, and nothing after it.
fn declaring_u64_max_at(frame: &[u8], at: usize) -> Vec<u8> {
    assert_eq!(frame[at], 0, "the length to replace is an empty sequence");
    let mut damaged = frame[..at].to_vec();
    damaged.push(0xFD);
    damaged.extend_from_slice(&u64::MAX.to_le_bytes());
    damaged
}

#[test]
fn lengths_declared_far_beyond_the_frame_are_rejected() {
    let commit = |op| ClientMessage::Commit {
        correlation_id: CorrelationId::new(1),
        room_id: sample_room(),
        client_id: ClientId::new("c").unwrap(),
        mutation_id: MutationId::from_u128(1),
        last_ack_seq: SequenceNumber::new(0),
        op,
    };
    // A row, a key and a list of deltas, each the last field of its frame.
    let row = encode_message(&commit(Operation::insert(
        0,
        PrimaryKey::single(1i64),
        CompactRow::new(vec![]),
        0,
    )))
    .unwrap();
    let deltas = encode_message(&commit(Operation::update(
        0,
        PrimaryKey::single(1i64),
        vec![],
        0,
    )))
    .unwrap();
    for frame in [row, deltas] {
        let damaged = declaring_u64_max_at(&frame, frame.len() - 1);
        // The declared length is over the column limit, which is checked before the value
        // budget is charged and before anything is reserved.
        assert!(matches!(
            decode_client_message(&damaged),
            Err(DecodeError::Malformed(_))
        ));
        assert!(matches!(
            decode_message::<ClientMessage>(&damaged),
            Err(DecodeError::Malformed(_))
        ));
    }

    // A batch of operations: two flags, a one-byte schema version and a schema id of one byte
    // (two with its length) follow the operations.
    let batch = encode_message(&ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1),
        room_id: sample_room(),
        head_seq: SequenceNumber::new(1),
        ops: vec![],
        has_more: false,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("s").unwrap(),
    })
    .unwrap();
    let damaged = declaring_u64_max_at(&batch, batch.len() - 6);
    assert!(matches!(
        decode_message::<ServerMessage>(&damaged),
        Err(DecodeError::Malformed(_))
    ));
}

#[test]
#[ignore = "long-running test (millions of operations in debug); run with --ignored in release"]
fn the_largest_batch_of_tiny_operations_decodes() {
    // As many deletes by a single null key as fit in one frame: the most operations a frame
    // can carry, decoded without a value budget, as a client decodes a response.
    let one = SequencedOperation::new(
        1u64,
        Operation::delete(0, PrimaryKey::single(Value::Null), 0),
    );
    let per_op = zemdb_core::protocol::encoded_len(&one).unwrap() as usize;
    let message = ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1),
        room_id: sample_room(),
        head_seq: SequenceNumber::new(1),
        ops: vec![one; (MAX_FRAME_SIZE - 64) / per_op],
        has_more: false,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("s").unwrap(),
    };
    let frame = encode_message(&message).unwrap();
    assert!(frame.len() > MAX_FRAME_SIZE - 64 - per_op);
    assert_eq!(decode_message::<ServerMessage>(&frame).unwrap(), message);
}

// ---------------------------------------------------------------------------------------------
// WAL frames
// ---------------------------------------------------------------------------------------------

/// Offsets of the frames of a valid log.
fn frame_offsets(log: &[u8]) -> Vec<usize> {
    let mut offsets = vec![];
    let mut at = 0;
    while at < log.len() {
        offsets.push(at);
        at += parse_batch_header(log[at..].first_chunk().unwrap())
            .unwrap()
            .frame_len();
    }
    offsets
}

/// Rewrites the checksums of the frame at `at` (the payload's, if the payload it declares fits,
/// then the header's), so that its damage reaches the payload decoder.
fn seal_frame(data: &mut [u8], at: usize) {
    if data.len() < at + BATCH_HEADER_SIZE {
        return;
    }
    let len = u32::from_le_bytes(data[at + 2..at + 6].try_into().unwrap()) as usize;
    let start = at + BATCH_HEADER_SIZE;
    if start + len <= data.len() {
        let crc = crc32fast::hash(&data[start..start + len]);
        data[at + 6..at + 10].copy_from_slice(&crc.to_le_bytes());
    }
    let header_crc = crc32fast::hash(&data[at..at + 14]);
    data[at + 14..at + 18].copy_from_slice(&header_crc.to_le_bytes());
}

fn check_replay(log: &[u8]) {
    match replay_wal_records(log) {
        Ok((ops, valid_len, torn)) => {
            assert!(valid_len <= log.len());
            assert_eq!(torn.is_none(), valid_len == log.len());
            // The valid prefix alone replays to the same operations, without damage.
            assert_eq!(
                replay_wal_records(&log[..valid_len]).unwrap(),
                (ops, valid_len, None)
            );
        }
        Err(error) => assert!(matches!(error, WalFrameError::Corruption(_)), "{error:?}"),
    }
}

fn check_first_frame(slice: &[u8]) {
    match decode_wal_batch_from_slice(slice) {
        Ok(WalBatchDecodeResult::Ok {
            ops,
            bytes_consumed,
            ..
        }) => {
            let header = parse_batch_header(slice.first_chunk().unwrap()).unwrap();
            assert_eq!(bytes_consumed, header.frame_len());
            assert!(bytes_consumed <= slice.len());
            assert_eq!(ops.len(), header.ops_count());
        }
        Ok(WalBatchDecodeResult::CleanEof) => assert!(slice.is_empty()),
        Ok(WalBatchDecodeResult::TornWrite { .. }) => assert!(!slice.is_empty()),
        Err(error) => assert!(matches!(error, WalFrameError::Corruption(_)), "{error:?}"),
    }
    if let Some(header) = slice.first_chunk() {
        if let Ok(header) = parse_batch_header(header) {
            assert!(header.payload_len() as u64 <= MAX_MESSAGE_SIZE);
        }
    }
}

/// A frame the search finds starts at or after `from`, has a valid header and fits in `log`.
fn check_search(log: &[u8], from: usize) {
    let found = find_valid_frame(&mut Cursor::new(log), from as u64, log.len() as u64)
        .expect("reads stay within the log");
    if let Some(at) = found {
        let at = at as usize;
        assert!(at >= from);
        let header = parse_batch_header(log[at..].first_chunk().unwrap()).unwrap();
        assert!(at + header.frame_len() <= log.len());
    }
}

fn check_wal_logs(seed: u64, cases: u64) {
    let log = sample_wal_log();
    let offsets = frame_offsets(&log);
    let pool = vec![log.clone()];
    for_each_case(seed, cases, |rng| {
        let mut data = log.clone();
        mutate(rng, &mut data, 0, &pool);
        if !rng.chance(1, 3) {
            let at = (*rng.pick(&offsets)).min(data.len());
            seal_frame(&mut data, at);
        }
        check_replay(&data);
        check_first_frame(&data[(*rng.pick(&offsets)).min(data.len())..]);
        check_search(&data, rng.below(data.len() + 1));
    });
}

#[test]
fn random_wal_logs_never_panic_the_replay() {
    check_wal_logs(0x3A1_0001, 4_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_wal_logs_never_panic_the_replay_long() {
    check_wal_logs(0x3A1_0002, 1_000_000);
}

fn check_wal_payloads(seed: u64, cases: u64) {
    let log = sample_wal_log();
    let frames: Vec<(BatchHeader, Vec<u8>)> = frame_offsets(&log)
        .into_iter()
        .map(|at| {
            let header = parse_batch_header(log[at..].first_chunk().unwrap()).unwrap();
            let payload = log[at + BATCH_HEADER_SIZE..at + header.frame_len()].to_vec();
            (header, payload)
        })
        .collect();
    let pool: Vec<Vec<u8>> = frames.iter().map(|(_, payload)| payload.clone()).collect();
    for_each_case(seed, cases, |rng| {
        let (source, payload) = rng.pick(&frames);
        let mut payload = payload.clone();
        mutate(rng, &mut payload, 0, &pool);
        let ops_count = if rng.coin() {
            source.ops_count() as u32
        } else {
            rng.below(4) as u32
        };
        // A header that matches the payload: only the payload decoder can reject it.
        let mut frame = encode_wal_batch(&[], None).unwrap()[..BATCH_HEADER_SIZE].to_vec();
        frame[2..6].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        frame[10..14].copy_from_slice(&ops_count.to_le_bytes());
        frame.extend_from_slice(&payload);
        seal_frame(&mut frame, 0);
        let header = parse_batch_header(frame.first_chunk().unwrap()).unwrap();

        match decode_batch_payload(&header, &payload) {
            Ok(BatchPayloadDecode::Batch { ops, .. }) => assert_eq!(ops.len(), ops_count as usize),
            Ok(mismatch @ BatchPayloadDecode::ChecksumMismatch { .. }) => {
                panic!("the checksum was sealed: {mismatch:?}")
            }
            Err(error) => assert!(matches!(error, WalFrameError::Corruption(_)), "{error:?}"),
        }
    });
}

#[test]
fn random_payloads_under_a_valid_header_never_panic_the_wal_decoder() {
    check_wal_payloads(0x9A7_0001, 8_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_payloads_under_a_valid_header_never_panic_the_wal_decoder_long() {
    check_wal_payloads(0x9A7_0002, 2_000_000);
}

#[test]
fn a_wal_payload_declaring_u64_max_operations_is_corruption() {
    // The payload of a frame without operations is a `None` mutation id and an empty list,
    // whose length is a fixed-width `u64`.
    let frame = encode_wal_batch(&[], None).unwrap();
    let mut payload = frame[BATCH_HEADER_SIZE..].to_vec();
    assert_eq!(payload, [0; 9]);
    payload[1..].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut damaged = frame[..BATCH_HEADER_SIZE].to_vec();
    damaged.extend_from_slice(&payload);
    seal_frame(&mut damaged, 0);

    let header = parse_batch_header(damaged.first_chunk().unwrap()).unwrap();
    assert!(matches!(
        decode_batch_payload(&header, &payload),
        Err(WalFrameError::Corruption(_))
    ));
    assert!(matches!(
        decode_wal_batch_from_slice(&damaged),
        Err(WalFrameError::Corruption(_))
    ));
}

// ---------------------------------------------------------------------------------------------
// Snapshot envelope headers
// ---------------------------------------------------------------------------------------------

fn check_snapshot_envelopes(seed: u64, cases: u64) {
    let body = b"payload bytes of a raw snapshot".to_vec();
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
    let mut envelope = header.to_bytes().to_vec();
    envelope.extend_from_slice(&body);
    let pool = vec![envelope.clone()];
    for_each_case(seed, cases, |rng| {
        let mut data = if rng.chance(1, 10) {
            rng.bytes_below(40)
        } else {
            envelope.clone()
        };
        mutate(rng, &mut data, 0, &pool);

        let parsed = SnapshotEnvelopeHeader::parse(&data);
        let whole = validate_snapshot_envelope(&data);
        // The incremental validator agrees with the whole one, however the bytes are split.
        let mut validator = SnapshotEnvelopeValidator::new();
        let mut rest: &[u8] = &data;
        while !rest.is_empty() {
            let (piece, tail) = rest.split_at(1 + rng.below(rest.len().min(16)));
            validator.update(piece);
            rest = tail;
        }
        assert_eq!(validator.finish(), whole);

        match (&parsed, &whole) {
            (Ok(parsed), Ok(validated)) => {
                assert_eq!(parsed, validated);
                assert_eq!(&parsed.to_bytes()[..], &data[..SNAPSHOT_HEADER_LEN]);
                if parsed.compression() == SnapshotCompression::Raw {
                    let body_len = (data.len() - SNAPSHOT_HEADER_LEN) as u64;
                    assert_eq!(u64::from(parsed.uncompressed_len()), body_len);
                }
            }
            // A valid header with a body that does not match it.
            (Ok(_), Err(error)) => assert!(
                matches!(
                    error,
                    SnapshotEnvelopeError::ChecksumMismatch { .. }
                        | SnapshotEnvelopeError::RawLengthMismatch { .. }
                ),
                "{error:?}"
            ),
            (Err(header_error), Err(error)) => assert_eq!(header_error, error),
            (Err(_), Ok(_)) => panic!("an envelope with an invalid header was validated"),
        }
    });
}

#[test]
fn random_snapshot_envelopes_never_panic_the_validators() {
    check_snapshot_envelopes(0x5A9_0001, 8_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_snapshot_envelopes_never_panic_the_validators_long() {
    check_snapshot_envelopes(0x5A9_0002, 2_000_000);
}

// ---------------------------------------------------------------------------------------------
// Schema JSON
// ---------------------------------------------------------------------------------------------

fn check_schema_json(seed: u64, cases: u64) {
    let schema = sample_schema();
    let pool = vec![
        serde_json::to_vec_pretty(&schema).unwrap(),
        serde_json::to_vec(&schema).unwrap(),
    ];
    for_each_case(seed, cases, |rng| {
        let mut data = rng.pick(&pool).clone();
        mutate(rng, &mut data, 0, &pool);
        let Ok(decoded) = serde_json::from_slice::<Schema>(&data) else {
            return;
        };
        // Whatever is accepted went through the same validation as a schema built in code,
        // and survives a round trip.
        assert!(decoded.version() >= Schema::FIRST_VERSION);
        let tables: Vec<TableSchema> = decoded.tables().cloned().collect();
        for table in &tables {
            let rebuilt = TableSchema::try_with_constraints(
                table.table_id(),
                table.name(),
                table.primary_key().to_vec(),
                table.columns().to_vec(),
                table.unique_constraints().to_vec(),
                table.foreign_keys().to_vec(),
            )
            .and_then(|rebuilt| rebuilt.try_with_owner(table.owner().cloned()))
            .unwrap()
            .with_immutable(table.is_immutable());
            assert_eq!(&rebuilt, table);
        }
        Schema::try_from_tables(tables).unwrap();
        let again = serde_json::to_vec(&decoded).unwrap();
        assert_eq!(serde_json::from_slice::<Schema>(&again).unwrap(), decoded);
    });
}

#[test]
fn random_schema_json_never_panics_the_decoder() {
    check_schema_json(0x5C4_0001, 6_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_schema_json_never_panics_the_decoder_long() {
    check_schema_json(0x5C4_0002, 1_000_000);
}

#[test]
fn deeply_nested_schema_json_is_rejected_without_overflowing_the_stack() {
    let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
    assert!(serde_json::from_str::<Schema>(&deep).is_err());
    let deep = format!("{}{}", "{\"tables\":".repeat(100_000), "}".repeat(100_000));
    assert!(serde_json::from_str::<Schema>(&deep).is_err());
}
