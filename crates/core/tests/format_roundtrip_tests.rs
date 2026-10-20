//! Encoding then decoding is the identity for random values of every format of this crate:
//! wire messages, WAL frames, schemas (JSON and binary) and snapshot envelope headers.
//!
//! The ignored variants run many more cases: `cargo test --release -- --ignored`.

mod common;

use common::*;
use zemdb_core::protocol::encoded_len;
use zemdb_core::protocol::wal_frame::wal_record_payload_len;
use zemdb_core::*;

fn check_client_messages(seed: u64, cases: u64) {
    for_each_case(seed, cases, |rng| {
        let message = client_message(rng);
        let frame = encode_message(&message).unwrap();
        assert_eq!(
            frame.len() as u64,
            encoded_len(&message).unwrap() + PROTOCOL_HEADER_LEN as u64
        );
        let back = decode_client_message(&frame).unwrap();
        assert_eq!(back, message);
        assert_eq!(decode_message::<ClientMessage>(&frame).unwrap(), message);
        assert_eq!(peek_client_message_kind(&frame).unwrap(), message.kind());
        // The encoding is a function of the value: the decoded message encodes to the same bytes.
        assert_eq!(encode_message(&back).unwrap(), frame);
    });
}

#[test]
fn random_client_messages_round_trip() {
    check_client_messages(0xC11E_1001, 4_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_client_messages_round_trip_long() {
    check_client_messages(0xC11E_1002, 1_000_000);
}

fn check_server_messages(seed: u64, cases: u64) {
    let error_codes = all_error_codes();
    for_each_case(seed, cases, |rng| {
        let message = server_message(rng, &error_codes);
        let frame = encode_message(&message).unwrap();
        assert_eq!(
            frame.len() as u64,
            encoded_len(&message).unwrap() + PROTOCOL_HEADER_LEN as u64
        );
        let back: ServerMessage = decode_message(&frame).unwrap();
        assert_eq!(back, message);
        assert_eq!(encode_message(&back).unwrap(), frame);
    });
}

#[test]
fn random_server_messages_round_trip() {
    check_server_messages(0x5E4E_1001, 3_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_server_messages_round_trip_long() {
    check_server_messages(0x5E4E_1002, 500_000);
}

fn check_wal_logs(seed: u64, cases: u64) {
    for_each_case(seed, cases, |rng| {
        let mut log = Vec::new();
        let mut all_ops = Vec::new();
        for _ in 0..rng.between(1, 5) {
            let ops = sequenced_operations(rng, 4);
            let id = option(rng, mutation_id);
            let frame = encode_wal_batch(&ops, id).unwrap();

            let header = parse_batch_header(frame.first_chunk().unwrap()).unwrap();
            assert_eq!(header.ops_count(), ops.len());
            assert_eq!(header.frame_len(), frame.len());
            if let [op] = &ops[..] {
                assert_eq!(
                    wal_record_payload_len(op, id).unwrap() as usize,
                    frame.len() - BATCH_HEADER_SIZE
                );
            }
            match decode_wal_batch_from_slice(&frame).unwrap() {
                WalBatchDecodeResult::Ok {
                    ops: back,
                    mutation_id: back_id,
                    bytes_consumed,
                } => {
                    assert_eq!(back, ops);
                    assert_eq!(back_id, id);
                    assert_eq!(bytes_consumed, frame.len());
                }
                other => panic!("a whole frame decodes: {other:?}"),
            }
            log.extend(frame);
            all_ops.extend(ops);
        }
        let (ops, valid_len, torn) = replay_wal_records(&log).unwrap();
        assert_eq!(ops, all_ops);
        assert_eq!(valid_len, log.len());
        assert!(torn.is_none());
    });
}

#[test]
fn random_wal_logs_round_trip() {
    check_wal_logs(0x3A1_1001, 2_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_wal_logs_round_trip_long() {
    check_wal_logs(0x3A1_1002, 500_000);
}

fn check_schemas(seed: u64, cases: u64) {
    let (mut unique, mut foreign_keys, mut owners, mut immutable) = (0, 0, 0, 0);
    for_each_case(seed, cases, |rng| {
        let schema = schema(rng);
        for table in schema.tables() {
            unique += table.unique_constraints().len();
            foreign_keys += table.foreign_keys().len();
            owners += usize::from(table.owner().is_some());
            immutable += usize::from(table.is_immutable());
        }
        let json = serde_json::to_string(&schema).unwrap();
        let back: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(back, schema);
        assert_eq!(back.version(), schema.version());
        let pretty = serde_json::to_string_pretty(&schema).unwrap();
        assert_eq!(serde_json::from_str::<Schema>(&pretty).unwrap(), schema);
        let frame = encode_message(&schema).unwrap();
        assert_eq!(decode_message::<Schema>(&frame).unwrap(), schema);
    });
    assert!(
        unique > 0 && foreign_keys > 0 && owners > 0 && immutable > 0,
        "the generator declares constraints, owners and immutable tables"
    );
}

#[test]
fn random_schemas_round_trip_in_json_and_binary() {
    check_schemas(0x5C4_1001, 1_500);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_schemas_round_trip_in_json_and_binary_long() {
    check_schemas(0x5C4_1002, 300_000);
}

fn check_envelope_headers(seed: u64, cases: u64) {
    for_each_case(seed, cases, |rng| {
        let body = rng.bytes_below(512);
        let compression = if rng.coin() {
            SnapshotCompression::Raw
        } else {
            SnapshotCompression::Zstd
        };
        // A raw body is its own payload; a compressed one declares any length.
        let uncompressed_len = match compression {
            SnapshotCompression::Raw => body.len() as u32,
            SnapshotCompression::Zstd => rng.u32(),
        };
        let header = SnapshotEnvelopeHeader::for_body(compression, uncompressed_len, &body);
        assert_eq!(
            SnapshotEnvelopeHeader::parse(&header.to_bytes()).unwrap(),
            header
        );
        assert_eq!(header.compression(), compression);
        assert_eq!(header.uncompressed_len(), uncompressed_len);

        let mut envelope = header.to_bytes().to_vec();
        envelope.extend_from_slice(&body);
        assert_eq!(validate_snapshot_envelope(&envelope).unwrap(), header);
        let mut validator = SnapshotEnvelopeValidator::new();
        for piece in envelope.chunks(1 + rng.below(64)) {
            validator.update(piece);
        }
        assert_eq!(validator.finish().unwrap(), header);
    });
}

#[test]
fn random_snapshot_envelope_headers_round_trip() {
    check_envelope_headers(0x5A9_1001, 4_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_snapshot_envelope_headers_round_trip_long() {
    check_envelope_headers(0x5A9_1002, 1_000_000);
}
