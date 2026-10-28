use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use zemdb_core::protocol::limits::RESPONSE_ENVELOPE_ALLOWANCE;
use zemdb_core::protocol::{encoded_len, PROTOCOL_MAGIC};
use zemdb_core::*;

fn assert_roundtrip<M: Serialize + DeserializeOwned + PartialEq + Debug>(msg: &M) {
    let frame = encode_message(msg).expect("serialization failed");
    assert_eq!(
        &decode_message::<M>(&frame).expect("deserialization failed"),
        msg
    );
}

fn room() -> RoomId {
    RoomId::new("room-abc").unwrap()
}

#[test]
fn test_protocol_rejects_payload_exceeding_max_message_size() {
    // A frame with a valid header that is longer than MAX_FRAME_SIZE is rejected before its
    // payload is read.
    let mut oversized = vec![0u8; MAX_FRAME_SIZE + 1];
    oversized[..2].copy_from_slice(&PROTOCOL_MAGIC);
    oversized[2] = PROTOCOL_VERSION;
    let result: Result<ClientMessage, _> = decode_message(&oversized);
    assert!(
        matches!(result, Err(DecodeError::TooLarge { len }) if len == MAX_FRAME_SIZE + 1),
        "Expected TooLarge, got: {:?}",
        result
    );
}

#[test]
fn test_protocol_binary_serialization_roundtrip() {
    let compact = CompactRow::new(vec![Value::Int(42), Value::String("Testing binary".into())]);

    let mutation_id = MutationId::new([1u8; 16]);
    assert_roundtrip(&ClientMessage::Commit {
        correlation_id: CorrelationId::new(1001),
        room_id: room(),
        client_id: ClientId::new("client-1").unwrap(),
        mutation_id,
        last_ack_seq: SequenceNumber::new(10),
        op: Operation::insert(0, PrimaryKey::single(42i64), compact, 500),
    });

    // CommitAck answers in one round trip, carrying the operations the client missed.
    assert_roundtrip(&ServerMessage::CommitAck {
        correlation_id: CorrelationId::new(1001),
        room_id: room(),
        mutation_id,
        assigned_seq: SequenceNumber::new(12),
        catchup_ops: vec![
            SequencedOperation {
                seq: SequenceNumber::new(11),
                op: Operation::delete(0, PrimaryKey::single(10i64), 900),
            },
            SequencedOperation {
                seq: SequenceNumber::new(12),
                op: Operation::insert(
                    0,
                    PrimaryKey::single(42i64),
                    CompactRow::new(vec![Value::Int(42)]),
                    500,
                ),
            },
        ],
        has_more: false,
        snapshot_wanted: true,
        schema_version: 2,
        schema_id: SchemaId::new("todo").unwrap(),
    });

    assert_roundtrip(&ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1001),
        room_id: room(),
        head_seq: SequenceNumber::new(150),
        ops: vec![SequencedOperation {
            seq: SequenceNumber::new(150),
            op: Operation::delete(0, PrimaryKey::single(42i64), 1000),
        }],
        has_more: false,
        snapshot_wanted: false,
        schema_version: 3,
        schema_id: SchemaId::new("todo-v2").unwrap(),
    });

    for (snapshot_wanted, active_snapshot_seq) in
        [(true, None), (false, Some(SequenceNumber::new(140)))]
    {
        assert_roundtrip(&ServerMessage::HeartbeatAck {
            correlation_id: CorrelationId::new(1001),
            room_id: room(),
            current_head_seq: SequenceNumber::new(150),
            snapshot_wanted,
            active_snapshot_seq,
            schema_version: 4,
            schema_id: SchemaId::new("todo").unwrap(),
        });
    }

    assert_roundtrip(&ClientMessage::DeregisterClient {
        correlation_id: CorrelationId::new(1002),
        room_id: room(),
        client_id: ClientId::new("client-1").unwrap(),
    });

    assert_roundtrip(&ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(1003),
        room_id: room(),
        chunk_index: 2,
        chunk_size: 4 * 1024 * 1024,
        snapshot_hash: Some([9u8; 32]),
        room_incarnation: u128::MAX - 1,
    });

    let dummy_payload = b"snapshot payload chunk 2 data...";
    let expected_hash = ServerMessage::compute_snapshot_hash(dummy_payload);
    assert_roundtrip(&ServerMessage::SnapshotChunk {
        correlation_id: CorrelationId::new(1003),
        room_id: room(),
        snapshot_head_seq: SequenceNumber::new(500),
        chunk_index: 2,
        total_chunks: 10,
        total_bytes: 40 * 1024 * 1024,
        snapshot_hash: expected_hash,
        data: bytes::Bytes::from_static(dummy_payload),
    });

    assert_roundtrip(&ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(1004),
        room_id: room(),
        snapshot_head_seq: SequenceNumber::new(500),
        chunk_index: 0,
        total_chunks: 5,
        total_bytes: 5 * 1024 * 1024,
        snapshot_hash: expected_hash,
        data: bytes::Bytes::from_static(dummy_payload),
        room_incarnation: u128::MAX - 2,
    });

    assert_roundtrip(&ServerMessage::SnapshotUploadChunkAck {
        correlation_id: CorrelationId::new(1004),
        room_id: room(),
        chunk_index: 0,
        total_chunks: 5,
        staged: false,
    });

    assert_roundtrip(&ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1004),
        room_id: room(),
        client_id: ClientId::new("client-1").unwrap(),
        auth_token: "jwt.signed.token.abc123xyz".to_string(),
        current_seq: Some(SequenceNumber::new(5)),
        known_incarnation: Some(u128::MAX - 1),
    });

    assert_roundtrip(&ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(1005),
        room_id: room(),
    });

    let test_schema = Schema::builder()
        .table(
            TableSchema::builder("tasks")
                .primary_key("id", DataType::Uuid)
                .column("title", DataType::String)
                .nullable_column("parent", DataType::Uuid)
                .unique("tasks_title", ["title"])
                .foreign_key("tasks_parent", ["parent"], "tasks"),
        )
        .build();
    assert_roundtrip(&ServerMessage::Registered {
        correlation_id: CorrelationId::new(1004),
        room_id: room(),
        head_seq: SequenceNumber::new(1),
        tail_seq: SequenceNumber::new(1),
        schema_id: SchemaId::new("schema-workspace-v1").unwrap(),
        schema: test_schema.clone(),
        active_snapshot_seq: Some(SequenceNumber::new(1)),
        room_incarnation: u128::MAX - 1,
    });

    assert_roundtrip(&ServerMessage::Schema {
        correlation_id: CorrelationId::new(1005),
        room_id: room(),
        schema_id: SchemaId::new("schema-workspace-v1").unwrap(),
        schema: test_schema,
    });

    for code in [
        ErrorCode::Unauthorized,
        ErrorCode::RoomAlreadyExists,
        ErrorCode::TableAlreadyExists,
        ErrorCode::SchemaNotFound,
    ] {
        assert_roundtrip(&ServerMessage::Error {
            correlation_id: Some(CorrelationId::new(9999)),
            room_id: Some(room()),
            code,
            message: format!("Test error for {:?}", code),
        });
    }
}

#[test]
fn test_protocol_rejects_trailing_bytes() {
    let client_msg = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(42),
        client_id: ClientId::new("client-test").unwrap(),
        room_id: RoomId::new("room-1").unwrap(),
    };

    let mut encoded = encode_message(&client_msg).expect("serialization failed");
    encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

    let result: Result<ClientMessage, _> = decode_message(&encoded);
    assert!(
        matches!(result, Err(DecodeError::Malformed(_))),
        "Expected trailing bytes to be rejected as malformed, got: {:?}",
        result
    );
}

#[test]
fn test_ack_messages_codec_roundtrip() {
    assert_roundtrip(&ClientMessage::Ack {
        correlation_id: CorrelationId::new(101),
        room_id: RoomId::new("room-ack").unwrap(),
        client_id: ClientId::new("client-ack").unwrap(),
        ack_seq: SequenceNumber::new(42),
    });
    assert_roundtrip(&ServerMessage::AckConfirmed {
        correlation_id: CorrelationId::new(101),
        room_id: RoomId::new("room-ack").unwrap(),
        ack_seq: SequenceNumber::new(42),
        head_seq: SequenceNumber::new(50),
    });
}

#[test]
fn test_wire_framing_header_and_magic_version_verification() {
    let msg = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(42),
        client_id: ClientId::new("client-test").unwrap(),
        room_id: RoomId::new("room-1").unwrap(),
    };

    let encoded = encode_message(&msg).expect("serialization should succeed");
    assert!(encoded.len() >= 4);
    assert_eq!(&encoded[0..2], b"ZM");
    assert_eq!(encoded[2], 0x01);
    assert_eq!(encoded[3], 0x00);

    // A frame with another protocol version gets a version mismatch, not a decode failure.
    let mut other_version = encoded.clone();
    other_version[2] = 0x02;
    assert!(matches!(
        decode_message::<ClientMessage>(&other_version),
        Err(DecodeError::UnsupportedVersion {
            expected: 0x01,
            got: 0x02
        })
    ));

    // Corrupted magic bytes must be rejected
    let mut bad_magic = encoded.clone();
    bad_magic[0] = b'X';
    let res: Result<ClientMessage, _> = decode_message(&bad_magic);
    assert!(matches!(
        res,
        Err(DecodeError::InvalidMagic { got: [b'X', 0x4D] })
    ));

    // A frame shorter than the header
    let res: Result<ClientMessage, _> = decode_message(&encoded[..3]);
    assert!(matches!(res, Err(DecodeError::TooShort { len: 3 })));

    // A valid header followed by bytes that are not a message of the expected type
    let mut garbage = encoded[..4].to_vec();
    garbage.extend_from_slice(&[0xFF; 8]);
    let res: Result<ClientMessage, _> = decode_message(&garbage);
    assert!(matches!(res, Err(DecodeError::Malformed(_))));
}

#[test]
fn test_version_is_checked_before_size_and_payload() {
    // The version rule holds for any frame: an oversized frame or one with an unreadable
    // payload is still reported as a version mismatch when its header has another version.
    let mut oversized = vec![0u8; MAX_FRAME_SIZE + 1];
    oversized[..2].copy_from_slice(&PROTOCOL_MAGIC);
    oversized[2] = 0x02;
    assert!(matches!(
        decode_message::<ServerMessage>(&oversized),
        Err(DecodeError::UnsupportedVersion { got: 0x02, .. })
    ));
    let garbage = [0x5A, 0x4D, 0x07, 0x00, 0xFF, 0xFF];
    assert!(matches!(
        decode_message::<ServerMessage>(&garbage),
        Err(DecodeError::UnsupportedVersion { got: 0x07, .. })
    ));
}

#[test]
fn test_peek_version_reads_the_header_only() {
    let frame = encode_message(&ServerMessage::Error {
        correlation_id: None,
        room_id: None,
        code: ErrorCode::BadRequest,
        message: "x".to_string(),
    })
    .unwrap();
    assert_eq!(peek_version(&frame), Some(PROTOCOL_VERSION));

    let mut other = frame.clone();
    other[2] = 0x09;
    other.truncate(PROTOCOL_HEADER_LEN);
    assert_eq!(peek_version(&other), Some(0x09));

    // Not a ZemDB frame: wrong magic, or shorter than the header (a JSON body, for example).
    assert_eq!(peek_version(b"{\"code\":1}"), None);
    assert_eq!(peek_version(&frame[..3]), None);
}

#[test]
fn test_deregister_ack_message_roundtrip() {
    assert_roundtrip(&ServerMessage::DeregisterAck {
        correlation_id: CorrelationId::new(55),
        room_id: RoomId::new("room-ack").unwrap(),
        client_id: ClientId::new("client-ack").unwrap(),
    });
}

/// Bytes value whose encoded frame is exactly `frame_len` bytes long. The length prefix of
/// the value has the same size for every length used here.
fn value_with_frame_len(frame_len: usize) -> bytes::Bytes {
    let probe_len = 1 << 20;
    let overhead = encode_message(&bytes::Bytes::from(vec![7u8; probe_len]))
        .unwrap()
        .len()
        - probe_len;
    bytes::Bytes::from(vec![7u8; frame_len - overhead])
}

#[test]
fn test_max_size_payload_round_trips_and_one_more_byte_is_rejected() {
    assert_eq!(
        MAX_FRAME_SIZE,
        MAX_MESSAGE_SIZE as usize + PROTOCOL_HEADER_LEN
    );

    // A payload of exactly MAX_MESSAGE_SIZE bytes is accepted by the encoder, and the frame it
    // produces (payload plus header) is accepted by the decoder.
    let value = value_with_frame_len(MAX_FRAME_SIZE);
    let frame = encode_message(&value).expect("max-size payload must encode");
    assert_eq!(frame.len(), MAX_FRAME_SIZE);
    let decoded: bytes::Bytes = decode_message(&frame).expect("max-size frame must decode");
    assert_eq!(decoded, value);

    // One byte more cannot be encoded, and a frame of that size is rejected by the decoder.
    assert!(encode_message(&value_with_frame_len(MAX_FRAME_SIZE + 1)).is_err());
    let mut oversized = frame;
    oversized.push(7);
    assert!(matches!(
        decode_message::<bytes::Bytes>(&oversized),
        Err(DecodeError::TooLarge { .. })
    ));
}

/// Sequenced insert whose wire encoding is exactly `len` bytes: a row with one Bytes value.
fn op_with_wire_len(seq: u64, len: u64) -> SequencedOperation {
    let op = |data_len: usize| {
        SequencedOperation::new(
            SequenceNumber::new(seq),
            Operation::new(
                u16::MAX,
                PrimaryKey::single(Value::Int(i64::MIN)),
                u64::MAX,
                OperationKind::Insert {
                    row: CompactRow::new(vec![Value::Bytes(vec![1u8; data_len].into())]),
                },
            ),
        )
    };
    let probe_len = 1 << 20;
    let overhead = encoded_len(&op(probe_len)).unwrap() - probe_len as u64;
    let result = op((len - overhead) as usize);
    assert_eq!(encoded_len(&result).unwrap(), len);
    result
}

/// What the largest schema id adds to a response: its length and 64 bytes.
const LARGEST_SCHEMA_ID_LEN: u64 = 9 + 64;

/// A `CommitAck` with `ops` and every other field at its largest encoding.
fn largest_commit_ack(ops: Vec<SequencedOperation>) -> ServerMessage {
    ServerMessage::CommitAck {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: RoomId::new("r".repeat(64)).unwrap(),
        mutation_id: MutationId::new([0xFF; 16]),
        assigned_seq: SequenceNumber::new(u64::MAX),
        catchup_ops: ops,
        has_more: true,
        snapshot_wanted: true,
        schema_version: u64::MAX,
        schema_id: SchemaId::new("s".repeat(64)).unwrap(),
    }
}

/// A `SyncBatch` with `ops` and every other field at its largest encoding.
fn largest_sync_batch(ops: Vec<SequencedOperation>) -> ServerMessage {
    ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: RoomId::new("r".repeat(64)).unwrap(),
        head_seq: SequenceNumber::new(u64::MAX),
        ops,
        has_more: true,
        snapshot_wanted: true,
        schema_version: u64::MAX,
        schema_id: SchemaId::new("s".repeat(64)).unwrap(),
    }
}

#[test]
fn response_envelope_allowance_covers_every_field_but_the_operations() {
    for response in [largest_commit_ack(vec![]), largest_sync_batch(vec![])] {
        let envelope = encoded_len(&response).unwrap();
        assert!(
            envelope <= RESPONSE_ENVELOPE_ALLOWANCE,
            "{envelope} bytes over an allowance of {RESPONSE_ENVELOPE_ALLOWANCE}: {response:?}"
        );
    }
}

#[test]
fn largest_accepted_operation_fits_alone_in_any_response() {
    let op = op_with_wire_len(u64::MAX, MAX_RESPONSE_OPS_BYTES);
    assert_eq!(
        check_operation_size(&op, Some(MutationId::new([0xFF; 16]))),
        Ok(())
    );

    // With every other field at its largest encoding, both responses still encode.
    let frame = encode_message(&largest_commit_ack(vec![op.clone()]))
        .expect("CommitAck with the largest operation");
    assert!(frame.len() <= MAX_FRAME_SIZE);
    assert!(encode_message(&largest_sync_batch(vec![op])).is_ok());

    // The allowance does not leave room for a schema id by overestimating the other fields:
    // with an operation as large as it would be without the schema id, neither response fits.
    let without_schema_id =
        op_with_wire_len(u64::MAX, MAX_RESPONSE_OPS_BYTES + LARGEST_SCHEMA_ID_LEN);
    assert!(encode_message(&largest_commit_ack(vec![without_schema_id.clone()])).is_err());
    assert!(encode_message(&largest_sync_batch(vec![without_schema_id])).is_err());

    // One byte more is rejected before it could ever be accepted.
    let too_large = op_with_wire_len(u64::MAX, MAX_RESPONSE_OPS_BYTES + 1);
    assert!(matches!(
        check_operation_size(&too_large, None),
        Err(OperationSizeError::Response { .. })
    ));
}

#[test]
fn a_response_with_an_invalid_schema_id_is_malformed() {
    let frame = encode_message(&ServerMessage::HeartbeatAck {
        correlation_id: CorrelationId::new(1),
        room_id: room(),
        current_head_seq: SequenceNumber::new(1),
        snapshot_wanted: false,
        active_snapshot_seq: None,
        schema_version: 1,
        schema_id: SchemaId::new("s").unwrap(),
    })
    .unwrap();
    // The schema id is the last field: its one byte becomes a character no id may hold.
    let mut damaged = frame.clone();
    *damaged.last_mut().unwrap() = b'.';
    assert!(matches!(
        decode_message::<ServerMessage>(&damaged),
        Err(DecodeError::Malformed(_))
    ));
    assert!(decode_message::<ServerMessage>(&frame).is_ok());
}

#[test]
fn operation_whose_log_record_is_too_large_is_rejected() {
    // Fits on the wire, but the log record uses fixed-width integers: a wide row of small
    // ints grows from 2 to 12 bytes per value.
    let small_ints = vec![Value::Int(0); 1_500_000];
    let op = SequencedOperation::new(
        SequenceNumber::new(1),
        Operation::new(
            0,
            PrimaryKey::single(Value::Int(0)),
            0,
            OperationKind::Insert {
                row: CompactRow::new(small_ints),
            },
        ),
    );
    assert!(encoded_len(&op).unwrap() < MAX_RESPONSE_OPS_BYTES);
    assert!(matches!(
        check_operation_size(&op, None),
        Err(OperationSizeError::LogRecord { .. })
    ));
    assert!(encode_wal_record(&op, None).is_err());
}

fn commit_with(op: Operation) -> ClientMessage {
    ClientMessage::Commit {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room-limits").unwrap(),
        client_id: ClientId::new("client-1").unwrap(),
        mutation_id: MutationId::new([3u8; 16]),
        last_ack_seq: SequenceNumber::new(0),
        op,
    }
}

fn nulls(count: usize) -> Vec<Value> {
    vec![Value::Null; count]
}

fn null_deltas(count: usize) -> Vec<ColumnUpdate> {
    (0..count)
        .map(|i| ColumnUpdate::new(i as u16, Value::Null))
        .collect()
}

fn delete_commit_frame() -> Vec<u8> {
    encode_message(&commit_with(Operation::delete(
        0,
        PrimaryKey::single(1i64),
        0,
    )))
    .unwrap()
}

#[test]
fn rows_keys_and_deltas_over_the_column_limit_are_rejected() {
    let over = schema::MAX_COLUMNS + 1;
    let row = Operation::insert(0, PrimaryKey::single(1i64), CompactRow::new(nulls(over)), 0);
    let key = Operation::delete(0, PrimaryKey::composite(nulls(over)), 0);
    let deltas = Operation::update(0, PrimaryKey::single(1i64), null_deltas(over), 0);
    for op in [row, key, deltas] {
        let frame = encode_message(&commit_with(op)).unwrap();
        assert!(matches!(
            decode_client_message(&frame),
            Err(DecodeError::Malformed(_))
        ));
        // The limit belongs to the types, so it holds without a value budget too.
        assert!(matches!(
            decode_message::<ClientMessage>(&frame),
            Err(DecodeError::Malformed(_))
        ));
    }
}

#[test]
fn the_largest_operation_a_table_allows_is_accepted() {
    // A row of MAX_COLUMNS values and a key of as many: the most values a commit can carry.
    let max = schema::MAX_COLUMNS;
    let insert = Operation::insert(
        0,
        PrimaryKey::composite(nulls(max)),
        CompactRow::new(nulls(max)),
        0,
    );
    let update = Operation::update(0, PrimaryKey::composite(nulls(max)), null_deltas(max), 0);
    for op in [insert, update] {
        let msg = commit_with(op);
        let frame = encode_message(&msg).unwrap();
        assert_eq!(decode_client_message(&frame).unwrap(), msg);
    }
}

#[test]
fn a_commit_of_large_values_near_the_frame_limit_is_accepted() {
    let probe = commit_with(Operation::insert(
        0,
        PrimaryKey::single(1i64),
        CompactRow::new(vec![
            Value::Bytes(Box::default()),
            Value::Bytes(Box::default()),
        ]),
        0,
    ));
    let room = MAX_FRAME_SIZE - encode_message(&probe).unwrap().len() - 64;
    let msg = commit_with(Operation::insert(
        0,
        PrimaryKey::single(1i64),
        CompactRow::new(vec![
            Value::Bytes(vec![1u8; room / 2].into_boxed_slice()),
            Value::Bytes(vec![2u8; room / 2].into_boxed_slice()),
        ]),
        0,
    ));
    let frame = encode_message(&msg).unwrap();
    assert!(frame.len() > MAX_FRAME_SIZE - 128);
    assert_eq!(decode_client_message(&frame).unwrap(), msg);
}

#[test]
fn the_kind_is_read_without_decoding_the_rest_of_the_frame() {
    let mut frame = delete_commit_frame();
    frame.truncate(PROTOCOL_HEADER_LEN + 1);
    frame.extend_from_slice(&[0xFF; 32]);
    assert_eq!(
        peek_client_message_kind(&frame).unwrap(),
        ClientMessageKind::Commit
    );
    assert!(matches!(
        decode_client_message(&frame),
        Err(DecodeError::Malformed(_))
    ));
}

#[test]
fn peeking_checks_the_header_and_the_variant() {
    let frame = delete_commit_frame();
    let mut other_version = frame.clone();
    other_version[2] = PROTOCOL_VERSION + 1;
    assert!(matches!(
        peek_client_message_kind(&other_version),
        Err(DecodeError::UnsupportedVersion { .. })
    ));
    assert!(matches!(
        peek_client_message_kind(&frame[..PROTOCOL_HEADER_LEN]),
        Err(DecodeError::Malformed(_))
    ));
    let mut unknown = frame[..PROTOCOL_HEADER_LEN].to_vec();
    unknown.push(9);
    assert!(matches!(
        peek_client_message_kind(&unknown),
        Err(DecodeError::Malformed(_))
    ));
}

#[test]
fn every_value_variant_round_trips_in_every_format() {
    let values = vec![
        Value::Null,
        Value::Int(-7),
        Value::Float(1.5),
        Value::Timestamp(1_700_000_000),
        Value::String("text".into()),
        Value::Bool(true),
        Value::Bytes(vec![1, 2, 3].into_boxed_slice()),
        Value::Uuid([9u8; 16]),
    ];
    for value in values {
        let frame = encode_message(&value).unwrap();
        assert_eq!(decode_message::<Value>(&frame).unwrap(), value);
        let fixint = bincode::serialize(&value).unwrap();
        assert_eq!(bincode::deserialize::<Value>(&fixint).unwrap(), value);
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&json).unwrap(), value);
    }
}

#[test]
fn error_codes_keep_their_wire_indexes() {
    // Codes are encoded by variant index: a new code goes at the end, never in between.
    let codes = [
        ErrorCode::SchemaViolation,
        ErrorCode::RoomNotFound,
        ErrorCode::ClientNotRegistered,
        ErrorCode::BehindCompaction,
        ErrorCode::RateLimited,
        ErrorCode::RoomLocked,
        ErrorCode::Internal,
        ErrorCode::Unauthorized,
        ErrorCode::RoomAlreadyExists,
        ErrorCode::TableAlreadyExists,
        ErrorCode::SchemaNotFound,
        ErrorCode::InvalidSequence,
        ErrorCode::ProtocolVersionMismatch,
        ErrorCode::BadRequest,
        ErrorCode::SnapshotSuperseded,
        ErrorCode::Forbidden,
        ErrorCode::Unavailable,
        ErrorCode::Timeout,
        ErrorCode::SchemaAlreadyExists,
        ErrorCode::RequestTimeout,
        ErrorCode::RoomReset,
    ];
    for (index, code) in codes.into_iter().enumerate() {
        let frame = encode_message(&code).unwrap();
        assert_eq!(frame[PROTOCOL_HEADER_LEN..], [index as u8], "{code:?}");
        assert_eq!(decode_message::<ErrorCode>(&frame).unwrap(), code);
    }
}

#[test]
fn a_delete_carrying_its_owner_round_trips_in_every_message() {
    let delete = Operation::delete_with_owner(2, PrimaryKey::single(7i64), "alice", 900);
    let commit = ClientMessage::Commit {
        correlation_id: CorrelationId::new(5),
        room_id: room(),
        client_id: ClientId::new("alice").unwrap(),
        mutation_id: MutationId::new([5u8; 16]),
        last_ack_seq: SequenceNumber::new(3),
        op: delete.clone(),
    };
    assert_roundtrip(&commit);
    // As the server decodes it, with the value budget.
    assert_eq!(
        decode_client_message(&encode_message(&commit).unwrap()).unwrap(),
        commit
    );
    let sequenced = vec![
        SequencedOperation::new(4u64, delete.clone()),
        SequencedOperation::new(5u64, Operation::delete(2, PrimaryKey::single(8i64), 901)),
    ];
    assert_roundtrip(&ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(6),
        room_id: room(),
        head_seq: SequenceNumber::new(5),
        ops: sequenced,
        has_more: false,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("owned").unwrap(),
    });
    // The owner is part of the operation, so of its size.
    let without = encoded_len(&Operation::delete(2, PrimaryKey::single(7i64), 900)).unwrap();
    assert!(encoded_len(&delete).unwrap() > without);
}
