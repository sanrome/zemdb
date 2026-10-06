use zemdb_core::protocol::encoded_len;
use zemdb_core::*;

#[test]
fn test_protocol_rejects_payload_exceeding_max_message_size() {
    // A frame with a valid header that is longer than MAX_FRAME_SIZE is rejected before its
    // payload is read.
    let mut oversized = vec![0u8; MAX_FRAME_SIZE + 1];
    oversized[..4].copy_from_slice(&[0x5A, 0x4D, PROTOCOL_VERSION, 0x00]);
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
    let client_msg = ClientMessage::Commit {
        correlation_id: CorrelationId::new(1001),
        room_id: RoomId::new("room-abc").unwrap(),
        client_id: ClientId::new("client-1").unwrap(),
        mutation_id,
        last_ack_seq: SequenceNumber::new(10),
        op: Operation::insert(0, PrimaryKey::single(42i64), compact, 500),
    };

    let encoded = encode_message(&client_msg).expect("serialization failed");
    let decoded: ClientMessage = decode_message(&encoded).expect("deserialization failed");
    assert_eq!(client_msg, decoded);

    // Test CommitAck 1-RTT roundtrip with catchup_ops
    let commit_ack_msg = ServerMessage::CommitAck {
        correlation_id: CorrelationId::new(1001),
        room_id: RoomId::new("room-abc").unwrap(),
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
    };
    let encoded_ack = encode_message(&commit_ack_msg).expect("serialization failed");
    let decoded_ack: ServerMessage = decode_message(&encoded_ack).expect("deserialization failed");
    assert_eq!(commit_ack_msg, decoded_ack);

    let server_msg = ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1001),
        room_id: RoomId::new("room-abc").unwrap(),
        head_seq: SequenceNumber::new(150),
        ops: vec![SequencedOperation {
            seq: SequenceNumber::new(150),
            op: Operation::delete(0, PrimaryKey::single(42i64), 1000),
        }],
        has_more: false,
        snapshot_wanted: false,
    };

    let encoded_server = encode_message(&server_msg).expect("serialization failed");
    let decoded_server: ServerMessage =
        decode_message(&encoded_server).expect("deserialization failed");
    assert_eq!(server_msg, decoded_server);

    // Test HeartbeatAck roundtrip with the snapshot signalling fields
    for (snapshot_wanted, active_snapshot_seq) in
        [(true, None), (false, Some(SequenceNumber::new(140)))]
    {
        let heartbeat_ack = ServerMessage::HeartbeatAck {
            correlation_id: CorrelationId::new(1001),
            room_id: RoomId::new("room-abc").unwrap(),
            current_head_seq: SequenceNumber::new(150),
            snapshot_wanted,
            active_snapshot_seq,
        };
        let encoded_hb = encode_message(&heartbeat_ack).expect("serialization failed");
        let decoded_hb: ServerMessage =
            decode_message(&encoded_hb).expect("deserialization failed");
        assert_eq!(heartbeat_ack, decoded_hb);
    }

    // Test DeregisterClient roundtrip
    let dereg_msg = ClientMessage::DeregisterClient {
        correlation_id: CorrelationId::new(1002),
        room_id: RoomId::new("room-abc").unwrap(),
        client_id: ClientId::new("client-1").unwrap(),
    };
    let encoded_dereg = encode_message(&dereg_msg).expect("serialization failed");
    let decoded_dereg: ClientMessage =
        decode_message(&encoded_dereg).expect("deserialization failed");
    assert_eq!(dereg_msg, decoded_dereg);

    // Test RequestSnapshotChunk roundtrip
    let req_chunk = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(1003),
        room_id: RoomId::new("room-abc").unwrap(),
        chunk_index: 2,
        chunk_size: 4 * 1024 * 1024,
        snapshot_hash: Some([9u8; 32]),
    };
    let enc_req = encode_message(&req_chunk).expect("serialization failed");
    let dec_req: ClientMessage = decode_message(&enc_req).expect("deserialization failed");
    assert_eq!(req_chunk, dec_req);

    // Test SnapshotChunk roundtrip
    let dummy_payload = b"snapshot payload chunk 2 data...";
    let expected_hash = ServerMessage::compute_snapshot_hash(dummy_payload);
    let snap_chunk = ServerMessage::SnapshotChunk {
        correlation_id: CorrelationId::new(1003),
        room_id: RoomId::new("room-abc").unwrap(),
        snapshot_head_seq: SequenceNumber::new(500),
        chunk_index: 2,
        total_chunks: 10,
        total_bytes: 40 * 1024 * 1024,
        snapshot_hash: expected_hash,
        data: bytes::Bytes::from_static(dummy_payload),
    };
    let enc_chunk = encode_message(&snap_chunk).expect("serialization failed");
    let dec_chunk: ServerMessage = decode_message(&enc_chunk).expect("deserialization failed");
    assert_eq!(snap_chunk, dec_chunk);

    // Test UploadSnapshotChunk roundtrip
    let upload_chunk = ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(1004),
        room_id: RoomId::new("room-abc").unwrap(),
        snapshot_head_seq: SequenceNumber::new(500),
        chunk_index: 0,
        total_chunks: 5,
        total_bytes: 5 * 1024 * 1024,
        snapshot_hash: expected_hash,
        data: bytes::Bytes::from_static(dummy_payload),
    };
    let enc_upload = encode_message(&upload_chunk).expect("serialization failed");
    let dec_upload: ClientMessage = decode_message(&enc_upload).expect("deserialization failed");
    assert_eq!(upload_chunk, dec_upload);

    // Test SnapshotUploadChunkAck roundtrip
    let upload_ack = ServerMessage::SnapshotUploadChunkAck {
        correlation_id: CorrelationId::new(1004),
        room_id: RoomId::new("room-abc").unwrap(),
        chunk_index: 0,
        total_chunks: 5,
        staged: false,
    };
    let enc_ack = encode_message(&upload_ack).expect("serialization failed");
    let dec_ack: ServerMessage = decode_message(&enc_ack).expect("deserialization failed");
    assert_eq!(upload_ack, dec_ack);

    // Test RegisterClient with auth_token roundtrip
    let reg_msg = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1004),
        room_id: RoomId::new("room-abc").unwrap(),
        client_id: ClientId::new("client-1").unwrap(),
        auth_token: "jwt.signed.token.abc123xyz".to_string(),
        current_seq: Some(SequenceNumber::new(5)),
    };
    let enc_reg = encode_message(&reg_msg).expect("serialization failed");
    let dec_reg: ClientMessage = decode_message(&enc_reg).expect("deserialization failed");
    assert_eq!(reg_msg, dec_reg);

    // Test GetSchema roundtrip
    let get_schema_msg = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(1005),
        room_id: RoomId::new("room-abc").unwrap(),
    };
    let enc_get_schema = encode_message(&get_schema_msg).expect("serialization failed");
    let dec_get_schema: ClientMessage =
        decode_message(&enc_get_schema).expect("deserialization failed");
    assert_eq!(get_schema_msg, dec_get_schema);

    // Test Registered message with schema and schema_id roundtrip
    let test_schema = Schema::from_tables(vec![TableSchema::builder("tasks")
        .primary_key("id", DataType::Uuid)
        .column("title", DataType::String)
        .build()
        .unwrap()]);
    let registered_msg = ServerMessage::Registered {
        correlation_id: CorrelationId::new(1004),
        room_id: RoomId::new("room-abc").unwrap(),
        head_seq: SequenceNumber::new(1),
        tail_seq: SequenceNumber::new(1),
        schema_id: SchemaId::new("schema-workspace-v1").unwrap(),
        schema: test_schema.clone(),
        active_snapshot_seq: Some(SequenceNumber::new(1)),
    };
    let enc_registered = encode_message(&registered_msg).expect("serialization failed");
    let dec_registered: ServerMessage =
        decode_message(&enc_registered).expect("deserialization failed");
    assert_eq!(registered_msg, dec_registered);

    // Test Schema response message roundtrip
    let schema_resp_msg = ServerMessage::Schema {
        correlation_id: CorrelationId::new(1005),
        room_id: RoomId::new("room-abc").unwrap(),
        schema_id: SchemaId::new("schema-workspace-v1").unwrap(),
        schema: test_schema,
    };
    let enc_schema_resp = encode_message(&schema_resp_msg).expect("serialization failed");
    let dec_schema_resp: ServerMessage =
        decode_message(&enc_schema_resp).expect("deserialization failed");
    assert_eq!(schema_resp_msg, dec_schema_resp);

    // Test new ErrorCodes roundtrip
    let error_codes = vec![
        ErrorCode::Unauthorized,
        ErrorCode::RoomAlreadyExists,
        ErrorCode::TableAlreadyExists,
        ErrorCode::SchemaNotFound,
    ];
    for code in error_codes {
        let err_msg = ServerMessage::Error {
            correlation_id: Some(CorrelationId::new(9999)),
            room_id: Some(RoomId::new("room-abc").unwrap()),
            code,
            message: format!("Test error for {:?}", code),
        };
        let enc_err = encode_message(&err_msg).expect("serialization failed");
        let dec_err: ServerMessage = decode_message(&enc_err).expect("deserialization failed");
        assert_eq!(err_msg, dec_err);
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
    // Append trailing corrupted/garbage bytes
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
    let ack_msg = ClientMessage::Ack {
        correlation_id: CorrelationId::new(101),
        room_id: RoomId::new("room-ack").unwrap(),
        client_id: ClientId::new("client-ack").unwrap(),
        ack_seq: SequenceNumber::new(42),
    };

    let encoded_ack = encode_message(&ack_msg).expect("serialization failed");
    let decoded_ack: ClientMessage = decode_message(&encoded_ack).expect("deserialization failed");
    assert_eq!(ack_msg, decoded_ack);

    let ack_confirmed = ServerMessage::AckConfirmed {
        correlation_id: CorrelationId::new(101),
        room_id: RoomId::new("room-ack").unwrap(),
        ack_seq: SequenceNumber::new(42),
        head_seq: SequenceNumber::new(50),
    };

    let enc_conf = encode_message(&ack_confirmed).expect("serialization failed");
    let dec_conf: ServerMessage = decode_message(&enc_conf).expect("deserialization failed");
    assert_eq!(ack_confirmed, dec_conf);
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
    oversized[..4].copy_from_slice(&[0x5A, 0x4D, 0x02, 0x00]);
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
    let ack = ServerMessage::DeregisterAck {
        correlation_id: CorrelationId::new(55),
        room_id: RoomId::new("room-ack").unwrap(),
        client_id: ClientId::new("client-ack").unwrap(),
    };
    let encoded = encode_message(&ack).expect("serialization failed");
    let decoded: ServerMessage = decode_message(&encoded).expect("deserialization failed");
    assert_eq!(ack, decoded);
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
    let max_frame = MAX_FRAME_SIZE;
    assert_eq!(
        MAX_FRAME_SIZE,
        MAX_MESSAGE_SIZE as usize + PROTOCOL_HEADER_LEN
    );

    // A payload of exactly MAX_MESSAGE_SIZE bytes is accepted by the encoder, and the frame it
    // produces (payload plus header) is accepted by the decoder.
    let value = value_with_frame_len(max_frame);
    let frame = encode_message(&value).expect("max-size payload must encode");
    assert_eq!(frame.len(), max_frame);
    let decoded: bytes::Bytes = decode_message(&frame).expect("max-size frame must decode");
    assert_eq!(decoded, value);

    // One byte more cannot be encoded, and a frame of that size is rejected by the decoder.
    assert!(encode_message(&value_with_frame_len(max_frame + 1)).is_err());
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

#[test]
fn largest_accepted_operation_fits_alone_in_any_response() {
    let max_room = RoomId::new("r".repeat(64)).unwrap();
    let op = op_with_wire_len(u64::MAX, MAX_RESPONSE_OPS_BYTES);
    assert_eq!(
        check_operation_size(&op, Some(MutationId::new([0xFF; 16]))),
        Ok(())
    );

    // With every other field at its largest encoding, both responses still encode.
    let commit_ack = ServerMessage::CommitAck {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: max_room.clone(),
        mutation_id: MutationId::new([0xFF; 16]),
        assigned_seq: SequenceNumber::new(u64::MAX),
        catchup_ops: vec![op.clone()],
        has_more: true,
        snapshot_wanted: true,
    };
    let frame = encode_message(&commit_ack).expect("CommitAck with the largest operation");
    assert!(frame.len() <= MAX_FRAME_SIZE);
    let sync_batch = ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: max_room,
        head_seq: SequenceNumber::new(u64::MAX),
        ops: vec![op],
        has_more: true,
        snapshot_wanted: true,
    };
    assert!(encode_message(&sync_batch).is_ok());

    // One byte more is rejected before it could ever be accepted.
    let too_large = op_with_wire_len(u64::MAX, MAX_RESPONSE_OPS_BYTES + 1);
    assert!(matches!(
        check_operation_size(&too_large, None),
        Err(OperationSizeError::Response { .. })
    ));
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
