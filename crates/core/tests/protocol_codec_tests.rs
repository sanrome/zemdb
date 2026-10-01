use zemdb_core::*;

#[test]
fn test_protocol_rejects_payload_exceeding_max_message_size() {
    // A payload whose size exceeds MAX_MESSAGE_SIZE must be rejected with SizeLimit
    let oversized = vec![0u8; (MAX_MESSAGE_SIZE + 1) as usize];
    let result: Result<ClientMessage, _> = decode_message(&oversized);
    assert!(
        result.is_err(),
        "Expected deserialization to be rejected by size limit"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(*err, bincode::ErrorKind::SizeLimit),
        "Expected SizeLimit error, got: {:?}",
        err
    );
}

#[test]
fn test_protocol_binary_serialization_roundtrip() {
    let compact = CompactRow::new(vec![Value::Int(42), Value::String("Testing binary".into())]);

    let mutation_id = MutationId::new([1u8; 16]);
    let client_msg = ClientMessage::Commit {
        correlation_id: CorrelationId::new(1001),
        room_id: RoomId::new("room-abc"),
        client_id: ClientId::new("client-1"),
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
        room_id: RoomId::new("room-abc"),
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
    };
    let encoded_ack = encode_message(&commit_ack_msg).expect("serialization failed");
    let decoded_ack: ServerMessage = decode_message(&encoded_ack).expect("deserialization failed");
    assert_eq!(commit_ack_msg, decoded_ack);

    let server_msg = ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1001),
        room_id: RoomId::new("room-abc"),
        head_seq: SequenceNumber::new(150),
        ops: vec![SequencedOperation {
            seq: SequenceNumber::new(150),
            op: Operation::delete(0, PrimaryKey::single(42i64), 1000),
        }],
        has_more: false,
    };

    let encoded_server = encode_message(&server_msg).expect("serialization failed");
    let decoded_server: ServerMessage =
        decode_message(&encoded_server).expect("deserialization failed");
    assert_eq!(server_msg, decoded_server);

    // Test DeregisterClient roundtrip
    let dereg_msg = ClientMessage::DeregisterClient {
        correlation_id: CorrelationId::new(1002),
        room_id: RoomId::new("room-abc"),
        client_id: ClientId::new("client-1"),
    };
    let encoded_dereg = encode_message(&dereg_msg).expect("serialization failed");
    let decoded_dereg: ClientMessage =
        decode_message(&encoded_dereg).expect("deserialization failed");
    assert_eq!(dereg_msg, decoded_dereg);

    // Test RequestSnapshotChunk roundtrip
    let req_chunk = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(1003),
        room_id: RoomId::new("room-abc"),
        chunk_index: 2,
        chunk_size: 4 * 1024 * 1024,
    };
    let enc_req = encode_message(&req_chunk).expect("serialization failed");
    let dec_req: ClientMessage = decode_message(&enc_req).expect("deserialization failed");
    assert_eq!(req_chunk, dec_req);

    // Test SnapshotChunk roundtrip
    let dummy_payload = b"snapshot payload chunk 2 data...";
    let expected_hash = ServerMessage::compute_snapshot_hash(dummy_payload);
    let snap_chunk = ServerMessage::SnapshotChunk {
        correlation_id: CorrelationId::new(1003),
        room_id: RoomId::new("room-abc"),
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
        room_id: RoomId::new("room-abc"),
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
        room_id: RoomId::new("room-abc"),
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
        room_id: RoomId::new("room-abc"),
        client_id: ClientId::new("client-1"),
        auth_token: "jwt.signed.token.abc123xyz".to_string(),
        current_seq: Some(SequenceNumber::new(5)),
    };
    let enc_reg = encode_message(&reg_msg).expect("serialization failed");
    let dec_reg: ClientMessage = decode_message(&enc_reg).expect("deserialization failed");
    assert_eq!(reg_msg, dec_reg);

    // Test GetSchema roundtrip
    let get_schema_msg = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(1005),
        room_id: RoomId::new("room-abc"),
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
        room_id: RoomId::new("room-abc"),
        head_seq: SequenceNumber::new(1),
        tail_seq: SequenceNumber::new(1),
        schema_id: SchemaId::new("schema-workspace-v1"),
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
        room_id: RoomId::new("room-abc"),
        schema_id: SchemaId::new("schema-workspace-v1"),
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
            room_id: Some(RoomId::new("room-abc")),
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
        client_id: ClientId::new("client-test"),
        room_id: RoomId::new("room-1"),
    };

    let mut encoded = encode_message(&client_msg).expect("serialization failed");
    // Append trailing corrupted/garbage bytes
    encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

    let result: Result<ClientMessage, _> = decode_message(&encoded);
    assert!(
        result.is_err(),
        "Expected deserialization to reject trailing bytes, but succeeded"
    );
}

#[test]
fn test_ack_messages_codec_roundtrip() {
    let ack_msg = ClientMessage::Ack {
        correlation_id: CorrelationId::new(101),
        room_id: RoomId::new("room-ack"),
        client_id: ClientId::new("client-ack"),
        ack_seq: SequenceNumber::new(42),
    };

    let encoded_ack = encode_message(&ack_msg).expect("serialization failed");
    let decoded_ack: ClientMessage = decode_message(&encoded_ack).expect("deserialization failed");
    assert_eq!(ack_msg, decoded_ack);

    let ack_confirmed = ServerMessage::AckConfirmed {
        correlation_id: CorrelationId::new(101),
        room_id: RoomId::new("room-ack"),
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
        client_id: ClientId::new("client-test"),
        room_id: RoomId::new("room-1"),
    };

    let encoded = encode_message(&msg).expect("serialization should succeed");
    assert!(encoded.len() >= 4);
    assert_eq!(&encoded[0..2], b"ZM");
    assert_eq!(encoded[2], 0x01);
    assert_eq!(encoded[3], 0x00);

    // Corrupted magic bytes must be rejected
    let mut bad_magic = encoded.clone();
    bad_magic[0] = b'X';
    let res: Result<ClientMessage, _> = decode_message(&bad_magic);
    assert!(res.is_err());
    let err_str = res.unwrap_err().to_string();
    assert!(err_str.contains("magic"));

    // Protocol version mismatch must be rejected with version mismatch error
    let mut bad_version = encoded.clone();
    bad_version[2] = 0x99;
    let res: Result<ClientMessage, _> = decode_message(&bad_version);
    assert!(res.is_err());
    let err_str = res.unwrap_err().to_string();
    assert!(err_str.contains("version"));
}

#[test]
fn test_deregister_ack_message_roundtrip() {
    let ack = ServerMessage::DeregisterAck {
        correlation_id: CorrelationId::new(55),
        room_id: RoomId::new("room-ack"),
        client_id: ClientId::new("client-ack"),
    };
    let encoded = encode_message(&ack).expect("serialization failed");
    let decoded: ServerMessage = decode_message(&encoded).expect("deserialization failed");
    assert_eq!(ack, decoded);
}
