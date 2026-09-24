use rimdb_core::*;

#[test]
fn test_protocol_rejects_payload_exceeding_max_message_size() {
    // A payload whose size exceeds MAX_MESSAGE_SIZE must be rejected with SizeLimit
    let oversized = vec![0u8; (MAX_MESSAGE_SIZE + 1) as usize];
    let result: Result<ClientMessage, _> = decode_message(&oversized);
    assert!(result.is_err(), "Expected deserialization to be rejected by size limit");
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
    let decoded_dereg: ClientMessage = decode_message(&encoded_dereg).expect("deserialization failed");
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

    // Test RegisterClient with auth_token roundtrip
    let reg_msg = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1004),
        room_id: RoomId::new("room-abc"),
        client_id: ClientId::new("client-1"),
        auth_token: "jwt.signed.token.abc123xyz".to_string(),
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
        schema_id: SchemaId::new("schema-workspace-v1"),
        schema: test_schema.clone(),
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
        last_ack_seq: SequenceNumber::new(10),
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
