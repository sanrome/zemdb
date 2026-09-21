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
        op: Operation::insert("notes", PrimaryKey::single(42i64), compact, 500),
    };

    let encoded = encode_message(&client_msg).expect("serialization failed");
    let decoded: ClientMessage = decode_message(&encoded).expect("deserialization failed");
    assert_eq!(client_msg, decoded);

    let server_msg = ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1001),
        room_id: RoomId::new("room-abc"),
        head_seq: SequenceNumber::new(150),
        ops: vec![SequencedOperation {
            seq: SequenceNumber::new(150),
            op: Operation::delete("notes", PrimaryKey::single(42i64), 1000),
        }],
        has_more: false,
    };

    let encoded_server = encode_message(&server_msg).expect("serialization failed");
    let decoded_server: ServerMessage =
        decode_message(&encoded_server).expect("deserialization failed");
    assert_eq!(server_msg, decoded_server);
}
