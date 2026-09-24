use rimdb_core::*;

#[test]
fn test_value_and_primary_key_memory_footprint() {
    assert_eq!(std::mem::size_of::<Value>(), 24);
    assert_eq!(std::mem::align_of::<Value>(), 8);
    assert_eq!(std::mem::size_of::<PrimaryKey>(), 40);
    assert_eq!(std::mem::size_of::<ColumnUpdate>(), 32);
    assert_eq!(std::mem::size_of::<OperationKind>(), 32);
    assert_eq!(std::mem::size_of::<Operation>(), 88);
    assert_eq!(std::mem::size_of::<SequencedOperation>(), 96);
    // Guarantee PrimaryKey fits in standard 64-byte L1 cache line without split!
    assert!(std::mem::size_of::<PrimaryKey>() <= 64);
    // Operation and SequencedOperation are strictly 8-byte aligned with 0 tail padding
    assert_eq!(std::mem::size_of::<Operation>() % 8, 0);
    assert_eq!(std::mem::size_of::<SequencedOperation>() % 8, 0);
}

#[test]
fn test_value_ord_and_type_order() {
    let uuid = Value::from_uuid_str("12345678-1234-1234-1234-123456789abc").unwrap();
    let mut values = [
        Value::String("hello".into()),
        Value::Null,
        Value::Timestamp(500),
        uuid.clone(),
        Value::Int(42),
        Value::Bool(true),
        Value::Float(2.75),
        Value::from(vec![1, 2, 3]),
    ];

    values.sort();

    assert_eq!(values[0], Value::Null);
    assert_eq!(values[1], Value::Bool(true));
    assert_eq!(values[2], Value::Int(42));
    assert_eq!(values[3], Value::Float(2.75));
    assert_eq!(values[4], Value::Timestamp(500));
    assert_eq!(values[5], uuid);
    assert_eq!(values[6], Value::String("hello".into()));
    assert_eq!(values[7], Value::from(vec![1, 2, 3]));
}

#[test]
fn test_uuid_data_type_primary_key_and_parsing() {
    let uuid_str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
    let parsed = Value::parse_uuid(uuid_str).expect("should parse valid uuid");
    let val = Value::Uuid(parsed);

    assert_eq!(val.data_type(), DataType::Uuid);
    assert_eq!(format!("{}", val), uuid_str);
    assert_eq!(val.to_uuid(), Some(parsed));

    // Also test parsing compact 32-hex string without hyphens
    let compact_hex = "a1b2c3d4e5f67890abcdef1234567890";
    assert_eq!(Value::parse_uuid(compact_hex), Some(parsed));

    // Schema with UUID primary key
    let items_table = TableSchema::builder("items")
        .primary_key("id", DataType::Uuid)
        .column("title", DataType::String)
        .build()
        .unwrap();

    let row = RowBuilder::new()
        .set("id", val.clone())
        .set("title", "Distributed Engine")
        .build();

    let pk = items_table.extract_pk(&row).unwrap();
    assert_eq!(pk, PrimaryKey::single(val));

    // Test bincode roundtrip of UUID Value
    let encoded = bincode::serialize(&row).unwrap();
    let decoded: Row = bincode::deserialize(&encoded).unwrap();
    assert_eq!(row, decoded);
}

#[test]
fn test_newtypes_ergonomics_and_serde() {
    let room = RoomId::new("room-123");
    let client = ClientId::from("client-456");
    let seq = SequenceNumber::from(42u64);
    let mutation = MutationId::from([7u8; 16]);
    let correlation = CorrelationId::from(999u64);

    // Deref ergonomics
    assert_eq!(&*room, "room-123");
    assert_eq!(&*client, "client-456");
    assert_eq!(*seq, 42);
    assert_eq!(*correlation, 999);
    assert_eq!(seq.next(), SequenceNumber::new(43));

    // Display
    assert_eq!(format!("{}", room), "room-123");
    assert_eq!(format!("{}", client), "client-456");
    assert_eq!(format!("{}", seq), "42");
    assert_eq!(format!("{}", mutation), "07070707070707070707070707070707");

    // Transparent Serde roundtrip: binary size should match primitive
    let encoded_room = bincode::serialize(&room).unwrap();
    let encoded_raw_str = bincode::serialize("room-123").unwrap();
    assert_eq!(encoded_room, encoded_raw_str);

    let encoded_seq = bincode::serialize(&seq).unwrap();
    let encoded_raw_u64 = bincode::serialize(&42u64).unwrap();
    assert_eq!(encoded_seq, encoded_raw_u64);
}

#[test]
fn test_operation_methods() {
    let row = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
    let op = Operation::insert(1, PrimaryKey::single(1i64), row, 100);

    assert_eq!(op.pk(), &PrimaryKey::single(1i64));
    assert_eq!(op.timestamp(), 100);
    assert!(op.is_insert());
    assert!(!op.is_delete());
    assert!(!op.is_update());
    assert_eq!(op.table_id(), 1);
}
