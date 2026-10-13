use zemdb_core::*;

#[test]
fn test_value_and_primary_key_memory_footprint() {
    assert_eq!(std::mem::size_of::<Value>(), 24);
    assert_eq!(std::mem::align_of::<Value>(), 8);
    assert_eq!(std::mem::size_of::<PrimaryKey>(), 40);
    assert_eq!(std::mem::size_of::<ColumnUpdate>(), 32);
    assert_eq!(std::mem::size_of::<OperationKind>(), 32);
    // A SequencedOperation is an 8-byte sequence number and an Operation, with no padding.
    assert_eq!(std::mem::size_of::<Operation>(), 88);
    assert_eq!(std::mem::size_of::<SequencedOperation>(), 96);
}

#[test]
fn test_value_ord_and_type_order() {
    let uuid = Value::from_uuid_str("12345678-1234-1234-1234-123456789abc").unwrap();
    let expected = [
        Value::Null,
        Value::Bool(true),
        Value::Int(42),
        Value::Float(2.75),
        Value::Timestamp(500),
        uuid,
        Value::String("hello".into()),
        Value::from(vec![1, 2, 3]),
    ];
    // The same values in another order.
    let mut values = [6, 0, 4, 5, 2, 1, 3, 7].map(|i| expected[i].clone());

    values.sort();

    assert_eq!(values, expected);
}

#[test]
fn test_uuid_data_type_primary_key_and_parsing() {
    let uuid_str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
    let parsed = Value::parse_uuid(uuid_str).expect("should parse valid uuid");
    let val = Value::Uuid(parsed);

    assert_eq!(val.data_type(), DataType::Uuid);
    assert_eq!(format!("{}", val), uuid_str);
    assert_eq!(val.to_uuid(), Some(parsed));

    let compact_hex = "a1b2c3d4e5f67890abcdef1234567890";
    assert_eq!(Value::parse_uuid(compact_hex), Some(parsed));
    assert_eq!(Value::parse_uuid(&uuid_str.to_uppercase()), Some(parsed));
    for invalid in [
        "a1b2c3d-4e5f6-7890-abcd-ef1234567890",
        "a1b2c3d4-e5f6-7890-abcd-ef123456789g",
        &uuid_str[..35],
        format!("{uuid_str}0").as_str(),
        &compact_hex[..31],
        format!("{compact_hex}0").as_str(),
    ] {
        assert_eq!(Value::parse_uuid(invalid), None, "{invalid:?}");
    }

    // Schema with UUID primary key
    let items_table = TableSchema::builder("items")
        .table_id(0)
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

    let encoded = bincode::serialize(&row).unwrap();
    let decoded: Row = bincode::deserialize(&encoded).unwrap();
    assert_eq!(row, decoded);
}

#[test]
fn test_newtypes_ergonomics_and_serde() {
    let room = RoomId::new("room-123").unwrap();
    let schema_id = SchemaId::new("schema-v1").unwrap();
    let client = ClientId::new("client-456").unwrap();
    let seq = SequenceNumber::from(42u64);
    let mutation = MutationId::from([7u8; 16]);
    let correlation = CorrelationId::from(999u64);

    // Accessors and AsRef instead of Deref
    assert_eq!(room.as_str(), "room-123");
    assert_eq!(room.as_ref(), "room-123");
    assert_eq!(schema_id.as_str(), "schema-v1");
    assert_eq!(schema_id.as_ref(), "schema-v1");
    assert_eq!(client.as_str(), "client-456");
    assert_eq!(client.as_ref(), "client-456");
    assert_eq!(seq.get(), 42);
    assert_eq!(correlation.get(), 999);
    assert_eq!(mutation.as_bytes(), &[7u8; 16]);
    assert_eq!(AsRef::<[u8; 16]>::as_ref(&mutation), &[7u8; 16]);
    assert_eq!(AsRef::<[u8]>::as_ref(&mutation), &[7u8; 16][..]);
    assert_eq!(seq.next(), SequenceNumber::new(43));

    // Display
    assert_eq!(format!("{}", room), "room-123");
    assert_eq!(format!("{}", schema_id), "schema-v1");
    assert_eq!(format!("{}", client), "client-456");
    assert_eq!(format!("{}", seq), "42");
    assert_eq!(format!("{}", mutation), "07070707070707070707070707070707");

    // Serialized like the value they wrap.
    assert_eq!(
        bincode::serialize(&schema_id).unwrap(),
        bincode::serialize("schema-v1").unwrap()
    );
    assert_eq!(
        bincode::serialize(&seq).unwrap(),
        bincode::serialize(&42u64).unwrap()
    );
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
