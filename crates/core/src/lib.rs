pub mod id;
pub mod operation;
pub mod protocol;
pub mod schema;
pub mod value;

pub use id::{ClientId, CorrelationId, MutationId, RoomId, SequenceNumber};
pub use operation::{
    squash_operations, squash_table_operations, ColumnUpdate, Operation, OperationKind,
    SequencedOperation, SquashOutcome, TableBuffer, TableOperation, UpdateBuilder,
};
pub use protocol::{
    decode_message, encode_message, ClientMessage, ErrorCode, ServerMessage, MAX_MESSAGE_SIZE,
};
pub use schema::{
    ColumnDef, Schema, SchemaBuilder, SchemaUpdateBuilder, TableBuilder, TableSchema,
    ValidationError,
};
pub use value::{CompactRow, DataType, PrimaryKey, Row, RowBuilder, Value};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_schema() -> Schema {
        let users = TableSchema::builder("users")
            .primary_key("id", DataType::Int)
            .column("name", DataType::String)
            .column("age", DataType::Int)
            .nullable_column("bio", DataType::String)
            .encrypted_column("secret_chat", DataType::String) // Stored as bytes in transit, decrypted as string
            .build()
            .expect("valid table schema");

        Schema::builder().table(users).build()
    }

    #[test]
    fn test_value_and_primary_key_memory_footprint() {
        assert_eq!(std::mem::size_of::<Value>(), 24);
        assert_eq!(std::mem::align_of::<Value>(), 8);
        assert_eq!(std::mem::size_of::<PrimaryKey>(), 40);
        assert_eq!(std::mem::size_of::<ColumnUpdate>(), 32);
        assert_eq!(std::mem::size_of::<OperationKind>(), 32);
        assert_eq!(std::mem::size_of::<TableOperation>(), 80);
        assert_eq!(std::mem::size_of::<Operation>(), 96);
        // Guarantee PrimaryKey fits in standard 64-byte L1 cache line without split!
        assert!(std::mem::size_of::<PrimaryKey>() <= 64);
        // TableOperation is exactly 80 bytes with 0 padding at struct tail
        assert_eq!(std::mem::size_of::<TableOperation>() % 8, 0);
    }

    #[test]
    fn test_schema_insert_validation_success() {
        let schema = sample_schema();
        let row = RowBuilder::new()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("age", 30i64)
            .set("secret_chat", vec![0xCA, 0xFE, 0xBA, 0xBE]) // Must be bytes in transit
            .build();

        let pk = schema.validate_insert("users", &row).expect("should succeed");
        assert_eq!(pk, PrimaryKey::single(1i64));
    }

    #[test]
    fn test_schema_rejects_missing_non_null_column() {
        let schema = sample_schema();
        // Missing required 'age' (nullable: false)
        let row = RowBuilder::new()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("secret_chat", vec![1, 2, 3])
            .build();

        let err = schema.validate_insert("users", &row).unwrap_err();
        assert_eq!(
            err,
            ValidationError::MissingRequiredColumn {
                table: "users".to_string(),
                column: "age".to_string()
            }
        );
    }

    #[test]
    fn test_encrypted_column_rejects_non_bytes_in_transit() {
        let schema = sample_schema();
        let row = RowBuilder::new()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("age", 30i64)
            .set("secret_chat", "plaintext_forbidden") // Invalid: must be opaque bytes!
            .build();

        let err = schema.validate_insert("users", &row).unwrap_err();
        assert_eq!(
            err,
            ValidationError::EncryptedColumnMustBeBytes {
                table: "users".to_string(),
                column: "secret_chat".to_string()
            }
        );
    }

    #[test]
    fn test_table_builder_rejects_encrypted_primary_key() {
        let result = TableSchema::builder("secrets")
            .column("data", DataType::String)
            // Invalid: PK cannot be encrypted
            .encrypted_column("id", DataType::Int)
            .primary_key("id", DataType::Int)
            .build();

        assert_eq!(
            result.unwrap_err(),
            ValidationError::EncryptedPrimaryKeyNotAllowed {
                table: "secrets".to_string(),
                column: "id".to_string()
            }
        );
    }

    #[test]
    fn test_schema_insert_missing_pk() {
        let schema = sample_schema();
        let row = RowBuilder::new()
            .set("name", "Bob")
            .set("age", 25i64)
            .set("secret_chat", vec![1, 2])
            .build();

        let err = schema.validate_insert("users", &row).unwrap_err();
        assert_eq!(
            err,
            ValidationError::MissingPrimaryKeyColumn {
                table: "users".to_string(),
                column: "id".to_string()
            }
        );
    }

    #[test]
    fn test_schema_insert_type_mismatch() {
        let schema = sample_schema();
        let row = RowBuilder::new()
            .set("id", 2i64)
            .set("name", 123i64) // Expects String
            .set("age", 20i64)
            .set("secret_chat", vec![1])
            .build();

        let err = schema.validate_insert("users", &row).unwrap_err();
        assert_eq!(
            err,
            ValidationError::TypeMismatch {
                table: "users".to_string(),
                column: "name".to_string(),
                expected: DataType::String,
                actual: DataType::Int
            }
        );
    }

    #[test]
    fn test_schema_validates_pk_type_on_update_and_delete() {
        let schema = sample_schema();
        let wrong_pk = PrimaryKey::single("not_an_int");
        let fields = RowBuilder::new().set("name", "Updated").build();

        let err_update = schema.validate_update("users", &wrong_pk, &fields).unwrap_err();
        assert_eq!(
            err_update,
            ValidationError::PrimaryKeyTypeMismatch {
                table: "users".to_string(),
                column: "id".to_string(),
                expected: DataType::Int,
                actual: DataType::String,
            }
        );

        let err_delete = schema.validate_delete("users", &wrong_pk).unwrap_err();
        assert_eq!(
            err_delete,
            ValidationError::PrimaryKeyTypeMismatch {
                table: "users".to_string(),
                column: "id".to_string(),
                expected: DataType::Int,
                actual: DataType::String,
            }
        );
    }

    #[test]
    fn test_schema_update_cannot_modify_pk() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::Int(2));
        let err = table.compact_update_fields(&fields).unwrap_err();
        assert_eq!(
            err,
            ValidationError::CannotUpdatePrimaryKey {
                table: "users".to_string(),
                column: "id".to_string(),
            }
        );
    }

    #[test]
    fn test_value_ord_and_type_order() {
        let mut values = [
            Value::String("hello".into()),
            Value::Null,
            Value::Timestamp(500),
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
        assert_eq!(values[5], Value::String("hello".into()));
        assert_eq!(values[6], Value::from(vec![1, 2, 3]));
    }

    #[test]
    fn test_squash_insert_then_update() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let row = RowBuilder::new()
            .set("id", 10i64)
            .set("name", "Alice")
            .set("age", 25i64)
            .set("secret_chat", vec![1, 2, 3])
            .build();

        let mut base_op = table.to_operation_insert(&row, 50).unwrap();

        let incoming = table
            .update_builder(PrimaryKey::single(10i64))
            .set("age", 26i64)
            .timestamp(100)
            .build()
            .unwrap();

        let outcome = squash_operations(&mut base_op, incoming);
        assert_eq!(outcome, SquashOutcome::Merged);

        if let OperationKind::Insert { row } = &base_op.kind {
            let restored = table.from_compact_row(row).unwrap();
            assert_eq!(restored.get("age"), Some(&Value::Int(26)));
            assert_eq!(restored.get("name"), Some(&Value::String("Alice".into())));
            assert_eq!(base_op.timestamp, 100);
        } else {
            panic!("Expected OperationKind::Insert");
        }
    }

    #[test]
    fn test_squash_update_then_update_field_merge() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let pk = PrimaryKey::single(10i64);

        let mut base_op = table
            .update_builder(pk.clone())
            .set("name", "Alice")
            .timestamp(100)
            .build()
            .unwrap();

        let incoming = table
            .update_builder(pk)
            .set("age", 30i64)
            .timestamp(105)
            .build()
            .unwrap();

        let outcome = squash_operations(&mut base_op, incoming);
        assert_eq!(outcome, SquashOutcome::Merged);

        if let OperationKind::Update { updates } = &base_op.kind {
            let fields = table.expand_update_fields(updates).unwrap();
            assert_eq!(fields.len(), 2);
            assert_eq!(fields.get("name"), Some(&Value::String("Alice".into())));
            assert_eq!(fields.get("age"), Some(&Value::Int(30)));
            assert_eq!(base_op.timestamp, 105);
        } else {
            panic!("Expected OperationKind::Update");
        }
    }

    #[test]
    fn test_anti_zombie_rule_delete_then_update_rejected() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let pk = PrimaryKey::single(1i64);
        let mut del_op = Operation::delete("users", pk.clone(), 100);
        let up_op = table
            .update_builder(pk.clone())
            .set("name", "Zombie Alice")
            .timestamp(105)
            .build()
            .unwrap();

        let outcome = squash_operations(&mut del_op, up_op);
        // Anti-zombie rule: A newer partial update CANNOT resurrect a delete!
        assert_eq!(outcome, SquashOutcome::Incompatible);
        assert!(del_op.is_delete());

        // An older update arriving after delete is discarded
        let older_up_op = table
            .update_builder(pk)
            .set("name", "Old Update")
            .timestamp(90)
            .build()
            .unwrap();
        let outcome_old = squash_operations(&mut del_op, older_up_op);
        assert_eq!(outcome_old, SquashOutcome::Discarded);
        assert!(del_op.is_delete());
    }

    #[test]
    fn test_squash_insert_then_delete() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let row = RowBuilder::new()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("age", 30i64)
            .set("secret_chat", vec![1, 2, 3])
            .build();
        let pk = PrimaryKey::single(1i64);

        let mut base_op = table.to_operation_insert(&row, 100).unwrap();
        let incoming = Operation::delete("users", pk, 200);

        let outcome = squash_operations(&mut base_op, incoming.clone());
        assert_eq!(outcome, SquashOutcome::Replaced);
        assert_eq!(base_op, incoming);
    }

    #[test]
    fn test_squash_older_insert_into_newer_update_preserves_data() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let pk = PrimaryKey::single(42i64);
        let mut target_op = table
            .update_builder(pk.clone())
            .set("age", 35i64)
            .timestamp(200)
            .build()
            .unwrap();

        let base_row = RowBuilder::new()
            .set("id", 42i64)
            .set("name", "Older Alice")
            .set("age", 30i64)
            .set("secret_chat", vec![1, 2, 3])
            .build();
        let incoming_insert = table.to_operation_insert(&base_row, 100).unwrap();

        let outcome = squash_operations(&mut target_op, incoming_insert);
        assert_eq!(outcome, SquashOutcome::Merged);

        // The target must have been converted to an Insert, keeping the base fields and newer update fields!
        if let OperationKind::Insert { row } = &target_op.kind {
            let restored = table.from_compact_row(row).unwrap();
            assert_eq!(restored.get("id"), Some(&Value::Int(42)));
            assert_eq!(restored.get("name"), Some(&Value::String("Older Alice".into())));
            assert_eq!(restored.get("age"), Some(&Value::Int(35))); // Newer update field won!
            assert_eq!(target_op.timestamp, 200);
        } else {
            panic!("Expected target to be OperationKind::Insert");
        }
    }

    #[test]
    fn test_table_buffer_partitioned_squashing() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let mut buffer = TableBuffer::new("users");

        let row1 = RowBuilder::new()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("age", 25i64)
            .set("secret_chat", vec![1, 2, 3])
            .build();
        let op1 = table.to_table_insert(&row1, 100).unwrap();

        let row2 = RowBuilder::new()
            .set("id", 2i64)
            .set("name", "Bob")
            .set("age", 40i64)
            .set("secret_chat", vec![4, 5, 6])
            .build();
        let op2 = table.to_table_insert(&row2, 100).unwrap();

        assert_eq!(buffer.apply(op1), SquashOutcome::Replaced);
        assert_eq!(buffer.apply(op2), SquashOutcome::Replaced);
        assert_eq!(buffer.len(), 2);

        // Update Alice
        let up_alice = table
            .update_builder(PrimaryKey::single(1i64))
            .set("age", 26i64)
            .timestamp(150)
            .build_table_op()
            .unwrap();
        assert_eq!(buffer.apply(up_alice), SquashOutcome::Merged);
        assert_eq!(buffer.len(), 2);

        // Verify squashed Alice
        let alice_op = buffer.get(&PrimaryKey::single(1i64)).unwrap();
        if let OperationKind::Insert { row } = &alice_op.kind {
            let restored = table.from_compact_row(row).unwrap();
            assert_eq!(restored.get("age"), Some(&Value::Int(26)));
        } else {
            panic!("Expected Insert");
        }

        // Delete Bob
        let del_bob = TableOperation::delete(PrimaryKey::single(2i64), 200);
        assert_eq!(buffer.apply(del_bob), SquashOutcome::Replaced);

        let bob_op = buffer.get(&PrimaryKey::single(2i64)).unwrap();
        assert!(bob_op.is_delete());
    }

    #[test]
    fn test_deref_ergonomics_on_operation() {
        let row = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
        let op = Operation::insert("users", PrimaryKey::single(1i64), row, 100);

        // Access via Deref
        assert_eq!(op.pk(), &PrimaryKey::single(1i64));
        assert_eq!(op.timestamp(), 100);
        assert!(op.is_insert());
        assert!(!op.is_delete());
        assert!(!op.is_update());
        assert_eq!(op.table(), "users");
    }

    #[test]
    fn test_table_schema_preserves_ddl_definition_order() {
        let table = TableSchema::builder("products")
            .primary_key("id", DataType::Int)
            .column("zebra", DataType::String)
            .column("alpha", DataType::Int)
            .column("beta", DataType::Bool)
            .build()
            .expect("valid table");

        // Columns must be stored in DDL definition order (id, zebra, alpha, beta),
        // NOT alphabetical order (alpha, beta, id, zebra)!
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[1].name, "zebra");
        assert_eq!(table.columns[2].name, "alpha");
        assert_eq!(table.columns[3].name, "beta");

        let row = RowBuilder::new()
            .set("id", 1i64)
            .set("zebra", "Z")
            .set("alpha", 10i64)
            .set("beta", true)
            .build();

        let compact = table.to_compact_row(&row).expect("compact");
        assert_eq!(compact.values[0], Value::Int(1));
        assert_eq!(compact.values[1], Value::String("Z".into()));
        assert_eq!(compact.values[2], Value::Int(10));
        assert_eq!(compact.values[3], Value::Bool(true));
    }

    #[test]
    fn test_compact_row_conversion() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let row = RowBuilder::new()
            .set("id", 99i64)
            .set("name", "Carol")
            .set("age", 28i64)
            .set("secret_chat", vec![0xDE, 0xAD])
            .build();

        let compact = table.to_compact_row(&row).expect("should convert");
        assert_eq!(compact.len(), table.columns.len());

        let restored = table.from_compact_row(&compact).expect("should restore");
        assert_eq!(restored.get("id"), Some(&Value::Int(99)));
        assert_eq!(restored.get("name"), Some(&Value::String("Carol".into())));
        assert_eq!(restored.get("age"), Some(&Value::Int(28)));
    }

    #[test]
    fn test_zero_copy_row_conversions() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        let row = RowBuilder::new()
            .set("id", 99i64)
            .set("name", "Carol")
            .set("age", 28i64)
            .set("secret_chat", vec![0xDE, 0xAD])
            .build();

        let compact = table.row_into_compact(row).expect("should convert zero-copy");
        assert_eq!(compact.len(), table.columns.len());

        let restored = table.compact_into_row(compact).expect("should restore zero-copy");
        assert_eq!(restored.get("id"), Some(&Value::Int(99)));
        assert_eq!(restored.get("name"), Some(&Value::String("Carol".into())));
        assert_eq!(restored.get("age"), Some(&Value::Int(28)));
    }

    #[test]
    fn test_compact_row_arity_mismatch_rejected() {
        let schema = sample_schema();
        let table = schema.get_table("users").unwrap();
        // Table has 5 columns: id, name, age, bio, secret_chat
        let invalid_compact = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);

        let result = table.from_compact_row(&invalid_compact);
        assert_eq!(
            result,
            Err(ValidationError::CompactRowArityMismatch {
                table: "users".to_string(),
                expected: 5,
                actual: 2,
            })
        );
    }

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
}
