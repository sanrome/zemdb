pub mod operation;
pub mod protocol;
pub mod schema;
pub mod value;

pub use operation::{
    squash_operations, Operation, SequencedOperation, SquashOutcome, UpdateBuilder,
};
pub use protocol::{
    decode_message, encode_message, ClientMessage, CorrelationId, ErrorCode, MutationId,
    ServerMessage, MAX_MESSAGE_SIZE,
};
pub use schema::{
    ColumnDef, Schema, SchemaBuilder, TableBuilder, TableSchema, ValidationError,
};
pub use value::{CompactRow, DataType, PrimaryKey, Row, RowBuilder, Value};

#[cfg(test)]
mod tests {
    use super::*;

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
        let pk = PrimaryKey::single(1i64);
        let update_op = Operation::update("users", pk.clone())
            .set("id", 2i64) // Illegal: modifying PK
            .build();

        if let Operation::Update { fields, .. } = update_op {
            let err = schema.validate_update("users", &pk, &fields).unwrap_err();
            assert_eq!(
                err,
                ValidationError::CannotUpdatePrimaryKey {
                    table: "users".to_string(),
                    column: "id".to_string()
                }
            );
        } else {
            panic!("Expected Operation::Update");
        }
    }

    #[test]
    fn test_value_ord_and_type_order() {
        let mut values = [
            Value::String("hello".to_string()),
            Value::Null,
            Value::Timestamp(500),
            Value::Int(42),
            Value::Bool(true),
            Value::Float(2.75),
            Value::Bytes(vec![1, 2, 3].into()),
        ];

        values.sort();

        assert_eq!(values[0], Value::Null);
        assert_eq!(values[1], Value::Bool(true));
        assert_eq!(values[2], Value::Int(42));
        assert_eq!(values[3], Value::Float(2.75));
        assert_eq!(values[4], Value::Timestamp(500));
        assert_eq!(values[5], Value::String("hello".to_string()));
        assert_eq!(values[6], Value::Bytes(vec![1, 2, 3].into()));
    }

    #[test]
    fn test_squash_insert_then_update() {
        let row = RowBuilder::new()
            .set("id", 10i64)
            .set("name", "Alice")
            .set("age", 25i64)
            .build();

        let mut base_op = Operation::insert("users", PrimaryKey::single(10i64), row);

        let incoming = Operation::update("users", PrimaryKey::single(10i64))
            .set("age", 26i64)
            .timestamp(100)
            .build();

        let outcome = squash_operations(&mut base_op, incoming);
        assert_eq!(outcome, SquashOutcome::Merged);

        if let Operation::Insert { row, timestamp, .. } = base_op {
            assert_eq!(row.get("age"), Some(&Value::Int(26)));
            assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(timestamp, 100);
        } else {
            panic!("Expected Operation::Insert");
        }
    }

    #[test]
    fn test_squash_update_then_update_field_merge() {
        let pk = PrimaryKey::single(10i64);

        let mut base_op = Operation::update("users", pk.clone())
            .set("name", "Alice")
            .timestamp(100)
            .build();

        let incoming = Operation::update("users", pk.clone())
            .set("age", 30i64)
            .timestamp(105)
            .build();

        let outcome = squash_operations(&mut base_op, incoming);
        assert_eq!(outcome, SquashOutcome::Merged);

        if let Operation::Update { fields, timestamp, .. } = base_op {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(fields.get("age"), Some(&Value::Int(30)));
            assert_eq!(timestamp, 105);
        } else {
            panic!("Expected Operation::Update");
        }
    }

    #[test]
    fn test_anti_zombie_rule_delete_then_update_rejected() {
        let pk = PrimaryKey::single(1i64);
        let mut del_op = Operation::delete("users", pk.clone(), 100);
        let up_op = Operation::update("users", pk)
            .set("name", "Zombie Alice")
            .timestamp(105)
            .build();

        let outcome = squash_operations(&mut del_op, up_op);
        // Anti-zombie rule: A partial update CANNOT resurrect a delete!
        assert_eq!(outcome, SquashOutcome::Incompatible);
        assert!(del_op.is_delete());
    }

    #[test]
    fn test_squash_insert_then_delete() {
        let row = RowBuilder::new().set("id", 1i64).build();
        let pk = PrimaryKey::single(1i64);

        let mut base_op = Operation::insert("users", pk.clone(), row);
        let incoming = Operation::delete("users", pk, 200);

        let outcome = squash_operations(&mut base_op, incoming.clone());
        assert_eq!(outcome, SquashOutcome::Replaced);
        assert_eq!(base_op, incoming);
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
        assert_eq!(restored.get("name"), Some(&Value::String("Carol".to_string())));
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
    fn test_protocol_binary_serialization_roundtrip() {
        let row = RowBuilder::new()
            .set("id", 42i64)
            .set("note", "Testing binary")
            .build();

        let mutation_id = [1u8; 16];
        let client_msg = ClientMessage::Commit {
            correlation_id: 1001,
            room_id: "room-abc".to_string(),
            client_id: "client-1".to_string(),
            mutation_id,
            op: Operation::insert_with_timestamp("notes", PrimaryKey::single(42i64), row, 500),
        };

        let encoded = encode_message(&client_msg).expect("serialization failed");
        let decoded: ClientMessage = decode_message(&encoded).expect("deserialization failed");
        assert_eq!(client_msg, decoded);

        let server_msg = ServerMessage::SyncBatch {
            correlation_id: 1001,
            room_id: "room-abc".to_string(),
            head_seq: 150,
            ops: vec![SequencedOperation {
                seq: 150,
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
