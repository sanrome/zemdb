use rimdb_core::*;
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
    let table = schema.get_table_by_name("users").unwrap();
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
fn test_compact_row_conversion() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();
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
fn test_compact_row_arity_mismatch_rejected() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();
    // Table has 5 columns: id, name, age, bio, secret_chat
    // Passing 6 columns exceeds table arity and must be rejected
    let invalid_compact = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Alice".into()),
        Value::Int(30),
        Value::Null,
        Value::Bytes(vec![1, 2].into_boxed_slice()),
        Value::String("Extra field".into()),
    ]);

    let result = table.from_compact_row(&invalid_compact);
    assert_eq!(
        result,
        Err(ValidationError::CompactRowArityMismatch {
            table: "users".to_string(),
            expected: 5,
            actual: 6,
        })
    );
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
fn test_zero_copy_row_conversions() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();
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
fn test_schema_json_deserialization_from_tables_list() {
    let json = r#"{
        "tables": [
            {
                "name": "projects",
                "primary_key": ["id"],
                "columns": [
                    { "name": "id", "data_type": "Uuid", "nullable": false, "encrypted": false },
                    { "name": "name", "data_type": "String", "nullable": false, "encrypted": false }
                ]
            },
            {
                "name": "tasks",
                "primary_key": ["id"],
                "columns": [
                    { "name": "id", "data_type": "Uuid", "nullable": false, "encrypted": false },
                    { "name": "title", "data_type": "String", "nullable": false, "encrypted": false },
                    { "name": "done", "data_type": "Bool", "nullable": true, "encrypted": false }
                ]
            }
        ]
    }"#;

    let schema: Schema = serde_json::from_str(json).expect("failed to deserialize Schema from JSON");
    assert!(schema.has_table_by_name("projects"));
    assert!(schema.has_table_by_name("tasks"));

    let projects_id = schema.get_table_id("projects").unwrap();
    let tasks_id = schema.get_table_id("tasks").unwrap();
    assert_ne!(projects_id, tasks_id);

    let projects_table = schema.get_table_by_name("projects").unwrap();
    assert_eq!(projects_table.primary_key, vec!["id"]);
    assert_eq!(projects_table.columns.len(), 2);
}

#[test]
fn test_table_schema_add_column_evolution() {
    let mut table = TableSchema::builder("kanban")
        .primary_key("id", DataType::Uuid)
        .column("title", DataType::String)
        .build()
        .expect("valid table");

    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.column_index("title"), Some(1));

    // 1. Adding a nullable column succeeds
    let priority_col = ColumnDef::new("priority", DataType::Int).nullable(true);
    let assigned_idx = table.add_column(priority_col).expect("should add column");
    assert_eq!(assigned_idx, 2);
    assert_eq!(table.columns.len(), 3);
    assert_eq!(table.columns[2].name, "priority");
    assert_eq!(table.column_index("priority"), Some(2));
    assert_eq!(table.get_column("priority").unwrap().data_type, DataType::Int);

    // 2. Adding a duplicate column is rejected
    let dup_col = ColumnDef::new("priority", DataType::String).nullable(true);
    let dup_err = table.add_column(dup_col).unwrap_err();
    assert_eq!(
        dup_err,
        ValidationError::DuplicateColumn {
            table: "kanban".to_string(),
            column: "priority".to_string(),
        }
    );

    // 3. Adding a non-nullable column is rejected for schema evolution
    let non_null_col = ColumnDef::new("deadline", DataType::Timestamp).nullable(false);
    let non_null_err = table.add_column(non_null_col).unwrap_err();
    assert_eq!(
        non_null_err,
        ValidationError::AddedColumnMustBeNullable {
            table: "kanban".to_string(),
            column: "deadline".to_string(),
        }
    );

    // 4. Adding DataType::Null is rejected
    let null_col = ColumnDef::new("invalid", DataType::Null).nullable(true);
    let null_err = table.add_column(null_col).unwrap_err();
    assert!(matches!(null_err, ValidationError::InvalidColumnDataType { .. }));
}

#[test]
fn test_compact_into_row_supports_schema_evolution_shorter_arity() {
    let mut table = TableSchema::builder("documents")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .build()
        .expect("valid table");

    // Historical row with 2 columns
    let historical_compact = CompactRow::new(vec![Value::Int(100), Value::String("Doc 1".into())]);

    // Now evolve the schema by adding 2 nullable columns
    table
        .add_column(ColumnDef::new("tags", DataType::String).nullable(true))
        .unwrap();
    table
        .add_column(ColumnDef::new("views", DataType::Int).nullable(true))
        .unwrap();
    assert_eq!(table.columns.len(), 4);

    // from_compact_row should succeed and restore the historical columns
    let restored = table
        .from_compact_row(&historical_compact)
        .expect("should allow shorter arity");
    assert_eq!(restored.get("id"), Some(&Value::Int(100)));
    assert_eq!(restored.get("title"), Some(&Value::String("Doc 1".into())));
    assert_eq!(restored.get("tags"), None);
    assert_eq!(restored.get("views"), None);

    // compact_into_row should also succeed zero-copy
    let restored_zero_copy = table
        .compact_into_row(historical_compact)
        .expect("should allow shorter arity zero-copy");
    assert_eq!(restored_zero_copy.get("id"), Some(&Value::Int(100)));
    assert_eq!(restored_zero_copy.get("title"), Some(&Value::String("Doc 1".into())));
    assert_eq!(restored_zero_copy.get("tags"), None);
    assert_eq!(restored_zero_copy.get("views"), None);
}

#[test]
fn test_table_schema_validate_column_updates() {
    let table = TableSchema::builder("metrics")
        .primary_key("id", DataType::Int)
        .column("cpu", DataType::Float)
        .column("mem", DataType::Int)
        .nullable_column("note", DataType::String)
        .encrypted_column("token", DataType::String)
        .build()
        .unwrap();

    // 1. Valid updates sorted ascending
    let valid_updates = vec![
        ColumnUpdate::new(1, Value::Float(55.5)),
        ColumnUpdate::new(2, Value::Int(1024)),
        ColumnUpdate::new(4, Value::Bytes(vec![1, 2, 3].into_boxed_slice())),
    ];
    assert!(table.validate_column_updates(&valid_updates).is_ok());

    // 2. Empty update rejected
    assert_eq!(
        table.validate_column_updates(&[]),
        Err(ValidationError::EmptyUpdate("metrics".to_string()))
    );

    // 3. Updating primary key column rejected
    let pk_update = vec![ColumnUpdate::new(0, Value::Int(99))];
    assert_eq!(
        table.validate_column_updates(&pk_update),
        Err(ValidationError::CannotUpdatePrimaryKey {
            table: "metrics".to_string(),
            column: "id".to_string(),
        })
    );

    // 4. Out of bounds index rejected
    let oob_update = vec![ColumnUpdate::new(99, Value::Int(1))];
    assert_eq!(
        table.validate_column_updates(&oob_update),
        Err(ValidationError::UnknownColumn {
            table: "metrics".to_string(),
            column: "index 99".to_string(),
        })
    );

    // 5. Unsorted updates rejected
    let unsorted = vec![
        ColumnUpdate::new(2, Value::Int(1024)),
        ColumnUpdate::new(1, Value::Float(55.5)),
    ];
    assert_eq!(
        table.validate_column_updates(&unsorted),
        Err(ValidationError::UnsortedColumnUpdates {
            table: "metrics".to_string(),
            prev_idx: 2,
            actual_idx: 1,
        })
    );

    // 6. Duplicate column updates rejected
    let duplicate = vec![
        ColumnUpdate::new(1, Value::Float(55.5)),
        ColumnUpdate::new(1, Value::Float(60.0)),
    ];
    assert_eq!(
        table.validate_column_updates(&duplicate),
        Err(ValidationError::DuplicateColumn {
            table: "metrics".to_string(),
            column: "cpu".to_string(),
        })
    );

    // 7. Type mismatch rejected
    let type_err = vec![ColumnUpdate::new(1, Value::String("not a float".into()))];
    assert_eq!(
        table.validate_column_updates(&type_err),
        Err(ValidationError::TypeMismatch {
            table: "metrics".to_string(),
            column: "cpu".to_string(),
            expected: DataType::Float,
            actual: DataType::String,
        })
    );

    // 8. Encrypted column requires Value::Bytes
    let enc_err = vec![ColumnUpdate::new(4, Value::String("plaintext".into()))];
    assert_eq!(
        table.validate_column_updates(&enc_err),
        Err(ValidationError::EncryptedColumnMustBeBytes {
            table: "metrics".to_string(),
            column: "token".to_string(),
        })
    );
}

#[test]
fn test_table_schema_validate_operation_insert_update_delete() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();

    // 1. Valid Insert
    let valid_row = CompactRow::new(vec![
        Value::Int(10),
        Value::String("Bob".into()),
        Value::Int(25),
        Value::String("Bio text".into()),
        Value::Bytes(vec![1, 2, 3].into_boxed_slice()),
    ]);
    let insert_op = Operation::insert(table.table_id, PrimaryKey::single(10i64), valid_row, 100);
    assert!(table.validate_operation(&insert_op).is_ok());
    assert!(schema.validate_operation(&insert_op).is_ok());

    // 2. Insert with table_id mismatch
    let wrong_id_op = Operation::insert(999, PrimaryKey::single(10i64), CompactRow::new(vec![]), 100);
    assert_eq!(
        table.validate_operation(&wrong_id_op),
        Err(ValidationError::TableIdMismatch {
            table: "users".to_string(),
            expected: table.table_id,
            actual: 999,
        })
    );

    // 3. Insert with PK mismatch between row and op.pk
    let row_mismatch = CompactRow::new(vec![
        Value::Int(20), // row has 20, but op.pk has 10
        Value::String("Bob".into()),
        Value::Int(25),
        Value::Null,
        Value::Bytes(vec![1].into_boxed_slice()),
    ]);
    let pk_mismatch_op = Operation::insert(table.table_id, PrimaryKey::single(10i64), row_mismatch, 100);
    assert_eq!(
        table.validate_operation(&pk_mismatch_op),
        Err(ValidationError::PrimaryKeyMismatch {
            table: "users".to_string(),
        })
    );

    // 4. Valid Update operation
    let update_op = Operation::update(
        table.table_id,
        PrimaryKey::single(10i64),
        vec![ColumnUpdate::new(2, Value::Int(26))],
        105,
    );
    assert!(table.validate_operation(&update_op).is_ok());

    // 5. Valid Delete operation
    let delete_op = Operation::delete(table.table_id, PrimaryKey::single(10i64), 110);
    assert!(table.validate_operation(&delete_op).is_ok());

    // 6. Delete with invalid PK type
    let bad_delete_op = Operation::delete(table.table_id, PrimaryKey::single("wrong_type"), 110);
    assert!(matches!(
        table.validate_operation(&bad_delete_op),
        Err(ValidationError::PrimaryKeyTypeMismatch { .. })
    ));
}

