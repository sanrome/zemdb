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
