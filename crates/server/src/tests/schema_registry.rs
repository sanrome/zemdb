use super::*;
use crate::durable;
use crate::fail_point;
use tempfile::tempdir;
use zemdb_core::schema::TableSchema;
use zemdb_core::value::DataType;

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .build()
        .unwrap();
    Schema::from_tables(vec![table])
}

fn has_priority(schema: &Schema) -> bool {
    schema
        .get_table_by_name("tasks")
        .unwrap()
        .get_column("priority")
        .is_some()
}

#[test]
fn interrupted_schema_write_keeps_previous_version() {
    let dir = tempdir().unwrap();
    let id = SchemaId::new("todo").unwrap();
    let registry = SchemaRegistry::new(dir.path()).unwrap();
    registry.register_schema(id.clone(), test_schema()).unwrap();

    fail_point::arm("write_atomic_before_rename", &dir.path().join("todo.json"));
    let res = registry.add_column(
        &id,
        "tasks",
        ColumnDef::new("priority", DataType::Int).nullable(true),
    );
    assert!(res.is_err(), "the interrupted schema write must fail");
    assert!(!has_priority(&registry.get_schema(&id).unwrap()));

    let reopened = SchemaRegistry::new(dir.path()).unwrap();
    assert!(!has_priority(&reopened.get_schema(&id).unwrap()));
}

#[test]
fn stale_schema_tmp_is_ignored_and_removed() {
    let dir = tempdir().unwrap();
    let id = SchemaId::new("todo").unwrap();
    SchemaRegistry::new(dir.path())
        .unwrap()
        .register_schema(id.clone(), test_schema())
        .unwrap();
    let tmp = durable::tmp_path_for(&dir.path().join("todo.json"));
    fs::write(&tmp, b"{\"tables\": [").unwrap();

    let reopened = SchemaRegistry::new(dir.path()).unwrap();

    assert!(reopened.get_schema(&id).is_some());
    assert!(!tmp.exists());
}

#[test]
fn unreadable_schema_fails_to_open() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("todo.json"), b"{\"tables\": [").unwrap();

    assert!(matches!(
        SchemaRegistry::new(dir.path()),
        Err(ServerError::Serialization(_))
    ));
}

#[test]
fn files_without_a_valid_schema_id_name_are_skipped() {
    let dir = tempdir().unwrap();
    let id = SchemaId::new("todo").unwrap();
    SchemaRegistry::new(dir.path())
        .unwrap()
        .register_schema(id.clone(), test_schema())
        .unwrap();
    // Files a user or the OS may leave next to the schemas: macOS AppleDouble metadata, a
    // manual backup, a name with uppercase letters. None of them is a schema of this registry.
    for name in ["._todo.json", "todo.old.json", "Backup.json"] {
        fs::write(dir.path().join(name), b"not a schema").unwrap();
    }

    let reopened = SchemaRegistry::new(dir.path()).unwrap();

    assert!(reopened.get_schema(&id).is_some());
    assert_eq!(reopened.list_schemas(), vec![id]);
}
