use super::*;
use crate::durable;
use crate::fail_point;
use tempfile::{tempdir, TempDir};
use zemdb_core::schema::TableSchema;
use zemdb_core::value::DataType;

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

/// A registry in a fresh directory with `test_schema` registered as `todo`.
fn todo_registry() -> (TempDir, SchemaId, SchemaRegistry) {
    let dir = tempdir().unwrap();
    let id = SchemaId::new("todo").unwrap();
    let registry = SchemaRegistry::new(dir.path()).unwrap();
    registry.register_schema(id.clone(), test_schema()).unwrap();
    (dir, id, registry)
}

fn todo_path(dir: &TempDir) -> PathBuf {
    dir.path().join("todo.json")
}

fn nullable_int(name: impl Into<String>) -> ColumnDef {
    ColumnDef::new(name, DataType::Int).nullable(true)
}

fn has_column(schema: &Schema, name: &str) -> bool {
    schema
        .get_table_by_name("tasks")
        .unwrap()
        .get_column(name)
        .is_some()
}

#[test]
fn interrupted_schema_write_keeps_previous_version() {
    let (dir, id, registry) = todo_registry();

    fail_point::arm("write_atomic_before_rename", &todo_path(&dir));
    let res = registry.add_column(&id, "tasks", nullable_int("priority"));
    assert!(res.is_err(), "the interrupted schema write must fail");
    assert!(!has_column(&registry.get_schema(&id).unwrap(), "priority"));

    let reopened = SchemaRegistry::new(dir.path()).unwrap();
    assert!(!has_column(&reopened.get_schema(&id).unwrap(), "priority"));
}

#[test]
fn schema_version_counts_every_added_column_and_survives_a_restart() {
    let (dir, id, registry) = todo_registry();
    assert_eq!(registry.get_schema(&id).unwrap().version(), 1);
    registry
        .add_column(&id, "tasks", nullable_int("priority"))
        .unwrap();
    assert!(registry
        .add_column(&id, "tasks", nullable_int("priority"))
        .is_err());
    registry
        .add_column(&id, "tasks", nullable_int("estimate"))
        .unwrap();
    assert_eq!(registry.get_schema(&id).unwrap().version(), 3);

    let reopened = SchemaRegistry::new(dir.path()).unwrap();
    assert_eq!(reopened.get_schema(&id).unwrap().version(), 3);
}

#[test]
fn new_schema_must_start_at_version_one() {
    let (_dir, _, registry) = todo_registry();
    let mut evolved = test_schema();
    evolved
        .add_column("tasks", nullable_int("priority"))
        .unwrap();
    let id = SchemaId::new("evolved").unwrap();
    let res = registry.register_schema(id.clone(), evolved);
    assert!(matches!(res, Err(ServerError::BadRequest(_))), "{res:?}");
    assert!(registry.get_schema(&id).is_none());
}

#[test]
fn stale_schema_tmp_is_ignored_and_removed() {
    let (dir, id, _) = todo_registry();
    let tmp = durable::tmp_path_for(&todo_path(&dir));
    fs::write(&tmp, b"{\"tables\": [").unwrap();

    let reopened = SchemaRegistry::new(dir.path()).unwrap();

    assert!(reopened.get_schema(&id).is_some());
    assert!(!tmp.exists());
}

#[test]
fn unreadable_schema_fails_to_open() {
    let dir = tempdir().unwrap();
    fs::write(todo_path(&dir), b"{\"tables\": [").unwrap();

    assert!(matches!(
        SchemaRegistry::new(dir.path()),
        Err(ServerError::Serialization(_))
    ));
}

#[test]
fn files_without_a_valid_schema_id_name_are_skipped() {
    let (dir, id, _) = todo_registry();
    // Files a user or the OS may leave next to the schemas: macOS AppleDouble metadata, a
    // manual backup, a name with uppercase letters. None of them is a schema of this registry.
    for name in ["._todo.json", "todo.old.json", "Backup.json"] {
        fs::write(dir.path().join(name), b"not a schema").unwrap();
    }

    let reopened = SchemaRegistry::new(dir.path()).unwrap();

    assert!(reopened.get_schema(&id).is_some());
    assert_eq!(reopened.list_schemas(), vec![id]);
}

#[test]
fn add_column_waits_for_a_concurrent_add_column_on_the_same_schema() {
    let (dir, id, registry) = todo_registry();
    let registry = Arc::new(registry);

    // While the first writer is between reading the schema and writing it back, a second
    // writer adds another column. If it got in, the first writer would overwrite its column
    // with the copy read before it. Writers are serialized, so the second one cannot finish
    // here: the wait below ends by timeout, the first writer completes, then the second.
    let (second_done_tx, second_done_rx) = std::sync::mpsc::channel();
    let second = Arc::new(std::sync::Mutex::new(None));
    {
        let registry = Arc::clone(&registry);
        let id = id.clone();
        let second = Arc::clone(&second);
        fail_point::arm_hook(
            "schema_add_column_after_read",
            &todo_path(&dir),
            move || {
                let handle = std::thread::spawn(move || {
                    let res = registry.add_column(&id, "tasks", nullable_int("estimate"));
                    let _ = second_done_tx.send(());
                    res
                });
                let _ = second_done_rx.recv_timeout(std::time::Duration::from_millis(500));
                *second.lock().unwrap() = Some(handle);
            },
        );
    }

    registry
        .add_column(&id, "tasks", nullable_int("priority"))
        .unwrap();
    let handle = second.lock().unwrap().take().expect("the hook ran");
    handle.join().unwrap().unwrap();

    for schema in [
        registry.get_schema(&id).unwrap(),
        SchemaRegistry::new(dir.path())
            .unwrap()
            .get_schema(&id)
            .unwrap(),
    ] {
        assert!(has_column(&schema, "priority"));
        assert!(has_column(&schema, "estimate"));
        assert_eq!(schema.version(), 3);
    }
}

#[test]
fn concurrent_schema_writes_all_succeed_without_losing_columns() {
    const WRITERS: usize = 8;
    let (dir, id, registry) = todo_registry();
    let barrier = std::sync::Barrier::new(WRITERS);

    std::thread::scope(|scope| {
        for i in 0..WRITERS {
            let (registry, id, barrier) = (&registry, &id, &barrier);
            scope.spawn(move || {
                barrier.wait();
                registry
                    .add_column(id, "tasks", nullable_int(format!("extra_{i}")))
                    .expect("every concurrent add_column succeeds");
            });
        }
    });

    let reopened = SchemaRegistry::new(dir.path()).unwrap();
    for schema in [
        registry.get_schema(&id).unwrap(),
        reopened.get_schema(&id).unwrap(),
    ] {
        for i in 0..WRITERS {
            assert!(has_column(&schema, &format!("extra_{i}")), "extra_{i}");
        }
    }
    assert!(!durable::tmp_path_for(&todo_path(&dir)).exists());
}
