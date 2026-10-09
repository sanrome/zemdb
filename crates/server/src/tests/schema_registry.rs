use super::*;
use crate::durable;
use crate::fail_point;
use tempfile::tempdir;
use zemdb_core::schema::TableSchema;
use zemdb_core::value::DataType;

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
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

#[test]
fn add_column_waits_for_a_concurrent_add_column_on_the_same_schema() {
    let dir = tempdir().unwrap();
    let id = SchemaId::new("todo").unwrap();
    let registry = Arc::new(SchemaRegistry::new(dir.path()).unwrap());
    registry.register_schema(id.clone(), test_schema()).unwrap();

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
            &dir.path().join("todo.json"),
            move || {
                let handle = std::thread::spawn(move || {
                    let res = registry.add_column(
                        &id,
                        "tasks",
                        ColumnDef::new("estimate", DataType::Int).nullable(true),
                    );
                    let _ = second_done_tx.send(());
                    res
                });
                let _ = second_done_rx.recv_timeout(std::time::Duration::from_millis(500));
                *second.lock().unwrap() = Some(handle);
            },
        );
    }

    registry
        .add_column(
            &id,
            "tasks",
            ColumnDef::new("priority", DataType::Int).nullable(true),
        )
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
        let tasks = schema.get_table_by_name("tasks").unwrap();
        assert!(tasks.get_column("priority").is_some());
        assert!(tasks.get_column("estimate").is_some());
    }
}

#[test]
fn concurrent_schema_writes_all_succeed_without_losing_columns() {
    const WRITERS: usize = 8;
    let dir = tempdir().unwrap();
    let id = SchemaId::new("todo").unwrap();
    let registry = SchemaRegistry::new(dir.path()).unwrap();
    registry.register_schema(id.clone(), test_schema()).unwrap();
    let barrier = std::sync::Barrier::new(WRITERS);

    std::thread::scope(|scope| {
        for i in 0..WRITERS {
            let (registry, id, barrier) = (&registry, &id, &barrier);
            scope.spawn(move || {
                barrier.wait();
                registry
                    .add_column(
                        id,
                        "tasks",
                        ColumnDef::new(format!("extra_{i}"), DataType::Int).nullable(true),
                    )
                    .expect("every concurrent add_column succeeds");
            });
        }
    });

    let reopened = SchemaRegistry::new(dir.path()).unwrap();
    for schema in [
        registry.get_schema(&id).unwrap(),
        reopened.get_schema(&id).unwrap(),
    ] {
        let tasks = schema.get_table_by_name("tasks").unwrap();
        for i in 0..WRITERS {
            assert!(
                tasks.get_column(&format!("extra_{i}")).is_some(),
                "extra_{i}"
            );
        }
    }
    assert!(!durable::tmp_path_for(&dir.path().join("todo.json")).exists());
}
