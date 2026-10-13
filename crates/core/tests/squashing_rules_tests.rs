use zemdb_core::*;

fn users() -> TableSchema {
    TableSchema::builder("users")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .column("age", DataType::Int)
        .nullable_column("bio", DataType::String)
        .encrypted_column("secret_chat", DataType::String)
        .build()
        .unwrap()
}

fn insert(table: &TableSchema, id: i64, name: &str, timestamp: u64) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("name", name)
        .set("age", 30i64)
        .set("secret_chat", vec![1, 2, 3])
        .build();
    table.to_operation_insert(&row, timestamp).unwrap()
}

fn update(table: &TableSchema, id: i64, column: &str, value: Value, timestamp: u64) -> Operation {
    table
        .update_builder(PrimaryKey::single(id))
        .set(column, value)
        .timestamp(timestamp)
        .build()
        .unwrap()
}

fn delete(table: &TableSchema, id: i64, timestamp: u64) -> Operation {
    Operation::delete(table.table_id(), PrimaryKey::single(id), timestamp)
}

/// The value of `column` in the pending insert or update `op`.
fn value_of(table: &TableSchema, op: &Operation, column: &str) -> Option<Value> {
    match op.kind() {
        OperationKind::Insert { row } => table.from_compact_row(row).unwrap().get(column).cloned(),
        OperationKind::Update { updates } => table
            .expand_update_fields(updates)
            .unwrap()
            .get(column)
            .cloned(),
        OperationKind::Delete => None,
    }
}

#[test]
fn insert_then_update_applies_the_deltas_to_the_row() {
    let mut table = users();
    let mut op = insert(&table, 10, "Alice", 50);
    // A column added after the insert: the row is widened to reach it.
    table
        .add_column(ColumnDef::new("nick", DataType::String).nullable(true))
        .unwrap();
    let table = &table;

    // Stamped before the insert, the updates still apply: arrival order decides.
    squash_operations(
        &mut op,
        update(table, 10, "nick", Value::String("Al".into()), 40),
    );
    let outcome = squash_operations(&mut op, update(table, 10, "age", Value::Int(26), 30));

    assert_eq!(outcome, SquashOutcome::Merged);
    assert_eq!(
        value_of(table, &op, "nick"),
        Some(Value::String("Al".into()))
    );
    assert_eq!(value_of(table, &op, "age"), Some(Value::Int(26)));
    assert_eq!(
        value_of(table, &op, "name"),
        Some(Value::String("Alice".into()))
    );
    assert_eq!(op.timestamp(), 30);
}

#[test]
fn update_then_update_merges_the_fields() {
    let table = &users();
    let mut op = update(table, 10, "name", Value::String("Alice".into()), 100);

    let outcome = squash_operations(&mut op, update(table, 10, "age", Value::Int(30), 105));

    assert_eq!(outcome, SquashOutcome::Merged);
    assert_eq!(
        value_of(table, &op, "name"),
        Some(Value::String("Alice".into()))
    );
    assert_eq!(value_of(table, &op, "age"), Some(Value::Int(30)));
}

#[test]
fn update_after_a_pending_delete_and_another_table_are_rejected() {
    let table = &users();
    let mut buffer = TableBuffer::new(table.table_id());
    buffer.apply(table, delete(table, 1, 100)).unwrap();
    let before = buffer.clone();

    for timestamp in [90, 105] {
        let zombie = update(table, 1, "name", Value::String("Zombie".into()), timestamp);
        let incompatible = BufferError::IncompatibleOperation { table_id: 0 };
        assert_eq!(buffer.apply(table, zombie), Err(incompatible));
    }
    let other = TableSchema::builder("other")
        .table_id(1)
        .primary_key("id", DataType::Int)
        .build()
        .unwrap();
    let result = buffer.apply(&other, Operation::delete(1, PrimaryKey::single(2i64), 1));
    assert!(matches!(
        result,
        Err(BufferError::Invalid(
            ValidationError::TableIdMismatch { .. }
        ))
    ));
    assert_eq!(buffer, before);
}

#[test]
fn delete_after_a_pending_insert_is_kept() {
    let table = &users();
    let mut buffer = TableBuffer::new(table.table_id());

    // Inserted offline, then deleted: the row may exist on the server, so the delete is sent.
    buffer.apply(table, insert(table, 1, "Alice", 100)).unwrap();
    buffer.apply(table, delete(table, 1, 200)).unwrap();
    // Deleted, inserted again and deleted again: still a delete.
    buffer.apply(table, delete(table, 2, 300)).unwrap();
    buffer.apply(table, insert(table, 2, "Bob", 400)).unwrap();
    buffer.apply(table, delete(table, 2, 500)).unwrap();

    for id in [1, 2] {
        assert!(buffer.get(&PrimaryKey::single(id)).unwrap().is_delete());
    }
}

#[test]
fn arrival_order_wins_over_timestamps() {
    let table = &users();
    let mut buffer = TableBuffer::new(table.table_id());
    let name = |value: &str| Value::String(value.into());
    let pending = |buffer: &TableBuffer, id| buffer.get(&PrimaryKey::single(id)).unwrap().clone();

    // A delete stamped before the insert (the clock moved back) still deletes.
    buffer
        .apply(table, insert(table, 1, "Alice", 1000))
        .unwrap();
    buffer.apply(table, delete(table, 1, 990)).unwrap();
    assert!(pending(&buffer, 1).is_delete());

    // The last update of a column wins, column by column, whatever the timestamps.
    let updates = [
        ("age", Value::Int(1), 100),
        ("name", name("old"), 90),
        ("name", name("new"), 95),
        ("bio", name("first"), 200),
        ("bio", name("second"), 80),
    ];
    for (column, value, timestamp) in updates {
        buffer
            .apply(table, update(table, 2, column, value, timestamp))
            .unwrap();
    }
    let merged = pending(&buffer, 2);
    assert_eq!(value_of(table, &merged, "age"), Some(Value::Int(1)));
    assert_eq!(value_of(table, &merged, "name"), Some(name("new")));
    assert_eq!(value_of(table, &merged, "bio"), Some(name("second")));
    assert_eq!(merged.timestamp(), 80);

    // An insert replaces a pending update, even if stamped before it.
    buffer.apply(table, insert(table, 2, "Bob", 50)).unwrap();
    assert_eq!(
        value_of(table, &pending(&buffer, 2), "name"),
        Some(name("Bob"))
    );
}

#[test]
fn invalid_operations_are_rejected_and_leave_the_buffer_unchanged() {
    let table = &users();
    let mut buffer = TableBuffer::new(table.table_id());
    buffer.apply(table, insert(table, 1, "Alice", 100)).unwrap();
    let before = buffer.clone();
    let mut rejects = |op: Operation| {
        let err = buffer.apply(table, op).unwrap_err();
        assert_eq!(buffer, before);
        match err {
            BufferError::Invalid(err) => err,
            other => panic!("{other}"),
        }
    };

    let (id, pk) = (table.table_id(), PrimaryKey::single(1i64));
    let repeated = vec![ColumnUpdate::new(1, "a"), ColumnUpdate::new(1, "b")];
    let out_of_range = vec![ColumnUpdate::new(99, 1i64)];
    let duplicate = rejects(Operation::update(id, pk.clone(), repeated, 200));
    assert!(matches!(duplicate, ValidationError::DuplicateColumn { .. }));
    let unknown = rejects(Operation::update(id, pk, out_of_range, 200));
    assert!(matches!(unknown, ValidationError::UnknownColumn { .. }));
    let unsorted = rejects(decode_operation_with_update(id, &[2, 1]));
    assert!(matches!(
        unsorted,
        ValidationError::UnsortedColumnUpdates { .. }
    ));
}

#[test]
fn update_builder_keeps_the_last_value_of_a_column() {
    let pk = PrimaryKey::single(1i64);
    let op = UpdateBuilder::new(0, pk.clone())
        .set(2, "a")
        .set(1, 7i64)
        .set(2, "b")
        .build();
    let expected = vec![ColumnUpdate::new(1, 7i64), ColumnUpdate::new(2, "b")];
    assert_eq!(op, Operation::update(0, pk, expected, 0));
}

#[test]
fn every_buffer_built_by_apply_deserializes_again() {
    let table = &users();
    let ops = [
        insert(table, 1, "Alice", 1),
        update(table, 1, "name", Value::String("Bob".into()), 2),
        update(table, 1, "age", Value::Int(40), 3),
        update(table, 1, "bio", Value::Null, 4),
        delete(table, 2, 5),
        // Repeats a column: `apply` must not accept it.
        Operation::update(
            table.table_id(),
            PrimaryKey::single(1i64),
            vec![ColumnUpdate::new(1, "a"), ColumnUpdate::new(1, "b")],
            6,
        ),
    ];
    // Every sequence of three operations, accepted or not.
    for sequence in 0..ops.len().pow(3) {
        let mut buffer = TableBuffer::new(table.table_id());
        for position in 0..3 {
            let op = ops[sequence / ops.len().pow(position) % ops.len()].clone();
            let _ = buffer.apply(table, op);
        }
        let decoded: TableBuffer =
            bincode::deserialize(&bincode::serialize(&buffer).unwrap()).unwrap();
        assert_eq!(decoded, buffer);
    }
}

#[test]
fn merged_column_updates_stay_sorted_and_incoming_values_win() {
    let mut existing = vec![
        ColumnUpdate::new(1, Value::Int(10)),
        ColumnUpdate::new(3, Value::String("old".into())),
        ColumnUpdate::new(5, Value::Bool(false)),
    ];
    let incoming = vec![
        ColumnUpdate::new(0, Value::Int(100)),
        ColumnUpdate::new(3, Value::String("new".into())),
        ColumnUpdate::new(4, Value::Int(400)),
        ColumnUpdate::new(6, Value::Bool(true)),
    ];

    merge_sorted_column_updates(&mut existing, incoming);

    let indices: Vec<u16> = existing.iter().map(|u| u.column_idx()).collect();
    assert_eq!(indices, vec![0, 1, 3, 4, 5, 6]);
    assert_eq!(existing[2].value(), &Value::String("new".into())); // incoming won
}

/// Bincode layout of a `TableBuffer`: the table id followed by the list of pending
/// operations, each stored under its own primary key once decoded.
#[derive(serde::Serialize)]
struct RawTableBuffer {
    table_id: u16,
    pending: Vec<Operation>,
}

fn decode_buffer(raw: &RawTableBuffer) -> Result<TableBuffer, bincode::Error> {
    bincode::deserialize(&bincode::serialize(raw).unwrap())
}

/// Bincode layout of an `Operation`, which the constructors cannot produce with unsorted
/// update deltas but a serialized buffer can contain.
#[derive(serde::Serialize)]
struct RawOperation {
    table_id: u16,
    timestamp: u64,
    pk: PrimaryKey,
    kind: OperationKind,
}

/// An update of row 1 of `table_id` with integer deltas at `column_indices`, in that order.
fn decode_operation_with_update(table_id: u16, column_indices: &[u16]) -> Operation {
    let updates = column_indices
        .iter()
        .map(|idx| ColumnUpdate::new(*idx, Value::Int(1)))
        .collect();
    let raw = RawOperation {
        table_id,
        timestamp: 10,
        pk: PrimaryKey::single(1i64),
        kind: OperationKind::Update { updates },
    };
    bincode::deserialize(&bincode::serialize(&raw).unwrap()).unwrap()
}

fn buffer_with_update(column_indices: &[u16]) -> RawTableBuffer {
    RawTableBuffer {
        table_id: 1,
        pending: vec![decode_operation_with_update(1, column_indices)],
    }
}

#[test]
fn table_buffer_deserialization_rejects_inconsistent_buffers() {
    assert!(decode_buffer(&buffer_with_update(&[1, 3])).is_ok());

    let pk = PrimaryKey::single(1i64);
    let invalid = [
        // An operation of another table.
        RawTableBuffer {
            table_id: 1,
            pending: vec![Operation::delete(2, pk.clone(), 10)],
        },
        // The same primary key twice.
        RawTableBuffer {
            table_id: 1,
            pending: vec![
                Operation::delete(1, pk.clone(), 10),
                Operation::delete(1, pk, 20),
            ],
        },
        // Squashing merges deltas assuming strictly ascending column indices.
        buffer_with_update(&[3, 1]),
        buffer_with_update(&[2, 2]),
    ];
    for (case, raw) in invalid.iter().enumerate() {
        assert!(decode_buffer(raw).is_err(), "case {case}");
    }
}
