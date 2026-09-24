use rimdb_core::*;

fn sample_schema() -> Schema {
    let users = TableSchema::builder("users")
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .column("age", DataType::Int)
        .nullable_column("bio", DataType::String)
        .encrypted_column("secret_chat", DataType::String)
        .build()
        .expect("valid table schema");

    Schema::builder().table(users).build()
}

#[test]
fn test_squash_insert_then_update() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();
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

    let outcome = client_squash_operations(&mut base_op, incoming);
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
    let table = schema.get_table_by_name("users").unwrap();
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

    let outcome = client_squash_operations(&mut base_op, incoming);
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
    let table = schema.get_table_by_name("users").unwrap();
    let pk = PrimaryKey::single(1i64);
    let mut del_op = Operation::delete(table.table_id, pk.clone(), 100);
    let up_op = table
        .update_builder(pk.clone())
        .set("name", "Zombie Alice")
        .timestamp(105)
        .build()
        .unwrap();

    let outcome = client_squash_operations(&mut del_op, up_op);
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
    let outcome_old = client_squash_operations(&mut del_op, older_up_op);
    assert_eq!(outcome_old, SquashOutcome::Discarded);
    assert!(del_op.is_delete());
}

#[test]
fn test_squash_insert_then_delete() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();
    let row = RowBuilder::new()
        .set("id", 1i64)
        .set("name", "Alice")
        .set("age", 30i64)
        .set("secret_chat", vec![1, 2, 3])
        .build();
    let pk = PrimaryKey::single(1i64);

    let mut base_op = table.to_operation_insert(&row, 100).unwrap();
    let incoming = Operation::delete(table.table_id, pk, 200);

    let outcome = client_squash_operations(&mut base_op, incoming.clone());
    assert_eq!(outcome, SquashOutcome::Replaced);
    assert_eq!(base_op, incoming);
}

#[test]
fn test_squash_older_insert_into_newer_update_preserves_data() {
    let schema = sample_schema();
    let table = schema.get_table_by_name("users").unwrap();
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

    let outcome = client_squash_operations(&mut target_op, incoming_insert);
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
    let table = schema.get_table_by_name("users").unwrap();
    let mut buffer = TableBuffer::new(table.table_id);

    let row1 = RowBuilder::new()
        .set("id", 1i64)
        .set("name", "Alice")
        .set("age", 25i64)
        .set("secret_chat", vec![1, 2, 3])
        .build();
    let op1 = table.to_operation_insert(&row1, 100).unwrap();

    let row2 = RowBuilder::new()
        .set("id", 2i64)
        .set("name", "Bob")
        .set("age", 40i64)
        .set("secret_chat", vec![4, 5, 6])
        .build();
    let op2 = table.to_operation_insert(&row2, 100).unwrap();

    assert_eq!(buffer.apply(op1), Ok(SquashOutcome::Replaced));
    assert_eq!(buffer.apply(op2), Ok(SquashOutcome::Replaced));
    assert_eq!(buffer.len(), 2);

    // Update Alice
    let up_alice = table
        .update_builder(PrimaryKey::single(1i64))
        .set("age", 26i64)
        .timestamp(150)
        .build()
        .unwrap();
    assert_eq!(buffer.apply(up_alice), Ok(SquashOutcome::Merged));
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
    let del_bob = Operation::delete(table.table_id, PrimaryKey::single(2i64), 200);
    assert_eq!(buffer.apply(del_bob), Ok(SquashOutcome::Replaced));

    let bob_op = buffer.get(&PrimaryKey::single(2i64)).unwrap();
    assert!(bob_op.is_delete());

    // Applying UPDATE after DELETE with higher timestamp should yield BufferError::IncompatibleOperation
    let up_bob = table
        .update_builder(PrimaryKey::single(2i64))
        .set("age", 50i64)
        .timestamp(250)
        .build()
        .unwrap();
    let err = buffer.apply(up_bob).unwrap_err();
    assert_eq!(
        err,
        BufferError::IncompatibleOperation {
            table_id: table.table_id
        }
    );
}

#[test]
fn test_two_pointer_column_merge_linear() {
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

    merge_sorted_column_updates(&mut existing, incoming, true);

    let indices: Vec<u16> = existing.iter().map(|u| u.column_idx).collect();
    assert_eq!(indices, vec![0, 1, 3, 4, 5, 6]);
    assert_eq!(existing[2].value, Value::String("new".into())); // incoming won
}
