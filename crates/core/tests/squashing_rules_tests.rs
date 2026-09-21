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
