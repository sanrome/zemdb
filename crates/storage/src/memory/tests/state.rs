use super::*;
use crate::memory::index::tests::oracle::{self, CHILDREN, PARENTS};
use crate::memory::index::tests::{assert_indexes_follow_tables, assert_same_contents, Rng};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use zemdb_core::{ColumnUpdate, DataType, TableSchema};

/// The snapshot payload as written while tables were `Arc<BTreeMap>`.
#[derive(Serialize)]
struct BTreeSnapshotPayload {
    head_seq: SequenceNumber,
    tables: HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
}

fn rows() -> Vec<(PrimaryKey, CompactRow)> {
    (0..200i64)
        .rev()
        .map(|key| {
            let row = CompactRow::new(vec![
                Value::Int(key),
                Value::String(format!("{key}").into()),
            ]);
            (PrimaryKey::single(key), row)
        })
        .collect()
}

/// Serialized with a `BTreeMap` table, as before tables became persistent maps.
fn btree_snapshot_bytes() -> Vec<u8> {
    let table: BTreeMap<_, _> = rows().into_iter().collect();
    bincode::serialize(&BTreeSnapshotPayload {
        head_seq: SequenceNumber::from(200u64),
        tables: HashMap::from([(0u16, Arc::new(table))]),
    })
    .unwrap()
}

#[test]
fn snapshot_bytes_match_the_btree_encoding() {
    let tables = HashMap::from([(0u16, rows().into_iter().collect::<Table>())]);
    let bytes = bincode::serialize(&RoomSnapshotRef {
        head_seq: SequenceNumber::from(200u64),
        tables: &tables,
    })
    .unwrap();

    assert_eq!(bytes, btree_snapshot_bytes());
}

#[test]
fn snapshot_written_with_btree_tables_decodes() {
    let payload: RoomSnapshotPayload = bincode::deserialize(&btree_snapshot_bytes()).unwrap();

    assert_eq!(payload.head_seq, SequenceNumber::from(200u64));
    let expected: Table = rows().into_iter().collect();
    assert_eq!(payload.tables[&0], expected);
}

#[test]
fn batch_past_the_last_sequence_number_is_a_mismatch() {
    let room_id = RoomId::new("room").unwrap();
    let schema = Schema::builder()
        .table(TableSchema::builder("t").primary_key("id", DataType::Int))
        .build();
    let op = |seq: u64| {
        let row = CompactRow::new(vec![Value::Int(1)]);
        SequencedOperation::new(seq, Operation::insert(0, PrimaryKey::single(1i64), row, 0))
    };
    for (head, seqs) in [
        (u64::MAX, vec![u64::MAX]),
        (u64::MAX - 1, vec![u64::MAX, u64::MAX]),
    ] {
        let ops: Vec<_> = seqs.into_iter().map(op).collect();
        let result = validate_batch(&room_id, &schema, SequenceNumber::from(head), &ops);
        assert!(
            matches!(result, Err(StorageError::SequenceMismatch { .. })),
            "{result:?}"
        );
    }
}

/// Rows of `table`, by key, from rows whose first values are their key.
fn table_of(rows: Vec<Vec<Value>>, key_width: usize) -> Table {
    rows.into_iter()
        .map(|row| {
            let pk = PrimaryKey::composite(row[..key_width].to_vec());
            (pk, CompactRow::new(row))
        })
        .collect()
}

/// An update checks the constraints with a column among its deltas, and only those. In a state
/// that already breaks a constraint (which the rules never produce), an update of other columns
/// is applied, while an insert checks every constraint of its row.
#[test]
fn an_update_checks_only_the_constraints_of_the_columns_it_sets() {
    let schema = oracle::schema();
    let parents = table_of(
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("a".into())],
        ],
        1,
    );
    let children = table_of(vec![vec![Value::Int(1), Value::Int(1), Value::Int(9)]], 2);
    let mut data = RoomData::build(
        &schema,
        HashMap::from([(PARENTS, parents), (CHILDREN, children)]),
    );
    let parent = PrimaryKey::single(1i64);
    let child = PrimaryKey::composite([Value::Int(1), Value::Int(1)]);
    let update = |table, pk: &PrimaryKey, column, value| {
        Operation::update(table, pk.clone(), vec![ColumnUpdate::new(column, value)], 0)
    };
    let cases = [
        (update(PARENTS, &parent, 2, Value::Float(1.5)), None),
        (
            update(PARENTS, &parent, 1, Value::String("a".into())),
            Some(RejectReason::Unique {
                constraint: "parents_code".into(),
            }),
        ),
        (
            Operation::insert(
                PARENTS,
                parent.clone(),
                CompactRow::new(vec![Value::Int(1), Value::String("a".into())]),
                0,
            ),
            Some(RejectReason::Unique {
                constraint: "parents_code".into(),
            }),
        ),
        (update(CHILDREN, &child, 5, Value::Int(3)), None),
        (
            update(CHILDREN, &child, 2, Value::Int(9)),
            Some(RejectReason::ForeignKey {
                constraint: "children_parent".into(),
            }),
        ),
    ];
    for (op, expected) in cases {
        schema.validate_operation(&op).unwrap();
        let described = format!("{op:?}");
        assert_eq!(
            apply_operation(&mut data, &schema, op),
            expected,
            "{described}"
        );
        assert_indexes_follow_tables(&schema, &data);
    }
}

/// Random operations, many of which break a rule, applied to random states, many of which
/// already break one (built from random rows without checking them), never make the verifier
/// panic, and a rejected operation changes neither the rows nor the indexes.
#[test]
fn the_verifier_never_panics_and_a_rejection_changes_nothing() {
    let mut broken_rules = 0;
    for seed in 0..40 {
        let mut rng = Rng::new(seed);
        let schema = if seed % 2 == 0 {
            oracle::schema()
        } else {
            oracle::evolved_schema()
        };
        let mut tables: HashMap<u16, Table> = HashMap::new();
        for _ in 0..rng.below(40) {
            let op = oracle::random_operation(&mut rng, &schema);
            let (pk, kind) = (op.pk().clone(), op.kind().clone());
            if let OperationKind::Insert { row } = kind {
                tables.entry(op.table_id()).or_default().insert(pk, row);
            }
        }
        let mut data = RoomData::build(&schema, tables);
        for _ in 0..300 {
            let op = oracle::random_operation(&mut rng, &schema);
            schema.validate_operation(&op).unwrap();
            let before = data.clone();
            if let Some(reason) = apply_operation(&mut data, &schema, op) {
                assert_same_contents(&before, &data);
                broken_rules += usize::from(reason != RejectReason::MissingRow);
            }
        }
        assert_indexes_follow_tables(&schema, &data);
    }
    assert!(broken_rules > 0, "no operation broke a rule");
}

/// The replay of a WAL applies its records without validating them again (the engine validated
/// them before writing them), so the verifier must not panic even on an operation the schema
/// rejects: keys of another width or type, rows of any width, deltas of columns the table does
/// not have, and owners of another type or missing, on tables with every kind of rule.
#[test]
fn the_verifier_never_panics_on_operations_the_schema_rejects() {
    let schema = oracle::evolved_schema();
    let mut rng = Rng::new(3);
    let mut tables: HashMap<u16, Table> = HashMap::new();
    for _ in 0..200 {
        let op = oracle::random_operation(&mut rng, &schema);
        if let OperationKind::Insert { row } = op.kind().clone() {
            tables
                .entry(op.table_id())
                .or_default()
                .insert(op.pk().clone(), row);
        }
    }
    let mut data = RoomData::build(&schema, tables);
    let keys = [
        PrimaryKey::composite(Vec::<Value>::new()),
        PrimaryKey::single(1i64),
        PrimaryKey::single("A"),
        PrimaryKey::composite([Value::Int(1), Value::Int(1)]),
        PrimaryKey::composite([Value::String("A".into()), Value::Int(1)]),
        PrimaryKey::composite([Value::Int(1), Value::Null, Value::Float(f64::NAN)]),
    ];
    let rows = [
        CompactRow::new(Vec::new()),
        CompactRow::new(vec![Value::Int(1)]),
        CompactRow::new(vec![Value::String("A".into()), Value::Int(1)]),
        CompactRow::new(vec![Value::Null; 12]),
        CompactRow::new(vec![Value::Int(1); 3]),
    ];
    let values = [
        Value::Null,
        Value::Int(1),
        Value::String("A".into()),
        Value::Float(-0.0),
    ];
    for table_id in schema.table_ids().chain([99]) {
        for pk in &keys {
            for row in &rows {
                let op = Operation::insert(table_id, pk.clone(), row.clone(), 0);
                apply_operation(&mut data, &schema, op);
            }
            for column in [0u16, 1, 2, 4, 6, 50, u16::MAX] {
                for value in &values {
                    let update = vec![ColumnUpdate::new(column, value.clone())];
                    let op = Operation::update(table_id, pk.clone(), update, 0);
                    apply_operation(&mut data, &schema, op);
                }
            }
            apply_operation(
                &mut data,
                &schema,
                Operation::delete(table_id, pk.clone(), 0),
            );
            for value in &values {
                let op = Operation::delete_with_owner(table_id, pk.clone(), value.clone(), 0);
                apply_operation(&mut data, &schema, op);
            }
        }
    }
}
