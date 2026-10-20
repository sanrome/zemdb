//! Tests of the constraint indexes, and the helpers that the tests of both engines share to check
//! that the indexes of a room are always those its rows give: a schema with constraints, a random
//! log with the report each of its batches must give, and the scenarios run on either engine.

use super::*;
use crate::engine::{ApplyReport, RejectReason};
use crate::memory::state::RoomData;
use crate::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use zemdb_core::{
    ColumnDef, ColumnUpdate, DataType, ForeignKey, Operation, OperationKind, RoomId, Schema,
    SequenceNumber, SequencedOperation, UniqueConstraint, Value,
};

/// The deterministic generator of the randomized tests, shared with the integration tests.
#[path = "../../../tests/common/rng.rs"]
mod rng;
pub(crate) use rng::Rng;

pub(crate) const PARENTS: u16 = 0;
pub(crate) const CHILDREN: u16 = 1;
pub(crate) const PLAIN: u16 = 2;

/// Three tables:
/// - `parents`: an integer key, a code and a score, with `UNIQUE (code)` and
///   `UNIQUE (score, code)`;
/// - `children`: a composite integer key `(a, b)`, a parent, the key of another child
///   (`up_a`, `up_b`) and a number, with a foreign key to `parents`, one from `(up_b, up_a)` to
///   `children` itself and `UNIQUE (x)`;
/// - `plain`: an integer key and a number, without constraints.
///
/// Every column but the keys is nullable, so rows may be short. The composite constraints name
/// their columns in another order than the table's, so the keys must follow the declaration.
pub(crate) fn constrained_schema() -> Schema {
    let parents = TableSchema::builder("parents")
        .table_id(PARENTS)
        .primary_key("id", DataType::Int)
        .nullable_column("code", DataType::String)
        .nullable_column("score", DataType::Float)
        .unique("parents_code", ["code"])
        .unique("parents_score_code", ["score", "code"]);
    let children = TableSchema::builder("children")
        .table_id(CHILDREN)
        .primary_key("a", DataType::Int)
        .primary_key("b", DataType::Int)
        .nullable_column("parent", DataType::Int)
        .nullable_column("up_a", DataType::Int)
        .nullable_column("up_b", DataType::Int)
        .nullable_column("x", DataType::Int)
        .foreign_key("children_parent", ["parent"], "parents")
        .foreign_key("children_up", ["up_b", "up_a"], "children")
        .unique("children_x", ["x"]);
    let plain = TableSchema::builder("plain")
        .table_id(PLAIN)
        .primary_key("id", DataType::Int)
        .nullable_column("v", DataType::Int);
    Schema::builder()
        .table(parents)
        .table(children)
        .table(plain)
        .build()
}

/// [`constrained_schema`] with a column `tag` added to `parents` together with `UNIQUE (tag)`,
/// so that the rows written before it are short.
pub(crate) fn evolved_schema() -> Schema {
    let mut schema = constrained_schema();
    let tag = ColumnDef::new("tag", DataType::String).nullable(true);
    let unique = vec![UniqueConstraint::new("parents_tag", ["tag"])];
    schema
        .add_column_with_constraints("parents", tag, unique, Vec::new())
        .unwrap();
    schema
}

/// A value for `column`: `NULL` a quarter of the time if it is nullable, else one of a few
/// integers or strings, or a float whose bits matter (both zeros and two NaNs).
fn random_value(rng: &mut Rng, column: &ColumnDef) -> Value {
    if column.is_nullable() && rng.chance(25) {
        return Value::Null;
    }
    match column.data_type() {
        DataType::Int => Value::Int(rng.below(4) as i64),
        DataType::String => Value::String(format!("s{}", rng.below(3)).into()),
        DataType::Float => Value::Float(match rng.below(5) {
            0 => 0.0,
            1 => -0.0,
            2 => f64::NAN,
            3 => f64::from_bits(f64::NAN.to_bits() | 1),
            _ => 1.5,
        }),
        other => panic!("no random values of type {other:?}"),
    }
}

/// An insert (of a row that may be short), an update of one or two columns, or a delete, of one
/// of a few keys of a random table of `schema`.
pub(crate) fn random_operation(rng: &mut Rng, schema: &Schema) -> Operation {
    let tables: Vec<&TableSchema> = schema.tables().collect();
    let table = tables[rng.below(tables.len() as u64) as usize];
    let columns = table.columns();
    let key_columns: Vec<usize> = table
        .primary_key()
        .iter()
        .map(|name| table.column_index(name).unwrap())
        .collect();
    let key: Vec<Value> = key_columns
        .iter()
        .map(|_| Value::Int(rng.below(4) as i64))
        .collect();
    let pk = PrimaryKey::composite(key.clone());
    let others: Vec<usize> = (0..columns.len())
        .filter(|i| !key_columns.contains(i))
        .collect();
    match rng.below(10) {
        0..=4 => {
            let min_width = key_columns.iter().max().unwrap() + 1;
            let width = min_width + rng.below((columns.len() - min_width + 1) as u64) as usize;
            let values = (0..width)
                .map(|i| match key_columns.iter().position(|&k| k == i) {
                    Some(k) => key[k].clone(),
                    None => random_value(rng, &columns[i]),
                })
                .collect();
            Operation::insert(table.table_id(), pk, CompactRow::new(values), 0)
        }
        5..=7 => {
            let mut chosen = BTreeSet::new();
            for _ in 0..=rng.below(2) {
                chosen.insert(others[rng.below(others.len() as u64) as usize]);
            }
            let updates = chosen
                .into_iter()
                .map(|i| ColumnUpdate::new(i as u16, random_value(rng, &columns[i])))
                .collect();
            Operation::update(table.table_id(), pk, updates, 0)
        }
        _ => Operation::delete(table.table_id(), pk, 0),
    }
}

/// A random log of batches over a schema, with the report each one must give. It keeps which
/// rows exist: an update of a row that does not is the only operation rejected.
pub(crate) struct RandomLog {
    rng: Rng,
    schema: Schema,
    head: u64,
    rows: BTreeSet<(u16, PrimaryKey)>,
}

impl RandomLog {
    pub(crate) fn new(seed: u64, schema: Schema) -> Self {
        Self {
            rng: Rng::new(seed),
            schema,
            head: 0,
            rows: BTreeSet::new(),
        }
    }

    /// Writes the next batches under `schema`, which must be a later version of the current one.
    pub(crate) fn evolve(&mut self, schema: Schema) {
        self.schema = schema;
    }

    pub(crate) fn head(&self) -> u64 {
        self.head
    }

    /// The next batch, of one to eight operations, and the report it must give.
    pub(crate) fn next_batch(&mut self) -> (Vec<SequencedOperation>, ApplyReport) {
        let mut ops = Vec::new();
        let mut rejected = Vec::new();
        for _ in 0..=self.rng.below(8) {
            self.head += 1;
            let op = random_operation(&mut self.rng, &self.schema);
            let row = (op.table_id(), op.pk().clone());
            match op.kind() {
                OperationKind::Insert { .. } => {
                    self.rows.insert(row);
                }
                OperationKind::Update { .. } if !self.rows.contains(&row) => {
                    rejected.push((SequenceNumber::from(self.head), RejectReason::MissingRow));
                }
                OperationKind::Update { .. } => {}
                OperationKind::Delete { .. } => {
                    self.rows.remove(&row);
                }
            }
            ops.push(SequencedOperation::new(self.head, op));
        }
        (
            ops,
            ApplyReport::new(SequenceNumber::from(self.head), rejected),
        )
    }
}

/// The schema and the contents of an open room, read from an engine's internal state.
pub(crate) trait RoomContents: StorageEngine {
    async fn contents(&self, room: &RoomId) -> (Schema, RoomData);
}

impl RoomContents for MemoryStorageEngine {
    async fn contents(&self, room: &RoomId) -> (Schema, RoomData) {
        let room = self.get_room(room).unwrap();
        let state = room.read().unwrap();
        (state.schema.clone(), state.data.clone())
    }
}

impl RoomContents for DiskStorageEngine {
    async fn contents(&self, room: &RoomId) -> (Schema, RoomData) {
        let room = self.get_room(room).await.unwrap();
        let state = room.state.read().await;
        (state.schema.clone(), state.data.clone())
    }
}

/// Every entry of a room's indexes, by table, kind of constraint and position of the constraint
/// among those of its kind.
type Entries = BTreeMap<(u16, &'static str, usize), BTreeSet<(PrimaryKey, PrimaryKey)>>;

fn entries(indexes: &HashMap<u16, TableIndexes>) -> Entries {
    let mut entries = Entries::new();
    for (&table_id, table) in indexes {
        for (kind, list) in [
            ("unique", &table.unique),
            ("foreign key", &table.foreign_keys),
        ] {
            for (position, index) in list.iter().enumerate() {
                entries.insert((table_id, kind, position), index.iter().cloned().collect());
            }
        }
    }
    entries
}

/// The entries that the indexes of `tables` must hold under `schema`, computed from the rows
/// alone: for each constraint, with its columns looked up by name, one per row whose values in
/// them are all present and not `NULL`.
fn expected_entries(schema: &Schema, tables: &HashMap<u16, Table>) -> Entries {
    let mut entries = Entries::new();
    for table in schema.tables() {
        let unique = table
            .unique_constraints()
            .iter()
            .map(UniqueConstraint::columns);
        let foreign = table.foreign_keys().iter().map(ForeignKey::columns);
        let kinds: [(&str, Vec<&[String]>); 2] = [
            ("unique", unique.collect()),
            ("foreign key", foreign.collect()),
        ];
        for (kind, constraints) in kinds {
            for (position, columns) in constraints.into_iter().enumerate() {
                let positions: Vec<usize> = columns
                    .iter()
                    .map(|name| table.column_index(name).unwrap())
                    .collect();
                let rows = tables.get(&table.table_id()).into_iter().flatten();
                let index = rows
                    .filter_map(|(pk, row)| {
                        let key: Option<Vec<Value>> = positions
                            .iter()
                            .map(|&p| row.get(p).filter(|v| !v.is_null()).cloned())
                            .collect();
                        key.map(|key| (PrimaryKey::composite(key), pk.clone()))
                    })
                    .collect();
                entries.insert((table.table_id(), kind, position), index);
            }
        }
    }
    entries
}

/// Checks that the indexes of `data` are exactly those its rows give under `schema`.
pub(crate) fn assert_indexes_follow_tables(schema: &Schema, data: &RoomData) {
    assert_eq!(
        entries(data.indexes()),
        expected_entries(schema, data.tables()),
        "the indexes do not follow the rows"
    );
}

/// Checks that `a` and `b` hold the same rows and the same indexes.
pub(crate) fn assert_same_contents(a: &RoomData, b: &RoomData) {
    assert_eq!(a.tables(), b.tables());
    assert_eq!(entries(a.indexes()), entries(b.indexes()));
}

/// Checks that the indexes of `room` follow its rows under its schema, and returns its schema
/// and contents.
pub(crate) async fn checked_contents<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) -> (Schema, RoomData) {
    let (schema, data) = engine.contents(room).await;
    assert_indexes_follow_tables(&schema, &data);
    (schema, data)
}

/// Applies `ops` to `room`, and checks that it gives the `expected` report and leaves indexes that
/// follow the rows.
pub(crate) async fn apply_checked<E: RoomContents>(
    engine: &E,
    room: &RoomId,
    ops: Vec<SequencedOperation>,
    expected: &ApplyReport,
) {
    let report = engine.apply_batch(room, ops).await.unwrap();
    assert_eq!(&report, expected);
    checked_contents(engine, room).await;
}

/// Applies the next `batches` batches of `log` to `room`, checking each one with
/// [`apply_checked`].
pub(crate) async fn apply_random_batches<E: RoomContents>(
    engine: &E,
    room: &RoomId,
    log: &mut RandomLog,
    batches: usize,
) {
    for _ in 0..batches {
        let (ops, expected) = log.next_batch();
        apply_checked(engine, room, ops, &expected).await;
    }
}

/// An insert into `parents` of the key 9, which the random logs never write, with `code`.
fn insert_parent(seq: u64, code: &str) -> SequencedOperation {
    let row = CompactRow::new(vec![Value::Int(9), Value::String(code.into())]);
    let op = Operation::insert(PARENTS, PrimaryKey::single(9i64), row, 0);
    SequencedOperation::new(seq, op)
}

/// Refuses, after random batches, a batch with a gap (`SequenceMismatch`) and one with an
/// operation the schema rejects (`SchemaValidation`), each after an insert that would change the
/// indexes, and checks that neither changes anything, indexes included. `room` is open, empty,
/// with [`constrained_schema`]. Returns its contents.
pub(crate) async fn refused_batches_change_nothing<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) -> RoomData {
    let mut log = RandomLog::new(7, constrained_schema());
    apply_random_batches(engine, room, &mut log, 10).await;
    let head = log.head();
    let (_, before) = checked_contents(engine, room).await;

    let invalid_row = CompactRow::new(vec![Value::String("not a key".into())]);
    let invalid = Operation::insert(PARENTS, PrimaryKey::single(8i64), invalid_row, 0);
    let gap = vec![
        insert_parent(head + 1, "gap"),
        insert_parent(head + 3, "gap"),
    ];
    let invalid = vec![
        insert_parent(head + 1, "invalid"),
        SequencedOperation::new(head + 2, invalid),
    ];
    for (batch, is_gap) in [(gap, true), (invalid, false)] {
        let err = engine.apply_batch(room, batch).await.unwrap_err();
        let expected = match err {
            StorageError::SequenceMismatch { .. } => is_gap,
            StorageError::SchemaValidation(_) => !is_gap,
            _ => false,
        };
        assert!(expected, "{err:?}");
        assert_eq!(engine.get_head_seq(room).await.unwrap().get(), head);
        let (_, after) = checked_contents(engine, room).await;
        assert_same_contents(&before, &after);
    }
    before
}

/// Applies snapshots to `room`, open with [`constrained_schema`] and empty: one at its head,
/// with [`evolved_schema`], changes nothing and keeps the room's schema; one ahead, with
/// [`evolved_schema`], replaces the schema, the rows and the indexes, built under the snapshot's
/// schema. Later batches keep the indexes on the rows.
pub(crate) async fn snapshots_replace_the_indexes<E: RoomContents>(engine: &E, room: &RoomId) {
    let mut log = RandomLog::new(11, constrained_schema());
    apply_random_batches(engine, room, &mut log, 5).await;
    let head = SequenceNumber::from(log.head());
    let (_, before) = checked_contents(engine, room).await;

    let at_head = engine.create_snapshot(room).await.unwrap();
    let applied = engine
        .apply_snapshot(room, evolved_schema(), at_head.into(), head)
        .await
        .unwrap();
    assert_eq!(applied, head);
    let (schema, after) = checked_contents(engine, room).await;
    assert_eq!(schema, constrained_schema());
    assert_same_contents(&before, &after);

    let source = MemoryStorageEngine::new();
    source.open_room(room, evolved_schema()).await.unwrap();
    let mut source_log = RandomLog::new(13, evolved_schema());
    while source_log.head() <= log.head() {
        apply_random_batches(&source, room, &mut source_log, 1).await;
    }
    let ahead = SequenceNumber::from(source_log.head());
    let snapshot = source.create_snapshot(room).await.unwrap();
    let applied = engine
        .apply_snapshot(room, evolved_schema(), snapshot.into(), ahead)
        .await
        .unwrap();
    assert_eq!(applied, ahead);
    let (schema, after) = checked_contents(engine, room).await;
    assert_eq!(schema, evolved_schema());
    let (_, expected) = checked_contents(&source, room).await;
    assert_same_contents(&after, &expected);

    for _ in 0..10 {
        let (ops, expected) = source_log.next_batch();
        apply_checked(&source, room, ops.clone(), &expected).await;
        apply_checked(engine, room, ops, &expected).await;
    }
    let (_, expected) = source.contents(room).await;
    let (_, after) = engine.contents(room).await;
    assert_same_contents(&after, &expected);
}

/// The thread point that building the indexes of a room's rows reaches.
pub(crate) const BUILD_POINT: &str = "room_data.build";

/// Applies to `room`, open with [`constrained_schema`] and empty, a snapshot ahead with rows,
/// and checks that its indexes were built off the current thread, the only one of the test's
/// runtime.
pub(crate) async fn snapshot_indexes_are_built_off_the_runtime<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) {
    let source = MemoryStorageEngine::new();
    source.open_room(room, evolved_schema()).await.unwrap();
    let mut source_log = RandomLog::new(17, evolved_schema());
    apply_random_batches(&source, room, &mut source_log, 10).await;
    let snapshot = source.create_snapshot(room).await.unwrap();
    let head = SequenceNumber::from(source_log.head());

    engine
        .apply_snapshot(room, evolved_schema(), snapshot.into(), head)
        .await
        .unwrap();
    let (_, data) = checked_contents(engine, room).await;
    assert!(data.tables().values().any(|table| !table.is_empty()));
    assert!(
        !crate::fail_point::reached_on_this_thread(BUILD_POINT),
        "the indexes were built on the runtime"
    );
}

// ---------------------------------------------------------------------------------------------
// The indexes of one table

/// `parents`, as [`constrained_schema`] declares it.
fn parents() -> TableSchema {
    constrained_schema()
        .get_table_by_id(PARENTS)
        .unwrap()
        .clone()
}

fn parent_row(id: i64, code: Option<&str>, score: Option<f64>) -> CompactRow {
    let code = code.map_or(Value::Null, |c| Value::String(c.into()));
    let score = score.map_or(Value::Null, Value::Float);
    CompactRow::new(vec![Value::Int(id), code, score])
}

/// The entries of `indexes`, a table of `parents`: those of `UNIQUE (code)` and of
/// `UNIQUE (score, code)`, as `(key, id)` pairs.
fn parent_entries(indexes: &TableIndexes) -> [Vec<(Vec<Value>, i64)>; 2] {
    let pairs = |index: &ConstraintIndex| {
        index
            .iter()
            .map(|(key, pk)| (key.values().to_vec(), pk.values()[0].as_int().unwrap()))
            .collect()
    };
    [pairs(&indexes.unique[0]), pairs(&indexes.unique[1])]
}

fn s(text: &str) -> Value {
    Value::String(text.into())
}

/// A key with a `NULL` is not indexed, nor one with a column missing from a short row; a
/// composite key with a single `NULL` is not either. A composite key lists its values in the
/// order the constraint declares its columns.
#[test]
fn only_keys_without_null_are_indexed() {
    let rows: Table = [
        parent_row(1, Some("a"), Some(1.5)),
        parent_row(2, None, Some(1.5)),
        parent_row(3, Some("c"), None),
        CompactRow::new(vec![Value::Int(4), s("d")]),
        CompactRow::new(vec![Value::Int(5)]),
    ]
    .into_iter()
    .map(|row| (PrimaryKey::single(row[0].clone()), row))
    .collect();

    let indexes = TableIndexes::build(&parents(), &rows);
    assert_eq!(
        parent_entries(&indexes),
        [
            vec![(vec![s("a")], 1), (vec![s("c")], 3), (vec![s("d")], 4)],
            vec![(vec![Value::Float(1.5), s("a")], 1)],
        ]
    );
}

/// Rows with the same key each have their entry, and removing one leaves the others.
#[test]
fn rows_with_the_same_key_each_have_an_entry() {
    let table = parents();
    let mut indexes = TableIndexes::new(&table);
    let (one, two) = (
        parent_row(1, Some("a"), None),
        parent_row(2, Some("a"), None),
    );
    indexes.update(&table, &PrimaryKey::single(1i64), None, Some(&one));
    indexes.update(&table, &PrimaryKey::single(2i64), None, Some(&two));
    assert_eq!(
        parent_entries(&indexes)[0],
        vec![(vec![s("a")], 1), (vec![s("a")], 2)]
    );

    indexes.update(&table, &PrimaryKey::single(1i64), Some(&one), None);
    assert_eq!(parent_entries(&indexes)[0], vec![(vec![s("a")], 2)]);
}

/// An update moves the entries of a row to its new key, removes them when the key gets a
/// `NULL`, adds them when it loses its last `NULL`, and a deleted row has none.
#[test]
fn updates_move_the_entries_of_a_row() {
    let table = parents();
    let pk = PrimaryKey::single(1i64);
    let mut indexes = TableIndexes::new(&table);
    let steps = [
        (
            parent_row(1, Some("a"), None),
            [vec![(vec![s("a")], 1)], vec![]],
        ),
        (
            parent_row(1, Some("b"), Some(1.5)),
            [
                vec![(vec![s("b")], 1)],
                vec![(vec![Value::Float(1.5), s("b")], 1)],
            ],
        ),
        (parent_row(1, None, Some(1.5)), [vec![], vec![]]),
        (
            parent_row(1, Some("c"), Some(1.5)),
            [
                vec![(vec![s("c")], 1)],
                vec![(vec![Value::Float(1.5), s("c")], 1)],
            ],
        ),
    ];
    let mut old: Option<CompactRow> = None;
    for (row, expected) in steps {
        indexes.update(&table, &pk, old.as_ref(), Some(&row));
        assert_eq!(parent_entries(&indexes), expected, "after {row:?}");
        old = Some(row);
    }
    indexes.update(&table, &pk, old.as_ref(), None);
    assert_eq!(parent_entries(&indexes), [vec![], vec![]]);
}
