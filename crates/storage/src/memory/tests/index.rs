//! Tests of the constraint indexes, and the helpers that the tests of both engines share to check
//! that the indexes of a room are always those its rows give: a schema with constraints, a random
//! log with the report each of its batches must give (from the reference model of the
//! integration tests), and the scenarios run on either engine.

use super::*;
use crate::engine::{ApplyReport, RejectReason};
use crate::memory::state::RoomData;
use crate::options::ScanOptions;
use crate::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use zemdb_core::{
    ColumnDef, DataType, ForeignKey, Operation, OwnerIdentity, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableBuilder, UniqueConstraint, Value,
};

/// The deterministic generator of the randomized tests, shared with the integration tests.
#[path = "../../../tests/common/rng.rs"]
mod rng;
pub(crate) use rng::Rng;

/// The reference model of a replica, shared with the integration tests. It shares no code with
/// the engines, so it also gives the report each batch of a random log must give.
#[path = "../../../tests/common/oracle.rs"]
#[allow(dead_code)]
pub(crate) mod oracle;

/// The reason an engine gives for an operation the reference model rejected for `reason`.
pub(crate) fn reject_reason(reason: oracle::Reason) -> RejectReason {
    match reason {
        oracle::Reason::MissingRow => RejectReason::MissingRow,
        oracle::Reason::Duplicate => RejectReason::Duplicate,
        oracle::Reason::NotOwner => RejectReason::NotOwner,
        oracle::Reason::Unique(constraint) => RejectReason::Unique { constraint },
        oracle::Reason::ForeignKey(constraint) => RejectReason::ForeignKey { constraint },
        oracle::Reason::Restrict(constraint) => RejectReason::Restrict { constraint },
    }
}

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
    Schema::builder()
        .table(parents_table())
        .table(children_table())
        .table(plain_table())
        .build()
}

/// `parents`, as [`constrained_schema`] declares it.
fn parents_table() -> TableBuilder {
    TableSchema::builder("parents")
        .table_id(PARENTS)
        .primary_key("id", DataType::Int)
        .nullable_column("code", DataType::String)
        .nullable_column("score", DataType::Float)
        .unique("parents_code", ["code"])
        .unique("parents_score_code", ["score", "code"])
}

/// `children`, as [`constrained_schema`] declares it.
fn children_table() -> TableBuilder {
    TableSchema::builder("children")
        .table_id(CHILDREN)
        .primary_key("a", DataType::Int)
        .primary_key("b", DataType::Int)
        .nullable_column("parent", DataType::Int)
        .nullable_column("up_a", DataType::Int)
        .nullable_column("up_b", DataType::Int)
        .nullable_column("x", DataType::Int)
        .foreign_key("children_parent", ["parent"], "parents")
        .foreign_key("children_up", ["up_b", "up_a"], "children")
        .unique("children_x", ["x"])
}

/// `plain`, as [`constrained_schema`] declares it.
fn plain_table() -> TableBuilder {
    TableSchema::builder("plain")
        .table_id(PLAIN)
        .primary_key("id", DataType::Int)
        .nullable_column("v", DataType::Int)
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

/// A random log of batches over a schema, with the report each one must give, which the
/// reference model decides.
pub(crate) struct RandomLog {
    rng: Rng,
    model: oracle::Oracle,
}

impl RandomLog {
    pub(crate) fn new(seed: u64, schema: Schema) -> Self {
        Self {
            rng: Rng::new(seed),
            model: oracle::Oracle::new(schema),
        }
    }

    /// Writes the next batches under `schema`, which must be a later version of the current one.
    pub(crate) fn evolve(&mut self, schema: Schema) {
        self.model.evolve(schema);
    }

    pub(crate) fn head(&self) -> u64 {
        self.model.head()
    }

    /// The next batch, of one to eight operations, and the report it must give.
    pub(crate) fn next_batch(&mut self) -> (Vec<SequencedOperation>, ApplyReport) {
        let len = 1 + self.rng.below(8);
        let first = self.model.head() + 1;
        let ops = oracle::random_log(&mut self.rng, self.model.schema(), first, len);
        let report = self.decide(&ops);
        (ops, report)
    }

    /// Takes `ops`, which must continue the log and be valid under its schema, as its next
    /// batch, and returns the report it must give.
    pub(crate) fn decide(&mut self, ops: &[SequencedOperation]) -> ApplyReport {
        let rejected = self
            .model
            .apply_batch(ops)
            .into_iter()
            .map(|(seq, reason)| (SequenceNumber::from(seq), reject_reason(reason)))
            .collect();
        ApplyReport::new(SequenceNumber::from(self.model.head()), rejected)
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

/// Checks that `a` and `b` hold the same rows but for the `NULL`s at the end of a row, and the
/// same indexes. A row updated under an older version of the schema is as wide as that version,
/// so a room that reloads a schema and one open with it from the start can differ in those.
pub(crate) fn assert_equivalent_contents(a: &RoomData, b: &RoomData) {
    let trimmed = |data: &RoomData| -> BTreeMap<u16, Vec<(PrimaryKey, Vec<Value>)>> {
        let tables = data.tables().iter();
        tables
            .map(|(&table_id, rows)| {
                let rows = rows.iter().map(|(pk, row)| {
                    let mut values = row.values().to_vec();
                    while values.last().is_some_and(Value::is_null) {
                        values.pop();
                    }
                    (pk.clone(), values)
                });
                (table_id, rows.collect())
            })
            .collect()
    };
    assert_eq!(trimmed(a), trimmed(b));
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
pub(crate) fn insert_parent(seq: u64, code: &str) -> SequencedOperation {
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
// Snapshots checked against the constraints

/// The tables of [`evolved_schema`] without any constraint, to write rows that break them.
fn unconstrained_schema() -> Schema {
    let parents = TableSchema::builder("parents")
        .table_id(PARENTS)
        .primary_key("id", DataType::Int)
        .nullable_column("code", DataType::String)
        .nullable_column("score", DataType::Float)
        .nullable_column("tag", DataType::String);
    let children = TableSchema::builder("children")
        .table_id(CHILDREN)
        .primary_key("a", DataType::Int)
        .primary_key("b", DataType::Int)
        .nullable_column("parent", DataType::Int)
        .nullable_column("up_a", DataType::Int)
        .nullable_column("up_b", DataType::Int)
        .nullable_column("x", DataType::Int);
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

/// A row as the id of its table and its values.
type TableRow = (u16, Vec<Value>);

/// A row of `table` from its values; its key is its first value, or its first two in
/// `children`.
fn keyed(table: u16, values: Vec<Value>) -> (PrimaryKey, CompactRow) {
    let width = if table == CHILDREN { 2 } else { 1 };
    let pk = PrimaryKey::composite(values[..width].to_vec());
    (pk, CompactRow::new(values))
}

/// A snapshot at sequence `head` of a room written under `schema`, which holds `rows` as
/// `(table, values)`: they are inserted first, and deletes of keys of `plain` that do not
/// exist take the head to `head`. Returns the source engine too, to compare with.
async fn snapshot_with(
    schema: &Schema,
    rows: Vec<TableRow>,
    head: u64,
) -> (MemoryStorageEngine, Vec<u8>) {
    let room = RoomId::new("snapshot-source").unwrap();
    let source = MemoryStorageEngine::new();
    source.open_room(&room, schema.clone()).await.unwrap();
    let count = rows.len() as u64;
    assert!(count <= head);
    let mut ops: Vec<SequencedOperation> = rows
        .into_iter()
        .zip(1..)
        .map(|((table, values), seq)| {
            let (pk, row) = keyed(table, values);
            SequencedOperation::new(seq, Operation::insert(table, pk, row, 0))
        })
        .collect();
    for seq in count + 1..=head {
        let pk = PrimaryKey::single(1000 + seq as i64);
        ops.push(SequencedOperation::new(
            seq,
            Operation::delete(PLAIN, pk, 0),
        ));
    }
    let report = source.apply_batch(&room, ops).await.unwrap();
    assert!(report.rejected().is_empty(), "{report:?}");
    let snapshot = source.create_snapshot(&room).await.unwrap();
    (source, snapshot)
}

fn int(value: i64) -> Value {
    Value::Int(value)
}

/// Rows that break each constraint of [`evolved_schema`] that can be broken alone, with the
/// table and the name of the constraint the error must give: a repeated `UNIQUE` key, in a
/// column of the table and in one added later, and a foreign key without its parent row, to
/// another table and to the table itself. Where several constraints are broken, the first table
/// by id and, within it, the first row by key is named.
fn rows_that_break_a_constraint() -> Vec<(u16, &'static str, Vec<TableRow>)> {
    let null = Value::Null;
    vec![
        (
            PARENTS,
            "parents_code",
            vec![
                (PARENTS, vec![int(1), s("dup"), null.clone()]),
                (PARENTS, vec![int(2), s("dup"), null.clone()]),
            ],
        ),
        (
            PARENTS,
            "parents_tag",
            vec![
                (PARENTS, vec![int(1), s("a"), null.clone(), s("t")]),
                (PARENTS, vec![int(2), s("b"), null.clone(), s("t")]),
            ],
        ),
        (
            CHILDREN,
            "children_x",
            vec![
                (
                    CHILDREN,
                    vec![
                        int(1),
                        int(1),
                        null.clone(),
                        null.clone(),
                        null.clone(),
                        int(5),
                    ],
                ),
                (
                    CHILDREN,
                    vec![
                        int(1),
                        int(2),
                        null.clone(),
                        null.clone(),
                        null.clone(),
                        int(5),
                    ],
                ),
            ],
        ),
        (
            CHILDREN,
            "children_parent",
            vec![
                (PARENTS, vec![int(1)]),
                (CHILDREN, vec![int(1), int(1), int(7)]),
            ],
        ),
        // `(up_b, up_a)` is `(3, 2)`: the child `a = 3, b = 2`, which does not exist.
        (
            CHILDREN,
            "children_up",
            vec![
                (CHILDREN, vec![int(2), int(3), null.clone(), int(2), int(3)]),
                (CHILDREN, vec![int(1), int(1), null.clone(), int(2), int(3)]),
            ],
        ),
        // Two tables break a constraint: the first by id is named.
        (
            PARENTS,
            "parents_code",
            vec![
                (
                    CHILDREN,
                    vec![
                        int(1),
                        int(1),
                        null.clone(),
                        null.clone(),
                        null.clone(),
                        int(5),
                    ],
                ),
                (
                    CHILDREN,
                    vec![
                        int(1),
                        int(2),
                        null.clone(),
                        null.clone(),
                        null.clone(),
                        int(5),
                    ],
                ),
                (PARENTS, vec![int(1), s("dup")]),
                (PARENTS, vec![int(2), s("dup")]),
            ],
        ),
        // Two rows break a constraint each: the first by key is named.
        (
            CHILDREN,
            "children_parent",
            vec![
                (CHILDREN, vec![int(2), int(2), null.clone(), int(2), int(3)]),
                (CHILDREN, vec![int(1), int(1), int(7)]),
            ],
        ),
    ]
}

/// Applies to `room`, open with [`constrained_schema`] and holding random batches, snapshots
/// ahead of it that break a constraint of [`evolved_schema`], the schema passed with them. Each
/// is `SnapshotConstraints` naming the table and the constraint, and leaves the head, the
/// schema, the rows and the indexes as they were. Returns the room's contents.
pub(crate) async fn snapshots_that_break_a_constraint_change_nothing<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) -> (Schema, RoomData) {
    let mut log = RandomLog::new(19, constrained_schema());
    apply_random_batches(engine, room, &mut log, 5).await;
    let head = log.head();
    let (schema, before) = checked_contents(engine, room).await;

    for (table, constraint, rows) in rows_that_break_a_constraint() {
        let (_, snapshot) = snapshot_with(&unconstrained_schema(), rows, head + 3).await;
        let err = engine
            .apply_snapshot(room, evolved_schema(), snapshot.into(), (head + 3).into())
            .await
            .unwrap_err();
        let StorageError::SnapshotConstraints {
            table_id,
            constraint: named,
        } = &err
        else {
            panic!("{constraint}: {err:?}");
        };
        assert_eq!((*table_id, named.as_str()), (table, constraint));
        assert_eq!(engine.get_head_seq(room).await.unwrap().get(), head);
        let (after_schema, after) = checked_contents(engine, room).await;
        assert_eq!(after_schema, schema, "{constraint}");
        assert_same_contents(&before, &after);
    }
    (schema, before)
}

/// Applies to `room`, open with [`constrained_schema`] and empty, snapshots that meet the
/// constraints of the schema passed with them, and checks that each is installed with its rows
/// and their indexes:
/// - keys with a `NULL`, repeated or without a parent, also in composite keys with a single
///   `NULL`;
/// - a row that references itself, through a composite foreign key that names its columns in
///   another order than the table;
/// - short rows written under [`constrained_schema`], installed under [`evolved_schema`], whose
///   `UNIQUE (tag)` reads the column they lack as `NULL`.
pub(crate) async fn snapshots_that_meet_the_constraints_are_installed<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) {
    let null = Value::Null;
    let with_null = vec![
        (PARENTS, vec![int(1), null.clone(), Value::Float(1.5)]),
        (PARENTS, vec![int(2), null.clone(), Value::Float(1.5)]),
        (
            CHILDREN,
            vec![
                int(1),
                int(1),
                null.clone(),
                int(4),
                null.clone(),
                null.clone(),
            ],
        ),
        (
            CHILDREN,
            vec![
                int(1),
                int(2),
                null.clone(),
                null.clone(),
                int(4),
                null.clone(),
            ],
        ),
    ];
    let referencing_itself = vec![
        (CHILDREN, vec![int(2), int(1), null.clone(), int(1), int(2)]),
        (CHILDREN, vec![int(3), int(1), null.clone(), int(1), int(2)]),
    ];
    let short_rows = vec![
        (PARENTS, vec![int(1), s("a")]),
        (PARENTS, vec![int(2), s("b"), Value::Float(1.5)]),
    ];
    let cases = [
        (
            "keys with NULL",
            unconstrained_schema(),
            with_null,
            constrained_schema(),
        ),
        (
            "a row that references itself",
            unconstrained_schema(),
            referencing_itself,
            constrained_schema(),
        ),
        (
            "short rows of an older version",
            constrained_schema(),
            short_rows,
            evolved_schema(),
        ),
    ];
    for (head, (case, written_under, rows, schema)) in (5u64..).step_by(5).zip(cases) {
        let (source, snapshot) = snapshot_with(&written_under, rows, head).await;
        let applied = engine
            .apply_snapshot(room, schema.clone(), snapshot.into(), head.into())
            .await;
        assert_eq!(applied.unwrap().get(), head, "{case}");
        let (installed_schema, installed) = checked_contents(engine, room).await;
        assert_eq!(installed_schema, schema, "{case}");
        let source_room = RoomId::new("snapshot-source").unwrap();
        let (_, expected) = source.contents(&source_room).await;
        assert_eq!(installed.tables(), expected.tables(), "{case}");
    }
}

// ---------------------------------------------------------------------------------------------
// Reloading the schema

pub(crate) const EXTRA: u16 = 3;
pub(crate) const OWNED: u16 = 3;

/// [`evolved_schema`] with a column `alt` added to `children` together with a foreign key to
/// `parents`, and a new table `extra` (an integer key and a parent, with a foreign key to
/// `parents` and `UNIQUE (parent)`). It evolves from [`constrained_schema`] and from
/// [`evolved_schema`].
pub(crate) fn reloaded_schema() -> Schema {
    let mut schema = evolved_schema();
    let alt = ColumnDef::new("alt", DataType::Int).nullable(true);
    let foreign = vec![ForeignKey::new("children_alt", ["alt"], "parents")];
    schema
        .add_column_with_constraints("children", alt, Vec::new(), foreign)
        .unwrap();
    let extra = TableSchema::builder("extra")
        .table_id(EXTRA)
        .primary_key("id", DataType::Int)
        .nullable_column("parent", DataType::Int)
        .foreign_key("extra_parent", ["parent"], "parents")
        .unique("extra_parent_once", ["parent"])
        .build()
        .unwrap();
    schema.insert_table(extra).unwrap();
    schema
}

/// `owned`: an integer key and a string `who`, which owns the row, and a nullable integer.
fn owned_table() -> TableBuilder {
    TableSchema::builder("owned")
        .table_id(OWNED)
        .primary_key("id", DataType::Int)
        .column("who", DataType::String)
        .nullable_column("n", DataType::Int)
        .owner("who", OwnerIdentity::ClientId)
}

/// [`constrained_schema`] with the table `owned`.
pub(crate) fn owned_schema() -> Schema {
    Schema::builder()
        .table(parents_table())
        .table(children_table())
        .table(plain_table())
        .table(owned_table())
        .build()
}

/// Schemas that extend [`owned_schema`] or take something from it but do not evolve from it,
/// one per rule of `Schema::evolves_from`, with what each one changes.
fn schemas_that_do_not_evolve() -> Vec<(&'static str, Schema)> {
    let schema = |parents: TableBuilder, plain: TableBuilder, owned: TableBuilder| {
        Schema::builder()
            .table(parents)
            .table(children_table())
            .table(plain)
            .table(owned)
            .build()
    };
    let reordered_parents = TableSchema::builder("parents")
        .table_id(PARENTS)
        .primary_key("id", DataType::Int)
        .nullable_column("code", DataType::String)
        .nullable_column("score", DataType::Float)
        .unique("parents_code", ["code"])
        .unique("parents_score_code", ["code", "score"]);
    let unowned = TableSchema::builder("owned")
        .table_id(OWNED)
        .primary_key("id", DataType::Int)
        .column("who", DataType::String)
        .nullable_column("n", DataType::Int);
    let narrower_plain = TableSchema::builder("plain")
        .table_id(PLAIN)
        .primary_key("id", DataType::Int);
    vec![
        (
            "a constraint over columns it had",
            schema(
                parents_table().unique("parents_score", ["score"]),
                plain_table(),
                owned_table(),
            ),
        ),
        (
            "a constraint over other columns",
            schema(reordered_parents, plain_table(), owned_table()),
        ),
        (
            "another owner",
            schema(parents_table(), plain_table(), unowned),
        ),
        (
            "another immutability",
            schema(parents_table(), plain_table(), owned_table().immutable()),
        ),
        (
            "a column less",
            schema(parents_table(), narrower_plain, owned_table()),
        ),
    ]
}

/// Refuses, after random batches, a reload to each schema of [`schemas_that_do_not_evolve`]
/// (`SchemaNotEvolving`), and checks that none changes the room: its schema, its rows, its
/// indexes and how the next batches are decided. `room` is open, empty, with [`owned_schema`].
pub(crate) async fn reloads_that_do_not_evolve_change_nothing<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) {
    let mut log = RandomLog::new(31, owned_schema());
    apply_random_batches(engine, room, &mut log, 10).await;
    let (_, before) = checked_contents(engine, room).await;
    for (change, schema) in schemas_that_do_not_evolve() {
        assert!(!schema.evolves_from(&owned_schema()), "{change}");
        let err = engine.reload_schema(room, schema).await.unwrap_err();
        assert!(
            matches!(&err, StorageError::SchemaNotEvolving { room_id } if room_id == room),
            "{change}: {err:?}"
        );
        let (schema, after) = checked_contents(engine, room).await;
        assert_eq!(schema, owned_schema(), "{change}");
        assert_same_contents(&before, &after);
    }
    apply_random_batches(engine, room, &mut log, 5).await;
}

/// A row of `parents` with key `id`, code `code` and tag `tag`, which only a schema with the
/// column `tag` accepts.
pub(crate) fn tagged_parent(seq: u64, id: i64, code: &str, tag: &str) -> SequencedOperation {
    let row = CompactRow::new(vec![int(id), s(code), Value::Null, s(tag)]);
    let op = Operation::insert(PARENTS, PrimaryKey::single(id), row, 0);
    SequencedOperation::new(seq, op)
}

/// A row of `children` with key `(a, b)` and `alt`, which only [`reloaded_schema`] accepts.
pub(crate) fn child_with_alt(seq: u64, (a, b): (i64, i64), alt: i64) -> SequencedOperation {
    let null = Value::Null;
    let values = vec![
        int(a),
        int(b),
        null.clone(),
        null.clone(),
        null.clone(),
        null,
        int(alt),
    ];
    let pk = PrimaryKey::composite([int(a), int(b)]);
    let op = Operation::insert(CHILDREN, pk, CompactRow::new(values), 0);
    SequencedOperation::new(seq, op)
}

/// A row of `extra` with key `id` and parent `parent`.
fn extra_row(seq: u64, id: i64, parent: i64) -> SequencedOperation {
    let row = CompactRow::new(vec![int(id), int(parent)]);
    let op = Operation::insert(EXTRA, PrimaryKey::single(id), row, 0);
    SequencedOperation::new(seq, op)
}

/// The reasons of the rejections of `report`, as they print.
fn rejected_constraints(report: &ApplyReport) -> Vec<String> {
    report
        .rejected()
        .iter()
        .map(|(_, reason)| format!("{reason:?}"))
        .collect()
}

/// Reloads `room`, open with [`constrained_schema`], to [`reloaded_schema`] after random
/// batches, and checks that from then on it decides as a room open with that schema from the
/// start, a memory replica that applies the same log: right after the reload, the rows are the
/// same, with an empty `extra`, and the indexes are those the rows give and those of the
/// replica; a batch that repeats the `UNIQUE` key of a new column, writes a foreign key of one
/// without its parent row, writes the new table and deletes a parent that a new foreign key
/// references gives the replica's report; and so do random batches over the new columns.
/// Reloading the same schema again changes nothing, and a scan begun before the reload keeps
/// the rows it started with.
pub(crate) async fn reloads_that_evolve_decide_as_the_new_schema<E: RoomContents>(
    engine: &E,
    room: &RoomId,
) {
    let replica = MemoryStorageEngine::new();
    replica.open_room(room, reloaded_schema()).await.unwrap();
    let mut log = RandomLog::new(37, constrained_schema());
    for _ in 0..10 {
        let (ops, expected) = log.next_batch();
        apply_checked(engine, room, ops.clone(), &expected).await;
        apply_checked(&replica, room, ops, &expected).await;
    }
    let (_, before) = checked_contents(engine, room).await;
    let scan = engine
        .scan_by_id(room, PARENTS, ScanOptions::default())
        .await
        .unwrap();

    engine.reload_schema(room, reloaded_schema()).await.unwrap();
    let (schema, after) = checked_contents(engine, room).await;
    assert_eq!(schema, reloaded_schema());
    let mut tables = before.tables().clone();
    tables.insert(EXTRA, Table::new());
    assert_eq!(after.tables(), &tables);
    let (_, in_replica) = replica.contents(room).await;
    assert_same_contents(&after, &in_replica);

    log.evolve(reloaded_schema());
    let head = log.head();
    let batch = vec![
        tagged_parent(head + 1, 100, "c100", "t"),
        tagged_parent(head + 2, 101, "c101", "t"),
        child_with_alt(head + 3, (100, 1), 999),
        child_with_alt(head + 4, (100, 2), 100),
        extra_row(head + 5, 1, 100),
        extra_row(head + 6, 2, 100),
        extra_row(head + 7, 3, 999),
        SequencedOperation::new(
            head + 8,
            Operation::delete(PARENTS, PrimaryKey::single(100i64), 0),
        ),
    ];
    let expected = log.decide(&batch);
    assert_eq!(
        rejected_constraints(&expected),
        [
            r#"Unique { constraint: "parents_tag" }"#,
            r#"ForeignKey { constraint: "children_alt" }"#,
            r#"Unique { constraint: "extra_parent_once" }"#,
            r#"ForeignKey { constraint: "extra_parent" }"#,
            r#"Restrict { constraint: "children_alt" }"#,
        ]
    );
    apply_checked(engine, room, batch.clone(), &expected).await;
    apply_checked(&replica, room, batch, &expected).await;
    for _ in 0..20 {
        let (ops, expected) = log.next_batch();
        apply_checked(engine, room, ops.clone(), &expected).await;
        apply_checked(&replica, room, ops, &expected).await;
    }
    let (_, now) = checked_contents(engine, room).await;
    let (_, in_replica) = replica.contents(room).await;
    assert_equivalent_contents(&now, &in_replica);

    engine.reload_schema(room, reloaded_schema()).await.unwrap();
    let (schema, again) = checked_contents(engine, room).await;
    assert_eq!(schema, reloaded_schema());
    assert_same_contents(&again, &now);

    let scanned: Vec<_> = scan.map(Result::unwrap).collect().await;
    let rows: Vec<_> = before.tables()[&PARENTS]
        .iter()
        .map(|(pk, row)| (pk.clone(), row.clone()))
        .collect();
    assert_eq!(scanned, rows);
}

/// Applies the random log of `seed` to `room`, open with [`constrained_schema`], reloading it
/// to [`evolved_schema`] and then to [`reloaded_schema`] at random points, and checks that it
/// gives the reports and ends with the rows and indexes of a memory replica that applies the
/// whole log under [`reloaded_schema`]; right after each reload, the indexes are those the rows
/// give.
pub(crate) async fn reloads_at_random_points_decide_as_the_new_schema<E: RoomContents>(
    engine: &E,
    room: &RoomId,
    seed: u64,
) {
    let replica = MemoryStorageEngine::new();
    replica.open_room(room, reloaded_schema()).await.unwrap();
    let mut log = RandomLog::new(seed, constrained_schema());
    let mut rng = Rng::new(seed ^ 0x5EED);
    let mut versions = vec![reloaded_schema(), evolved_schema()];
    let mut at = [rng.below(15), 15 + rng.below(15)];
    at.sort_unstable();
    for batch in 0..40 {
        if at.first() == Some(&batch) && !versions.is_empty() {
            at.rotate_left(1);
            let schema = versions.pop().unwrap();
            engine.reload_schema(room, schema.clone()).await.unwrap();
            let (reloaded, _) = checked_contents(engine, room).await;
            assert_eq!(reloaded, schema, "seed {seed}, batch {batch}");
            log.evolve(schema);
        }
        let (ops, expected) = log.next_batch();
        apply_checked(engine, room, ops.clone(), &expected).await;
        apply_checked(&replica, room, ops, &expected).await;
    }
    assert!(versions.is_empty(), "seed {seed}");
    let (_, now) = checked_contents(engine, room).await;
    let (_, in_replica) = replica.contents(room).await;
    assert_equivalent_contents(&now, &in_replica);
}

/// Races, on `room` (open with [`constrained_schema`], empty), a writer, a reload to
/// [`evolved_schema`] once the head reaches a random point, and a reader. Every batch of the
/// writer ends with an insert that writes the new column `tag`: until the reload, the whole
/// batch is refused (`SchemaValidation`) and changes nothing, and the writer sends it again
/// without that insert; once a batch with the insert is applied, no later one is refused. Every
/// applied batch gives the report it gives under [`evolved_schema`], every state the reader
/// sees has indexes that follow its rows, and the room ends with the rows and indexes of a
/// memory replica that applies the same batches under [`evolved_schema`].
pub(crate) async fn reloads_race_batches_and_readers<E: RoomContents>(
    engine: &E,
    room: &RoomId,
    seed: u64,
) {
    let mut rng = Rng::new(seed);
    let reload_at = rng.below(12);
    let mut log = RandomLog::new(seed, evolved_schema());
    let replica = MemoryStorageEngine::new();
    replica.open_room(room, evolved_schema()).await.unwrap();
    let writing = std::sync::atomic::AtomicBool::new(true);

    let writer = async {
        let mut tagged_applied = false;
        for batch in 0..30u64 {
            let len = 1 + rng.below(4);
            let mut ops = oracle::random_log(&mut rng, &constrained_schema(), log.head() + 1, len);
            let tag = format!("t{}", batch / 2);
            let code = format!("c{batch}");
            ops.push(tagged_parent(
                log.head() + 1 + len,
                1000 + batch as i64,
                &code,
                &tag,
            ));
            let head = engine.get_head_seq(room).await.unwrap();
            let applied = match engine.apply_batch(room, ops.clone()).await {
                Ok(report) => {
                    tagged_applied = true;
                    (ops, report)
                }
                Err(StorageError::SchemaValidation(_)) => {
                    assert!(!tagged_applied, "seed {seed}, batch {batch}");
                    assert_eq!(engine.get_head_seq(room).await.unwrap(), head);
                    ops.pop();
                    let report = engine.apply_batch(room, ops.clone()).await.unwrap();
                    (ops, report)
                }
                Err(err) => panic!("seed {seed}, batch {batch}: {err:?}"),
            };
            let (ops, report) = applied;
            assert_eq!(report, log.decide(&ops), "seed {seed}, batch {batch}");
            replica.apply_batch(room, ops).await.unwrap();
            tokio::task::yield_now().await;
        }
        assert!(tagged_applied, "seed {seed}: the reload never came");
        writing.store(false, std::sync::atomic::Ordering::Release);
    };
    let reloader = async {
        while engine.get_head_seq(room).await.unwrap().get() < reload_at {
            tokio::task::yield_now().await;
        }
        engine.reload_schema(room, evolved_schema()).await.unwrap();
    };
    let reader = async {
        let mut seen = 0;
        while writing.load(std::sync::atomic::Ordering::Acquire) {
            checked_contents(engine, room).await;
            let scan = engine.scan_by_id(room, PARENTS, ScanOptions::default());
            let _: Vec<_> = scan.await.unwrap().map(Result::unwrap).collect().await;
            seen += 1;
            tokio::task::yield_now().await;
        }
        seen
    };
    let ((), (), seen) = tokio::join!(writer, reloader, reader);
    assert!(seen > 0);
    let (schema, now) = checked_contents(engine, room).await;
    assert_eq!(schema, evolved_schema());
    let (_, in_replica) = replica.contents(room).await;
    assert_equivalent_contents(&now, &in_replica);
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
