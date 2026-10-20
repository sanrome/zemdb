//! The integrity rules that every replica checks while applying the log: `UNIQUE`, foreign keys
//! with `ON DELETE RESTRICT`, immutable tables and row owners. Each rule runs on both engines
//! through the same helper, and the disk engine must decide the same again when it reopens and
//! replays its WAL. A reference model, which checks every rule by walking whole tables, must
//! agree with both engines on random logs.
//!
//! The ignored variant runs many more logs: `cargo test --release -- --ignored`.

mod common;

use common::oracle::{self, Oracle, CHILDREN, LEDGER, NOTES, PARENTS, POSTS, PROFILES, VOTES};
use common::rng::Rng;
use common::{disk_engine, reject_reasons, room_rows, trimmed_dump};
use zemdb_core::{
    ColumnUpdate, CompactRow, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, Value,
};
use zemdb_storage::{MemoryStorageEngine, RejectReason, StorageEngine};

// ---------------------------------------------------------------------------------------------
// Helpers

/// What both engines did with `batches`, applied in order to an empty room with `schema`: the
/// sequence and the reason of every operation they rejected, and the rows they hold. Both
/// engines must report the same and hold the same, and so must the disk room after a reopen,
/// whose replay of the WAL takes the decisions again.
async fn apply_in_both_engines(
    schema: &Schema,
    batches: &[Vec<SequencedOperation>],
) -> (Vec<(u64, RejectReason)>, oracle::Rows) {
    let room = RoomId::new("rules").unwrap();
    let memory = MemoryStorageEngine::new();
    memory.open_room(&room, schema.clone()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let disk = disk_engine(dir.path());
    disk.open_room(&room, schema.clone()).await.unwrap();

    let mut rejected = Vec::new();
    for batch in batches {
        let head = batch.last().map(|op| op.seq);
        let in_memory = memory.apply_batch(&room, batch.clone()).await.unwrap();
        let on_disk = disk.apply_batch(&room, batch.clone()).await.unwrap();
        assert_eq!(in_memory, on_disk, "the engines report differently");
        if let Some(head) = head {
            assert_eq!(
                in_memory.head_seq(),
                head,
                "the head moves past every operation"
            );
        }
        rejected.extend(
            in_memory
                .rejected()
                .iter()
                .map(|(seq, reason)| (seq.get(), reason.clone())),
        );
    }

    let rows = room_rows(&memory, &room, schema).await;
    assert_eq!(
        oracle::dump(&room_rows(&disk, &room, schema).await),
        oracle::dump(&rows),
        "the engines hold different rows"
    );
    let head = memory.get_head_seq(&room).await.unwrap();
    disk.close_room(&room).await.unwrap();
    let reopened = disk_engine(dir.path());
    reopened.open_room(&room, schema.clone()).await.unwrap();
    assert_eq!(reopened.get_head_seq(&room).await.unwrap(), head);
    assert_eq!(
        oracle::dump(&room_rows(&reopened, &room, schema).await),
        oracle::dump(&rows),
        "the replay decided differently"
    );
    (rejected, rows)
}

/// `batches` numbered from sequence 1 on, in order.
fn numbered(batches: Vec<Vec<Operation>>) -> Vec<Vec<SequencedOperation>> {
    let mut seq = 0;
    batches
        .into_iter()
        .map(|batch| {
            batch
                .into_iter()
                .map(|op| {
                    seq += 1;
                    SequencedOperation::new(seq, op)
                })
                .collect()
        })
        .collect()
}

/// Each operation in a batch of its own, numbered from sequence 1 on.
fn one_per_batch(ops: Vec<Operation>) -> Vec<Vec<SequencedOperation>> {
    numbered(ops.into_iter().map(|op| vec![op]).collect())
}

/// What both engines do with `ops`, all in one batch, under [`oracle::schema`].
async fn in_one_batch(ops: Vec<Operation>) -> (Vec<(u64, RejectReason)>, oracle::Rows) {
    apply_in_both_engines(&oracle::schema(), &numbered(vec![ops])).await
}

/// What both engines do with `ops`, each in a batch of its own, under [`oracle::schema`].
async fn apply(ops: Vec<Operation>) -> (Vec<(u64, RejectReason)>, oracle::Rows) {
    apply_in_both_engines(&oracle::schema(), &one_per_batch(ops)).await
}

fn s(text: &str) -> Value {
    Value::String(text.into())
}

fn int(value: Option<i64>) -> Value {
    value.map_or(Value::Null, Value::Int)
}

fn text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, s)
}

fn insert(table: u16, key: Vec<Value>, row: Vec<Value>) -> Operation {
    Operation::insert(table, PrimaryKey::composite(key), CompactRow::new(row), 0)
}

fn update(table: u16, key: Vec<Value>, updates: Vec<(u16, Value)>) -> Operation {
    let updates = updates
        .into_iter()
        .map(|(column, value)| ColumnUpdate::new(column, value))
        .collect();
    Operation::update(table, PrimaryKey::composite(key), updates, 0)
}

fn delete(table: u16, key: Vec<Value>) -> Operation {
    Operation::delete(table, PrimaryKey::composite(key), 0)
}

fn delete_as(table: u16, key: Vec<Value>, owner: &str) -> Operation {
    Operation::delete_with_owner(table, PrimaryKey::composite(key), s(owner), 0)
}

/// A row of `parents (id, code, score, rank)` without a rank.
fn parent(id: i64, code: Option<&str>, score: Option<f64>) -> Operation {
    let score = score.map_or(Value::Null, Value::Float);
    insert(
        PARENTS,
        vec![Value::Int(id)],
        vec![Value::Int(id), text(code), score, Value::Null],
    )
}

/// A row of `parents (id, code, score, rank)` without a code.
fn ranked(id: i64, score: Option<f64>, rank: Option<i64>) -> Operation {
    let score = score.map_or(Value::Null, Value::Float);
    insert(
        PARENTS,
        vec![Value::Int(id)],
        vec![Value::Int(id), Value::Null, score, int(rank)],
    )
}

/// A row of `children (a, b, parent, up_a, up_b, x)` that references the child `up` through
/// `children_up`, which declares its columns as `(up_b, up_a)`: `up_b` holds the `a` of the
/// referenced child and `up_a` its `b`.
fn child(
    key: (i64, i64),
    parent: Option<i64>,
    up: (Option<i64>, Option<i64>),
    x: Option<i64>,
) -> Operation {
    let (a, b) = (Value::Int(key.0), Value::Int(key.1));
    insert(
        CHILDREN,
        vec![a.clone(), b.clone()],
        vec![a, b, int(parent), int(up.1), int(up.0), int(x)],
    )
}

fn child_key(a: i64, b: i64) -> Vec<Value> {
    vec![Value::Int(a), Value::Int(b)]
}

/// A row of `votes (voter, poll, choice)`.
fn vote(voter: &str, poll: i64, choice: i64) -> Operation {
    insert(
        VOTES,
        vec![s(voter), Value::Int(poll)],
        vec![s(voter), Value::Int(poll), Value::Int(choice)],
    )
}

/// A row of `posts (id, author, slug, note)`.
fn post(id: i64, author: &str, slug: Option<&str>, note: Option<f64>) -> Operation {
    let note = note.map_or(Value::Null, Value::Float);
    insert(
        POSTS,
        vec![Value::Int(id)],
        vec![Value::Int(id), s(author), text(slug), note],
    )
}

/// A row of `notes (k, post)`.
fn note(k: f64, post: Option<i64>) -> Operation {
    insert(
        NOTES,
        vec![Value::Float(k)],
        vec![Value::Float(k), int(post)],
    )
}

/// A row of `ledger (id, holder, amount)`.
fn entry(id: i64, holder: &str, amount: Option<i64>) -> Operation {
    insert(
        LEDGER,
        vec![Value::Int(id)],
        vec![Value::Int(id), s(holder), int(amount)],
    )
}

fn unique(name: &str) -> RejectReason {
    RejectReason::Unique {
        constraint: name.into(),
    }
}

fn foreign_key(name: &str) -> RejectReason {
    RejectReason::ForeignKey {
        constraint: name.into(),
    }
}

fn restrict(name: &str) -> RejectReason {
    RejectReason::Restrict {
        constraint: name.into(),
    }
}

/// The row of `table` with key `key` in `rows`.
fn row_of(rows: &oracle::Rows, table: u16, key: Vec<Value>) -> Option<&CompactRow> {
    rows.get(&table)?.get(&PrimaryKey::composite(key))
}

/// The keys of the rows of `table` in `rows`.
fn keys_of(rows: &oracle::Rows, table: u16) -> Vec<PrimaryKey> {
    rows.get(&table)
        .map(|rows| rows.keys().cloned().collect())
        .unwrap_or_default()
}

fn ids(ids: impl IntoIterator<Item = i64>) -> Vec<PrimaryKey> {
    ids.into_iter().map(PrimaryKey::single).collect()
}

// ---------------------------------------------------------------------------------------------
// UNIQUE

#[tokio::test]
async fn unique_rejects_another_row_with_the_same_key() {
    let (rejected, rows) = apply(vec![
        parent(1, Some("a"), None),
        parent(2, Some("a"), None),
        parent(3, Some("b"), None),
    ])
    .await;
    assert_eq!(rejected, vec![(2, unique("parents_code"))]);
    assert_eq!(keys_of(&rows, PARENTS), ids([1, 3]));
}

/// The row an operation writes is not another row: an upsert or an update of a row keeps its
/// own key.
#[tokio::test]
async fn a_row_keeps_its_own_unique_key() {
    let (rejected, rows) = apply(vec![
        parent(1, Some("a"), Some(1.5)),
        parent(1, Some("a"), Some(1.5)),
        update(PARENTS, vec![Value::Int(1)], vec![(1, s("a"))]),
        update(
            PARENTS,
            vec![Value::Int(1)],
            vec![(1, s("a")), (2, Value::Float(1.5))],
        ),
    ])
    .await;
    assert_eq!(rejected, vec![]);
    assert_eq!(keys_of(&rows, PARENTS), ids([1]));
}

#[tokio::test]
async fn an_update_that_takes_the_unique_key_of_another_row_is_rejected() {
    let (rejected, rows) = apply(vec![
        parent(1, Some("a"), None),
        parent(2, Some("b"), None),
        update(PARENTS, vec![Value::Int(2)], vec![(1, s("a"))]),
        update(PARENTS, vec![Value::Int(2)], vec![(1, s("c"))]),
    ])
    .await;
    assert_eq!(rejected, vec![(3, unique("parents_code"))]);
    let row = row_of(&rows, PARENTS, vec![Value::Int(2)]).unwrap();
    assert_eq!(row.get(1), Some(&s("c")));
}

/// A key with a `NULL` is not checked: several rows may have it, a composite key with a single
/// `NULL` included.
#[tokio::test]
async fn unique_keys_with_null_are_not_checked() {
    let (rejected, rows) = apply(vec![
        parent(1, None, None),
        parent(2, None, None),
        ranked(3, None, Some(1)),
        ranked(4, None, Some(1)),
        ranked(5, Some(1.5), None),
        ranked(6, Some(1.5), None),
        update(PARENTS, vec![Value::Int(6)], vec![(1, Value::Null)]),
    ])
    .await;
    assert_eq!(rejected, vec![]);
    assert_eq!(keys_of(&rows, PARENTS), ids(1..=6));
}

/// A composite `UNIQUE` key compares every value, floats as values: 0.0 and -0.0 are the same,
/// NaNs with the same bits are the same, and NaNs with different bits are not.
#[tokio::test]
async fn composite_unique_keys_compare_every_value_as_a_value() {
    let nan = f64::NAN;
    let other_nan = f64::from_bits(nan.to_bits() | 1);
    let (rejected, rows) = apply(vec![
        ranked(1, Some(0.0), Some(1)),
        ranked(2, Some(-0.0), Some(1)),
        ranked(3, Some(0.0), Some(2)),
        ranked(4, Some(nan), Some(1)),
        ranked(5, Some(other_nan), Some(1)),
        ranked(6, Some(nan), Some(1)),
        update(PARENTS, vec![Value::Int(3)], vec![(3, Value::Int(1))]),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (2, unique("parents_rank_score")),
            (6, unique("parents_rank_score")),
            (7, unique("parents_rank_score")),
        ]
    );
    assert_eq!(keys_of(&rows, PARENTS), ids([1, 3, 4, 5]));
}

/// Under a schema that added `posts.tag` with `UNIQUE (tag)`, rows written without the column
/// read `NULL` there, and an update widens a short row before computing its keys.
#[tokio::test]
async fn short_rows_read_null_and_are_widened_before_the_check() {
    let short_post = |id: i64| insert(POSTS, vec![Value::Int(id)], vec![Value::Int(id), s("A")]);
    let short_parent = |id: i64| insert(PARENTS, vec![Value::Int(id)], vec![Value::Int(id)]);
    let set_tag = |id: i64, tag: i64| {
        update(
            POSTS,
            vec![Value::Int(id)],
            vec![(1, s("A")), (4, Value::Int(tag))],
        )
    };
    let (rejected, rows) = apply_in_both_engines(
        &oracle::evolved_schema(),
        &one_per_batch(vec![
            short_post(1),
            short_post(2),
            set_tag(1, 7),
            set_tag(2, 7),
            short_parent(1),
            parent(2, Some("a"), None),
            update(PARENTS, vec![Value::Int(1)], vec![(1, s("a"))]),
        ]),
    )
    .await;
    assert_eq!(
        rejected,
        vec![(4, unique("posts_tag")), (7, unique("parents_code"))]
    );
    assert_eq!(
        row_of(&rows, POSTS, vec![Value::Int(2)]).unwrap().len(),
        2,
        "a rejected update does not widen the row"
    );
}

// ---------------------------------------------------------------------------------------------
// Foreign keys

#[tokio::test]
async fn a_foreign_key_needs_its_parent() {
    let (rejected, rows) = apply(vec![
        child((1, 1), Some(5), (None, None), None),
        parent(5, None, None),
        child((1, 1), Some(5), (None, None), None),
        update(CHILDREN, child_key(1, 1), vec![(2, Value::Int(6))]),
        update(CHILDREN, child_key(1, 1), vec![(2, Value::Null)]),
        update(CHILDREN, child_key(1, 1), vec![(2, Value::Int(5))]),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (1, foreign_key("children_parent")),
            (4, foreign_key("children_parent"))
        ]
    );
    let row = row_of(&rows, CHILDREN, child_key(1, 1)).unwrap();
    assert_eq!(row.get(2), Some(&Value::Int(5)));
}

/// A foreign key with a `NULL` is not checked, a composite one with a single `NULL` included.
#[tokio::test]
async fn foreign_keys_with_null_are_not_checked() {
    let (rejected, rows) = apply(vec![
        child((1, 1), None, (None, None), None),
        child((1, 2), None, (Some(9), None), None),
        child((1, 3), None, (None, Some(9)), None),
    ])
    .await;
    assert_eq!(rejected, vec![]);
    assert_eq!(keys_of(&rows, CHILDREN).len(), 3);
}

/// A row may reference itself through a foreign key of its own table, on insert and update; a
/// composite key matches the parent's key in the order the foreign key declares its columns.
#[tokio::test]
async fn a_row_may_reference_itself() {
    let (rejected, rows) = apply(vec![
        child((1, 2), None, (Some(1), Some(2)), None),
        child((2, 1), None, (Some(1), Some(2)), None),
        child((3, 3), None, (Some(2), Some(2)), None),
        child((4, 4), None, (None, None), None),
        update(
            CHILDREN,
            child_key(4, 4),
            vec![(3, Value::Int(4)), (4, Value::Int(4))],
        ),
        child((5, 6), None, (Some(6), Some(5)), None),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (3, foreign_key("children_up")),
            (6, foreign_key("children_up"))
        ]
    );
    assert_eq!(keys_of(&rows, CHILDREN).len(), 3);
}

/// Keys compare as values: 0.0 and -0.0 are the same key, NaNs with different bits are not.
#[tokio::test]
async fn foreign_keys_compare_keys_as_values() {
    let nan = f64::NAN;
    let other_nan = f64::from_bits(nan.to_bits() | 1);
    let (rejected, _) = apply(vec![
        note(0.0, None),
        note(nan, None),
        post(1, "A", None, Some(-0.0)),
        post(2, "A", None, Some(nan)),
        post(3, "A", None, Some(other_nan)),
    ])
    .await;
    assert_eq!(rejected, vec![(5, foreign_key("posts_note"))]);
}

// ---------------------------------------------------------------------------------------------
// RESTRICT

#[tokio::test]
async fn a_referenced_row_cannot_be_deleted() {
    let (rejected, rows) = apply(vec![
        parent(1, None, None),
        child((1, 1), Some(1), (None, None), None),
        delete(PARENTS, vec![Value::Int(1)]),
        update(CHILDREN, child_key(1, 1), vec![(2, Value::Null)]),
        delete(PARENTS, vec![Value::Int(1)]),
    ])
    .await;
    assert_eq!(rejected, vec![(3, restrict("children_parent"))]);
    assert_eq!(keys_of(&rows, PARENTS), vec![]);
}

/// The row being deleted does not count as its own child; any other row of the same table
/// does. A delete of a row that does not exist reports nothing.
#[tokio::test]
async fn a_row_referenced_only_by_itself_can_be_deleted() {
    let (rejected, rows) = apply(vec![
        child((1, 1), None, (Some(1), Some(1)), None),
        child((2, 2), None, (Some(2), Some(2)), None),
        child((3, 3), None, (Some(2), Some(2)), None),
        delete(CHILDREN, child_key(1, 1)),
        delete(CHILDREN, child_key(2, 2)),
        delete(CHILDREN, child_key(9, 9)),
    ])
    .await;
    assert_eq!(rejected, vec![(5, restrict("children_up"))]);
    assert_eq!(keys_of(&rows, CHILDREN).len(), 2);
}

#[tokio::test]
async fn restrict_compares_keys_as_values() {
    let (rejected, _) = apply(vec![
        note(0.0, None),
        post(1, "A", None, Some(0.0)),
        delete(NOTES, vec![Value::Float(-0.0)]),
    ])
    .await;
    assert_eq!(rejected, vec![(3, restrict("posts_note"))]);
}

/// Only a delete checks the rows that reference it: an upsert keeps the key they reference.
#[tokio::test]
async fn an_upsert_of_a_referenced_row_is_applied() {
    let (rejected, rows) = apply(vec![
        parent(1, None, None),
        child((1, 1), Some(1), (None, None), None),
        parent(1, Some("new"), None),
    ])
    .await;
    assert_eq!(rejected, vec![]);
    let row = row_of(&rows, PARENTS, vec![Value::Int(1)]).unwrap();
    assert_eq!(row.get(1), Some(&s("new")));
}

/// The lookups find every row with a key, whatever its primary key sorts after: negative keys,
/// and keys of the referencing table that sort before the referenced key.
#[tokio::test]
async fn lookups_find_rows_with_any_primary_key() {
    let (rejected, _) = apply(vec![
        parent(-5, Some("a"), None),
        parent(1, Some("a"), None),
        parent(i64::MIN, None, None),
        child((i64::MIN, -1), Some(i64::MIN), (None, None), None),
        delete(PARENTS, vec![Value::Int(i64::MIN)]),
        child((-3, -3), None, (Some(i64::MIN), Some(-1)), None),
        delete(CHILDREN, child_key(i64::MIN, -1)),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (2, unique("parents_code")),
            (5, restrict("children_parent")),
            (7, restrict("children_up")),
        ]
    );
}

/// A foreign key to another table that equals the row's own key is no self-reference: the
/// parent must exist, and the parent cannot be deleted while the row references it.
#[tokio::test]
async fn a_key_that_is_also_a_foreign_key_to_another_table_needs_its_parent() {
    let profile = |id: i64| {
        insert(
            PROFILES,
            vec![Value::Int(id)],
            vec![Value::Int(id), Value::Null],
        )
    };
    let (rejected, rows) = apply(vec![
        profile(5),
        parent(5, None, None),
        profile(5),
        delete(PARENTS, vec![Value::Int(5)]),
        delete(PROFILES, vec![Value::Int(5)]),
        delete(PARENTS, vec![Value::Int(5)]),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (1, foreign_key("profiles_parent")),
            (4, restrict("profiles_parent")),
        ]
    );
    assert!(rows.values().all(|table| table.is_empty()));
}

// ---------------------------------------------------------------------------------------------
// Order of the log

/// Within a batch, each operation sees the state the previous one left.
#[tokio::test]
async fn a_parent_and_its_child_in_one_batch() {
    let (rejected, rows) = in_one_batch(vec![
        parent(1, None, None),
        child((1, 1), Some(1), (None, None), None),
    ])
    .await;
    assert_eq!(rejected, vec![]);
    assert_eq!(keys_of(&rows, CHILDREN).len(), 1);

    let (rejected, rows) = in_one_batch(vec![
        child((1, 1), Some(1), (None, None), None),
        parent(1, None, None),
    ])
    .await;
    assert_eq!(rejected, vec![(1, foreign_key("children_parent"))]);
    assert_eq!(keys_of(&rows, CHILDREN).len(), 0);
    assert_eq!(keys_of(&rows, PARENTS), ids([1]));
}

/// A client deletes a parent while another inserts a child of it: the order the server gives
/// them decides, the same way in every replica.
#[tokio::test]
async fn a_concurrent_delete_of_a_parent_and_insert_of_a_child() {
    let create = parent(1, None, None);
    let remove = delete(PARENTS, vec![Value::Int(1)]);
    let add = child((1, 1), Some(1), (None, None), None);

    let (rejected, rows) = apply(vec![create.clone(), remove.clone(), add.clone()]).await;
    assert_eq!(rejected, vec![(3, foreign_key("children_parent"))]);
    assert!(rows.values().all(|table| table.is_empty()));

    let (rejected, rows) = apply(vec![create, add, remove]).await;
    assert_eq!(rejected, vec![(3, restrict("children_parent"))]);
    assert_eq!(keys_of(&rows, PARENTS), ids([1]));
    assert_eq!(keys_of(&rows, CHILDREN).len(), 1);
}

/// `posts` and `notes` reference each other: whichever is inserted first must not reference
/// the other yet.
#[tokio::test]
async fn a_cycle_between_tables_is_decided_by_order() {
    let (rejected, rows) = apply(vec![
        post(1, "A", None, Some(1.5)),
        note(1.5, Some(1)),
        note(1.5, None),
        post(1, "A", None, Some(1.5)),
        note(1.5, Some(1)),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (1, foreign_key("posts_note")),
            (2, foreign_key("notes_post"))
        ]
    );
    assert_eq!(keys_of(&rows, POSTS), ids([1]));
    let row = row_of(&rows, NOTES, vec![Value::Float(1.5)]).unwrap();
    assert_eq!(row.get(1), Some(&Value::Int(1)));
}

// ---------------------------------------------------------------------------------------------
// Immutable tables and owners

#[tokio::test]
async fn an_immutable_row_is_inserted_once() {
    let (rejected, rows) = apply(vec![
        parent(1, None, None),
        parent(2, None, None),
        vote("A", 1, 1),
        vote("A", 1, 2),
        vote("A", 2, 2),
        vote("B", 1, 3),
        entry(1, "A", Some(10)),
        entry(1, "A", Some(20)),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![(4, RejectReason::Duplicate), (8, RejectReason::Duplicate)]
    );
    let first = row_of(&rows, VOTES, vec![s("A"), Value::Int(1)]).unwrap();
    assert_eq!(first.get(2), Some(&Value::Int(1)), "the first vote stays");
    assert_eq!(keys_of(&rows, VOTES).len(), 3);
}

/// With the owner outside the key, nobody inserts over, updates or deletes the row of another
/// owner, and an update cannot hand a row over; the owner does all three. With the owner in the
/// key, the key already tells rows of different owners apart.
#[tokio::test]
async fn only_the_owner_writes_its_row() {
    let key = || vec![Value::Int(1)];
    let (rejected, rows) = apply(vec![
        parent(7, None, None),
        post(1, "A", Some("x"), None),
        post(1, "B", Some("y"), None),
        update(POSTS, key(), vec![(1, s("B")), (2, s("y"))]),
        delete_as(POSTS, key(), "B"),
        update(POSTS, key(), vec![(1, s("A")), (2, s("z"))]),
        post(1, "A", Some("w"), None),
        vote("A", 7, 1),
        vote("B", 7, 2),
    ])
    .await;
    assert_eq!(
        rejected,
        vec![
            (3, RejectReason::NotOwner),
            (4, RejectReason::NotOwner),
            (5, RejectReason::NotOwner),
        ]
    );
    let row = row_of(&rows, POSTS, key()).unwrap();
    assert_eq!(row.get(2), Some(&s("w")));
    assert_eq!(keys_of(&rows, VOTES).len(), 2);

    let (rejected, rows) = apply(vec![
        post(1, "A", None, None),
        update(POSTS, key(), vec![(1, s("B"))]),
        delete_as(POSTS, key(), "A"),
        post(1, "B", None, None),
    ])
    .await;
    assert_eq!(rejected, vec![(2, RejectReason::NotOwner)]);
    let row = row_of(&rows, POSTS, key()).unwrap();
    assert_eq!(row.get(1), Some(&s("B")), "a deleted row has no owner left");
}

// ---------------------------------------------------------------------------------------------
// The reason reported when several rules fail

#[tokio::test]
async fn the_first_failing_rule_is_reported() {
    let cases: Vec<(&str, Schema, Vec<Operation>, RejectReason)> = vec![
        (
            "a missing row before its owner",
            oracle::schema(),
            vec![update(POSTS, vec![Value::Int(1)], vec![(1, s("B"))])],
            RejectReason::MissingRow,
        ),
        (
            "an existing immutable key before its owner",
            oracle::schema(),
            vec![entry(1, "A", None), entry(1, "B", None)],
            RejectReason::Duplicate,
        ),
        (
            "the owner before UNIQUE",
            oracle::schema(),
            vec![
                post(1, "A", Some("x"), None),
                post(2, "A", Some("y"), None),
                post(1, "B", Some("y"), None),
            ],
            RejectReason::NotOwner,
        ),
        (
            "the owner before the foreign keys",
            oracle::schema(),
            vec![post(1, "A", None, None), post(1, "B", None, Some(9.0))],
            RejectReason::NotOwner,
        ),
        (
            "an existing immutable key before UNIQUE",
            oracle::schema(),
            vec![
                entry(1, "A", Some(1)),
                entry(2, "A", Some(2)),
                entry(1, "A", Some(2)),
            ],
            RejectReason::Duplicate,
        ),
        (
            "UNIQUE in declaration order",
            oracle::schema(),
            vec![
                insert(
                    PARENTS,
                    vec![Value::Int(1)],
                    vec![Value::Int(1), s("a"), Value::Float(1.5), Value::Int(1)],
                ),
                insert(
                    PARENTS,
                    vec![Value::Int(2)],
                    vec![Value::Int(2), s("a"), Value::Float(1.5), Value::Int(1)],
                ),
            ],
            unique("parents_code"),
        ),
        (
            "UNIQUE before the foreign keys, declared earlier",
            oracle::schema(),
            vec![
                child((1, 1), None, (None, None), Some(1)),
                child((2, 2), Some(9), (Some(9), Some(9)), Some(1)),
            ],
            unique("children_x"),
        ),
        (
            "the foreign keys in declaration order",
            oracle::schema(),
            vec![child((1, 1), Some(9), (Some(9), Some(9)), None)],
            foreign_key("children_parent"),
        ),
        (
            "the foreign keys in declaration order, on an update",
            oracle::schema(),
            vec![
                child((1, 1), None, (None, None), None),
                update(
                    CHILDREN,
                    child_key(1, 1),
                    vec![(2, Value::Int(9)), (3, Value::Int(9)), (4, Value::Int(9))],
                ),
            ],
            foreign_key("children_parent"),
        ),
        (
            "the owner before RESTRICT",
            oracle::schema(),
            vec![
                post(1, "A", None, None),
                note(1.5, Some(1)),
                delete_as(POSTS, vec![Value::Int(1)], "B"),
            ],
            RejectReason::NotOwner,
        ),
        (
            "RESTRICT by id of the referencing table, not by order of insertion",
            oracle::schema(),
            vec![
                parent(1, None, None),
                vote("A", 1, 1),
                child((1, 1), Some(1), (None, None), None),
                delete(PARENTS, vec![Value::Int(1)]),
            ],
            restrict("children_parent"),
        ),
        (
            "RESTRICT through a foreign key of another table",
            oracle::schema(),
            vec![
                parent(1, None, None),
                vote("A", 1, 1),
                delete(PARENTS, vec![Value::Int(1)]),
            ],
            restrict("votes_poll"),
        ),
        (
            "RESTRICT in declaration order within the referencing table",
            oracle::evolved_schema(),
            vec![
                parent(1, None, None),
                insert(
                    CHILDREN,
                    child_key(1, 1),
                    vec![
                        Value::Int(1),
                        Value::Int(1),
                        Value::Int(1),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int(1),
                    ],
                ),
                delete(PARENTS, vec![Value::Int(1)]),
            ],
            restrict("children_parent"),
        ),
        (
            "a later foreign key of the referencing table when the first does not reference",
            oracle::evolved_schema(),
            vec![
                parent(1, None, None),
                insert(
                    CHILDREN,
                    child_key(1, 1),
                    vec![
                        Value::Int(1),
                        Value::Int(1),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int(1),
                    ],
                ),
                delete(PARENTS, vec![Value::Int(1)]),
            ],
            restrict("children_alt"),
        ),
    ];
    for (case, schema, ops, reason) in cases {
        let last = ops.len() as u64;
        let (rejected, _) = apply_in_both_engines(&schema, &one_per_batch(ops)).await;
        assert_eq!(rejected, vec![(last, reason)], "{case}");
    }
}

// ---------------------------------------------------------------------------------------------
// Random logs

/// A random log of 30 to 89 operations under [`oracle::schema`], in batches of one to eight.
fn random_batches(rng: &mut Rng) -> Vec<Vec<SequencedOperation>> {
    let len = 30 + rng.below(60);
    let log = oracle::random_log(rng, &oracle::schema(), 1, len);
    let mut batches = Vec::new();
    let mut rest = log.as_slice();
    while !rest.is_empty() {
        let len = (1 + rng.below(8) as usize).min(rest.len());
        batches.push(rest[..len].to_vec());
        rest = &rest[len..];
    }
    batches
}

/// The state with a rejected operation is the state without it: for a few operations of random
/// logs rejected by a constraint, both engines applying the log without it (renumbered) end with the same rows
/// and reject the same later operations.
#[tokio::test]
async fn a_rejected_operation_leaves_no_trace() {
    let mut checked = 0;
    for seed in 0..4 {
        let mut rng = Rng::new(seed * 31 + 5);
        let batches = random_batches(&mut rng);
        let (rejected, rows) = apply_in_both_engines(&oracle::schema(), &batches).await;
        // The constraints' rejections: an update of a missing row is not one of them.
        let removable = rejected
            .iter()
            .filter(|(_, reason)| *reason != RejectReason::MissingRow)
            .take(3);
        for &(removed, _) in removable {
            let without: Vec<Vec<SequencedOperation>> = batches
                .iter()
                .map(|batch| {
                    batch
                        .iter()
                        .filter(|op| op.seq.get() != removed)
                        .map(|op| {
                            let seq = op.seq.get() - u64::from(op.seq.get() > removed);
                            SequencedOperation::new(seq, op.op.clone())
                        })
                        .collect::<Vec<_>>()
                })
                .filter(|batch| !batch.is_empty())
                .collect();
            let (rejected_without, rows_without) =
                apply_in_both_engines(&oracle::schema(), &without).await;
            assert_eq!(
                oracle::dump(&rows_without),
                oracle::dump(&rows),
                "seed {seed}: without {removed}"
            );
            let expected: Vec<(u64, RejectReason)> = rejected
                .iter()
                .filter(|(seq, _)| *seq != removed)
                .map(|(seq, reason)| (seq - u64::from(*seq > removed), reason.clone()))
                .collect();
            assert_eq!(rejected_without, expected, "seed {seed}: without {removed}");
            checked += 1;
        }
    }
    assert!(checked > 0, "no random log broke a constraint");
}

/// Applies a random log to the reference model, a memory engine and a disk engine, and checks
/// that they report the same rejections for every batch and end with the same rows.
///
/// The log switches to [`oracle::evolved_schema`] at a random point: the model and the disk
/// engine (which reopens with it) change schema there, and the memory engine applies the whole
/// log under the later version. The disk engine also compacts and reopens at random. Rows are
/// compared without their trailing nulls, which depend on the width an update widens a row to.
async fn check_against_the_model(seed: u64) {
    let case = format!("seed {seed}");
    let mut rng = Rng::new(seed.wrapping_mul(0x9E37) + 1);
    let room = RoomId::new("model").unwrap();
    let mut model = Oracle::new(oracle::schema());
    let memory = MemoryStorageEngine::new();
    memory
        .open_room(&room, oracle::evolved_schema())
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut disk = disk_engine(dir.path());
    disk.open_room(&room, oracle::schema()).await.unwrap();

    let len = 40 + rng.below(100);
    let evolve_at = rng.below(len);
    let mut rejected_any = false;
    while model.head() < len {
        if model.head() >= evolve_at && model.schema() != &oracle::evolved_schema() {
            model.evolve(oracle::evolved_schema());
            disk.close_room(&room).await.unwrap();
            disk = disk_engine(dir.path());
            disk.open_room(&room, oracle::evolved_schema())
                .await
                .unwrap();
        }
        match rng.below(20) {
            0 => disk.compact_room(&room).await.unwrap(),
            1 => {
                disk.close_room(&room).await.unwrap();
                disk = disk_engine(dir.path());
                disk.open_room(&room, model.schema().clone()).await.unwrap();
            }
            _ => {}
        }
        let size = 1 + rng.below(8);
        let batch = oracle::random_log(&mut rng, model.schema(), model.head() + 1, size);
        let expected = reject_reasons(model.apply_batch(&batch));
        rejected_any |= !expected.is_empty();
        for (name, engine) in [
            ("memory", &memory as &dyn StorageEngine),
            ("disk", &disk as &dyn StorageEngine),
        ] {
            let report = engine.apply_batch(&room, batch.clone()).await.unwrap();
            assert_eq!(
                report.rejected(),
                expected,
                "{case}: {name}, batch at {}",
                batch[0].seq
            );
            assert_eq!(report.head_seq().get(), model.head(), "{case}: {name}");
        }
    }
    assert!(rejected_any, "{case}: the log rejected nothing");

    let schema = oracle::evolved_schema();
    let want = trimmed_dump(model.rows());
    assert_eq!(
        trimmed_dump(&room_rows(&memory, &room, &schema).await),
        want,
        "{case}: memory"
    );
    assert_eq!(
        trimmed_dump(&room_rows(&disk, &room, &schema).await),
        want,
        "{case}: disk"
    );
    disk.close_room(&room).await.unwrap();
    let reopened = disk_engine(dir.path());
    reopened.open_room(&room, schema.clone()).await.unwrap();
    assert_eq!(
        trimmed_dump(&room_rows(&reopened, &room, &schema).await),
        want,
        "{case}: disk reopened"
    );
}

#[tokio::test]
async fn both_engines_decide_like_the_reference_model() {
    for seed in 0..12 {
        check_against_the_model(seed).await;
    }
}

#[tokio::test]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn both_engines_decide_like_the_reference_model_over_many_logs() {
    for seed in 0..400 {
        check_against_the_model(1000 + seed).await;
    }
}

/// The random logs of the reference model break every rule, through every constraint of the
/// schema, and write what the rules treat apart: keys with `NULL`, short rows, rows that
/// reference themselves, -0.0 and NaN, and deletes that declare an owner. Otherwise the tests
/// against the model would not tell much.
#[test]
fn random_logs_break_every_rule() {
    let schema = oracle::evolved_schema();
    let mut reasons = std::collections::BTreeSet::new();
    let (mut nulls, mut short, mut itself, mut zeros, mut nans, mut owned_deletes) =
        (0, 0, 0, 0, 0, 0);
    for seed in 0..60 {
        let mut rng = Rng::new(seed);
        let mut model = Oracle::new(schema.clone());
        let log = oracle::random_log(&mut rng, &schema, 1, 150);
        for op in &log {
            match op.op.kind() {
                zemdb_core::OperationKind::Insert { row } => {
                    let table = schema.get_table_by_id(op.op.table_id()).unwrap();
                    short += usize::from(row.len() < table.columns().len());
                    nulls += row.iter().filter(|value| value.is_null()).count();
                    for value in row.iter() {
                        if let Value::Float(f) = value {
                            zeros += usize::from(f.to_bits() == (-0.0f64).to_bits());
                            nans += usize::from(f.is_nan());
                        }
                    }
                    if op.op.table_id() == CHILDREN
                        && row.get(4) == op.op.pk().get(0)
                        && row.get(3) == op.op.pk().get(1)
                    {
                        itself += 1;
                    }
                }
                zemdb_core::OperationKind::Delete { owner: Some(_) } => owned_deletes += 1,
                _ => {}
            }
        }
        for (_, reason) in model.apply_batch(&log) {
            reasons.insert(format!("{reason:?}"));
        }
    }
    let mut expected = vec![
        "MissingRow".to_string(),
        "Duplicate".to_string(),
        "NotOwner".to_string(),
    ];
    for table in schema.tables() {
        for unique in table.unique_constraints() {
            expected.push(format!("Unique({:?})", unique.name()));
        }
        for foreign_key in table.foreign_keys() {
            expected.push(format!("ForeignKey({:?})", foreign_key.name()));
            expected.push(format!("Restrict({:?})", foreign_key.name()));
        }
    }
    let missing: Vec<&String> = expected.iter().filter(|e| !reasons.contains(*e)).collect();
    assert!(missing.is_empty(), "never rejected for {missing:?}");
    for (what, count) in [
        ("nulls", nulls),
        ("short rows", short),
        ("rows referencing themselves", itself),
        ("-0.0", zeros),
        ("NaN", nans),
        ("deletes with an owner", owned_deletes),
    ] {
        assert!(count > 0, "the random logs write no {what}");
    }
}
