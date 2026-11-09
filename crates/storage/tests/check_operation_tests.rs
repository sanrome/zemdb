//! Checking an operation without applying it (`StorageEngine::check_operation`, which the
//! precheck of the client asks): in both engines, it gives the decision that `apply_batch` then
//! takes, for every rule, and the decision of the reference model on random states, with
//! reloads, compactions and reopens; and it changes nothing.
//!
//! The ignored variant runs many more logs: `cargo test --release -- --ignored`.

mod common;

use common::oracle::{self, Oracle, CHILDREN, LEDGER, PARENTS, POSTS, VOTES};
use common::rng::Rng;
use common::{disk_engine, reject_reason, reject_reasons, room_rows};
use tempfile::TempDir;
use zemdb_core::{
    ColumnUpdate, CompactRow, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, Value,
};
use zemdb_storage::{
    DiskStorageEngine, MemoryStorageEngine, RejectReason, StorageEngine, StorageError,
};

// ---------------------------------------------------------------------------------------------
// Helpers

/// A memory engine and a disk engine, each with the same room open under the same schema.
struct Engines {
    room: RoomId,
    memory: MemoryStorageEngine,
    disk: DiskStorageEngine,
    _dir: TempDir,
}

impl Engines {
    /// Both engines with the room open under `schema`, and `ops` applied in order, one per
    /// batch, from sequence 1 on. None of `ops` may be rejected.
    async fn open(schema: &Schema, ops: Vec<Operation>) -> Self {
        let room = RoomId::new("checked").unwrap();
        let memory = MemoryStorageEngine::new();
        memory.open_room(&room, schema.clone()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let disk = disk_engine(dir.path());
        disk.open_room(&room, schema.clone()).await.unwrap();
        let engines = Self {
            room,
            memory,
            disk,
            _dir: dir,
        };
        for op in ops {
            for (name, engine) in engines.each() {
                assert_eq!(
                    apply(engine, &engines.room, op.clone()).await,
                    None,
                    "{name}"
                );
            }
        }
        engines
    }

    fn each(&self) -> [(&'static str, &dyn StorageEngine); 2] {
        [("memory", &self.memory), ("disk", &self.disk)]
    }
}

/// Applies `op` to `room` as the next operation, in a batch of its own, and returns the reason
/// `apply_batch` rejected it for, if it did.
async fn apply(engine: &dyn StorageEngine, room: &RoomId, op: Operation) -> Option<RejectReason> {
    let head = engine.get_head_seq(room).await.unwrap().get();
    let batch = vec![SequencedOperation::new(head + 1, op)];
    let report = engine.apply_batch(room, batch).await.unwrap();
    assert_eq!(report.head_seq().get(), head + 1);
    report.rejected().first().map(|(_, reason)| reason.clone())
}

fn s(text: &str) -> Value {
    Value::String(text.into())
}

fn int(value: Option<i64>) -> Value {
    value.map_or(Value::Null, Value::Int)
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

/// A row of `parents (id, code, score, rank)` with only a code.
fn parent(id: i64, code: Option<&str>) -> Operation {
    let code = code.map_or(Value::Null, s);
    insert(
        PARENTS,
        vec![Value::Int(id)],
        vec![Value::Int(id), code, Value::Null, Value::Null],
    )
}

/// A row of `children (a, b, parent, up_a, up_b, x)` with only a parent.
fn child(a: i64, b: i64, parent: Option<i64>) -> Operation {
    let (a, b) = (Value::Int(a), Value::Int(b));
    let null = Value::Null;
    insert(
        CHILDREN,
        vec![a.clone(), b.clone()],
        vec![a, b, int(parent), null.clone(), null.clone(), null],
    )
}

/// A row of `votes (voter, poll, choice)`.
fn vote(voter: &str, poll: i64) -> Operation {
    insert(
        VOTES,
        vec![s(voter), Value::Int(poll)],
        vec![s(voter), Value::Int(poll), Value::Int(1)],
    )
}

/// A row of `posts (id, author, slug, note)` with only an author.
fn post(id: i64, author: &str) -> Operation {
    insert(
        POSTS,
        vec![Value::Int(id)],
        vec![Value::Int(id), s(author), Value::Null, Value::Null],
    )
}

/// A row of `ledger (id, holder, amount)` without an amount.
fn entry(id: i64, holder: &str) -> Operation {
    insert(
        LEDGER,
        vec![Value::Int(id)],
        vec![Value::Int(id), s(holder), Value::Null],
    )
}

fn unique(name: &str) -> Option<RejectReason> {
    Some(RejectReason::Unique {
        constraint: name.into(),
    })
}

fn foreign_key(name: &str) -> Option<RejectReason> {
    Some(RejectReason::ForeignKey {
        constraint: name.into(),
    })
}

fn restrict(name: &str) -> Option<RejectReason> {
    Some(RejectReason::Restrict {
        constraint: name.into(),
    })
}

// ---------------------------------------------------------------------------------------------
// The rules

/// For each rule, a check gives the reason where `apply_batch` then rejects the operation for
/// it, with the same constraint, and `None` where it applies it; a delete of a row that does not
/// exist changes nothing and is not reported. In both engines.
#[tokio::test]
async fn each_rule_is_decided_as_apply_batch_decides_it() {
    let one = || vec![Value::Int(1)];
    let cases: Vec<(&str, Vec<Operation>, Operation, Option<RejectReason>)> = vec![
        (
            "an update of a row that does not exist",
            vec![],
            update(POSTS, one(), vec![(1, s("A"))]),
            Some(RejectReason::MissingRow),
        ),
        (
            "an existing key of an immutable table",
            vec![entry(1, "A")],
            entry(1, "A"),
            Some(RejectReason::Duplicate),
        ),
        (
            "another owner",
            vec![post(1, "A")],
            post(1, "B"),
            Some(RejectReason::NotOwner),
        ),
        (
            "another owner, on a delete",
            vec![post(1, "A")],
            delete_as(POSTS, one(), "B"),
            Some(RejectReason::NotOwner),
        ),
        (
            "UNIQUE",
            vec![parent(1, Some("a"))],
            parent(2, Some("a")),
            unique("parents_code"),
        ),
        (
            "a foreign key",
            vec![parent(1, None)],
            child(1, 1, Some(2)),
            foreign_key("children_parent"),
        ),
        (
            "RESTRICT",
            vec![parent(1, None), vote("A", 1)],
            delete(PARENTS, one()),
            restrict("votes_poll"),
        ),
        (
            "a delete of a row that does not exist",
            vec![],
            delete(PARENTS, one()),
            None,
        ),
        (
            "another UNIQUE key",
            vec![parent(1, Some("a"))],
            parent(2, Some("b")),
            None,
        ),
        (
            "a row's own UNIQUE key",
            vec![parent(1, Some("a"))],
            parent(1, Some("a")),
            None,
        ),
        (
            "a foreign key with its parent",
            vec![parent(1, None)],
            child(1, 1, Some(1)),
            None,
        ),
        (
            "a delete by the owner of an unreferenced row",
            vec![post(1, "A")],
            delete_as(POSTS, one(), "A"),
            None,
        ),
        (
            "an update by the owner",
            vec![post(1, "A")],
            update(POSTS, one(), vec![(1, s("A")), (2, s("slug"))]),
            None,
        ),
    ];
    for (case, before, op, expected) in cases {
        let engines = Engines::open(&oracle::schema(), before).await;
        for (name, engine) in engines.each() {
            let checked = engine.check_operation(&engines.room, &op).await.unwrap();
            assert_eq!(checked, expected, "{case}: {name}");
            let applied = apply(engine, &engines.room, op.clone()).await;
            assert_eq!(applied, checked, "{case}: {name}, apply_batch");
        }
    }
}

/// A check changes neither the head, nor the rows, nor the indexes: after checking an insert that
/// would be applied and a delete that would be rejected, `apply_batch` decides as if they had not
/// been checked.
#[tokio::test]
async fn a_check_changes_nothing() {
    let engines = Engines::open(&oracle::schema(), vec![parent(1, Some("a")), vote("A", 1)]).await;
    let schema = oracle::schema();
    for (name, engine) in engines.each() {
        let room = &engines.room;
        let head = engine.get_head_seq(room).await.unwrap();
        let rows = oracle::dump(&room_rows(engine, room, &schema).await);
        let applied = parent(2, Some("b"));
        let rejected = delete(PARENTS, vec![Value::Int(1)]);
        assert_eq!(engine.check_operation(room, &applied).await.unwrap(), None);
        assert_eq!(
            engine.check_operation(room, &rejected).await.unwrap(),
            restrict("votes_poll"),
            "{name}"
        );
        assert_eq!(engine.get_head_seq(room).await.unwrap(), head, "{name}");
        let after = oracle::dump(&room_rows(engine, room, &schema).await);
        assert_eq!(after, rows, "{name}");

        // Had the check inserted the parent 2, its code would be taken, and it would be there.
        assert_eq!(
            apply(engine, room, parent(3, Some("b"))).await,
            None,
            "{name}"
        );
        let orphan = child(1, 1, Some(2));
        assert_eq!(
            apply(engine, room, orphan).await,
            foreign_key("children_parent"),
            "{name}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Errors

/// An operation of a table the room does not have is `TableNotFound`, one that is not valid
/// under the room's schema is `SchemaValidation`, and a room that is not open, `RoomNotFound`.
#[tokio::test]
async fn invalid_operations_and_rooms_that_are_not_open_are_errors() {
    let engines = Engines::open(&oracle::schema(), vec![parent(1, None)]).await;
    let room = &engines.room;
    let other = RoomId::new("never-opened").unwrap();
    for (name, engine) in engines.each() {
        let no_table = insert(99, vec![Value::Int(1)], vec![Value::Int(1)]);
        let result = engine.check_operation(room, &no_table).await;
        assert!(
            matches!(result, Err(StorageError::TableNotFound { .. })),
            "{name}: {result:?}"
        );
        let wrong_key = insert(PARENTS, vec![s("1")], vec![s("1")]);
        let result = engine.check_operation(room, &wrong_key).await;
        assert!(
            matches!(result, Err(StorageError::SchemaValidation(_))),
            "{name}: {result:?}"
        );
        let result = engine.check_operation(&other, &parent(2, None)).await;
        assert!(
            matches!(result, Err(StorageError::RoomNotFound(_))),
            "{name}: {result:?}"
        );
    }
    for (name, engine) in engines.each() {
        engine.close_room(room).await.unwrap();
        let result = engine.check_operation(room, &parent(2, None)).await;
        assert!(
            matches!(result, Err(StorageError::RoomNotFound(_))),
            "{name}: {result:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Schema reloads and reopens

/// After `reload_schema`, a check decides with the new schema: an operation on a new column,
/// which was `SchemaValidation`, is decided by the constraints that come with it.
#[tokio::test]
async fn a_check_decides_with_the_reloaded_schema() {
    let engines = Engines::open(&oracle::schema(), vec![parent(1, None)]).await;
    let room = &engines.room;
    // `posts.tag`, with `UNIQUE (tag)`, and `children.alt`, with a foreign key to `parents`.
    let tagged = |id: i64| {
        insert(
            POSTS,
            vec![Value::Int(id)],
            vec![
                Value::Int(id),
                s("A"),
                Value::Null,
                Value::Null,
                Value::Int(7),
            ],
        )
    };
    let null = Value::Null;
    let with_alt = insert(
        CHILDREN,
        vec![Value::Int(1), Value::Int(1)],
        vec![
            Value::Int(1),
            Value::Int(1),
            null.clone(),
            null.clone(),
            null.clone(),
            null,
            Value::Int(2),
        ],
    );
    for (name, engine) in engines.each() {
        for op in [tagged(2), with_alt.clone()] {
            let result = engine.check_operation(room, &op).await;
            assert!(
                matches!(result, Err(StorageError::SchemaValidation(_))),
                "{name}: {result:?}"
            );
        }
        engine
            .reload_schema(room, oracle::evolved_schema())
            .await
            .unwrap();
        assert_eq!(
            engine.check_operation(room, &tagged(2)).await.unwrap(),
            None,
            "{name}"
        );
        assert_eq!(apply(engine, room, tagged(3)).await, None, "{name}");
        assert_eq!(
            engine.check_operation(room, &tagged(2)).await.unwrap(),
            unique("posts_tag"),
            "{name}"
        );
        assert_eq!(
            engine.check_operation(room, &with_alt).await.unwrap(),
            foreign_key("children_alt"),
            "{name}"
        );
    }
}

/// After `apply_snapshot`, a check decides on the rows of the snapshot, with its schema, and
/// the next batch decides the same. In both engines.
#[tokio::test]
async fn a_check_decides_on_an_applied_snapshot() {
    let source = Engines::open(&oracle::schema(), vec![parent(1, Some("a")), vote("A", 1)]).await;
    let snapshot = source.memory.create_snapshot(&source.room).await.unwrap();
    let head = SequenceNumber::from(2u64);
    let engines = Engines::open(&oracle::schema(), vec![]).await;
    let room = &engines.room;
    let cases = [
        (parent(3, Some("a")), unique("parents_code")),
        (delete(PARENTS, vec![Value::Int(1)]), restrict("votes_poll")),
        (vote("A", 1), Some(RejectReason::Duplicate)),
        (parent(3, Some("b")), None),
    ];
    // The tag of a post, a column of the snapshot's schema only.
    let tagged = insert(
        POSTS,
        vec![Value::Int(2)],
        vec![
            Value::Int(2),
            s("A"),
            Value::Null,
            Value::Null,
            Value::Int(7),
        ],
    );
    for (name, engine) in engines.each() {
        let schema = oracle::evolved_schema();
        let applied = engine
            .apply_snapshot(room, schema, snapshot.clone().into(), head)
            .await
            .unwrap();
        assert_eq!(applied, head, "{name}");
        for (op, expected) in &cases {
            let checked = engine.check_operation(room, op).await.unwrap();
            assert_eq!(&checked, expected, "{name}: {op:?}");
        }
        let checked = engine.check_operation(room, &tagged).await.unwrap();
        assert_eq!(checked, None, "{name}");
        for (op, expected) in cases.clone() {
            assert_eq!(apply(engine, room, op).await, expected, "{name}");
        }
    }
}

/// A disk room reopened from a snapshot and the WAL after it decides on the state it recovers.
#[tokio::test]
async fn a_reopened_disk_room_decides_on_the_state_it_recovers() {
    let room = RoomId::new("reopened").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let schema = oracle::schema();
    let engine = disk_engine(dir.path());
    engine.open_room(&room, schema.clone()).await.unwrap();
    let ops = [parent(1, Some("a")), vote("A", 1), parent(2, Some("b"))];
    for (seq, op) in (1..).zip(ops) {
        if seq == 3 {
            engine.compact_room(&room).await.unwrap();
        }
        let batch = vec![SequencedOperation::new(seq, op)];
        assert!(engine
            .apply_batch(&room, batch)
            .await
            .unwrap()
            .rejected()
            .is_empty());
    }
    engine.close_room(&room).await.unwrap();
    let reopened = disk_engine(dir.path());
    reopened.open_room(&room, schema).await.unwrap();

    let cases = [
        (parent(3, Some("a")), unique("parents_code")),
        (parent(3, Some("b")), unique("parents_code")),
        (parent(3, Some("c")), None),
        (delete(PARENTS, vec![Value::Int(1)]), restrict("votes_poll")),
        (delete(PARENTS, vec![Value::Int(2)]), None),
    ];
    for (op, expected) in cases {
        let checked = reopened.check_operation(&room, &op).await.unwrap();
        assert_eq!(checked, expected, "{op:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Random states, against the reference model

/// Runs a random log on the reference model, a memory engine and a disk engine, reloading both
/// engines to [`oracle::evolved_schema`] at a random point, and compacting and reopening the disk
/// engine at random. Before each batch, random operations are checked, and each engine gives
/// the decision of the model on the current state; the first operation of the batch is checked
/// too, and `apply_batch` then decides it the same. Returns how many checks were rejections.
async fn check_like_the_model(seed: u64) -> usize {
    let case = format!("seed {seed}");
    let mut rng = Rng::new(seed.wrapping_mul(0x5851) + 7);
    let room = RoomId::new("model").unwrap();
    let mut model = Oracle::new(oracle::schema());
    let memory = MemoryStorageEngine::new();
    memory.open_room(&room, oracle::schema()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut disk = disk_engine(dir.path());
    disk.open_room(&room, oracle::schema()).await.unwrap();

    let len = 40 + rng.below(80);
    let evolve_at = rng.below(len);
    let mut rejections = 0;
    while model.head() < len {
        if model.head() >= evolve_at && model.schema() != &oracle::evolved_schema() {
            model.evolve(oracle::evolved_schema());
            for engine in [&memory as &dyn StorageEngine, &disk] {
                engine
                    .reload_schema(&room, oracle::evolved_schema())
                    .await
                    .unwrap();
            }
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
        for _ in 0..4 {
            let op = oracle::random_operation(&mut rng, model.schema());
            let expected = model.decide(&op).map(reject_reason);
            rejections += usize::from(expected.is_some());
            for (name, engine) in [
                ("memory", &memory as &dyn StorageEngine),
                ("disk", &disk as &dyn StorageEngine),
            ] {
                let checked = engine.check_operation(&room, &op).await.unwrap();
                assert_eq!(checked, expected, "{case}: {name}, {op:?}");
            }
        }

        let size = 1 + rng.below(8);
        let batch = oracle::random_log(&mut rng, model.schema(), model.head() + 1, size);
        let first = batch[0].seq;
        let expected = reject_reasons(model.apply_batch(&batch));
        let decided = expected
            .iter()
            .find(|(seq, _)| *seq == first)
            .map(|(_, reason)| reason.clone());
        for (name, engine) in [
            ("memory", &memory as &dyn StorageEngine),
            ("disk", &disk as &dyn StorageEngine),
        ] {
            let checked = engine.check_operation(&room, &batch[0].op).await.unwrap();
            assert_eq!(
                checked, decided,
                "{case}: {name}, first of the batch at {first}"
            );
            let report = engine.apply_batch(&room, batch.clone()).await.unwrap();
            assert_eq!(
                report.rejected(),
                expected,
                "{case}: {name}, batch at {first}"
            );
            assert_eq!(report.head_seq(), SequenceNumber::from(model.head()));
        }
    }
    rejections
}

#[tokio::test]
async fn both_engines_check_like_the_reference_model() {
    let mut rejections = 0;
    for seed in 0..12 {
        rejections += check_like_the_model(seed).await;
    }
    assert!(rejections > 0, "no check was a rejection");
}

#[tokio::test]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn both_engines_check_like_the_reference_model_over_many_logs() {
    for seed in 0..400 {
        check_like_the_model(1000 + seed).await;
    }
}
