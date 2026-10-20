//! Replicas that apply the same prefix of the log converge, bit for bit, whether they apply every
//! operation or restore a snapshot taken at any point of it, by either engine, and apply the
//! rest; a disk replica holds the same after a compaction and a reopen. With integrity
//! constraints, they also reject the same operations for the same reasons, as the reference
//! model does.

mod common;

use common::oracle::{self, Oracle};
use common::rng::Rng;
use common::{reject_reasons, room_rows};
use futures::StreamExt;
use zemdb_core::{
    ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, RejectReason, ScanOptions,
    StorageEngine,
};

const ITEMS: u16 = 0;
const NOTES: u16 = 1;

/// `items`: a composite key (a float and an integer) and three nullable columns. `notes`: an
/// integer key, a required string and a nullable float.
fn schema() -> Schema {
    let items = TableSchema::builder("items")
        .table_id(ITEMS)
        .primary_key("k", DataType::Float)
        .primary_key("n", DataType::Int)
        .nullable_column("a", DataType::String)
        .nullable_column("b", DataType::Int)
        .nullable_column("c", DataType::Float);
    let notes = TableSchema::builder("notes")
        .table_id(NOTES)
        .primary_key("id", DataType::Int)
        .column("text", DataType::String)
        .nullable_column("w", DataType::Float);
    Schema::builder().table(items).table(notes).build()
}

/// A float whose bits matter: both zeros, NaNs with different bits, and two plain numbers.
/// Built as `Value::Float` directly, since `From<f64>` canonicalizes -0.0.
fn float_value(rng: &mut Rng) -> Value {
    Value::Float(match rng.below(7) {
        0 => 0.0,
        1 => -0.0,
        2 => f64::NAN,
        3 => f64::from_bits(f64::NAN.to_bits() | 1),
        4 => -f64::NAN,
        5 => 1.5,
        _ => -2.25,
    })
}

/// An insert, update or delete of a few keys of `items`: the key columns of an inserted row
/// equal its key but may differ in their bits (0.0 and -0.0), and the row may be short.
fn random_item_op(rng: &mut Rng) -> Operation {
    let k = float_value(rng);
    let n = Value::Int(rng.below(2) as i64);
    let pk = PrimaryKey::composite([k.clone(), n.clone()]);
    match rng.below(6) {
        0 | 1 => {
            let k_in_row = match k {
                Value::Float(f) if f == 0.0 && rng.below(2) == 0 => Value::Float(-f),
                other => other,
            };
            let mut values = vec![k_in_row, n];
            let width = rng.below(4);
            if width >= 1 {
                values.push(if rng.below(3) == 0 {
                    Value::Null
                } else {
                    Value::String(format!("s{}", rng.below(3)).into())
                });
            }
            if width >= 2 {
                values.push(Value::Int(rng.below(5) as i64));
            }
            if width >= 3 {
                values.push(float_value(rng));
            }
            Operation::insert(ITEMS, pk, CompactRow::new(values), rng.next_u64())
        }
        2..=4 => {
            let mut updates = Vec::new();
            for column in 2..=4u16 {
                if rng.below(2) == 0 {
                    let value = match column {
                        2 => Value::String(format!("u{}", rng.below(3)).into()),
                        3 if rng.below(4) == 0 => Value::Null,
                        3 => Value::Int(rng.below(9) as i64),
                        _ => float_value(rng),
                    };
                    updates.push(ColumnUpdate::new(column, value));
                }
            }
            if updates.is_empty() {
                updates.push(ColumnUpdate::new(3, Value::Int(7)));
            }
            Operation::update(ITEMS, pk, updates, rng.next_u64())
        }
        _ => Operation::delete(ITEMS, pk, rng.next_u64()),
    }
}

/// An insert, update or delete of a few keys of `notes`, some rows without their last column.
fn random_note_op(rng: &mut Rng) -> Operation {
    let id = rng.below(4) as i64;
    let pk = PrimaryKey::single(id);
    match rng.below(5) {
        0 | 1 => {
            let text = Value::String(format!("t{}", rng.below(3)).into());
            let mut values = vec![Value::Int(id), text];
            if rng.below(2) == 0 {
                values.push(float_value(rng));
            }
            Operation::insert(NOTES, pk, CompactRow::new(values), rng.next_u64())
        }
        2 | 3 => {
            let updates = if rng.below(2) == 0 {
                vec![ColumnUpdate::new(2, float_value(rng))]
            } else {
                vec![
                    ColumnUpdate::new(1, Value::String("x".into())),
                    ColumnUpdate::new(2, Value::Null),
                ]
            };
            Operation::update(NOTES, pk, updates, rng.next_u64())
        }
        _ => Operation::delete(NOTES, pk, rng.next_u64()),
    }
}

/// A random log of 20 to 79 operations on `items` and `notes`.
fn random_log(rng: &mut Rng) -> Vec<SequencedOperation> {
    let len = 20 + rng.below(60);
    (1..=len)
        .map(|seq| {
            let op = if rng.below(2) == 0 {
                random_item_op(rng)
            } else {
                random_note_op(rng)
            };
            SequencedOperation::new(seq, op)
        })
        .collect()
}

/// A random log of 20 to 79 operations under the reference model's schema with constraints,
/// owners and immutable tables.
fn random_constrained_log(rng: &mut Rng) -> Vec<SequencedOperation> {
    let len = 20 + rng.below(60);
    oracle::random_log(rng, &oracle::schema(), 1, len)
}

use oracle::bits;

/// Every row of every table of `schema`, in order, as `(table, key, row)`, keys included bit
/// for bit.
type Dump = Vec<(u16, Vec<String>, Vec<String>)>;

async fn dump(engine: &dyn StorageEngine, room: &RoomId, schema: &Schema) -> Dump {
    let mut rows = Vec::new();
    for table in schema.table_ids() {
        let mut scan = engine
            .scan_by_id(room, table, ScanOptions::new())
            .await
            .unwrap();
        while let Some(item) = scan.next().await {
            let (pk, row) = item.unwrap();
            rows.push((
                table,
                pk.iter().map(bits).collect(),
                row.iter().map(bits).collect(),
            ));
        }
    }
    rows
}

/// Applies `ops` in batches of one to five operations, and returns the operations it rejected.
async fn apply_in_batches(
    engine: &dyn StorageEngine,
    room: &RoomId,
    ops: &[SequencedOperation],
    rng: &mut Rng,
) -> Vec<(SequenceNumber, RejectReason)> {
    let mut rejected = Vec::new();
    let mut start = 0;
    while start < ops.len() {
        let len = (1 + rng.below(5) as usize).min(ops.len() - start);
        let batch = ops[start..start + len].to_vec();
        let report = engine.apply_batch(room, batch).await.unwrap();
        rejected.extend_from_slice(report.rejected());
        start += len;
    }
    rejected
}

/// A disk engine in `dir` that compacts only when asked to.
fn disk(dir: &std::path::Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false)).unwrap()
}

/// For each of `seeds`, a random log from `make_log` under `schema`, applied whole by a memory
/// engine, is the reference; the reference model must reject the same operations for the same
/// reasons and hold the same rows. A disk engine applies it with a compaction halfway and must
/// hold the same and reject the same, before and after a reopen. Then each engine applies a
/// prefix of random length and takes a snapshot, which each engine restores before applying the
/// rest of the log: every replica must hold the same, reject the same operations of the rest,
/// and a disk replica hold the same again after a reopen.
async fn replicas_converge(
    seeds: std::ops::Range<u64>,
    schema: fn() -> Schema,
    make_log: fn(&mut Rng) -> Vec<SequencedOperation>,
) {
    let room = RoomId::new("convergence").unwrap();
    for seed in seeds {
        let mut rng = Rng::new(seed.wrapping_mul(7919) + 3);
        let log = make_log(&mut rng);
        let len = log.len() as u64;
        for op in &log {
            schema().validate_operation(&op.op).unwrap();
        }

        let reference_engine = MemoryStorageEngine::new();
        reference_engine.open_room(&room, schema()).await.unwrap();
        let rejected = apply_in_batches(&reference_engine, &room, &log, &mut rng).await;
        let reference = dump(&reference_engine, &room, &schema()).await;
        let mut model = Oracle::new(schema());
        assert_eq!(
            rejected,
            reject_reasons(model.apply_batch(&log)),
            "seed {seed}: model"
        );
        assert_eq!(
            oracle::dump(&room_rows(&reference_engine, &room, &schema()).await),
            oracle::dump(model.rows()),
            "seed {seed}: model"
        );

        let dir = tempfile::tempdir().unwrap();
        let full = disk(dir.path());
        full.open_room(&room, schema()).await.unwrap();
        let half = log.len() / 2;
        let mut full_rejected = apply_in_batches(&full, &room, &log[..half], &mut rng).await;
        full.compact_room(&room).await.unwrap();
        full_rejected.extend(apply_in_batches(&full, &room, &log[half..], &mut rng).await);
        assert_eq!(full_rejected, rejected, "seed {seed}: disk");
        assert_eq!(
            dump(&full, &room, &schema()).await,
            reference,
            "seed {seed}: disk"
        );
        full.close_room(&room).await.unwrap();
        let full = disk(dir.path());
        full.open_room(&room, schema()).await.unwrap();
        let reopened = dump(&full, &room, &schema()).await;
        assert_eq!(reopened, reference, "seed {seed}: disk reopened");

        let cut = rng.below(len + 1) as usize;
        let rest_rejected: Vec<_> = rejected
            .iter()
            .filter(|(seq, _)| seq.get() > cut as u64)
            .cloned()
            .collect();
        for source_on_disk in [false, true] {
            let source_dir = tempfile::tempdir().unwrap();
            let (source_disk, source_memory) =
                (disk(source_dir.path()), MemoryStorageEngine::new());
            let source: &dyn StorageEngine = if source_on_disk {
                &source_disk
            } else {
                &source_memory
            };
            source.open_room(&room, schema()).await.unwrap();
            apply_in_batches(source, &room, &log[..cut], &mut rng).await;
            let snapshot = source.create_snapshot(&room).await.unwrap();

            for target_on_disk in [false, true] {
                let case = format!(
                    "seed {seed}, snapshot at {cut} from {} into {}",
                    if source_on_disk { "disk" } else { "memory" },
                    if target_on_disk { "disk" } else { "memory" }
                );
                let target_dir = tempfile::tempdir().unwrap();
                let (target_disk, target_memory) =
                    (disk(target_dir.path()), MemoryStorageEngine::new());
                let target: &dyn StorageEngine = if target_on_disk {
                    &target_disk
                } else {
                    &target_memory
                };
                target.open_room(&room, schema()).await.unwrap();
                let head = SequenceNumber::new(cut as u64);
                target
                    .apply_snapshot(&room, schema(), snapshot.clone().into(), head)
                    .await
                    .unwrap_or_else(|e| panic!("{case}: restore: {e}"));
                let target_rejected = apply_in_batches(target, &room, &log[cut..], &mut rng).await;
                assert_eq!(target_rejected, rest_rejected, "{case}");
                assert_eq!(dump(target, &room, &schema()).await, reference, "{case}");
                if target_on_disk {
                    target_disk.close_room(&room).await.unwrap();
                    let reopened = disk(target_dir.path());
                    reopened.open_room(&room, schema()).await.unwrap();
                    let rows = dump(&reopened, &room, &schema()).await;
                    assert_eq!(rows, reference, "{case}, reopened");
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_converge_from_a_snapshot_at_any_point_in_both_engines() {
    replicas_converge(0..1, schema, random_log).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn replicas_converge_from_a_snapshot_at_any_point_in_both_engines_over_many_logs() {
    replicas_converge(0..40, schema, random_log).await;
}

/// With constraints, owners and immutable tables, replicas also reject the same operations, for
/// the same reasons, whichever way they reached the state each one was decided on.
#[tokio::test(flavor = "multi_thread")]
async fn replicas_with_constraints_converge_and_reject_the_same() {
    replicas_converge(0..4, oracle::schema, random_constrained_log).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn replicas_with_constraints_converge_and_reject_the_same_over_many_logs() {
    replicas_converge(0..100, oracle::schema, random_constrained_log).await;
}
