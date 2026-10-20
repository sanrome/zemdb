//! Randomized tests of the storage engines:
//! - snapshots of random states round trip between the memory and the disk engine, and through
//!   the disk engine's own files;
//! - a disk replica written under a schema reopens under any later version of it;
//! - damaged snapshots never panic `apply_snapshot`, and damaged room files never panic
//!   `open_room`: both fail with a corruption error or accept a valid state.
//!
//! The ignored variants run many more cases: `cargo test --release -- --ignored`.

use bytes::Bytes;
use futures::StreamExt;
use std::collections::BTreeSet;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use zemdb_core::{
    validate_snapshot_envelope, ColumnDef, ColumnUpdate, CompactRow, DataType, Operation, OwnerDef,
    OwnerIdentity, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
    SnapshotCompression, SnapshotEnvelopeHeader, TableSchema, Value, SNAPSHOT_HEADER_LEN,
};
use zemdb_storage::format::HEADER_SIZE;
use zemdb_storage::snapshot::{decode_snapshot_envelope, encode_snapshot_envelope};
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, ScanOptions, StorageEngine,
    StorageError,
};

// ---------------------------------------------------------------------------------------------
// Generator, case runner and mutator
// ---------------------------------------------------------------------------------------------

/// A splitmix64 generator: a few lines, fast, and fully determined by its seed, so that a
/// failing case is replayed from its seed alone.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..n`, or 0 if `n` is 0.
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// A number in `low..high`.
    fn between(&mut self, low: usize, high: usize) -> usize {
        low + self.below(high - low)
    }

    /// `true` with probability `num / den`.
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.next_u64() % den < num
    }

    fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next_u64() as u8).collect()
    }

    /// Fewer than `n` random bytes.
    fn bytes_below(&mut self, n: usize) -> Vec<u8> {
        let len = self.below(n);
        self.bytes(len)
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// Runs `check` on `cases` cases, each with a generator seeded from `seed` and the case number.
/// A failing case prints its seed, so that `check(&mut Rng::new(seed))` replays it alone.
fn for_each_case(seed: u64, cases: u64, mut check: impl FnMut(&mut Rng)) {
    for case in 0..cases {
        let case_seed = Rng::new(seed ^ case.wrapping_mul(0xD1B5_4A32_D192_ED03)).next_u64();
        let outcome = catch_unwind(AssertUnwindSafe(|| check(&mut Rng::new(case_seed))));
        if let Err(panic) = outcome {
            eprintln!(
                "case {case} of seed {seed:#x} failed; replay it with Rng::new({case_seed:#x})"
            );
            resume_unwind(panic);
        }
    }
}

const INTERESTING_U8: [u8; 12] = [
    0, 1, 0x7F, 0x80, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x10, 0x40,
];

/// Extremes and lengths around the limits of the formats.
const INTERESTING_U64: [u64; 14] = [
    0,
    1,
    0xFF,
    0xFFFF,
    0x1_0000,
    1 << 20,
    (1 << 20) + 1,
    16 * 1024 * 1024,
    0x7FFF_FFFF,
    0xFFFF_FFFF,
    0x1_0000_0000,
    1 << 40,
    u64::MAX - 1,
    u64::MAX,
];

/// Applies 1 to 4 random mutations to `data`: bit flips, byte changes, insertions, deletions,
/// truncations, duplicated ranges, ranges spliced from another sample of `pool`, and
/// fixed-width integers of interesting values (snapshots and WAL payloads use fixed-width
/// lengths).
fn mutate(rng: &mut Rng, data: &mut Vec<u8>, pool: &[Vec<u8>]) {
    for _ in 0..1 + rng.below(4) {
        let len = data.len();
        match rng.below(9) {
            0 if len > 0 => {
                let i = rng.below(len);
                data[i] ^= 1 << rng.below(8);
            }
            1 if len > 0 => {
                let i = rng.below(len);
                data[i] = rng.next_u64() as u8;
            }
            2 if len > 0 => {
                let i = rng.below(len);
                data[i] = *rng.pick(&INTERESTING_U8);
            }
            3 => {
                let i = rng.below(len + 1);
                let n = 1 + rng.below(16);
                let inserted = rng.bytes(n);
                data.splice(i..i, inserted);
            }
            4 if len > 0 => {
                let i = rng.below(len);
                let n = 1 + rng.below((len - i).min(32));
                data.drain(i..i + n);
            }
            5 if len > 0 => {
                let i = rng.below(len);
                data.truncate(i);
            }
            6 if len > 1 => {
                let from = rng.below(len);
                let n = 1 + rng.below((len - from).min(64));
                let chunk = data[from..from + n].to_vec();
                let at = rng.below(len + 1);
                data.splice(at..at, chunk);
            }
            7 if !pool.is_empty() => {
                let other = rng.pick(pool);
                if !other.is_empty() {
                    let from = rng.below(other.len());
                    let n = 1 + rng.below((other.len() - from).min(64));
                    let at = rng.below(len + 1);
                    let end = (at + n).min(len);
                    data.splice(at..end, other[from..from + n].iter().copied());
                }
            }
            _ if len > 0 => {
                let i = rng.below(len);
                let width = if rng.coin() { 4 } else { 8 };
                let bytes = rng.pick(&INTERESTING_U64).to_le_bytes();
                for (k, b) in bytes.iter().take(width).enumerate() {
                    if let Some(slot) = data.get_mut(i + k) {
                        *slot = *b;
                    }
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Engines and states
// ---------------------------------------------------------------------------------------------

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn disk_engine(dir: &std::path::Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false)).unwrap()
}

fn sequenced(ops: impl IntoIterator<Item = Operation>) -> Vec<SequencedOperation> {
    ops.into_iter()
        .enumerate()
        .map(|(i, op)| SequencedOperation::new(i as u64 + 1, op))
        .collect()
}

/// Every row of the tables `table_ids`, in key order.
async fn dump<E: StorageEngine>(
    engine: &E,
    room: &RoomId,
    table_ids: &[u16],
) -> Vec<(u16, Vec<(PrimaryKey, CompactRow)>)> {
    let mut tables = vec![];
    for &table_id in table_ids {
        let mut stream = engine
            .scan_by_id(room, table_id, ScanOptions::default())
            .await
            .unwrap();
        let mut rows = vec![];
        while let Some(row) = stream.next().await {
            rows.push(row.unwrap());
        }
        tables.push((table_id, rows));
    }
    tables
}

/// `rows` without the nulls at the end of each row.
///
/// An update widens a row to the width of the schema it is applied with, so the same log
/// replayed under a later version of the schema can leave rows with more trailing nulls: the
/// same content.
fn without_trailing_nulls(
    tables: Vec<(u16, Vec<(PrimaryKey, CompactRow)>)>,
) -> Vec<(u16, Vec<(PrimaryKey, CompactRow)>)> {
    tables
        .into_iter()
        .map(|(table_id, rows)| {
            let rows = rows
                .into_iter()
                .map(|(pk, row)| {
                    let mut values = row.into_values();
                    while values.last() == Some(&Value::Null) {
                        values.pop();
                    }
                    (pk, CompactRow::new(values))
                })
                .collect();
            (table_id, rows)
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Snapshots round trip between engines
// ---------------------------------------------------------------------------------------------

const ITEMS: u16 = 3;
const PAIRS: u16 = 0;

/// `items`, keyed by an integer, with a column added later, and `pairs`, keyed by a float and
/// bytes (`-0.0` and `0.0` are the same key).
fn state_schema() -> Schema {
    let mut schema = Schema::builder()
        .table(
            TableSchema::builder("items")
                .table_id(ITEMS)
                .primary_key("id", DataType::Int)
                .column("name", DataType::String)
                .nullable_column("price", DataType::Float),
        )
        .table(
            TableSchema::builder("pairs")
                .table_id(PAIRS)
                .primary_key("a", DataType::Float)
                .primary_key("b", DataType::Bytes)
                .nullable_column("u", DataType::Uuid),
        )
        .build();
    schema
        .add_column(
            "items",
            ColumnDef::new("blob", DataType::Bytes).nullable(true),
        )
        .unwrap();
    schema
}

fn float(rng: &mut Rng) -> f64 {
    match rng.below(8) {
        0 => -0.0,
        1 => 0.0,
        2 => f64::NAN,
        3 => f64::INFINITY,
        4 => f64::NEG_INFINITY,
        5 => f64::MIN_POSITIVE,
        6 => (rng.next_u64() as i64) as f64 / 1024.0,
        _ => f64::from_bits(rng.next_u64()),
    }
}

fn nullable(rng: &mut Rng, make: impl FnOnce(&mut Rng) -> Value) -> Value {
    if rng.coin() {
        Value::Null
    } else {
        make(rng)
    }
}

/// A random operation valid under `state_schema`, on a few keys, so that operations hit rows
/// written by earlier ones. Some inserts leave out the column added later.
fn state_op(rng: &mut Rng) -> Operation {
    let item = rng.below(20) as i64;
    let pair_a = *rng.pick(&[0.0f64, -0.0, f64::NAN, 1.5, -2.0]);
    let pair_b = rng.bytes_below(3);
    let pair_key =
        || PrimaryKey::composite([Value::Float(pair_a), Value::Bytes(pair_b.clone().into())]);
    match rng.below(6) {
        0 => {
            let name = (0..rng.below(7))
                .map(|_| *rng.pick(&['a', 'é', '漢', ' ', '\u{0}']))
                .collect::<String>();
            let price = nullable(rng, |rng| Value::Float(float(rng)));
            let blob = nullable(rng, |rng| Value::Bytes(rng.bytes_below(6).into()));
            let mut row = vec![Value::Int(item), Value::String(name.into()), price, blob];
            if rng.coin() {
                row.truncate(3);
            }
            Operation::insert(ITEMS, PrimaryKey::single(item), CompactRow::new(row), 1)
        }
        1 => {
            let price = nullable(rng, |rng| Value::Float(float(rng)));
            Operation::update(
                ITEMS,
                PrimaryKey::single(item),
                vec![ColumnUpdate::new(2, price)],
                1,
            )
        }
        2 => {
            let blob = Value::Bytes(rng.bytes_below(4).into());
            Operation::update(
                ITEMS,
                PrimaryKey::single(item),
                vec![ColumnUpdate::new(3, blob)],
                1,
            )
        }
        3 => Operation::delete(ITEMS, PrimaryKey::single(item), 1),
        4 => {
            let u = nullable(rng, |rng| Value::Uuid(rng.bytes(16).try_into().unwrap()));
            let row = vec![Value::Float(pair_a), Value::Bytes(pair_b.clone().into()), u];
            Operation::insert(PAIRS, pair_key(), CompactRow::new(row), 1)
        }
        _ => Operation::delete(PAIRS, pair_key(), 1),
    }
}

fn check_snapshots_between_engines(seed: u64, cases: u64) {
    let rt = runtime();
    let schema = state_schema();
    let room = RoomId::new("r").unwrap();
    let tables = [PAIRS, ITEMS];
    for_each_case(seed, cases, |rng| {
        let ops = sequenced((0..rng.between(1, 40)).map(|_| state_op(rng)));
        let head = SequenceNumber::new(ops.len() as u64);
        rt.block_on(async {
            let memory = MemoryStorageEngine::new();
            memory.open_room(&room, schema.clone()).await.unwrap();
            memory.apply_batch(&room, ops.clone()).await.unwrap();
            let expected = dump(&memory, &room, &tables).await;

            let dir = tempfile::tempdir().unwrap();
            let disk = disk_engine(dir.path());
            disk.open_room(&room, schema.clone()).await.unwrap();
            disk.apply_batch(&room, ops).await.unwrap();
            assert_eq!(
                dump(&disk, &room, &tables).await,
                expected,
                "engines disagree"
            );

            let snapshots = [
                memory.create_snapshot(&room).await.unwrap(),
                disk.create_snapshot(&room).await.unwrap(),
            ];
            for snapshot in snapshots {
                let memory = MemoryStorageEngine::new();
                memory.open_room(&room, schema.clone()).await.unwrap();
                let applied = memory
                    .apply_snapshot(&room, schema.clone(), Bytes::from(snapshot.clone()), head)
                    .await;
                assert_eq!(applied.unwrap(), head);
                assert_eq!(dump(&memory, &room, &tables).await, expected);

                let replica_dir = tempfile::tempdir().unwrap();
                let replica = disk_engine(replica_dir.path());
                replica.open_room(&room, schema.clone()).await.unwrap();
                let applied = replica
                    .apply_snapshot(&room, schema.clone(), Bytes::from(snapshot), head)
                    .await;
                assert_eq!(applied.unwrap(), head);
                assert_eq!(dump(&replica, &room, &tables).await, expected);
                // And through the engine's own snapshot file.
                replica.compact_room(&room).await.unwrap();
                replica.close_room(&room).await.unwrap();
                replica.open_room(&room, schema.clone()).await.unwrap();
                assert_eq!(
                    dump(&replica, &room, &tables).await,
                    expected,
                    "snapshot file"
                );
            }

            // And through the engine's own WAL: reopen without compacting.
            disk.close_room(&room).await.unwrap();
            disk.open_room(&room, schema.clone()).await.unwrap();
            assert_eq!(dump(&disk, &room, &tables).await, expected, "WAL replay");
        });
    });
}

#[test]
fn random_snapshots_round_trip_between_engines() {
    check_snapshots_between_engines(0x5A9_2001, 3);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_snapshots_round_trip_between_engines_long() {
    check_snapshots_between_engines(0x5A9_2002, 150);
}

fn check_envelopes(seed: u64, cases: u64) {
    for_each_case(seed, cases, |rng| {
        let payload = rng.bytes_below(4096);
        let zstd = rng.coin();
        let envelope = encode_snapshot_envelope(&payload, zstd, 3).unwrap();
        assert_eq!(decode_snapshot_envelope(&envelope).unwrap(), payload);
        let header = validate_snapshot_envelope(&envelope).unwrap();
        assert_eq!(header.uncompressed_len() as usize, payload.len());
        let compression = if zstd {
            SnapshotCompression::Zstd
        } else {
            SnapshotCompression::Raw
        };
        assert_eq!(header.compression(), compression);
    });
}

#[test]
fn random_snapshot_envelopes_round_trip() {
    check_envelopes(0x5A9_3001, 1_500);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn random_snapshot_envelopes_round_trip_long() {
    check_envelopes(0x5A9_3002, 300_000);
}

// ---------------------------------------------------------------------------------------------
// A disk replica reopens under every later schema version
// ---------------------------------------------------------------------------------------------

/// A table as its id, its columns as (type, nullable, encrypted), its key width (the key is its
/// first columns), where its owner is and whether it is immutable. Columns are named `c0`,
/// `c1`, ...
type TableSpec = (u16, Vec<(DataType, bool, bool)>, usize, Owner, bool);

/// Where a table's owner column is.
#[derive(Debug, Clone, Copy)]
enum Owner {
    None,
    /// The first column, a string of the key.
    InKey,
    /// A string column `owner` after the others.
    OutsideKey,
}

fn data_type(rng: &mut Rng) -> DataType {
    *rng.pick(&[
        DataType::Int,
        DataType::Float,
        DataType::Timestamp,
        DataType::String,
        DataType::Bool,
        DataType::Bytes,
        DataType::Uuid,
    ])
}

fn table_specs(rng: &mut Rng) -> Vec<TableSpec> {
    (0..rng.between(1, 4))
        .map(|_| {
            let columns = (0..rng.between(1, 6))
                .map(|_| (data_type(rng), rng.coin(), rng.coin()))
                .collect();
            let owner = *rng.pick(&[Owner::None, Owner::None, Owner::InKey, Owner::OutsideKey]);
            let immutable = rng.chance(1, 4);
            (
                rng.below(6) as u16,
                columns,
                rng.between(1, 3),
                owner,
                immutable,
            )
        })
        .collect()
}

/// The schema of `specs`, skipping a table whose id is taken. Key columns are neither nullable
/// nor encrypted, and neither is an owner column, which holds strings.
fn build_schema(specs: &[TableSpec]) -> Schema {
    let mut tables = vec![];
    let mut ids = BTreeSet::new();
    for (i, (id, columns, key_len, owner, immutable)) in specs.iter().enumerate() {
        if !ids.insert(*id) {
            continue;
        }
        let key_len = (*key_len).min(columns.len());
        let mut columns: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(c, (data_type, nullable, encrypted))| {
                let in_key = c < key_len;
                let data_type = match owner {
                    Owner::InKey if c == 0 => DataType::String,
                    _ => *data_type,
                };
                ColumnDef::new(format!("c{c}"), data_type)
                    .nullable(*nullable && !in_key)
                    .encrypted(*encrypted && !in_key)
            })
            .collect();
        let owner = match owner {
            Owner::None => None,
            Owner::InKey => Some(OwnerDef::new("c0", OwnerIdentity::ClientId)),
            Owner::OutsideKey => {
                columns.push(ColumnDef::new("owner", DataType::String));
                Some(OwnerDef::new("owner", OwnerIdentity::ClientId))
            }
        };
        let key = (0..key_len).map(|c| format!("c{c}")).collect();
        let table = TableSchema::try_new(*id, format!("t{i}"), key, columns)
            .and_then(|table| table.try_with_owner(owner))
            .unwrap()
            .with_immutable(*immutable);
        tables.push(table);
    }
    Schema::try_from_tables(tables).unwrap()
}

/// The position of the owner column of `table` if it is outside the key.
fn owner_outside_key(table: &TableSchema) -> Option<usize> {
    let owner = table.owner()?.column();
    (!table.primary_key().iter().any(|column| column == owner))
        .then(|| table.column_index(owner))
        .flatten()
}

fn value_of(data_type: DataType, seed: u64) -> Value {
    match data_type {
        DataType::Int => Value::Int(seed as i64),
        DataType::Float => Value::Float(if seed % 3 == 0 {
            -0.0
        } else {
            seed as f64 / 3.0
        }),
        DataType::Timestamp => Value::Timestamp(seed as i64),
        DataType::String => Value::String(format!("s{seed}").into()),
        DataType::Bool => Value::Bool(seed % 2 == 0),
        DataType::Bytes => Value::Bytes(seed.to_le_bytes().to_vec().into()),
        DataType::Uuid => Value::Uuid([seed as u8; 16]),
        DataType::Null => Value::Null,
    }
}

/// A value valid for `column`: now and then a null if it is nullable, and bytes if it is
/// encrypted (encrypted values travel as ciphertext).
fn cell(column: &ColumnDef, seed: u64) -> Value {
    if column.is_nullable() && seed % 4 == 0 {
        return Value::Null;
    }
    if column.is_encrypted() {
        return Value::Bytes(vec![seed as u8; 3].into());
    }
    value_of(column.data_type(), seed)
}

/// One operation per (table, seed, kind) of `seeds`, valid under `schema`: inserts of whole
/// rows or of rows cut short after their last non-null column, updates of some columns, and
/// deletes. Keys repeat often, so later operations hit rows written by earlier ones. An
/// immutable table only gets inserts; with the owner outside the key, updates and deletes
/// declare one of two owners, which may not be the row's.
fn operations_for(schema: &Schema, seeds: &[(u8, u64, u8)]) -> Vec<Operation> {
    let tables: Vec<&TableSchema> = schema.tables().collect();
    seeds
        .iter()
        .map(|&(table, seed, kind)| {
            let table = tables[table as usize % tables.len()];
            let key: Vec<Value> = table
                .primary_key()
                .iter()
                .map(|name| value_of(table.get_column(name).unwrap().data_type(), seed % 7))
                .collect();
            let pk = PrimaryKey::composite(key.clone());
            let columns = table.columns();
            let key_len = table.primary_key().len();
            let owner = owner_outside_key(table);
            let declared = Value::String(format!("s{}", seed % 2).into());
            let kind = if table.is_immutable() { 0 } else { kind };
            match kind % 3 {
                0 => {
                    let mut row: Vec<Value> = columns
                        .iter()
                        .enumerate()
                        .map(|(i, column)| match key.get(i) {
                            Some(key_value) => key_value.clone(),
                            None => cell(column, seed.wrapping_add(i as u64)),
                        })
                        .collect();
                    while row.len() > 1 && seed % 2 == 0 && columns[row.len() - 1].is_nullable() {
                        row.pop();
                    }
                    Operation::insert(table.table_id(), pk, CompactRow::new(row), seed)
                }
                1 => {
                    let mut updates: Vec<ColumnUpdate> = columns
                        .iter()
                        .enumerate()
                        .skip(key_len)
                        .filter(|(i, _)| (seed >> i) & 1 == 1 && Some(*i) != owner)
                        .map(|(i, column)| {
                            ColumnUpdate::new(i as u16, cell(column, seed ^ i as u64))
                        })
                        .collect();
                    if let (Some(owner), false) = (owner, updates.is_empty()) {
                        updates.push(ColumnUpdate::new(owner as u16, declared.clone()));
                    }
                    if updates.is_empty() {
                        delete(table, pk, owner, declared, seed)
                    } else {
                        Operation::update(table.table_id(), pk, updates, seed)
                    }
                }
                _ => delete(table, pk, owner, declared, seed),
            }
        })
        .collect()
}

/// A delete of `pk` from `table`, declaring `declared` as the owner if the table's owner is
/// outside the key.
fn delete(
    table: &TableSchema,
    pk: PrimaryKey,
    owner: Option<usize>,
    declared: Value,
    seed: u64,
) -> Operation {
    match owner {
        Some(_) => Operation::delete_with_owner(table.table_id(), pk, declared, seed),
        None => Operation::delete(table.table_id(), pk, seed),
    }
}

/// `v1` with a nullable column `added{k}` added to the table `table` for each (table, type,
/// encrypted) of `additions`.
fn evolve(v1: &Schema, additions: &[(u8, DataType, bool)]) -> Schema {
    let names: Vec<String> = v1.tables().map(|t| t.name().to_string()).collect();
    let mut evolved = v1.clone();
    for (k, (table, data_type, encrypted)) in additions.iter().enumerate() {
        let column = ColumnDef::new(format!("added{k}"), *data_type)
            .nullable(true)
            .encrypted(*encrypted);
        evolved
            .add_column(&names[*table as usize % names.len()], column)
            .unwrap();
    }
    evolved
}

/// Writes `ops` to a disk room opened under `v1`, reopens it under `evolved` and checks that
/// it holds the same rows at the same head sequence.
fn check_reopen_under(
    rt: &tokio::runtime::Runtime,
    v1: &Schema,
    evolved: &Schema,
    ops: Vec<Operation>,
) {
    let room = RoomId::new("evolving").unwrap();
    let tables: Vec<u16> = v1.table_ids().collect();
    let ops = sequenced(ops);
    let head = SequenceNumber::new(ops.len() as u64);
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let engine = disk_engine(dir.path());
        engine.open_room(&room, v1.clone()).await.unwrap();
        engine.apply_batch(&room, ops).await.unwrap();
        let before = dump(&engine, &room, &tables).await;
        engine.close_room(&room).await.unwrap();

        engine.open_room(&room, evolved.clone()).await.unwrap();
        assert_eq!(engine.get_head_seq(&room).await.unwrap(), head);
        assert_eq!(
            without_trailing_nulls(dump(&engine, &room, &tables).await),
            without_trailing_nulls(before)
        );
    });
}

fn check_evolution(seed: u64, cases: u64) {
    let rt = runtime();
    for_each_case(seed, cases, |rng| {
        let v1 = build_schema(&table_specs(rng));
        let seeds: Vec<(u8, u64, u8)> = (0..rng.between(1, 30))
            .map(|_| (rng.next_u64() as u8, rng.next_u64(), rng.next_u64() as u8))
            .collect();
        let additions: Vec<(u8, DataType, bool)> = (0..rng.between(1, 6))
            .map(|_| (rng.next_u64() as u8, data_type(rng), rng.coin()))
            .collect();
        let evolved = evolve(&v1, &additions);
        check_reopen_under(&rt, &v1, &evolved, operations_for(&v1, &seeds));
    });
}

#[test]
fn a_disk_replica_reopens_under_every_later_schema_version() {
    check_evolution(0xE1_2001, 15);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn a_disk_replica_reopens_under_every_later_schema_version_long() {
    check_evolution(0xE1_2002, 600);
}

/// The random schemas have owners inside and outside the key and immutable tables, and their
/// random operations include deletes that declare an owner, so the evolution test replays the
/// replicas' decisions on them.
#[test]
fn random_schemas_have_owners_and_immutable_tables() {
    let (mut in_key, mut outside_key, mut immutable, mut owned_deletes) = (0, 0, 0, 0);
    for_each_case(0xE1_2003, 40, |rng| {
        let schema = build_schema(&table_specs(rng));
        for table in schema.tables() {
            match (table.owner(), owner_outside_key(table)) {
                (Some(_), Some(_)) => outside_key += 1,
                (Some(_), None) => in_key += 1,
                _ => {}
            }
            immutable += usize::from(table.is_immutable());
        }
        let seeds: Vec<(u8, u64, u8)> = (0..30)
            .map(|_| (rng.next_u64() as u8, rng.next_u64(), rng.next_u64() as u8))
            .collect();
        for op in operations_for(&schema, &seeds) {
            schema.validate_operation(&op).unwrap();
            if let zemdb_core::OperationKind::Delete { owner: Some(_) } = op.kind() {
                owned_deletes += 1;
            }
        }
    });
    assert!(in_key > 0 && outside_key > 0 && immutable > 0 && owned_deletes > 0);
}

#[test]
fn an_update_replayed_under_a_wider_schema_keeps_the_row() {
    // A row inserted and then updated under a table of two columns, replayed under the table
    // with a third, nullable column: the update widens the row with a null.
    let v1 = build_schema(&[(
        0,
        vec![(DataType::Int, false, false), (DataType::Int, false, false)],
        1,
        Owner::None,
        false,
    )]);
    let seeds = [
        (0, 14_822_749_083_394_190_377, 150),
        (0, 18_040_384_226_490_305_559, 43),
    ];
    let ops = operations_for(&v1, &seeds);
    assert!(ops[0].is_insert() && ops[1].is_update());
    let evolved = evolve(&v1, &[(0, DataType::Int, false)]);
    check_reopen_under(&runtime(), &v1, &evolved, ops);
}

// ---------------------------------------------------------------------------------------------
// Damaged snapshots and room files
// ---------------------------------------------------------------------------------------------

/// Two tables, one with a composite key and an encrypted column, and a column added later.
fn sample_schema() -> Schema {
    let mut schema = Schema::builder()
        .table(
            TableSchema::builder("items")
                .table_id(0)
                .primary_key("id", DataType::Int)
                .column("name", DataType::String)
                .nullable_column("price", DataType::Float),
        )
        .table(
            TableSchema::builder("links")
                .table_id(1)
                .primary_key("a", DataType::Int)
                .primary_key("b", DataType::Uuid)
                .encrypted_column("secret", DataType::String)
                .nullable_column("flag", DataType::Bool),
        )
        .build();
    schema
        .add_column(
            "items",
            ColumnDef::new("extra", DataType::Bytes).nullable(true),
        )
        .unwrap();
    schema
}

/// Rows of every value type in both tables of `sample_schema`, a row from before the column
/// added later, an update and a delete.
fn sample_ops() -> Vec<SequencedOperation> {
    let mut ops = vec![
        Operation::insert(
            0,
            PrimaryKey::single(1i64),
            CompactRow::new(vec![Value::Int(1), "a".into(), Value::Null, Value::Null]),
            1,
        ),
        Operation::update(
            0,
            PrimaryKey::single(1i64),
            vec![ColumnUpdate::new(2, Value::Float(-0.0))],
            2,
        ),
        Operation::insert(
            1,
            PrimaryKey::composite([Value::Int(1), Value::Uuid([3; 16])]),
            CompactRow::new(vec![
                Value::Int(1),
                Value::Uuid([3; 16]),
                Value::Bytes(vec![1, 2].into()),
                Value::Null,
            ]),
            3,
        ),
        Operation::insert(
            0,
            PrimaryKey::single(2i64),
            CompactRow::new(vec![Value::Int(2), "b".into(), Value::Float(f64::NAN)]),
            4,
        ),
        Operation::delete(0, PrimaryKey::single(3i64), 5),
    ];
    for k in 10..40i64 {
        ops.push(Operation::insert(
            0,
            PrimaryKey::single(k),
            CompactRow::new(vec![
                Value::Int(k),
                format!("row {k}").into(),
                Value::Float(k as f64),
                Value::Bytes(vec![k as u8; 5].into()),
            ]),
            k as u64,
        ));
    }
    sequenced(ops)
}

/// An envelope of `payload`, raw or compressed, whose header declares `declared` bytes once
/// decompressed and matches the body's checksum.
fn sealed_envelope(compression: SnapshotCompression, body: &[u8], declared: u32) -> Vec<u8> {
    let header = SnapshotEnvelopeHeader::for_body(compression, declared, body);
    let mut envelope = header.to_bytes().to_vec();
    envelope.extend_from_slice(body);
    envelope
}

fn check_damaged_snapshots(seed: u64, cases: u64) {
    let rt = runtime();
    let schema = sample_schema();
    let source = RoomId::new("source").unwrap();
    let ops = sample_ops();
    let head = SequenceNumber::new(ops.len() as u64);

    // A raw envelope (memory engine) and a compressed one (disk engine) of the same state.
    let (raw, compressed) = rt.block_on(async {
        let memory = MemoryStorageEngine::new();
        memory.open_room(&source, schema.clone()).await.unwrap();
        memory.apply_batch(&source, ops.clone()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let disk = disk_engine(dir.path());
        disk.open_room(&source, schema.clone()).await.unwrap();
        disk.apply_batch(&source, ops).await.unwrap();
        (
            memory.create_snapshot(&source).await.unwrap(),
            disk.create_snapshot(&source).await.unwrap(),
        )
    });
    let raw_payload = raw[SNAPSHOT_HEADER_LEN..].to_vec();
    let compressed_body = compressed[SNAPSHOT_HEADER_LEN..].to_vec();
    let pool = vec![raw_payload.clone()];

    let target = MemoryStorageEngine::new();
    let room = RoomId::new("target").unwrap();
    rt.block_on(target.open_room(&room, schema.clone()))
        .unwrap();

    for_each_case(seed, cases, |rng| {
        let envelope = match rng.below(4) {
            // A damaged envelope: most fail its checksum.
            0 => {
                let mut envelope = if rng.coin() {
                    raw.clone()
                } else {
                    compressed.clone()
                };
                mutate(rng, &mut envelope, &pool);
                envelope
            }
            // A damaged raw payload in a sealed envelope.
            1 => {
                let mut payload = raw_payload.clone();
                mutate(rng, &mut payload, &pool);
                sealed_envelope(SnapshotCompression::Raw, &payload, payload.len() as u32)
            }
            // A damaged payload, compressed and sealed, now and then with a wrong length.
            2 => {
                let mut payload = raw_payload.clone();
                mutate(rng, &mut payload, &pool);
                let body = zstd::encode_all(&payload[..], 1).unwrap();
                let declared = if rng.chance(1, 4) {
                    rng.next_u64() as u32 % (1 << 22)
                } else {
                    payload.len() as u32
                };
                sealed_envelope(SnapshotCompression::Zstd, &body, declared)
            }
            // A damaged compressed body, sealed.
            _ => {
                let mut body = compressed_body.clone();
                mutate(rng, &mut body, &[]);
                let declared = if rng.chance(1, 4) {
                    rng.next_u64() as u32
                } else {
                    raw_payload.len() as u32
                };
                sealed_envelope(SnapshotCompression::Zstd, &body, declared)
            }
        };

        if let Err(error) = decode_snapshot_envelope(&envelope) {
            assert!(
                matches!(error, StorageError::SnapshotCorruption(_)),
                "{error:?}"
            );
        }
        let applied = rt.block_on(target.apply_snapshot(
            &room,
            schema.clone(),
            Bytes::from(envelope.clone()),
            head,
        ));
        match applied {
            Ok(applied) => {
                assert_eq!(applied, head);
                assert!(validate_snapshot_envelope(&envelope).is_ok());
                rt.block_on(async {
                    // Every row accepted is valid under the schema.
                    for table in schema.tables() {
                        for (pk, row) in dump(&target, &room, &[table.table_id()]).await.remove(0).1
                        {
                            table.validate_keyed_row(&pk, &row).unwrap();
                        }
                    }
                    // Start again from an empty room, so that the next snapshot is not a no-op.
                    target.close_room(&room).await.unwrap();
                    target.open_room(&room, schema.clone()).await.unwrap();
                });
            }
            // A panic while decoding on the blocking pool would come back as `Other`.
            Err(error) => assert!(
                matches!(
                    error,
                    StorageError::SnapshotCorruption(_) | StorageError::SnapshotHeadMismatch { .. }
                ),
                "{error:?}"
            ),
        }
    });
}

#[test]
fn damaged_snapshots_never_panic_apply_snapshot() {
    check_damaged_snapshots(0xDA5_0001, 2_000);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn damaged_snapshots_never_panic_apply_snapshot_long() {
    check_damaged_snapshots(0xDA5_0002, 400_000);
}

/// Rewrites the checksums of a damaged `.snap` file (the payload's, if the length it declares
/// fits, then the header's), so that the damage reaches the decoder. The header layout is that
/// of `zemdb_storage::format::FileHeader`.
fn seal_snapshot_file(file: &mut [u8]) {
    if file.len() < HEADER_SIZE {
        return;
    }
    let len = u64::from_le_bytes(file[24..32].try_into().unwrap());
    if let Some(payload) = (HEADER_SIZE as u64)
        .checked_add(len)
        .filter(|end| *end <= file.len() as u64)
        .map(|end| &file[HEADER_SIZE..end as usize])
    {
        let crc = crc32fast::hash(payload);
        file[32..36].copy_from_slice(&crc.to_le_bytes());
    }
    let header_crc = crc32fast::hash(&file[..36]);
    file[36..40].copy_from_slice(&header_crc.to_le_bytes());
}

fn check_damaged_room_files(seed: u64, cases: u64) {
    let rt = runtime();
    let schema = sample_schema();
    let room = RoomId::new("room").unwrap();
    let ops = sample_ops();

    // A snapshot of the first operations and a WAL of one frame per operation after them.
    let base = tempfile::tempdir().unwrap();
    let (snapshot, wal) = rt.block_on(async {
        let engine = disk_engine(base.path());
        engine.open_room(&room, schema.clone()).await.unwrap();
        engine.apply_batch(&room, ops[..20].to_vec()).await.unwrap();
        engine.compact_room(&room).await.unwrap();
        for op in &ops[20..] {
            engine.apply_batch(&room, vec![op.clone()]).await.unwrap();
        }
        engine.close_room(&room).await.unwrap();
        (
            std::fs::read(engine.snap_file_path(&room)).unwrap(),
            std::fs::read(engine.wal_file_path(&room)).unwrap(),
        )
    });
    assert!(snapshot.len() > HEADER_SIZE && !wal.is_empty());

    for_each_case(seed, cases, |rng| {
        let mut snapshot = snapshot.clone();
        let mut wal = wal.clone();
        // The damage goes to the snapshot, the WAL, both, or a WAL left by an interrupted
        // compaction (`.wal.compacting`).
        let target = rng.below(4);
        if target == 0 || target == 3 {
            mutate(rng, &mut snapshot, &[]);
            if rng.chance(2, 3) {
                seal_snapshot_file(&mut snapshot);
            }
        }
        if target != 0 {
            mutate(rng, &mut wal, &[]);
        }

        let dir = tempfile::tempdir().unwrap();
        let engine = disk_engine(dir.path());
        std::fs::write(engine.snap_file_path(&room), &snapshot).unwrap();
        if target == 2 {
            let mut compacting = engine.wal_file_path(&room).into_os_string();
            compacting.push(".compacting");
            std::fs::write(compacting, &wal).unwrap();
            std::fs::write(engine.wal_file_path(&room), b"").unwrap();
        } else {
            std::fs::write(engine.wal_file_path(&room), &wal).unwrap();
        }

        rt.block_on(async {
            match engine.open_room(&room, schema.clone()).await {
                Ok(()) => {
                    for table_id in schema.table_ids() {
                        dump(&engine, &room, &[table_id]).await;
                    }
                    engine.close_room(&room).await.unwrap();
                }
                // A panic while decoding on the blocking pool would come back as `Other`.
                Err(error) => assert!(
                    matches!(
                        error,
                        StorageError::SnapshotCorruption(_)
                            | StorageError::WalCorruption(_)
                            | StorageError::SchemaMismatch { .. }
                    ),
                    "{error:?}"
                ),
            }
        });
    });
}

#[test]
fn damaged_room_files_never_panic_open_room() {
    check_damaged_room_files(0xDA5_1001, 100);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn damaged_room_files_never_panic_open_room_long() {
    check_damaged_room_files(0xDA5_1002, 2_000);
}
