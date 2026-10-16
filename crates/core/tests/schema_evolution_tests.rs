//! Schemas only grow: adding a column leaves every table, key and existing column as it was, so
//! an operation valid under a version of a schema stays valid under every later one, and a
//! column that the rows already written would lack is refused without changing anything.
//!
//! The ignored variant runs many more cases: `cargo test --release -- --ignored`.

mod common;

use common::*;
use zemdb_core::*;

/// A table as its id, its columns as (type, nullable, encrypted) and its key width: the key is
/// its first columns. Columns are named `c0`, `c1`, ...
type TableSpec = (u16, Vec<(DataType, bool, bool)>, usize);

fn table_specs(rng: &mut Rng) -> Vec<TableSpec> {
    (0..rng.between(1, 4))
        .map(|_| {
            let columns = (0..rng.between(1, 6))
                .map(|_| (data_type(rng), rng.coin(), rng.coin()))
                .collect();
            (rng.below(6) as u16, columns, rng.between(1, 3))
        })
        .collect()
}

/// The schema of `specs`, skipping a table whose id is taken. Key columns are neither nullable
/// nor encrypted.
fn build_schema(specs: &[TableSpec]) -> Schema {
    let mut tables = vec![];
    let mut ids = std::collections::BTreeSet::new();
    for (i, (id, columns, key_len)) in specs.iter().enumerate() {
        if !ids.insert(*id) {
            continue;
        }
        let key_len = (*key_len).min(columns.len());
        let columns = columns
            .iter()
            .enumerate()
            .map(|(c, (data_type, nullable, encrypted))| {
                let in_key = c < key_len;
                ColumnDef::new(format!("c{c}"), *data_type)
                    .nullable(*nullable && !in_key)
                    .encrypted(*encrypted && !in_key)
            })
            .collect();
        let key = (0..key_len).map(|c| format!("c{c}")).collect();
        tables.push(TableSchema::try_new(*id, format!("t{i}"), key, columns).unwrap());
    }
    Schema::try_from_tables(tables).unwrap()
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
/// deletes. Keys repeat often, so later operations hit rows written by earlier ones.
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
                    let updates: Vec<ColumnUpdate> = columns
                        .iter()
                        .enumerate()
                        .skip(key_len)
                        .filter(|(i, _)| (seed >> i) & 1 == 1)
                        .map(|(i, column)| {
                            ColumnUpdate::new(i as u16, cell(column, seed ^ i as u64))
                        })
                        .collect();
                    if updates.is_empty() {
                        Operation::delete(table.table_id(), pk, seed)
                    } else {
                        Operation::update(table.table_id(), pk, updates, seed)
                    }
                }
                _ => Operation::delete(table.table_id(), pk, seed),
            }
        })
        .collect()
}

fn operation_seeds(rng: &mut Rng) -> Vec<(u8, u64, u8)> {
    (0..rng.between(1, 30))
        .map(|_| (rng.next_u64() as u8, rng.next_u64(), rng.next_u64() as u8))
        .collect()
}

fn check_evolution(seed: u64, cases: u64) {
    for_each_case(seed, cases, |rng| {
        let v1 = build_schema(&table_specs(rng));
        let ops = operations_for(&v1, &operation_seeds(rng));
        for op in &ops {
            v1.validate_operation(op).unwrap();
        }

        let names: Vec<String> = v1.tables().map(|t| t.name().to_string()).collect();
        let mut evolved = v1.clone();
        for k in 0..rng.between(1, 6) {
            let table = rng.pick(&names).clone();
            let data_type = data_type(rng);
            let before = evolved.version();
            let column = ColumnDef::new(format!("added{k}"), data_type)
                .nullable(true)
                .encrypted(rng.coin());
            evolved.add_column(&table, column).unwrap();
            assert_eq!(evolved.version(), before + 1);

            // Nothing is lost, renumbered or retyped.
            for old in v1.tables() {
                let new = evolved.get_table_by_id(old.table_id()).unwrap();
                assert_eq!(new.name(), old.name());
                assert_eq!(new.primary_key(), old.primary_key());
                assert_eq!(&new.columns()[..old.columns().len()], old.columns());
            }
            for op in &ops {
                evolved.validate_operation(op).unwrap();
            }

            // A column the rows already written would lack is refused, and changes nothing.
            let mut refused = evolved.clone();
            assert!(matches!(
                refused.add_column(&table, ColumnDef::new("strict", data_type)),
                Err(ValidationError::AddedColumnMustBeNullable { .. })
            ));
            assert_eq!(refused, evolved);
        }

        // The evolved schema survives JSON with its version.
        let json = serde_json::to_string(&evolved).unwrap();
        let back = serde_json::from_str::<Schema>(&json).unwrap();
        assert_eq!(back, evolved);
        assert_eq!(back.version(), evolved.version());
    });
}

#[test]
fn operations_stay_valid_under_every_later_schema_version() {
    check_evolution(0xE1_0001, 1_500);
}

#[test]
#[ignore = "long-running randomized test; run with --ignored in release"]
fn operations_stay_valid_under_every_later_schema_version_long() {
    check_evolution(0xE1_0002, 200_000);
}
