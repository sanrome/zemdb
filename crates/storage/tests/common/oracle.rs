//! A reference model of a replica: the rules that decide whether an operation of the log is
//! applied or rejected, written from their specification and sharing no code with the storage
//! engines. It keeps the rows in ordered maps and checks every rule by walking whole tables,
//! without indexes, and it uses only the types of `zemdb-core`.
//!
//! Also here: a generator of random operations over small domains, so that keys collide often,
//! and the schemas the randomized tests use. The unit tests of the crate include this file too.

use super::rng::Rng;
use std::collections::{BTreeMap, BTreeSet};
use zemdb_core::{
    ColumnDef, ColumnUpdate, CompactRow, DataType, ForeignKey, Operation, OperationKind,
    OwnerIdentity, PrimaryKey, Schema, SequencedOperation, TableSchema, UniqueConstraint, Value,
};

/// Why the model did not apply an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    MissingRow,
    Duplicate,
    NotOwner,
    Unique(String),
    ForeignKey(String),
    Restrict(String),
}

/// The rows of every table, by table id and primary key.
pub type Rows = BTreeMap<u16, BTreeMap<PrimaryKey, CompactRow>>;

/// A replica as the specification describes it.
#[derive(Debug, Clone)]
pub struct Oracle {
    schema: Schema,
    head: u64,
    rows: Rows,
}

impl Oracle {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            head: 0,
            rows: Rows::new(),
        }
    }

    /// Applies the next operations under `schema`, a later version of the current one.
    pub fn evolve(&mut self, schema: Schema) {
        self.schema = schema;
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn rows(&self) -> &Rows {
        &self.rows
    }

    /// Applies `ops` in order, each on the state the previous one left, and returns the sequence
    /// and the reason of every operation it did not apply.
    pub fn apply_batch(&mut self, ops: &[SequencedOperation]) -> Vec<(u64, Reason)> {
        let mut rejected = Vec::new();
        for op in ops {
            if let Some(reason) = self.apply(&op.op) {
                rejected.push((op.seq.get(), reason));
            }
            self.head = op.seq.get();
        }
        rejected
    }

    /// Applies `op`, which must be valid under the schema, or rejects it and changes nothing.
    pub fn apply(&mut self, op: &Operation) -> Option<Reason> {
        let table = self
            .schema
            .get_table_by_id(op.table_id())
            .expect("an operation of a table of the schema");
        let pk = op.pk();
        let existing = self.table_rows(op.table_id()).get(pk);
        let result = match op.kind() {
            OperationKind::Insert { row } => Some(row.clone()),
            OperationKind::Update { updates } => {
                let Some(existing) = existing else {
                    return Some(Reason::MissingRow);
                };
                let mut row = existing.clone();
                for update in updates {
                    let column = usize::from(update.column_idx());
                    let width = table.columns().len().max(column + 1);
                    if row.len() < width {
                        row.resize(width, Value::Null);
                    }
                    row[column] = update.value().clone();
                }
                Some(row)
            }
            // A delete of a row that does not exist changes nothing and is not reported.
            OperationKind::Delete { .. } if existing.is_none() => return None,
            OperationKind::Delete { .. } => None,
        };
        if let Some(reason) = self.check(table, op, existing, result.as_ref()) {
            return Some(reason);
        }
        let rows = self.rows.entry(op.table_id()).or_default();
        match result {
            Some(row) => {
                rows.remove(pk);
                rows.insert(pk.clone(), row);
            }
            None => {
                rows.remove(pk);
            }
        }
        None
    }

    fn table_rows(&self, table_id: u16) -> &BTreeMap<PrimaryKey, CompactRow> {
        static EMPTY: BTreeMap<PrimaryKey, CompactRow> = BTreeMap::new();
        self.rows.get(&table_id).unwrap_or(&EMPTY)
    }

    /// The first rule that `op` breaks, in the order the specification fixes: an existing key
    /// of an immutable table, another owner, the `UNIQUE` constraints and the foreign keys of
    /// the table in declaration order; for a delete, the foreign keys that reference the table,
    /// by id of the referencing table and in declaration order. `result` is the row the
    /// operation leaves, `None` for a delete of an existing row.
    fn check(
        &self,
        table: &TableSchema,
        op: &Operation,
        existing: Option<&CompactRow>,
        result: Option<&CompactRow>,
    ) -> Option<Reason> {
        let pk = op.pk();
        if table.is_immutable() && op.is_insert() && existing.is_some() {
            return Some(Reason::Duplicate);
        }
        if let (Some(column), Some(existing)) = (owner_outside_key(table), existing) {
            let owner = existing.get(column);
            if owner != declared_owner(op, column) {
                return Some(Reason::NotOwner);
            }
        }
        let Some(row) = result else {
            return self.restrict(table, pk);
        };
        for unique in table.unique_constraints() {
            if !touches(op, table, unique.columns()) {
                continue;
            }
            let Some(key) = key_of(table, row, unique.columns()) else {
                continue;
            };
            let clash = self
                .table_rows(table.table_id())
                .iter()
                .any(|(other, other_row)| {
                    other != pk && key_of(table, other_row, unique.columns()).as_ref() == Some(&key)
                });
            if clash {
                return Some(Reason::Unique(unique.name().to_string()));
            }
        }
        for foreign_key in table.foreign_keys() {
            if !touches(op, table, foreign_key.columns()) {
                continue;
            }
            let Some(key) = key_of(table, row, foreign_key.columns()) else {
                continue;
            };
            let parent = self
                .schema
                .get_table_by_name(foreign_key.references())
                .expect("a referenced table of the schema");
            if parent.table_id() == table.table_id() && &key == pk {
                continue;
            }
            let found = self
                .table_rows(parent.table_id())
                .keys()
                .any(|parent_pk| *parent_pk == key);
            if !found {
                return Some(Reason::ForeignKey(foreign_key.name().to_string()));
            }
        }
        None
    }

    /// The first foreign key through which another row references the row `pk` of `table`.
    fn restrict(&self, table: &TableSchema, pk: &PrimaryKey) -> Option<Reason> {
        for child in self.schema.tables() {
            for foreign_key in child.foreign_keys() {
                if foreign_key.references() != table.name() {
                    continue;
                }
                let same_table = child.table_id() == table.table_id();
                let referenced = self
                    .table_rows(child.table_id())
                    .iter()
                    .any(|(child_pk, row)| {
                        !(same_table && child_pk == pk)
                            && key_of(child, row, foreign_key.columns()).as_ref() == Some(pk)
                    });
                if referenced {
                    return Some(Reason::Restrict(foreign_key.name().to_string()));
                }
            }
        }
        None
    }
}

/// The position of the owner column of `table`, if it is outside the primary key.
fn owner_outside_key(table: &TableSchema) -> Option<usize> {
    let owner = table.owner()?.column();
    if table.primary_key().iter().any(|column| column == owner) {
        return None;
    }
    table.column_index(owner)
}

/// The owner that `op` declares, with the owner column at `column` outside the key.
fn declared_owner(op: &Operation, column: usize) -> Option<&Value> {
    match op.kind() {
        OperationKind::Insert { row } => row.get(column),
        OperationKind::Update { updates } => updates
            .iter()
            .find(|update| usize::from(update.column_idx()) == column)
            .map(ColumnUpdate::value),
        OperationKind::Delete { owner } => owner.as_ref(),
    }
}

/// Whether `op` checks a constraint over `columns`: an insert checks all of them, an update
/// those with a column among its deltas.
fn touches(op: &Operation, table: &TableSchema, columns: &[String]) -> bool {
    match op.kind() {
        OperationKind::Update { updates } => columns.iter().any(|name| {
            let position = table.column_index(name);
            updates
                .iter()
                .any(|update| Some(usize::from(update.column_idx())) == position)
        }),
        _ => true,
    }
}

/// The values of `columns` in `row`, in that order, or `None` if one of them is `NULL` or
/// missing from a short row.
fn key_of(table: &TableSchema, row: &CompactRow, columns: &[String]) -> Option<PrimaryKey> {
    let mut values = Vec::new();
    for name in columns {
        let position = table.column_index(name).expect("a column of the table");
        match row.get(position) {
            Some(value) if !value.is_null() => values.push(value.clone()),
            _ => return None,
        }
    }
    Some(PrimaryKey::composite(values))
}

// ---------------------------------------------------------------------------------------------
// Comparing states

/// A value with its float bits spelled out, so that 0.0 and -0.0, or two NaNs, differ.
pub fn bits(value: &Value) -> String {
    match value {
        Value::Float(f) => format!("F{:016x}", f.to_bits()),
        other => format!("{other:?}"),
    }
}

/// A key value: like [`bits`], but both zeros are the same key, and which of them a stored key
/// keeps is not part of the state (the row holds the key columns as written).
fn key_bits(value: &Value) -> String {
    match value {
        Value::Float(f) if *f == 0.0 => "F0".to_string(),
        other => bits(other),
    }
}

/// Every row of `rows`, in order, as `(table, key, row)`, with floats as bits.
pub fn dump(rows: &Rows) -> Vec<(u16, Vec<String>, Vec<String>)> {
    rows.iter()
        .flat_map(|(&table, rows)| {
            rows.iter().map(move |(pk, row)| {
                (
                    table,
                    pk.iter().map(key_bits).collect(),
                    row.iter().map(bits).collect(),
                )
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Random operations

/// The owners of the owner columns: two clients.
const OWNERS: [&str; 2] = ["A", "B"];

/// A float whose bits matter: both zeros, two NaNs and a plain number.
fn random_float(rng: &mut Rng) -> f64 {
    match rng.below(5) {
        0 => 0.0,
        1 => -0.0,
        2 => f64::NAN,
        3 => f64::from_bits(f64::NAN.to_bits() | 1),
        _ => 1.5,
    }
}

/// One of four values of `data_type` (integers from -1, so that keys sort on both sides of
/// zero), or of the two owners for an owner column.
fn random_plain_value(rng: &mut Rng, data_type: DataType, is_owner: bool) -> Value {
    if is_owner {
        return Value::String(OWNERS[rng.below(2) as usize].into());
    }
    match data_type {
        DataType::Int => Value::Int(rng.below(4) as i64 - 1),
        DataType::String => Value::String(format!("s{}", rng.below(4)).into()),
        DataType::Float => Value::Float(random_float(rng)),
        DataType::Bool => Value::Bool(rng.below(2) == 0),
        DataType::Bytes => Value::Bytes(vec![rng.below(4) as u8].into()),
        other => panic!("no random values of type {other:?}"),
    }
}

/// A value for the column at `position` of `table`: `NULL` a quarter of the time if it is
/// nullable.
fn random_value(rng: &mut Rng, table: &TableSchema, position: usize) -> Value {
    let column = &table.columns()[position];
    if column.is_nullable() && rng.chance(25) {
        return Value::Null;
    }
    let is_owner = table.owner().map(|owner| owner.column()) == Some(column.name());
    random_plain_value(rng, column.data_type(), is_owner)
}

/// An insert (of a row that may be short, and may reference itself), an update of one or two
/// columns (with the owner, if the table declares it there) or a delete (with the owner, if the
/// table declares it there), of one of a few keys of a random table of `schema`. An immutable
/// table only gets inserts. The owner declared is either client, so some operations claim rows
/// of another owner.
pub fn random_operation(rng: &mut Rng, schema: &Schema) -> Operation {
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
        .map(|&position| random_value(rng, table, position))
        .collect();
    let pk = PrimaryKey::composite(key.clone());
    let owner = owner_outside_key(table);
    let others: Vec<usize> = (0..columns.len())
        .filter(|i| !key_columns.contains(i) && Some(*i) != owner)
        .collect();
    let roll = if table.is_immutable() {
        0
    } else {
        rng.below(10)
    };
    match roll {
        0..=4 => {
            let required = (0..columns.len())
                .filter(|&i| !columns[i].is_nullable())
                .max()
                .unwrap_or(0);
            let width = required + 1 + rng.below((columns.len() - required) as u64) as usize;
            let mut values: Vec<Value> = (0..width)
                .map(|i| match key_columns.iter().position(|&k| k == i) {
                    Some(k) => key[k].clone(),
                    None => random_value(rng, table, i),
                })
                .collect();
            for foreign_key in table.foreign_keys() {
                if foreign_key.references() == table.name() && rng.chance(25) {
                    for (name, value) in foreign_key.columns().iter().zip(&key) {
                        let position = table.column_index(name).unwrap();
                        if position < width {
                            values[position] = value.clone();
                        }
                    }
                }
            }
            Operation::insert(table.table_id(), pk, CompactRow::new(values), 0)
        }
        5..=7 => {
            let mut chosen = BTreeSet::new();
            if let Some(owner) = owner {
                chosen.insert(owner);
            }
            if !others.is_empty() {
                for _ in 0..=rng.below(2) {
                    chosen.insert(others[rng.below(others.len() as u64) as usize]);
                }
            }
            let updates = chosen
                .into_iter()
                .map(|i| ColumnUpdate::new(i as u16, random_value(rng, table, i)))
                .collect();
            Operation::update(table.table_id(), pk, updates, 0)
        }
        _ => match owner {
            Some(owner) => {
                let declared = random_value(rng, table, owner);
                Operation::delete_with_owner(table.table_id(), pk, declared, 0)
            }
            None => Operation::delete(table.table_id(), pk, 0),
        },
    }
}

// ---------------------------------------------------------------------------------------------
// Schemas

pub const PARENTS: u16 = 0;
pub const CHILDREN: u16 = 1;
pub const VOTES: u16 = 2;
pub const POSTS: u16 = 3;
pub const NOTES: u16 = 4;
pub const LEDGER: u16 = 5;
pub const PROFILES: u16 = 6;

/// Seven tables that between them have every kind of rule:
/// - `parents (id)`: `UNIQUE (code)` and `UNIQUE (rank, score)`, with a float and named in
///   another order than the table's;
/// - `children (a, b)`: a foreign key to `parents`, one from `(up_b, up_a)` to `children`
///   itself (named in another order than the table's) and `UNIQUE (x)`;
/// - `votes (voter, poll)`: immutable, owned by `voter`, inside the key, and a foreign key to
///   `parents`;
/// - `posts (id)`: owned by `author`, outside the key, with `UNIQUE (slug)` and a foreign key
///   to `notes`;
/// - `notes (k)`: keyed by a float, with a foreign key to `posts`, which closes a cycle;
/// - `ledger (id)`: immutable, owned by `holder`, outside the key, with `UNIQUE (amount)`;
/// - `profiles (id)`: its key is also a foreign key to `parents`, one profile per parent, so a
///   row's foreign key to another table can equal its own key.
///
/// Every other column is nullable, so rows may be short.
pub fn schema() -> Schema {
    let parents = TableSchema::builder("parents")
        .table_id(PARENTS)
        .primary_key("id", DataType::Int)
        .nullable_column("code", DataType::String)
        .nullable_column("score", DataType::Float)
        .nullable_column("rank", DataType::Int)
        .unique("parents_code", ["code"])
        .unique("parents_rank_score", ["rank", "score"]);
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
    let votes = TableSchema::builder("votes")
        .table_id(VOTES)
        .primary_key("voter", DataType::String)
        .primary_key("poll", DataType::Int)
        .nullable_column("choice", DataType::Int)
        .owner("voter", OwnerIdentity::ClientId)
        .immutable()
        .foreign_key("votes_poll", ["poll"], "parents");
    let posts = TableSchema::builder("posts")
        .table_id(POSTS)
        .primary_key("id", DataType::Int)
        .column("author", DataType::String)
        .nullable_column("slug", DataType::String)
        .nullable_column("note", DataType::Float)
        .owner("author", OwnerIdentity::ClientId)
        .unique("posts_slug", ["slug"])
        .foreign_key("posts_note", ["note"], "notes");
    let notes = TableSchema::builder("notes")
        .table_id(NOTES)
        .primary_key("k", DataType::Float)
        .nullable_column("post", DataType::Int)
        .foreign_key("notes_post", ["post"], "posts");
    let ledger = TableSchema::builder("ledger")
        .table_id(LEDGER)
        .primary_key("id", DataType::Int)
        .column("holder", DataType::String)
        .nullable_column("amount", DataType::Int)
        .owner("holder", OwnerIdentity::ClientId)
        .immutable()
        .unique("ledger_amount", ["amount"]);
    let profiles = TableSchema::builder("profiles")
        .table_id(PROFILES)
        .primary_key("id", DataType::Int)
        .nullable_column("bio", DataType::String)
        .foreign_key("profiles_parent", ["id"], "parents");
    Schema::builder()
        .table(parents)
        .table(children)
        .table(votes)
        .table(posts)
        .table(notes)
        .table(ledger)
        .table(profiles)
        .build()
}

/// [`schema`] with two columns added with constraints that include them: `posts.tag`, with
/// `UNIQUE (tag)`, and `children.alt`, with a foreign key to `parents`. Rows written before them
/// are short.
pub fn evolved_schema() -> Schema {
    let mut schema = schema();
    let tag = ColumnDef::new("tag", DataType::Int).nullable(true);
    let unique = vec![UniqueConstraint::new("posts_tag", ["tag"])];
    schema
        .add_column_with_constraints("posts", tag, unique, Vec::new())
        .unwrap();
    let alt = ColumnDef::new("alt", DataType::Int).nullable(true);
    let foreign = vec![ForeignKey::new("children_alt", ["alt"], "parents")];
    schema
        .add_column_with_constraints("children", alt, Vec::new(), foreign)
        .unwrap();
    schema
}

/// A random log of `len` operations from sequence `first`, valid under `schema`.
pub fn random_log(rng: &mut Rng, schema: &Schema, first: u64, len: u64) -> Vec<SequencedOperation> {
    (first..first + len)
        .map(|seq| {
            let op = random_operation(rng, schema);
            schema
                .validate_operation(&op)
                .expect("a valid random operation");
            SequencedOperation::new(seq, op)
        })
        .collect()
}
