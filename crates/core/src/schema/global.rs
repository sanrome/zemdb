use super::column::ColumnDef;
use super::constraint::{ForeignKey, UniqueConstraint};
use super::table::{TableBuilder, TableSchema};
use super::validation::ValidationError;
use crate::mutation::Operation;
use crate::value::{PrimaryKey, Row, Value};
use serde::de;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};

/// Builder for constructing Schema instances programmatically.
///
/// The only place where table ids are assigned automatically: a table added without an
/// explicit id ([`TableBuilder::table_id`]) gets the id after the largest one in the schema so
/// far, or 0 for the first table. An explicit id is kept exactly as given, and an id or a name
/// that is already taken is an error; no table is ever renumbered.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    schema: Schema,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and adds a table, assigning the next free id if the builder has none.
    ///
    /// A foreign key may reference a table that is added later: what depends on other
    /// tables is checked by [`SchemaBuilder::try_build`].
    pub fn try_table(mut self, table: TableBuilder) -> Result<Self, ValidationError> {
        let table_id = match table.explicit_table_id() {
            Some(id) => id,
            None => self.schema.next_table_id()?,
        };
        let table = table.build_with_id(table_id)?;
        self.schema.insert_table_unresolved(table)?;
        Ok(self)
    }

    pub fn table(self, table: TableBuilder) -> Self {
        self.try_table(table).expect("valid table schema")
    }

    /// Builds the schema, validating what depends on more than one table: every foreign key
    /// references an existing table and matches its primary key, and constraint names are
    /// unique.
    pub fn try_build(mut self) -> Result<Schema, ValidationError> {
        self.schema.referenced_by = self.schema.resolve_constraints(None)?;
        Ok(self.schema)
    }

    /// Builds the schema. Panics where [`SchemaBuilder::try_build`] fails.
    pub fn build(self) -> Schema {
        self.try_build().expect("valid schema")
    }
}

/// Global database schema containing all tables in a Room with bidirectional ID/Name indexing.
///
/// Both maps are private and change only through [`Schema::insert_table`],
/// [`Schema::add_column`] and [`Schema::add_column_with_constraints`], which keep them
/// consistent: every table is stored under its own `table_id`, ids and names are unique, and
/// `id_by_name` indexes exactly the stored tables.
///
/// The `version` starts at [`Schema::FIRST_VERSION`] and every [`Schema::add_column`] adds 1,
/// so a client that sees a version other than its own knows its schema is outdated.
///
/// Serialized form (JSON and binary): `{"version": n, "tables": [table, ...]}`, the tables in
/// ascending `table_id` order, each carrying its own `table_id`; JSON without `version` is
/// version 1, and version 0 is an error. Deserialization inserts the tables through the same
/// strict path: a repeated id or name, a table without an id, or an unknown field is an error,
/// and no id is ever assigned or changed while reading.
///
/// Constraints are part of the tables. The schema checks what depends on more than one table
/// whenever its tables change, and precomputes, for each table, the foreign keys that
/// reference it (`referenced_by`, rebuilt on deserialization like `id_by_name`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema {
    version: u64,
    tables_by_id: BTreeMap<u16, TableSchema>,
    id_by_name: BTreeMap<String, u16>,
    referenced_by: BTreeMap<u16, Vec<(u16, usize)>>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            version: Self::FIRST_VERSION,
            tables_by_id: BTreeMap::new(),
            id_by_name: BTreeMap::new(),
            referenced_by: BTreeMap::new(),
        }
    }
}

impl Serialize for Schema {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Schema", 2)?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("tables", &self.tables_by_id.values().collect::<Vec<_>>())?;
        state.end()
    }
}

fn first_version() -> u64 {
    Schema::FIRST_VERSION
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "Schema", deny_unknown_fields)]
        struct SchemaHelper {
            #[serde(default = "first_version")]
            version: u64,
            tables: Vec<TableSchema>,
        }
        let helper = SchemaHelper::deserialize(deserializer)?;
        if helper.version == 0 {
            return Err(de::Error::custom("Schema versions start at 1"));
        }
        let mut schema = Schema::try_from_tables(helper.tables).map_err(de::Error::custom)?;
        schema.version = helper.version;
        Ok(schema)
    }
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    /// Version of a new schema.
    pub const FIRST_VERSION: u64 = 1;

    /// The schema's version: [`Schema::FIRST_VERSION`] when created, plus 1 per `add_column`.
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Validates and constructs a Schema from tables that already carry their ids.
    ///
    /// Each table keeps its own `table_id`; a repeated id or name is an error.
    pub fn try_from_tables(
        tables: impl IntoIterator<Item = TableSchema>,
    ) -> Result<Self, ValidationError> {
        let mut schema = Schema::new();
        for table in tables {
            schema.insert_table_unresolved(table)?;
        }
        schema.referenced_by = schema.resolve_constraints(None)?;
        Ok(schema)
    }

    /// Constructs a Schema from tables that already carry their ids. Panics on a repeated id
    /// or name; see [`Schema::try_from_tables`].
    pub fn from_tables(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self::try_from_tables(tables).expect("valid table schemas")
    }

    /// Adds `table` under its own `table_id`. Fails, changing nothing, if the id or the name
    /// is already used by another table, if one of its foreign keys references a table that
    /// is not in the schema (itself aside) or does not match it, or if one of its constraint
    /// names is already used. A table can thus only be inserted after the tables it
    /// references; [`Schema::try_from_tables`] and [`SchemaBuilder`] take them in any order.
    pub fn insert_table(&mut self, table: TableSchema) -> Result<(), ValidationError> {
        self.check_new_table(&table)?;
        let referenced_by = self.resolve_constraints(Some(&table))?;
        self.insert_table_unresolved(table)?;
        self.referenced_by = referenced_by;
        Ok(())
    }

    /// Adds `table` like [`Schema::insert_table`], leaving the checks that depend on other
    /// tables to [`Schema::resolve_constraints`], which the caller runs once every table is in.
    fn insert_table_unresolved(&mut self, table: TableSchema) -> Result<(), ValidationError> {
        self.check_new_table(&table)?;
        self.id_by_name
            .insert(table.name().to_string(), table.table_id());
        self.tables_by_id.insert(table.table_id(), table);
        Ok(())
    }

    /// Rejects a table whose id or name is already used by another table.
    fn check_new_table(&self, table: &TableSchema) -> Result<(), ValidationError> {
        if self.id_by_name.contains_key(table.name()) {
            return Err(ValidationError::DuplicateTable {
                table: table.name().to_string(),
            });
        }
        if self.tables_by_id.contains_key(&table.table_id()) {
            return Err(ValidationError::DuplicateTableId {
                table: table.name().to_string(),
                table_id: table.table_id(),
            });
        }
        Ok(())
    }

    /// Appends a nullable column to the table `table_name` (append-only schema evolution, see
    /// [`TableSchema::add_column`]). Returns the column's positional index.
    pub fn add_column(&mut self, table_name: &str, col: ColumnDef) -> Result<u16, ValidationError> {
        let table = self
            .id_by_name
            .get(table_name)
            .and_then(|id| self.tables_by_id.get_mut(id))
            .ok_or_else(|| ValidationError::TableNotFound(table_name.to_string()))?;
        let index = table.add_column(col)?;
        self.version += 1;
        Ok(index)
    }

    /// Appends a nullable column to the table `table_name` together with constraints that
    /// include it, and returns the column's positional index.
    ///
    /// Each `UNIQUE` constraint has the new column among its columns, and so does each
    /// foreign key among the referencing ones: a constraint over existing columns only would
    /// also judge the operations sequenced before it existed. The version goes up by one.
    /// On any error nothing changes, the version included.
    pub fn add_column_with_constraints(
        &mut self,
        table_name: &str,
        col: ColumnDef,
        unique: Vec<UniqueConstraint>,
        foreign_keys: Vec<ForeignKey>,
    ) -> Result<u16, ValidationError> {
        let table = self.require_table(table_name)?;
        let table_id = table.table_id();
        let table = table.with_added_column(col, unique, foreign_keys)?;
        let index = (table.columns().len() - 1) as u16;
        let referenced_by = self.resolve_constraints(Some(&table))?;
        self.tables_by_id.insert(table_id, table);
        self.referenced_by = referenced_by;
        self.version += 1;
        Ok(index)
    }

    /// Checks what depends on more than one table, in the schema as it would be with
    /// `candidate` added (or replacing the table with its id), and returns the `referenced_by`
    /// it would have. Changes nothing, so the caller validates before mutating.
    ///
    /// Constraint names are unique in the schema, and every foreign key references a table of
    /// the schema whose primary key matches its columns in count and, in order, in type.
    fn resolve_constraints(
        &self,
        candidate: Option<&TableSchema>,
    ) -> Result<BTreeMap<u16, Vec<(u16, usize)>>, ValidationError> {
        let mut tables: BTreeMap<u16, &TableSchema> =
            self.tables_by_id.iter().map(|(&id, t)| (id, t)).collect();
        if let Some(candidate) = candidate {
            tables.insert(candidate.table_id(), candidate);
        }
        let by_name: BTreeMap<&str, &TableSchema> =
            tables.values().map(|&t| (t.name(), t)).collect();
        let mut names = BTreeSet::new();
        let mut referenced_by: BTreeMap<u16, Vec<(u16, usize)>> = BTreeMap::new();
        for &table in tables.values() {
            let constraint_names = table
                .unique_constraints()
                .iter()
                .map(|c| c.name())
                .chain(table.foreign_keys().iter().map(|fk| fk.name()));
            for name in constraint_names {
                if !names.insert(name) {
                    return Err(ValidationError::DuplicateConstraintName {
                        constraint: name.to_string(),
                    });
                }
            }
            let foreign_keys = table
                .foreign_keys()
                .iter()
                .zip(table.foreign_key_column_indices());
            for (index, (fk, positions)) in foreign_keys.enumerate() {
                let &target = by_name.get(fk.references()).ok_or_else(|| {
                    ValidationError::ForeignKeyTableNotFound {
                        table: table.name().to_string(),
                        constraint: fk.name().to_string(),
                        references: fk.references().to_string(),
                    }
                })?;
                if positions.len() != target.primary_key().len() {
                    return Err(ValidationError::ForeignKeyArityMismatch {
                        table: table.name().to_string(),
                        constraint: fk.name().to_string(),
                        expected: target.primary_key().len(),
                        actual: positions.len(),
                    });
                }
                for (&position, key_column) in positions.iter().zip(target.primary_key()) {
                    let column = &table.columns()[position];
                    let expected = target
                        .get_column(key_column)
                        .map(ColumnDef::data_type)
                        .ok_or_else(|| ValidationError::UnknownColumn {
                            table: target.name().to_string(),
                            column: key_column.clone(),
                        })?;
                    if column.data_type() != expected {
                        return Err(ValidationError::ForeignKeyTypeMismatch {
                            table: table.name().to_string(),
                            constraint: fk.name().to_string(),
                            column: column.name().to_string(),
                            expected,
                            actual: column.data_type(),
                        });
                    }
                }
                referenced_by
                    .entry(target.table_id())
                    .or_default()
                    .push((table.table_id(), index));
            }
        }
        Ok(referenced_by)
    }

    /// The foreign keys that reference the table `table_id`, as each referencing table and
    /// the position of the foreign key in [`TableSchema::foreign_keys`]: by ascending id of
    /// the referencing table (the referenced table itself included) and, within it, in
    /// declaration order.
    pub fn foreign_keys_referencing(
        &self,
        table_id: u16,
    ) -> impl Iterator<Item = (&TableSchema, usize)> + '_ {
        self.referenced_by
            .get(&table_id)
            .into_iter()
            .flatten()
            .filter_map(|&(child, index)| self.tables_by_id.get(&child).map(|t| (t, index)))
    }

    /// The id a table added without an explicit one receives: one past the largest id.
    fn next_table_id(&self) -> Result<u16, ValidationError> {
        match self.tables_by_id.keys().next_back() {
            None => Ok(0),
            Some(max) => max.checked_add(1).ok_or(ValidationError::TableIdOverflow),
        }
    }

    /// The tables of the schema in ascending `table_id` order.
    pub fn tables(&self) -> impl Iterator<Item = &TableSchema> + '_ {
        self.tables_by_id.values()
    }

    /// The table ids of the schema in ascending order.
    pub fn table_ids(&self) -> impl Iterator<Item = u16> + '_ {
        self.tables_by_id.keys().copied()
    }

    pub fn table_count(&self) -> usize {
        self.tables_by_id.len()
    }

    pub fn get_table_by_id(&self, table_id: u16) -> Option<&TableSchema> {
        self.tables_by_id.get(&table_id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableSchema> {
        let id = self.id_by_name.get(name)?;
        self.tables_by_id.get(id)
    }

    pub fn get_table_id(&self, name: &str) -> Option<u16> {
        self.id_by_name.get(name).copied()
    }

    pub fn has_table_by_id(&self, table_id: u16) -> bool {
        self.tables_by_id.contains_key(&table_id)
    }

    pub fn has_table_by_name(&self, name: &str) -> bool {
        self.id_by_name.contains_key(name)
    }

    /// The table named `name`, or `TableNotFound`.
    fn require_table(&self, name: &str) -> Result<&TableSchema, ValidationError> {
        self.get_table_by_name(name)
            .ok_or_else(|| ValidationError::TableNotFound(name.to_string()))
    }

    pub fn validate_insert(&self, table: &str, row: &Row) -> Result<PrimaryKey, ValidationError> {
        let t = self.require_table(table)?;
        t.validate_row(row)?;
        t.extract_pk(row)
    }

    pub fn validate_update(
        &self,
        table: &str,
        pk: &PrimaryKey,
        fields: &BTreeMap<String, Value>,
    ) -> Result<(), ValidationError> {
        let t = self.require_table(table)?;
        t.validate_pk(pk)?;
        t.validate_update(fields)
    }

    pub fn validate_delete(&self, table: &str, pk: &PrimaryKey) -> Result<(), ValidationError> {
        self.require_table(table)?.validate_pk(pk)
    }

    /// Validates an Operation against the corresponding TableSchema by table_id.
    pub fn validate_operation(&self, op: &Operation) -> Result<(), ValidationError> {
        let t = self
            .get_table_by_id(op.table_id)
            .ok_or_else(|| ValidationError::TableNotFound(format!("id:{}", op.table_id)))?;
        t.validate_operation(op)
    }

    pub fn to_operation_insert(
        &self,
        table: &str,
        row: &Row,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        self.require_table(table)?
            .to_operation_insert(row, timestamp)
    }

    pub fn to_operation_update(
        &self,
        table: &str,
        pk: PrimaryKey,
        fields: &BTreeMap<String, Value>,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        self.require_table(table)?
            .to_operation_update(pk, fields, timestamp)
    }

    pub fn to_operation_delete(
        &self,
        table: &str,
        pk: PrimaryKey,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        self.require_table(table)?
            .to_operation_delete(pk, timestamp)
    }
}
