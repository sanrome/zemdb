use super::column::ColumnDef;
use super::table::{TableBuilder, TableSchema};
use super::validation::ValidationError;
use crate::mutation::Operation;
use crate::value::{PrimaryKey, Row, Value};
use serde::de;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

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
    pub fn try_table(mut self, table: TableBuilder) -> Result<Self, ValidationError> {
        let table_id = match table.explicit_table_id() {
            Some(id) => id,
            None => self.schema.next_table_id()?,
        };
        let table = table.build_with_id(table_id)?;
        self.schema.insert_table(table)?;
        Ok(self)
    }

    pub fn table(self, table: TableBuilder) -> Self {
        self.try_table(table).expect("valid table schema")
    }

    pub fn build(self) -> Schema {
        self.schema
    }
}

/// Global database schema containing all tables in a Room with bidirectional ID/Name indexing.
///
/// Both maps are private and change only through [`Schema::insert_table`] and
/// [`Schema::add_column`], which keep them consistent: every table is stored under its own
/// `table_id`, ids and names are unique, and `id_by_name` indexes exactly the stored tables.
///
/// The `version` starts at [`Schema::FIRST_VERSION`] and every [`Schema::add_column`] adds 1,
/// so a client that sees a version other than its own knows its schema is outdated.
///
/// Serialized form (JSON and binary): `{"version": n, "tables": [table, ...]}`, the tables in
/// ascending `table_id` order, each carrying its own `table_id`; JSON without `version` is
/// version 1, and version 0 is an error. Deserialization inserts the tables through the same
/// strict path: a repeated id or name, a table without an id, or an unknown field is an error,
/// and no id is ever assigned or changed while reading.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema {
    version: u64,
    tables_by_id: BTreeMap<u16, TableSchema>,
    id_by_name: BTreeMap<String, u16>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            version: Self::FIRST_VERSION,
            tables_by_id: BTreeMap::new(),
            id_by_name: BTreeMap::new(),
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
            schema.insert_table(table)?;
        }
        Ok(schema)
    }

    /// Constructs a Schema from tables that already carry their ids. Panics on a repeated id
    /// or name; see [`Schema::try_from_tables`].
    pub fn from_tables(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self::try_from_tables(tables).expect("valid table schemas")
    }

    /// Adds `table` under its own `table_id`. Fails, changing nothing, if the id or the name
    /// is already used by another table.
    pub fn insert_table(&mut self, table: TableSchema) -> Result<(), ValidationError> {
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
        self.id_by_name
            .insert(table.name().to_string(), table.table_id());
        self.tables_by_id.insert(table.table_id(), table);
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
