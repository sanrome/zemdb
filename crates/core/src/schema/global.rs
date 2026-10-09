use super::column::ColumnDef;
use super::table::{TableBuilder, TableSchema};
use super::validation::{self, ValidationError};
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
/// Serialized form (JSON and binary): `{"tables": [table, ...]}`, in ascending `table_id`
/// order, each table carrying its own `table_id`. Deserialization inserts the tables through
/// the same strict path: a repeated id or name, a table without an id, or an unknown field is
/// an error, and no id is ever assigned or changed while reading.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Schema {
    tables_by_id: BTreeMap<u16, TableSchema>,
    id_by_name: BTreeMap<String, u16>,
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        struct Tables<'a>(&'a BTreeMap<u16, TableSchema>);

        impl Serialize for Tables<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_seq(self.0.values())
            }
        }

        let mut state = serializer.serialize_struct("Schema", 1)?;
        state.serialize_field("tables", &Tables(&self.tables_by_id))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "Schema", deny_unknown_fields)]
        struct SchemaHelper {
            tables: Vec<TableSchema>,
        }
        let helper = SchemaHelper::deserialize(deserializer)?;
        Schema::try_from_tables(helper.tables).map_err(de::Error::custom)
    }
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables_by_id: BTreeMap::new(),
            id_by_name: BTreeMap::new(),
        }
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
        table.add_column(col)
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

    /// Number of tables in the schema.
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

    pub fn validate_insert(&self, table: &str, row: &Row) -> Result<PrimaryKey, ValidationError> {
        validation::validate_insert(self, table, row)
    }

    pub fn validate_update(
        &self,
        table: &str,
        pk: &PrimaryKey,
        fields: &BTreeMap<String, Value>,
    ) -> Result<(), ValidationError> {
        validation::validate_update(self, table, pk, fields)
    }

    pub fn validate_delete(&self, table: &str, pk: &PrimaryKey) -> Result<(), ValidationError> {
        validation::validate_delete(self, table, pk)
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
        let t = self
            .get_table_by_name(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;
        t.to_operation_insert(row, timestamp)
    }

    pub fn to_operation_update(
        &self,
        table: &str,
        pk: PrimaryKey,
        fields: &BTreeMap<String, Value>,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        let t = self
            .get_table_by_name(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;
        t.to_operation_update(pk, fields, timestamp)
    }

    pub fn to_operation_delete(
        &self,
        table: &str,
        pk: PrimaryKey,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        let t = self
            .get_table_by_name(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;
        t.to_operation_delete(pk, timestamp)
    }
}
