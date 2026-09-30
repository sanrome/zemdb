use super::table::TableSchema;
use super::validation::{self, ValidationError};
use crate::mutation::Operation;
use crate::value::{PrimaryKey, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// Builder for constructing Schema instances declaratively.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    tables_by_id: BTreeMap<u16, TableSchema>,
    id_by_name: BTreeMap<String, u16>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn try_table(mut self, mut table: TableSchema) -> Result<Self, ValidationError> {
        if self.id_by_name.contains_key(table.name()) {
            return Err(ValidationError::DuplicateTable {
                table: table.name().to_string(),
            });
        }
        if self.tables_by_id.contains_key(&table.table_id())
            || (table.table_id() == 0 && !self.tables_by_id.is_empty())
        {
            let next_id = self
                .tables_by_id
                .keys()
                .max()
                .map_or(Some(0), |m| m.checked_add(1))
                .ok_or(ValidationError::TableIdOverflow)?;
            table.set_table_id(next_id);
        }
        self.id_by_name
            .insert(table.name().to_string(), table.table_id());
        self.tables_by_id.insert(table.table_id(), table);
        Ok(self)
    }

    pub fn table(self, table: TableSchema) -> Self {
        self.try_table(table).expect("valid table schema")
    }

    pub fn build(self) -> Schema {
        Schema {
            tables_by_id: self.tables_by_id,
            id_by_name: self.id_by_name,
        }
    }
}

/// Global database schema containing all tables in a Room with bidirectional ID/Name indexing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize)]
pub struct Schema {
    pub tables_by_id: BTreeMap<u16, TableSchema>,
    #[serde(skip)]
    pub id_by_name: BTreeMap<String, u16>,
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            #[derive(Deserialize)]
            struct HumanSchemaHelper {
                #[serde(default)]
                tables_by_id: BTreeMap<u16, TableSchema>,
                #[serde(default)]
                tables: Vec<TableSchema>,
            }
            let helper = HumanSchemaHelper::deserialize(deserializer)?;
            if !helper.tables.is_empty() {
                Schema::try_from_tables(helper.tables).map_err(serde::de::Error::custom)
            } else {
                let mut schema = Schema::new();
                for (_, table) in helper.tables_by_id {
                    schema.add_table(table).map_err(serde::de::Error::custom)?;
                }
                Ok(schema)
            }
        } else {
            #[derive(Deserialize)]
            struct BinarySchemaHelper {
                tables_by_id: BTreeMap<u16, TableSchema>,
            }
            let helper = BinarySchemaHelper::deserialize(deserializer)?;
            let mut schema = Schema::new();
            for (_, table) in helper.tables_by_id {
                schema.add_table(table).map_err(serde::de::Error::custom)?;
            }
            Ok(schema)
        }
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

    /// Validates and constructs a Schema from an iterable of TableSchema, auto-assigning IDs and building indexes.
    pub fn try_from_tables(
        tables: impl IntoIterator<Item = TableSchema>,
    ) -> Result<Self, ValidationError> {
        let mut schema = Schema::new();
        for table in tables {
            schema.add_table(table)?;
        }
        Ok(schema)
    }

    /// Constructs a Schema from an iterable of TableSchema, auto-assigning IDs and building indexes.
    pub fn from_tables(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self::try_from_tables(tables).expect("valid table schemas")
    }

    pub fn add_table(&mut self, mut table: TableSchema) -> Result<u16, ValidationError> {
        if self.id_by_name.contains_key(table.name()) {
            return Err(ValidationError::DuplicateTable {
                table: table.name().to_string(),
            });
        }
        if self.tables_by_id.contains_key(&table.table_id())
            || (table.table_id() == 0 && !self.tables_by_id.is_empty())
        {
            let next_id = self
                .tables_by_id
                .keys()
                .max()
                .map_or(Some(0), |m| m.checked_add(1))
                .ok_or(ValidationError::TableIdOverflow)?;
            table.set_table_id(next_id);
        }
        let assigned_id = table.table_id();
        self.id_by_name
            .insert(table.name().to_string(), assigned_id);
        self.tables_by_id.insert(assigned_id, table);
        Ok(assigned_id)
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
