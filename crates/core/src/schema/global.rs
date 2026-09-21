use super::table::TableSchema;
use super::validation::{self, ValidationError};
use crate::mutation::Operation;
use crate::value::{PrimaryKey, Row, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Builder for constructing Schema instances declaratively.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    tables: BTreeMap<String, TableSchema>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn table(mut self, table: TableSchema) -> Self {
        self.tables.insert(table.name.clone(), table);
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables: self.tables,
        }
    }
}

/// Global database schema containing all tables in a Room.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub tables: BTreeMap<String, TableSchema>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn add_table(&mut self, table: TableSchema) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn get_table(&self, table: &str) -> Option<&TableSchema> {
        self.tables.get(table)
    }

    pub fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
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

    pub fn to_operation_insert(
        &self,
        table: &str,
        row: &Row,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        let t = self
            .get_table(table)
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
            .get_table(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;
        t.to_operation_update(pk, fields, timestamp)
    }
}
