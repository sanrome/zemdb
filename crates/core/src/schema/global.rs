use super::table::TableSchema;
use super::validation::{self, ValidationError};
use crate::mutation::Operation;
use crate::value::{PrimaryKey, Row, Value};
use serde::{Deserialize, Serialize};
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

    pub fn table(mut self, mut table: TableSchema) -> Self {
        if self.tables_by_id.contains_key(&table.table_id) {
            let next_id = self.tables_by_id.keys().max().map_or(0, |m| m + 1);
            table.table_id = next_id;
        } else if table.table_id == 0 && !self.tables_by_id.is_empty() {
            let next_id = self.tables_by_id.keys().max().map_or(0, |m| m + 1);
            table.table_id = next_id;
        }
        self.id_by_name.insert(table.name.clone(), table.table_id);
        self.tables_by_id.insert(table.table_id, table);
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables_by_id: self.tables_by_id,
            id_by_name: self.id_by_name,
        }
    }
}

/// Global database schema containing all tables in a Room with bidirectional ID/Name indexing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub tables_by_id: BTreeMap<u16, TableSchema>,
    pub id_by_name: BTreeMap<String, u16>,
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

    pub fn add_table(&mut self, mut table: TableSchema) {
        if self.tables_by_id.contains_key(&table.table_id) {
            let next_id = self.tables_by_id.keys().max().map_or(0, |m| m + 1);
            table.table_id = next_id;
        } else if table.table_id == 0 && !self.tables_by_id.is_empty() {
            let next_id = self.tables_by_id.keys().max().map_or(0, |m| m + 1);
            table.table_id = next_id;
        }
        self.id_by_name.insert(table.name.clone(), table.table_id);
        self.tables_by_id.insert(table.table_id, table);
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
