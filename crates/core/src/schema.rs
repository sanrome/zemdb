use crate::operation::{ColumnUpdate, Operation, TableOperation};
use crate::value::{CompactRow, DataType, PrimaryKey, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("Table '{0}' does not exist in schema")]
    TableNotFound(String),

    #[error("Primary key column '{column}' missing from row for table '{table}'")]
    MissingPrimaryKeyColumn { table: String, column: String },

    #[error("Required non-null column '{column}' missing from row for table '{table}'")]
    MissingRequiredColumn { table: String, column: String },

    #[error("Primary key arity mismatch for table '{table}': expected {expected}, got {actual}")]
    PrimaryKeyArityMismatch {
        table: String,
        expected: usize,
        actual: usize,
    },

    #[error("Primary key column '{column}' in table '{table}' expected type {expected}, got {actual}")]
    PrimaryKeyTypeMismatch {
        table: String,
        column: String,
        expected: DataType,
        actual: DataType,
    },

    #[error("Column '{column}' in table '{table}' expected type {expected}, got {actual}")]
    TypeMismatch {
        table: String,
        column: String,
        expected: DataType,
        actual: DataType,
    },

    #[error("Column '{column}' in table '{table}' is encrypted and must be transmitted as Value::Bytes")]
    EncryptedColumnMustBeBytes { table: String, column: String },

    #[error("Primary key column '{column}' in table '{table}' cannot be encrypted")]
    EncryptedPrimaryKeyNotAllowed { table: String, column: String },

    #[error("Primary key column '{column}' in table '{table}' cannot be nullable")]
    NullablePrimaryKeyNotAllowed { table: String, column: String },

    #[error("Unknown column '{column}' for table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("Primary key columns cannot be modified via UPDATE in table '{table}' (column '{column}')")]
    CannotUpdatePrimaryKey { table: String, column: String },

    #[error("Empty update payload for table '{0}'")]
    EmptyUpdate(String),

    #[error("Primary key definition cannot be empty for table '{0}'")]
    EmptyPrimaryKeyDefinition(String),

    #[error("Invalid column data type for column '{column}' in table '{table}': {message}")]
    InvalidColumnDataType {
        table: String,
        column: String,
        message: String,
    },

    #[error("CompactRow arity mismatch for table '{table}': expected {expected}, got {actual}")]
    CompactRowArityMismatch {
        table: String,
        expected: usize,
        actual: usize,
    },
}

/// Definition of a single column in a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub encrypted: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
            encrypted: false,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn encrypted(mut self, encrypted: bool) -> Self {
        self.encrypted = encrypted;
        self
    }
}

/// Builder for constructing TableSchema instances declaratively.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    name: String,
    primary_key: Vec<String>,
    columns: Vec<ColumnDef>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            primary_key: Vec::new(),
            columns: Vec::new(),
        }
    }

    /// Declares a primary key column, recording both its name and data type.
    /// Primary key columns are automatically enforced as non-nullable.
    /// Can be called multiple times to define composite primary keys.
    pub fn primary_key(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        if !self.primary_key.contains(&name) {
            self.primary_key.push(name.clone());
        }
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == name) {
            col.data_type = data_type;
            col.nullable = false;
        } else {
            self.columns.push(ColumnDef::new(name, data_type));
        }
        self
    }

    /// Adds a standard non-nullable column.
    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == name) {
            col.data_type = data_type;
        } else {
            self.columns.push(ColumnDef::new(name, data_type));
        }
        self
    }

    /// Adds a nullable column.
    pub fn nullable_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == name) {
            col.data_type = data_type;
            col.nullable = true;
        } else {
            self.columns.push(ColumnDef::new(name, data_type).nullable(true));
        }
        self
    }

    /// Adds an end-to-end encrypted column.
    /// The data_type represents the underlying decrypted type for clients,
    /// while the server only accepts opaque Value::Bytes.
    pub fn encrypted_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == name) {
            col.data_type = data_type;
            col.encrypted = true;
        } else {
            self.columns.push(ColumnDef::new(name, data_type).encrypted(true));
        }
        self
    }

    /// Validates and builds the TableSchema.
    pub fn build(self) -> Result<TableSchema, ValidationError> {
        if self.primary_key.is_empty() {
            return Err(ValidationError::EmptyPrimaryKeyDefinition(self.name));
        }

        // Schema integrity: columns cannot have DataType::Null as their underlying type
        for col in &self.columns {
            if col.data_type == DataType::Null {
                return Err(ValidationError::InvalidColumnDataType {
                    table: self.name.clone(),
                    column: col.name.clone(),
                    message: "Columns cannot have DataType::Null as their schema definition type".to_string(),
                });
            }
        }

        // Primary key columns cannot be marked as encrypted or nullable
        for pk_col in &self.primary_key {
            let col_def = self
                .columns
                .iter()
                .find(|c| &c.name == pk_col)
                .ok_or_else(|| ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                })?;

            if col_def.encrypted {
                return Err(ValidationError::EncryptedPrimaryKeyNotAllowed {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                });
            }

            if col_def.nullable {
                return Err(ValidationError::NullablePrimaryKeyNotAllowed {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                });
            }
        }

        let mut column_indices = BTreeMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            column_indices.insert(col.name.clone(), i);
        }

        Ok(TableSchema {
            name: self.name,
            primary_key: self.primary_key,
            columns: self.columns,
            column_indices,
        })
    }
}

/// Schema definition for a single table.
///
/// Preserves physical column order (DDL definition order) in `columns`
/// to guarantee stable binary positional serialization and schema evolution,
/// while providing O(log C) column lookups via `column_indices`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableSchema {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnDef>,
    #[serde(skip)]
    column_indices: BTreeMap<String, usize>,
}

impl<'de> Deserialize<'de> for TableSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TableSchemaHelper {
            name: String,
            primary_key: Vec<String>,
            columns: Vec<ColumnDef>,
        }
        let helper = TableSchemaHelper::deserialize(deserializer)?;
        let mut column_indices = BTreeMap::new();
        for (i, col) in helper.columns.iter().enumerate() {
            column_indices.insert(col.name.clone(), i);
        }
        Ok(TableSchema {
            name: helper.name,
            primary_key: helper.primary_key,
            columns: helper.columns,
            column_indices,
        })
    }
}

impl TableSchema {
    pub fn builder(name: impl Into<String>) -> TableBuilder {
        TableBuilder::new(name)
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_indices.get(name).map(|&idx| &self.columns[idx])
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_indices.get(name).copied()
    }

    pub fn extract_pk(&self, row: &Row) -> Result<PrimaryKey, ValidationError> {
        let mut pk_values = SmallVec::new();
        for pk_col in &self.primary_key {
            match row.get(pk_col) {
                Some(val) => pk_values.push(val.clone()),
                None => {
                    return Err(ValidationError::MissingPrimaryKeyColumn {
                        table: self.name.clone(),
                        column: pk_col.clone(),
                    });
                }
            }
        }
        let pk = PrimaryKey(pk_values);
        self.validate_pk(&pk)?;
        Ok(pk)
    }

    pub fn validate_pk(&self, pk: &PrimaryKey) -> Result<(), ValidationError> {
        if pk.len() != self.primary_key.len() {
            return Err(ValidationError::PrimaryKeyArityMismatch {
                table: self.name.clone(),
                expected: self.primary_key.len(),
                actual: pk.len(),
            });
        }

        for (pk_col, val) in self.primary_key.iter().zip(pk.iter()) {
            let col_def = self.get_column(pk_col).ok_or_else(|| {
                ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                }
            })?;

            if val.data_type() != col_def.data_type {
                return Err(ValidationError::PrimaryKeyTypeMismatch {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                    expected: col_def.data_type,
                    actual: val.data_type(),
                });
            }
        }

        Ok(())
    }

    fn validate_field_value(&self, col_name: &str, col_def: &ColumnDef, val: &Value) -> Result<(), ValidationError> {
        if val.is_null() {
            if !col_def.nullable {
                return Err(ValidationError::MissingRequiredColumn {
                    table: self.name.clone(),
                    column: col_name.to_string(),
                });
            }
            return Ok(());
        }

        if col_def.encrypted {
            if val.data_type() != DataType::Bytes {
                return Err(ValidationError::EncryptedColumnMustBeBytes {
                    table: self.name.clone(),
                    column: col_name.to_string(),
                });
            }
        } else if val.data_type() != col_def.data_type {
            return Err(ValidationError::TypeMismatch {
                table: self.name.clone(),
                column: col_name.to_string(),
                expected: col_def.data_type,
                actual: val.data_type(),
            });
        }
        Ok(())
    }

    pub fn validate_row(&self, row: &Row) -> Result<(), ValidationError> {
        // 1. Ensure all PK columns exist and match types
        for pk_col in &self.primary_key {
            let col_def = self.get_column(pk_col).ok_or_else(|| {
                ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                }
            })?;
            let val = row.get(pk_col).ok_or_else(|| {
                ValidationError::MissingPrimaryKeyColumn {
                    table: self.name.clone(),
                    column: pk_col.clone(),
                }
            })?;
            self.validate_field_value(pk_col, col_def, val)?;
        }

        // 2. Ensure all required non-null columns exist in row
        for col in &self.columns {
            if !col.nullable && !self.primary_key.contains(&col.name) {
                let val = row.get(&col.name).ok_or_else(|| {
                    ValidationError::MissingRequiredColumn {
                        table: self.name.clone(),
                        column: col.name.clone(),
                    }
                })?;
                self.validate_field_value(&col.name, col, val)?;
            }
        }

        // 3. Validate all provided columns match schema
        for (col_name, val) in row {
            if let Some(col_def) = self.get_column(col_name) {
                if col_def.nullable {
                    self.validate_field_value(col_name, col_def, val)?;
                }
            } else {
                return Err(ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: col_name.clone(),
                });
            }
        }

        Ok(())
    }

    pub fn validate_update(&self, fields: &BTreeMap<String, Value>) -> Result<(), ValidationError> {
        if fields.is_empty() {
            return Err(ValidationError::EmptyUpdate(self.name.clone()));
        }

        for (col_name, val) in fields {
            // PK columns cannot be updated directly
            if self.primary_key.contains(col_name) {
                return Err(ValidationError::CannotUpdatePrimaryKey {
                    table: self.name.clone(),
                    column: col_name.clone(),
                });
            }

            let col_def = self.get_column(col_name).ok_or_else(|| {
                ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: col_name.clone(),
                }
            })?;

            self.validate_field_value(col_name, col_def, val)?;
        }

        Ok(())
    }

    /// Converts a validated Row to CompactRow ordered by physical DDL schema columns.
    pub fn to_compact_row(&self, row: &Row) -> Result<CompactRow, ValidationError> {
        self.validate_row(row)?;
        let mut values = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let val = row.get(&col.name).cloned().unwrap_or(Value::Null);
            values.push(val);
        }
        Ok(CompactRow::new(values))
    }

    /// Zero-copy conversion of Row to CompactRow ordered by physical DDL schema columns.
    pub fn row_into_compact(&self, mut row: Row) -> Result<CompactRow, ValidationError> {
        self.validate_row(&row)?;
        let mut values = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let val = row.remove(&col.name).unwrap_or(Value::Null);
            values.push(val);
        }
        Ok(CompactRow::new(values))
    }

    /// Converts a CompactRow back to a structured Row, validating arity.
    pub fn from_compact_row(&self, compact: &CompactRow) -> Result<Row, ValidationError> {
        if compact.len() != self.columns.len() {
            return Err(ValidationError::CompactRowArityMismatch {
                table: self.name.clone(),
                expected: self.columns.len(),
                actual: compact.len(),
            });
        }
        let mut row = Row::new();
        for (col, val) in self.columns.iter().zip(compact.iter()) {
            if !val.is_null() {
                row.insert(col.name.clone(), val.clone());
            }
        }
        Ok(row)
    }

    /// Zero-copy conversion of CompactRow back to a structured Row.
    pub fn compact_into_row(&self, compact: CompactRow) -> Result<Row, ValidationError> {
        if compact.len() != self.columns.len() {
            return Err(ValidationError::CompactRowArityMismatch {
                table: self.name.clone(),
                expected: self.columns.len(),
                actual: compact.len(),
            });
        }
        let mut row = Row::new();
        for (col, val) in self.columns.iter().zip(compact.into_values()) {
            if !val.is_null() {
                row.insert(col.name.clone(), val);
            }
        }
        Ok(row)
    }

    /// Converts and validates named update fields into positional `ColumnUpdate` deltas
    /// strictly ordered by ascending `column_idx`.
    pub fn compact_update_fields(
        &self,
        fields: &BTreeMap<String, Value>,
    ) -> Result<Vec<ColumnUpdate>, ValidationError> {
        self.validate_update(fields)?;
        let mut updates = Vec::with_capacity(fields.len());
        for (col_name, val) in fields {
            let col_idx = self.column_index(col_name).expect("already validated") as u16;
            updates.push(ColumnUpdate::new(col_idx, val.clone()));
        }
        updates.sort_by_key(|u| u.column_idx);
        Ok(updates)
    }

    /// Converts positional `ColumnUpdate` deltas back into a named field map.
    pub fn expand_update_fields(
        &self,
        updates: &[ColumnUpdate],
    ) -> Result<BTreeMap<String, Value>, ValidationError> {
        let mut fields = BTreeMap::new();
        for u in updates {
            let col_idx = u.column_idx as usize;
            if col_idx >= self.columns.len() {
                return Err(ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: format!("index {}", u.column_idx),
                });
            }
            let col_name = self.columns[col_idx].name.clone();
            fields.insert(col_name, u.value.clone());
        }
        Ok(fields)
    }

    /// Validates a Row and compiles it into a dense `TableOperation::insert`.
    pub fn to_table_insert(&self, row: &Row, timestamp: u64) -> Result<TableOperation, ValidationError> {
        let pk = self.extract_pk(row)?;
        let compact = self.to_compact_row(row)?;
        Ok(TableOperation::insert(pk, compact, timestamp))
    }

    /// Validates an update payload and compiles it into a dense `TableOperation::update`.
    pub fn to_table_update(
        &self,
        pk: PrimaryKey,
        fields: &BTreeMap<String, Value>,
        timestamp: u64,
    ) -> Result<TableOperation, ValidationError> {
        self.validate_pk(&pk)?;
        let updates = self.compact_update_fields(fields)?;
        Ok(TableOperation::update(pk, updates, timestamp))
    }

    /// Validates a Row and compiles it into a self-describing `Operation::insert`.
    pub fn to_operation_insert(&self, row: &Row, timestamp: u64) -> Result<Operation, ValidationError> {
        let table_op = self.to_table_insert(row, timestamp)?;
        Ok(Operation::new(self.name.clone(), table_op))
    }

    /// Validates an update payload and compiles it into a self-describing `Operation::update`.
    pub fn to_operation_update(
        &self,
        pk: PrimaryKey,
        fields: &BTreeMap<String, Value>,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        let table_op = self.to_table_update(pk, fields, timestamp)?;
        Ok(Operation::new(self.name.clone(), table_op))
    }

    /// Creates a fluent update builder for this table using column names.
    pub fn update_builder(&self, pk: PrimaryKey) -> SchemaUpdateBuilder<'_> {
        SchemaUpdateBuilder::new(self, pk)
    }
}

/// Fluent builder for constructing updates validated against a TableSchema.
#[derive(Debug, Clone)]
pub struct SchemaUpdateBuilder<'a> {
    schema: &'a TableSchema,
    pk: PrimaryKey,
    fields: BTreeMap<String, Value>,
    timestamp: u64,
}

impl<'a> SchemaUpdateBuilder<'a> {
    pub fn new(schema: &'a TableSchema, pk: PrimaryKey) -> Self {
        Self {
            schema,
            pk,
            fields: BTreeMap::new(),
            timestamp: 0,
        }
    }

    pub fn set(mut self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(column.into(), value.into());
        self
    }

    pub fn timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }

    pub fn build(self) -> Result<Operation, ValidationError> {
        self.schema.to_operation_update(self.pk, &self.fields, self.timestamp)
    }

    pub fn build_table_op(self) -> Result<TableOperation, ValidationError> {
        self.schema.to_table_update(self.pk, &self.fields, self.timestamp)
    }
}

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
        let t = self
            .get_table(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;
        t.validate_row(row)?;
        t.extract_pk(row)
    }

    pub fn validate_update(
        &self,
        table: &str,
        pk: &PrimaryKey,
        fields: &BTreeMap<String, Value>,
    ) -> Result<(), ValidationError> {
        let t = self
            .get_table(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;

        t.validate_pk(pk)?;
        t.validate_update(fields)
    }

    pub fn validate_delete(&self, table: &str, pk: &PrimaryKey) -> Result<(), ValidationError> {
        let t = self
            .get_table(table)
            .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;

        t.validate_pk(pk)
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
