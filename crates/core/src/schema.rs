use crate::value::{CompactRow, DataType, PrimaryKey, Row, Value};
use serde::{Deserialize, Serialize};
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

    #[error("Unknown column '{column}' for table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("Primary key columns cannot be modified via UPDATE in table '{table}' (column '{column}')")]
    CannotUpdatePrimaryKey { table: String, column: String },

    #[error("Empty update payload for table '{0}'")]
    EmptyUpdate(String),

    #[error("Primary key definition cannot be empty for table '{0}'")]
    EmptyPrimaryKeyDefinition(String),

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
    pub data_type: DataType,
    pub nullable: bool,
    pub encrypted: bool,
}

impl ColumnDef {
    pub fn new(data_type: DataType) -> Self {
        Self {
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
    columns: BTreeMap<String, ColumnDef>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            primary_key: Vec::new(),
            columns: BTreeMap::new(),
        }
    }

    /// Declares a primary key column, recording both its name and data type.
    /// Can be called multiple times to define composite primary keys.
    pub fn primary_key(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        self.primary_key.push(name.clone());
        let col = self
            .columns
            .entry(name)
            .or_insert_with(|| ColumnDef::new(data_type));
        col.data_type = data_type;
        self
    }

    /// Adds a standard non-nullable column.
    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        let col = self
            .columns
            .entry(name)
            .or_insert_with(|| ColumnDef::new(data_type));
        col.data_type = data_type;
        self
    }

    /// Adds a nullable column.
    pub fn nullable_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        let col = self
            .columns
            .entry(name)
            .or_insert_with(|| ColumnDef::new(data_type));
        col.data_type = data_type;
        col.nullable = true;
        self
    }

    /// Adds an end-to-end encrypted column.
    /// The data_type represents the underlying decrypted type for clients,
    /// while the server only accepts opaque Value::Bytes.
    pub fn encrypted_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        let col = self
            .columns
            .entry(name)
            .or_insert_with(|| ColumnDef::new(data_type));
        col.data_type = data_type;
        col.encrypted = true;
        self
    }

    /// Validates and builds the TableSchema.
    pub fn build(self) -> Result<TableSchema, ValidationError> {
        if self.primary_key.is_empty() {
            return Err(ValidationError::EmptyPrimaryKeyDefinition(self.name));
        }

        // Primary key columns cannot be marked as encrypted
        for pk_col in &self.primary_key {
            if let Some(col_def) = self.columns.get(pk_col) {
                if col_def.encrypted {
                    return Err(ValidationError::EncryptedPrimaryKeyNotAllowed {
                        table: self.name.clone(),
                        column: pk_col.clone(),
                    });
                }
            }
        }

        Ok(TableSchema {
            name: self.name,
            primary_key: self.primary_key,
            columns: self.columns,
        })
    }
}

/// Schema definition for a single table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: BTreeMap<String, ColumnDef>,
}

impl TableSchema {
    pub fn builder(name: impl Into<String>) -> TableBuilder {
        TableBuilder::new(name)
    }

    pub fn extract_pk(&self, row: &Row) -> Result<PrimaryKey, ValidationError> {
        let mut pk_values = Vec::with_capacity(self.primary_key.len());
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
        let pk = PrimaryKey::composite(pk_values);
        self.validate_pk(&pk)?;
        Ok(pk)
    }

    pub fn validate_pk(&self, pk: &PrimaryKey) -> Result<(), ValidationError> {
        if pk.values().len() != self.primary_key.len() {
            return Err(ValidationError::PrimaryKeyArityMismatch {
                table: self.name.clone(),
                expected: self.primary_key.len(),
                actual: pk.values().len(),
            });
        }

        for (pk_col, val) in self.primary_key.iter().zip(pk.values().iter()) {
            let col_def = self.columns.get(pk_col).ok_or_else(|| {
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
            let col_def = self.columns.get(pk_col).ok_or_else(|| {
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
        for (col_name, col_def) in &self.columns {
            if !col_def.nullable && !self.primary_key.contains(col_name) {
                let val = row.get(col_name).ok_or_else(|| {
                    ValidationError::MissingRequiredColumn {
                        table: self.name.clone(),
                        column: col_name.clone(),
                    }
                })?;
                self.validate_field_value(col_name, col_def, val)?;
            }
        }

        // 3. Validate all provided columns match schema
        for (col_name, val) in row {
            if let Some(col_def) = self.columns.get(col_name) {
                self.validate_field_value(col_name, col_def, val)?;
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

            let col_def = self.columns.get(col_name).ok_or_else(|| {
                ValidationError::UnknownColumn {
                    table: self.name.clone(),
                    column: col_name.clone(),
                }
            })?;

            self.validate_field_value(col_name, col_def, val)?;
        }

        Ok(())
    }

    /// Converts a validated Row to CompactRow ordered by schema columns.
    pub fn to_compact_row(&self, row: &Row) -> Result<CompactRow, ValidationError> {
        self.validate_row(row)?;
        let mut values = Vec::with_capacity(self.columns.len());
        for col_name in self.columns.keys() {
            let val = row.get(col_name).cloned().unwrap_or(Value::Null);
            values.push(val);
        }
        Ok(CompactRow::new(values))
    }

    /// Converts a CompactRow back to a structured Row, validating arity.
    pub fn from_compact_row(&self, compact: &CompactRow) -> Result<Row, ValidationError> {
        if compact.values.len() != self.columns.len() {
            return Err(ValidationError::CompactRowArityMismatch {
                table: self.name.clone(),
                expected: self.columns.len(),
                actual: compact.values.len(),
            });
        }
        let mut row = Row::new();
        for ((col_name, _), val) in self.columns.iter().zip(compact.values.iter()) {
            if !val.is_null() {
                row.insert(col_name.clone(), val.clone());
            }
        }
        Ok(row)
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
}
