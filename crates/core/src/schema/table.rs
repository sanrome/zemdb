use super::column::ColumnDef;
use super::validation::{self, ValidationError};
use crate::mutation::{ColumnUpdate, Operation};
use crate::value::{CompactRow, DataType, PrimaryKey, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;

/// Builder for constructing TableSchema instances declaratively.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    table_id: u16,
    name: String,
    primary_key: Vec<String>,
    columns: Vec<ColumnDef>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table_id: 0,
            name: name.into(),
            primary_key: Vec::new(),
            columns: Vec::new(),
        }
    }

    /// Sets an explicit table_id for this table.
    pub fn table_id(mut self, id: u16) -> Self {
        self.table_id = id;
        self
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
                    message: "Columns cannot have DataType::Null as their schema definition type"
                        .to_string(),
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
            table_id: self.table_id,
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
    pub table_id: u16,
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnDef>,
    #[serde(skip)]
    pub(crate) column_indices: BTreeMap<String, usize>,
}

impl<'de> Deserialize<'de> for TableSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TableSchemaHelper {
            #[serde(default)]
            table_id: u16,
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
            table_id: helper.table_id,
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

    /// Appends a new nullable column to the table schema for append-only DDL evolution.
    /// Preserves physical column order and updates secondary column indexing.
    /// Returns the assigned positional 0-indexed column index (`u16`).
    pub fn add_column(&mut self, col: ColumnDef) -> Result<u16, ValidationError> {
        if self.column_indices.contains_key(&col.name) {
            return Err(ValidationError::DuplicateColumn {
                table: self.name.clone(),
                column: col.name,
            });
        }

        if !col.nullable {
            return Err(ValidationError::AddedColumnMustBeNullable {
                table: self.name.clone(),
                column: col.name,
            });
        }

        if col.data_type == DataType::Null {
            return Err(ValidationError::InvalidColumnDataType {
                table: self.name.clone(),
                column: col.name,
                message: "Columns cannot have DataType::Null as their schema definition type"
                    .to_string(),
            });
        }

        let new_idx = self.columns.len();
        self.column_indices.insert(col.name.clone(), new_idx);
        self.columns.push(col);
        Ok(new_idx as u16)
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
        validation::validate_pk(&self.name, &self.primary_key, |col| self.get_column(col), pk)
    }

    pub fn validate_row(&self, row: &Row) -> Result<(), ValidationError> {
        validation::validate_row(self, row)
    }

    pub fn validate_update(&self, fields: &BTreeMap<String, Value>) -> Result<(), ValidationError> {
        validation::validate_table_update(self, fields)
    }

    /// Validates an entire CompactRow in O(C) time against this TableSchema.
    pub fn validate_compact_row(&self, row: &CompactRow) -> Result<(), ValidationError> {
        validation::validate_compact_row(self, row)
    }

    /// Validates positional ColumnUpdates in O(C) time against this TableSchema.
    pub fn validate_column_updates(&self, updates: &[ColumnUpdate]) -> Result<(), ValidationError> {
        validation::validate_column_updates(self, updates)
    }

    /// Validates an Operation in O(C) time against this TableSchema.
    pub fn validate_operation(&self, op: &Operation) -> Result<(), ValidationError> {
        validation::validate_operation(self, op)
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
    ///
    /// Allows `compact.len() <= self.columns.len()`, treating omitted columns as null.
    pub fn from_compact_row(&self, compact: &CompactRow) -> Result<Row, ValidationError> {
        if compact.len() > self.columns.len() {
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
    ///
    /// Allows `compact.len() <= self.columns.len()`, treating omitted columns as null.
    pub fn compact_into_row(&self, compact: CompactRow) -> Result<Row, ValidationError> {
        if compact.len() > self.columns.len() {
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

    /// Validates a Row and compiles it into an `Operation::insert`.
    pub fn to_operation_insert(
        &self,
        row: &Row,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        let pk = self.extract_pk(row)?;
        let compact = self.to_compact_row(row)?;
        Ok(Operation::insert(self.table_id, pk, compact, timestamp))
    }

    /// Validates an update payload and compiles it into an `Operation::update`.
    pub fn to_operation_update(
        &self,
        pk: PrimaryKey,
        fields: &BTreeMap<String, Value>,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        self.validate_pk(&pk)?;
        let updates = self.compact_update_fields(fields)?;
        Ok(Operation::update(self.table_id, pk, updates, timestamp))
    }

    /// Validates a PK and compiles it into an `Operation::delete`.
    pub fn to_operation_delete(
        &self,
        pk: PrimaryKey,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        self.validate_pk(&pk)?;
        Ok(Operation::delete(self.table_id, pk, timestamp))
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
        self.schema
            .to_operation_update(self.pk, &self.fields, self.timestamp)
    }
}
