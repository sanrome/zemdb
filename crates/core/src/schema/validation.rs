use super::column::ColumnDef;
use super::global::Schema;
use super::table::TableSchema;
use crate::mutation::{ColumnUpdate, Operation, OperationKind};
use crate::value::{CompactRow, DataType, PrimaryKey, Row, Value};
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

    #[error("Column '{column}' already exists in table '{table}'")]
    DuplicateColumn { table: String, column: String },

    #[error("Added column '{column}' in table '{table}' must be nullable for schema evolution")]
    AddedColumnMustBeNullable { table: String, column: String },

    #[error("Table ID mismatch for table '{table}': expected {expected}, got {actual}")]
    TableIdMismatch {
        table: String,
        expected: u16,
        actual: u16,
    },

    #[error("Column updates for table '{table}' must be strictly sorted by column index (got index {actual_idx} after {prev_idx})")]
    UnsortedColumnUpdates {
        table: String,
        prev_idx: u16,
        actual_idx: u16,
    },

    #[error("Row primary key does not match operation primary key for table '{table}'")]
    PrimaryKeyMismatch { table: String },
}

/// Validates an individual field value against column definition rules.
pub fn validate_field_value(
    table: &str,
    col_name: &str,
    col_def: &ColumnDef,
    val: &Value,
) -> Result<(), ValidationError> {
    if val.is_null() {
        if !col_def.nullable {
            return Err(ValidationError::MissingRequiredColumn {
                table: table.to_string(),
                column: col_name.to_string(),
            });
        }
        return Ok(());
    }

    if col_def.encrypted {
        if val.data_type() != DataType::Bytes {
            return Err(ValidationError::EncryptedColumnMustBeBytes {
                table: table.to_string(),
                column: col_name.to_string(),
            });
        }
    } else if val.data_type() != col_def.data_type {
        return Err(ValidationError::TypeMismatch {
            table: table.to_string(),
            column: col_name.to_string(),
            expected: col_def.data_type,
            actual: val.data_type(),
        });
    }
    Ok(())
}

/// Validates that a primary key matches expected arity and types.
pub fn validate_pk<'a>(
    table_name: &str,
    pk_cols: &[String],
    get_column: impl Fn(&str) -> Option<&'a ColumnDef>,
    pk: &PrimaryKey,
) -> Result<(), ValidationError> {
    if pk.len() != pk_cols.len() {
        return Err(ValidationError::PrimaryKeyArityMismatch {
            table: table_name.to_string(),
            expected: pk_cols.len(),
            actual: pk.len(),
        });
    }

    for (pk_col, val) in pk_cols.iter().zip(pk.iter()) {
        let col_def = get_column(pk_col).ok_or_else(|| ValidationError::UnknownColumn {
            table: table_name.to_string(),
            column: pk_col.clone(),
        })?;

        if val.data_type() != col_def.data_type {
            return Err(ValidationError::PrimaryKeyTypeMismatch {
                table: table_name.to_string(),
                column: pk_col.clone(),
                expected: col_def.data_type,
                actual: val.data_type(),
            });
        }
    }

    Ok(())
}

/// Validates an entire structured row against a TableSchema.
pub fn validate_row(table: &TableSchema, row: &Row) -> Result<(), ValidationError> {
    // 1. Ensure all PK columns exist and match types
    for pk_col in &table.primary_key {
        let col_def = table.get_column(pk_col).ok_or_else(|| ValidationError::UnknownColumn {
            table: table.name.clone(),
            column: pk_col.clone(),
        })?;
        let val = row.get(pk_col).ok_or_else(|| ValidationError::MissingPrimaryKeyColumn {
            table: table.name.clone(),
            column: pk_col.clone(),
        })?;
        validate_field_value(&table.name, pk_col, col_def, val)?;
    }

    // 2. Ensure all required non-null columns exist in row
    for col in &table.columns {
        if !col.nullable && !table.primary_key.contains(&col.name) {
            let val = row.get(&col.name).ok_or_else(|| ValidationError::MissingRequiredColumn {
                table: table.name.clone(),
                column: col.name.clone(),
            })?;
            validate_field_value(&table.name, &col.name, col, val)?;
        }
    }

    // 3. Validate all provided columns match schema
    for (col_name, val) in row {
        if let Some(col_def) = table.get_column(col_name) {
            if col_def.nullable {
                validate_field_value(&table.name, col_name, col_def, val)?;
            }
        } else {
            return Err(ValidationError::UnknownColumn {
                table: table.name.clone(),
                column: col_name.clone(),
            });
        }
    }

    Ok(())
}

/// Validates update field values against a TableSchema.
pub fn validate_table_update(
    table: &TableSchema,
    fields: &BTreeMap<String, Value>,
) -> Result<(), ValidationError> {
    if fields.is_empty() {
        return Err(ValidationError::EmptyUpdate(table.name.clone()));
    }

    for (col_name, val) in fields {
        // PK columns cannot be updated directly
        if table.primary_key.contains(col_name) {
            return Err(ValidationError::CannotUpdatePrimaryKey {
                table: table.name.clone(),
                column: col_name.clone(),
            });
        }

        let col_def = table.get_column(col_name).ok_or_else(|| ValidationError::UnknownColumn {
            table: table.name.clone(),
            column: col_name.clone(),
        })?;

        validate_field_value(&table.name, col_name, col_def, val)?;
    }

    Ok(())
}

/// Validates an insert operation on a Schema, returning the extracted PrimaryKey on success.
pub fn validate_insert(
    schema: &Schema,
    table: &str,
    row: &Row,
) -> Result<PrimaryKey, ValidationError> {
    let t = schema
        .get_table_by_name(table)
        .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;
    t.validate_row(row)?;
    t.extract_pk(row)
}

/// Validates an update operation on a Schema.
pub fn validate_update(
    schema: &Schema,
    table: &str,
    pk: &PrimaryKey,
    fields: &BTreeMap<String, Value>,
) -> Result<(), ValidationError> {
    let t = schema
        .get_table_by_name(table)
        .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;

    t.validate_pk(pk)?;
    t.validate_update(fields)
}

/// Validates a delete operation on a Schema.
pub fn validate_delete(
    schema: &Schema,
    table: &str,
    pk: &PrimaryKey,
) -> Result<(), ValidationError> {
    let t = schema
        .get_table_by_name(table)
        .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;

    t.validate_pk(pk)
}

/// Validates an array of positional ColumnUpdates in $O(C)$ time against TableSchema.
pub fn validate_column_updates(
    table: &TableSchema,
    updates: &[ColumnUpdate],
) -> Result<(), ValidationError> {
    if updates.is_empty() {
        return Err(ValidationError::EmptyUpdate(table.name.clone()));
    }

    let mut seen = SmallVec::<[bool; 32]>::from_elem(false, table.columns.len());
    let mut last_idx: Option<u16> = None;

    for u in updates {
        let col_idx = u.column_idx as usize;
        if col_idx >= table.columns.len() {
            return Err(ValidationError::UnknownColumn {
                table: table.name.clone(),
                column: format!("index {}", u.column_idx),
            });
        }

        // Verify strictly ascending ordering without duplicates
        if let Some(prev) = last_idx {
            if u.column_idx == prev {
                return Err(ValidationError::DuplicateColumn {
                    table: table.name.clone(),
                    column: table.columns[col_idx].name.clone(),
                });
            } else if u.column_idx < prev {
                return Err(ValidationError::UnsortedColumnUpdates {
                    table: table.name.clone(),
                    prev_idx: prev,
                    actual_idx: u.column_idx,
                });
            }
        }
        last_idx = Some(u.column_idx);

        if seen[col_idx] {
            return Err(ValidationError::DuplicateColumn {
                table: table.name.clone(),
                column: table.columns[col_idx].name.clone(),
            });
        }
        seen[col_idx] = true;

        let col_def = &table.columns[col_idx];
        if table.primary_key.contains(&col_def.name) {
            return Err(ValidationError::CannotUpdatePrimaryKey {
                table: table.name.clone(),
                column: col_def.name.clone(),
            });
        }

        validate_field_value(&table.name, &col_def.name, col_def, &u.value)?;
    }

    Ok(())
}

/// Validates an entire positional CompactRow in $O(C)$ time against TableSchema.
pub fn validate_compact_row(
    table: &TableSchema,
    compact: &CompactRow,
) -> Result<(), ValidationError> {
    if compact.len() > table.columns.len() {
        return Err(ValidationError::CompactRowArityMismatch {
            table: table.name.clone(),
            expected: table.columns.len(),
            actual: compact.len(),
        });
    }

    for (i, col_def) in table.columns.iter().enumerate() {
        let val = if i < compact.len() {
            &compact.values[i]
        } else {
            &Value::Null
        };
        validate_field_value(&table.name, &col_def.name, col_def, val)?;
    }

    Ok(())
}

/// Validates an Operation against TableSchema in $O(C)$ time without reconstructing HashMaps.
pub fn validate_operation(
    table: &TableSchema,
    op: &Operation,
) -> Result<(), ValidationError> {
    if op.table_id != table.table_id {
        return Err(ValidationError::TableIdMismatch {
            table: table.name.clone(),
            expected: table.table_id,
            actual: op.table_id,
        });
    }

    table.validate_pk(&op.pk)?;

    match &op.kind {
        OperationKind::Insert { row } => {
            validate_compact_row(table, row)?;

            // Validate that the primary key columns in `row` match `op.pk`
            for (pk_idx, pk_col_name) in table.primary_key.iter().enumerate() {
                let col_idx = *table
                    .column_indices
                    .get(pk_col_name)
                    .expect("primary key column must exist in column_indices");

                if col_idx >= row.len() || row.values[col_idx].is_null() {
                    return Err(ValidationError::MissingPrimaryKeyColumn {
                        table: table.name.clone(),
                        column: pk_col_name.clone(),
                    });
                }

                if row.values[col_idx] != op.pk.0[pk_idx] {
                    return Err(ValidationError::PrimaryKeyMismatch {
                        table: table.name.clone(),
                    });
                }
            }
        }
        OperationKind::Update { updates } => {
            validate_column_updates(table, updates)?;
        }
        OperationKind::Delete => {
            // Delete only mutates by PK, which was already verified by validate_pk above
        }
    }

    Ok(())
}
