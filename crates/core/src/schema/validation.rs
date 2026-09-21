use super::column::ColumnDef;
use super::global::Schema;
use super::table::TableSchema;
use crate::value::{DataType, PrimaryKey, Row, Value};
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
        .get_table(table)
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
        .get_table(table)
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
        .get_table(table)
        .ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;

    t.validate_pk(pk)
}
