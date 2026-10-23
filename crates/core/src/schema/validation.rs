use super::column::ColumnDef;
use super::table::TableSchema;
use crate::mutation::{ColumnUpdate, Operation, OperationKind};
use crate::value::{CompactRow, DataType, PrimaryKey, Row, Value};
use std::cmp::Ordering;
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

    #[error(
        "Primary key column '{column}' in table '{table}' expected type {expected}, got {actual}"
    )]
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

    #[error(
        "Column '{column}' in table '{table}' is encrypted and must be transmitted as Value::Bytes"
    )]
    EncryptedColumnMustBeBytes { table: String, column: String },

    #[error("Primary key column '{column}' in table '{table}' cannot be encrypted")]
    EncryptedPrimaryKeyNotAllowed { table: String, column: String },

    #[error("Primary key column '{column}' in table '{table}' cannot be nullable")]
    NullablePrimaryKeyNotAllowed { table: String, column: String },

    #[error("Unknown column '{column}' for table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error(
        "Primary key columns cannot be modified via UPDATE in table '{table}' (column '{column}')"
    )]
    CannotUpdatePrimaryKey { table: String, column: String },

    #[error("Empty update payload for table '{0}'")]
    EmptyUpdate(String),

    #[error("Primary key definition cannot be empty for table '{0}'")]
    EmptyPrimaryKeyDefinition(String),

    #[error(
        "Invalid column data type for column '{column}' in table '{table}': \
         Columns cannot have DataType::Null as their schema definition type"
    )]
    InvalidColumnDataType { table: String, column: String },

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

    #[error("Table '{table}' already exists in schema")]
    DuplicateTable { table: String },

    #[error("Table ID assignment overflowed u16 capacity")]
    TableIdOverflow,

    #[error("Table '{table}' has no table_id")]
    MissingTableId { table: String },

    #[error("Table id {table_id} of table '{table}' is already used by another table")]
    DuplicateTableId { table: String, table_id: u16 },

    #[error("Table '{table}' would have more than {max} columns")]
    TooManyColumns { table: String, max: usize },

    #[error("Table '{table}' would have more than {max} constraints")]
    TooManyConstraints { table: String, max: usize },

    #[error("Constraint '{constraint}' of table '{table}' has more than {max} columns")]
    TooManyConstraintColumns {
        table: String,
        constraint: String,
        max: usize,
    },

    #[error("Constraint '{constraint}' of table '{table}' has no columns")]
    EmptyConstraint { table: String, constraint: String },

    #[error("Constraint '{constraint}' of table '{table}' names unknown column '{column}'")]
    UnknownConstraintColumn {
        table: String,
        constraint: String,
        column: String,
    },

    #[error("Constraint '{constraint}' of table '{table}' repeats column '{column}'")]
    DuplicateConstraintColumn {
        table: String,
        constraint: String,
        column: String,
    },

    #[error(
        "Constraint '{constraint}' of table '{table}' cannot include encrypted column '{column}'"
    )]
    EncryptedConstraintColumn {
        table: String,
        constraint: String,
        column: String,
    },

    #[error("Unique constraint '{constraint}' of table '{table}' is the primary key")]
    UniqueIsPrimaryKey { table: String, constraint: String },

    #[error("Constraint name '{constraint}' is used more than once in the schema")]
    DuplicateConstraintName { constraint: String },

    #[error(
        "Foreign key '{constraint}' of table '{table}' references table '{references}', \
         which does not exist in schema"
    )]
    ForeignKeyTableNotFound {
        table: String,
        constraint: String,
        references: String,
    },

    #[error(
        "Foreign key '{constraint}' of table '{table}' has {actual} columns, but the primary \
         key it references has {expected}"
    )]
    ForeignKeyArityMismatch {
        table: String,
        constraint: String,
        expected: usize,
        actual: usize,
    },

    #[error(
        "Column '{column}' of foreign key '{constraint}' in table '{table}' has type {actual}, \
         but the primary key column it references has type {expected}"
    )]
    ForeignKeyTypeMismatch {
        table: String,
        constraint: String,
        column: String,
        expected: DataType,
        actual: DataType,
    },

    #[error(
        "Constraint '{constraint}' added to table '{table}' does not include the new column \
         '{column}'"
    )]
    ConstraintOmitsNewColumn {
        table: String,
        constraint: String,
        column: String,
    },

    #[error(
        "Owner column '{column}' of table '{table}' must be a String column, neither nullable \
         nor encrypted, holding the owner's client id"
    )]
    InvalidOwnerColumn { table: String, column: String },

    #[error("Table '{table}' declares a pseudonymous owner, which is not supported")]
    UnsupportedOwnerIdentity { table: String },

    #[error("Table '{table}' is immutable: its rows cannot be updated or deleted")]
    ImmutableTable { table: String },

    #[error(
        "Operation on table '{table}' does not declare its owner: an update must set the \
         owner column and a delete must carry the owner"
    )]
    MissingOwner { table: String },

    #[error(
        "A delete on table '{table}' cannot declare an owner: the table has none outside its \
         primary key"
    )]
    UnexpectedOwner { table: String },

    #[error("Operation on table '{table}' declares an owner other than the owner of the client that sends it")]
    OwnerMismatch { table: String },
}

fn validate_field_value(
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

impl TableSchema {
    pub fn validate_pk(&self, pk: &PrimaryKey) -> Result<(), ValidationError> {
        let pk_cols = self.primary_key();
        if pk.len() != pk_cols.len() {
            return Err(ValidationError::PrimaryKeyArityMismatch {
                table: self.name().to_string(),
                expected: pk_cols.len(),
                actual: pk.len(),
            });
        }

        for (pk_col, val) in pk_cols.iter().zip(pk.iter()) {
            let col_def =
                self.get_column(pk_col)
                    .ok_or_else(|| ValidationError::UnknownColumn {
                        table: self.name().to_string(),
                        column: pk_col.clone(),
                    })?;

            if val.data_type() != col_def.data_type {
                return Err(ValidationError::PrimaryKeyTypeMismatch {
                    table: self.name().to_string(),
                    column: pk_col.clone(),
                    expected: col_def.data_type,
                    actual: val.data_type(),
                });
            }
        }

        Ok(())
    }

    pub fn validate_row(&self, row: &Row) -> Result<(), ValidationError> {
        // 1. Ensure all PK columns exist and match types
        for pk_col in self.primary_key() {
            let col_def =
                self.get_column(pk_col)
                    .ok_or_else(|| ValidationError::UnknownColumn {
                        table: self.name().to_string(),
                        column: pk_col.clone(),
                    })?;
            let val = row
                .get(pk_col)
                .ok_or_else(|| ValidationError::MissingPrimaryKeyColumn {
                    table: self.name().to_string(),
                    column: pk_col.clone(),
                })?;
            validate_field_value(self.name(), pk_col, col_def, val)?;
        }

        // 2. Ensure all required non-null columns exist in row
        for col in self.columns() {
            if !col.nullable && !self.primary_key().contains(&col.name) {
                let val =
                    row.get(&col.name)
                        .ok_or_else(|| ValidationError::MissingRequiredColumn {
                            table: self.name().to_string(),
                            column: col.name.clone(),
                        })?;
                validate_field_value(self.name(), &col.name, col, val)?;
            }
        }

        // 3. Validate all provided columns match schema
        for (col_name, val) in row {
            let col_def =
                self.get_column(col_name)
                    .ok_or_else(|| ValidationError::UnknownColumn {
                        table: self.name().to_string(),
                        column: col_name.clone(),
                    })?;
            if col_def.nullable {
                validate_field_value(self.name(), col_name, col_def, val)?;
            }
        }

        Ok(())
    }

    pub fn validate_update(&self, fields: &BTreeMap<String, Value>) -> Result<(), ValidationError> {
        if fields.is_empty() {
            return Err(ValidationError::EmptyUpdate(self.name().to_string()));
        }

        for (col_name, val) in fields {
            if self.primary_key().contains(col_name) {
                return Err(ValidationError::CannotUpdatePrimaryKey {
                    table: self.name().to_string(),
                    column: col_name.clone(),
                });
            }

            let col_def =
                self.get_column(col_name)
                    .ok_or_else(|| ValidationError::UnknownColumn {
                        table: self.name().to_string(),
                        column: col_name.clone(),
                    })?;

            validate_field_value(self.name(), col_name, col_def, val)?;
        }

        Ok(())
    }

    /// Validates positional ColumnUpdates against this TableSchema.
    pub fn validate_column_updates(&self, updates: &[ColumnUpdate]) -> Result<(), ValidationError> {
        if updates.is_empty() {
            return Err(ValidationError::EmptyUpdate(self.name().to_string()));
        }

        let mut last_idx: Option<u16> = None;

        for u in updates {
            let col_def = self.columns().get(u.column_idx as usize).ok_or_else(|| {
                ValidationError::UnknownColumn {
                    table: self.name().to_string(),
                    column: format!("index {}", u.column_idx),
                }
            })?;

            // Verify strictly ascending ordering without duplicates
            if let Some(prev) = last_idx {
                match u.column_idx.cmp(&prev) {
                    Ordering::Equal => {
                        return Err(ValidationError::DuplicateColumn {
                            table: self.name().to_string(),
                            column: col_def.name.clone(),
                        });
                    }
                    Ordering::Less => {
                        return Err(ValidationError::UnsortedColumnUpdates {
                            table: self.name().to_string(),
                            prev_idx: prev,
                            actual_idx: u.column_idx,
                        });
                    }
                    Ordering::Greater => {}
                }
            }
            last_idx = Some(u.column_idx);

            if self.primary_key().contains(&col_def.name) {
                return Err(ValidationError::CannotUpdatePrimaryKey {
                    table: self.name().to_string(),
                    column: col_def.name.clone(),
                });
            }

            validate_field_value(self.name(), &col_def.name, col_def, &u.value)?;
        }

        Ok(())
    }

    /// Rejects a CompactRow with more values than the table has columns. A shorter one is
    /// valid: its missing trailing columns read as null.
    pub(super) fn check_compact_arity(&self, len: usize) -> Result<(), ValidationError> {
        if len > self.columns().len() {
            return Err(ValidationError::CompactRowArityMismatch {
                table: self.name().to_string(),
                expected: self.columns().len(),
                actual: len,
            });
        }
        Ok(())
    }

    /// Validates an entire positional CompactRow in O(C) time against this TableSchema.
    pub fn validate_compact_row(&self, compact: &CompactRow) -> Result<(), ValidationError> {
        self.check_compact_arity(compact.len())?;

        for (i, col_def) in self.columns().iter().enumerate() {
            let val = compact.get(i).unwrap_or(&Value::Null);
            validate_field_value(self.name(), &col_def.name, col_def, val)?;
        }

        Ok(())
    }

    /// Validates a row stored under the key `pk` in O(C) time: the key, the row, and that the
    /// row's primary key columns hold the key's values.
    pub fn validate_keyed_row(
        &self,
        pk: &PrimaryKey,
        row: &CompactRow,
    ) -> Result<(), ValidationError> {
        self.validate_pk(pk)?;
        self.validate_compact_row(row)?;

        for (pk_idx, pk_col_name) in self.primary_key().iter().enumerate() {
            let col_idx = *self.column_indices().get(pk_col_name).ok_or_else(|| {
                ValidationError::UnknownColumn {
                    table: self.name().to_string(),
                    column: pk_col_name.clone(),
                }
            })?;

            if row[col_idx] != pk[pk_idx] {
                return Err(ValidationError::PrimaryKeyMismatch {
                    table: self.name().to_string(),
                });
            }
        }

        Ok(())
    }

    /// Validates an Operation against this table.
    ///
    /// An immutable table only takes inserts. In a table whose owner column is outside the
    /// primary key, every operation declares its owner: an insert in its row, an update with a
    /// delta of the owner column and a delete in its `owner`; a delete on any other table
    /// declares none. Whether the declared owner is the sender, or the owner of the row, is
    /// not checked here.
    pub fn validate_operation(&self, op: &Operation) -> Result<(), ValidationError> {
        if op.table_id != self.table_id() {
            return Err(ValidationError::TableIdMismatch {
                table: self.name().to_string(),
                expected: self.table_id(),
                actual: op.table_id,
            });
        }
        let table = || self.name().to_string();
        if self.is_immutable() && !op.is_insert() {
            return Err(ValidationError::ImmutableTable { table: table() });
        }

        let owner_outside_key = self.owner_outside_key();
        match &op.kind {
            OperationKind::Insert { row } => self.validate_keyed_row(&op.pk, row),
            OperationKind::Update { updates } => {
                self.validate_pk(&op.pk)?;
                self.validate_column_updates(updates)?;
                match owner_outside_key {
                    Some(col) if !updates.iter().any(|u| usize::from(u.column_idx) == col) => {
                        Err(ValidationError::MissingOwner { table: table() })
                    }
                    _ => Ok(()),
                }
            }
            OperationKind::Delete { owner } => {
                self.validate_pk(&op.pk)?;
                match (owner_outside_key, owner) {
                    (Some(col), Some(owner)) => {
                        let col_def = &self.columns()[col];
                        validate_field_value(self.name(), &col_def.name, col_def, owner)
                    }
                    (Some(_), None) => Err(ValidationError::MissingOwner { table: table() }),
                    (None, Some(_)) => Err(ValidationError::UnexpectedOwner { table: table() }),
                    (None, None) => Ok(()),
                }
            }
        }
    }

    /// The position of the owner column, if the table has one outside its primary key.
    fn owner_outside_key(&self) -> Option<usize> {
        self.owner_column_index()
            .filter(|&col| !self.primary_key().contains(&self.columns()[col].name))
    }

    /// The owner that `op`, valid for this table, declares: its value of the owner column, read
    /// from the primary key if the column is part of it, and otherwise from the inserted row,
    /// the update's delta or the delete's `owner`. `None` if the table has no owner.
    pub fn declared_owner<'a>(&self, op: &'a Operation) -> Option<&'a Value> {
        let col = self.owner_column_index()?;
        let column = &self.columns()[col].name;
        if let Some(key_idx) = self.primary_key().iter().position(|c| c == column) {
            return op.pk.get(key_idx);
        }
        match &op.kind {
            OperationKind::Insert { row } => row.get(col),
            OperationKind::Update { updates } => updates
                .iter()
                .find(|u| usize::from(u.column_idx) == col)
                .map(ColumnUpdate::value),
            OperationKind::Delete { owner } => owner.as_ref(),
        }
    }
}
