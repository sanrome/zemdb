use super::column::ColumnDef;
use super::validation::ValidationError;
use crate::mutation::{ColumnUpdate, Operation};
use crate::value::{CompactRow, DataType, PrimaryKey, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// Maximum number of columns of a table. Columns are addressed by a `u16` positional index
/// (`ColumnUpdate::column_idx`, the result of `add_column`), so every index must fit in it.
pub const MAX_COLUMNS: usize = u16::MAX as usize;

/// Rejects a column declared with `DataType::Null`: that is the type of `Value::Null`, not a
/// column type.
fn check_column_type(table: &str, col: &ColumnDef) -> Result<(), ValidationError> {
    if col.data_type == DataType::Null {
        return Err(ValidationError::InvalidColumnDataType {
            table: table.to_string(),
            column: col.name.clone(),
        });
    }
    Ok(())
}

/// Builder for constructing TableSchema instances declaratively.
///
/// The table id is explicit ([`TableBuilder::table_id`]) or, when the builder is handed to
/// [`SchemaBuilder::table`](super::SchemaBuilder::table), assigned by the schema builder.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    table_id: Option<u16>,
    name: String,
    primary_key: Vec<String>,
    columns: Vec<ColumnDef>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table_id: None,
            name: name.into(),
            primary_key: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn table_id(mut self, id: u16) -> Self {
        self.table_id = Some(id);
        self
    }

    pub(crate) fn explicit_table_id(&self) -> Option<u16> {
        self.table_id
    }

    /// Declares a primary key column, recording both its name and data type.
    /// Primary key columns are automatically enforced as non-nullable.
    /// Can be called multiple times to define composite primary keys.
    pub fn primary_key(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        if !self.primary_key.contains(&name) {
            self.primary_key.push(name.clone());
        }
        self.upsert_column(name, data_type).nullable = false;
        self
    }

    /// Declares a column (non-nullable if new); re-declaring only changes its type.
    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.upsert_column(name.into(), data_type);
        self
    }

    /// Declares a nullable column; re-declaring changes its type and makes it nullable.
    pub fn nullable_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.upsert_column(name.into(), data_type).nullable = true;
        self
    }

    /// Declares an end-to-end encrypted column; re-declaring changes its type and makes it
    /// encrypted. The data_type represents the underlying decrypted type for clients,
    /// while the server only accepts opaque Value::Bytes.
    pub fn encrypted_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.upsert_column(name.into(), data_type).encrypted = true;
        self
    }

    /// The column `name` with its type set to `data_type`, appended (neither nullable nor
    /// encrypted) if it was not declared yet. An existing column keeps its flags.
    fn upsert_column(&mut self, name: String, data_type: DataType) -> &mut ColumnDef {
        let idx = match self.columns.iter().position(|c| c.name == name) {
            Some(idx) => idx,
            None => {
                self.columns.push(ColumnDef::new(name, data_type));
                self.columns.len() - 1
            }
        };
        let col = &mut self.columns[idx];
        col.data_type = data_type;
        col
    }

    /// Validates and builds the TableSchema. The table id must have been set explicitly;
    /// otherwise this fails with `ValidationError::MissingTableId`.
    pub fn build(self) -> Result<TableSchema, ValidationError> {
        let table_id = self
            .table_id
            .ok_or_else(|| ValidationError::MissingTableId {
                table: self.name.clone(),
            })?;
        self.build_with_id(table_id)
    }

    pub(crate) fn build_with_id(self, table_id: u16) -> Result<TableSchema, ValidationError> {
        TableSchema::try_new(table_id, self.name, self.primary_key, self.columns)
    }
}

/// Schema definition for a single table.
///
/// Preserves physical column order (DDL definition order) in `columns`
/// to guarantee stable binary positional serialization and schema evolution,
/// while providing O(log C) column lookups via `column_indices`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TableSchema {
    table_id: u16,
    name: String,
    primary_key: Vec<String>,
    columns: Vec<ColumnDef>,
    #[serde(skip)]
    column_indices: BTreeMap<String, usize>,
}

impl<'de> Deserialize<'de> for TableSchema {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // `table_id` has no default: a table read from JSON or disk keeps exactly the id it was
        // written with, and one without an id is rejected rather than numbered on the fly.
        #[derive(Deserialize)]
        #[serde(rename = "TableSchema", deny_unknown_fields)]
        struct TableSchemaHelper {
            table_id: u16,
            name: String,
            primary_key: Vec<String>,
            columns: Vec<ColumnDef>,
        }
        let TableSchemaHelper {
            table_id,
            name,
            primary_key,
            columns,
        } = TableSchemaHelper::deserialize(deserializer)?;
        TableSchema::try_new(table_id, name, primary_key, columns).map_err(serde::de::Error::custom)
    }
}

impl TableSchema {
    #[inline]
    pub fn table_id(&self) -> u16 {
        self.table_id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    #[inline]
    pub fn column_indices(&self) -> &BTreeMap<String, usize> {
        &self.column_indices
    }

    /// Validates all structural schema invariants and constructs a `TableSchema`.
    pub fn try_new(
        table_id: u16,
        name: impl Into<String>,
        primary_key: Vec<String>,
        columns: Vec<ColumnDef>,
    ) -> Result<Self, ValidationError> {
        let name = name.into();
        if primary_key.is_empty() {
            return Err(ValidationError::EmptyPrimaryKeyDefinition(name));
        }
        if columns.len() > MAX_COLUMNS {
            return Err(ValidationError::TooManyColumns {
                table: name,
                max: MAX_COLUMNS,
            });
        }

        let mut column_indices = BTreeMap::new();
        for (i, col) in columns.iter().enumerate() {
            check_column_type(&name, col)?;

            if column_indices.insert(col.name.clone(), i).is_some() {
                return Err(ValidationError::DuplicateColumn {
                    table: name.clone(),
                    column: col.name.clone(),
                });
            }
        }

        for pk_col in &primary_key {
            let col_idx =
                column_indices
                    .get(pk_col)
                    .ok_or_else(|| ValidationError::UnknownColumn {
                        table: name.clone(),
                        column: pk_col.clone(),
                    })?;
            let col_def = &columns[*col_idx];

            if col_def.encrypted {
                return Err(ValidationError::EncryptedPrimaryKeyNotAllowed {
                    table: name.clone(),
                    column: pk_col.clone(),
                });
            }

            if col_def.nullable {
                return Err(ValidationError::NullablePrimaryKeyNotAllowed {
                    table: name.clone(),
                    column: pk_col.clone(),
                });
            }
        }

        Ok(TableSchema {
            table_id,
            name,
            primary_key,
            columns,
            column_indices,
        })
    }

    pub fn builder(name: impl Into<String>) -> TableBuilder {
        TableBuilder::new(name)
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_indices.get(name).map(|&idx| &self.columns[idx])
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_indices.get(name).copied()
    }

    /// Appends a nullable column (append-only evolution) and returns its positional index.
    pub fn add_column(&mut self, col: ColumnDef) -> Result<u16, ValidationError> {
        if self.columns.len() >= MAX_COLUMNS {
            return Err(ValidationError::TooManyColumns {
                table: self.name.clone(),
                max: MAX_COLUMNS,
            });
        }

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

        check_column_type(&self.name, &col)?;

        let new_idx = self.columns.len();
        self.column_indices.insert(col.name.clone(), new_idx);
        self.columns.push(col);
        Ok(new_idx as u16)
    }

    pub fn extract_pk(&self, row: &Row) -> Result<PrimaryKey, ValidationError> {
        let pk_values = self
            .primary_key
            .iter()
            .map(|pk_col| {
                row.get(pk_col)
                    .cloned()
                    .ok_or_else(|| ValidationError::MissingPrimaryKeyColumn {
                        table: self.name.clone(),
                        column: pk_col.clone(),
                    })
            })
            .collect::<Result<_, _>>()?;
        let pk = PrimaryKey::from_smallvec(pk_values);
        self.validate_pk(&pk)?;
        Ok(pk)
    }

    /// Converts a validated Row to CompactRow ordered by physical DDL schema columns.
    pub fn to_compact_row(&self, row: &Row) -> Result<CompactRow, ValidationError> {
        self.validate_row(row)?;
        let values = self
            .columns
            .iter()
            .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
            .collect();
        Ok(CompactRow::new(values))
    }

    /// Zero-copy conversion of Row to CompactRow ordered by physical DDL schema columns.
    pub fn row_into_compact(&self, mut row: Row) -> Result<CompactRow, ValidationError> {
        self.validate_row(&row)?;
        let values = self
            .columns
            .iter()
            .map(|col| row.remove(&col.name).unwrap_or(Value::Null))
            .collect();
        Ok(CompactRow::new(values))
    }

    /// Converts a CompactRow back to a structured Row, validating arity.
    ///
    /// Allows `compact.len() <= self.columns.len()`, treating omitted columns as null.
    pub fn from_compact_row(&self, compact: &CompactRow) -> Result<Row, ValidationError> {
        self.check_compact_arity(compact.len())?;
        Ok(self
            .columns
            .iter()
            .zip(compact.iter())
            .filter(|(_, val)| !val.is_null())
            .map(|(col, val)| (col.name.clone(), val.clone()))
            .collect())
    }

    /// Consuming variant of [`Self::from_compact_row`].
    pub fn compact_into_row(&self, compact: CompactRow) -> Result<Row, ValidationError> {
        self.check_compact_arity(compact.len())?;
        Ok(self
            .columns
            .iter()
            .zip(compact.into_values())
            .filter(|(_, val)| !val.is_null())
            .map(|(col, val)| (col.name.clone(), val))
            .collect())
    }

    /// Converts and validates named update fields into positional `ColumnUpdate` deltas
    /// strictly ordered by ascending `column_idx`.
    pub fn compact_update_fields(
        &self,
        fields: &BTreeMap<String, Value>,
    ) -> Result<Vec<ColumnUpdate>, ValidationError> {
        self.validate_update(fields)?;
        let mut updates: Vec<_> = fields
            .iter()
            .map(|(col_name, val)| {
                let col_idx = self.column_index(col_name).expect("already validated") as u16;
                ColumnUpdate::new(col_idx, val.clone())
            })
            .collect();
        updates.sort_by_key(|u| u.column_idx);
        Ok(updates)
    }

    /// Converts positional `ColumnUpdate` deltas back into a named field map.
    pub fn expand_update_fields(
        &self,
        updates: &[ColumnUpdate],
    ) -> Result<BTreeMap<String, Value>, ValidationError> {
        updates
            .iter()
            .map(|u| {
                let col = self.columns.get(u.column_idx as usize).ok_or_else(|| {
                    ValidationError::UnknownColumn {
                        table: self.name.clone(),
                        column: format!("index {}", u.column_idx),
                    }
                })?;
                Ok((col.name.clone(), u.value.clone()))
            })
            .collect()
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
