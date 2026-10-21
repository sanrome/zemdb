use super::column::ColumnDef;
use super::constraint::{
    ForeignKey, OwnerDef, OwnerIdentity, UniqueConstraint, MAX_CONSTRAINTS_PER_TABLE,
    MAX_CONSTRAINT_COLUMNS,
};
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

/// The positions of the columns of the constraint `constraint`, in its order. Rejects an empty
/// constraint, one over too many columns, and one with an unknown, repeated or encrypted
/// column: a ciphertext is random, so no replica could compare it.
fn constraint_column_indices(
    table: &str,
    constraint: &str,
    names: &[String],
    columns: &[ColumnDef],
    column_indices: &BTreeMap<String, usize>,
) -> Result<Vec<usize>, ValidationError> {
    if names.is_empty() {
        return Err(ValidationError::EmptyConstraint {
            table: table.to_string(),
            constraint: constraint.to_string(),
        });
    }
    if names.len() > MAX_CONSTRAINT_COLUMNS {
        return Err(ValidationError::TooManyConstraintColumns {
            table: table.to_string(),
            constraint: constraint.to_string(),
            max: MAX_CONSTRAINT_COLUMNS,
        });
    }
    let mut positions = Vec::with_capacity(names.len());
    for column in names {
        let Some(&idx) = column_indices.get(column) else {
            return Err(ValidationError::UnknownConstraintColumn {
                table: table.to_string(),
                constraint: constraint.to_string(),
                column: column.clone(),
            });
        };
        if positions.contains(&idx) {
            return Err(ValidationError::DuplicateConstraintColumn {
                table: table.to_string(),
                constraint: constraint.to_string(),
                column: column.clone(),
            });
        }
        if columns[idx].encrypted {
            return Err(ValidationError::EncryptedConstraintColumn {
                table: table.to_string(),
                constraint: constraint.to_string(),
                column: column.clone(),
            });
        }
        positions.push(idx);
    }
    Ok(positions)
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
    unique: Vec<UniqueConstraint>,
    foreign_keys: Vec<ForeignKey>,
    owner: Option<OwnerDef>,
    immutable: bool,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table_id: None,
            name: name.into(),
            primary_key: Vec::new(),
            columns: Vec::new(),
            unique: Vec::new(),
            foreign_keys: Vec::new(),
            owner: None,
            immutable: false,
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

    /// Declares a `UNIQUE` constraint named `name` over `columns`, after the ones declared so
    /// far. It is validated when the table is built.
    pub fn unique(
        mut self,
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.unique.push(UniqueConstraint::new(name, columns));
        self
    }

    /// Declares a foreign key named `name` from `columns` to the primary key of the table
    /// named `references`, after the ones declared so far. It is validated when the table is
    /// built, and its reference when the schema is built.
    pub fn foreign_key(
        mut self,
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
        references: impl Into<String>,
    ) -> Self {
        self.foreign_keys
            .push(ForeignKey::new(name, columns, references));
        self
    }

    /// Declares `column` as the owner column, identifying each row's owner by `identity`. It is
    /// validated when the table is built.
    pub fn owner(mut self, column: impl Into<String>, identity: OwnerIdentity) -> Self {
        self.owner = Some(OwnerDef::new(column, identity));
        self
    }

    /// Makes the table immutable: its rows are inserted once and never replaced, updated or
    /// deleted.
    pub fn immutable(mut self) -> Self {
        self.immutable = true;
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
        let table = TableSchema::try_with_constraints(
            table_id,
            self.name,
            self.primary_key,
            self.columns,
            self.unique,
            self.foreign_keys,
        )?;
        Ok(table
            .try_with_owner(self.owner)?
            .with_immutable(self.immutable))
    }
}

/// Schema definition for a single table.
///
/// Preserves physical column order (DDL definition order) in `columns`
/// to guarantee stable binary positional serialization and schema evolution,
/// while providing O(log C) column lookups via `column_indices`.
///
/// The `UNIQUE` constraints and foreign keys are kept in declaration order, which is the
/// order in which they are checked. Their column positions are precomputed like
/// `column_indices`; like it, they are derived from the serialized fields and never
/// serialized themselves. So is the position of the owner column, if the table has one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TableSchema {
    table_id: u16,
    name: String,
    primary_key: Vec<String>,
    columns: Vec<ColumnDef>,
    unique: Vec<UniqueConstraint>,
    foreign_keys: Vec<ForeignKey>,
    owner: Option<OwnerDef>,
    immutable: bool,
    #[serde(skip)]
    column_indices: BTreeMap<String, usize>,
    #[serde(skip)]
    unique_column_indices: Vec<Vec<usize>>,
    #[serde(skip)]
    foreign_key_column_indices: Vec<Vec<usize>>,
    #[serde(skip)]
    owner_column: Option<usize>,
}

impl<'de> Deserialize<'de> for TableSchema {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // `table_id` has no default: a table read from JSON or disk keeps exactly the id it was
        // written with, and one without an id is rejected rather than numbered on the fly.
        // The constraint lists default to empty in JSON, and the owner and immutability to
        // none, so a table written before they existed still reads; the binary encoding is
        // positional and always carries them.
        #[derive(Deserialize)]
        #[serde(rename = "TableSchema", deny_unknown_fields)]
        struct TableSchemaHelper {
            table_id: u16,
            name: String,
            primary_key: Vec<String>,
            columns: Vec<ColumnDef>,
            #[serde(default)]
            unique: Vec<UniqueConstraint>,
            #[serde(default)]
            foreign_keys: Vec<ForeignKey>,
            #[serde(default)]
            owner: Option<OwnerDef>,
            #[serde(default)]
            immutable: bool,
        }
        let TableSchemaHelper {
            table_id,
            name,
            primary_key,
            columns,
            unique,
            foreign_keys,
            owner,
            immutable,
        } = TableSchemaHelper::deserialize(deserializer)?;
        TableSchema::try_with_constraints(
            table_id,
            name,
            primary_key,
            columns,
            unique,
            foreign_keys,
        )
        .and_then(|table| table.try_with_owner(owner))
        .map(|table| table.with_immutable(immutable))
        .map_err(serde::de::Error::custom)
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

    /// The `UNIQUE` constraints, in declaration order.
    #[inline]
    pub fn unique_constraints(&self) -> &[UniqueConstraint] {
        &self.unique
    }

    /// The foreign keys, in declaration order.
    #[inline]
    pub fn foreign_keys(&self) -> &[ForeignKey] {
        &self.foreign_keys
    }

    /// The column positions of each `UNIQUE` constraint, aligned with
    /// [`Self::unique_constraints`].
    #[inline]
    pub fn unique_column_indices(&self) -> &[Vec<usize>] {
        &self.unique_column_indices
    }

    /// The column positions of each foreign key, aligned with [`Self::foreign_keys`].
    #[inline]
    pub fn foreign_key_column_indices(&self) -> &[Vec<usize>] {
        &self.foreign_key_column_indices
    }

    /// The owner of the table's rows, if it has one.
    #[inline]
    pub fn owner(&self) -> Option<&OwnerDef> {
        self.owner.as_ref()
    }

    /// The position of the owner column, if the table has an owner.
    #[inline]
    pub fn owner_column_index(&self) -> Option<usize> {
        self.owner_column
    }

    /// Whether the table is immutable: its rows are inserted once and never replaced, updated
    /// or deleted.
    #[inline]
    pub fn is_immutable(&self) -> bool {
        self.immutable
    }

    /// Validates all structural schema invariants and constructs a `TableSchema` without
    /// constraints.
    pub fn try_new(
        table_id: u16,
        name: impl Into<String>,
        primary_key: Vec<String>,
        columns: Vec<ColumnDef>,
    ) -> Result<Self, ValidationError> {
        Self::try_with_constraints(table_id, name, primary_key, columns, Vec::new(), Vec::new())
    }

    /// Validates all structural schema invariants, those of the constraints included, and
    /// constructs a `TableSchema`.
    ///
    /// What depends on other tables (that a foreign key's table exists and matches it, and
    /// that constraint names are unique in the schema) is checked when the table joins a
    /// [`Schema`](super::Schema).
    pub fn try_with_constraints(
        table_id: u16,
        name: impl Into<String>,
        primary_key: Vec<String>,
        columns: Vec<ColumnDef>,
        unique: Vec<UniqueConstraint>,
        foreign_keys: Vec<ForeignKey>,
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

        if unique.len() + foreign_keys.len() > MAX_CONSTRAINTS_PER_TABLE {
            return Err(ValidationError::TooManyConstraints {
                table: name,
                max: MAX_CONSTRAINTS_PER_TABLE,
            });
        }
        let mut unique_column_indices = Vec::with_capacity(unique.len());
        for constraint in &unique {
            let positions = constraint_column_indices(
                &name,
                &constraint.name,
                &constraint.columns,
                &columns,
                &column_indices,
            )?;
            // The primary key is already unique: the same columns, in any order, would only
            // duplicate it. The columns are distinct, so equal length and inclusion suffice.
            if constraint.columns.len() == primary_key.len()
                && constraint.columns.iter().all(|c| primary_key.contains(c))
            {
                return Err(ValidationError::UniqueIsPrimaryKey {
                    table: name,
                    constraint: constraint.name.clone(),
                });
            }
            unique_column_indices.push(positions);
        }
        let foreign_key_column_indices = foreign_keys
            .iter()
            .map(|fk| {
                constraint_column_indices(&name, &fk.name, &fk.columns, &columns, &column_indices)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TableSchema {
            table_id,
            name,
            primary_key,
            columns,
            unique,
            foreign_keys,
            owner: None,
            immutable: false,
            column_indices,
            unique_column_indices,
            foreign_key_column_indices,
            owner_column: None,
        })
    }

    /// This table with its rows owned by the client that `owner`'s column identifies, or
    /// without an owner. The column exists, is neither nullable nor encrypted, and is a
    /// `String`, the owner's `ClientId`; pseudonymous owners are not supported yet.
    pub fn try_with_owner(mut self, owner: Option<OwnerDef>) -> Result<Self, ValidationError> {
        self.owner_column = match &owner {
            None => None,
            Some(owner) if owner.identity != OwnerIdentity::ClientId => {
                return Err(ValidationError::UnsupportedOwnerIdentity { table: self.name });
            }
            Some(owner) => {
                let Some(col) = self.get_column(&owner.column) else {
                    return Err(ValidationError::UnknownColumn {
                        table: self.name,
                        column: owner.column.clone(),
                    });
                };
                if col.nullable || col.encrypted || col.data_type != DataType::String {
                    return Err(ValidationError::InvalidOwnerColumn {
                        table: self.name,
                        column: owner.column.clone(),
                    });
                }
                self.column_index(&owner.column)
            }
        };
        self.owner = owner;
        Ok(self)
    }

    /// Whether the table declares a constraint, an owner or immutability.
    pub(crate) fn has_constraints(&self) -> bool {
        !self.unique.is_empty()
            || !self.foreign_keys.is_empty()
            || self.owner.is_some()
            || self.immutable
    }

    /// Whether `other` has this table's id and the same constraints, owner and immutability,
    /// over the same column positions.
    pub(crate) fn same_constraints(&self, other: &TableSchema) -> bool {
        self.table_id == other.table_id
            && self.unique == other.unique
            && self.unique_column_indices == other.unique_column_indices
            && self.foreign_keys == other.foreign_keys
            && self.foreign_key_column_indices == other.foreign_key_column_indices
            && self.owner == other.owner
            && self.owner_column == other.owner_column
            && self.immutable == other.immutable
    }

    /// Whether this table, the one with the id of `older`, is `older` with columns added after
    /// its own: the same name and primary key, the columns of `older` as its first ones (same
    /// names, types, nullability and encryption), and every further column nullable.
    pub(crate) fn extends(&self, older: &TableSchema) -> bool {
        self.name == older.name
            && self.primary_key == older.primary_key
            && self.columns.starts_with(&older.columns)
            && self.columns[older.columns.len()..]
                .iter()
                .all(ColumnDef::is_nullable)
    }

    /// This table, immutable if `immutable` is set.
    pub fn with_immutable(mut self, immutable: bool) -> Self {
        self.immutable = immutable;
        self
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

    /// This table with `col` appended (as [`Self::add_column`]) and the constraints added
    /// after its own, each of which must include the new column. What depends on other tables
    /// is left to the schema.
    pub(crate) fn with_added_column(
        &self,
        col: ColumnDef,
        unique: Vec<UniqueConstraint>,
        foreign_keys: Vec<ForeignKey>,
    ) -> Result<TableSchema, ValidationError> {
        let mut next = self.clone();
        let index = usize::from(next.add_column(col)?);
        let column = &next.columns[index].name;
        let added = unique
            .iter()
            .map(|c| (&c.name, &c.columns))
            .chain(foreign_keys.iter().map(|fk| (&fk.name, &fk.columns)));
        for (constraint, columns) in added {
            if !columns.contains(column) {
                return Err(ValidationError::ConstraintOmitsNewColumn {
                    table: next.name.clone(),
                    constraint: constraint.clone(),
                    column: column.clone(),
                });
            }
        }
        next.unique.extend(unique);
        next.foreign_keys.extend(foreign_keys);
        let (owner, immutable) = (next.owner, next.immutable);
        TableSchema::try_with_constraints(
            next.table_id,
            next.name,
            next.primary_key,
            next.columns,
            next.unique,
            next.foreign_keys,
        )?
        .try_with_owner(owner)
        .map(|table| table.with_immutable(immutable))
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

    /// Validates an update payload and compiles it into an `Operation::update`, which must be
    /// valid for the table as a whole (see [`TableSchema::validate_operation`]).
    pub fn to_operation_update(
        &self,
        pk: PrimaryKey,
        fields: &BTreeMap<String, Value>,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        self.validate_pk(&pk)?;
        let updates = self.compact_update_fields(fields)?;
        let op = Operation::update(self.table_id, pk, updates, timestamp);
        self.validate_operation(&op)?;
        Ok(op)
    }

    /// Validates a PK and compiles it into an `Operation::delete` that declares no owner,
    /// which a table with its owner outside the primary key rejects (see
    /// [`Operation::delete_with_owner`]).
    pub fn to_operation_delete(
        &self,
        pk: PrimaryKey,
        timestamp: u64,
    ) -> Result<Operation, ValidationError> {
        let op = Operation::delete(self.table_id, pk, timestamp);
        self.validate_operation(&op)?;
        Ok(op)
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
