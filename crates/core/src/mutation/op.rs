use crate::value::row::deserialize_columns;
use crate::value::{CompactRow, PrimaryKey, Value};
use serde::{Deserialize, Serialize};

/// Atomic column update targeting a specific column by its positional DDL index.
///
/// Bounded to 32 bytes (2B column_idx + 6B padding + 24B Value).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnUpdate {
    pub(crate) column_idx: u16,
    pub(crate) value: Value,
}

impl ColumnUpdate {
    pub fn new(column_idx: u16, value: impl Into<Value>) -> Self {
        Self {
            column_idx,
            value: value.into(),
        }
    }

    #[inline]
    pub fn column_idx(&self) -> u16 {
        self.column_idx
    }

    #[inline]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Consumes the update, returning its value without cloning it.
    #[inline]
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// Specific mutation variant payload.
///
/// Memory footprint is strictly bounded to 32 bytes (max payload 24 bytes + 1 byte tag + 7 bytes padding).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationKind {
    /// Inserts a tuple. If the PK already exists, replaces all fields (upsert).
    Insert { row: CompactRow },
    /// Updates specific fields of an existing tuple by PK.
    /// Updates are maintained strictly sorted by `column_idx` ascending. Deserialization
    /// rejects more than [`MAX_COLUMNS`](crate::schema::MAX_COLUMNS) deltas.
    Update {
        #[serde(deserialize_with = "deserialize_columns")]
        updates: Vec<ColumnUpdate>,
    },
    /// Deletes a tuple by PK.
    Delete,
}

/// Fully self-describing mutation operation with table ID.
///
/// Occupies exactly 88 bytes in memory (2B table_id + 6B padding + 8B timestamp + 40B pk + 32B kind).
/// 100% stack-allocated, zero heap pointers, aligned to 8 bytes.
///
/// The fields are read through accessors and are not mutable from outside the crate. The
/// constructors keep update deltas sorted by `column_idx`. This does not make every
/// `Operation` valid: it is a wire type, and one decoded from a client message or a WAL record
/// comes straight from `Deserialize`, with whatever table, primary key and deltas it was
/// encoded with. Only [`Schema::validate_operation`](crate::schema::Schema::validate_operation)
/// checks an operation against the table it targets (table id, primary key types, row arity
/// and types, deltas strictly ascending and in range); the server and the storage engines run
/// it on every operation they receive before using it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Operation {
    pub(crate) table_id: u16,
    pub(crate) timestamp: u64,
    pub(crate) pk: PrimaryKey,
    pub(crate) kind: OperationKind,
}

impl Operation {
    /// Builds an operation of any kind. Update deltas are sorted by `column_idx`, as in
    /// [`Operation::update`].
    pub fn new(table_id: u16, pk: PrimaryKey, timestamp: u64, kind: OperationKind) -> Self {
        let kind = match kind {
            OperationKind::Update { mut updates } => {
                updates.sort_by_key(|u| u.column_idx);
                OperationKind::Update { updates }
            }
            other => other,
        };
        Self {
            table_id,
            timestamp,
            pk,
            kind,
        }
    }

    pub fn insert(table_id: u16, pk: PrimaryKey, row: CompactRow, timestamp: u64) -> Self {
        Self {
            table_id,
            timestamp,
            pk,
            kind: OperationKind::Insert { row },
        }
    }

    pub fn update(
        table_id: u16,
        pk: PrimaryKey,
        mut updates: Vec<ColumnUpdate>,
        timestamp: u64,
    ) -> Self {
        updates.sort_by_key(|u| u.column_idx);
        Self {
            table_id,
            timestamp,
            pk,
            kind: OperationKind::Update { updates },
        }
    }

    pub fn delete(table_id: u16, pk: PrimaryKey, timestamp: u64) -> Self {
        Self {
            table_id,
            timestamp,
            pk,
            kind: OperationKind::Delete,
        }
    }

    #[inline]
    pub fn table_id(&self) -> u16 {
        self.table_id
    }

    #[inline]
    pub fn pk(&self) -> &PrimaryKey {
        &self.pk
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    #[inline]
    pub fn kind(&self) -> &OperationKind {
        &self.kind
    }

    /// Consumes the operation, returning its primary key and payload without cloning them.
    #[inline]
    pub fn into_pk_and_kind(self) -> (PrimaryKey, OperationKind) {
        (self.pk, self.kind)
    }

    #[inline]
    pub fn is_delete(&self) -> bool {
        matches!(self.kind, OperationKind::Delete)
    }

    #[inline]
    pub fn is_insert(&self) -> bool {
        matches!(self.kind, OperationKind::Insert { .. })
    }

    #[inline]
    pub fn is_update(&self) -> bool {
        matches!(self.kind, OperationKind::Update { .. })
    }
}

/// Fluent builder for constructing an `Operation::Update` with positional column deltas.
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    table_id: u16,
    pk: PrimaryKey,
    updates: Vec<ColumnUpdate>,
    timestamp: u64,
}

impl UpdateBuilder {
    pub fn new(table_id: u16, pk: PrimaryKey) -> Self {
        Self {
            table_id,
            pk,
            updates: Vec::new(),
            timestamp: 0,
        }
    }

    pub fn set(mut self, column_idx: u16, value: impl Into<Value>) -> Self {
        self.updates.push(ColumnUpdate::new(column_idx, value));
        self
    }

    pub fn timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }

    pub fn build(mut self) -> Operation {
        self.updates.sort_by_key(|u| u.column_idx);
        Operation::update(self.table_id, self.pk, self.updates, self.timestamp)
    }
}
