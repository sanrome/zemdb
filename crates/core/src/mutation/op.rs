use crate::value::{CompactRow, PrimaryKey, Value};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Atomic column update targeting a specific column by its positional DDL index.
///
/// Bounded to 32 bytes (2B column_idx + 6B padding + 24B Value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnUpdate {
    pub column_idx: u16,
    pub value: Value,
}

impl ColumnUpdate {
    pub fn new(column_idx: u16, value: impl Into<Value>) -> Self {
        Self {
            column_idx,
            value: value.into(),
        }
    }
}

/// Specific mutation variant payload.
///
/// Memory footprint is strictly bounded to 32 bytes (max payload 24 bytes + 1 byte tag + 7 bytes padding).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OperationKind {
    /// Inserts a tuple. If the PK already exists, replaces all fields (upsert).
    Insert {
        row: CompactRow,
    },
    /// Updates specific fields of an existing tuple by PK.
    /// Updates are maintained strictly sorted by `column_idx` ascending.
    Update {
        updates: Vec<ColumnUpdate>,
    },
    /// Deletes a tuple by PK.
    Delete,
}

/// Pure row mutation operation on a specific table, without table metadata redundancy.
///
/// Designed with a density of exactly 80 bytes (40B PrimaryKey + 8B timestamp + 32B OperationKind).
/// Ideal for table-partitioned in-memory storage (`TableBuffer`) in coordination servers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableOperation {
    pub pk: PrimaryKey,
    pub timestamp: u64,
    pub kind: OperationKind,
}

impl TableOperation {
    pub fn insert(pk: PrimaryKey, row: CompactRow, timestamp: u64) -> Self {
        Self {
            pk,
            timestamp,
            kind: OperationKind::Insert { row },
        }
    }

    pub fn update(pk: PrimaryKey, mut updates: Vec<ColumnUpdate>, timestamp: u64) -> Self {
        updates.sort_by_key(|u| u.column_idx);
        Self {
            pk,
            timestamp,
            kind: OperationKind::Update { updates },
        }
    }

    pub fn delete(pk: PrimaryKey, timestamp: u64) -> Self {
        Self {
            pk,
            timestamp,
            kind: OperationKind::Delete,
        }
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

/// Fully self-describing mutation operation with table namespace.
///
/// Composes `table: Arc<str>` with `TableOperation` (total 96 bytes).
/// Implements `Deref<Target = TableOperation>` for transparent zero-cost ergonomics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub table: Arc<str>,
    pub op: TableOperation,
}

impl Deref for Operation {
    type Target = TableOperation;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

impl DerefMut for Operation {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}

impl Operation {
    pub fn new(table: impl Into<Arc<str>>, op: TableOperation) -> Self {
        Self {
            table: table.into(),
            op,
        }
    }

    pub fn insert(
        table: impl Into<Arc<str>>,
        pk: PrimaryKey,
        row: CompactRow,
        timestamp: u64,
    ) -> Self {
        Self::new(table, TableOperation::insert(pk, row, timestamp))
    }

    pub fn update(
        table: impl Into<Arc<str>>,
        pk: PrimaryKey,
        updates: Vec<ColumnUpdate>,
        timestamp: u64,
    ) -> Self {
        Self::new(table, TableOperation::update(pk, updates, timestamp))
    }

    pub fn delete(table: impl Into<Arc<str>>, pk: PrimaryKey, timestamp: u64) -> Self {
        Self::new(table, TableOperation::delete(pk, timestamp))
    }

    #[inline]
    pub fn table(&self) -> &str {
        &self.table
    }

    #[inline]
    pub fn into_parts(self) -> (Arc<str>, TableOperation) {
        (self.table, self.op)
    }
}

/// Fluent builder for constructing an `Operation::Update` with positional column deltas.
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    table: Arc<str>,
    pk: PrimaryKey,
    updates: Vec<ColumnUpdate>,
    timestamp: u64,
}

impl UpdateBuilder {
    pub fn new(table: impl Into<Arc<str>>, pk: PrimaryKey) -> Self {
        Self {
            table: table.into(),
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
        Operation::update(self.table, self.pk, self.updates, self.timestamp)
    }

    pub fn build_table_op(mut self) -> TableOperation {
        self.updates.sort_by_key(|u| u.column_idx);
        TableOperation::update(self.pk, self.updates, self.timestamp)
    }
}
