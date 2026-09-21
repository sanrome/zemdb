use crate::id::SequenceNumber;
use crate::value::{CompactRow, PrimaryKey, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// An operation ordered by the coordination server with an assigned sequence ID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequencedOperation {
    pub seq: SequenceNumber,
    pub op: Operation,
}

impl SequencedOperation {
    pub fn new(seq: impl Into<SequenceNumber>, op: Operation) -> Self {
        Self {
            seq: seq.into(),
            op,
        }
    }
}

/// Result of attempting to squash two sequential operations for the same PK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SquashOutcome {
    /// The incoming operation was merged into the existing operation.
    Merged,
    /// The incoming operation completely replaced the existing operation.
    Replaced,
    /// The incoming operation was obsolete or a no-op and was discarded (existing remains unchanged).
    Discarded,
    /// Operations cannot be squashed (e.g. different table, PK, or invalid transition like Delete followed by Update).
    Incompatible,
}

/// Merges an incoming TableOperation into an existing pending TableOperation for the same PK.
pub fn squash_table_operations(
    existing: &mut TableOperation,
    incoming: TableOperation,
) -> SquashOutcome {
    if existing.pk != incoming.pk {
        return SquashOutcome::Incompatible;
    }

    match (&mut existing.kind, incoming.kind) {
        // Rule 1: INSERT followed by UPDATE -> direct slot update on CompactRow
        (OperationKind::Insert { row }, OperationKind::Update { updates }) => {
            if incoming.timestamp >= existing.timestamp {
                for u in updates {
                    let idx = u.column_idx as usize;
                    if let Some(slot) = row.values.get_mut(idx) {
                        *slot = u.value; // Move semantics: 0 clones
                    }
                }
                existing.timestamp = incoming.timestamp;
            } else {
                for u in updates {
                    let idx = u.column_idx as usize;
                    if let Some(slot) = row.values.get_mut(idx) {
                        if slot.is_null() {
                            *slot = u.value;
                        }
                    }
                }
            }
            SquashOutcome::Merged
        }

        // Rule 2: UPDATE followed by UPDATE -> sorted delta merge preserving column_idx ascending
        (
            OperationKind::Update {
                updates: existing_updates,
            },
            OperationKind::Update {
                updates: incoming_updates,
            },
        ) => {
            if incoming.timestamp >= existing.timestamp {
                for inc in incoming_updates {
                    match existing_updates.binary_search_by_key(&inc.column_idx, |u| u.column_idx) {
                        Ok(pos) => {
                            existing_updates[pos].value = inc.value;
                        }
                        Err(pos) => {
                            existing_updates.insert(pos, inc);
                        }
                    }
                }
                existing.timestamp = incoming.timestamp;
            } else {
                for inc in incoming_updates {
                    if let Err(pos) =
                        existing_updates.binary_search_by_key(&inc.column_idx, |u| u.column_idx)
                    {
                        existing_updates.insert(pos, inc);
                    }
                }
            }
            SquashOutcome::Merged
        }

        // Rule 3: DELETE followed by UPDATE (Anti-Zombie rule)
        // A partial update CANNOT resurrect a deleted entity.
        (OperationKind::Delete, OperationKind::Update { .. }) => {
            if incoming.timestamp <= existing.timestamp {
                SquashOutcome::Discarded
            } else {
                SquashOutcome::Incompatible
            }
        }

        // Rule 4: Any operation followed by DELETE
        (target_kind, OperationKind::Delete) => {
            if incoming.timestamp >= existing.timestamp {
                *target_kind = OperationKind::Delete;
                existing.timestamp = incoming.timestamp;
                SquashOutcome::Replaced
            } else {
                SquashOutcome::Discarded
            }
        }

        // Rule 5: Any operation followed by INSERT
        (target_kind, OperationKind::Insert { mut row }) => {
            if incoming.timestamp >= existing.timestamp {
                *target_kind = OperationKind::Insert { row };
                existing.timestamp = incoming.timestamp;
                SquashOutcome::Replaced
            } else {
                match target_kind {
                    OperationKind::Update { updates } => {
                        for u in updates.drain(..) {
                            let idx = u.column_idx as usize;
                            if let Some(slot) = row.values.get_mut(idx) {
                                *slot = u.value; // Move semantics: 0 clones
                            }
                        }
                        *target_kind = OperationKind::Insert { row };
                        SquashOutcome::Merged
                    }
                    OperationKind::Delete => SquashOutcome::Discarded,
                    OperationKind::Insert { .. } => SquashOutcome::Discarded,
                }
            }
        }
    }
}

/// Merges an incoming operation into an existing pending operation for the same table and PK.
pub fn squash_operations(existing: &mut Operation, incoming: Operation) -> SquashOutcome {
    if existing.table != incoming.table {
        return SquashOutcome::Incompatible;
    }
    squash_table_operations(&mut existing.op, incoming.op)
}

/// In-memory mutation buffer for a single table.
///
/// Collects pending mutations partitioned by PK and applies squashing automatically.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TableBuffer {
    pub table: Arc<str>,
    pub pending: HashMap<PrimaryKey, TableOperation>,
}

impl TableBuffer {
    pub fn new(table: impl Into<Arc<str>>) -> Self {
        Self {
            table: table.into(),
            pending: HashMap::new(),
        }
    }

    pub fn apply(&mut self, op: TableOperation) -> SquashOutcome {
        use std::collections::hash_map::Entry;
        match self.pending.entry(op.pk.clone()) {
            Entry::Occupied(mut entry) => squash_table_operations(entry.get_mut(), op),
            Entry::Vacant(entry) => {
                entry.insert(op);
                SquashOutcome::Replaced
            }
        }
    }

    #[inline]
    pub fn get(&self, pk: &PrimaryKey) -> Option<&TableOperation> {
        self.pending.get(pk)
    }

    #[inline]
    pub fn get_mut(&mut self, pk: &PrimaryKey) -> Option<&mut TableOperation> {
        self.pending.get_mut(pk)
    }

    #[inline]
    pub fn remove(&mut self, pk: &PrimaryKey) -> Option<TableOperation> {
        self.pending.remove(pk)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = (PrimaryKey, TableOperation)> + '_ {
        self.pending.drain()
    }
}
