use super::op::TableOperation;
use super::squash::{squash_table_operations, SquashOutcome};
use crate::value::PrimaryKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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
