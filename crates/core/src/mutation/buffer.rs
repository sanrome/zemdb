use super::op::TableOperation;
use super::squash::{client_squash_table_operations, SquashOutcome};
use crate::value::PrimaryKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Error returned when an operation cannot be applied to `TableBuffer`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BufferError {
    #[error("Incompatible operation for table '{table}': entity state forbids this transition")]
    IncompatibleOperation { table: String },
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

    /// Applies an operation to the buffer with move semantics and squashing.
    ///
    /// Avoids unconditional cloning of the primary key by inspecting existing entries first.
    /// Returns `Err(BufferError::IncompatibleOperation)` if the mutation transition is incompatible.
    pub fn apply(&mut self, op: TableOperation) -> Result<SquashOutcome, BufferError> {
        if let Some(existing) = self.pending.get_mut(&op.pk) {
            let outcome = client_squash_table_operations(existing, op);
            if outcome == SquashOutcome::Incompatible {
                return Err(BufferError::IncompatibleOperation {
                    table: self.table.to_string(),
                });
            }
            Ok(outcome)
        } else {
            let pk = op.pk.clone();
            self.pending.insert(pk, op);
            Ok(SquashOutcome::Replaced)
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
