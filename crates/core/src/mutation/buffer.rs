use super::op::{Operation, OperationKind};
use super::squash::{client_squash_operations, SquashOutcome};
use crate::value::PrimaryKey;
use serde::de;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

/// Error returned when an operation cannot be applied to `TableBuffer`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BufferError {
    #[error(
        "Incompatible operation for table_id '{table_id}': entity state forbids this transition"
    )]
    IncompatibleOperation { table_id: u16 },
}

/// In-memory mutation buffer for a single table.
///
/// Collects pending mutations partitioned by PK and applies squashing automatically.
///
/// The pending map changes only through [`TableBuffer::apply`], which enforces the squashing
/// rules (last-write-wins, anti-zombie, mutual annihilation), and [`TableBuffer::drain`]. Every
/// entry is stored under its operation's own primary key, and every operation targets the
/// buffer's table.
///
/// Serialized form: `{"table_id": .., "pending": [operation, ...]}`. The key of each entry is
/// the operation's own primary key, so it is not serialized separately. Deserialization
/// rejects an operation of another table, a repeated primary key, and update deltas that are
/// not strictly ascending by column index (squashing merges deltas relying on that order). It
/// does not replay the squashing rules, so it trusts that the serialized buffer was built by
/// `apply`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableBuffer {
    table_id: u16,
    pending: HashMap<PrimaryKey, Operation>,
}

impl Serialize for TableBuffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        struct Pending<'a>(&'a HashMap<PrimaryKey, Operation>);

        impl Serialize for Pending<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_seq(self.0.values())
            }
        }

        let mut state = serializer.serialize_struct("TableBuffer", 2)?;
        state.serialize_field("table_id", &self.table_id)?;
        state.serialize_field("pending", &Pending(&self.pending))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for TableBuffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "TableBuffer", deny_unknown_fields)]
        struct TableBufferHelper {
            table_id: u16,
            pending: Vec<Operation>,
        }
        let helper = TableBufferHelper::deserialize(deserializer)?;
        let mut pending = HashMap::with_capacity(helper.pending.len());
        for op in helper.pending {
            if op.table_id != helper.table_id {
                return Err(de::Error::custom(format!(
                    "pending operation targets table_id {} in the buffer of table_id {}",
                    op.table_id, helper.table_id
                )));
            }
            if let OperationKind::Update { updates } = &op.kind {
                if updates
                    .windows(2)
                    .any(|pair| pair[0].column_idx >= pair[1].column_idx)
                {
                    return Err(de::Error::custom(
                        "pending update deltas must be strictly ascending by column index",
                    ));
                }
            }
            if pending.contains_key(&op.pk) {
                return Err(de::Error::custom("pending operations repeat a primary key"));
            }
            pending.insert(op.pk.clone(), op);
        }
        Ok(TableBuffer {
            table_id: helper.table_id,
            pending,
        })
    }
}

impl TableBuffer {
    pub fn new(table_id: u16) -> Self {
        Self {
            table_id,
            pending: HashMap::new(),
        }
    }

    #[inline]
    pub fn table_id(&self) -> u16 {
        self.table_id
    }

    /// Applies an operation to the buffer with move semantics and squashing.
    ///
    /// The pending entry is looked up by reference, so squashing into an existing entry never
    /// clones the primary key. A clone is made only to insert a new entry (the map key and the
    /// stored operation each own one) and to remove an entry purged by mutual annihilation.
    /// Returns `Err(BufferError::IncompatibleOperation)` if the mutation transition is incompatible.
    pub fn apply(&mut self, op: Operation) -> Result<SquashOutcome, BufferError> {
        if op.table_id != self.table_id {
            return Err(BufferError::IncompatibleOperation {
                table_id: self.table_id,
            });
        }

        match self.pending.get_mut(&op.pk) {
            Some(existing) => {
                let outcome = client_squash_operations(existing, op);
                match outcome {
                    SquashOutcome::Incompatible => Err(BufferError::IncompatibleOperation {
                        table_id: self.table_id,
                    }),
                    SquashOutcome::Purged => {
                        let pk = existing.pk.clone();
                        self.pending.remove(&pk);
                        Ok(outcome)
                    }
                    _ => Ok(outcome),
                }
            }
            None => {
                self.pending.insert(op.pk.clone(), op);
                Ok(SquashOutcome::Replaced)
            }
        }
    }

    #[inline]
    pub fn get(&self, pk: &PrimaryKey) -> Option<&Operation> {
        self.pending.get(pk)
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
    pub fn drain(&mut self) -> impl Iterator<Item = (PrimaryKey, Operation)> + '_ {
        self.pending.drain()
    }
}
