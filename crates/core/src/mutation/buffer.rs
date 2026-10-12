use super::op::{Operation, OperationKind};
use super::squash::{squash_operations, SquashOutcome};
use crate::schema::{TableSchema, ValidationError};
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
    #[error("Invalid operation: {0}")]
    Invalid(#[from] ValidationError),
}

/// In-memory mutation buffer for a single table.
///
/// Collects pending mutations partitioned by PK and applies squashing automatically.
///
/// The pending map changes only through [`TableBuffer::apply`], which validates each operation
/// and enforces the squashing rules (see [`squash_operations`]), and [`TableBuffer::drain`]. Every
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

    /// Validates `op` against `table`, the schema of this buffer's table, and squashes it into
    /// the pending operation for its primary key with move semantics. An invalid operation (or
    /// the schema of another table) is `BufferError::Invalid` and an incompatible one
    /// `IncompatibleOperation`; either leaves the buffer unchanged. Only valid operations are
    /// kept, so a buffer built by `apply` always deserializes again.
    ///
    /// The pending entry is looked up by reference, so squashing into an existing entry never
    /// clones the primary key; a new entry clones it once (the map key and the stored
    /// operation each own one).
    pub fn apply(
        &mut self,
        table: &TableSchema,
        op: Operation,
    ) -> Result<SquashOutcome, BufferError> {
        if table.table_id() != self.table_id {
            return Err(ValidationError::TableIdMismatch {
                table: table.name().to_string(),
                expected: self.table_id,
                actual: table.table_id(),
            }
            .into());
        }
        table.validate_operation(&op)?;

        match self.pending.get_mut(&op.pk) {
            Some(existing) => match squash_operations(existing, op) {
                SquashOutcome::Incompatible => Err(BufferError::IncompatibleOperation {
                    table_id: self.table_id,
                }),
                outcome => Ok(outcome),
            },
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
