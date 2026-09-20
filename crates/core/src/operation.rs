use crate::id::SequenceNumber;
use crate::value::{PrimaryKey, Row, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Elemental mutation operation on the database.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    /// Inserts a tuple. If the PK already exists, replaces all fields (upsert).
    Insert {
        table: String,
        pk: PrimaryKey,
        row: Row,
        timestamp: u64,
    },
    /// Updates specific fields of an existing tuple by PK.
    Update {
        table: String,
        pk: PrimaryKey,
        fields: BTreeMap<String, Value>,
        timestamp: u64,
    },
    /// Deletes a tuple by PK.
    Delete {
        table: String,
        pk: PrimaryKey,
        timestamp: u64,
    },
}

impl Operation {
    /// Inserts a tuple with default timestamp 0.
    /// In production distributed synchronization, prefer `insert_with_timestamp`
    /// with a monotonic timestamp or Hybrid Logical Clock (HLC).
    pub fn insert(table: impl Into<String>, pk: PrimaryKey, row: Row) -> Self {
        Self::Insert {
            table: table.into(),
            pk,
            row,
            timestamp: 0,
        }
    }

    pub fn insert_with_timestamp(
        table: impl Into<String>,
        pk: PrimaryKey,
        row: Row,
        timestamp: u64,
    ) -> Self {
        Self::Insert {
            table: table.into(),
            pk,
            row,
            timestamp,
        }
    }

    pub fn delete(table: impl Into<String>, pk: PrimaryKey, timestamp: u64) -> Self {
        Self::Delete {
            table: table.into(),
            pk,
            timestamp,
        }
    }

    pub fn update(table: impl Into<String>, pk: PrimaryKey) -> UpdateBuilder {
        UpdateBuilder::new(table, pk)
    }

    pub fn table(&self) -> &str {
        match self {
            Operation::Insert { table, .. } => table,
            Operation::Update { table, .. } => table,
            Operation::Delete { table, .. } => table,
        }
    }

    pub fn pk(&self) -> &PrimaryKey {
        match self {
            Operation::Insert { pk, .. } => pk,
            Operation::Update { pk, .. } => pk,
            Operation::Delete { pk, .. } => pk,
        }
    }

    pub fn timestamp(&self) -> u64 {
        match self {
            Operation::Insert { timestamp, .. } => *timestamp,
            Operation::Update { timestamp, .. } => *timestamp,
            Operation::Delete { timestamp, .. } => *timestamp,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Operation::Delete { .. })
    }
}

/// Fluent builder for constructing an Operation::Update.
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    table: String,
    pk: PrimaryKey,
    fields: BTreeMap<String, Value>,
    timestamp: u64,
}

impl UpdateBuilder {
    pub fn new(table: impl Into<String>, pk: PrimaryKey) -> Self {
        Self {
            table: table.into(),
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

    pub fn build(self) -> Operation {
        Operation::Update {
            table: self.table,
            pk: self.pk,
            fields: self.fields,
            timestamp: self.timestamp,
        }
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

/// Result of attempting to squash two sequential operations for the same (table, PK).
#[derive(Debug, PartialEq, Eq)]
pub enum SquashOutcome {
    /// The incoming operation was merged into the existing operation.
    Merged,
    /// The incoming operation completely replaced the existing operation.
    Replaced,
    /// Operations cannot be squashed (e.g. different table, PK, or invalid transition like Delete followed by Update).
    Incompatible,
}

/// Merges an incoming operation into an existing pending operation for the same table and PK.
/// Returns `SquashOutcome`.
pub fn squash_operations(existing: &mut Operation, incoming: Operation) -> SquashOutcome {
    if existing.table() != incoming.table() || existing.pk() != incoming.pk() {
        return SquashOutcome::Incompatible;
    }

    match (existing, incoming) {
        // Rule 1: INSERT followed by UPDATE -> merge update fields into INSERT row
        (
            Operation::Insert {
                row,
                timestamp: ins_ts,
                ..
            },
            Operation::Update {
                fields,
                timestamp: up_ts,
                ..
            },
        ) => {
            if up_ts >= *ins_ts {
                for (k, v) in fields {
                    row.insert(k, v);
                }
                *ins_ts = up_ts;
            } else {
                for (k, v) in fields {
                    row.entry(k).or_insert(v);
                }
            }
            SquashOutcome::Merged
        }

        // Rule 2: UPDATE followed by UPDATE -> field-level merge with LWW
        (
            Operation::Update {
                fields: existing_fields,
                timestamp: existing_ts,
                ..
            },
            Operation::Update {
                fields: incoming_fields,
                timestamp: incoming_ts,
                ..
            },
        ) => {
            if incoming_ts >= *existing_ts {
                for (k, v) in incoming_fields {
                    existing_fields.insert(k, v);
                }
                *existing_ts = incoming_ts;
            } else {
                for (k, v) in incoming_fields {
                    existing_fields.entry(k).or_insert(v);
                }
            }
            SquashOutcome::Merged
        }

        // Rule 3: DELETE followed by UPDATE -> ANTI-ZOMBIE RULE!
        // A partial update CANNOT resurrect a deleted entity.
        (Operation::Delete { .. }, Operation::Update { .. }) => {
            SquashOutcome::Incompatible
        }

        // Rule 4: Any operation followed by DELETE -> DELETE replaces and purges prior mutations if newer
        (target, incoming @ Operation::Delete { timestamp: del_ts, .. }) => {
            if del_ts >= target.timestamp() {
                *target = incoming;
                SquashOutcome::Replaced
            } else {
                SquashOutcome::Merged
            }
        }

        // Rule 5: Any operation followed by INSERT -> new INSERT completely overwrites (Upsert) if newer
        (target, incoming @ Operation::Insert { timestamp: ins_ts, .. }) => {
            if ins_ts >= target.timestamp() {
                *target = incoming;
                SquashOutcome::Replaced
            } else {
                SquashOutcome::Merged
            }
        }
    }
}
