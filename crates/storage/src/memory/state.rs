use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use zemdb_core::{
    CompactRow, Operation, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, Value,
};

use crate::error::StorageError;

/// Rows of one table, ordered by primary key.
///
/// A persistent B-tree: a clone takes O(1) and shares every node, and a write to either copy
/// duplicates only the O(log n) nodes on the path to the written key. Scans and compactions keep
/// a clone of the table while writes continue, so a write never copies the whole table.
/// It serializes as a map in key order, exactly like a `BTreeMap`.
pub type Table = imbl::OrdMap<PrimaryKey, CompactRow>;

/// In-memory state of an open database room.
///
/// Holds the room schema, monotonic head sequence number, and in-memory tables indexed by table_id.
#[derive(Debug)]
pub struct RoomState {
    pub schema: Schema,
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, Table>,
}

impl RoomState {
    /// Creates a new `RoomState` initialized from a schema with empty tables.
    pub fn new(schema: Schema) -> Self {
        let tables = empty_tables(&schema);
        Self {
            schema,
            head_seq: SequenceNumber::from(0u64),
            tables,
        }
    }
}

/// An empty table for every table of `schema`.
pub(crate) fn empty_tables(schema: &Schema) -> HashMap<u16, Table> {
    schema
        .tables_by_id
        .keys()
        .map(|table_id| (*table_id, Table::new()))
        .collect()
}

/// Validates a batch against the room's schema and checks that its sequence numbers continue
/// `head_seq` without gaps.
pub(crate) fn validate_batch(
    room_id: &RoomId,
    schema: &Schema,
    head_seq: SequenceNumber,
    ops: &[SequencedOperation],
) -> Result<(), StorageError> {
    for (expected_seq, op) in (head_seq.get() + 1..).zip(ops.iter()) {
        if !schema.has_table_by_id(op.op.table_id) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: format!("id:{}", op.op.table_id),
            });
        }

        if op.seq.get() != expected_seq {
            return Err(StorageError::SequenceMismatch {
                expected: SequenceNumber::from(expected_seq),
                actual: op.seq,
            });
        }

        schema.validate_operation(&op.op)?;
    }
    Ok(())
}

/// Applies one operation to `tables`. The operation must already be validated against `schema`.
///
/// An update widens a row written under an older schema to the table's current width.
pub(crate) fn apply_operation(tables: &mut HashMap<u16, Table>, schema: &Schema, op: Operation) {
    let table = tables.entry(op.table_id).or_default();
    match op.kind {
        OperationKind::Insert { row } => {
            table.insert(op.pk, row);
        }
        OperationKind::Update { updates } => {
            if let Some(existing) = table.get_mut(&op.pk) {
                let target_len = schema
                    .get_table_by_id(op.table_id)
                    .map(|t| t.columns().len())
                    .unwrap_or(0);
                for col_up in updates {
                    let idx = col_up.column_idx as usize;
                    let min_len = target_len.max(idx + 1);
                    if existing.len() < min_len {
                        existing.resize(min_len, Value::Null);
                    }
                    existing[idx] = col_up.value;
                }
            }
        }
        OperationKind::Delete => {
            table.remove(&op.pk);
        }
    }
}

/// In-memory snapshot payload serialized by reference to avoid copying tables during snapshot creation.
#[derive(Debug, Serialize)]
pub struct RoomSnapshotRef<'a> {
    pub head_seq: SequenceNumber,
    pub tables: &'a HashMap<u16, Table>,
}

/// Payload format for deserialized database snapshots.
#[derive(Debug, Serialize, Deserialize)]
pub struct RoomSnapshotPayload {
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, Table>,
}

impl RoomSnapshotPayload {
    /// Adds an empty table for every table of `schema` the snapshot does not contain.
    pub(crate) fn fill_missing_tables(&mut self, schema: &Schema) {
        for table_id in schema.tables_by_id.keys() {
            self.tables.entry(*table_id).or_default();
        }
    }
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
