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
pub(crate) struct RoomState {
    pub(crate) schema: Schema,
    pub(crate) head_seq: SequenceNumber,
    pub(crate) tables: HashMap<u16, Table>,
}

impl RoomState {
    /// Creates a new `RoomState` initialized from a schema with empty tables.
    pub(crate) fn new(schema: Schema) -> Self {
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
        .table_ids()
        .map(|table_id| (table_id, Table::new()))
        .collect()
}

/// A table named by the caller of a query, by name or by numeric id.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TableRef<'a> {
    Name(&'a str),
    Id(u16),
}

/// Resolves `table` to its id in `schema`, or `TableNotFound`.
///
/// Queries call it under the same read guard they then read the rows with, so the name is
/// translated against the schema those rows belong to.
pub(crate) fn resolve_table_id(
    room_id: &RoomId,
    schema: &Schema,
    table: TableRef<'_>,
) -> Result<u16, StorageError> {
    let resolved = match table {
        TableRef::Name(name) => schema.get_table_id(name),
        TableRef::Id(id) => Some(id).filter(|id| schema.has_table_by_id(*id)),
    };
    resolved.ok_or_else(|| StorageError::TableNotFound {
        room_id: room_id.clone(),
        table: match table {
            TableRef::Name(name) => name.to_string(),
            TableRef::Id(id) => format!("id:{id}"),
        },
    })
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
        if !schema.has_table_by_id(op.op.table_id()) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: format!("id:{}", op.op.table_id()),
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
    let table_id = op.table_id();
    let table = tables.entry(table_id).or_default();
    let (pk, kind) = op.into_pk_and_kind();
    match kind {
        OperationKind::Insert { row } => {
            table.insert(pk, row);
        }
        OperationKind::Update { updates } => {
            if let Some(existing) = table.get_mut(&pk) {
                let target_len = schema
                    .get_table_by_id(table_id)
                    .map(|t| t.columns().len())
                    .unwrap_or(0);
                for col_up in updates {
                    let idx = col_up.column_idx() as usize;
                    let min_len = target_len.max(idx + 1);
                    if existing.len() < min_len {
                        existing.resize(min_len, Value::Null);
                    }
                    existing[idx] = col_up.into_value();
                }
            }
        }
        OperationKind::Delete => {
            table.remove(&pk);
        }
    }
}

/// In-memory snapshot payload serialized by reference to avoid copying tables during snapshot creation.
#[derive(Debug, Serialize)]
pub(crate) struct RoomSnapshotRef<'a> {
    pub(crate) head_seq: SequenceNumber,
    pub(crate) tables: &'a HashMap<u16, Table>,
}

/// Payload format for deserialized database snapshots.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RoomSnapshotPayload {
    pub(crate) head_seq: SequenceNumber,
    pub(crate) tables: HashMap<u16, Table>,
}

impl RoomSnapshotPayload {
    /// Adds an empty table for every table of `schema` the snapshot does not contain.
    pub(crate) fn fill_missing_tables(&mut self, schema: &Schema) {
        for table_id in schema.table_ids() {
            self.tables.entry(table_id).or_default();
        }
    }
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
