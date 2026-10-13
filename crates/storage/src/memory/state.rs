use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt;
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
#[derive(Debug)]
pub(crate) struct RoomState {
    pub(crate) schema: Schema,
    pub(crate) head_seq: SequenceNumber,
    pub(crate) tables: HashMap<u16, Table>,
}

impl RoomState {
    pub(crate) fn new(schema: Schema) -> Self {
        Self {
            tables: empty_tables(&schema),
            schema,
            head_seq: SequenceNumber::from(0u64),
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
                    .map_or(0, |t| t.columns().len());
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

/// Snapshot payload that owns its tables: serialized by compaction and produced by decoding.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RoomSnapshotPayload {
    pub(crate) head_seq: SequenceNumber,
    #[serde(deserialize_with = "deserialize_tables")]
    pub(crate) tables: HashMap<u16, Table>,
}

/// Deserializes the tables of a snapshot, inserting each table and row as it is read.
///
/// The number of tables and rows is declared by the payload, which may come from another peer.
/// `imbl`'s own `Deserialize` reserves room for the declared number of rows up front, so a few
/// bytes declaring billions of rows would abort the process; here nothing is reserved, and a
/// payload that declares more rows than it holds fails when it runs out of bytes.
fn deserialize_tables<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<HashMap<u16, Table>, D::Error> {
    struct TablesVisitor;

    impl<'de> Visitor<'de> for TablesVisitor {
        type Value = HashMap<u16, Table>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map of tables by id")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
            let mut tables = HashMap::new();
            while let Some(table_id) = access.next_key::<u16>()? {
                let RowsAsRead(table) = access.next_value()?;
                tables.insert(table_id, table);
            }
            Ok(tables)
        }
    }

    deserializer.deserialize_map(TablesVisitor)
}

/// A table deserialized row by row, without reserving room for the declared number of rows.
struct RowsAsRead(Table);

impl<'de> Deserialize<'de> for RowsAsRead {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowsVisitor;

        impl<'de> Visitor<'de> for RowsVisitor {
            type Value = RowsAsRead;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map of rows by primary key")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut table = Table::new();
                while let Some((pk, row)) = access.next_entry::<PrimaryKey, CompactRow>()? {
                    table.insert(pk, row);
                }
                Ok(RowsAsRead(table))
            }
        }

        deserializer.deserialize_map(RowsVisitor)
    }
}

impl RoomSnapshotPayload {
    /// Adds an empty table for every table of `schema` the snapshot does not contain.
    pub(crate) fn fill_missing_tables(&mut self, schema: &Schema) {
        for table_id in schema.table_ids() {
            self.tables.entry(table_id).or_default();
        }
    }
}

/// Decodes a snapshot envelope that `apply_snapshot` received and checks it before anything is
/// replaced: its head sequence must be `expected_head_seq`, and every table and row must be
/// valid under `schema` (a known table, a valid key and row, and the row's key columns equal to
/// the key it is stored under). Tables of `schema` the snapshot does not contain are added
/// empty.
///
/// Every row is decoded and checked, so outside WebAssembly the engines run it on the blocking
/// pool.
pub(crate) fn decode_received_snapshot(
    envelope: &[u8],
    max_uncompressed_bytes: u64,
    schema: &Schema,
    expected_head_seq: SequenceNumber,
) -> Result<RoomSnapshotPayload, StorageError> {
    let mut payload: RoomSnapshotPayload =
        crate::snapshot::decode_snapshot_payload(envelope, max_uncompressed_bytes)?;
    if payload.head_seq != expected_head_seq {
        return Err(StorageError::SnapshotHeadMismatch {
            expected: expected_head_seq,
            actual: payload.head_seq,
        });
    }
    for (&table_id, table) in &payload.tables {
        let table_schema = schema.get_table_by_id(table_id).ok_or_else(|| {
            StorageError::SnapshotCorruption(format!(
                "Snapshot holds table id {table_id}, which is not in the schema"
            ))
        })?;
        for (pk, row) in table.iter() {
            table_schema.validate_keyed_row(pk, row).map_err(|e| {
                StorageError::SnapshotCorruption(format!(
                    "Invalid row in snapshot table '{}': {e}",
                    table_schema.name()
                ))
            })?;
        }
    }
    payload.fill_missing_tables(schema);
    Ok(payload)
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
