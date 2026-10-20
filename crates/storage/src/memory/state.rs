use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt;
use zemdb_core::{
    CompactRow, Operation, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, Value,
};

use crate::engine::RejectReason;
use crate::error::StorageError;
use crate::memory::index::TableIndexes;

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
    pub(crate) data: RoomData,
}

impl RoomState {
    pub(crate) fn new(schema: Schema) -> Self {
        Self {
            data: RoomData::empty(&schema),
            schema,
            head_seq: SequenceNumber::from(0u64),
        }
    }
}

/// The rows of a room's tables and the indexes of their constraints under the room's schema.
///
/// The indexes are derived from the rows: they are built from them, change only with them (in
/// [`apply_operation`]) and are cloned and replaced with them. A clone takes O(1) per table and
/// per constraint.
#[derive(Debug, Clone)]
pub(crate) struct RoomData {
    tables: HashMap<u16, Table>,
    indexes: HashMap<u16, TableIndexes>,
}

impl RoomData {
    /// An empty table, with empty indexes, for every table of `schema`.
    pub(crate) fn empty(schema: &Schema) -> Self {
        let indexes = schema
            .tables()
            .map(|table| (table.table_id(), TableIndexes::new(table)))
            .collect();
        Self {
            tables: empty_tables(schema),
            indexes,
        }
    }

    /// `tables` with the indexes of the constraints of `schema` built from their rows, in
    /// O(N log N) per constraint: outside WebAssembly, callers run it on the blocking pool.
    pub(crate) fn build(schema: &Schema, tables: HashMap<u16, Table>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        crate::fail_point::thread_point("room_data.build");
        let indexes = schema
            .tables()
            .map(|table| {
                let rows = tables.get(&table.table_id());
                let indexes = rows.map_or_else(
                    || TableIndexes::new(table),
                    |rows| TableIndexes::build(table, rows),
                );
                (table.table_id(), indexes)
            })
            .collect();
        Self { tables, indexes }
    }

    pub(crate) fn tables(&self) -> &HashMap<u16, Table> {
        &self.tables
    }

    #[cfg(test)]
    pub(crate) fn indexes(&self) -> &HashMap<u16, TableIndexes> {
        &self.indexes
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

/// Checks that the sequence numbers of a batch continue `head_seq` without gaps, and then that
/// each operation is valid under the room's schema: a gap is reported before any other error.
/// An operation after `u64::MAX` has no sequence number, so it is a mismatch too (reported as
/// expecting `u64::MAX`, the last number).
pub(crate) fn validate_batch(
    room_id: &RoomId,
    schema: &Schema,
    head_seq: SequenceNumber,
    ops: &[SequencedOperation],
) -> Result<(), StorageError> {
    let mut last = head_seq;
    for op in ops {
        let expected = last.get().checked_add(1);
        if expected != Some(op.seq.get()) {
            return Err(StorageError::SequenceMismatch {
                expected: expected.map_or(last, SequenceNumber::from),
                actual: op.seq,
            });
        }
        last = op.seq;
    }
    for op in ops {
        resolve_table_id(room_id, schema, TableRef::Id(op.op.table_id()))?;
        schema.validate_operation(&op.op)?;
    }
    Ok(())
}

/// Applies one operation to `data`, its rows and their indexes, or rejects it and changes
/// nothing. The operation must already be validated against `schema`.
///
/// The decision is taken before anything changes. An update of a row that does not exist is
/// rejected (`MissingRow`); one of a row that does exist widens a row written under an older
/// schema to the table's current width. A delete of a row that does not exist changes nothing.
pub(crate) fn apply_operation(
    data: &mut RoomData,
    schema: &Schema,
    op: Operation,
) -> Option<RejectReason> {
    let table_id = op.table_id();
    let table_schema = schema.get_table_by_id(table_id);
    let (pk, kind) = op.into_pk_and_kind();
    let old = data.tables.get(&table_id).and_then(|table| table.get(&pk));
    let row = match kind {
        OperationKind::Insert { row } => Some(row),
        OperationKind::Update { updates } => {
            let Some(existing) = old else {
                return Some(RejectReason::MissingRow);
            };
            let mut row = existing.clone();
            let target_len = table_schema.map_or(0, |t| t.columns().len());
            for col_up in updates {
                let idx = col_up.column_idx() as usize;
                let min_len = target_len.max(idx + 1);
                if row.len() < min_len {
                    row.resize(min_len, Value::Null);
                }
                row[idx] = col_up.into_value();
            }
            Some(row)
        }
        OperationKind::Delete { .. } => None,
    };

    if let (Some(table_schema), Some(indexes)) = (table_schema, data.indexes.get_mut(&table_id)) {
        indexes.update(table_schema, &pk, old, row.as_ref());
    }
    match row {
        Some(row) => {
            data.tables.entry(table_id).or_default().insert(pk, row);
        }
        None => {
            if let Some(table) = data.tables.get_mut(&table_id) {
                table.remove(&pk);
            }
        }
    }
    None
}

/// In-memory snapshot payload serialized by reference to avoid copying tables during snapshot creation.
#[derive(Debug, Serialize)]
pub(crate) struct RoomSnapshotRef<'a> {
    pub(crate) head_seq: SequenceNumber,
    pub(crate) tables: &'a HashMap<u16, Table>,
}

/// Snapshot payload that owns its tables, produced by decoding (compaction writes the same
/// encoding through [`RoomSnapshotRef`]).
#[derive(Debug, Deserialize)]
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
/// empty. Returns the snapshot's head sequence and its rows, with the indexes of the
/// constraints of `schema` built from them.
///
/// Every row is decoded and checked, and every index built, so outside WebAssembly the engines
/// run it on the blocking pool.
pub(crate) fn decode_received_snapshot(
    envelope: &[u8],
    max_uncompressed_bytes: u64,
    schema: &Schema,
    expected_head_seq: SequenceNumber,
) -> Result<(SequenceNumber, RoomData), StorageError> {
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
    Ok((payload.head_seq, RoomData::build(schema, payload.tables)))
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
