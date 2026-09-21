use async_trait::async_trait;
use rimdb_core::{
    CompactRow, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
    Value,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use crate::engine::{RowStream, StorageEngine};
use crate::error::StorageError;
use crate::options::{ScanDirection, ScanOptions};

#[derive(Debug)]
struct RoomState {
    schema: Schema,
    head_seq: SequenceNumber,
    tables: HashMap<String, BTreeMap<PrimaryKey, CompactRow>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomSnapshotPayload {
    head_seq: SequenceNumber,
    tables: HashMap<String, Vec<(PrimaryKey, CompactRow)>>,
}

/// In-memory implementation of `StorageEngine` with per-room isolation.
///
/// Each room is protected by its own `RwLock`, allowing concurrent access
/// across different rooms without global lock contention.
/// Inside each room, multiple concurrent readers can execute queries (`get`, `scan`)
/// while writes (`apply_batch`) take an exclusive lock on that specific room only.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorageEngine {
    rooms: Arc<RwLock<HashMap<RoomId, Arc<RwLock<RoomState>>>>>,
}

impl MemoryStorageEngine {
    /// Creates a new, empty `MemoryStorageEngine`.
    pub fn new() -> Self {
        Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Fast lookup of a room's isolated state handle without holding the engine map lock.
    fn get_room(&self, room_id: &RoomId) -> Result<Arc<RwLock<RoomState>>, StorageError> {
        let rooms = self
            .rooms
            .read()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;

        rooms
            .get(room_id)
            .cloned()
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))
    }
}

fn apply_scan_transforms<'a, I>(
    iter: I,
    projection: Option<Vec<u16>>,
    limit: Option<usize>,
) -> Vec<Result<(PrimaryKey, CompactRow), StorageError>>
where
    I: Iterator<Item = (&'a PrimaryKey, &'a CompactRow)>,
{
    let mapped = iter.map(|(pk, row)| {
        let row_to_return = match &projection {
            Some(indices) => {
                let values = indices
                    .iter()
                    .map(|&idx| {
                        row.values
                            .get(idx as usize)
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                CompactRow::new(values)
            }
            None => row.clone(),
        };
        Ok((pk.clone(), row_to_return))
    });

    match limit {
        Some(limit) => mapped.take(limit).collect(),
        None => mapped.collect(),
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StorageEngine for MemoryStorageEngine {
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let mut rooms = self
            .rooms
            .write()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;

        if rooms.contains_key(room_id) {
            return Err(StorageError::RoomAlreadyOpen(room_id.clone()));
        }

        let mut tables = HashMap::new();
        for table_name in schema.tables.keys() {
            tables.insert(table_name.clone(), BTreeMap::new());
        }

        rooms.insert(
            room_id.clone(),
            Arc::new(RwLock::new(RoomState {
                schema,
                head_seq: SequenceNumber::from(0u64),
                tables,
            })),
        );
        Ok(())
    }

    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let mut rooms = self
            .rooms
            .write()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;

        if rooms.remove(room_id).is_some() {
            Ok(())
        } else {
            Err(StorageError::RoomNotFound(room_id.clone()))
        }
    }

    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<SequenceNumber, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let mut room_guard = room_arc
            .write()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

        let RoomState {
            ref schema,
            ref mut head_seq,
            ref mut tables,
        } = *room_guard;

        for SequencedOperation { seq, op } in ops {
            let table_name = op.table.clone();
            if !schema.has_table(&table_name) {
                return Err(StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: table_name.to_string(),
                });
            }

            let table_map = tables
                .entry(table_name.to_string())
                .or_default();

            match op.op.kind {
                OperationKind::Insert { row } => {
                    table_map.insert(op.op.pk, row);
                }
                OperationKind::Update { updates } => {
                    if let Some(existing) = table_map.get_mut(&op.op.pk) {
                        for col_up in updates {
                            let idx = col_up.column_idx as usize;
                            if idx < existing.values.len() {
                                existing.values[idx] = col_up.value;
                            }
                        }
                    } else if let Some(table_def) = schema.get_table(&table_name) {
                        let mut values = vec![Value::Null; table_def.columns.len()];
                        for (pk_idx, col_name) in table_def.primary_key.iter().enumerate() {
                            if let Some(col_pos) = table_def.column_index(col_name) {
                                if pk_idx < op.op.pk.len() {
                                    values[col_pos] = op.op.pk[pk_idx].clone();
                                }
                            }
                        }
                        for col_up in updates {
                            let idx = col_up.column_idx as usize;
                            if idx < values.len() {
                                values[idx] = col_up.value;
                            }
                        }
                        table_map.insert(op.op.pk, CompactRow::new(values));
                    }
                }
                OperationKind::Delete => {
                    table_map.remove(&op.op.pk);
                }
            }

            if seq > *head_seq {
                *head_seq = seq;
            }
        }

        Ok(*head_seq)
    }

    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

        if !room_state.schema.has_table(table) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: table.to_string(),
            });
        }

        let row = room_state
            .tables
            .get(table)
            .and_then(|t| t.get(pk).cloned());
        Ok(row)
    }

    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

        if !room_state.schema.has_table(table) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: table.to_string(),
            });
        }

        let empty = BTreeMap::new();
        let table_data = room_state.tables.get(table).unwrap_or(&empty);

        let items = match options.direction {
            ScanDirection::Forward => {
                let iter = table_data.range(options.range);
                apply_scan_transforms(iter, options.projection, options.limit)
            }
            ScanDirection::Backward => {
                let iter = table_data.range(options.range).rev();
                apply_scan_transforms(iter, options.projection, options.limit)
            }
        };

        Ok(Box::pin(futures::stream::iter(items)))
    }

    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;
        Ok(room_state.head_seq)
    }

    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

        let tables = room_state
            .tables
            .iter()
            .map(|(name, data)| {
                (
                    name.clone(),
                    data.iter().map(|(pk, r)| (pk.clone(), r.clone())).collect(),
                )
            })
            .collect();

        let payload = RoomSnapshotPayload {
            head_seq: room_state.head_seq,
            tables,
        };

        bincode::serialize(&payload).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError> {
        let payload: RoomSnapshotPayload = bincode::deserialize(snapshot)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

        let mut tables = HashMap::new();
        for (table_name, rows) in payload.tables {
            let mut map = BTreeMap::new();
            for (pk, row) in rows {
                map.insert(pk, row);
            }
            tables.insert(table_name, map);
        }

        let new_state = Arc::new(RwLock::new(RoomState {
            schema,
            head_seq: payload.head_seq,
            tables,
        }));

        let mut rooms = self
            .rooms
            .write()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;
        rooms.insert(room_id.clone(), new_state);

        Ok(payload.head_seq)
    }
}
