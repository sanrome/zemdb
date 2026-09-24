pub mod state;

pub use state::{RoomSnapshotPayload, RoomState};

use async_trait::async_trait;
use rimdb_core::{
    CompactRow, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use crate::engine::{apply_scan_transforms, RowStream, StorageEngine};
use crate::error::StorageError;
use crate::options::{KeyRange, ScanDirection, ScanOptions};

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
    pub fn get_room(&self, room_id: &RoomId) -> Result<Arc<RwLock<RoomState>>, StorageError> {
        let rooms = self
            .rooms
            .read()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;

        rooms
            .get(room_id)
            .map(Arc::clone)
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))
    }
}

struct MemoryScanState {
    room_arc: Arc<RwLock<RoomState>>,
    table_id: u16,
    range: KeyRange,
    direction: ScanDirection,
    projection: Option<Vec<u16>>,
    remaining_limit: Option<usize>,
    cursor: Option<PrimaryKey>,
    exhausted: bool,
    buffer: VecDeque<Result<(PrimaryKey, CompactRow), StorageError>>,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StorageEngine for MemoryStorageEngine {
    #[tracing::instrument(skip(self, schema), fields(room_id = %room_id))]
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let mut rooms = self
            .rooms
            .write()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;

        if rooms.contains_key(room_id) {
            return Err(StorageError::RoomAlreadyOpen(room_id.clone()));
        }

        rooms.insert(
            room_id.clone(),
            Arc::new(RwLock::new(RoomState::new(schema))),
        );
        tracing::info!(room_id = %room_id, "Opened in-memory room");
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let mut rooms = self
            .rooms
            .write()
            .map_err(|e| StorageError::Other(format!("Engine lock poisoned: {e}")))?;

        if rooms.remove(room_id).is_some() {
            tracing::info!(room_id = %room_id, "Closed in-memory room");
            Ok(())
        } else {
            Err(StorageError::RoomNotFound(room_id.clone()))
        }
    }

    #[tracing::instrument(skip(self, ops), fields(room_id = %room_id, ops_count = ops.len()))]
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

        // 1. Validate schema and strict monotonic sequence
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
        }

        // 2. Apply operations to in-memory tables
        for SequencedOperation { seq, op } in ops {
            let table_map = tables
                .entry(op.table_id)
                .or_default();

            match op.kind {
                OperationKind::Insert { row } => {
                    table_map.insert(op.pk, row);
                }
                OperationKind::Update { updates } => {
                    if let Some(existing) = table_map.get_mut(&op.pk) {
                        for col_up in updates {
                            let idx = col_up.column_idx as usize;
                            if idx < existing.values.len() {
                                existing.values[idx] = col_up.value;
                            }
                        }
                    }
                }
                OperationKind::Delete => {
                    table_map.remove(&op.pk);
                }
            }

            if seq > *head_seq {
                *head_seq = seq;
            }
        }

        tracing::debug!(room_id = %room_id, new_head_seq = %head_seq, "Applied batch to in-memory room");
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

        let table_id = room_state.schema.get_table_id(table).ok_or_else(|| StorageError::TableNotFound {
            room_id: room_id.clone(),
            table: table.to_string(),
        })?;

        let row = room_state
            .tables
            .get(&table_id)
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
        let table_id = {
            let room_state = room_arc
                .read()
                .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

            room_state.schema.get_table_id(table).ok_or_else(|| StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: table.to_string(),
            })?
        };

        let state = MemoryScanState {
            room_arc,
            table_id,
            range: options.range,
            direction: options.direction,
            projection: options.projection,
            remaining_limit: options.limit,
            cursor: None,
            exhausted: false,
            buffer: VecDeque::new(),
        };

        let stream = futures::stream::unfold(state, |mut state| async move {
            if let Some(item) = state.buffer.pop_front() {
                return Some((item, state));
            }

            if state.exhausted || state.remaining_limit == Some(0) {
                return None;
            }

            const BATCH_SIZE: usize = 64;
            let batch_limit = match state.remaining_limit {
                Some(limit) => limit.min(BATCH_SIZE),
                None => BATCH_SIZE,
            };

            let room_arc = Arc::clone(&state.room_arc);
            let room_guard = match room_arc.read() {
                Ok(guard) => guard,
                Err(e) => {
                    state.exhausted = true;
                    return Some((
                        Err(StorageError::Other(format!("Room lock poisoned: {e}"))),
                        state,
                    ));
                }
            };

            let empty = BTreeMap::new();
            let table_data = room_guard.tables.get(&state.table_id).unwrap_or(&empty);

            let batch_items: Vec<Result<(PrimaryKey, CompactRow), StorageError>> =
                match state.direction {
                    ScanDirection::Forward => {
                        let (start_bound, end_bound) = match &state.cursor {
                            Some(cur) => (std::ops::Bound::Excluded(cur), state.range.end_bound()),
                            None => (state.range.start_bound(), state.range.end_bound()),
                        };
                        let iter = table_data.range((start_bound, end_bound));
                        apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                            .collect()
                    }
                    ScanDirection::Backward => {
                        let (start_bound, end_bound) = match &state.cursor {
                            Some(cur) => (state.range.start_bound(), std::ops::Bound::Excluded(cur)),
                            None => (state.range.start_bound(), state.range.end_bound()),
                        };
                        let iter = table_data.range((start_bound, end_bound)).rev();
                        apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                            .collect()
                    }
                };
            drop(room_guard);

            let count = batch_items.len();
            if count == 0 {
                return None;
            }

            if count < batch_limit {
                state.exhausted = true;
            }

            if let Some(ref mut rem) = state.remaining_limit {
                *rem = rem.saturating_sub(count);
                if *rem == 0 {
                    state.exhausted = true;
                }
            }

            if let Some(Ok((ref last_pk, _))) = batch_items.last() {
                state.cursor = Some(last_pk.clone());
            }

            state.buffer.extend(batch_items);
            state.buffer.pop_front().map(|item| (item, state))
        });

        Ok(Box::pin(stream))
    }

    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;
        Ok(room_state.head_seq)
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

        let tables = room_state
            .tables
            .iter()
            .map(|(&id, data)| {
                (
                    id,
                    data.iter().map(|(pk, r)| (pk.clone(), r.clone())).collect(),
                )
            })
            .collect();

        let payload = RoomSnapshotPayload {
            head_seq: room_state.head_seq,
            tables,
        };

        let bytes = bincode::serialize(&payload).map_err(|e| StorageError::Serialization(e.to_string()))?;
        tracing::debug!(room_id = %room_id, snapshot_size = bytes.len(), "Created in-memory snapshot");
        Ok(bytes)
    }

    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError> {
        let payload: RoomSnapshotPayload = bincode::deserialize(snapshot)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

        let mut tables = HashMap::new();
        for (table_id, rows) in payload.tables {
            let mut map = BTreeMap::new();
            for (pk, row) in rows {
                map.insert(pk, row);
            }
            tables.insert(table_id, map);
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

        tracing::info!(room_id = %room_id, head_seq = %payload.head_seq, "Applied in-memory snapshot");
        Ok(payload.head_seq)
    }
}
