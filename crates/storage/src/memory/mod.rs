pub mod state;

pub use state::{RoomSnapshotPayload, RoomSnapshotRef, RoomState, Table};

use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};
use zemdb_core::{CompactRow, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation};

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
    table_data: Table,
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
        state::validate_batch(room_id, schema, *head_seq, &ops)?;

        // 2. Apply operations to in-memory tables
        for SequencedOperation { seq, op } in ops {
            state::apply_operation(tables, schema, op);
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
        let table_id = {
            let room_state = room_arc
                .read()
                .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

            room_state
                .schema
                .get_table_id(table)
                .ok_or_else(|| StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: table.to_string(),
                })?
        };

        self.get_by_id(room_id, table_id, pk).await
    }

    async fn get_by_id(
        &self,
        room_id: &RoomId,
        table_id: u16,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc
            .read()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

        if !room_state.schema.has_table_by_id(table_id) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: format!("id:{}", table_id),
            });
        }

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

            room_state
                .schema
                .get_table_id(table)
                .ok_or_else(|| StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: table.to_string(),
                })?
        };

        self.scan_by_id(room_id, table_id, options).await
    }

    async fn scan_by_id<'a>(
        &'a self,
        room_id: &RoomId,
        table_id: u16,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let table_data = {
            let room_state = room_arc
                .read()
                .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;

            if !room_state.schema.has_table_by_id(table_id) {
                return Err(StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: format!("id:{}", table_id),
                });
            }

            // An O(1) clone that shares the table's nodes; writes during the scan copy only the
            // nodes they change.
            room_state
                .tables
                .get(&table_id)
                .cloned()
                .unwrap_or_default()
        };

        let state = MemoryScanState {
            table_data,
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

            let batch_items: Vec<Result<(PrimaryKey, CompactRow), StorageError>> = match state
                .direction
            {
                ScanDirection::Forward => {
                    let (start_bound, end_bound) = match &state.cursor {
                        Some(cur) => (std::ops::Bound::Excluded(cur), state.range.end_bound()),
                        None => (state.range.start_bound(), state.range.end_bound()),
                    };
                    let iter = state
                        .table_data
                        .range::<_, PrimaryKey>((start_bound, end_bound));
                    apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                        .collect()
                }
                ScanDirection::Backward => {
                    let (start_bound, end_bound) = match &state.cursor {
                        Some(cur) => (state.range.start_bound(), std::ops::Bound::Excluded(cur)),
                        None => (state.range.start_bound(), state.range.end_bound()),
                    };
                    let iter = state
                        .table_data
                        .range::<_, PrimaryKey>((start_bound, end_bound))
                        .rev();
                    apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                        .collect()
                }
            };

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

        let payload = RoomSnapshotRef {
            head_seq: room_state.head_seq,
            tables: &room_state.tables,
        };

        let raw_bytes =
            bincode::serialize(&payload).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let envelope = crate::snapshot::encode_snapshot_envelope(&raw_bytes, false, 0)?;
        tracing::debug!(room_id = %room_id, snapshot_size = envelope.len(), "Created in-memory snapshot");
        Ok(envelope)
    }

    /// Replaces the room's contents with `snapshot` (see the trait for the rules on its head
    /// sequence). The contents are replaced in place under the room's lock: a writer that
    /// already holds the room's handle writes into the new state, never into a detached copy.
    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError> {
        let room_arc = self.get_room(room_id)?;

        let decompressed = crate::snapshot::decode_snapshot_envelope(snapshot)?;
        let mut payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;
        payload.fill_missing_tables(&schema);

        let mut room = room_arc
            .write()
            .map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))?;
        if payload.head_seq < room.head_seq {
            return Err(StorageError::SnapshotBehind {
                current: room.head_seq,
                snapshot: payload.head_seq,
            });
        }
        if payload.head_seq == room.head_seq {
            return Ok(room.head_seq);
        }

        room.schema = schema;
        room.head_seq = payload.head_seq;
        room.tables = payload.tables;

        tracing::info!(room_id = %room_id, head_seq = %payload.head_seq, "Applied in-memory snapshot");
        Ok(payload.head_seq)
    }
}

#[cfg(test)]
#[path = "../tests/memory.rs"]
mod tests;
