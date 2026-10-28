pub(crate) mod index;
pub(crate) mod state;

pub use state::Table;
pub(crate) use state::{RoomSnapshotRef, RoomState};

use state::{decode_received_snapshot, resolve_table_id, TableRef};

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use zemdb_core::{CompactRow, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation};

use crate::engine::{batched_scan, ApplyReport, RowStream, StorageEngine};
use crate::error::StorageError;
use crate::options::ScanOptions;
use crate::snapshot::DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES;
use crate::sys::run_blocking;

/// In-memory implementation of `StorageEngine` with per-room isolation.
///
/// One `RwLock` per room: reads within a room run concurrently, and a write locks only its own
/// room.
///
/// Outside WebAssembly, `create_snapshot` and `apply_snapshot` serialize and decode on Tokio's
/// blocking pool, so they must run inside a Tokio runtime.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorageEngine {
    rooms: Arc<RwLock<HashMap<RoomId, Arc<RwLock<RoomState>>>>>,
}

impl MemoryStorageEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fast lookup of a room's isolated state handle without holding the engine map lock.
    /// Internal: the state changes only through the validating `StorageEngine` methods.
    pub(crate) fn get_room(
        &self,
        room_id: &RoomId,
    ) -> Result<Arc<RwLock<RoomState>>, StorageError> {
        let rooms = self.rooms.read().map_err(poisoned("Engine"))?;

        rooms
            .get(room_id)
            .map(Arc::clone)
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))
    }

    /// Point lookup behind `get` and `get_by_id`: resolves the table and reads the row under
    /// a single read lock of the room.
    fn get_row(
        &self,
        room_id: &RoomId,
        table: TableRef<'_>,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc.read().map_err(poisoned("Room"))?;
        let table_id = resolve_table_id(room_id, &room_state.schema, table)?;
        Ok(room_state
            .data
            .tables()
            .get(&table_id)
            .and_then(|t| t.get(pk).cloned()))
    }

    /// Scan behind `scan` and `scan_by_id`: resolves the table and takes its rows under a
    /// single read lock of the room.
    fn scan_table<'a>(
        &'a self,
        room_id: &RoomId,
        table: TableRef<'_>,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let table_data = {
            let room_state = room_arc.read().map_err(poisoned("Room"))?;
            let table_id = resolve_table_id(room_id, &room_state.schema, table)?;

            // An O(1) clone that shares the table's nodes; writes during the scan copy only the
            // nodes they change.
            room_state
                .data
                .tables()
                .get(&table_id)
                .cloned()
                .unwrap_or_default()
        };

        Ok(batched_scan(table_data, options))
    }
}

/// Maps the poisoning of the engine's or a room's lock (`lock` is "Engine" or "Room") to an error.
fn poisoned<G>(lock: &'static str) -> impl FnOnce(PoisonError<G>) -> StorageError {
    move |e| StorageError::Other(format!("{lock} lock poisoned: {e}"))
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StorageEngine for MemoryStorageEngine {
    #[tracing::instrument(skip(self, schema), fields(room_id = %room_id))]
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let mut rooms = self.rooms.write().map_err(poisoned("Engine"))?;

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
        let mut rooms = self.rooms.write().map_err(poisoned("Engine"))?;

        rooms
            .remove(room_id)
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))?;
        tracing::info!(room_id = %room_id, "Closed in-memory room");
        Ok(())
    }

    /// A closed room holds nothing in memory: there is nothing to remove.
    async fn delete_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let rooms = self.rooms.read().map_err(poisoned("Engine"))?;
        if rooms.contains_key(room_id) {
            return Err(StorageError::RoomAlreadyOpen(room_id.clone()));
        }
        Ok(())
    }

    /// Changes the room's schema (see the trait) under the room's write lock, in O(tables and
    /// constraints): the rows of a room in memory are never wider than its schema (every batch
    /// and every snapshot is validated against it), so they hold `NULL` in every new column, and
    /// the indexes of the new constraints are empty.
    #[tracing::instrument(skip(self, schema), fields(room_id = %room_id))]
    async fn reload_schema(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let room_arc = self.get_room(room_id)?;
        let mut room = room_arc.write().map_err(poisoned("Room"))?;
        if room.schema == schema {
            return Ok(());
        }
        if !schema.evolves_from(&room.schema) {
            return Err(StorageError::SchemaNotEvolving {
                room_id: room_id.clone(),
            });
        }
        room.data = room.data.evolved(&schema, false);
        room.schema = schema;
        tracing::info!(room_id = %room_id, "Reloaded the schema of an in-memory room");
        Ok(())
    }

    #[tracing::instrument(skip(self, ops), fields(room_id = %room_id, ops_count = ops.len()))]
    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<ApplyReport, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let mut room_guard = room_arc.write().map_err(poisoned("Room"))?;

        let RoomState {
            ref schema,
            ref mut head_seq,
            ref mut data,
        } = *room_guard;

        state::validate_batch(room_id, schema, *head_seq, &ops)?;

        let mut rejected = Vec::new();
        for SequencedOperation { seq, op } in ops {
            if let Some(reason) = state::apply_operation(data, schema, op) {
                rejected.push((seq, reason));
            }
            if seq > *head_seq {
                *head_seq = seq;
            }
        }

        tracing::debug!(room_id = %room_id, new_head_seq = %head_seq, "Applied batch to in-memory room");
        Ok(ApplyReport::new(*head_seq, rejected))
    }

    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        self.get_row(room_id, TableRef::Name(table), pk)
    }

    async fn get_by_id(
        &self,
        room_id: &RoomId,
        table_id: u16,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        self.get_row(room_id, TableRef::Id(table_id), pk)
    }

    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        self.scan_table(room_id, TableRef::Name(table), options)
    }

    async fn scan_by_id<'a>(
        &'a self,
        room_id: &RoomId,
        table_id: u16,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        self.scan_table(room_id, TableRef::Id(table_id), options)
    }

    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError> {
        let room_arc = self.get_room(room_id)?;
        let room_state = room_arc.read().map_err(poisoned("Room"))?;
        Ok(room_state.head_seq)
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError> {
        let room_arc = self.get_room(room_id)?;
        // O(1) clones of the tables: serialization runs without the lock.
        let (head_seq, tables) = {
            let room_state = room_arc.read().map_err(poisoned("Room"))?;
            (room_state.head_seq, room_state.data.tables().clone())
        };

        let envelope = run_blocking(move || {
            let raw_bytes = bincode::serialize(&RoomSnapshotRef {
                head_seq,
                tables: &tables,
            })
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
            crate::snapshot::encode_snapshot_envelope(&raw_bytes, false, 0)
        })
        .await?;

        tracing::debug!(room_id = %room_id, snapshot_size = envelope.len(), "Created in-memory snapshot");
        Ok(envelope)
    }

    /// Replaces the room's contents with `snapshot` (see the trait for its checks and the rules
    /// on its head sequence). The contents are replaced in place under the room's lock: a
    /// writer that already holds the room's handle writes into the new state, never into a
    /// detached copy.
    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: Bytes,
        expected_head_seq: SequenceNumber,
    ) -> Result<SequenceNumber, StorageError> {
        // Checked before decoding, so that an unopened room is reported without decoding.
        self.get_room(room_id)?;

        #[cfg(not(target_arch = "wasm32"))]
        let fail_key = std::path::PathBuf::from(room_id.as_str());
        let (schema, (snapshot_head, data)) = run_blocking(move || {
            #[cfg(not(target_arch = "wasm32"))]
            crate::fail_point::pause_blocking("apply_snapshot.decode", &fail_key);
            let received = decode_received_snapshot(
                &snapshot,
                DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
                &schema,
                expected_head_seq,
            )?;
            Ok::<_, StorageError>((schema, received))
        })
        .await?;

        // Looked up again: the room may have been closed, or closed and reopened, while the
        // snapshot was decoded. The snapshot goes to the room open now, as in the disk engine.
        let room_arc = self.get_room(room_id)?;
        let mut room = room_arc.write().map_err(poisoned("Room"))?;
        if snapshot_head < room.head_seq {
            return Err(StorageError::SnapshotBehind {
                current: room.head_seq,
                snapshot: snapshot_head,
            });
        }
        if snapshot_head == room.head_seq {
            return Ok(room.head_seq);
        }

        room.schema = schema;
        room.head_seq = snapshot_head;
        room.data = data;

        tracing::info!(room_id = %room_id, head_seq = %snapshot_head, "Applied in-memory snapshot");
        Ok(snapshot_head)
    }
}

#[cfg(test)]
#[path = "../tests/memory.rs"]
mod tests;
