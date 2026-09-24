use async_trait::async_trait;
use futures::Stream;
use rimdb_core::{CompactRow, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation, Value};
use std::pin::Pin;

use crate::error::StorageError;
use crate::options::ScanOptions;

#[cfg(not(target_arch = "wasm32"))]
pub trait EngineConcurrencyBounds: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> EngineConcurrencyBounds for T {}

#[cfg(target_arch = "wasm32")]
pub trait EngineConcurrencyBounds {}
#[cfg(target_arch = "wasm32")]
impl<T> EngineConcurrencyBounds for T {}

#[cfg(not(target_arch = "wasm32"))]
pub type RowStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(PrimaryKey, CompactRow), StorageError>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub type RowStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(PrimaryKey, CompactRow), StorageError>> + 'a>>;

/// Storage engine contract for RimDB local persistence.
///
/// Implementations must be thread-safe (or single-thread compatible on wasm32)
/// and handle room lifecycle, sequenced operation batches, point lookups,
/// range scans with pushdowns, and full state snapshots.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait StorageEngine: EngineConcurrencyBounds {
    /// Opens or registers a room with its schema.
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError>;

    /// Closes a room, releasing any associated resources or file handles.
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError>;

    /// Applies a batch of sequenced operations to the storage engine (move semantics).
    /// Returns the new head sequence number.
    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<SequenceNumber, StorageError>;

    /// Fetches a single row by primary key from a table.
    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError>;

    /// Scans a table using the specified options (key range, direction, limit, projection).
    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError>;

    /// Gets the current head sequence number for a room.
    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError>;

    /// Creates a complete snapshot of the room state as a byte buffer.
    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError>;

    /// Restores room state from a snapshot byte buffer and sets its head sequence number.
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError>;
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ScanIterator<'a> =
    Box<dyn Iterator<Item = Result<(PrimaryKey, CompactRow), StorageError>> + Send + 'a>;

#[cfg(target_arch = "wasm32")]
pub(crate) type ScanIterator<'a> =
    Box<dyn Iterator<Item = Result<(PrimaryKey, CompactRow), StorageError>> + 'a>;

/// Helper to apply column projection and limit pushdowns lazily over an iterator of rows without eager allocation.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn apply_scan_transforms<'a, I>(
    iter: I,
    projection: Option<Vec<u16>>,
    limit: Option<usize>,
) -> ScanIterator<'a>
where
    I: Iterator<Item = (&'a PrimaryKey, &'a CompactRow)> + Send + 'a,
{
    let mapped = iter.map(move |(pk, row)| {
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
        Some(limit) => Box::new(mapped.take(limit)),
        None => Box::new(mapped),
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn apply_scan_transforms<'a, I>(
    iter: I,
    projection: Option<Vec<u16>>,
    limit: Option<usize>,
) -> ScanIterator<'a>
where
    I: Iterator<Item = (&'a PrimaryKey, &'a CompactRow)> + 'a,
{
    let mapped = iter.map(move |(pk, row)| {
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
        Some(limit) => Box::new(mapped.take(limit)),
        None => Box::new(mapped),
    }
}
