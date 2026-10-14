use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::pin::Pin;
use zemdb_core::{
    CompactRow, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation, Value,
};

use crate::error::StorageError;
use crate::memory::Table;
use crate::options::{KeyRange, ScanDirection, ScanOptions};

/// `Send + Sync`, except on wasm32, where engines are single-threaded.
#[cfg(not(target_arch = "wasm32"))]
pub trait EngineConcurrencyBounds: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> EngineConcurrencyBounds for T {}

/// `Send + Sync`, except on wasm32, where engines are single-threaded.
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

/// Storage engine contract for ZemDB local persistence.
///
/// Implementations must be thread-safe (or single-thread compatible on wasm32)
/// and handle room lifecycle, sequenced operation batches, point lookups,
/// range scans with pushdowns, and full state snapshots.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait StorageEngine: EngineConcurrencyBounds {
    /// Opens or registers a room with its schema. A disk room whose snapshot or WAL holds data
    /// of a table that `schema` does not have is not opened (`SchemaMismatch`), and that data
    /// stays on disk for a later open with a schema that has the table.
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError>;

    /// Closes a room, releasing any associated resources or file handles.
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError>;

    /// Applies a batch of sequenced operations to the storage engine.
    /// Returns the new head sequence number.
    ///
    /// The whole batch must continue the room's head without gaps; otherwise it is
    /// `SequenceMismatch`, whatever else is wrong with it, since a gap means the batch does not
    /// belong on the room's current state. Only then is each operation checked against the
    /// schema. A batch that fails either check changes nothing.
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

    /// Fetches a single row by primary key from a table identified by its numeric table_id.
    async fn get_by_id(
        &self,
        room_id: &RoomId,
        table_id: u16,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError>;

    /// Scans a table identified by its name using the specified options (key range, direction,
    /// limit, projection).
    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError>;

    /// Scans a table identified by its numeric table_id using the specified options.
    async fn scan_by_id<'a>(
        &'a self,
        room_id: &RoomId,
        table_id: u16,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError>;

    /// Gets the current head sequence number for a room.
    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError>;

    /// Creates a complete snapshot of the room state as a byte buffer.
    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError>;

    /// Replaces the state of an open room with a snapshot and returns the room's head sequence.
    ///
    /// The snapshot is checked first, and an invalid one leaves the room untouched:
    /// - Its head sequence must be `expected_head_seq`, the one announced for it (by the
    ///   server's snapshot relay); otherwise `StorageError::SnapshotHeadMismatch`.
    /// - Every table must be in `schema` and every row valid under it, with its key columns
    ///   equal to the key it is stored under; otherwise `StorageError::SnapshotCorruption`.
    ///   Whether `schema` is the room's registered schema is up to the caller.
    ///
    /// Outside WebAssembly the snapshot is decoded and checked on the blocking pool. Then:
    /// - A snapshot behind the room's head sequence is rejected with
    ///   `StorageError::SnapshotBehind`: a room never moves back.
    /// - A snapshot at the room's head sequence changes nothing and succeeds, so a retried
    ///   apply is idempotent (without offline writes, the room at a sequence number already
    ///   equals any snapshot taken at it). The `schema` argument is ignored too: the room keeps
    ///   its schema.
    /// - A snapshot ahead of the room replaces its schema, head sequence and every table.
    ///
    /// The room must be open; otherwise `StorageError::RoomNotFound` is returned.
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: Bytes,
        expected_head_seq: SequenceNumber,
    ) -> Result<SequenceNumber, StorageError>;
}

#[cfg(not(target_arch = "wasm32"))]
type ScanIterator<'a> =
    Box<dyn Iterator<Item = Result<(PrimaryKey, CompactRow), StorageError>> + Send + 'a>;

#[cfg(target_arch = "wasm32")]
type ScanIterator<'a> =
    Box<dyn Iterator<Item = Result<(PrimaryKey, CompactRow), StorageError>> + 'a>;

/// `Send`, except on wasm32, where engines are single-threaded.
#[cfg(not(target_arch = "wasm32"))]
trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

/// `Send`, except on wasm32, where engines are single-threaded.
#[cfg(target_arch = "wasm32")]
trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// Rows a scan takes from its table at a time.
const SCAN_BATCH_SIZE: usize = 64;

/// Streams the rows of `table_data` selected by `options`, taking them from the table in
/// batches of [`SCAN_BATCH_SIZE`].
///
/// `table_data` is the scan's own clone of the table, which shares its nodes with the room's,
/// so the stream holds no lock of the room.
pub(crate) fn batched_scan(table_data: Table, options: ScanOptions) -> RowStream<'static> {
    let state = ScanState {
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

        let batch_limit = state
            .remaining_limit
            .map_or(SCAN_BATCH_SIZE, |limit| limit.min(SCAN_BATCH_SIZE));
        // The cursor is the last key of the previous batch, which this one starts after.
        let (start, end) = match (&state.cursor, state.direction) {
            (None, _) => (state.range.start_bound(), state.range.end_bound()),
            (Some(cur), ScanDirection::Forward) => (Bound::Excluded(cur), state.range.end_bound()),
            (Some(cur), ScanDirection::Backward) => {
                (state.range.start_bound(), Bound::Excluded(cur))
            }
        };
        let rows = state.table_data.range::<_, PrimaryKey>((start, end));
        let projection = state.projection.clone();
        let batch_items: Vec<_> = match state.direction {
            ScanDirection::Forward => {
                apply_scan_transforms(rows, projection, batch_limit).collect()
            }
            ScanDirection::Backward => {
                apply_scan_transforms(rows.rev(), projection, batch_limit).collect()
            }
        };

        let count = batch_items.len();
        state.exhausted = count < batch_limit;
        if let Some(rem) = state.remaining_limit.as_mut() {
            *rem = rem.saturating_sub(count);
        }
        if let Some(Ok((last_pk, _))) = batch_items.last() {
            state.cursor = Some(last_pk.clone());
        }

        state.buffer.extend(batch_items);
        state.buffer.pop_front().map(|item| (item, state))
    });

    Box::pin(stream)
}

struct ScanState {
    table_data: Table,
    range: KeyRange,
    direction: ScanDirection,
    projection: Option<Vec<u16>>,
    remaining_limit: Option<usize>,
    cursor: Option<PrimaryKey>,
    exhausted: bool,
    buffer: VecDeque<Result<(PrimaryKey, CompactRow), StorageError>>,
}

/// Applies the column projection and the limit lazily, without collecting the rows.
fn apply_scan_transforms<'a, I>(
    iter: I,
    projection: Option<Vec<u16>>,
    limit: usize,
) -> ScanIterator<'a>
where
    I: Iterator<Item = (&'a PrimaryKey, &'a CompactRow)> + MaybeSend + 'a,
{
    Box::new(iter.take(limit).map(move |(pk, row)| {
        let row_to_return = match &projection {
            Some(indices) => {
                let values = indices
                    .iter()
                    .map(|&idx| row.get(idx as usize).cloned().unwrap_or(Value::Null))
                    .collect();
                CompactRow::new(values)
            }
            None => row.clone(),
        };
        Ok((pk.clone(), row_to_return))
    }))
}
