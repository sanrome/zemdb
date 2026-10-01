use zemdb_core::{CompactRow, PrimaryKey, Schema, SequenceNumber};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// In-memory state of an open database room.
///
/// Holds the room schema, monotonic head sequence number, and in-memory tables indexed by table_id.
/// Tables are stored inside `Arc` pointers to enable copy-on-write snapshot isolation without locking.
#[derive(Debug)]
pub struct RoomState {
    pub schema: Schema,
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
}

impl RoomState {
    /// Creates a new `RoomState` initialized from a schema with empty tables.
    pub fn new(schema: Schema) -> Self {
        let mut tables = HashMap::new();
        for table_id in schema.tables_by_id.keys() {
            tables.insert(*table_id, Arc::new(BTreeMap::new()));
        }
        Self {
            schema,
            head_seq: SequenceNumber::from(0u64),
            tables,
        }
    }
}

/// In-memory snapshot payload serialized by reference to avoid copying tables during snapshot creation.
#[derive(Debug, Serialize)]
pub struct RoomSnapshotRef<'a> {
    pub head_seq: SequenceNumber,
    pub tables: &'a HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
}

/// Payload format for deserialized database snapshots.
#[derive(Debug, Serialize, Deserialize)]
pub struct RoomSnapshotPayload {
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
}
