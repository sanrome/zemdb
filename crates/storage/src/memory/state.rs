use rimdb_core::{CompactRow, PrimaryKey, Schema, SequenceNumber};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// In-memory state of an open database room.
///
/// Holds the room schema, monotonic head sequence number, and in-memory tables indexed by table_id.
#[derive(Debug)]
pub struct RoomState {
    pub schema: Schema,
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, BTreeMap<PrimaryKey, CompactRow>>,
}

impl RoomState {
    /// Creates a new `RoomState` initialized from a schema with empty tables.
    pub fn new(schema: Schema) -> Self {
        let mut tables = HashMap::new();
        for table_id in schema.tables_by_id.keys() {
            tables.insert(*table_id, BTreeMap::new());
        }
        Self {
            schema,
            head_seq: SequenceNumber::from(0u64),
            tables,
        }
    }
}

/// Payload format for serialized in-memory snapshots.
#[derive(Debug, Serialize, Deserialize)]
pub struct RoomSnapshotPayload {
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, Vec<(PrimaryKey, CompactRow)>>,
}
