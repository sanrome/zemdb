use super::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use zemdb_core::{DataType, TableSchema};

/// The snapshot payload as written while tables were `Arc<BTreeMap>`.
#[derive(Serialize)]
struct BTreeSnapshotPayload {
    head_seq: SequenceNumber,
    tables: HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
}

fn rows() -> Vec<(PrimaryKey, CompactRow)> {
    (0..200i64)
        .rev()
        .map(|key| {
            let row = CompactRow::new(vec![
                Value::Int(key),
                Value::String(format!("{key}").into()),
            ]);
            (PrimaryKey::single(key), row)
        })
        .collect()
}

/// Serialized with a `BTreeMap` table, as before tables became persistent maps.
fn btree_snapshot_bytes() -> Vec<u8> {
    let table: BTreeMap<_, _> = rows().into_iter().collect();
    bincode::serialize(&BTreeSnapshotPayload {
        head_seq: SequenceNumber::from(200u64),
        tables: HashMap::from([(0u16, Arc::new(table))]),
    })
    .unwrap()
}

#[test]
fn snapshot_bytes_match_the_btree_encoding() {
    let tables = HashMap::from([(0u16, rows().into_iter().collect::<Table>())]);
    let bytes = bincode::serialize(&RoomSnapshotRef {
        head_seq: SequenceNumber::from(200u64),
        tables: &tables,
    })
    .unwrap();

    assert_eq!(bytes, btree_snapshot_bytes());
}

#[test]
fn snapshot_written_with_btree_tables_decodes() {
    let payload: RoomSnapshotPayload = bincode::deserialize(&btree_snapshot_bytes()).unwrap();

    assert_eq!(payload.head_seq, SequenceNumber::from(200u64));
    let expected: Table = rows().into_iter().collect();
    assert_eq!(payload.tables[&0], expected);
}

#[test]
fn batch_past_the_last_sequence_number_is_a_mismatch() {
    let room_id = RoomId::new("room").unwrap();
    let schema = Schema::builder()
        .table(TableSchema::builder("t").primary_key("id", DataType::Int))
        .build();
    let op = |seq: u64| {
        let row = CompactRow::new(vec![Value::Int(1)]);
        SequencedOperation::new(seq, Operation::insert(0, PrimaryKey::single(1i64), row, 0))
    };
    for (head, seqs) in [
        (u64::MAX, vec![u64::MAX]),
        (u64::MAX - 1, vec![u64::MAX, u64::MAX]),
    ] {
        let ops: Vec<_> = seqs.into_iter().map(op).collect();
        let result = validate_batch(&room_id, &schema, SequenceNumber::from(head), &ops);
        assert!(
            matches!(result, Err(StorageError::SequenceMismatch { .. })),
            "{result:?}"
        );
    }
}
