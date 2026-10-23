use super::*;
use crate::id::{CorrelationId, RoomId, SchemaId, SequenceNumber};
use crate::mutation::{ColumnUpdate, Operation};
use crate::protocol::messages::{SequencedOperation, ServerMessage};
use crate::schema::MAX_COLUMNS;
use crate::value::{CompactRow, PrimaryKey, Value};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

/// Inserts whose rows hold `nulls` values each (plus one primary key value).
fn null_inserts(count: usize, nulls: usize) -> Vec<Operation> {
    (0..count)
        .map(|i| {
            Operation::insert(
                0,
                PrimaryKey::single(i as i64),
                CompactRow::new(vec![Value::Null; nulls]),
                0,
            )
        })
        .collect()
}

/// Rows of `MAX_COLUMNS` values that together carry more than `MAX_MESSAGE_VALUES`.
fn rows_over_budget() -> usize {
    (MAX_MESSAGE_VALUES as usize).div_ceil(MAX_COLUMNS) + 1
}

/// `value` decodes with a budget of exactly `values`, and one less is rejected.
fn assert_budget_is_exact<T: Serialize + DeserializeOwned + PartialEq + Debug>(
    value: &T,
    values: u64,
) {
    let frame = encode_message(value).unwrap();
    assert_eq!(
        &decode_with_value_budget::<T>(&frame, values).unwrap(),
        value
    );
    let result = decode_with_value_budget::<T>(&frame, values - 1);
    assert!(
        matches!(result, Err(DecodeError::TooManyValues { max }) if max == values - 1),
        "{result:?}"
    );
}

#[test]
fn a_message_over_the_value_budget_is_rejected() {
    // 3 operations of 1 key value and 4 row values each: 15 values.
    assert_budget_is_exact(&null_inserts(3, 4), 15);
}

#[test]
fn key_values_and_update_deltas_count_against_the_budget() {
    // 3 key values and 4 deltas: 7 values.
    let op = Operation::update(
        0,
        PrimaryKey::composite([1i64, 2, 3]),
        (0..4).map(|i| ColumnUpdate::new(i, Value::Null)).collect(),
        0,
    );
    assert_budget_is_exact(&op, 7);
}

#[test]
fn the_owner_of_a_delete_counts_against_the_budget() {
    // 2 key values and the owner: 3 values.
    let op = Operation::delete_with_owner(0, PrimaryKey::composite([1i64, 2]), "alice", 0);
    assert_budget_is_exact(&op, 3);
}

#[test]
fn rows_within_the_column_limit_still_exceed_the_message_budget() {
    // Each row is within the column limit; together they carry more than the budget, in a
    // frame far below the size limit (1 byte per Null on the wire).
    let rows = rows_over_budget();
    let ops = null_inserts(rows, MAX_COLUMNS);
    let frame = encode_message(&ops).unwrap();
    assert!(frame.len() < MAX_FRAME_SIZE / 8);
    let result = decode_with_value_budget::<Vec<Operation>>(&frame, MAX_MESSAGE_VALUES);
    assert!(
        matches!(result, Err(DecodeError::TooManyValues { max }) if max == MAX_MESSAGE_VALUES),
        "{result:?}"
    );
    // Without a budget (as clients decode server responses) the same values decode.
    assert_eq!(
        decode_message::<Vec<Operation>>(&frame).unwrap().len(),
        rows
    );
}

#[test]
fn server_responses_are_not_budgeted() {
    let rows = rows_over_budget();
    let batch = ServerMessage::SyncBatch {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room").unwrap(),
        head_seq: SequenceNumber::new(rows as u64),
        ops: null_inserts(rows, MAX_COLUMNS)
            .into_iter()
            .enumerate()
            .map(|(i, op)| SequencedOperation::new(i as u64 + 1, op))
            .collect(),
        has_more: false,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("schema").unwrap(),
    };
    let frame = encode_message(&batch).unwrap();
    assert_eq!(decode_message::<ServerMessage>(&frame).unwrap(), batch);
}

#[test]
fn a_failed_decode_does_not_leave_its_budget_on_the_thread() {
    let frame = encode_message(&null_inserts(1, 10)).unwrap();
    assert!(decode_with_value_budget::<Vec<Operation>>(&frame, 1).is_err());
    // The thread decodes without a budget again.
    assert_eq!(
        decode_message::<Vec<Operation>>(&frame).unwrap(),
        null_inserts(1, 10)
    );
    // And a later budgeted decode starts from its own, full budget.
    assert!(decode_with_value_budget::<Vec<Operation>>(&frame, 11).is_ok());
}
