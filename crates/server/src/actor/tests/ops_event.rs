use super::*;
use crate::actor::test_support::test_schema;
use base64::Engine;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::codec::{decode_message, encode_message};
use zemdb_core::protocol::messages::ServerMessage;
use zemdb_core::value::RowBuilder;

fn room() -> RoomId {
    RoomId::new("room").unwrap()
}

fn schema_id() -> SchemaId {
    SchemaId::new("todo").unwrap()
}

const INCARNATION: u128 = u128::MAX - 11;

fn op_with_title(seq: u64, title_len: usize) -> SequencedOperation {
    let row = RowBuilder::new()
        .set("id", seq as i64)
        .set("title", "t".repeat(title_len))
        .build();
    let op: Operation = test_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap();
    SequencedOperation::new(seq, op)
}

fn ops(seqs: std::ops::RangeInclusive<u64>) -> Vec<SequencedOperation> {
    seqs.map(|seq| op_with_title(seq, 4)).collect()
}

fn event_of(budget: &OpsEventBudget, ops: &[SequencedOperation]) -> Option<OpsEvent> {
    budget.event(&room(), INCARNATION, &schema_id(), 3, ops.iter())
}

/// The frame an event of `ops` carries, as the event would encode it.
fn frame_of(ops: &[SequencedOperation]) -> Vec<u8> {
    encode_message(&ServerMessage::OpsEvent {
        room_id: room(),
        room_incarnation: INCARNATION,
        head_seq: ops.last().unwrap().seq,
        ops: ops.to_vec(),
        schema_version: 3,
        schema_id: schema_id(),
    })
    .unwrap()
}

fn decoded(event: &OpsEvent) -> ServerMessage {
    let frame = base64::engine::general_purpose::STANDARD
        .decode(event.data())
        .unwrap();
    decode_message(&frame).unwrap()
}

#[test]
fn an_event_carries_its_operations_with_their_head_room_incarnation_and_schema() {
    let budget = OpsEventBudget::new(u64::MAX);
    let ops = ops(4..=6);
    let event = event_of(&budget, &ops).expect("three small operations fit in an event");
    assert_eq!(event.head_seq(), SequenceNumber::new(6));
    assert_eq!(
        decoded(&event),
        ServerMessage::OpsEvent {
            room_id: room(),
            room_incarnation: INCARNATION,
            head_seq: SequenceNumber::new(6),
            ops,
            schema_version: 3,
            schema_id: schema_id(),
        }
    );
    assert!(
        !event.data().contains(['\n', '\r']),
        "an SSE data line cannot hold a line break"
    );
}

#[test]
fn without_operations_there_is_no_event() {
    let budget = OpsEventBudget::new(u64::MAX);
    assert!(event_of(&budget, &[]).is_none());
    assert_eq!(budget.held(), 0);
}

/// The longest title whose operation, alone, makes a frame of exactly `len` bytes.
fn title_for_frame_of(len: usize) -> usize {
    let base = frame_of(&[op_with_title(1, 1000)]).len();
    // Around 4 KiB, one more byte of title is one more byte of frame.
    let title = 1000 + len - base;
    assert_eq!(frame_of(&[op_with_title(1, title)]).len(), len);
    title
}

#[test]
fn operations_whose_frame_passes_four_kib_are_announced_without_them() {
    let budget = OpsEventBudget::new(u64::MAX);
    let at_cap = title_for_frame_of(MAX_OPS_EVENT_FRAME as usize);
    assert!(event_of(&budget, &[op_with_title(1, at_cap)]).is_some());
    assert!(
        event_of(&budget, &[op_with_title(1, at_cap + 1)]).is_none(),
        "one byte over the cap"
    );
    // The cap is on the frame of the whole group, not on each operation.
    let two_thirds = title_for_frame_of(MAX_OPS_EVENT_FRAME as usize * 2 / 3);
    let group = [op_with_title(1, two_thirds), op_with_title(2, two_thirds)];
    assert!(event_of(&budget, &group[..1]).is_some());
    assert!(frame_of(&group).len() as u64 > MAX_OPS_EVENT_FRAME);
    assert!(event_of(&budget, &group).is_none());
    assert_eq!(budget.held(), 0);
}

#[test]
fn an_event_holds_its_bytes_of_the_total_until_its_last_copy_is_dropped() {
    let event = event_of(&OpsEventBudget::new(u64::MAX), &ops(1..=2)).unwrap();
    let len = event.retained();
    assert!(
        len > event.data().len(),
        "an event holds its base64 and what it takes besides"
    );
    drop(event);
    // Room for one event and a half.
    let budget = OpsEventBudget::new((len + len / 2) as u64);
    let first = event_of(&budget, &ops(1..=2)).unwrap();
    assert_eq!(budget.held(), len);
    assert!(
        event_of(&budget, &ops(1..=2)).is_none(),
        "a second event does not fit while the first is held"
    );
    let copy = first.clone();
    drop(first);
    assert_eq!(budget.held(), len, "a copy still holds it");
    drop(copy);
    assert_eq!(budget.held(), 0);
    assert!(event_of(&budget, &ops(1..=2)).is_some());
    assert_eq!(budget.held(), 0);
}

#[test]
fn with_a_total_of_zero_no_event_is_built() {
    let budget = OpsEventBudget::new(0);
    assert!(event_of(&budget, &ops(1..=1)).is_none());
}
