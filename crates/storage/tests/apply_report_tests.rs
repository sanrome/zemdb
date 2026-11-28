//! The report of `apply_batch`: the room's head after the batch, and the operations it did not
//! apply. The only one it does not apply is an update of a row that does not exist, reported as
//! `MissingRow`; the head moves past it all the same. Both engines report the same, and a
//! reopened disk room replays the same decisions.

mod common;

use common::*;
use zemdb_core::{RoomId, SequenceNumber, Value};
use zemdb_storage::{ApplyReport, MemoryStorageEngine, RejectReason, StorageEngine};

/// The reports of [`batches`], in order.
fn expected_reports() -> Vec<(u64, Vec<u64>)> {
    vec![(7, vec![1, 5]), (11, vec![8]), (11, vec![]), (12, vec![12])]
}

/// Batches of the `users` table with updates of rows that never existed, that were deleted in
/// the same batch and in an earlier one, together with inserts, updates of existing rows and a
/// delete of a row that does not exist, none of which is reported. The last batch has only a
/// rejected update.
fn batches() -> Vec<Vec<zemdb_core::SequencedOperation>> {
    let name = |text: &str| Value::String(text.into());
    vec![
        vec![
            update(1, 1, 1, name("never inserted"), 10),
            insert(2, 1, user_row(1), 10),
            update(3, 1, 1, name("first"), 10),
            delete(4, 1, 10),
            update(5, 1, 1, name("deleted in this batch"), 10),
            delete(6, 2, 10),
            insert(7, 2, user_row(2), 10),
        ],
        vec![
            update(8, 1, 1, name("deleted in an earlier batch"), 10),
            update(9, 2, 1, name("second"), 10),
            insert(10, 1, user_row(1), 10),
            update(11, 1, 2, Value::Int(5), 10),
        ],
        Vec::new(),
        vec![update(12, 3, 1, name("never inserted"), 10)],
    ]
}

/// Applies [`batches`] to `room`, opened with `users_schema`, checks each report and the rows it
/// leaves, and returns the reports.
async fn reports_only_updates_of_missing_rows<E: StorageEngine>(
    engine: &E,
    room: &RoomId,
) -> Vec<ApplyReport> {
    engine.open_room(room, users_schema()).await.unwrap();
    let mut reports = Vec::new();
    for (batch, (head, rejected)) in batches().into_iter().zip(expected_reports()) {
        let report = engine.apply_batch(room, batch).await.unwrap();
        assert_eq!(report.head_seq().get(), head);
        let expected: Vec<_> = rejected
            .into_iter()
            .map(|seq| (SequenceNumber::from(seq), RejectReason::MissingRow))
            .collect();
        assert_eq!(report.rejected(), expected.as_slice());
        reports.push(report);
    }
    assert_rows_after_batches(engine, room).await;
    reports
}

/// Checks that `room` holds what [`batches`] leave: the rejected updates changed nothing.
async fn assert_rows_after_batches<E: StorageEngine>(engine: &E, room: &RoomId) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), 12);
    let mut first = user_row(1);
    first[2] = Value::Int(5);
    let mut second = user_row(2);
    second[1] = Value::String("second".into());
    assert_eq!(get_user(engine, room, 1).await, Some(first));
    assert_eq!(get_user(engine, room, 2).await, Some(second));
    assert_eq!(get_user(engine, room, 3).await, None);
}

#[tokio::test]
async fn memory_reports_only_updates_of_missing_rows() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("report").unwrap();
    reports_only_updates_of_missing_rows(&engine, &room).await;
}

/// The disk engine reports the same as the memory engine, and a reopened room, which replays
/// the batches and discards their reports, holds the same rows.
#[tokio::test]
async fn disk_reports_the_same_and_replays_the_same_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("report").unwrap();
    let disk = disk_engine(dir.path());
    let on_disk = reports_only_updates_of_missing_rows(&disk, &room).await;
    let memory = MemoryStorageEngine::new();
    let in_memory = reports_only_updates_of_missing_rows(&memory, &room).await;
    assert_eq!(on_disk, in_memory);

    disk.close_room(&room).await.unwrap();
    let reopened = disk_engine(dir.path());
    reopened.open_room(&room, users_schema()).await.unwrap();
    assert_rows_after_batches(&reopened, &room).await;
}
