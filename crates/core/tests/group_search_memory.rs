//! What the search after a damaged frame of the last group of `active.wal`
//! (`find_frame_after_group`) holds in memory when the bytes after the damage hold a frame that a
//! member stored as a value (any bytes, with a valid header and payload, that decode to however
//! many values or operations fit in its payload), and when it reads frames of the group of
//! growing size.
//!
//! The file holds a single test, because it measures the peak resident memory of the whole
//! process (`VmHWM` in `/proc/self/status`, reset through `/proc/self/clear_refs`): Linux only.
//! The test runs each case in a process of its own (the test binary again, with the case in an
//! environment variable), so that no case reuses memory that another one freed and the process
//! still holds.

#![cfg(target_os = "linux")]

use std::io::Cursor;
use std::process::Command;

use zemdb_core::mutation::Operation;
use zemdb_core::protocol::codec::{MAX_FRAME_SIZE, MAX_MESSAGE_SIZE};
use zemdb_core::protocol::limits::MAX_MESSAGE_VALUES;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::{
    encode_wal_batch_in_group, find_frame_after_group, FrameAnomaly, InvalidHeader,
    BATCH_HEADER_SIZE, BATCH_MAGIC,
};
use zemdb_core::schema::MAX_COLUMNS;
use zemdb_core::value::{CompactRow, PrimaryKey, Value};

/// The name of the test, which runs again for each case.
const TEST_NAME: &str =
    "the_search_after_a_damaged_group_holds_at_most_a_frame_whatever_the_frames_after_the_damage_hold";

/// The environment variable that tells the test which case to run, in a process of its own.
const CASE_VAR: &str = "ZEMDB_GROUP_SEARCH_MEMORY_CASE";

/// How many cases there are.
const CASES: usize = 5;

/// A field of `/proc/self/status`, in bytes.
fn status_bytes(field: &str) -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .unwrap_or_else(|| panic!("no {field} in /proc/self/status"));
    let kib: usize = line
        .split_whitespace()
        .nth(1)
        .and_then(|kib| kib.parse().ok())
        .unwrap();
    kib * 1024
}

/// Resets the peak resident memory of the process (`VmHWM`) to what it holds now, and returns
/// that.
fn reset_peak() -> usize {
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
    status_bytes("VmRSS:")
}

/// Bytes before the operations in a payload of the log: where its group starts (8 bytes), no
/// mutation id (1) and the number of operations (8).
const PAYLOAD_PREFIX: usize = 17;

/// A frame whose header declares `ops_count` operations and whose payload is `payload`.
fn frame_with(payload: &[u8], ops_count: usize) -> Vec<u8> {
    let mut frame = Vec::with_capacity(BATCH_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&BATCH_MAGIC);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    frame.extend_from_slice(&(ops_count as u32).to_le_bytes());
    let header_crc = crc32fast::hash(&frame);
    frame.extend_from_slice(&header_crc.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// A frame of the log that says its group starts `group_offset` bytes before it, holding as many
/// copies of `encoded_op`, the encoding of one operation in a payload, as fit in a payload, built
/// from the bytes alone, so that the test never holds the operations decoded. Returns the frame
/// and how many operations it holds.
fn full_frame_of(encoded_op: &[u8], group_offset: u64) -> (Vec<u8>, usize) {
    let count = (MAX_MESSAGE_SIZE as usize - PAYLOAD_PREFIX) / encoded_op.len();
    let mut payload = Vec::with_capacity(PAYLOAD_PREFIX + encoded_op.len() * count);
    payload.extend_from_slice(&group_offset.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&(count as u64).to_le_bytes());
    for _ in 0..count {
        payload.extend_from_slice(encoded_op);
    }
    (frame_with(&payload, count), count)
}

/// The encoding of `op` in a payload of the log, checked against the frame the log writes for
/// it alone.
fn encoded(op: &SequencedOperation) -> Vec<u8> {
    let one = encode_wal_batch_in_group(std::slice::from_ref(op), None, 0).unwrap();
    let encoded_op = one[BATCH_HEADER_SIZE + PAYLOAD_PREFIX..].to_vec();
    let mut payload = 0u64.to_le_bytes().to_vec();
    payload.push(0);
    payload.extend_from_slice(&1u64.to_le_bytes());
    payload.extend_from_slice(&encoded_op);
    assert_eq!(
        payload,
        one[BATCH_HEADER_SIZE..],
        "a payload of one operation"
    );
    encoded_op
}

/// An insert whose row is `MAX_COLUMNS` `Null`s.
fn null_row() -> SequencedOperation {
    let row = CompactRow::new(vec![Value::Null; MAX_COLUMNS]);
    SequencedOperation::new(1, Operation::insert(0, PrimaryKey::single(1i64), row, 100))
}

/// A delete with an empty key and no owner: no values at all.
fn empty_delete() -> SequencedOperation {
    SequencedOperation::new(
        1,
        Operation::delete(0, PrimaryKey::composite(Vec::<Value>::new()), 100),
    )
}

/// A frame at `position` of a group that starts at offset 0, with a payload of `len` bytes that
/// only says where its group starts.
fn frame_of_the_damaged_group(position: usize, len: usize) -> Vec<u8> {
    let mut payload = vec![0x5A; len];
    payload[..8].copy_from_slice(&(position as u64).to_le_bytes());
    frame_with(&payload, 0)
}

/// Case `case`: what it is, the bytes of `active.wal` after damage at offset 0 that leaves no
/// header there, and where the search must find a frame of a later group.
fn case(case: usize) -> (String, Vec<u8>, Option<u64>) {
    let mut log = vec![0u8; BATCH_HEADER_SIZE];
    if case < 4 {
        // A frame stored as a value right after the damage, of one of two shapes that the size of
        // its payload does not bound once decoded, that says its group starts with it (a later
        // group: corruption) or before the damage (the damaged group: a torn write).
        let (shape, op, values_per_op) = match case / 2 {
            0 => ("rows of nulls", null_row(), MAX_COLUMNS + 1),
            _ => ("empty deletes", empty_delete(), 0),
        };
        let (group, group_offset, expected) = match case % 2 {
            0 => ("a later group", 0, Some(BATCH_HEADER_SIZE as u64)),
            _ => ("the damaged group", BATCH_HEADER_SIZE as u64, None),
        };
        let (frame, count) = full_frame_of(&encoded(&op), group_offset);
        log.extend_from_slice(&frame);
        let values = count * values_per_op;
        match values_per_op {
            0 => assert!(count > 500_000, "{shape}: {count} operations"),
            _ => assert!(values as u64 > 3 * MAX_MESSAGE_VALUES, "{shape}: {values}"),
        }
        let what = format!("{shape} ({count} operations, {values} values), of {group}");
        return (what, log, expected);
    }
    // Two frames of the damaged group after the damage, the second one larger: 9 MiB and then as
    // large as a payload can be, so that the buffer the search reads them into has to grow.
    let small = (MAX_MESSAGE_SIZE as usize) * 9 / 16;
    for len in [small, MAX_MESSAGE_SIZE as usize] {
        let frame = frame_of_the_damaged_group(log.len(), len);
        log.extend_from_slice(&frame);
    }
    let what = format!("frames of {small} and {MAX_MESSAGE_SIZE} bytes of the damaged group");
    (what, log, None)
}

/// Runs case `index` in this process: the search finds what it must, and holds at most a frame.
fn run_case(index: usize) {
    let (what, log, expected) = case(index);
    let anomaly = FrameAnomaly::InvalidHeader(InvalidHeader::Magic([0, 0]));
    let end = log.len() as u64;
    let mut reader = Cursor::new(log.as_slice());

    let before = reset_peak();
    let found = find_frame_after_group(&mut reader, 0, &anomaly, end).unwrap();
    let held = status_bytes("VmHWM:").saturating_sub(before);
    eprintln!(
        "{what}: the search held {} MiB at once, from {} MiB",
        held >> 20,
        before >> 20
    );

    assert_eq!(found, expected, "{what}");
    // What one frame of the log takes read: the frame, with room for the window of the search
    // and the allocator.
    let bound = MAX_FRAME_SIZE + (4 << 20);
    assert!(
        held <= bound,
        "{what}: the search held {held} bytes at once, more than a frame ({bound} bytes)"
    );
}

/// The search after a damaged frame of the last group holds at most a frame, whatever the frames
/// it finds after the damage hold, so that opening a room whose last group was torn holds what
/// reading one frame of its log takes.
///
/// The first cases hold a valid frame after the damage, as a member can store one in a `Bytes`
/// value of its commit, in two shapes that the size of its payload does not bound once decoded:
/// operations whose rows are `MAX_COLUMNS` `Null`s, about 4 million values, more than a client
/// message may carry (`MAX_MESSAGE_VALUES`), and about half a million deletes with an empty key
/// and no owner, which carry no values at all. Each one says its group starts with it, after the
/// damage, which makes it a later write (corruption: the room fails to open, the risk accepted
/// for a frame stored in a value), or before the damage, which makes it a frame of the damaged
/// group (the search goes on after it and finds nothing else: a torn write). The last one holds
/// two frames of the damaged group, the second larger than the first: growing the buffer it reads
/// payloads into must not hold the first payload and the second at once.
#[test]
fn the_search_after_a_damaged_group_holds_at_most_a_frame_whatever_the_frames_after_the_damage_hold(
) {
    if let Ok(index) = std::env::var(CASE_VAR) {
        run_case(index.parse().unwrap());
        return;
    }
    let mut failed = Vec::new();
    for index in 0..CASES {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([TEST_NAME, "--exact", "--nocapture", "--test-threads=1"])
            .env(CASE_VAR, index.to_string())
            .output()
            .unwrap();
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("1 passed") || stdout.contains("1 failed"),
            "the case {index} did not run: {stdout}"
        );
        if !output.status.success() {
            failed.push(index);
        }
    }
    assert!(failed.is_empty(), "the cases {failed:?} failed");
}
