//! Retention contract of a room log, shared by the log, the client lease tracker and the
//! snapshot relay.
//!
//! `tail_seq` is the first sequence number the log still retains and `head_seq` the last one
//! committed. A new room has `tail_seq = 1` and `head_seq = 0`; a log pruned completely has
//! `tail_seq = head_seq + 1`. So `tail_seq` may exceed `head_seq` by one, and is never 0.
//!
//! A client at cursor `c` has applied everything up to `c` and needs every operation after it,
//! so it can catch up from the log iff `c >= tail_seq - 1`. A snapshot at sequence `S` is usable
//! iff `tail_seq - 1 <= S <= head_seq`: a client restoring it can continue from the log, and it
//! does not claim operations the room never committed.

use zemdb_core::id::SequenceNumber;

/// Whether a client at `cursor` can no longer catch up from a log whose first retained
/// sequence is `tail_seq`, because the operation right after its cursor has been pruned
/// (`cursor < tail_seq - 1`).
pub fn is_behind_tail(cursor: SequenceNumber, tail_seq: SequenceNumber) -> bool {
    cursor.get().saturating_add(1) < tail_seq.get()
}

/// Whether a client restoring a snapshot at `seq` can catch up from a log retaining
/// `tail_seq..=head_seq` (`tail_seq - 1 <= seq <= head_seq`).
pub fn is_usable_snapshot(
    seq: SequenceNumber,
    tail_seq: SequenceNumber,
    head_seq: SequenceNumber,
) -> bool {
    !is_behind_tail(seq, tail_seq) && seq <= head_seq
}

#[cfg(test)]
#[path = "tests/retention.rs"]
mod tests;
