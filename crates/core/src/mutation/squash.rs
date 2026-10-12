use super::op::{ColumnUpdate, Operation, OperationKind};
use crate::value::Value;

/// Result of squashing an incoming operation into the pending one for the same primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SquashOutcome {
    /// The incoming operation was merged into the pending one.
    Merged,
    /// The incoming operation replaced the pending one, or was the first for its key.
    Replaced,
    /// The operations cannot be combined: a different table or primary key, or an update of a
    /// row with a pending delete.
    Incompatible,
}

/// Merges `incoming` into `existing`, both sorted by `column_idx`, in $O(M+N)$; a column in
/// both takes the incoming value.
pub fn merge_sorted_column_updates(existing: &mut Vec<ColumnUpdate>, incoming: Vec<ColumnUpdate>) {
    let mut merged = Vec::with_capacity(existing.len() + incoming.len());
    let mut older = std::mem::take(existing).into_iter().peekable();
    for update in incoming {
        while let Some(kept) = older.next_if(|u| u.column_idx < update.column_idx) {
            merged.push(kept);
        }
        older.next_if(|u| u.column_idx == update.column_idx);
        merged.push(update);
    }
    merged.extend(older);
    *existing = merged;
}

/// Squashes `incoming` into `existing`, the pending operation for the same table and primary
/// key, so that sending the result is equivalent to sending both in the order they arrived,
/// provided it is not too large to send (see `check_operation_size`): the incoming operation
/// always wins. Timestamps are only metadata; the result takes the incoming one.
pub fn squash_operations(existing: &mut Operation, incoming: Operation) -> SquashOutcome {
    if existing.table_id != incoming.table_id || existing.pk != incoming.pk {
        return SquashOutcome::Incompatible;
    }
    let outcome = match (&mut existing.kind, incoming.kind) {
        // An update cannot resurrect a row with a pending delete.
        (OperationKind::Delete, OperationKind::Update { .. }) => {
            return SquashOutcome::Incompatible
        }
        (OperationKind::Insert { row }, OperationKind::Update { updates }) => {
            for u in updates {
                let idx = usize::from(u.column_idx);
                if idx >= row.len() {
                    row.resize(idx + 1, Value::Null);
                }
                row[idx] = u.value;
            }
            SquashOutcome::Merged
        }
        (OperationKind::Update { updates }, OperationKind::Update { updates: incoming }) => {
            merge_sorted_column_updates(updates, incoming);
            SquashOutcome::Merged
        }
        // An insert (an upsert) or a delete replaces whatever was pending. A delete after a
        // pending insert is still sent: the row may exist on the server, and deleting a
        // missing row changes nothing.
        (kind, incoming_kind) => {
            *kind = incoming_kind;
            SquashOutcome::Replaced
        }
    };
    existing.timestamp = incoming.timestamp;
    outcome
}
