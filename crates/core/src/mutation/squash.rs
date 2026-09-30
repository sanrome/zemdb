use super::op::{ColumnUpdate, Operation, OperationKind};
use crate::value::Value;

/// Result of attempting to squash two sequential operations for the same PK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SquashOutcome {
    /// The incoming operation was merged into the existing operation.
    Merged,
    /// The incoming operation completely replaced the existing operation.
    Replaced,
    /// The incoming operation was obsolete or a no-op and was discarded (existing remains unchanged).
    Discarded,
    /// Both operations cancelled each other out (e.g. pending Insert followed by Delete in client buffer).
    Purged,
    /// Operations cannot be squashed (e.g. different table_id, PK, or invalid transition like Delete followed by Update).
    Incompatible,
}

/// Merges two sorted lists of `ColumnUpdate` in $O(M+N)$ linear time using a two-pointer merge.
///
/// If `incoming_wins` is true, conflicting columns take the value from `incoming`;
/// otherwise, the value in `existing` is retained.
pub fn merge_sorted_column_updates(
    existing: &mut Vec<ColumnUpdate>,
    incoming: Vec<ColumnUpdate>,
    incoming_wins: bool,
) {
    let old_existing = std::mem::take(existing);
    let mut merged = Vec::with_capacity(old_existing.len() + incoming.len());
    let mut it_a = old_existing.into_iter().peekable();
    let mut it_b = incoming.into_iter().peekable();

    loop {
        match (it_a.peek(), it_b.peek()) {
            (Some(a), Some(b)) => {
                if a.column_idx < b.column_idx {
                    merged.push(it_a.next().unwrap());
                } else if a.column_idx > b.column_idx {
                    merged.push(it_b.next().unwrap());
                } else {
                    let item_a = it_a.next().unwrap();
                    let item_b = it_b.next().unwrap();
                    if incoming_wins {
                        merged.push(item_b);
                    } else {
                        merged.push(item_a);
                    }
                }
            }
            (Some(_), None) => {
                merged.extend(it_a);
                break;
            }
            (None, Some(_)) => {
                merged.extend(it_b);
                break;
            }
            (None, None) => break,
        }
    }
    *existing = merged;
}

/// Merges an incoming Operation into an existing pending Operation for the same table_id and PK (Client-side LWW by timestamp).
pub fn squash_operations(existing: &mut Operation, incoming: Operation) -> SquashOutcome {
    if existing.table_id != incoming.table_id || existing.pk != incoming.pk {
        return SquashOutcome::Incompatible;
    }

    match (&mut existing.kind, incoming.kind) {
        // Rule 1: INSERT followed by UPDATE -> direct slot update on CompactRow
        (OperationKind::Insert { row }, OperationKind::Update { updates }) => {
            if incoming.timestamp >= existing.timestamp {
                for u in updates {
                    let idx = u.column_idx as usize;
                    if idx >= row.len() {
                        row.resize(idx + 1, Value::Null);
                    }
                    row[idx] = u.value; // Move semantics: 0 clones
                }
                existing.timestamp = incoming.timestamp;
                SquashOutcome::Merged
            } else {
                SquashOutcome::Discarded
            }
        }

        // Rule 2: UPDATE followed by UPDATE -> O(M+N) two-pointer merge preserving column_idx ascending
        (
            OperationKind::Update {
                updates: existing_updates,
            },
            OperationKind::Update {
                updates: incoming_updates,
            },
        ) => {
            let incoming_wins = incoming.timestamp >= existing.timestamp;
            merge_sorted_column_updates(existing_updates, incoming_updates, incoming_wins);
            if incoming_wins {
                existing.timestamp = incoming.timestamp;
            }
            SquashOutcome::Merged
        }

        // Rule 3: DELETE followed by UPDATE (Anti-Zombie rule)
        // A partial update CANNOT resurrect a deleted entity.
        (OperationKind::Delete, OperationKind::Update { .. }) => {
            if incoming.timestamp <= existing.timestamp {
                SquashOutcome::Discarded
            } else {
                SquashOutcome::Incompatible
            }
        }

        // Rule 4: Any operation followed by DELETE
        (target_kind, OperationKind::Delete) => {
            if incoming.timestamp >= existing.timestamp {
                if matches!(target_kind, OperationKind::Insert { .. }) {
                    SquashOutcome::Purged
                } else {
                    *target_kind = OperationKind::Delete;
                    existing.timestamp = incoming.timestamp;
                    SquashOutcome::Replaced
                }
            } else {
                SquashOutcome::Discarded
            }
        }

        // Rule 5: Any operation followed by INSERT
        (target_kind, OperationKind::Insert { mut row }) => {
            if incoming.timestamp >= existing.timestamp {
                *target_kind = OperationKind::Insert { row };
                existing.timestamp = incoming.timestamp;
                SquashOutcome::Replaced
            } else {
                match target_kind {
                    OperationKind::Update { updates } => {
                        for u in updates.drain(..) {
                            let idx = u.column_idx as usize;
                            if idx >= row.len() {
                                row.resize(idx + 1, Value::Null);
                            }
                            row[idx] = u.value; // Move semantics: 0 clones
                        }
                        *target_kind = OperationKind::Insert { row };
                        SquashOutcome::Merged
                    }
                    OperationKind::Delete => SquashOutcome::Discarded,
                    OperationKind::Insert { .. } => SquashOutcome::Discarded,
                }
            }
        }
    }
}

pub use squash_operations as client_squash_operations;
