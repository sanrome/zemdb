use super::op::{Operation, OperationKind, TableOperation};

/// Result of attempting to squash two sequential operations for the same PK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SquashOutcome {
    /// The incoming operation was merged into the existing operation.
    Merged,
    /// The incoming operation completely replaced the existing operation.
    Replaced,
    /// The incoming operation was obsolete or a no-op and was discarded (existing remains unchanged).
    Discarded,
    /// Operations cannot be squashed (e.g. different table, PK, or invalid transition like Delete followed by Update).
    Incompatible,
}

/// Merges an incoming TableOperation into an existing pending TableOperation for the same PK.
pub fn squash_table_operations(
    existing: &mut TableOperation,
    incoming: TableOperation,
) -> SquashOutcome {
    if existing.pk != incoming.pk {
        return SquashOutcome::Incompatible;
    }

    match (&mut existing.kind, incoming.kind) {
        // Rule 1: INSERT followed by UPDATE -> direct slot update on CompactRow
        (OperationKind::Insert { row }, OperationKind::Update { updates }) => {
            if incoming.timestamp >= existing.timestamp {
                for u in updates {
                    let idx = u.column_idx as usize;
                    if let Some(slot) = row.values.get_mut(idx) {
                        *slot = u.value; // Move semantics: 0 clones
                    }
                }
                existing.timestamp = incoming.timestamp;
            } else {
                for u in updates {
                    let idx = u.column_idx as usize;
                    if let Some(slot) = row.values.get_mut(idx) {
                        if slot.is_null() {
                            *slot = u.value;
                        }
                    }
                }
            }
            SquashOutcome::Merged
        }

        // Rule 2: UPDATE followed by UPDATE -> sorted delta merge preserving column_idx ascending
        (
            OperationKind::Update {
                updates: existing_updates,
            },
            OperationKind::Update {
                updates: incoming_updates,
            },
        ) => {
            if incoming.timestamp >= existing.timestamp {
                for inc in incoming_updates {
                    match existing_updates.binary_search_by_key(&inc.column_idx, |u| u.column_idx) {
                        Ok(pos) => {
                            existing_updates[pos].value = inc.value;
                        }
                        Err(pos) => {
                            existing_updates.insert(pos, inc);
                        }
                    }
                }
                existing.timestamp = incoming.timestamp;
            } else {
                for inc in incoming_updates {
                    if let Err(pos) =
                        existing_updates.binary_search_by_key(&inc.column_idx, |u| u.column_idx)
                    {
                        existing_updates.insert(pos, inc);
                    }
                }
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
                *target_kind = OperationKind::Delete;
                existing.timestamp = incoming.timestamp;
                SquashOutcome::Replaced
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
                            if let Some(slot) = row.values.get_mut(idx) {
                                *slot = u.value; // Move semantics: 0 clones
                            }
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

/// Merges an incoming operation into an existing pending operation for the same table and PK.
pub fn squash_operations(existing: &mut Operation, incoming: Operation) -> SquashOutcome {
    if existing.table != incoming.table {
        return SquashOutcome::Incompatible;
    }
    squash_table_operations(&mut existing.op, incoming.op)
}
