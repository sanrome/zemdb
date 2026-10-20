//! Indexes of the `UNIQUE` constraints and foreign keys of a room's tables, derived from their
//! rows.

use zemdb_core::{CompactRow, PrimaryKey, TableSchema};

use crate::memory::Table;

/// The index of one constraint: an entry per row whose key for the constraint has no `NULL`,
/// holding that key and the row's primary key, in that order.
///
/// A persistent ordered set, like [`Table`]: a clone takes O(1) and shares every node. It is a
/// multimap, so it follows the rows even if several of them have the same key.
pub(crate) type ConstraintIndex = imbl::OrdSet<(PrimaryKey, PrimaryKey)>;

/// The indexes of the constraints of one table: for each `UNIQUE` constraint, its key to the
/// primary key; for each foreign key, its key to the primary key of the referencing row. Both
/// are in declaration order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableIndexes {
    unique: Vec<ConstraintIndex>,
    foreign_keys: Vec<ConstraintIndex>,
}

impl TableIndexes {
    /// Empty indexes for every constraint of `table`.
    pub(crate) fn new(table: &TableSchema) -> Self {
        Self {
            unique: vec![ConstraintIndex::new(); table.unique_column_indices().len()],
            foreign_keys: vec![ConstraintIndex::new(); table.foreign_key_column_indices().len()],
        }
    }

    /// The indexes of `rows`, the rows of `table`.
    pub(crate) fn build(table: &TableSchema, rows: &Table) -> Self {
        let mut indexes = Self::new(table);
        for (pk, row) in rows {
            indexes.update(table, pk, None, Some(row));
        }
        indexes
    }

    /// Replaces the entries of the row with key `pk` as it was (`old`) with those of the row as
    /// it is (`new`). `None` is a row that does not exist.
    pub(crate) fn update(
        &mut self,
        table: &TableSchema,
        pk: &PrimaryKey,
        old: Option<&CompactRow>,
        new: Option<&CompactRow>,
    ) {
        debug_assert_eq!(
            (self.unique.len(), self.foreign_keys.len()),
            (
                table.unique_column_indices().len(),
                table.foreign_key_column_indices().len()
            ),
            "indexes built under another schema"
        );
        let unique = table.unique_column_indices().iter().zip(&mut self.unique);
        let foreign = table.foreign_key_column_indices().iter();
        for (columns, index) in unique.chain(foreign.zip(&mut self.foreign_keys)) {
            let old_key = old.and_then(|row| constraint_key(row, columns));
            let new_key = new.and_then(|row| constraint_key(row, columns));
            if old_key == new_key {
                continue;
            }
            if let Some(key) = old_key {
                index.remove(&(key, pk.clone()));
            }
            if let Some(key) = new_key {
                index.insert((key, pk.clone()));
            }
        }
    }
}

/// The key of a constraint over the columns at `columns` in `row`, or `None` if any of its
/// values is `NULL`. A column missing from a short row, written before the column was added,
/// reads `NULL`.
fn constraint_key(row: &CompactRow, columns: &[usize]) -> Option<PrimaryKey> {
    let value = |&column: &usize| row.get(column).filter(|value| !value.is_null());
    columns
        .iter()
        .all(|column| value(column).is_some())
        .then(|| PrimaryKey::composite(columns.iter().filter_map(value).cloned()))
}

#[cfg(test)]
#[path = "tests/index.rs"]
pub(crate) mod tests;
