use rimdb_core::{CompactRow, PrimaryKey};
use std::collections::btree_map::{BTreeMap, IntoIter, Iter, IterMut, Range, RangeMut};
use std::ops::RangeBounds;

/// In-memory primary index mapping primary keys to compact rows.
///
/// Backed by an ordered `BTreeMap`, providing $O(\log N)$ point lookups,
/// inserts, and deletes, as well as efficient ordered range traversals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryIndex {
    entries: BTreeMap<PrimaryKey, CompactRow>,
}

impl PrimaryIndex {
    /// Creates a new, empty `PrimaryIndex`.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Returns the number of elements in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the index contains no elements.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns a reference to the row corresponding to the primary key.
    pub fn get(&self, pk: &PrimaryKey) -> Option<&CompactRow> {
        self.entries.get(pk)
    }

    /// Returns a mutable reference to the row corresponding to the primary key.
    pub fn get_mut(&mut self, pk: &PrimaryKey) -> Option<&mut CompactRow> {
        self.entries.get_mut(pk)
    }

    /// Inserts a primary key and row into the index.
    pub fn insert(&mut self, pk: PrimaryKey, row: CompactRow) -> Option<CompactRow> {
        self.entries.insert(pk, row)
    }

    /// Removes a primary key from the index, returning the row if it was present.
    pub fn remove(&mut self, pk: &PrimaryKey) -> Option<CompactRow> {
        self.entries.remove(pk)
    }

    /// Constructs a double-ended iterator over a sub-range of elements in the index.
    pub fn range<R>(&self, range: R) -> Range<'_, PrimaryKey, CompactRow>
    where
        R: RangeBounds<PrimaryKey>,
    {
        self.entries.range(range)
    }

    /// Constructs a mutable double-ended iterator over a sub-range of elements in the index.
    pub fn range_mut<R>(&mut self, range: R) -> RangeMut<'_, PrimaryKey, CompactRow>
    where
        R: RangeBounds<PrimaryKey>,
    {
        self.entries.range_mut(range)
    }

    /// Gets an iterator over the entries of the index, sorted by key.
    pub fn iter(&self) -> Iter<'_, PrimaryKey, CompactRow> {
        self.entries.iter()
    }

    /// Gets a mutable iterator over the entries of the index, sorted by key.
    pub fn iter_mut(&mut self) -> IterMut<'_, PrimaryKey, CompactRow> {
        self.entries.iter_mut()
    }

    /// Clears the index, removing all elements.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns an immutable reference to the underlying `BTreeMap`.
    pub fn as_btree_map(&self) -> &BTreeMap<PrimaryKey, CompactRow> {
        &self.entries
    }

    /// Returns a mutable reference to the underlying `BTreeMap`.
    pub fn as_btree_map_mut(&mut self) -> &mut BTreeMap<PrimaryKey, CompactRow> {
        &mut self.entries
    }

    /// Consumes the index and returns the underlying `BTreeMap`.
    pub fn into_inner(self) -> BTreeMap<PrimaryKey, CompactRow> {
        self.entries
    }
}

impl IntoIterator for PrimaryIndex {
    type Item = (PrimaryKey, CompactRow);
    type IntoIter = IntoIter<PrimaryKey, CompactRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a> IntoIterator for &'a PrimaryIndex {
    type Item = (&'a PrimaryKey, &'a CompactRow);
    type IntoIter = Iter<'a, PrimaryKey, CompactRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a mut PrimaryIndex {
    type Item = (&'a PrimaryKey, &'a mut CompactRow);
    type IntoIter = IterMut<'a, PrimaryKey, CompactRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter_mut()
    }
}

impl FromIterator<(PrimaryKey, CompactRow)> for PrimaryIndex {
    fn from_iter<T: IntoIterator<Item = (PrimaryKey, CompactRow)>>(iter: T) -> Self {
        Self {
            entries: BTreeMap::from_iter(iter),
        }
    }
}

impl From<BTreeMap<PrimaryKey, CompactRow>> for PrimaryIndex {
    fn from(entries: BTreeMap<PrimaryKey, CompactRow>) -> Self {
        Self { entries }
    }
}
