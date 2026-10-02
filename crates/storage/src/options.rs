use std::ops::{
    Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
};
use zemdb_core::PrimaryKey;

/// Direction for scanning keys in a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanDirection {
    #[default]
    Forward,
    Backward,
}

/// Key range specification for range scan queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Bound<PrimaryKey>,
    pub end: Bound<PrimaryKey>,
}

impl Default for KeyRange {
    fn default() -> Self {
        Self::all()
    }
}

impl KeyRange {
    /// Creates a range covering all possible keys (`..`).
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Checks if a given primary key falls within this range.
    pub fn contains(&self, pk: &PrimaryKey) -> bool {
        let start_ok = match &self.start {
            Bound::Included(start) => pk >= start,
            Bound::Excluded(start) => pk > start,
            Bound::Unbounded => true,
        };
        let end_ok = match &self.end {
            Bound::Included(end) => pk <= end,
            Bound::Excluded(end) => pk < end,
            Bound::Unbounded => true,
        };
        start_ok && end_ok
    }
}

impl RangeBounds<PrimaryKey> for KeyRange {
    fn start_bound(&self) -> Bound<&PrimaryKey> {
        match &self.start {
            Bound::Included(pk) => Bound::Included(pk),
            Bound::Excluded(pk) => Bound::Excluded(pk),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&PrimaryKey> {
        match &self.end {
            Bound::Included(pk) => Bound::Included(pk),
            Bound::Excluded(pk) => Bound::Excluded(pk),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl From<RangeFull> for KeyRange {
    fn from(_: RangeFull) -> Self {
        Self::all()
    }
}

impl From<Range<PrimaryKey>> for KeyRange {
    fn from(r: Range<PrimaryKey>) -> Self {
        Self {
            start: Bound::Included(r.start),
            end: Bound::Excluded(r.end),
        }
    }
}

impl From<RangeInclusive<PrimaryKey>> for KeyRange {
    fn from(r: RangeInclusive<PrimaryKey>) -> Self {
        let (start, end) = r.into_inner();
        Self {
            start: Bound::Included(start),
            end: Bound::Included(end),
        }
    }
}

impl From<RangeFrom<PrimaryKey>> for KeyRange {
    fn from(r: RangeFrom<PrimaryKey>) -> Self {
        Self {
            start: Bound::Included(r.start),
            end: Bound::Unbounded,
        }
    }
}

impl From<RangeTo<PrimaryKey>> for KeyRange {
    fn from(r: RangeTo<PrimaryKey>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(r.end),
        }
    }
}

impl From<RangeToInclusive<PrimaryKey>> for KeyRange {
    fn from(r: RangeToInclusive<PrimaryKey>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(r.end),
        }
    }
}

/// Options to configure scanning a table.
///
/// Supports query optimizations like limit pushdown, column projection pushdown,
/// and reverse traversal (`ORDER BY pk DESC`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScanOptions {
    /// Key range to scan.
    pub range: KeyRange,
    /// Scan direction (Forward = ASC, Backward = DESC).
    pub direction: ScanDirection,
    /// Maximum number of rows to return (Limit Pushdown).
    pub limit: Option<usize>,
    /// Column indices to project (Projection Pushdown). If None, all columns are returned.
    pub projection: Option<Vec<u16>>,
}

impl ScanOptions {
    /// Creates a default `ScanOptions` scanning all rows forwards.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the key range to scan.
    pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
        self.range = range.into();
        self
    }

    /// Sets the scan direction.
    pub fn direction(mut self, direction: ScanDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Sets the scan direction to backward (`DESC`).
    pub fn backward(mut self) -> Self {
        self.direction = ScanDirection::Backward;
        self
    }

    /// Sets the scan direction to forward (`ASC`).
    pub fn forward(mut self) -> Self {
        self.direction = ScanDirection::Forward;
        self
    }

    /// Sets the limit for rows returned.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the projected column indices.
    pub fn projection(mut self, projection: Vec<u16>) -> Self {
        self.projection = Some(projection);
        self
    }
}
