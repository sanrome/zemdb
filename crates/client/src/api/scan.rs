use serde::{Deserialize, Serialize};
use std::ops::{Bound, RangeBounds};

use super::Key;

/// The way a scan goes through the keys of a table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// In the order of the keys.
    #[default]
    Forward,
    /// In the reverse order of the keys.
    Backward,
}

/// A scan of a table: the rows whose keys are in a range, in the order of their keys or the
/// reverse, up to a limit, with every column or the columns it names. By default, every row,
/// forward, with every column.
///
/// Keys are ordered as their values in the order of the columns of the primary key. A bound of
/// the range is a key with every column of the primary key, as a key of a read is. A range whose
/// start is after its end holds no row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scan {
    /// Where the range starts.
    pub start: Bound<Key>,
    /// Where the range ends.
    pub end: Bound<Key>,
    /// The way the scan goes.
    pub direction: Direction,
    /// The most rows it gives.
    pub limit: Option<usize>,
    /// The columns of the rows it gives, each once, or every column of the table.
    pub columns: Option<Vec<String>>,
}

impl Default for Scan {
    fn default() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            direction: Direction::Forward,
            limit: None,
            columns: None,
        }
    }
}

impl Scan {
    /// Every row, forward, with every column.
    pub fn new() -> Self {
        Self::default()
    }

    /// Of the rows whose keys are in `range`.
    pub fn range(mut self, range: impl RangeBounds<Key>) -> Self {
        self.start = range.start_bound().cloned();
        self.end = range.end_bound().cloned();
        self
    }

    /// In the reverse order of the keys.
    pub fn backward(mut self) -> Self {
        self.direction = Direction::Backward;
        self
    }

    /// In the order of the keys.
    pub fn forward(mut self) -> Self {
        self.direction = Direction::Forward;
        self
    }

    /// Up to `limit` rows.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// With only `columns`, each once.
    pub fn columns<C: Into<String>>(mut self, columns: impl IntoIterator<Item = C>) -> Self {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }
}
