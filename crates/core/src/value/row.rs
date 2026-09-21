use super::scalar::Value;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Deref, Index};

/// Primary key representation, optimized with SmallVec to keep scalar keys on the stack
/// while strictly fitting within a single 64-byte L1 cache line (size: 40 bytes).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PrimaryKey(pub SmallVec<[Value; 1]>);

impl PrimaryKey {
    pub fn single(value: impl Into<Value>) -> Self {
        let mut v = SmallVec::new();
        v.push(value.into());
        Self(v)
    }

    pub fn composite(values: impl IntoIterator<Item = impl Into<Value>>) -> Self {
        Self(values.into_iter().map(|v| v.into()).collect())
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn into_values(self) -> SmallVec<[Value; 1]> {
        self.0
    }
}

impl Deref for PrimaryKey {
    type Target = [Value];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Index<usize> for PrimaryKey {
    type Output = Value;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() == 1 {
            write!(f, "{}", self.0[0])
        } else {
            write!(f, "(")?;
            for (i, val) in self.0.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", val)?;
            }
            write!(f, ")")
        }
    }
}

/// Positional row storage for high memory density and zero redundant column name strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CompactRow {
    pub values: Vec<Value>,
}

impl CompactRow {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

impl Deref for CompactRow {
    type Target = [Value];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl From<Vec<Value>> for CompactRow {
    fn from(values: Vec<Value>) -> Self {
        Self { values }
    }
}

/// A structured row represented as a map from column name to Value.
pub type Row = BTreeMap<String, Value>;

/// Ergonomic builder for creating structured rows.
#[derive(Debug, Clone, Default)]
pub struct RowBuilder {
    fields: BTreeMap<String, Value>,
}

impl RowBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(mut self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(column.into(), value.into());
        self
    }

    pub fn build(self) -> Row {
        self.fields
    }
}
