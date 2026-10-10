use super::decode_budget::charge_values;
use super::scalar::Value;
use crate::schema::MAX_COLUMNS;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Index;

/// Primary key representation, optimized with SmallVec to keep scalar keys on the stack
/// while strictly fitting within a single 64-byte L1 cache line (size: 40 bytes).
///
/// Deserialization rejects more than [`MAX_COLUMNS`] values, counted as they are decoded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PrimaryKey(#[serde(deserialize_with = "deserialize_columns")] SmallVec<[Value; 1]>);

impl PrimaryKey {
    pub fn single(value: impl Into<Value>) -> Self {
        let mut v = SmallVec::new();
        v.push(value.into());
        Self(v)
    }

    pub fn composite(values: impl IntoIterator<Item = impl Into<Value>>) -> Self {
        Self(values.into_iter().map(|v| v.into()).collect())
    }

    pub fn from_smallvec(values: SmallVec<[Value; 1]>) -> Self {
        Self(values)
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn as_slice(&self) -> &[Value] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.0.iter()
    }

    pub fn into_values(self) -> SmallVec<[Value; 1]> {
        self.0
    }
}

impl<'a> IntoIterator for &'a PrimaryKey {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl AsRef<[Value]> for PrimaryKey {
    #[inline]
    fn as_ref(&self) -> &[Value] {
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
///
/// Deserialization rejects more than [`MAX_COLUMNS`] values, counted as they are decoded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct CompactRow {
    #[serde(deserialize_with = "deserialize_columns")]
    values: Vec<Value>,
}

impl CompactRow {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn as_slice(&self) -> &[Value] {
        &self.values
    }

    pub fn resize(&mut self, new_len: usize, value: Value) {
        self.values.resize(new_len, value);
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.values.iter()
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

impl<'a> IntoIterator for &'a CompactRow {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl IntoIterator for CompactRow {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.into_values().into_iter()
    }
}

impl AsRef<[Value]> for CompactRow {
    #[inline]
    fn as_ref(&self) -> &[Value] {
        &self.values
    }
}

impl Index<usize> for CompactRow {
    type Output = Value;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl std::ops::IndexMut<usize> for CompactRow {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

impl From<Vec<Value>> for CompactRow {
    fn from(values: Vec<Value>) -> Self {
        Self { values }
    }
}

/// A collection with at most one element per column of a table, deserialized by
/// [`deserialize_columns`].
pub(crate) trait ColumnSeq: Default {
    type Item;
    fn reserve(&mut self, additional: usize);
    fn push(&mut self, item: Self::Item);
}

impl<T> ColumnSeq for Vec<T> {
    type Item = T;

    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional);
    }

    fn push(&mut self, item: T) {
        Vec::push(self, item);
    }
}

impl<A: smallvec::Array> ColumnSeq for SmallVec<A> {
    type Item = A::Item;

    fn reserve(&mut self, additional: usize) {
        SmallVec::reserve(self, additional);
    }

    fn push(&mut self, item: A::Item) {
        SmallVec::push(self, item);
    }
}

/// Largest preallocation of [`deserialize_columns`], in bytes. Longer sequences grow as their
/// elements arrive, so memory follows what was actually decoded, not the declared length.
const MAX_COLUMNS_PREALLOC_BYTES: usize = 64 * 1024;

/// Deserializes a sequence with at most one element per column (rows, primary keys, update
/// deltas), rejecting more than [`MAX_COLUMNS`] elements.
///
/// The derived implementations would trust the length a frame declares: `Vec` reserves up to
/// 1 MiB from it and `SmallVec` all of it, and both accept any number of elements, which lets a
/// 16 MiB frame materialize millions of values. Here a declared length above the limit is
/// rejected before decoding anything, the preallocation is capped, and the elements are counted
/// as they arrive, so the limit holds whatever the format declares. The serialized form is
/// that of a plain sequence.
///
/// Every element holds one `Value` (a row or key value, or the value of an update delta), and
/// these sequences are the only place a client message carries values, so they charge the
/// value budget of the decode, if any (see `decode_budget`): the declared length up front,
/// before decoding the elements (the wire format always declares it), and one by one any
/// element beyond it, so at most one value is materialized past the budget.
///
/// Rows are the hot path of log and snapshot reads: without `#[inline]` the visitor is not
/// inlined into the derived decoders, and decoding rows gets about a third slower.
#[inline]
pub(crate) fn deserialize_columns<'de, D, C>(deserializer: D) -> Result<C, D::Error>
where
    D: Deserializer<'de>,
    C: ColumnSeq,
    C::Item: Deserialize<'de>,
{
    struct ColumnsVisitor<C>(PhantomData<C>);

    impl<'de, C> Visitor<'de> for ColumnsVisitor<C>
    where
        C: ColumnSeq,
        C::Item: Deserialize<'de>,
    {
        type Value = C;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "a sequence of at most {MAX_COLUMNS} elements")
        }

        #[inline]
        fn visit_seq<S>(self, mut seq: S) -> Result<C, S::Error>
        where
            S: SeqAccess<'de>,
        {
            let declared = seq.size_hint().unwrap_or(0);
            if declared > MAX_COLUMNS {
                return Err(de::Error::invalid_length(declared, &self));
            }
            charge(declared)?;
            let mut items = C::default();
            let item_size = std::mem::size_of::<C::Item>().max(1);
            items.reserve(declared.min(MAX_COLUMNS_PREALLOC_BYTES / item_size));
            let mut len = 0;
            while let Some(item) = seq.next_element()? {
                if len == MAX_COLUMNS {
                    return Err(de::Error::invalid_length(len + 1, &self));
                }
                if len >= declared {
                    charge(1)?;
                }
                items.push(item);
                len += 1;
            }
            Ok(items)
        }
    }

    deserializer.deserialize_seq(ColumnsVisitor(PhantomData))
}

/// Charges `count` values against the value budget of the decode, if any.
fn charge<E: de::Error>(count: usize) -> Result<(), E> {
    charge_values(count as u64)
        .map_err(|_| E::custom("message carries more values than its budget"))
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

#[cfg(test)]
#[path = "tests/row.rs"]
mod tests;
