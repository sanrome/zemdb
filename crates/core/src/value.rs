use bytes::Bytes;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Supported primitive data types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Int,
    Float,
    Timestamp,
    String,
    Bool,
    Bytes,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "Null"),
            DataType::Int => write!(f, "Int"),
            DataType::Float => write!(f, "Float"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::String => write!(f, "String"),
            DataType::Bool => write!(f, "Bool"),
            DataType::Bytes => write!(f, "Bytes"),
        }
    }
}

/// Dynamic strongly typed value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Timestamp(i64),
    String(String),
    Bool(bool),
    Bytes(Bytes),
}

impl Value {
    #[inline]
    pub const fn type_order(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::Float(_) => 3,
            Value::Timestamp(_) => 4,
            Value::String(_) => 5,
            Value::Bytes(_) => 6,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Int(_) => DataType::Int,
            Value::Float(_) => DataType::Float,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::String(_) => DataType::String,
            Value::Bool(_) => DataType::Bool,
            Value::Bytes(_) => DataType::Bytes,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b.as_ref()),
            _ => None,
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<Bytes> for Value {
    fn from(b: Bytes) -> Self {
        Value::Bytes(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(Bytes::from(v))
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Bytes(Bytes::copy_from_slice(v))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b) == Ordering::Equal,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_order().hash(state);
        match self {
            Value::Null => {}
            Value::Int(v) => v.hash(state),
            Value::Float(v) => v.to_bits().hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::String(v) => v.hash(state),
            Value::Bool(v) => v.hash(state),
            Value::Bytes(v) => v.as_ref().hash(state),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (a, b) => a.type_order().cmp(&b.type_order()),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "Timestamp({})", v),
            Value::String(v) => write!(f, "\"{}\"", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "<bytes len={}>", v.len()),
        }
    }
}

/// Primary key representation, optimized with SmallVec to keep scalar keys on the stack.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PrimaryKey(pub SmallVec<[Value; 2]>);

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
