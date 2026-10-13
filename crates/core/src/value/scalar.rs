use super::data_type::DataType;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Dynamic strongly typed value.
///
/// Memory footprint is strictly bounded to 24 bytes on 64-bit platforms
/// by boxing heap-allocated dynamic payloads (`Box<str>` and `Box<[u8]>`)
/// while keeping fixed-size payloads (`[u8; 16]` for UUID) directly inline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Timestamp(i64),
    String(Box<str>),
    Bool(bool),
    Bytes(Box<[u8]>),
    Uuid([u8; 16]),
}

impl Value {
    /// Rank of the variant in the order between values of different types, used by `Ord`
    /// and `Hash`.
    #[inline]
    pub const fn type_order(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::Float(_) => 3,
            Value::Timestamp(_) => 4,
            Value::Uuid(_) => 5,
            Value::String(_) => 6,
            Value::Bytes(_) => 7,
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
            Value::Uuid(_) => DataType::Uuid,
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Value::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b.as_ref()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_uuid(&self) -> Option<&[u8; 16]> {
        match self {
            Value::Uuid(b) => Some(b),
            _ => None,
        }
    }

    #[inline]
    pub fn to_uuid(&self) -> Option<[u8; 16]> {
        self.as_uuid().copied()
    }

    /// Parses a 32-character or 36-character hyphenated UUID hex string into `[u8; 16]`.
    pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
        const HYPHENS: [usize; 4] = [8, 13, 18, 23];
        let b = s.as_bytes();
        let hyphenated = match b.len() {
            32 => false,
            36 if HYPHENS.iter().all(|&i| b[i] == b'-') => true,
            _ => return None,
        };
        // Every segment has an even length, so the digits pair up into bytes across hyphens.
        let mut digits = (0..b.len())
            .filter(|i| !(hyphenated && HYPHENS.contains(i)))
            .map(|i| (b[i] as char).to_digit(16));
        let mut bytes = [0u8; 16];
        for byte in &mut bytes {
            let hi = digits.next()??;
            let lo = digits.next()??;
            *byte = ((hi << 4) | lo) as u8;
        }
        Some(bytes)
    }

    /// Like [`Value::parse_uuid`], wrapped in `Value::Uuid`.
    pub fn from_uuid_str(s: &str) -> Option<Self> {
        Self::parse_uuid(s).map(Value::Uuid)
    }
}

macro_rules! from_lossless_int {
    ($($t:ty),*) => {$(
        impl From<$t> for Value {
            fn from(v: $t) -> Self {
                Value::Int(i64::from(v))
            }
        }
    )*};
}

from_lossless_int!(i8, i16, i32, i64, u8, u16, u32);

/// `-0.0` and `0.0` are the same value: both map to `0.0`, which keeps `Hash` consistent with
/// `Eq`.
#[inline]
fn canonical(v: f64) -> f64 {
    if v == 0.0 {
        0.0
    } else {
        v
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::from(f64::from(v))
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(canonical(v))
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v.into_boxed_str())
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.into())
    }
}

impl From<Box<str>> for Value {
    fn from(v: Box<str>) -> Self {
        Value::String(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<Bytes> for Value {
    fn from(b: Bytes) -> Self {
        Value::Bytes(b.to_vec().into_boxed_slice())
    }
}

impl From<Box<[u8]>> for Value {
    fn from(b: Box<[u8]>) -> Self {
        Value::Bytes(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v.into_boxed_slice())
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Bytes(v.to_vec().into_boxed_slice())
    }
}

impl From<[u8; 16]> for Value {
    fn from(bytes: [u8; 16]) -> Self {
        Value::Uuid(bytes)
    }
}

// `Eq`, `Ord` and `Hash` are manual because of `Float`: floats are ordered totally with
// `total_cmp`, after turning `-0.0` into `0.0` so that both are the same value, and `Hash`
// canonicalizes them the same way to stay consistent with `Eq`.
impl PartialEq for Value {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_order().hash(state);
        match self {
            Value::Null => {}
            Value::Int(v) => v.hash(state),
            Value::Float(v) => canonical(*v).to_bits().hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::Uuid(v) => v.hash(state),
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
            (Value::Float(a), Value::Float(b)) => canonical(*a).total_cmp(&canonical(*b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (a, b) => a.type_order().cmp(&b.type_order()),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Timestamp(v) => write!(f, "Timestamp({v})"),
            Value::Uuid(b) => {
                for (i, byte) in b.iter().enumerate() {
                    if matches!(i, 4 | 6 | 8 | 10) {
                        f.write_str("-")?;
                    }
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Value::String(v) => write!(f, "\"{v}\""),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Bytes(v) => write!(f, "<bytes len={}>", v.len()),
        }
    }
}
