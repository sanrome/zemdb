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
        match self {
            Value::Uuid(b) => Some(*b),
            _ => None,
        }
    }

    /// Parses a 32-character or 36-character hyphenated UUID hex string into `[u8; 16]`.
    pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
        let b = s.as_bytes();
        let mut bytes = [0u8; 16];
        if b.len() == 36 && b[8] == b'-' && b[13] == b'-' && b[18] == b'-' && b[23] == b'-' {
            let mut bi = 0;
            let mut i = 0;
            while i < 36 {
                if i == 8 || i == 13 || i == 18 || i == 23 {
                    i += 1;
                    continue;
                }
                let hi = hex_val(b[i])?;
                let lo = hex_val(b[i + 1])?;
                bytes[bi] = (hi << 4) | lo;
                bi += 1;
                i += 2;
            }
            Some(bytes)
        } else if b.len() == 32 {
            for bi in 0..16 {
                let hi = hex_val(b[bi * 2])?;
                let lo = hex_val(b[bi * 2 + 1])?;
                bytes[bi] = (hi << 4) | lo;
            }
            Some(bytes)
        } else {
            None
        }
    }

    /// Creates a Value::Uuid from a UUID hex string (32 or 36 chars with hyphens).
    pub fn from_uuid_str(s: &str) -> Option<Self> {
        Self::parse_uuid(s).map(Value::Uuid)
    }
}

#[inline]
const fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        let val = if v == 0.0 { 0.0 } else { v as f64 };
        Value::Float(val)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        let val = if v == 0.0 { 0.0 } else { v };
        Value::Float(val)
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
            Value::Float(v) => {
                let canonical = if *v == 0.0 { 0.0 } else { *v };
                canonical.to_bits().hash(state);
            }
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
            (Value::Float(a), Value::Float(b)) => {
                let ca = if *a == 0.0 { 0.0 } else { *a };
                let cb = if *b == 0.0 { 0.0 } else { *b };
                ca.total_cmp(&cb)
            }
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
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "Timestamp({})", v),
            Value::Uuid(b) => write!(
                f,
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
            ),
            Value::String(v) => write!(f, "\"{}\"", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "<bytes len={}>", v.len()),
        }
    }
}
