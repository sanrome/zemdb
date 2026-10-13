use serde::{Deserialize, Serialize};
use std::fmt;

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
    Uuid,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The variant name, e.g. "Int": renaming a variant changes this text.
        fmt::Debug::fmt(self, f)
    }
}
