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
        match self {
            DataType::Null => write!(f, "Null"),
            DataType::Int => write!(f, "Int"),
            DataType::Float => write!(f, "Float"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::String => write!(f, "String"),
            DataType::Bool => write!(f, "Bool"),
            DataType::Bytes => write!(f, "Bytes"),
            DataType::Uuid => write!(f, "Uuid"),
        }
    }
}
