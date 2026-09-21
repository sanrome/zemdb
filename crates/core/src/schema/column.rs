use crate::value::DataType;
use serde::{Deserialize, Serialize};

/// Definition of a single column in a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub encrypted: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
            encrypted: false,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn encrypted(mut self, encrypted: bool) -> Self {
        self.encrypted = encrypted;
        self
    }
}
