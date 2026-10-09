use crate::value::DataType;
use serde::{Deserialize, Serialize};

/// Definition of a single column in a table.
///
/// A column has no invariant of its own: whether it is acceptable (its type, whether it may be
/// part of the primary key, whether it may be appended later) depends on the table, which
/// checks it when the table is built, deserialized or evolved with `add_column`. The fields
/// are read through accessors so that a column cannot change once it belongs to a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
    pub(crate) encrypted: bool,
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

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }
}
