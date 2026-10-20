use serde::{Deserialize, Serialize};

/// Maximum number of constraints (`UNIQUE` and foreign keys together) of a table. Every
/// constraint is checked, and indexed, on every operation of its table, so the count bounds
/// the cost of applying one operation.
pub const MAX_CONSTRAINTS_PER_TABLE: usize = 32;

/// Maximum number of columns of a single constraint.
pub const MAX_CONSTRAINT_COLUMNS: usize = 16;

/// A `UNIQUE` constraint over one or more columns of a table.
///
/// It is declared by name and validated when its table is built: the columns exist, are not
/// repeated, are not encrypted, and are not exactly the primary key. The name is unique in the
/// whole schema. The fields are read through accessors so that a constraint cannot change once
/// it belongs to a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniqueConstraint {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
}

impl UniqueConstraint {
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

/// What deleting a referenced row does to the rows that reference it.
///
/// The semantics of a variant never change: another behavior is another variant, which a
/// replica that does not know it fails to decode instead of applying the log without the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OnDelete {
    /// The delete is rejected while any row references the deleted one.
    Restrict,
}

/// A foreign key: columns of a table that reference the whole primary key of a table of the
/// same schema, possibly the same table.
///
/// The referenced table is named, like the primary key columns, and resolved when the schema
/// is built: it exists, and the columns match its primary key in count and, in order, in type.
/// The columns exist in the referencing table, are not repeated and are not encrypted. The
/// name is unique in the whole schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) references: String,
    pub(crate) on_delete: OnDelete,
}

impl ForeignKey {
    /// A foreign key with `on_delete` set to [`OnDelete::Restrict`], the only action.
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
        references: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            references: references.into(),
            on_delete: OnDelete::Restrict,
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The name of the referenced table.
    #[inline]
    pub fn references(&self) -> &str {
        &self.references
    }

    #[inline]
    pub fn on_delete(&self) -> OnDelete {
        self.on_delete
    }
}
