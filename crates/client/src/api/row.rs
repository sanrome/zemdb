use serde::{Deserialize, Serialize};
use std::collections::btree_map::{self, BTreeMap};
use zemdb_core::Value;

/// A map of the values of some columns of a table, by the names of the columns.
macro_rules! columns {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(BTreeMap<String, Value>);

        impl $name {
            /// Without columns.
            pub fn new() -> Self {
                Self::default()
            }

            /// With `value` in `column`, in place of the value it had.
            pub fn with(mut self, column: impl Into<String>, value: impl Into<Value>) -> Self {
                self.set(column, value);
                self
            }

            /// Puts `value` in `column`, and returns the value it had.
            pub fn set(
                &mut self,
                column: impl Into<String>,
                value: impl Into<Value>,
            ) -> Option<Value> {
                self.0.insert(column.into(), value.into())
            }

            /// The value of `column`, if it has one.
            pub fn get(&self, column: &str) -> Option<&Value> {
                self.0.get(column)
            }

            /// Takes `column` out, with its value.
            pub fn remove(&mut self, column: &str) -> Option<Value> {
                self.0.remove(column)
            }

            /// How many columns it has.
            pub fn len(&self) -> usize {
                self.0.len()
            }

            /// Whether it has no columns.
            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            /// Its columns with their values, in the order of their names.
            pub fn iter(&self) -> btree_map::Iter<'_, String, Value> {
                self.0.iter()
            }
        }

        impl<C: Into<String>, V: Into<Value>> FromIterator<(C, V)> for $name {
            fn from_iter<I: IntoIterator<Item = (C, V)>>(columns: I) -> Self {
                let columns = columns.into_iter();
                Self(columns.map(|(column, value)| (column.into(), value.into())).collect())
            }
        }

        impl IntoIterator for $name {
            type Item = (String, Value);
            type IntoIter = btree_map::IntoIter<String, Value>;

            fn into_iter(self) -> Self::IntoIter {
                self.0.into_iter()
            }
        }

        impl<'a> IntoIterator for &'a $name {
            type Item = (&'a String, &'a Value);
            type IntoIter = btree_map::Iter<'a, String, Value>;

            fn into_iter(self) -> Self::IntoIter {
                self.0.iter()
            }
        }
    };
}

columns! {
    /// A row of a table, as the values of its columns by their names.
    ///
    /// A row that a read gives has every column of the schema it was read with (those a short
    /// row, written before a column was added, does not have, in `NULL`), or the columns that its
    /// scan asks for.
    Row
}

columns! {
    /// The key of a row of a table: the values of the columns of its primary key, by their
    /// names. The key of a read has every column of the primary key, the owner column too, if it
    /// is one of them; that of a write may lack the owner column, which the client fills, or have
    /// it with the owner of the client, as the key that an insert returns has it.
    Key
}
