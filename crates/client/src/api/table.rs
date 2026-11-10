use futures::StreamExt;
use std::fmt;
use std::ops::Bound;
use zemdb_core::{ColumnDef, CompactRow, DataType, PrimaryKey, RoomId, Schema, TableSchema, Value};
use zemdb_storage::{KeyRange, ScanDirection, ScanOptions, StorageEngine, StorageError};

use super::{Direction, Key, RoomHandle, Row, Scan};
use crate::ClientError;

/// A table of a room, by its name, which each call resolves against the schema of the replica:
/// the schema only grows, and a table keeps its id and its name. A clone shares the client.
#[derive(Clone)]
pub struct TableHandle {
    room: RoomHandle,
    name: String,
}

impl TableHandle {
    pub(super) fn new(room: RoomHandle, name: String) -> Self {
        Self { room, name }
    }

    /// The name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The handle of the room of the table.
    pub fn room(&self) -> &RoomHandle {
        &self.room
    }

    /// The row of `key` in the replica, with every column of the schema of the replica, if the
    /// replica has one.
    ///
    /// It reads the replica, also if it fails closed, and sends nothing. A replica that is not
    /// open (closed, or empty after a discard until the room registers) is `RoomClosed`, and so
    /// is a read whose storage room is closed when it reaches it (a close, or the reopening after
    /// an error), or whose replica was discarded meanwhile. A table, a key or a value that are not
    /// of the schema are `InvalidOperation`, and a table with an encrypted column, `Crypto`.
    pub async fn get(&self, key: &Key) -> Result<Option<Row>, ClientError> {
        let read = self.plan(|schema, crypto| Read::get(schema, &self.name, key, crypto))?;
        Ok(self.read(read).await?.pop())
    }

    /// The rows of the replica that `scan` asks for, in its order, as [`get`](Self::get) reads
    /// them: with every column of the schema of the replica, or with those `scan` names (the
    /// table has each, once). A scan that gives an encrypted column is `Crypto`.
    pub async fn scan(&self, scan: &Scan) -> Result<Vec<Row>, ClientError> {
        let read = self.plan(|schema, crypto| Read::scan(schema, &self.name, scan, crypto))?;
        self.read(read).await
    }

    /// The read that `plan` makes against the schema of the replica, given whether the
    /// application has a `CryptoEngine`, with the epoch of the replica.
    fn plan(
        &self,
        plan: impl FnOnce(&Schema, bool) -> Result<Read, ClientError>,
    ) -> Result<(u64, Read), ClientError> {
        let client = self.room.client();
        let crypto = client.has_crypto();
        let room_id = self.room.room_id();
        let (epoch, read) = client
            .core()
            .read_schema(room_id, |schema| plan(schema, crypto))?;
        Ok((epoch, read?))
    }

    /// The rows of `read`, planned in `epoch`, from the storage room, without the applier:
    /// `RoomClosed` if the storage room is closed when the read reaches it (a close, or the
    /// reopening after an error), or if, once it read, the replica is no longer of that epoch (a
    /// discard, after which the storage room may hold another incarnation, with another schema).
    async fn read(&self, (epoch, read): (u64, Read)) -> Result<Vec<Row>, ClientError> {
        let (core, room_id) = (self.room.client().core(), self.room.room_id());
        let rows = read.fetch(core.storage(), room_id).await;
        if !core.is_of_epoch(room_id, epoch) {
            return Err(ClientError::RoomClosed(room_id.clone()));
        }
        match rows {
            Err(StorageError::RoomNotFound(_)) => Err(ClientError::RoomClosed(room_id.clone())),
            rows => Ok(rows?),
        }
    }
}

impl fmt::Debug for TableHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableHandle")
            .field("room_id", self.room.room_id())
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// A read of a table, planned against a schema: what it reads from the storage room, and the
/// names of the values of the rows it reads.
#[derive(Debug)]
struct Read {
    table_id: u16,
    /// The columns of the values of a row of the storage, in their order. A row with fewer
    /// values (a short row) has the others in `NULL`, and the values of one with more (written
    /// with a later version of the schema, which adds columns at the end) are left out.
    columns: Vec<String>,
    target: Target,
}

#[derive(Debug)]
enum Target {
    Key(PrimaryKey),
    Scan(ScanOptions),
}

impl Read {
    /// The read of the row of `key` in the table `name` of `schema`, with every column.
    fn get(schema: &Schema, name: &str, key: &Key, crypto: bool) -> Result<Self, ClientError> {
        let table = table(schema, name)?;
        let pk = primary_key(table, key)?;
        readable(table, None, crypto)?;
        let columns = table.columns().iter();
        Ok(Self {
            table_id: table.table_id(),
            columns: columns.map(|column| column.name().to_string()).collect(),
            target: Target::Key(pk),
        })
    }

    /// The read of the rows that `scan` asks for in the table `name` of `schema`.
    fn scan(schema: &Schema, name: &str, scan: &Scan, crypto: bool) -> Result<Self, ClientError> {
        let table = table(schema, name)?;
        let (columns, projection) = match &scan.columns {
            None => {
                let names = table
                    .columns()
                    .iter()
                    .map(|column| column.name().to_string());
                (names.collect(), None)
            }
            Some(names) => (names.clone(), Some(projection(table, names)?)),
        };
        let range = KeyRange {
            start: bound(table, &scan.start)?,
            end: bound(table, &scan.end)?,
        };
        readable(table, projection.as_deref(), crypto)?;
        let direction = match scan.direction {
            Direction::Forward => ScanDirection::Forward,
            Direction::Backward => ScanDirection::Backward,
        };
        let options = ScanOptions {
            range,
            direction,
            limit: scan.limit,
            projection,
        };
        Ok(Self {
            table_id: table.table_id(),
            columns,
            target: Target::Scan(options),
        })
    }

    /// The rows of the read in the storage room `room_id` of `storage`, by the names of their
    /// columns.
    async fn fetch(
        self,
        storage: &dyn StorageEngine,
        room_id: &RoomId,
    ) -> Result<Vec<Row>, StorageError> {
        let options = match self.target {
            Target::Key(pk) => {
                let row = storage.get_by_id(room_id, self.table_id, &pk).await?;
                return Ok(row
                    .map(|row| named(&self.columns, row))
                    .into_iter()
                    .collect());
            }
            Target::Scan(options) => options,
        };
        let mut stream = storage.scan_by_id(room_id, self.table_id, options).await?;
        let mut rows = Vec::new();
        while let Some(item) = stream.next().await {
            let (_, row) = item?;
            rows.push(named(&self.columns, row));
        }
        Ok(rows)
    }
}

fn invalid(message: String) -> ClientError {
    ClientError::InvalidOperation(message)
}

/// The table `name` of `schema`.
fn table<'a>(schema: &'a Schema, name: &str) -> Result<&'a TableSchema, ClientError> {
    let table = schema.get_table_by_name(name);
    table.ok_or_else(|| invalid(format!("The room has no table '{name}'")))
}

/// Checks that a read of `table` that gives the columns at `projection`, or every column, gives
/// no encrypted column.
fn readable(
    table: &TableSchema,
    projection: Option<&[u16]>,
    crypto: bool,
) -> Result<(), ClientError> {
    let columns = table.columns();
    let given = match projection {
        None => columns.iter().find(|column| column.is_encrypted()),
        Some(indices) => (indices.iter())
            .filter_map(|index| columns.get(usize::from(*index)))
            .find(|column| column.is_encrypted()),
    };
    match given {
        Some(column) => Err(encrypted(table, column, crypto)),
        None => Ok(()),
    }
}

/// The error of a read or a write that touches the encrypted `column` of `table`, which the
/// facade does not encrypt or decrypt.
fn encrypted(table: &TableSchema, column: &ColumnDef, crypto: bool) -> ClientError {
    let why = match crypto {
        true => "the client does not encrypt or decrypt columns yet",
        false => "the client has no CryptoEngine",
    };
    let (table, column) = (table.name(), column.name());
    ClientError::Crypto(format!(
        "The column '{column}' of '{table}' is encrypted, and {why}"
    ))
}

/// Checks that `value` can be in `column` of `table`: of the type of the column (the type of its
/// values in the clear, if it is encrypted), or `NULL` if the column takes it.
fn check_value(table: &TableSchema, column: &ColumnDef, value: &Value) -> Result<(), ClientError> {
    let (name, found) = (column.name(), value.data_type());
    match found {
        DataType::Null if column.is_nullable() => Ok(()),
        DataType::Null => Err(invalid(format!(
            "The column '{name}' of '{}' does not take NULL",
            table.name()
        ))),
        found if found == column.data_type() => Ok(()),
        found => Err(invalid(format!(
            "The column '{name}' of '{}' is of type {}, not {found}",
            table.name(),
            column.data_type()
        ))),
    }
}

/// The primary key of `table` that `key` names: `key` has every column of the primary key, each
/// with a value of its type, and no other column.
fn primary_key(table: &TableSchema, key: &Key) -> Result<PrimaryKey, ClientError> {
    let columns = table.primary_key();
    let missing = |name: &str| invalid(format!("The key of '{}' lacks '{name}'", table.name()));
    let mut values = Vec::with_capacity(columns.len());
    for name in columns {
        let value = key.get(name).ok_or_else(|| missing(name))?;
        let column = table.get_column(name).ok_or_else(|| missing(name))?;
        check_value(table, column, value)?;
        values.push(value.clone());
    }
    // Every column of the primary key is in the key, so a key with more holds another.
    if let Some((name, _)) = key.iter().find(|(name, _)| !columns.contains(name)) {
        return Err(invalid(format!(
            "The column '{name}' is not of the key of '{}'",
            table.name()
        )));
    }
    Ok(PrimaryKey::composite(values))
}

/// The bound of the keys of `table` that `bound` names.
fn bound(table: &TableSchema, bound: &Bound<Key>) -> Result<Bound<PrimaryKey>, ClientError> {
    Ok(match bound {
        Bound::Included(key) => Bound::Included(primary_key(table, key)?),
        Bound::Excluded(key) => Bound::Excluded(primary_key(table, key)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

/// The positions in `table` of the columns `names`, each a column of it, once.
fn projection(table: &TableSchema, names: &[String]) -> Result<Vec<u16>, ClientError> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let no_column = || {
            invalid(format!(
                "The table '{}' has no column '{name}'",
                table.name()
            ))
        };
        // A table has at most `u16::MAX` columns.
        let index = table.column_index(name).ok_or_else(no_column)?;
        let index = u16::try_from(index).map_err(|_| no_column())?;
        if indices.contains(&index) {
            return Err(invalid(format!(
                "The scan of '{}' asks for the column '{name}' twice",
                table.name()
            )));
        }
        indices.push(index);
    }
    Ok(indices)
}

/// The row whose values, in their order, are those of `columns`: a column without a value is
/// `NULL`, and a value without a column is left out.
fn named(columns: &[String], row: CompactRow) -> Row {
    let mut values = row.into_values().into_iter();
    let value = |column: &String| (column.clone(), values.next().unwrap_or(Value::Null));
    columns.iter().map(value).collect()
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
