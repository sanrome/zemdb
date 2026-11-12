use futures::StreamExt;
use std::fmt;
use std::ops::Bound;
use zemdb_core::{
    ColumnDef, CompactRow, CryptoEngine, DataType, Operation, PrimaryKey, RoomId, Schema,
    TableSchema, Value,
};
use zemdb_storage::{KeyRange, ScanDirection, ScanOptions, StorageEngine, StorageError};

use super::crypto::{decrypt, no_engine, Cell, Draft};
use super::write::{bounded, checked, fitting, prechecked, Writer};
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
    /// of the schema are `InvalidOperation`.
    ///
    /// The value of an encrypted column is decrypted with the `CryptoEngine` of the application,
    /// with the additional data of its cell, and has the type of its column; a `NULL` that the
    /// replica holds (a row written before the column was added) is `NULL`. A table with an
    /// encrypted column is `Crypto` without a `CryptoEngine`, and a value that does not decrypt
    /// with the additional data of its cell, or whose plaintext is not one value of the type of its
    /// column, is `Decryption`: the read gives no row.
    pub async fn get(&self, key: &Key) -> Result<Option<Row>, ClientError> {
        let read = self.plan(|schema, crypto| Read::get(schema, &self.name, key, crypto))?;
        Ok(self.read(read).await?.pop())
    }

    /// The rows of the replica that `scan` asks for, in its order, as [`get`](Self::get) reads
    /// them: with every column of the schema of the replica, or with those `scan` names (the
    /// table has each, once). A value of an encrypted column that does not decrypt fails the whole
    /// scan (`Decryption`); a scan that does not name that column reads the others.
    pub async fn scan(&self, scan: &Scan) -> Result<Vec<Row>, ClientError> {
        let read = self.plan(|schema, crypto| Read::scan(schema, &self.name, scan, crypto))?;
        self.read(read).await
    }

    /// Inserts `row`, the `Insert` of the protocol: a row with its key already in the table is
    /// replaced whole, except in an immutable table, where the replica rejects it (`Duplicate`).
    /// Returns the key of the row, with the owner column, as reads take it.
    ///
    /// `row` has a value of its type for each column of the table (the type of its values in the
    /// clear, in an encrypted column), or lacks only columns that take `NULL`, which they get, and
    /// does not have the owner column, which the client fills with its owner
    /// ([`ClientId::owner`](zemdb_core::ClientId::owner)). Otherwise, the insert is
    /// `InvalidOperation`, and it sends nothing. The value of each encrypted column of the table,
    /// `NULL` too, is sent encrypted with the `CryptoEngine` of the application, with the
    /// additional data of its cell: without a `CryptoEngine`, or if it fails to encrypt one, the
    /// insert is `Crypto`, and sends nothing. With the precheck (`ClientConfig::precheck`, on by
    /// default), an insert that the replica would reject in its state is `PrecheckFailed`, with
    /// the reason, and is not sent either: an advice, since the replica may be behind the log.
    /// Then the client writes it
    /// once ([`Client::write`](crate::Client::write)): it returns once the replica decided on its
    /// sequence (`ConstraintViolation` if it rejects it), and is never repeated, so
    /// `OutcomeUnknown` is an outcome that reaches the replica with the log.
    ///
    /// A replica that is not open is `RoomClosed`, and so is one discarded before the insert is
    /// sent, which may have another schema; one that fails closed is `FailedClosed`, without the
    /// precheck.
    pub async fn insert(&self, row: &Row) -> Result<Key, ClientError> {
        bounded(row.iter().map(|(_, value)| value))?;
        let (epoch, (draft, key)) =
            self.plan_write(|writer, schema| writer.insert(schema, &self.name, row))?;
        self.write(epoch, draft).await?;
        Ok(key)
    }

    /// Updates the row of `key` with `changes`, as [`insert`](Self::insert) writes: only the
    /// columns that change, at least one, none of the key nor the owner column.
    ///
    /// `key` names the row as a read does, but it may lack the owner column, if it is of the
    /// key, which the client fills, and if it has it, it has the owner of the client: the client
    /// only writes its own rows, and the replica rejects an update of the row of another
    /// (`NotOwner`), or of one that is not there (`RowMissing`), and so does the precheck, if the
    /// replica has the row of another or lacks it (`PrecheckFailed`). A change of an encrypted
    /// column, `NULL` too, is encrypted as an insert encrypts it.
    pub async fn update(&self, key: &Key, changes: &Row) -> Result<(), ClientError> {
        bounded(changes.iter().map(|(_, value)| value))?;
        let (epoch, draft) =
            self.plan_write(|writer, schema| writer.update(schema, &self.name, key, changes))?;
        self.write(epoch, draft).await
    }

    /// Deletes the row of `key`, named as [`update`](Self::update) names it, as
    /// [`insert`](Self::insert) writes, also with the precheck. The delete of a row that is not
    /// there changes nothing, and the precheck lets it out.
    pub async fn delete(&self, key: &Key) -> Result<(), ClientError> {
        let (epoch, draft) =
            self.plan_write(|writer, schema| writer.delete(schema, &self.name, key))?;
        self.write(epoch, draft).await
    }

    /// The write that `plan` makes against the schema of the replica, with the writer of the
    /// client, and the epoch of the replica.
    fn plan_write<T>(
        &self,
        plan: impl FnOnce(&Writer, &Schema) -> Result<T, ClientError>,
    ) -> Result<(u64, T), ClientError> {
        let core = self.room.client().core();
        let writer = Writer::new(&core.client_id());
        let room_id = self.room.room_id();
        let (epoch, write) = core.read_schema(room_id, |schema| plan(&writer, schema))?;
        Ok((epoch, write?))
    }

    /// Writes `draft`, planned in `epoch`, in the order of DC32 from its step 5: with its
    /// encrypted columns encrypted, if the server takes it from the client, if it fits and, with
    /// the precheck, if the replica would take it; once, to the replica of that epoch.
    async fn write(&self, epoch: u64, draft: Draft) -> Result<(), ClientError> {
        let (client, room_id) = (self.room.client(), self.room.room_id());
        let core = client.core();
        let op = draft.seal(client.crypto(), room_id).await?;
        let op = self.checked(epoch, op)?;
        let op = fitting(op, &core.client_id(), room_id)?;
        if client.prechecks() {
            self.precheck(epoch, &op).await?;
        }
        core.write_in(room_id.clone(), op, Some(epoch)).await
    }

    /// `op`, as it is sent, if the server takes it from the client ([`checked`]), against the
    /// schema of the replica of `epoch`, in which it was planned: `RoomClosed` if the replica is
    /// no longer of that epoch (a discard while it was encrypted), whose schema may be another. In
    /// that epoch the schema may have grown since the plan, and the operation is valid all the
    /// same: the schema only grows.
    fn checked(&self, epoch: u64, op: Operation) -> Result<Operation, ClientError> {
        let (core, room_id) = (self.room.client().core(), self.room.room_id());
        let client_id = core.client_id();
        let (now, op) = core.read_schema(room_id, |schema| checked(schema, op, &client_id))?;
        if now != epoch {
            return Err(ClientError::RoomClosed(room_id.clone()));
        }
        op
    }

    /// The precheck of `op`, planned in `epoch` (DC33): what the replica would decide on it in
    /// the state its storage room publishes, without the applier, as [`prechecked`] takes it.
    /// `RoomClosed` if, once it checked, the replica is no longer of that epoch, as after a read.
    /// A replica that fails closed, then, leaves it without a decision: its state no longer
    /// catches up, and its writes are `FailedClosed`, which the write gives without sending.
    async fn precheck(&self, epoch: u64, op: &Operation) -> Result<(), ClientError> {
        let (core, room_id) = (self.room.client().core(), self.room.room_id());
        let checked = core.storage().check_operation(room_id, op).await;
        if !core.is_of_epoch(room_id, epoch) {
            return Err(ClientError::RoomClosed(room_id.clone()));
        }
        if core.fails_closed(room_id) {
            return Ok(());
        }
        prechecked(checked, room_id)
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
    /// Then its encrypted columns are decrypted, with the `CryptoEngine` of the application.
    async fn read(&self, (epoch, read): (u64, Read)) -> Result<Vec<Row>, ClientError> {
        let client = self.room.client();
        let (core, room_id) = (client.core(), self.room.room_id());
        let fetched = read.fetch(core.storage(), room_id).await;
        if !core.is_of_epoch(room_id, epoch) {
            return Err(ClientError::RoomClosed(room_id.clone()));
        }
        let fetched = match fetched {
            Err(StorageError::RoomNotFound(_)) => {
                return Err(ClientError::RoomClosed(room_id.clone()))
            }
            fetched => fetched?,
        };
        read.rows(fetched, client.crypto(), room_id).await
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
    /// The name of the table.
    table: String,
    table_id: u16,
    /// The columns of the values of a row of the storage, in their order. A row with fewer
    /// values (a short row) has the others in `NULL`, and the values of one with more (written
    /// with a later version of the schema, which adds columns at the end) are left out.
    columns: Vec<String>,
    /// The encrypted columns among `columns`: their place among the values of a row of the
    /// read, their position in the table and their definition.
    sealed: Vec<(usize, u16, ColumnDef)>,
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
        let pk = primary_key(table, key, None)?;
        let sealed = sealed(table, None, crypto)?;
        let columns = table.columns().iter();
        Ok(Self {
            table: table.name().to_string(),
            table_id: table.table_id(),
            columns: columns.map(|column| column.name().to_string()).collect(),
            sealed,
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
        let sealed = sealed(table, projection.as_deref(), crypto)?;
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
            table: table.name().to_string(),
            table_id: table.table_id(),
            columns,
            sealed,
            target: Target::Scan(options),
        })
    }

    /// The rows of the read in the storage room `room_id` of `storage`, with their primary keys,
    /// as the storage holds them.
    async fn fetch(
        &self,
        storage: &dyn StorageEngine,
        room_id: &RoomId,
    ) -> Result<Vec<(PrimaryKey, CompactRow)>, StorageError> {
        let options = match &self.target {
            Target::Key(pk) => {
                let row = storage.get_by_id(room_id, self.table_id, pk).await?;
                return Ok(row.map(|row| (pk.clone(), row)).into_iter().collect());
            }
            Target::Scan(options) => options.clone(),
        };
        let mut stream = storage.scan_by_id(room_id, self.table_id, options).await?;
        let mut rows = Vec::new();
        while let Some(item) = stream.next().await {
            rows.push(item?);
        }
        Ok(rows)
    }

    /// The rows that the read fetched from the storage room `room_id`, by the names of their
    /// columns, with the value of each encrypted column decrypted with `crypto` for its cell.
    async fn rows(
        &self,
        fetched: Vec<(PrimaryKey, CompactRow)>,
        crypto: Option<&dyn CryptoEngine>,
        room_id: &RoomId,
    ) -> Result<Vec<Row>, ClientError> {
        let mut rows = Vec::with_capacity(fetched.len());
        for (pk, row) in fetched {
            let mut values = row.into_values();
            for (place, index, column) in &self.sealed {
                // A short row lacks the column, which reads `NULL`.
                let Some(value) = values.get_mut(*place) else {
                    continue;
                };
                let crypto = crypto.ok_or_else(|| no_engine(&self.table, column.name()))?;
                let cell = Cell {
                    room_id,
                    table_id: self.table_id,
                    pk: &pk,
                    column_idx: *index,
                };
                let stored = std::mem::replace(value, Value::Null);
                *value = decrypt(crypto, cell, stored, &self.table, column).await?;
            }
            rows.push(named(&self.columns, CompactRow::new(values)));
        }
        Ok(rows)
    }
}

pub(super) fn invalid(message: String) -> ClientError {
    ClientError::InvalidOperation(message)
}

/// The table `name` of `schema`.
pub(super) fn table<'a>(schema: &'a Schema, name: &str) -> Result<&'a TableSchema, ClientError> {
    let table = schema.get_table_by_name(name);
    table.ok_or_else(|| invalid(format!("The room has no table '{name}'")))
}

/// The encrypted columns that a read of `table` gives, of those at `projection` or of every
/// column, with their place among the values of a row it reads, their position in the table and
/// their definition; `Crypto` if it gives one and the application has no `CryptoEngine`.
fn sealed(
    table: &TableSchema,
    projection: Option<&[u16]>,
    crypto: bool,
) -> Result<Vec<(usize, u16, ColumnDef)>, ClientError> {
    let columns = table.columns();
    // A table has at most `u16::MAX` columns.
    let every: Vec<u16>;
    let indices = match projection {
        Some(indices) => indices,
        None => {
            every = (0..columns.len())
                .filter_map(|index| u16::try_from(index).ok())
                .collect();
            &every
        }
    };
    let sealed: Vec<_> = (indices.iter().enumerate())
        .filter_map(|(place, index)| {
            let column = columns.get(usize::from(*index))?;
            column
                .is_encrypted()
                .then(|| (place, *index, column.clone()))
        })
        .collect();
    match sealed.first() {
        Some((_, _, column)) if !crypto => Err(no_engine(table.name(), column.name())),
        _ => Ok(sealed),
    }
}

/// Checks that `value` can be in `column` of `table`: of the type of the column (the type of its
/// values in the clear, if it is encrypted), or `NULL` if the column takes it.
pub(super) fn check_value(
    table: &TableSchema,
    column: &ColumnDef,
    value: &Value,
) -> Result<(), ClientError> {
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
/// with a value of its type, and no other column. With `owner`, the owner column and the owner of
/// the client in a write, `key` may lack that column, which then has the owner, and if it has
/// it, it has that owner.
pub(super) fn primary_key(
    table: &TableSchema,
    key: &Key,
    owner: Option<(&str, &Value)>,
) -> Result<PrimaryKey, ClientError> {
    let columns = table.primary_key();
    let missing = |name: &str| invalid(format!("The key of '{}' lacks '{name}'", table.name()));
    let mut values = Vec::with_capacity(columns.len());
    for name in columns {
        let column = table.get_column(name).ok_or_else(|| missing(name))?;
        let owner = owner
            .filter(|(owner, _)| owner == name)
            .map(|(_, owner)| owner);
        let value = match (key.get(name), owner) {
            (Some(value), owner) => {
                check_value(table, column, value)?;
                if owner.is_some_and(|owner| owner != value) {
                    return Err(invalid(format!(
                        "The key of '{}' has another owner than the client in '{name}'",
                        table.name()
                    )));
                }
                value
            }
            (None, Some(owner)) => owner,
            (None, None) => return Err(missing(name)),
        };
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
        Bound::Included(key) => Bound::Included(primary_key(table, key, None)?),
        Bound::Excluded(key) => Bound::Excluded(primary_key(table, key, None)?),
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
