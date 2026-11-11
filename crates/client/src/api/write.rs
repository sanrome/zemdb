use std::time::{SystemTime, UNIX_EPOCH};
use zemdb_core::protocol::codec::encoded_len;
use zemdb_core::{
    check_operation_size, ClientId, ClientMessage, ColumnDef, ColumnUpdate, CompactRow,
    CorrelationId, MutationId, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value, MAX_MESSAGE_SIZE,
};

use super::table::{check_value, encrypted, invalid, primary_key, table};
use super::{Key, Row};
use crate::ClientError;

/// What the writes of a client are planned with against the schema of the replica: the client,
/// whose owner fills the owner column of each write (DC33), whether the application has a
/// `CryptoEngine`, and the timestamp of the write.
pub(super) struct Writer<'a> {
    client: &'a ClientId,
    /// The owner of the client, as the owner column holds it.
    owner: Value,
    crypto: bool,
    /// The wall clock, in milliseconds: informative, since the order is that of the sequences.
    timestamp: u64,
}

impl<'a> Writer<'a> {
    pub(super) fn new(client: &'a ClientId, crypto: bool) -> Self {
        let since = SystemTime::now().duration_since(UNIX_EPOCH);
        let millis = since.map_or(0, |since| since.as_millis());
        Self {
            client,
            owner: Value::from(client.owner()),
            crypto,
            timestamp: u64::try_from(millis).unwrap_or(u64::MAX),
        }
    }

    /// The insert of `row` into the table `name` of `schema`, and the key of the row, with the
    /// owner column: `row` has a value of its type for each column of the table but the owner
    /// column, which the client fills, and lacks only columns that take `NULL`, which they get.
    pub(super) fn insert(
        &self,
        schema: &Schema,
        name: &str,
        row: &Row,
    ) -> Result<(Operation, Key), ClientError> {
        let table = table(schema, name)?;
        if let Some((name, _)) = row
            .iter()
            .find(|(name, _)| table.get_column(name).is_none())
        {
            return Err(no_column(table, name));
        }
        let owner = owner_column(table);
        if let Some(owner) = owner.filter(|owner| row.get(owner.name()).is_some()) {
            return Err(owner_given(table, owner));
        }
        let mut values = Vec::with_capacity(table.columns().len());
        for column in table.columns() {
            let value = match row.get(column.name()) {
                Some(value) => {
                    check_value(table, column, value)?;
                    value.clone()
                }
                None if owner.is_some_and(|owner| owner.name() == column.name()) => {
                    self.owner.clone()
                }
                None if column.is_nullable() => Value::Null,
                None => {
                    return Err(invalid(format!(
                        "The row of '{}' lacks the column '{}', which does not take NULL",
                        table.name(),
                        column.name()
                    )))
                }
            };
            values.push(value);
        }
        // Until the client encrypts, every insert into a table with an encrypted column touches
        // it: NULL is encrypted too.
        if let Some(column) = table.columns().iter().find(|column| column.is_encrypted()) {
            return Err(encrypted(table, column, self.crypto));
        }
        let pk: Vec<_> = (table.primary_key().iter())
            .filter_map(|name| values.get(table.column_index(name)?).cloned())
            .collect();
        let key = (table.primary_key().iter())
            .zip(pk.iter().cloned())
            .collect();
        let pk = PrimaryKey::composite(pk);
        let op = Operation::insert(
            table.table_id(),
            pk,
            CompactRow::new(values),
            self.timestamp,
        );
        Ok((self.checked(schema, op)?, key))
    }

    /// The update of the row of `key` in the table `name` of `schema` with `changes`: at least a
    /// column of the table, none of the key nor the owner column, each with a value of its type.
    pub(super) fn update(
        &self,
        schema: &Schema,
        name: &str,
        key: &Key,
        changes: &Row,
    ) -> Result<Operation, ClientError> {
        let table = table(schema, name)?;
        let pk = self.key(table, key)?;
        if changes.is_empty() {
            return Err(invalid(format!(
                "The update of '{}' changes no column",
                table.name()
            )));
        }
        let owner = owner_column(table);
        let mut updates = Vec::with_capacity(changes.len() + 1);
        let mut encrypted_column = None;
        for (name, value) in changes {
            let (index, column) = column(table, name)?;
            if owner.is_some_and(|owner| owner.name() == name) {
                return Err(owner_given(table, column));
            }
            if table.primary_key().contains(name) {
                return Err(invalid(format!(
                    "The column '{name}' is of the key of '{}', which an update does not change",
                    table.name()
                )));
            }
            check_value(table, column, value)?;
            if column.is_encrypted() {
                encrypted_column.get_or_insert(column);
            }
            updates.push(ColumnUpdate::new(index, value.clone()));
        }
        // Until the client encrypts, an update of an encrypted column touches it, also with NULL.
        if let Some(column) = encrypted_column {
            return Err(encrypted(table, column, self.crypto));
        }
        // An owner column outside the key goes as one more delta, which the operation needs.
        if let Some(index) = owner_outside_key(table) {
            updates.push(ColumnUpdate::new(index, self.owner.clone()));
        }
        let op = Operation::update(table.table_id(), pk, updates, self.timestamp);
        self.checked(schema, op)
    }

    /// The delete of the row of `key` in the table `name` of `schema`.
    pub(super) fn delete(
        &self,
        schema: &Schema,
        name: &str,
        key: &Key,
    ) -> Result<Operation, ClientError> {
        let table = table(schema, name)?;
        let pk = self.key(table, key)?;
        let (table_id, timestamp) = (table.table_id(), self.timestamp);
        // An owner column outside the key goes as the owner of the delete, which needs it.
        let op = match owner_outside_key(table) {
            Some(_) => Operation::delete_with_owner(table_id, pk, self.owner.clone(), timestamp),
            None => Operation::delete(table_id, pk, timestamp),
        };
        self.checked(schema, op)
    }

    /// The primary key of `table` that the key of a write names, as a read names it, but for the
    /// owner column, if it is of the key: the key may lack it, and the client fills it, or have it
    /// with the owner of the client.
    fn key(&self, table: &TableSchema, key: &Key) -> Result<PrimaryKey, ClientError> {
        let owner = owner_column(table).map(|owner| (owner.name(), &self.owner));
        primary_key(table, key, owner)
    }

    /// `op` if the server takes it from the client: valid for `schema`, with the owner of the
    /// client if its table has one (INV-A5).
    fn checked(&self, schema: &Schema, op: Operation) -> Result<Operation, ClientError> {
        (schema.validate_operation(&op))
            .and_then(|()| schema.check_owner(&op, self.client))
            .map_err(|err| invalid(err.to_string()))?;
        Ok(op)
    }
}

/// `op`, a write of `client` in `room_id`, if it fits: alone in a record of the log and in a
/// response, as the server checks it, with the sequence of the worst case and a mutation id, which
/// takes the same whatever it is; and in the `Commit` that carries it, with the ids of the client
/// and the room and the correlation id and the cursor of the worst case, within the limit of a
/// message.
pub(super) fn fitting(
    op: Operation,
    client: &ClientId,
    room_id: &RoomId,
) -> Result<Operation, ClientError> {
    let too_large = |why: String| invalid(format!("The operation is too large: {why}"));
    let op = SequencedOperation::new(SequenceNumber::new(u64::MAX), op);
    let fits = check_operation_size(&op, Some(MutationId::from(0)));
    fits.map_err(|err| too_large(err.to_string()))?;
    let commit = ClientMessage::Commit {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: room_id.clone(),
        client_id: client.clone(),
        mutation_id: MutationId::from(0),
        last_ack_seq: SequenceNumber::new(u64::MAX),
        op: op.op,
    };
    let size = encoded_len(&commit).map_err(|err| too_large(err.to_string()))?;
    let ClientMessage::Commit { op, .. } = commit else {
        return Err(too_large("its commit cannot be built".to_string()));
    };
    if size > MAX_MESSAGE_SIZE {
        return Err(too_large(format!(
            "its commit encodes to {size} bytes; the maximum is {MAX_MESSAGE_SIZE} bytes"
        )));
    }
    Ok(op)
}

/// Checks that `values`, the values of a row or of the changes of an update, encoded, do not take
/// more than a message: the operation with them takes at least that, so it would not fit. It runs
/// before the write is planned, so that the plan does not copy them under the lock of the room.
pub(super) fn bounded<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<(), ClientError> {
    let mut size = 0_u64;
    for value in values {
        size = size.saturating_add(encoded_len(value).unwrap_or(u64::MAX));
        if size > MAX_MESSAGE_SIZE {
            return Err(invalid(format!(
                "The operation is too large: its values encode to more than {MAX_MESSAGE_SIZE} \
                 bytes"
            )));
        }
    }
    Ok(())
}

/// The owner column of `table`, if it has one.
fn owner_column(table: &TableSchema) -> Option<&ColumnDef> {
    table.columns().get(table.owner_column_index()?)
}

/// The position of the owner column of `table`, if it has one outside its primary key.
fn owner_outside_key(table: &TableSchema) -> Option<u16> {
    let index = table.owner_column_index()?;
    let column = table.columns().get(index)?;
    match table.primary_key().iter().any(|key| key == column.name()) {
        true => None,
        // A table has at most `u16::MAX` columns.
        false => u16::try_from(index).ok(),
    }
}

/// The column `name` of `table`, with its position.
fn column<'a>(table: &'a TableSchema, name: &str) -> Result<(u16, &'a ColumnDef), ClientError> {
    let index = table
        .column_index(name)
        .ok_or_else(|| no_column(table, name))?;
    let column = table.columns().get(index);
    // A table has at most `u16::MAX` columns.
    let index = u16::try_from(index).ok();
    index.zip(column).ok_or_else(|| no_column(table, name))
}

fn no_column(table: &TableSchema, name: &str) -> ClientError {
    invalid(format!(
        "The table '{}' has no column '{name}'",
        table.name()
    ))
}

/// The error of a write that gives the owner column of `table`, which the client fills.
fn owner_given(table: &TableSchema, owner: &ColumnDef) -> ClientError {
    invalid(format!(
        "The column '{}' of '{}' is its owner, which the client fills",
        owner.name(),
        table.name()
    ))
}

#[cfg(test)]
#[path = "tests/write.rs"]
mod tests;
