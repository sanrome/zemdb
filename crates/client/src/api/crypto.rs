//! The encrypted columns of the facade (DC34): a write encrypts the value of each encrypted
//! column that it carries with the `CryptoEngine` of the application, `NULL` too, with the
//! additional data of its cell, and a read decrypts it.

use bincode::Options;
use serde::{Deserialize, Deserializer};
use zemdb_core::{
    ColumnDef, ColumnUpdate, CompactRow, CryptoEngine, CryptoError, DataType, Operation,
    OperationKind, PrimaryKey, RoomId, TableSchema, Value,
};

use crate::ClientError;

/// The version of the encoding of the additional data: their first byte.
const ADDITIONAL_DATA_VERSION: u8 = 1;

/// The encoding of the protocol, without the wire header: bincode with varints.
fn encoding() -> impl Options + Copy {
    bincode::DefaultOptions::new()
}

/// A cell of an encrypted column, as its additional data name it: the room, the table, the
/// primary key of the row and the position of the column.
#[derive(Debug, Clone, Copy)]
pub(super) struct Cell<'a> {
    pub(super) room_id: &'a RoomId,
    pub(super) table_id: u16,
    pub(super) pk: &'a PrimaryKey,
    pub(super) column_idx: u16,
}

impl Cell<'_> {
    /// The additional data of the cell (INV-K9): their version, and then the room id, the table
    /// id, the values of the primary key and the position of the column, in the encoding of the
    /// protocol, where a value has one encoding and each field holds its length, so two cells never
    /// share them. A `Float` of the key goes as `Value` compares it, with `-0.0` as `0.0`, so that
    /// the keys that name a row give the same data whatever their bits.
    fn additional_data(&self) -> Result<Vec<u8>, bincode::Error> {
        let pk: Vec<Value> = self.pk.iter().map(canonical).collect();
        let mut data = vec![ADDITIONAL_DATA_VERSION];
        let fields = (self.room_id.as_str(), self.table_id, &pk, self.column_idx);
        encoding().serialize_into(&mut data, &fields)?;
        Ok(data)
    }
}

/// `value`, with a `Float` canonical as `Value` compares it: `-0.0` as `0.0`.
fn canonical(value: &Value) -> Value {
    match value {
        Value::Float(float) => Value::from(*float),
        value => value.clone(),
    }
}

/// A write planned against the schema of the replica, by name and with its owner (steps 2 and 3
/// of DC32), with the values of its encrypted columns still in the clear, which
/// [`seal`](Self::seal) encrypts (step 5).
#[derive(Debug)]
pub(super) struct Draft {
    op: Operation,
    /// The name of its table.
    table: String,
    /// The encrypted columns whose value the operation carries, by their positions, with their
    /// names: every one of its table in an insert, since `NULL` is encrypted too, and those of
    /// the changes in an update.
    sealed: Vec<(u16, String)>,
}

impl Draft {
    /// The draft of `op`, a write into `table`.
    pub(super) fn new(op: Operation, table: &TableSchema) -> Self {
        let columns = table.columns();
        let encrypted = |index: u16| {
            let column = columns.get(usize::from(index))?;
            column
                .is_encrypted()
                .then(|| (index, column.name().to_string()))
        };
        let sealed = match op.kind() {
            OperationKind::Insert { .. } => (0..columns.len())
                .filter_map(|index| u16::try_from(index).ok())
                .filter_map(encrypted)
                .collect(),
            OperationKind::Update { updates } => (updates.iter())
                .filter_map(|update| encrypted(update.column_idx()))
                .collect(),
            OperationKind::Delete { .. } => Vec::new(),
        };
        Self {
            op,
            table: table.name().to_string(),
            sealed,
        }
    }

    /// The operation that is sent, with the value of each encrypted column it carries encrypted
    /// with `crypto` for its cell in `room_id`. `Crypto` if it carries one and the application has
    /// no `CryptoEngine`, or if `crypto` fails to encrypt one.
    pub(super) async fn seal(
        self,
        crypto: Option<&dyn CryptoEngine>,
        room_id: &RoomId,
    ) -> Result<Operation, ClientError> {
        let Some((_, first)) = self.sealed.first() else {
            return Ok(self.op);
        };
        let Some(crypto) = crypto else {
            return Err(no_engine(&self.table, first));
        };
        let (table_id, timestamp) = (self.op.table_id(), self.op.timestamp());
        let (pk, kind) = self.op.into_pk_and_kind();
        let cell = |column_idx| Cell {
            room_id,
            table_id,
            pk: &pk,
            column_idx,
        };
        let kind = match kind {
            OperationKind::Insert { row } => {
                let mut values = row.into_values();
                for (index, name) in &self.sealed {
                    if let Some(value) = values.get_mut(usize::from(*index)) {
                        let column = (self.table.as_str(), name.as_str());
                        *value = encrypt(crypto, cell(*index), value, column).await?;
                    }
                }
                OperationKind::Insert {
                    row: CompactRow::new(values),
                }
            }
            OperationKind::Update { updates } => {
                let mut sealed = Vec::with_capacity(updates.len());
                for update in updates {
                    let index = update.column_idx();
                    let name = self.sealed.iter().find(|(sealed, _)| *sealed == index);
                    let update = match name {
                        Some((_, name)) => {
                            let column = (self.table.as_str(), name.as_str());
                            let value = encrypt(crypto, cell(index), update.value(), column);
                            ColumnUpdate::new(index, value.await?)
                        }
                        None => update,
                    };
                    sealed.push(update);
                }
                OperationKind::Update { updates: sealed }
            }
            delete => delete,
        };
        Ok(Operation::new(table_id, pk, timestamp, kind))
    }
}

/// `value`, of the encrypted `column` of `table`, encrypted with `crypto` for `cell`: the value
/// with its type in the encoding of the protocol, `NULL` too, as the `Bytes` that the server takes
/// in an encrypted column.
async fn encrypt(
    crypto: &dyn CryptoEngine,
    cell: Cell<'_>,
    value: &Value,
    (table, column): (&str, &str),
) -> Result<Value, ClientError> {
    let failed = |why: &str| {
        ClientError::Crypto(format!(
            "The column '{column}' of '{table}' was not encrypted: {why}"
        ))
    };
    let data = cell.additional_data();
    let data = data.map_err(|_| failed("its additional data do not encode"))?;
    let plaintext = encoding().serialize(value);
    let plaintext = plaintext.map_err(|_| failed("its value does not encode"))?;
    let ciphertext = crypto.encrypt(cell.room_id, &data, &plaintext).await;
    let ciphertext = ciphertext.map_err(|err| failed(engine_failure(&err)))?;
    Ok(Value::from(ciphertext))
}

/// The value of the encrypted `column` of `table` that the storage holds in `cell`, `stored`:
/// `NULL` as it is (a short row, or a cell left in `NULL`), and a ciphertext decrypted with
/// `crypto` for the cell and decoded as one value of the type of the column ([`decoded`]).
/// Anything else is `Decryption`, never a value made up.
pub(super) async fn decrypt(
    crypto: &dyn CryptoEngine,
    cell: Cell<'_>,
    stored: Value,
    table: &str,
    column: &ColumnDef,
) -> Result<Value, ClientError> {
    let failed = |why: &str| {
        let name = column.name();
        ClientError::Decryption(format!("The column '{name}' of '{table}' {why}"))
    };
    let ciphertext = match stored {
        Value::Null => return Ok(Value::Null),
        Value::Bytes(ciphertext) => ciphertext,
        _ => return Err(failed("holds no ciphertext")),
    };
    let data = cell.additional_data();
    let data = data.map_err(|_| failed("does not decrypt: its additional data do not encode"))?;
    let plaintext = crypto.decrypt(cell.room_id, &data, &ciphertext).await;
    let plaintext = plaintext.map_err(|err| {
        failed(&format!(
            "does not decrypt with its additional data: {}",
            engine_failure(&err)
        ))
    })?;
    decoded(&plaintext, column).ok_or_else(|| {
        failed(&format!(
            "does not decrypt to a value of its type, {}",
            column.data_type()
        ))
    })
}

/// What the `CryptoEngine` failed at, without its message: the engine is of the application, and
/// its message may hold anything.
fn engine_failure(err: &CryptoError) -> &'static str {
    match err {
        CryptoError::Encryption(_) => "the CryptoEngine failed to encrypt",
        CryptoError::Decryption(_) => "the CryptoEngine failed to decrypt",
        CryptoError::KeyDerivation(_) => "the CryptoEngine failed to derive the key",
    }
}

/// The value that `plaintext` holds for `column`: one value in the encoding of the protocol,
/// decoded within the length of `plaintext` (never a reservation larger than what came) and
/// without bytes left over, of the type of the column, or `NULL` if the column takes it.
fn decoded(plaintext: &[u8], column: &ColumnDef) -> Option<Value> {
    let limit = u64::try_from(plaintext.len()).ok()?;
    let options = encoding().with_limit(limit).reject_trailing_bytes();
    // A `Bytes` decodes as a sequence, whose decode reserves the length it declares (up to a
    // bound of serde) before it reads its bytes: its shape, first, checks that they are there.
    options.deserialize::<Shape>(plaintext).ok()?;
    let value: Value = options.deserialize(plaintext).ok()?;
    let typed = match value.data_type() {
        DataType::Null => column.is_nullable(),
        found => found == column.data_type(),
    };
    typed.then_some(value)
}

/// The shape of a [`Value`] in the encoding of the protocol: its variants, in its order, with the
/// bytes of a `Bytes` read one at a time into nothing, so that its decode reserves nothing for
/// them.
#[derive(Deserialize)]
#[expect(
    dead_code,
    reason = "its payloads are decoded only to check that they are there"
)]
enum Shape {
    Null,
    Int(i64),
    Float(f64),
    Timestamp(i64),
    String(Box<str>),
    Bool(bool),
    Bytes(Vec<Byte>),
    Uuid([u8; 16]),
}

/// A byte of a `Bytes`, read and dropped: a vector of them takes no memory.
struct Byte;

impl<'de> Deserialize<'de> for Byte {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u8::deserialize(deserializer).map(|_| Self)
    }
}

/// The error of a write that touches the encrypted `column` of `table`, or of a read that gives
/// it, without a `CryptoEngine`.
pub(super) fn no_engine(table: &str, column: &str) -> ClientError {
    ClientError::Crypto(format!(
        "The column '{column}' of '{table}' is encrypted, and the client has no CryptoEngine"
    ))
}

#[cfg(test)]
#[path = "tests/crypto.rs"]
mod tests;
