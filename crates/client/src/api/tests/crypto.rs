use super::*;
use crate::api::write::{checked, Writer};
use crate::api::{Key, Row};
use async_trait::async_trait;
use std::sync::Mutex;
use zemdb_core::{ClientId, NoOpCryptoEngine, OwnerIdentity, Schema};

fn room_id() -> RoomId {
    RoomId::new("room-a").unwrap()
}

fn client() -> ClientId {
    ClientId::new("ana#phone").unwrap()
}

fn cell<'a>(room_id: &'a RoomId, table_id: u16, pk: &'a PrimaryKey, column_idx: u16) -> Cell<'a> {
    Cell {
        room_id,
        table_id,
        pk,
        column_idx,
    }
}

/// Table `rounds`, keyed by an int `round_id`, with a string `title`, the nullable encrypted
/// string `notes` and the encrypted int `rating`; and `entries`, keyed by an int `entry_id`, with
/// its owner `author` outside the key and the encrypted string `body`.
fn schema() -> Schema {
    let rounds = TableSchema::builder("rounds")
        .primary_key("round_id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String)
        .encrypted_column("rating", DataType::Int);
    let entries = TableSchema::builder("entries")
        .primary_key("entry_id", DataType::Int)
        .column("author", DataType::String)
        .encrypted_column("body", DataType::String)
        .owner("author", OwnerIdentity::ClientId);
    Schema::builder().table(rounds).table(entries).build()
}

/// A column `notes` of `data_type`, encrypted, that takes `NULL` or not.
fn column(data_type: DataType, nullable: bool) -> ColumnDef {
    ColumnDef::new("notes", data_type)
        .nullable(nullable)
        .encrypted(true)
}

fn encoded(value: &Value) -> Vec<u8> {
    encoding().serialize(value).unwrap()
}

/// A value of each type but `NULL`.
fn samples() -> Vec<Value> {
    vec![
        Value::Int(-7),
        Value::Float(1.5),
        Value::Timestamp(1_700_000_000_000),
        Value::from("secret"),
        Value::from(""),
        Value::Bool(true),
        Value::from(vec![0_u8, 1, 255]),
        Value::from(Vec::<u8>::new()),
        Value::Uuid([7; 16]),
    ]
}

const TYPES: [DataType; 7] = [
    DataType::Int,
    DataType::Float,
    DataType::Timestamp,
    DataType::String,
    DataType::Bool,
    DataType::Bytes,
    DataType::Uuid,
];

#[test]
fn the_additional_data_are_versioned_and_name_the_cell() {
    let room = room_id();
    let pk = PrimaryKey::single(1);
    // The version, the room id with its length, the table id, the values of the key with their
    // count (an `Int`, in zigzag), and the position of the column.
    let data = cell(&room, 0, &pk, 2).additional_data().unwrap();
    let expected = [1, 6, b'r', b'o', b'o', b'm', b'-', b'a', 0, 1, 1, 2, 2];
    assert_eq!(data, expected);

    // Each field tells two cells apart, also where a field would run into the next.
    let (other_room, long_room) = (
        RoomId::new("room-b").unwrap(),
        RoomId::new("room-a0").unwrap(),
    );
    let keys = [
        PrimaryKey::single(1),
        PrimaryKey::single(2),
        PrimaryKey::single("1"),
        PrimaryKey::single(Value::Timestamp(1)),
        PrimaryKey::composite([Value::from(1), Value::from("x")]),
        PrimaryKey::composite([Value::from("x"), Value::from(1)]),
    ];
    let mut all = Vec::new();
    for room in [&room, &other_room, &long_room] {
        for table_id in [0, 1] {
            for pk in &keys {
                for column_idx in [0, 2, 256] {
                    all.push(
                        cell(room, table_id, pk, column_idx)
                            .additional_data()
                            .unwrap(),
                    );
                }
            }
        }
    }
    let count = all.len();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), count);

    // A `Float` of the key goes as `Value` compares it: `-0.0` as `0.0`, wherever it is.
    for (zero, negative) in [
        (Value::Float(0.0), Value::Float(-0.0)),
        (
            Value::Float(0.0),
            Value::Float(f64::from_bits((-0.0_f64).to_bits())),
        ),
    ] {
        let key = |float: &Value| PrimaryKey::composite([Value::from(1), float.clone()]);
        let (zero, negative) = (key(&zero), key(&negative));
        assert_eq!(zero, negative);
        let data = |pk| cell(&room, 0, pk, 2).additional_data().unwrap();
        assert_eq!(data(&zero), data(&negative));
    }
    let one = PrimaryKey::single(Value::Float(1.0));
    let zero = PrimaryKey::single(Value::Float(-0.0));
    let data = |pk| cell(&room, 0, pk, 2).additional_data().unwrap();
    assert_ne!(data(&one), data(&zero));
}

#[test]
fn a_plaintext_decodes_as_one_value_of_the_type_of_its_column() {
    for value in samples() {
        let data_type = value.data_type();
        let plaintext = encoded(&value);
        for nullable in [false, true] {
            let decoded = decoded(&plaintext, &column(data_type, nullable));
            assert_eq!(decoded, Some(value.clone()));
        }
        // Of another type than the column's.
        for other in TYPES.into_iter().filter(|other| *other != data_type) {
            assert_eq!(decoded(&plaintext, &column(other, true)), None, "{other}");
        }
        // With a byte left over, or a byte short.
        let mut longer = plaintext.clone();
        longer.push(0);
        assert_eq!(decoded(&longer, &column(data_type, true)), None);
        let shorter = &plaintext[..plaintext.len() - 1];
        assert_eq!(decoded(shorter, &column(data_type, true)), None);
    }

    // `NULL`, where the column takes it.
    let null = encoded(&Value::Null);
    assert_eq!(null, [0]);
    let decoded_null = decoded(&null, &column(DataType::String, true));
    assert_eq!(decoded_null, Some(Value::Null));
    assert_eq!(decoded(&null, &column(DataType::String, false)), None);
    // Nothing, a variant that `Value` does not have, a bool that is not one, and a string that is
    // not UTF-8.
    let string = column(DataType::String, true);
    assert_eq!(decoded(&[], &string), None);
    assert_eq!(decoded(&[8], &string), None);
    assert_eq!(decoded(&[5, 2], &column(DataType::Bool, true)), None);
    assert_eq!(decoded(&[4, 2, 0xc3, 0x28], &string), None);
}

#[test]
fn a_plaintext_that_declares_more_bytes_than_it_has_decodes_to_nothing() {
    // A `String` and a `Bytes` (its variants 4 and 6) that declare the largest length, or one
    // byte more than they have, decode to nothing. That their decode reserves no more than what
    // came rests on the decode of the shape of the value first (bincode bounds a `String` on its
    // own): no test tells it apart without measuring the reservations, which needs an allocator
    // of its own, with `unsafe`.
    for (variant, data_type) in [(4_u8, DataType::String), (6, DataType::Bytes)] {
        let column = column(data_type, true);
        let mut huge = vec![variant, 253];
        huge.extend(u64::MAX.to_le_bytes());
        huge.extend(b"abc");
        assert_eq!(decoded(&huge, &column), None);
        assert_eq!(decoded(&[variant, 4, b'a', b'b', b'c'], &column), None);
        let exact = decoded(&[variant, 3, b'a', b'b', b'c'], &column).unwrap();
        assert_eq!(exact.data_type(), data_type);
    }
}

/// A random number generator of the tests (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> usize {
        usize::try_from(self.next() % bound).unwrap()
    }

    fn byte(&mut self) -> u8 {
        self.next().to_le_bytes()[0]
    }
}

/// A plaintext for the fuzzing: random bytes, a value encoded and then damaged (a byte changed,
/// cut, or one more), or one with a random length after its variant.
fn plaintext(rng: &mut Rng) -> Vec<u8> {
    let samples = samples();
    let sample = |rng: &mut Rng| encoded(&samples[rng.below(samples.len() as u64)]);
    match rng.below(4) {
        0 => (0..rng.below(24)).map(|_| rng.byte()).collect(),
        1 => {
            let mut plaintext = sample(rng);
            let at = rng.below(plaintext.len() as u64);
            plaintext[at] = rng.byte();
            plaintext
        }
        2 => {
            let mut plaintext = sample(rng);
            match rng.below(2) {
                0 => plaintext.truncate(rng.below(plaintext.len() as u64)),
                _ => plaintext.push(rng.byte()),
            }
            plaintext
        }
        _ => {
            let mut plaintext = vec![[4, 6][rng.below(2)], rng.byte()];
            plaintext.extend((0..rng.below(8)).map(|_| rng.byte()));
            plaintext
        }
    }
}

/// Decrypts random ciphertexts with an engine that gives them back as they are, `rounds` times:
/// the shape of a value decodes what `Value` decodes, and each one decrypts to a value of the type
/// of its column (or `NULL`, if it takes it) or is `Decryption`, without panicking.
async fn fuzz(rounds: u64) {
    let (room, pk) = (room_id(), PrimaryKey::single(1));
    let mut rng = Rng(0x005e_ed0f_c1a5_7e47);
    for _ in 0..rounds {
        let plaintext = plaintext(&mut rng);
        let limit = plaintext.len() as u64;
        let options = encoding().with_limit(limit).reject_trailing_bytes();
        let shape = options.deserialize::<Shape>(&plaintext).is_ok();
        let value = options.deserialize::<Value>(&plaintext).is_ok();
        assert_eq!(shape, value, "{plaintext:?}");
        let data_type = TYPES[rng.below(TYPES.len() as u64)];
        let nullable = rng.below(2) == 0;
        let column = column(data_type, nullable);
        let stored = Value::from(plaintext.clone());
        let cell = cell(&room, 0, &pk, 2);
        match decrypt(&NoOpCryptoEngine, cell, stored, "rounds", &column).await {
            Ok(Value::Null) => assert!(nullable, "{plaintext:?}"),
            Ok(value) => assert_eq!(value.data_type(), data_type, "{plaintext:?}"),
            Err(ClientError::Decryption(message)) => {
                assert!(message.starts_with("The column 'notes' of 'rounds'"))
            }
            Err(err) => panic!("{plaintext:?}: {err:?}"),
        }
    }
}

#[tokio::test]
async fn random_plaintexts_decrypt_to_a_value_of_the_type_of_their_column_or_to_decryption() {
    fuzz(20_000).await;
}

#[tokio::test]
#[ignore = "long: runs the fuzzing of the plaintexts for two million rounds"]
async fn random_plaintexts_decrypt_to_a_value_of_their_type_over_many_rounds() {
    fuzz(2_000_000).await;
}

/// What a [`Logging`] engine was asked: the room, the additional data and the bytes.
type Call = (RoomId, Vec<u8>, Vec<u8>);

/// The error a [`Logging`] engine fails with.
type Failure = fn() -> CryptoError;

/// A `CryptoEngine` that gives the bytes back as they are, as `NoOpCryptoEngine` does, and logs
/// each call; or that fails, with `failure`.
#[derive(Default)]
struct Logging {
    calls: Mutex<Vec<Call>>,
    failure: Option<Failure>,
}

impl Logging {
    fn failing(failure: Failure) -> Self {
        Self {
            failure: Some(failure),
            ..Self::default()
        }
    }

    fn run(&self, room_id: &RoomId, aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let call = (room_id.clone(), aad.to_vec(), bytes.to_vec());
        self.calls.lock().unwrap().push(call);
        match self.failure {
            Some(failure) => Err(failure()),
            None => Ok(bytes.to_vec()),
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl CryptoEngine for Logging {
    async fn encrypt(
        &self,
        room: &RoomId,
        aad: &[u8],
        bytes: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        self.run(room, aad, bytes)
    }

    async fn decrypt(
        &self,
        room: &RoomId,
        aad: &[u8],
        bytes: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        self.run(room, aad, bytes)
    }
}

/// The data of the cell of `column_idx` in the row of `pk` of the table `table_id` of `room-a`.
fn data(table_id: u16, pk: &PrimaryKey, column_idx: u16) -> Vec<u8> {
    let room = room_id();
    cell(&room, table_id, pk, column_idx)
        .additional_data()
        .unwrap()
}

fn kickoff(rating: i64) -> Row {
    Row::new()
        .with("round_id", 1)
        .with("title", "Kickoff")
        .with("rating", rating)
}

#[tokio::test]
async fn an_insert_seals_every_encrypted_column_of_its_table_null_too() {
    let (schema, ana, room) = (schema(), client(), room_id());
    let (draft, _) = Writer::new(&ana)
        .insert(&schema, "rounds", &kickoff(5))
        .unwrap();
    // In the clear, the server would not take it.
    assert!(checked(&schema, draft.op.clone(), &ana).is_err());
    let engine = Logging::default();
    let op = draft.seal(Some(&engine), &room).await.unwrap();

    let pk = PrimaryKey::single(1);
    let (null, five) = (encoded(&Value::Null), encoded(&Value::Int(5)));
    let OperationKind::Insert { row } = op.kind() else {
        panic!("{op:?}");
    };
    let sealed = [
        Value::from(1),
        Value::from("Kickoff"),
        Value::from(null.clone()),
        Value::from(five.clone()),
    ];
    assert_eq!(row.values(), sealed);
    let calls = [
        (room.clone(), data(0, &pk, 2), null),
        (room.clone(), data(0, &pk, 3), five),
    ];
    assert_eq!(engine.calls(), calls);
    assert_eq!(op.pk(), &pk);
    assert!(checked(&schema, op, &ana).is_ok());
}

#[tokio::test]
async fn an_update_seals_its_encrypted_changes_and_not_its_owner() {
    let (schema, ana, room) = (schema(), client(), room_id());
    let key = Key::new().with("entry_id", 4);
    let changes = Row::new().with("body", "secret");
    let draft = Writer::new(&ana).update(&schema, "entries", &key, &changes);
    let engine = Logging::default();
    let op = draft.unwrap().seal(Some(&engine), &room).await.unwrap();

    let secret = encoded(&Value::from("secret"));
    let OperationKind::Update { updates } = op.kind() else {
        panic!("{op:?}");
    };
    let sealed = [
        ColumnUpdate::new(1, "ana"),
        ColumnUpdate::new(2, secret.clone()),
    ];
    assert_eq!(updates, &sealed);
    let pk = PrimaryKey::single(4);
    assert_eq!(engine.calls(), [(room, data(1, &pk, 2), secret)]);
    assert!(checked(&schema, op, &ana).is_ok());

    // An update of a column in the clear, and a delete, seal nothing, and need no engine.
    let draft = Writer::new(&ana).update(
        &schema,
        "rounds",
        &Key::new().with("round_id", 1),
        &Row::new().with("title", "Final"),
    );
    let draft = draft.unwrap();
    let unsealed = draft.op.clone();
    assert_eq!(draft.seal(None, &room_id()).await, Ok(unsealed));
    let draft = Writer::new(&ana).delete(&schema, "entries", &key).unwrap();
    let unsealed = draft.op.clone();
    assert_eq!(draft.seal(None, &room_id()).await, Ok(unsealed));
}

#[tokio::test]
async fn a_write_that_carries_an_encrypted_column_is_crypto_without_an_engine_or_if_it_fails() {
    let (schema, ana, room) = (schema(), client(), room_id());
    let draft = || {
        Writer::new(&ana)
            .insert(&schema, "rounds", &kickoff(5))
            .unwrap()
            .0
    };
    let message = match draft().seal(None, &room).await {
        Err(ClientError::Crypto(message)) => message,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        message,
        "The column 'notes' of 'rounds' is encrypted, and the client has no CryptoEngine"
    );

    // The message says what the engine failed at, without the message of the engine.
    let failures: [(Failure, &str); 3] = [
        (
            || CryptoError::Encryption("secret".to_string()),
            "failed to encrypt",
        ),
        (
            || CryptoError::KeyDerivation("secret".to_string()),
            "failed to derive the key",
        ),
        (
            || CryptoError::Decryption("secret".to_string()),
            "failed to decrypt",
        ),
    ];
    for (failure, why) in failures {
        let engine = Logging::failing(failure);
        let message = match draft().seal(Some(&engine), &room).await {
            Err(ClientError::Crypto(message)) => message,
            other => panic!("{other:?}"),
        };
        assert!(
            message.starts_with("The column 'notes' of 'rounds' was not encrypted"),
            "{message}"
        );
        assert!(message.contains(why), "{message}");
        assert!(!message.contains("secret"), "{message}");
        assert_eq!(engine.calls().len(), 1);
    }
}

#[tokio::test]
async fn a_stored_value_decrypts_with_the_data_of_its_cell_to_a_value_of_its_type() {
    let room = room_id();
    let pk = PrimaryKey::single(1);
    let notes = column(DataType::String, true);
    let engine = Logging::default();
    let decrypt = |engine, stored: Value, column| {
        let cell = cell(&room, 0, &pk, 2);
        async move { decrypt(engine, cell, stored, "rounds", &column).await }
    };

    let secret = Value::from(encoded(&Value::from("secret")));
    let decrypted = decrypt(&engine, secret.clone(), notes.clone()).await;
    assert_eq!(decrypted, Ok(Value::from("secret")));
    assert_eq!(
        engine.calls(),
        [(
            room.clone(),
            data(0, &pk, 2),
            encoded(&Value::from("secret"))
        )]
    );
    // A `NULL` that the storage holds is `NULL`, without the engine.
    assert_eq!(
        decrypt(&engine, Value::Null, notes.clone()).await,
        Ok(Value::Null)
    );
    assert_eq!(engine.calls().len(), 1);

    let failed = |decrypted: Result<Value, ClientError>| match decrypted {
        Err(ClientError::Decryption(message)) => message,
        other => panic!("{other:?}"),
    };
    // Something else than a ciphertext.
    let message = failed(decrypt(&engine, Value::from("secret"), notes.clone()).await);
    assert_eq!(
        message,
        "The column 'notes' of 'rounds' holds no ciphertext"
    );
    // A plaintext of another type, or `NULL` in a column that does not take it.
    let rating = column(DataType::Int, false);
    let message = failed(decrypt(&engine, secret.clone(), rating.clone()).await);
    assert_eq!(
        message,
        "The column 'notes' of 'rounds' does not decrypt to a value of its type, Int"
    );
    let null = Value::from(encoded(&Value::Null));
    failed(decrypt(&engine, null, rating).await);
    // An engine that fails, whose message does not show.
    let failing = Logging::failing(|| CryptoError::Decryption("secret".to_string()));
    let message = failed(decrypt(&failing, secret, notes).await);
    assert!(
        message.contains("does not decrypt with its additional data"),
        "{message}"
    );
    assert!(message.contains("failed to decrypt"), "{message}");
    assert!(!message.contains("secret"), "{message}");
}
