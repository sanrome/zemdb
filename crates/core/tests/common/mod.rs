//! Support for the randomized tests: a small deterministic generator, a mutator of byte strings
//! in the style of coverage-guided fuzzers, generators of random values of every wire type, and
//! valid sample inputs of every format.

// Each file in `tests/` is a crate of its own and uses only part of these helpers.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use bytes::Bytes;
use smallvec::SmallVec;
use zemdb_core::protocol::PROTOCOL_MAGIC;
use zemdb_core::schema::MAX_COLUMNS;
use zemdb_core::*;

// ---------------------------------------------------------------------------------------------
// Generator and case runner
// ---------------------------------------------------------------------------------------------

/// A splitmix64 generator: a few lines, fast, and fully determined by its seed, so that a
/// failing case is replayed from its seed alone.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..n`, or 0 if `n` is 0.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// A number in `low..high`.
    pub fn between(&mut self, low: usize, high: usize) -> usize {
        low + self.below(high - low)
    }

    /// `true` with probability `num / den`.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.next_u64() % den < num
    }

    pub fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next_u64() as u8).collect()
    }

    /// Fewer than `n` random bytes.
    pub fn bytes_below(&mut self, n: usize) -> Vec<u8> {
        let len = self.below(n);
        self.bytes(len)
    }

    pub fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        out.iter_mut().for_each(|b| *b = self.next_u64() as u8);
        out
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    /// A `u64` that is often a boundary of the variable-length integer encoding (where it
    /// changes width) or an extreme, and otherwise uniform.
    pub fn u64(&mut self) -> u64 {
        if self.chance(1, 3) {
            *self.pick(&INTERESTING_U64)
        } else {
            self.next_u64()
        }
    }

    pub fn u32(&mut self) -> u32 {
        self.u64() as u32
    }

    pub fn u16(&mut self) -> u16 {
        self.u64() as u16
    }

    pub fn u128(&mut self) -> u128 {
        u128::from(self.u64()) << 64 | u128::from(self.u64())
    }
}

/// Runs `check` on `cases` cases, each with a generator seeded from `seed` and the case number.
/// A failing case prints its seed, so that `check(&mut Rng::new(seed))` replays it alone.
pub fn for_each_case(seed: u64, cases: u64, mut check: impl FnMut(&mut Rng)) {
    for case in 0..cases {
        let case_seed = Rng::new(seed ^ case.wrapping_mul(0xD1B5_4A32_D192_ED03)).next_u64();
        let outcome = catch_unwind(AssertUnwindSafe(|| check(&mut Rng::new(case_seed))));
        if let Err(panic) = outcome {
            eprintln!(
                "case {case} of seed {seed:#x} failed; replay it with Rng::new({case_seed:#x})"
            );
            resume_unwind(panic);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Mutator
// ---------------------------------------------------------------------------------------------

const INTERESTING_U8: [u8; 12] = [
    0, 1, 0x7F, 0x80, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x10, 0x40,
];

/// Extremes and the edges of the variable-length integer encoding: up to 250 a value takes one
/// byte, and 0xFB, 0xFC and 0xFD announce a 2, 4 and 8 byte value.
const INTERESTING_U64: [u64; 18] = [
    0,
    1,
    250,
    251,
    0xFF,
    0xFFFF,
    0x1_0000,
    1 << 20,
    (1 << 20) + 1,
    16 * 1024 * 1024,
    16 * 1024 * 1024 + 1,
    0x7FFF_FFFF,
    0xFFFF_FFFF,
    0x1_0000_0000,
    1 << 40,
    i64::MAX as u64,
    u64::MAX - 1,
    u64::MAX,
];

/// Applies 1 to 4 random mutations to `data`: bit flips, byte changes, insertions, deletions,
/// truncations, duplicated ranges, ranges spliced from another sample of `pool`, and lengths
/// declared with the variable-length encoding. The first `keep` bytes (a header) are left
/// alone, except now and then.
pub fn mutate(rng: &mut Rng, data: &mut Vec<u8>, keep: usize, pool: &[Vec<u8>]) {
    for _ in 0..1 + rng.below(4) {
        let lo = if rng.chance(1, 20) {
            0
        } else {
            keep.min(data.len())
        };
        let len = data.len();
        match rng.below(11) {
            0 if len > lo => {
                let i = rng.between(lo, len);
                data[i] ^= 1 << rng.below(8);
            }
            1 if len > lo => {
                let i = rng.between(lo, len);
                data[i] = rng.next_u64() as u8;
            }
            2 if len > lo => {
                let i = rng.between(lo, len);
                data[i] = *rng.pick(&INTERESTING_U8);
            }
            3 => {
                let i = rng.between(lo, len + 1);
                let n = 1 + rng.below(16);
                let inserted = rng.bytes(n);
                data.splice(i..i, inserted);
            }
            4 if len > lo => {
                let i = rng.between(lo, len);
                let n = 1 + rng.below((len - i).min(32));
                data.drain(i..i + n);
            }
            5 if len > lo => {
                // A variable-length integer of an interesting value, in any of its widths.
                let i = rng.between(lo, len);
                let v = *rng.pick(&INTERESTING_U64);
                let mut encoded = Vec::new();
                match rng.below(4) {
                    0 => {
                        encoded.push(0xFB);
                        encoded.extend_from_slice(&(v as u16).to_le_bytes());
                    }
                    1 => {
                        encoded.push(0xFC);
                        encoded.extend_from_slice(&(v as u32).to_le_bytes());
                    }
                    2 => {
                        encoded.push(0xFD);
                        encoded.extend_from_slice(&v.to_le_bytes());
                    }
                    // As a fixed-width integer.
                    _ => encoded.extend_from_slice(&v.to_le_bytes()),
                }
                let end = (i + encoded.len()).min(len);
                data.splice(i..end, encoded);
            }
            6 if len > lo => {
                let i = rng.between(lo, len);
                data.truncate(i);
            }
            7 if len > lo + 1 => {
                let from = rng.between(lo, len);
                let n = 1 + rng.below((len - from).min(64));
                let chunk = data[from..from + n].to_vec();
                let at = rng.between(lo, len + 1);
                data.splice(at..at, chunk);
            }
            8 if !pool.is_empty() => {
                let other = rng.pick(pool);
                if !other.is_empty() {
                    let from = rng.below(other.len());
                    let n = 1 + rng.below((other.len() - from).min(64));
                    let at = rng.between(lo, len + 1);
                    let end = (at + n).min(len);
                    data.splice(at..end, other[from..from + n].iter().copied());
                }
            }
            9 if len > lo => {
                // A fixed-width little-endian `u32` or `u64` of an interesting value.
                let i = rng.between(lo, len);
                let width = if rng.coin() { 4 } else { 8 };
                let bytes = rng.pick(&INTERESTING_U64).to_le_bytes();
                for (k, b) in bytes.iter().take(width).enumerate() {
                    if let Some(slot) = data.get_mut(i + k) {
                        *slot = *b;
                    }
                }
            }
            _ if len > lo => {
                let i = rng.between(lo, len);
                data[i] = data[i].wrapping_add(1 + rng.below(4) as u8);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Random values
// ---------------------------------------------------------------------------------------------

pub fn float(rng: &mut Rng) -> f64 {
    match rng.below(9) {
        0 => -0.0,
        1 => 0.0,
        2 => f64::NAN,
        3 => -f64::NAN,
        4 => f64::INFINITY,
        5 => f64::NEG_INFINITY,
        6 => f64::MIN_POSITIVE,
        7 => (rng.next_u64() as i64) as f64 / 1024.0,
        _ => f64::from_bits(rng.next_u64()),
    }
}

/// Text of up to `max_chars` characters: ASCII, multi-byte characters and any Unicode scalar.
pub fn text(rng: &mut Rng, max_chars: usize) -> String {
    (0..rng.below(max_chars + 1))
        .map(|_| match rng.below(4) {
            0 | 1 => (b' ' + rng.below(95) as u8) as char,
            2 => *rng.pick(&['é', 'ä', 'ñ', '€', '漢', '🦀', '\u{0}', '\u{7F}']),
            _ => loop {
                if let Some(c) = char::from_u32(rng.below(0x11_0000) as u32) {
                    break c;
                }
            },
        })
        .collect()
}

/// A string of 1 to `max_len` characters from `alphabet`.
fn word(rng: &mut Rng, alphabet: &[u8], max_len: usize) -> String {
    (0..rng.between(1, max_len + 1))
        .map(|_| *rng.pick(alphabet) as char)
        .collect()
}

pub fn value(rng: &mut Rng) -> Value {
    match rng.below(8) {
        0 => Value::Null,
        1 => Value::Int(rng.u64() as i64),
        2 => Value::Float(float(rng)),
        3 => Value::Timestamp(rng.u64() as i64),
        4 => Value::String(text(rng, 12).into()),
        5 => Value::Bool(rng.coin()),
        6 => Value::Bytes(rng.bytes_below(24).into()),
        _ => Value::Uuid(rng.array()),
    }
}

pub fn values(rng: &mut Rng, max: usize) -> Vec<Value> {
    (0..rng.below(max + 1)).map(|_| value(rng)).collect()
}

pub fn primary_key(rng: &mut Rng) -> PrimaryKey {
    PrimaryKey::from_smallvec(SmallVec::from_vec(values(rng, 3)))
}

pub fn compact_row(rng: &mut Rng) -> CompactRow {
    CompactRow::new(values(rng, 5))
}

pub fn operation_kind(rng: &mut Rng) -> OperationKind {
    match rng.below(3) {
        0 => OperationKind::Insert {
            row: compact_row(rng),
        },
        1 => OperationKind::Update {
            updates: (0..rng.below(5))
                .map(|_| ColumnUpdate::new(rng.u16(), value(rng)))
                .collect(),
        },
        _ => OperationKind::Delete,
    }
}

pub fn operation(rng: &mut Rng) -> Operation {
    let table_id = rng.u16();
    let pk = primary_key(rng);
    let timestamp = rng.u64();
    Operation::new(table_id, pk, timestamp, operation_kind(rng))
}

pub fn sequenced_operation(rng: &mut Rng) -> SequencedOperation {
    let seq = rng.u64();
    SequencedOperation::new(seq, operation(rng))
}

pub fn sequenced_operations(rng: &mut Rng, max: usize) -> Vec<SequencedOperation> {
    (0..rng.below(max + 1))
        .map(|_| sequenced_operation(rng))
        .collect()
}

const PATH_ID_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_-";

pub fn room_id(rng: &mut Rng) -> RoomId {
    loop {
        // Some names are reserved (Windows device names): draw again.
        if let Ok(id) = RoomId::new(word(rng, PATH_ID_ALPHABET, 64)) {
            return id;
        }
    }
}

pub fn schema_id(rng: &mut Rng) -> SchemaId {
    loop {
        if let Ok(id) = SchemaId::new(word(rng, PATH_ID_ALPHABET, 64)) {
            return id;
        }
    }
}

pub fn client_id(rng: &mut Rng) -> ClientId {
    loop {
        let candidate = if rng.coin() {
            word(
                rng,
                b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _.@-",
                40,
            )
        } else {
            text(rng, 20)
        };
        if let Ok(id) = ClientId::new(candidate) {
            return id;
        }
    }
}

pub fn seq(rng: &mut Rng) -> SequenceNumber {
    SequenceNumber::new(rng.u64())
}

pub fn correlation_id(rng: &mut Rng) -> CorrelationId {
    CorrelationId::new(rng.u64())
}

pub fn mutation_id(rng: &mut Rng) -> MutationId {
    MutationId::new(rng.array())
}

pub fn option<T>(rng: &mut Rng, make: impl FnOnce(&mut Rng) -> T) -> Option<T> {
    rng.coin().then(|| make(rng))
}

pub fn data_type(rng: &mut Rng) -> DataType {
    *rng.pick(&[
        DataType::Int,
        DataType::Float,
        DataType::Timestamp,
        DataType::String,
        DataType::Bool,
        DataType::Bytes,
        DataType::Uuid,
    ])
}

/// Every error code, read from the wire by variant index until one does not decode, so that a
/// new code is covered without listing it here.
pub fn all_error_codes() -> Vec<ErrorCode> {
    (0u8..)
        .map_while(|index| {
            let mut frame = PROTOCOL_MAGIC.to_vec();
            frame.extend_from_slice(&[PROTOCOL_VERSION, 0, index]);
            decode_message::<ErrorCode>(&frame).ok()
        })
        .collect()
}

/// A random valid schema: up to 3 tables of up to 5 columns with a key of 1 or 2 of them, then
/// up to 3 columns added with `add_column`.
pub fn schema(rng: &mut Rng) -> Schema {
    let mut tables = Vec::new();
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for _ in 0..rng.below(4) {
        let id = rng.u16();
        let name = word(rng, b"abcdefghijklmnopqrstuvwxyz", 6);
        if !ids.insert(id) || !names.insert(name.clone()) {
            continue;
        }
        let key_len = rng.between(1, 3);
        let mut column_names = Vec::new();
        for _ in 0..rng.between(1, 6) {
            let name = word(rng, b"abcdefghijklmnopqrstuvwxyz", 5);
            if !column_names.contains(&name) {
                column_names.push(name);
            }
        }
        let columns: Vec<ColumnDef> = column_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                // Key columns can be neither nullable nor encrypted.
                let in_key = i < key_len;
                ColumnDef::new(name, data_type(rng))
                    .nullable(rng.coin() && !in_key)
                    .encrypted(rng.coin() && !in_key)
            })
            .collect();
        let key = columns
            .iter()
            .take(key_len)
            .map(|c| c.name().to_string())
            .collect();
        tables.push(TableSchema::try_new(id, name, key, columns).unwrap());
    }
    let mut schema = Schema::try_from_tables(tables).unwrap();
    let names: Vec<String> = schema.tables().map(|t| t.name().to_string()).collect();
    for _ in 0..rng.below(4) {
        if names.is_empty() {
            break;
        }
        let table = rng.pick(&names).clone();
        let column = ColumnDef::new(word(rng, b"abcdefghijklmnopqrstuvwxyz", 6), data_type(rng))
            .nullable(true)
            .encrypted(rng.coin());
        match schema.add_column(&table, column) {
            Ok(_) | Err(ValidationError::DuplicateColumn { .. }) => {}
            Err(other) => panic!("a nullable column is always accepted: {other}"),
        }
    }
    schema
}

pub fn client_message(rng: &mut Rng) -> ClientMessage {
    let correlation_id = correlation_id(rng);
    let room_id = room_id(rng);
    match rng.below(9) {
        0 => ClientMessage::Commit {
            correlation_id,
            room_id,
            client_id: client_id(rng),
            mutation_id: mutation_id(rng),
            last_ack_seq: seq(rng),
            op: operation(rng),
        },
        1 => ClientMessage::Ack {
            correlation_id,
            room_id,
            client_id: client_id(rng),
            ack_seq: seq(rng),
        },
        2 => ClientMessage::Sync {
            correlation_id,
            room_id,
            client_id: client_id(rng),
            from_seq: seq(rng),
            max_batch_size: rng.u32(),
        },
        3 => ClientMessage::Heartbeat {
            correlation_id,
            room_id,
            client_id: client_id(rng),
        },
        4 => ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            client_id: client_id(rng),
            auth_token: text(rng, 40),
            current_seq: option(rng, seq),
            known_incarnation: option(rng, Rng::u128),
        },
        5 => ClientMessage::GetSchema {
            correlation_id,
            room_id,
        },
        6 => ClientMessage::DeregisterClient {
            correlation_id,
            room_id,
            client_id: client_id(rng),
        },
        7 => ClientMessage::RequestSnapshotChunk {
            correlation_id,
            room_id,
            chunk_index: rng.u32(),
            chunk_size: rng.u32(),
            snapshot_hash: option(rng, Rng::array),
        },
        _ => ClientMessage::UploadSnapshotChunk {
            correlation_id,
            room_id,
            snapshot_head_seq: seq(rng),
            chunk_index: rng.u32(),
            total_chunks: rng.u32(),
            total_bytes: rng.u64(),
            snapshot_hash: rng.array(),
            data: Bytes::from(rng.bytes_below(64)),
        },
    }
}

pub fn server_message(rng: &mut Rng, error_codes: &[ErrorCode]) -> ServerMessage {
    let correlation_id = correlation_id(rng);
    let room_id = room_id(rng);
    match rng.below(11) {
        0 => ServerMessage::CommitAck {
            correlation_id,
            room_id,
            mutation_id: mutation_id(rng),
            assigned_seq: seq(rng),
            catchup_ops: sequenced_operations(rng, 5),
            has_more: rng.coin(),
            snapshot_wanted: rng.coin(),
            schema_version: rng.u64(),
        },
        1 => ServerMessage::AckConfirmed {
            correlation_id,
            room_id,
            ack_seq: seq(rng),
            head_seq: seq(rng),
        },
        2 => ServerMessage::SyncBatch {
            correlation_id,
            room_id,
            head_seq: seq(rng),
            ops: sequenced_operations(rng, 5),
            has_more: rng.coin(),
            snapshot_wanted: rng.coin(),
            schema_version: rng.u64(),
        },
        3 => ServerMessage::SnapshotChunk {
            correlation_id,
            room_id,
            snapshot_head_seq: seq(rng),
            chunk_index: rng.u32(),
            total_chunks: rng.u32(),
            total_bytes: rng.u64(),
            snapshot_hash: rng.array(),
            data: Bytes::from(rng.bytes_below(64)),
        },
        4 => ServerMessage::SnapshotUploadChunkAck {
            correlation_id,
            room_id,
            chunk_index: rng.u32(),
            total_chunks: rng.u32(),
            staged: rng.coin(),
        },
        5 => ServerMessage::SnapshotStaged {
            room_id,
            snapshot_head_seq: seq(rng),
            snapshot_hash: rng.array(),
        },
        6 => ServerMessage::HeartbeatAck {
            correlation_id,
            room_id,
            current_head_seq: seq(rng),
            snapshot_wanted: rng.coin(),
            active_snapshot_seq: option(rng, seq),
            schema_version: rng.u64(),
        },
        7 => ServerMessage::DeregisterAck {
            correlation_id,
            room_id,
            client_id: client_id(rng),
        },
        8 => ServerMessage::Registered {
            correlation_id,
            room_id,
            head_seq: seq(rng),
            tail_seq: seq(rng),
            schema_id: schema_id(rng),
            schema: schema(rng),
            active_snapshot_seq: option(rng, seq),
            room_incarnation: rng.u128(),
        },
        9 => ServerMessage::Schema {
            correlation_id,
            room_id,
            schema_id: schema_id(rng),
            schema: schema(rng),
        },
        _ => ServerMessage::Error {
            correlation_id: option(rng, self::correlation_id),
            room_id: rng.coin().then_some(room_id),
            code: *rng.pick(error_codes),
            message: text(rng, 30),
        },
    }
}

// ---------------------------------------------------------------------------------------------
// Checks shared by the decoders' tests
// ---------------------------------------------------------------------------------------------

/// Whether every row, key and list of deltas in `op` is within the column limit that decoding
/// enforces.
pub fn within_column_limit(op: &Operation) -> bool {
    let kind_len = match op.kind() {
        OperationKind::Insert { row } => row.len(),
        OperationKind::Update { updates } => updates.len(),
        OperationKind::Delete => 0,
    };
    op.pk().len() <= MAX_COLUMNS && kind_len <= MAX_COLUMNS
}

// ---------------------------------------------------------------------------------------------
// Valid samples, the seeds of the mutations
// ---------------------------------------------------------------------------------------------

pub fn sample_room() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// One value of every type, with the floats whose encoding is special.
pub fn sample_values() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Int(-5),
        Value::Int(i64::MAX),
        Value::Float(-0.0),
        Value::Float(f64::NAN),
        Value::Float(1.5),
        Value::Timestamp(1_700_000_000),
        Value::String("héllo".into()),
        Value::Bool(true),
        Value::Bytes(vec![1, 2, 3, 0xFF].into_boxed_slice()),
        Value::Uuid([7u8; 16]),
    ]
}

/// An insert, an update, a delete with a composite key and an insert of every value type.
pub fn sample_ops() -> Vec<SequencedOperation> {
    vec![
        SequencedOperation::new(
            1u64,
            Operation::insert(
                0,
                PrimaryKey::single(1i64),
                CompactRow::new(vec![Value::Int(1), Value::String("a".into()), Value::Null]),
                10,
            ),
        ),
        SequencedOperation::new(
            2u64,
            Operation::update(
                0,
                PrimaryKey::single(1i64),
                vec![
                    ColumnUpdate::new(1, "b"),
                    ColumnUpdate::new(2, Value::Float(2.0)),
                ],
                11,
            ),
        ),
        SequencedOperation::new(
            3u64,
            Operation::delete(
                1,
                PrimaryKey::composite([Value::Int(1), Value::Uuid([1; 16])]),
                12,
            ),
        ),
        SequencedOperation::new(
            4u64,
            Operation::insert(
                1,
                PrimaryKey::single(Value::Bytes(vec![9; 3].into())),
                CompactRow::new(sample_values()),
                13,
            ),
        ),
    ]
}

/// Two tables, one with a composite key and an encrypted column, and a column added later.
pub fn sample_schema() -> Schema {
    let mut schema = Schema::builder()
        .table(
            TableBuilder::new("items")
                .table_id(0)
                .primary_key("id", DataType::Int)
                .column("name", DataType::String)
                .nullable_column("price", DataType::Float),
        )
        .table(
            TableBuilder::new("links")
                .table_id(1)
                .primary_key("a", DataType::Int)
                .primary_key("b", DataType::Uuid)
                .encrypted_column("secret", DataType::String)
                .nullable_column("flag", DataType::Bool),
        )
        .build();
    schema
        .add_column(
            "items",
            ColumnDef::new("extra", DataType::Bytes).nullable(true),
        )
        .unwrap();
    schema
}

/// Every client message kind, and a commit of every sample operation.
pub fn sample_client_messages() -> Vec<ClientMessage> {
    let client_id = ClientId::new("client-ä").unwrap();
    let mut messages: Vec<ClientMessage> = sample_ops()
        .into_iter()
        .map(|op| ClientMessage::Commit {
            correlation_id: CorrelationId::new(7),
            room_id: sample_room(),
            client_id: client_id.clone(),
            mutation_id: MutationId::from_u128(99),
            last_ack_seq: SequenceNumber::new(300),
            op: op.op,
        })
        .collect();
    let correlation_id = CorrelationId::new(1);
    messages.extend([
        ClientMessage::Ack {
            correlation_id,
            room_id: sample_room(),
            client_id: client_id.clone(),
            ack_seq: SequenceNumber::new(u64::MAX),
        },
        ClientMessage::Sync {
            correlation_id,
            room_id: sample_room(),
            client_id: client_id.clone(),
            from_seq: SequenceNumber::new(70_000),
            max_batch_size: 1000,
        },
        ClientMessage::Heartbeat {
            correlation_id,
            room_id: sample_room(),
            client_id: client_id.clone(),
        },
        ClientMessage::RegisterClient {
            correlation_id,
            room_id: sample_room(),
            client_id: client_id.clone(),
            auth_token: "v1.abc.def.123.ff".into(),
            current_seq: Some(SequenceNumber::new(5)),
            known_incarnation: Some(u128::MAX - 3),
        },
        ClientMessage::GetSchema {
            correlation_id,
            room_id: sample_room(),
        },
        ClientMessage::DeregisterClient {
            correlation_id,
            room_id: sample_room(),
            client_id,
        },
        ClientMessage::RequestSnapshotChunk {
            correlation_id,
            room_id: sample_room(),
            chunk_index: 3,
            chunk_size: 65536,
            snapshot_hash: Some([5; 32]),
        },
        ClientMessage::UploadSnapshotChunk {
            correlation_id,
            room_id: sample_room(),
            snapshot_head_seq: SequenceNumber::new(9),
            chunk_index: 0,
            total_chunks: 2,
            total_bytes: 100,
            snapshot_hash: [1; 32],
            data: Bytes::from_static(b"some snapshot bytes"),
        },
    ]);
    messages
}

/// Every server message kind.
pub fn sample_server_messages() -> Vec<ServerMessage> {
    let correlation_id = CorrelationId::new(1);
    vec![
        ServerMessage::CommitAck {
            correlation_id,
            room_id: sample_room(),
            mutation_id: MutationId::from_u128(5),
            assigned_seq: SequenceNumber::new(5),
            catchup_ops: sample_ops(),
            has_more: true,
            snapshot_wanted: false,
            schema_version: 3,
        },
        ServerMessage::AckConfirmed {
            correlation_id,
            room_id: sample_room(),
            ack_seq: SequenceNumber::new(4),
            head_seq: SequenceNumber::new(5),
        },
        ServerMessage::SyncBatch {
            correlation_id,
            room_id: sample_room(),
            head_seq: SequenceNumber::new(5),
            ops: sample_ops(),
            has_more: false,
            snapshot_wanted: true,
            schema_version: 2,
        },
        ServerMessage::SnapshotChunk {
            correlation_id,
            room_id: sample_room(),
            snapshot_head_seq: SequenceNumber::new(5),
            chunk_index: 0,
            total_chunks: 1,
            total_bytes: 5,
            snapshot_hash: [3; 32],
            data: Bytes::from_static(b"hello"),
        },
        ServerMessage::SnapshotUploadChunkAck {
            correlation_id,
            room_id: sample_room(),
            chunk_index: 0,
            total_chunks: 1,
            staged: true,
        },
        ServerMessage::SnapshotStaged {
            room_id: sample_room(),
            snapshot_head_seq: SequenceNumber::new(5),
            snapshot_hash: [4; 32],
        },
        ServerMessage::HeartbeatAck {
            correlation_id,
            room_id: sample_room(),
            current_head_seq: SequenceNumber::new(5),
            snapshot_wanted: false,
            active_snapshot_seq: Some(SequenceNumber::new(3)),
            schema_version: 2,
        },
        ServerMessage::DeregisterAck {
            correlation_id,
            room_id: sample_room(),
            client_id: ClientId::new("x").unwrap(),
        },
        ServerMessage::Registered {
            correlation_id,
            room_id: sample_room(),
            head_seq: SequenceNumber::new(5),
            tail_seq: SequenceNumber::new(1),
            schema_id: SchemaId::new("todo").unwrap(),
            schema: sample_schema(),
            active_snapshot_seq: None,
            room_incarnation: 77,
        },
        ServerMessage::Schema {
            correlation_id,
            room_id: sample_room(),
            schema_id: SchemaId::new("todo").unwrap(),
            schema: sample_schema(),
        },
        ServerMessage::Error {
            correlation_id: None,
            room_id: Some(sample_room()),
            code: ErrorCode::RoomReset,
            message: "boom".into(),
        },
    ]
}

/// A log of four frames: with and without a mutation id, and one without operations.
pub fn sample_wal_log() -> Vec<u8> {
    let ops = sample_ops();
    let mut log = Vec::new();
    log.extend(encode_wal_batch(&ops[..1], Some(MutationId::from_u128(1))).unwrap());
    log.extend(encode_wal_batch(&ops[1..3], None).unwrap());
    log.extend(encode_wal_batch(&[], None).unwrap());
    log.extend(encode_wal_batch(&ops[3..], Some(MutationId::from_u128(9))).unwrap());
    log
}
