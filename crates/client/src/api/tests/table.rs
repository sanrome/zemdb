use super::*;
use crate::{
    ClientConfig, MemoryMetadata, TokenError, TokenProvider, TransportClient, TransportError,
    ZemdbClient,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use zemdb_core::{
    encode_message, ClientMessage, ColumnDef, ErrorCode, Operation, RowBuilder, SchemaId,
    SequenceNumber, SequencedOperation, ServerMessage,
};
use zemdb_storage::MemoryStorageEngine;

/// Table `tasks`: an int key `id` and a string `title`.
fn tasks_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

/// [`tasks_schema`] with the nullable string column `tag`: its version 2.
fn tagged_schema() -> Schema {
    let mut schema = tasks_schema();
    let tag = ColumnDef::new("tag", DataType::String).nullable(true);
    schema.add_column("tasks", tag).unwrap();
    schema
}

/// Table `notes`, with the id of `tasks`: an int key `number` and an int `size`.
fn notes_schema() -> Schema {
    let table = TableSchema::builder("notes")
        .primary_key("number", DataType::Int)
        .column("size", DataType::Int);
    Schema::builder().table(table).build()
}

/// A server of one room that answers registrations only: `Registered` with its incarnation and
/// schema, at the cursor of the registration, or `RoomReset` to a cursor of another incarnation.
#[derive(Debug, Clone)]
struct Server(Arc<Mutex<(u128, Schema)>>);

impl Server {
    fn new(schema: Schema) -> Self {
        Self(Arc::new(Mutex::new((1, schema))))
    }

    /// Creates the room again, with another incarnation and `schema`.
    fn recreate(&self, schema: Schema) {
        let mut room = self.0.lock().unwrap();
        *room = (room.0 + 1, schema);
    }

    /// Moves the room to `schema`, in the same incarnation.
    fn evolve(&self, schema: Schema) {
        self.0.lock().unwrap().1 = schema;
    }
}

#[async_trait]
impl TransportClient for Server {
    async fn send(&self, message: &ClientMessage, _: &str) -> Result<Vec<u8>, TransportError> {
        let ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            current_seq,
            known_incarnation,
            ..
        } = message
        else {
            return Err(TransportError("It answers registrations only".to_string()));
        };
        let (incarnation, schema) = self.0.lock().unwrap().clone();
        let reply = match known_incarnation {
            Some(known) if *known != incarnation => ServerMessage::Error {
                correlation_id: Some(*correlation_id),
                room_id: Some(room_id.clone()),
                code: ErrorCode::RoomReset,
                message: "The room was created again".to_string(),
            },
            _ => ServerMessage::Registered {
                correlation_id: *correlation_id,
                room_id: room_id.clone(),
                head_seq: current_seq.unwrap_or(SequenceNumber::new(0)),
                tail_seq: SequenceNumber::new(0),
                schema_id: SchemaId::new(format!("schema-{incarnation}")).unwrap(),
                schema,
                active_snapshot_seq: None,
                room_incarnation: incarnation,
            },
        };
        Ok(encode_message(&reply).unwrap())
    }
}

fn room_id() -> RoomId {
    RoomId::new("room-a").unwrap()
}

/// A token provider that gives no token: the server of the test takes any.
#[derive(Debug)]
struct NoTokens;

#[async_trait]
impl TokenProvider for NoTokens {
    async fn token(&self, _: &zemdb_core::ClientId, _: &RoomId) -> Result<String, TokenError> {
        Err(TokenError("No tokens".to_string()))
    }
}

/// A client of ana over a memory engine, with `room-a` of `server` open, and its table `table`.
async fn open(server: &Server, table: &str) -> (ZemdbClient, TableHandle) {
    let client = ZemdbClient::open(
        ClientConfig::new("ana"),
        MemoryStorageEngine::new(),
        MemoryMetadata::default(),
        server.clone(),
        NoTokens,
        None,
    );
    let client = client.await.unwrap();
    let room = client.room(room_id(), "token".to_string()).await.unwrap();
    let table = room.table(table).unwrap();
    (client, table)
}

/// Applies `ops` to the storage room of `room-a` of `client` after its head, as a sync does.
async fn apply(client: &ZemdbClient, ops: impl IntoIterator<Item = Operation>) {
    let storage = client.core().storage();
    let head = storage.get_head_seq(&room_id()).await.unwrap();
    let ops = (ops.into_iter().zip(head.get() + 1..))
        .map(|(op, seq)| SequencedOperation::new(SequenceNumber::new(seq), op))
        .collect();
    storage.apply_batch(&room_id(), ops).await.unwrap();
}

/// An insert into `table` of `schema` of `row`.
fn insert(schema: &Schema, table: &str, row: RowBuilder) -> Operation {
    let row = row.build();
    schema.to_operation_insert(table, &row, 1000).unwrap()
}

fn task(id: i64, title: &str) -> RowBuilder {
    RowBuilder::new().set("id", id).set("title", title)
}

fn row(columns: &[(&str, Value)]) -> Row {
    columns
        .iter()
        .map(|(name, value)| (*name, value.clone()))
        .collect()
}

/// The read of the row `id` of `tasks`, planned against the schema of the replica.
fn get(table: &TableHandle, id: i64) -> (u64, Read) {
    let key = Key::new().with("id", id);
    let planned = table.plan(|schema, crypto| Read::get(schema, "tasks", &key, crypto));
    planned.unwrap()
}

/// The read of every row of `tasks`, planned against the schema of the replica.
fn scan(table: &TableHandle) -> (u64, Read) {
    let scan = Scan::new();
    let planned = table.plan(|schema, crypto| Read::scan(schema, "tasks", &scan, crypto));
    planned.unwrap()
}

#[tokio::test]
async fn a_read_with_a_reload_of_the_schema_after_it_read_the_schema_gives_its_columns() {
    let server = Server::new(tasks_schema());
    let (client, table) = open(&server, "tasks").await;
    apply(&client, [insert(&tasks_schema(), "tasks", task(1, "one"))]).await;
    let (get, scan) = (get(&table, 1), scan(&table));

    // The storage room reloads the schema, and takes rows written with it, before the replica
    // takes the schema.
    let storage = client.core().storage();
    storage
        .reload_schema(&room_id(), tagged_schema())
        .await
        .unwrap();
    let fields = BTreeMap::from([("tag".to_string(), Value::from("red"))]);
    let pk = PrimaryKey::single(1);
    let tagged = tagged_schema().to_operation_update("tasks", pk, &fields, 1000);
    let two = insert(&tagged_schema(), "tasks", task(2, "two").set("tag", "blue"));
    apply(&client, [tagged.unwrap(), two]).await;

    let one = row(&[("id", Value::from(1)), ("title", Value::from("one"))]);
    let two = row(&[("id", Value::from(2)), ("title", Value::from("two"))]);
    assert_eq!(table.read(get).await, Ok(vec![one.clone()]));
    assert_eq!(table.read(scan).await, Ok(vec![one, two]));
}

#[tokio::test]
async fn a_read_across_a_discard_is_room_closed_and_one_across_a_reopening_is_not() {
    let server = Server::new(tasks_schema());
    let (client, table) = open(&server, "tasks").await;
    apply(&client, [insert(&tasks_schema(), "tasks", task(1, "one"))]).await;

    // Closed and opened again in the same epoch: a memory room opens empty.
    let read = get(&table, 1);
    table.room().close().await.unwrap();
    client.room(room_id(), "token".to_string()).await.unwrap();
    assert_eq!(table.read(read).await, Ok(vec![]));

    // Discarded for a room created again, whose table of the same id is another, with a row of
    // a key of the same value: the read does not take it for a row of `tasks`.
    let (read, rows) = (get(&table, 1), scan(&table));
    server.recreate(notes_schema());
    table.room().close().await.unwrap();
    client.room(room_id(), "token".to_string()).await.unwrap();
    let note = RowBuilder::new().set("number", 1).set("size", 7);
    let note = insert(&notes_schema(), "notes", note);
    apply(&client, [note]).await;
    let closed = Err(ClientError::RoomClosed(room_id()));
    assert_eq!(table.read(read).await, closed);
    assert_eq!(table.read(rows).await, closed);
    // A new read resolves the table against the new schema, which has no `tasks`.
    let invalid = table.get(&Key::new().with("id", 1)).await;
    assert!(
        matches!(invalid, Err(ClientError::InvalidOperation(_))),
        "{invalid:?}"
    );
}

#[test]
fn a_row_of_the_storage_is_named_with_the_columns_of_the_read() {
    let columns = ["id".to_string(), "title".to_string(), "tag".to_string()];
    // A short row, and one with a value of a later version of the schema.
    let short = CompactRow::new(vec![1.into(), "one".into()]);
    let short_named = row(&[
        ("id", Value::from(1)),
        ("title", Value::from("one")),
        ("tag", Value::Null),
    ]);
    assert_eq!(named(&columns, short), short_named);
    let long = CompactRow::new(vec![1.into(), "one".into(), "red".into(), 7.into()]);
    let long_named = row(&[
        ("id", Value::from(1)),
        ("title", Value::from("one")),
        ("tag", Value::from("red")),
    ]);
    assert_eq!(named(&columns, long), long_named);
}

#[tokio::test]
async fn a_write_checked_after_a_reload_of_the_schema_is_valid_and_after_a_discard_room_closed() {
    let server = Server::new(tasks_schema());
    let (client, table) = open(&server, "tasks").await;
    let one = Row::new().with("id", 1).with("title", "one");
    let plan = || {
        let (epoch, (draft, _)) = table
            .plan_write(|writer, schema| writer.insert(schema, "tasks", &one))
            .unwrap();
        (epoch, draft)
    };

    // Planned, and then the replica takes a schema that extends its own, in the same epoch: the
    // insert, shorter than a row of it, is still valid.
    let (epoch, draft) = plan();
    let op = draft.seal(None, &room_id()).await.unwrap();
    server.evolve(tagged_schema());
    table.room().close().await.unwrap();
    client.room(room_id(), "token".to_string()).await.unwrap();
    let tagged = table
        .room()
        .client()
        .core()
        .read_schema(&room_id(), Schema::clone);
    assert_eq!(tagged, Ok((epoch, tagged_schema())));
    assert_eq!(table.checked(epoch, op.clone()), Ok(op));

    // Discarded for a room created again, whose table of the same id does not take the insert:
    // it is not checked against the schema of the new room.
    let (epoch, draft) = plan();
    let op = draft.seal(None, &room_id()).await.unwrap();
    server.recreate(notes_schema());
    table.room().close().await.unwrap();
    client.room(room_id(), "token".to_string()).await.unwrap();
    let closed = Err(ClientError::RoomClosed(room_id()));
    assert_eq!(table.checked(epoch, op), closed);
}
