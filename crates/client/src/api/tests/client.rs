use super::*;
use crate::{Key, MemoryMetadata, Row, TokenError, TransportError};
use async_trait::async_trait;
use std::time::Duration;
use zemdb_core::{
    encode_message, ClientMessage, DataType, RowBuilder, Schema, SchemaId, SequenceNumber,
    SequencedOperation, ServerMessage, TableSchema,
};
use zemdb_storage::MemoryStorageEngine;

/// Table `tasks`: an int key `id` and a string `title`.
fn tasks_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

fn room_id() -> RoomId {
    RoomId::new("room-a").unwrap()
}

/// A server of one room that answers registrations, with `Registered` at the cursor of the
/// registration, and nothing else: every other request waits for a reply forever.
#[derive(Debug, Clone)]
struct Server;

#[async_trait]
impl TransportClient for Server {
    async fn send(&self, message: &ClientMessage, _: &str) -> Result<Vec<u8>, TransportError> {
        let ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            current_seq,
            ..
        } = message
        else {
            return std::future::pending().await;
        };
        let reply = ServerMessage::Registered {
            correlation_id: *correlation_id,
            room_id: room_id.clone(),
            head_seq: current_seq.unwrap_or(SequenceNumber::new(0)),
            tail_seq: SequenceNumber::new(0),
            schema_id: SchemaId::new("tasks").unwrap(),
            schema: tasks_schema(),
            active_snapshot_seq: None,
            room_incarnation: 1,
        };
        Ok(encode_message(&reply).unwrap())
    }
}

/// A token provider that gives no token: the room opens without one, and the server of the test
/// takes it.
#[derive(Debug)]
struct NoTokens;

#[async_trait]
impl TokenProvider for NoTokens {
    async fn token(&self, _: &ClientId, _: &RoomId) -> Result<String, TokenError> {
        Err(TokenError("No tokens".to_string()))
    }
}

#[tokio::test(start_paused = true)]
async fn room_asks_for_the_mode_again_for_a_stream_made_while_it_started_the_worker() {
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(59),
        ..ClientConfig::new("ana")
    };
    let (storage, metadata) = (MemoryStorageEngine::new(), MemoryMetadata::default());
    let client = ZemdbClient::open(config, storage, metadata, Server, NoTokens, None);
    let client = client.await.unwrap();
    let room = client.room(room_id()).await.unwrap();
    // The head moves past the cursor of the registration, so the worker that stops acknowledges
    // it, and waits for the reply that never comes (5 s).
    let row = RowBuilder::new().set("id", 1).set("title", "one").build();
    let insert = tasks_schema().to_operation_insert("tasks", &row, 1000);
    let insert = SequencedOperation::new(SequenceNumber::new(1), insert.unwrap());
    let storage = client.core().storage();
    storage.apply_batch(&room_id(), vec![insert]).await.unwrap();

    // While the worker stops, the room is opened again, whose worker waits for that stop, in the
    // mode read before: `Periodic`, without streams.
    let core = Arc::clone(client.core());
    let stopping = tokio::spawn(async move { core.stop_worker(room_id()).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.room(room_id()).await.map(drop) }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!opening.is_finished());

    // A stream made meanwhile has no worker that runs to ask; the room asks the worker that it
    // starts once the stop ended, for `Live`.
    let _watched = room
        .table("tasks")
        .unwrap()
        .watch(&Key::new().with("id", 1));
    stopping.await.unwrap().unwrap();
    opening.await.unwrap().unwrap();
    let worker = room.status().await.unwrap().worker.unwrap();
    assert_eq!((worker.freshness, worker.running), (Freshness::Live, true));
}

/// A server of one room that answers registrations as [`Server`] does, and no other request:
/// each gets no reply at once.
#[derive(Debug, Clone)]
struct Unreachable;

#[async_trait]
impl TransportClient for Unreachable {
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError> {
        match message {
            ClientMessage::RegisterClient { .. } => Server.send(message, token).await,
            _ => Err(TransportError("No reply".to_string())),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_room_without_a_worker_writes_as_the_core_does() {
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(59),
        ..ClientConfig::new("ana")
    };
    let (storage, metadata) = (MemoryStorageEngine::new(), MemoryMetadata::default());
    let client = ZemdbClient::open(config, storage, metadata, Unreachable, NoTokens, None);
    let client = client.await.unwrap();
    let room = client.room(room_id()).await.unwrap();
    // Without a worker (stopped by the core, as a call of `room` cut once the room opened leaves
    // it without one), after a request without a reply, the write is not offline: it is sent,
    // and retried until its deadline, as a write of the core.
    client.core().stop_worker(room_id()).await.unwrap();
    assert!(matches!(room.sync().await, Err(ClientError::Network(_))));
    let row = Row::new().with("id", 1).with("title", "one");
    let written = room.table("tasks").unwrap().insert(&row).await;
    assert_eq!(written.map(drop), Err(ClientError::OutcomeUnknown));
}
