//! Opening and shutting down the client, and the transport to the server in process.

mod common;

use common::*;
use std::path::Path;
use tempfile::tempdir;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, MemoryMetadata, MetadataStore, TransportClient,
};
use zemdb_core::*;
use zemdb_storage::MemoryStorageEngine;

/// A client of `person` over `metadata`, with a memory engine and a server it does not reach.
async fn open_with(
    person: &str,
    metadata: impl MetadataStore + 'static,
) -> Result<Client, ClientError> {
    let transport = TestServer::new().transport();
    let storage = MemoryStorageEngine::default();
    Client::open(ClientConfig::new(person), storage, metadata, transport).await
}

async fn open(dir: &Path) -> Result<Client, ClientError> {
    open_with("ana", FileMetadata::new(dir)).await
}

#[tokio::test]
async fn the_client_id_is_the_person_and_the_device_id_of_the_directory() {
    let (dir, other) = (tempdir().unwrap(), tempdir().unwrap());
    let client_id = open(dir.path()).await.unwrap().client_id();
    let (person, device_id) = client_id.as_str().split_once('#').unwrap();
    assert_eq!((person, client_id.owner()), ("ana", "ana"));
    assert_eq!(device_id.len(), 32);
    assert!(device_id
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));

    assert_eq!(open(dir.path()).await.unwrap().client_id(), client_id);
    assert_ne!(open(other.path()).await.unwrap().client_id(), client_id);
}

#[tokio::test]
async fn a_second_client_over_the_same_directory_fails_until_the_first_shuts_down() {
    let dir = tempdir().unwrap();
    let first = open(dir.path()).await.unwrap();
    assert!(matches!(
        open(dir.path()).await,
        Err(ClientError::Storage(_))
    ));
    first.shutdown().await.unwrap();
    assert_eq!(
        open(dir.path()).await.unwrap().client_id(),
        first.client_id()
    );
}

#[tokio::test]
async fn a_person_that_is_empty_or_holds_a_hash_is_a_configuration_error() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    for person in ["", "ana#phone", "#", " ana", "a\nb"] {
        let opened = open_with(person, FileMetadata::new(&data_dir)).await;
        assert!(matches!(opened, Err(ClientError::Config(_))), "{person:?}");
    }
    // Rejected before the metadata are touched.
    assert!(!data_dir.exists());
}

#[tokio::test]
async fn an_error_reading_the_device_id_is_a_storage_error() {
    let dir = tempdir().unwrap();
    let client_id = open(dir.path()).await.unwrap().client_id();
    let flaky =
        FileMetadata::with_files(dir.path(), TestFiles::failing("open_read", "client.meta"));
    let opened = open_with("ana", flaky).await;
    assert!(matches!(opened, Err(ClientError::Storage(_))));
    assert_eq!(open(dir.path()).await.unwrap().client_id(), client_id);
}

#[tokio::test]
async fn a_client_without_rooms_has_every_room_closed() {
    let client = open_with("ana", MemoryMetadata::default()).await.unwrap();
    let room_id = RoomId::new("room-a").unwrap();
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(client.status(room_id.clone()).await, closed);
    client.shutdown().await.unwrap();
    assert_eq!(client.status(room_id).await, closed);
}

#[tokio::test]
async fn every_message_reaches_the_endpoint_of_its_kind_through_the_test_transport() {
    let server = TestServer::new();
    let transport = server.transport();
    let room_id = server.create_room("room-a").await;
    let client = open_with("ana", MemoryMetadata::default()).await.unwrap();
    let client_id = client.client_id();
    let token = token(&client_id, &room_id);
    let send = |message: ClientMessage| {
        let (transport, token) = (transport.clone(), token.clone());
        async move {
            let reply = transport.send(&message, &token).await.unwrap();
            decode_message::<ServerMessage>(&reply).unwrap()
        }
    };
    let correlation_id = CorrelationId::new(1);

    let registered = send(ClientMessage::RegisterClient {
        correlation_id,
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        auth_token: token.clone(),
        current_seq: None,
        known_incarnation: None,
    })
    .await;
    let ServerMessage::Registered {
        room_incarnation, ..
    } = registered
    else {
        panic!("{registered:?}");
    };

    type Expected = fn(&ServerMessage) -> bool;
    let (room, client) = (room_id.clone(), client_id.clone());
    let replies: Vec<(ClientMessage, Expected)> = vec![
        (
            ClientMessage::Commit {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                mutation_id: MutationId::from(1u128),
                last_ack_seq: SequenceNumber::new(0),
                op: insert_task(1),
            },
            |reply| matches!(reply, ServerMessage::CommitAck { .. }),
        ),
        (
            ClientMessage::Sync {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                from_seq: SequenceNumber::new(0),
                max_batch_size: 10,
            },
            |reply| matches!(reply, ServerMessage::SyncBatch { .. }),
        ),
        (
            ClientMessage::Ack {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                ack_seq: SequenceNumber::new(1),
            },
            |reply| matches!(reply, ServerMessage::AckConfirmed { .. }),
        ),
        (
            ClientMessage::Heartbeat {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
            },
            |reply| matches!(reply, ServerMessage::HeartbeatAck { .. }),
        ),
        (
            ClientMessage::GetSchema {
                correlation_id,
                room_id: room.clone(),
            },
            |reply| matches!(reply, ServerMessage::Schema { .. }),
        ),
        (
            // The room has no snapshot.
            ClientMessage::RequestSnapshotChunk {
                correlation_id,
                room_id: room.clone(),
                chunk_index: 0,
                chunk_size: 1 << 16,
                snapshot_hash: None,
                room_incarnation,
            },
            |reply| {
                matches!(
                    reply,
                    ServerMessage::Error {
                        code: ErrorCode::RoomNotFound,
                        ..
                    }
                )
            },
        ),
        (
            // A chunk of another incarnation.
            ClientMessage::UploadSnapshotChunk {
                correlation_id,
                room_id: room.clone(),
                snapshot_head_seq: SequenceNumber::new(1),
                chunk_index: 0,
                total_chunks: 1,
                total_bytes: 4,
                snapshot_hash: [0; 32],
                data: b"zemd".to_vec().into(),
                room_incarnation: room_incarnation.wrapping_add(1),
            },
            |reply| {
                matches!(
                    reply,
                    ServerMessage::Error {
                        code: ErrorCode::RoomReset,
                        ..
                    }
                )
            },
        ),
        (
            ClientMessage::DeregisterClient {
                correlation_id,
                room_id: room,
                client_id: client,
            },
            |reply| matches!(reply, ServerMessage::DeregisterAck { .. }),
        ),
    ];
    for (message, expected) in replies {
        let kind = message.kind();
        let reply = send(message).await;
        assert!(expected(&reply), "{kind:?}: {reply:?}");
    }
}
