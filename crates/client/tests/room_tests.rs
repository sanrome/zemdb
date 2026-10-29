//! Opening, registering, discarding and closing the rooms of the client, against the server in
//! process.

mod common;

use common::*;
use std::io;
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, MemoryMetadata, MetadataStore, RoomMetadata,
    RoomStatus,
};
use zemdb_core::*;
use zemdb_storage::format::{BATCH_HEADER_SIZE, HEADER_SIZE};
use zemdb_storage::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};

fn closed(room_id: &RoomId) -> Result<RoomStatus, ClientError> {
    Err(ClientError::RoomClosed(room_id.clone()))
}

/// Applies `ops` to `room_id` of `storage`, each in a batch of its own.
async fn apply(storage: &impl StorageEngine, room_id: &RoomId, ops: Vec<SequencedOperation>) {
    for op in ops {
        storage.apply_batch(room_id, vec![op]).await.unwrap();
    }
}

/// Which of the tasks 1 to 3 `room_id` of `storage` holds.
async fn tasks(storage: &impl StorageEngine, room_id: &RoomId) -> Vec<i64> {
    let mut tasks = Vec::new();
    for id in 1..=3 {
        let row = storage.get(room_id, "tasks", &PrimaryKey::single(id)).await;
        if row.unwrap().is_some() {
            tasks.push(id);
        }
    }
    tasks
}

/// Leaves in `dir` the replica of `room_id` of a client that opened it, applied the tasks 1 to
/// 3 committed to the server, and shut down. Returns the incarnation of the room.
async fn replica(server: &TestServer, dir: &Path, room_id: &RoomId) -> u128 {
    let storage = engine(dir);
    let client = open_client(dir, storage.clone(), &server.transport()).await;
    open_room(&client, room_id).await.unwrap();
    apply(&storage, room_id, server.commit(room_id, 1..=3).await).await;
    client.shutdown().await.unwrap();
    server.incarnation(room_id).await
}

/// Moves the operations of the replica of `room_id` in `dir` into its snapshot.
async fn compact(dir: &Path, room_id: &RoomId) {
    let storage = engine(dir);
    storage.open_room(room_id, tasks_schema()).await.unwrap();
    storage.compact_room(room_id).await.unwrap();
    storage.close_room(room_id).await.unwrap();
}

async fn metadata(dir: &Path, room_id: &RoomId) -> Option<RoomMetadata> {
    FileMetadata::new(dir).room(room_id).await.unwrap()
}

/// Flips the bits of the byte at `index` of the file `name` of `dir`.
fn damage(dir: &Path, name: &str, index: usize) {
    let path = dir.join(name);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[index] ^= 0xFF;
    std::fs::write(&path, bytes).unwrap();
}

/// `Registered` for the registration `correlation_id` of `room_id`, at the head of a replica.
fn registered(correlation_id: CorrelationId, room_id: &RoomId, room_incarnation: u128) -> Vec<u8> {
    encode_message(&ServerMessage::Registered {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(3),
        tail_seq: SequenceNumber::new(0),
        schema_id: SchemaId::new("tasks").unwrap(),
        schema: tasks_schema(),
        active_snapshot_seq: None,
        room_incarnation,
    })
    .unwrap()
}

#[tokio::test]
async fn a_new_client_keeps_the_metadata_of_registered_and_reopens_with_its_cursor() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    // Opened without metadata, so its data were deleted.
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    let saved = RoomMetadata {
        room_incarnation: server.incarnation(&room_id).await,
        schema_id: SchemaId::new("tasks").unwrap(),
        schema: tasks_schema(),
    };
    assert_eq!(metadata(dir.path(), &room_id).await, Some(saved.clone()));
    apply(&storage, &room_id, server.commit(&room_id, 1..=3).await).await;
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));
    client.shutdown().await.unwrap();

    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    let cursor = (Some(3), Some(saved.room_incarnation));
    assert_eq!(transport.registrations(), [(None, None), cursor]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 0, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
    assert_eq!(metadata(dir.path(), &room_id).await, Some(saved));
}

#[tokio::test]
async fn a_replica_that_opens_behind_presents_its_head_and_is_kept() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let incarnation = replica(&server, dir.path(), &room_id).await;
    // The snapshot is lost after a compaction that emptied the log.
    compact(dir.path(), &room_id).await;
    std::fs::remove_file(dir.path().join("room_room-a.snap")).unwrap();

    let transport = server.transport();
    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(transport.registrations(), [(Some(0), Some(incarnation))]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 0, true)));
}

#[tokio::test]
async fn a_room_created_again_resets_the_replica_which_registers_without_a_cursor() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let old = replica(&server, dir.path(), &room_id).await;
    server.recreate_room(&room_id).await;

    let transport = server.transport();
    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(
        transport.registrations(),
        [(Some(3), Some(old)), (None, None)]
    );
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    assert!(tasks(&storage, &room_id).await.is_empty());
    let new = server.incarnation(&room_id).await;
    assert_ne!(new, old);
    let saved = metadata(dir.path(), &room_id).await.unwrap();
    assert_eq!(saved.room_incarnation, new);
}

#[tokio::test]
async fn an_invalid_sequence_to_the_cursor_discards_the_replica() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let incarnation = replica(&server, dir.path(), &room_id).await;
    let transport = server.transport();
    // As from a server that lost the replica's sequences.
    transport.answer_next_registration(|correlation_id, room_id| {
        error_reply(correlation_id, room_id, ErrorCode::InvalidSequence)
    });

    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    let cursor = (Some(3), Some(incarnation));
    assert_eq!(transport.registrations(), [cursor, (None, None)]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    assert!(tasks(&storage, &room_id).await.is_empty());
    let saved = metadata(dir.path(), &room_id).await.unwrap();
    assert_eq!(saved.room_incarnation, incarnation);
}

#[tokio::test]
async fn missing_metadata_and_damaged_data_discard_the_replica() {
    for case in [
        "missing metadata",
        "damaged metadata",
        "damaged snapshot",
        "damaged log",
    ] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        replica(&server, dir.path(), &room_id).await;
        match case {
            "missing metadata" => {
                std::fs::remove_file(dir.path().join("room_room-a.meta")).unwrap()
            }
            "damaged metadata" => damage(dir.path(), "room_room-a.meta", 0),
            "damaged snapshot" => {
                compact(dir.path(), &room_id).await;
                damage(dir.path(), "room_room-a.snap", HEADER_SIZE + 2);
            }
            // The first of the three batches of the log, which valid batches follow.
            _ => damage(dir.path(), "room_room-a.wal", BATCH_HEADER_SIZE + 2),
        }

        let transport = server.transport();
        let storage = engine(dir.path());
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        open_room(&client, &room_id).await.unwrap();
        assert_eq!(transport.registrations(), [(None, None)], "{case}");
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(0, 1, true)), "{case}");
        assert!(tasks(&storage, &room_id).await.is_empty(), "{case}");
        let saved = metadata(dir.path(), &room_id).await.unwrap();
        assert_eq!(saved.room_incarnation, server.incarnation(&room_id).await);
    }
}

#[tokio::test]
async fn errors_other_than_damage_keep_the_replica() {
    for (case, error) in [
        ("room locked", "is locked"),
        ("I/O error", "Injected"),
        ("schema mismatch", "which its schema does not have"),
        (
            "snapshot constraints",
            "break the constraint 'unique_title'",
        ),
        ("unreadable metadata", "injected"),
    ] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        let incarnation = replica(&server, dir.path(), &room_id).await;
        let saved = metadata(dir.path(), &room_id).await.unwrap();
        let save = |schema| {
            let metadata = RoomMetadata {
                schema,
                ..saved.clone()
            };
            let dir = dir.path().to_path_buf();
            let room_id = room_id.clone();
            async move { FileMetadata::new(dir).save_room(&room_id, &metadata).await }
        };
        let storage = TestEngine::new(engine(dir.path()));
        let other = engine(dir.path());
        let mut files = TestFiles::default();
        match case {
            "room locked" => other.open_room(&room_id, tasks_schema()).await.unwrap(),
            "I/O error" => {
                let error = io::Error::new(io::ErrorKind::PermissionDenied, "Injected");
                storage.fail_next("open_room", StorageError::Io(error));
            }
            // A schema without the table of the replica's data.
            "schema mismatch" => save(Schema::builder().build()).await.unwrap(),
            // The rows of the snapshot repeat the title, which this schema makes unique.
            "snapshot constraints" => {
                compact(dir.path(), &room_id).await;
                let table = TableSchema::builder("tasks")
                    .primary_key("id", DataType::Int)
                    .column("title", DataType::String)
                    .unique("unique_title", ["title"]);
                save(Schema::builder().table(table).build()).await.unwrap();
            }
            _ => files = TestFiles::failing("open_read", "room_room-a.meta"),
        }

        let transport = server.transport();
        let metadata = FileMetadata::with_files(dir.path(), files);
        let client = Client::open(
            ClientConfig::new("ana"),
            storage,
            metadata,
            transport.clone(),
        )
        .await
        .unwrap();
        let opened = open_room(&client, &room_id).await;
        let failed =
            matches!(&opened, Err(ClientError::Storage(message)) if message.contains(error));
        assert!(failed, "{case}: {opened:?}");
        assert_eq!(
            client.status(room_id.clone()).await,
            closed(&room_id),
            "{case}"
        );
        assert!(transport.registrations().is_empty(), "{case}");
        client.shutdown().await.unwrap();

        // With the cause gone, the replica opens as it was.
        if case == "room locked" {
            other.close_room(&room_id).await.unwrap();
        }
        save(saved.schema.clone()).await.unwrap();
        let storage = engine(dir.path());
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        open_room(&client, &room_id).await.unwrap();
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(3, 0, true)), "{case}");
        let cursor = (Some(3), Some(incarnation));
        assert_eq!(transport.registrations(), [cursor], "{case}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{case}");
    }
}

#[tokio::test]
async fn a_discard_removes_the_metadata_before_it_touches_the_storage_room() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    replica(&server, dir.path(), &room_id).await;
    // What a save cut before its rename leaves.
    std::fs::write(dir.path().join("room_room-a.meta.tmp"), "cut").unwrap();
    server.recreate_room(&room_id).await;

    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let metadata = FileMetadata::with_files(dir.path(), files.clone());
    let transport = server.transport();
    let client = Client::open(
        ClientConfig::new("ana"),
        storage,
        metadata,
        transport.clone(),
    );
    open_room(&client.await.unwrap(), &room_id).await.unwrap();

    let log = files.log();
    let removed = log
        .iter()
        .position(|entry| entry == "remove room_room-a.meta");
    let discard = &log[removed.unwrap()..];
    let dir_name = dir.path().file_name().unwrap().to_str().unwrap();
    let synced = format!("sync_dir {dir_name}");
    assert_eq!(
        discard[..3],
        [
            "remove room_room-a.meta",
            "remove room_room-a.meta.tmp",
            &synced
        ]
    );
    assert!(discard[3..].contains(&"remove room_room-a.wal".to_string()));
    // The metadata of `Registered` are saved before the storage room opens.
    let at = |entry: &str| log.iter().rposition(|logged| logged == entry).unwrap();
    assert!(
        at("rename room_room-a.meta.tmp room_room-a.meta") < at("open_locked room_room-a.lock")
    );
}

#[tokio::test]
async fn a_failed_step_of_a_discard_leaves_the_room_out_and_its_next_open_deletes_again() {
    for (case, method) in [
        ("an open without metadata", "delete_room"),
        ("a discard after a reset", "delete_room"),
        ("a discard after a reset", "close_room"),
    ] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        let old = replica(&server, dir.path(), &room_id).await;
        let reset = case == "a discard after a reset";
        match reset {
            true => server.recreate_room(&room_id).await,
            false => std::fs::remove_file(dir.path().join("room_room-a.meta")).unwrap(),
        }

        let transport = server.transport();
        let storage = TestEngine::new(engine(dir.path()));
        storage.fail_next(method, StorageError::RoomLocked(room_id.clone()));
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        let opened = open_room(&client, &room_id).await;
        let case = format!("{method} in {case}");
        assert!(
            matches!(opened, Err(ClientError::Storage(_))),
            "{case}: {opened:?}"
        );
        assert_eq!(
            client.status(room_id.clone()).await,
            closed(&room_id),
            "{case}"
        );
        let sent = if reset {
            vec![(Some(3), Some(old))]
        } else {
            vec![]
        };
        assert_eq!(transport.registrations(), sent, "{case}");
        assert_eq!(metadata(dir.path(), &room_id).await, None, "{case}");

        open_room(&client, &room_id).await.unwrap();
        assert_eq!(
            transport.registrations().last(),
            Some(&(None, None)),
            "{case}"
        );
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(0, 1, true)), "{case}");
        assert!(tasks(&storage, &room_id).await.is_empty(), "{case}");
    }
}

#[tokio::test]
async fn a_failed_save_of_the_metadata_of_registered_leaves_the_room_out() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let storage = engine(dir.path());
    let files = TestFiles::failing("rename", "room_room-a.meta");
    let metadata_store = FileMetadata::with_files(dir.path(), files);
    let client = Client::open(
        ClientConfig::new("ana"),
        storage.clone(),
        metadata_store,
        transport.clone(),
    )
    .await
    .unwrap();
    let opened = open_room(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Storage(_))), "{opened:?}");
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
    let head = storage.get_head_seq(&room_id).await;
    assert!(
        matches!(head, Err(StorageError::RoomNotFound(_))),
        "{head:?}"
    );
    assert_eq!(metadata(dir.path(), &room_id).await, None);
    client.shutdown().await.unwrap();

    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(transport.registrations(), [(None, None), (None, None)]);
    assert_eq!(client.status(room_id).await, Ok(status(0, 1, true)));
}

#[tokio::test]
async fn a_discard_whose_metadata_are_not_removed_keeps_the_data() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let old = replica(&server, dir.path(), &room_id).await;
    server.recreate_room(&room_id).await;

    let transport = server.transport();
    let files = TestFiles::failing("remove", "room_room-a.meta");
    let metadata_store = FileMetadata::with_files(dir.path(), files);
    let client = Client::open(
        ClientConfig::new("ana"),
        engine(dir.path()),
        metadata_store,
        transport.clone(),
    )
    .await
    .unwrap();
    let opened = open_room(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Storage(_))), "{opened:?}");
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
    assert_eq!(transport.registrations(), [(Some(3), Some(old))]);
    client.shutdown().await.unwrap();

    let saved = metadata(dir.path(), &room_id).await.unwrap();
    assert_eq!(saved.room_incarnation, old);
    let storage = engine(dir.path());
    storage.open_room(&room_id, tasks_schema()).await.unwrap();
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
}

#[tokio::test]
async fn without_network_a_room_with_metadata_opens_unregistered_and_one_without_fails() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let other = server.create_room("room-b").await;
    let incarnation = replica(&server, dir.path(), &room_id).await;
    let transport = server.transport();
    transport.offline();

    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(3, 0, false))
    );
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
    let opened = open_room(&client, &other).await;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    assert_eq!(client.status(other.clone()).await, closed(&other));
    assert_eq!(metadata(dir.path(), &other).await, None);

    // Opened again with the network, they register.
    transport.online();
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 0, true)));
    let cursor = (Some(3), Some(incarnation));
    assert_eq!(transport.registrations().last(), Some(&cursor));
    // Its replica stayed empty: it registers without deleting again.
    open_room(&client, &other).await.unwrap();
    assert_eq!(client.status(other.clone()).await, Ok(status(0, 1, true)));

    // A token that the server refuses leaves a room unregistered, and a valid one registers it.
    client.close_room(room_id.clone()).await.unwrap();
    let refused = token(&client.client_id(), &other);
    client.open_room(room_id.clone(), refused).await.unwrap();
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(3, 0, false))
    );
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(client.status(room_id).await, Ok(status(3, 0, true)));
}

#[tokio::test]
async fn a_memory_replica_reopens_empty_with_its_metadata_and_its_cursor() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let incarnation = server.incarnation(&room_id).await;
    let transport = server.transport();
    let storage = MemoryStorageEngine::default();
    let metadata_store = MemoryMetadata::default();
    let client = Client::open(
        ClientConfig::new("ana"),
        storage.clone(),
        metadata_store,
        transport.clone(),
    )
    .await
    .unwrap();
    open_room(&client, &room_id).await.unwrap();
    apply(&storage, &room_id, server.commit(&room_id, 1..=3).await).await;
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));

    // A memory room holds nothing once closed, and its metadata stay: it reopens empty, at the
    // cursor 0 of its incarnation, to catch up.
    client.close_room(room_id.clone()).await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(
        transport.registrations(),
        [(None, None), (Some(0), Some(incarnation))]
    );
    assert_eq!(client.status(room_id).await, Ok(status(0, 1, true)));
}

#[tokio::test]
async fn concurrent_opens_of_a_room_register_it_once() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    let (first, second) = tokio::join!(open_room(&client, &room_id), open_room(&client, &room_id));
    first.unwrap();
    second.unwrap();
    assert_eq!(transport.registrations(), [(None, None)]);
    assert_eq!(client.status(room_id).await, Ok(status(0, 1, true)));
}

#[tokio::test]
async fn a_room_closed_while_it_registers_stays_closed_and_keeps_its_replica() {
    // Without metadata, and with metadata whose registration gets a reset.
    for reset in [false, true] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        let mut old = None;
        if reset {
            old = Some(replica(&server, dir.path(), &room_id).await);
            server.recreate_room(&room_id).await;
        }
        let transport = server.transport();
        transport.slow(Duration::from_millis(100));
        let storage = engine(dir.path());
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        let close = async {
            let sent = async {
                while transport.registrations().is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            let sent = tokio::time::timeout(Duration::from_secs(10), sent).await;
            sent.expect("no registration was sent");
            client.close_room(room_id.clone()).await
        };
        let (opened, closed_now) = tokio::join!(open_room(&client, &room_id), close);
        closed_now.unwrap();
        let room_closed = Err(ClientError::RoomClosed(room_id.clone()));
        assert_eq!(opened, room_closed, "{reset}");
        assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
        let head = storage.get_head_seq(&room_id).await;
        assert!(
            matches!(head, Err(StorageError::RoomNotFound(_))),
            "{head:?}"
        );
        let saved = metadata(dir.path(), &room_id).await;
        assert_eq!(saved.map(|saved| saved.room_incarnation), old, "{reset}");
    }
}

#[tokio::test]
async fn a_reply_that_does_not_answer_the_registration_changes_nothing() {
    type Reply = fn(CorrelationId, &RoomId, u128) -> Vec<u8>;
    fn another_room() -> RoomId {
        RoomId::new("room-b").unwrap()
    }
    fn next(correlation_id: CorrelationId) -> CorrelationId {
        CorrelationId::new(correlation_id.get() + 1)
    }
    let cases: [(&str, Reply); 8] = [
        ("Registered", registered),
        ("another correlation id", |id, room, inc| {
            registered(next(id), room, inc)
        }),
        ("another room", |id, _, inc| {
            registered(id, &another_room(), inc)
        }),
        ("another incarnation", |id, room, inc| {
            registered(id, room, inc ^ 1)
        }),
        ("a reset of another correlation id", |id, room, _| {
            error_reply(next(id), room, ErrorCode::RoomReset)
        }),
        ("a reset of another room", |id, _, _| {
            error_reply(id, &another_room(), ErrorCode::RoomReset)
        }),
        ("another kind of reply", |correlation_id, room_id, _| {
            encode_message(&ServerMessage::HeartbeatAck {
                correlation_id,
                room_id: room_id.clone(),
                current_head_seq: SequenceNumber::new(3),
                snapshot_wanted: false,
                active_snapshot_seq: None,
                schema_version: 1,
                schema_id: SchemaId::new("tasks").unwrap(),
            })
            .unwrap()
        }),
        ("bytes that do not decode", |_, _, _| b"zemd".to_vec()),
    ];
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let incarnation = replica(&server, dir.path(), &room_id).await;
    for (case, reply) in cases {
        let transport = server.transport();
        transport.answer_next_registration(move |id, room| reply(id, room, incarnation));
        let storage = engine(dir.path());
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        open_room(&client, &room_id).await.unwrap();
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(3, 0, case == "Registered")), "{case}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{case}");
        let saved = metadata(dir.path(), &room_id).await.unwrap();
        assert_eq!(saved.room_incarnation, incarnation, "{case}");
        client.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn without_metadata_a_registration_without_registered_fails_the_open() {
    // A reset or an invalid sequence without a cursor does not discard again.
    for code in [
        None,
        Some(ErrorCode::RoomReset),
        Some(ErrorCode::InvalidSequence),
    ] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        let transport = server.transport();
        transport.answer_next_registration(move |id, room_id| match code {
            Some(code) => error_reply(id, room_id, code),
            None => registered(CorrelationId::new(id.get() + 1), room_id, 7),
        });
        let client = open_client(dir.path(), engine(dir.path()), &transport).await;
        let opened = open_room(&client, &room_id).await;
        assert!(
            matches!(opened, Err(ClientError::Network(_))),
            "{code:?}: {opened:?}"
        );
        assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
        assert_eq!(transport.registrations(), [(None, None)], "{code:?}");
        assert_eq!(metadata(dir.path(), &room_id).await, None);
    }
}

#[tokio::test]
async fn closing_a_room_closes_its_storage_room_even_if_the_close_fails() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let incarnation = server.incarnation(&room_id).await;
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();

    client.close_room(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
    let head = storage.get_head_seq(&room_id).await;
    assert!(
        matches!(head, Err(StorageError::RoomNotFound(_))),
        "{head:?}"
    );
    let again = client.close_room(room_id.clone()).await;
    assert_eq!(again, Err(ClientError::RoomClosed(room_id.clone())));
    // A failed read of the head for the cursor fails the open, which does not register: the
    // replica stays open, unregistered.
    let error = io::Error::new(io::ErrorKind::PermissionDenied, "Injected");
    storage.fail_next("get_head_seq", StorageError::Io(error));
    let opened = open_room(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Storage(_))), "{opened:?}");
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(0, 1, false))
    );
    // Opened again, it keeps its epoch and registers with its cursor.
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    let cursor = (Some(0), Some(incarnation));
    assert_eq!(transport.registrations(), [(None, None), cursor]);
    // A read of the head that finds the storage room closed, as when a close wins it, is
    // `RoomClosed`.
    storage.fail_next("get_head_seq", StorageError::RoomNotFound(room_id.clone()));
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));

    let error = io::Error::new(io::ErrorKind::PermissionDenied, "Injected");
    storage.fail_next("close_room", StorageError::Io(error));
    let failed = client.close_room(room_id.clone()).await;
    assert!(matches!(failed, Err(ClientError::Storage(_))), "{failed:?}");
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
    open_room(&client, &room_id).await.unwrap();
}

#[tokio::test]
async fn shutting_down_closes_every_room_and_reports_a_failed_close() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let rooms = [
        server.create_room("room-a").await,
        server.create_room("room-b").await,
    ];
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    for room_id in &rooms {
        open_room(&client, room_id).await.unwrap();
    }
    let error = io::Error::new(io::ErrorKind::PermissionDenied, "Injected");
    storage.fail_next("close_room", StorageError::Io(error));
    let shut = client.shutdown().await;
    assert!(matches!(shut, Err(ClientError::Storage(_))), "{shut:?}");
    for room_id in &rooms {
        assert_eq!(client.status(room_id.clone()).await, closed(room_id));
        let head = storage.get_head_seq(room_id).await;
        assert!(
            matches!(head, Err(StorageError::RoomNotFound(_))),
            "{head:?}"
        );
    }
    // The metadata were released all the same.
    open_client(dir.path(), engine(dir.path()), &transport).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutting_down_with_a_compaction_in_progress_closes_the_room_and_loses_nothing() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    apply(&storage, &room_id, server.commit(&room_id, 1..=3).await).await;

    files.slow(Duration::from_millis(300));
    tokio::select! {
        _ = storage.compact_room(&room_id) => panic!("the write is slow"),
        () = files.logged("write_at room_room-a.snap.tmp.") => {}
    }
    client.shutdown().await.unwrap();
    // The compaction ended before the room closed.
    let installed = |entry: &String| {
        entry.starts_with("rename room_room-a.snap.tmp.") && entry.ends_with(" room_room-a.snap")
    };
    assert!(files.log().iter().any(installed), "{:?}", files.log());
    let head = storage.get_head_seq(&room_id).await;
    assert!(
        matches!(head, Err(StorageError::RoomNotFound(_))),
        "{head:?}"
    );

    // Reopened in the same process.
    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 0, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_waits_for_a_change_of_the_replica_cut_by_its_caller() {
    for shutdown in [false, true] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        let transport = server.transport();
        let files = TestFiles::default();
        let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
        let metadata_store = FileMetadata::with_files(dir.path(), files.clone());
        let client = Client::open(
            ClientConfig::new("ana"),
            storage.clone(),
            metadata_store,
            transport.clone(),
        )
        .await
        .unwrap();

        // The open is cut while it saves the metadata of `Registered`.
        files.slow(Duration::from_millis(300));
        tokio::select! {
            _ = open_room(&client, &room_id) => panic!("the write is slow"),
            () = files.logged("write_at room_room-a.meta.tmp") => {}
        }
        // The change saved the metadata and opened the storage room, and then the room closed.
        match shutdown {
            true => client.shutdown().await.unwrap(),
            false => client.close_room(room_id.clone()).await.unwrap(),
        }
        assert!(metadata(dir.path(), &room_id).await.is_some(), "{shutdown}");
        let head = storage.get_head_seq(&room_id).await;
        assert!(
            matches!(head, Err(StorageError::RoomNotFound(_))),
            "{head:?}"
        );
        if shutdown {
            // The metadata are released once the storage room is closed.
            let log = files.log();
            let at = |entry: &str| log.iter().rposition(|logged| logged == entry).unwrap();
            assert!(
                at("rename room_room-a.meta.tmp room_room-a.meta") < at("close room_room-a.lock")
            );
            assert!(at("close room_room-a.lock") < at("close client.lock"));
            // No room opens after the shutdown.
            let opened = open_room(&client, &room_id).await;
            assert_eq!(opened, Err(ClientError::RoomClosed(room_id.clone())));
        }
    }
}
