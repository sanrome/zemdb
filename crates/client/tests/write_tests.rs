//! The writes of the client, against the server in process with a paused clock: each ends once,
//! with the decision of the replica on its sequence, a refusal of the server, an error without
//! effect or an unknown outcome.

mod common;

use common::*;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::Instant;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, RoomStatus, TransportClient, TransportError,
    Violation,
};
use zemdb_core::ClientMessageKind::{Commit, Sync};
use zemdb_core::*;
use zemdb_storage::{DiskStorageEngine, RejectReason, StorageEngine, StorageError};

/// The schema `tasks` of the rooms of [`TestServer::create_room`], at its first version.
const TASKS: (&str, u64) = ("tasks", 1);

/// [`tasks_schema`] with the nullable string column `tag`, unique.
fn tagged_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("tag", DataType::String)
        .unique("tasks_tag", ["tag"]);
    Schema::builder().table(table).build()
}

/// An insert of row `id` with `tag` into `tasks` of [`tagged_schema`].
fn insert_tagged(id: i64, tag: &str) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "task")
        .set("tag", tag)
        .build();
    tagged_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// [`tasks_schema`] with `title` an int, which does not evolve from it.
fn retyped_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    Schema::builder().table(table).build()
}

/// A `CommitAck` for the commit `message`, which assigns it `seq` with `ops` as its catch-up and
/// announces the version `version` of the schema `schema_id`.
fn commit_ack(
    message: &ClientMessage,
    seq: u64,
    ops: &[SequencedOperation],
    has_more: bool,
    (schema_id, version): (&str, u64),
) -> Vec<u8> {
    let ClientMessage::Commit {
        correlation_id,
        room_id,
        mutation_id,
        ..
    } = message
    else {
        panic!("{message:?}");
    };
    encode_message(&ServerMessage::CommitAck {
        correlation_id: *correlation_id,
        room_id: room_id.clone(),
        mutation_id: *mutation_id,
        assigned_seq: SequenceNumber::new(seq),
        catchup_ops: ops.to_vec(),
        has_more,
        snapshot_wanted: false,
        schema_version: version,
        schema_id: SchemaId::new(schema_id).unwrap(),
    })
    .unwrap()
}

/// The reply to `message`, a commit, with an error with `code`.
fn refusal(message: &ClientMessage, code: ErrorCode) -> Vec<u8> {
    let (correlation_id, room_id) = request(message);
    error_reply(correlation_id, &room_id, code)
}

/// A `SyncBatch` without operations for the sync `message`, at the head 0, that announces the
/// schema `other`.
fn other_schema_batch(message: &ClientMessage) -> Vec<u8> {
    let (correlation_id, room_id) = request(message);
    encode_message(&ServerMessage::SyncBatch {
        correlation_id,
        room_id,
        head_seq: SequenceNumber::new(0),
        ops: Vec::new(),
        has_more: false,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("other").unwrap(),
    })
    .unwrap()
}

/// A `Registered` for the registration `message` with `schema` as `schema_id`, at the head 0.
fn registered(
    message: &ClientMessage,
    room_incarnation: u128,
    schema_id: &str,
    schema: Schema,
) -> Vec<u8> {
    let ClientMessage::RegisterClient {
        correlation_id,
        room_id,
        ..
    } = message
    else {
        panic!("{message:?}");
    };
    encode_message(&ServerMessage::Registered {
        correlation_id: *correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(0),
        tail_seq: SequenceNumber::new(0),
        schema_id: SchemaId::new(schema_id).unwrap(),
        schema,
        active_snapshot_seq: None,
        room_incarnation,
    })
    .unwrap()
}

/// A `Registered` for the registration `message` with [`retyped_schema`] as the schema
/// `other`, which does not evolve from [`tasks_schema`], at the head 0.
fn retyped_registered(message: &ClientMessage, room_incarnation: u128) -> Vec<u8> {
    registered(message, room_incarnation, "other", retyped_schema())
}

/// `reply`, with `change` applied to its message.
fn changed(reply: Vec<u8>, change: impl FnOnce(&mut ServerMessage)) -> Vec<u8> {
    let mut message = decode_message(&reply).unwrap();
    change(&mut message);
    encode_message(&message).unwrap()
}

/// A `SyncBatch` for the sync `correlation_id` of `room_id` with `ops`, more after them, and
/// the head 9.
fn batch_to_nine(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    ops: Vec<SequencedOperation>,
) -> Vec<u8> {
    let batch = ServerMessage::SyncBatch {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(9),
        ops,
        has_more: true,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("tasks").unwrap(),
    };
    encode_message(&batch).unwrap()
}

/// Makes the next sync through `transport` fail its room closed: its batch announces another
/// schema, and the registration that follows gets one that does not evolve from the replica's.
fn fail_next_sync_closed(transport: &TestTransport, room_incarnation: u128) {
    transport.answer(move |message| match message {
        ClientMessage::Sync { .. } => Some(Ok(other_schema_batch(message))),
        ClientMessage::RegisterClient { .. } => {
            Some(Ok(retyped_registered(message, room_incarnation)))
        }
        _ => None,
    });
}

/// The writes in flight and what `room_id` of `client` retains for them.
async fn in_flight(client: &zemdb_client::Client, room_id: &RoomId) -> (usize, usize) {
    let RoomStatus {
        writes_in_flight,
        retained,
        ..
    } = client.status(room_id.clone()).await.unwrap();
    (writes_in_flight, retained)
}

/// The head of `room_id` of `client`.
async fn head(client: &zemdb_client::Client, room_id: &RoomId) -> u64 {
    client.status(room_id.clone()).await.unwrap().head_seq.get()
}

/// Waits until the server replied to the message `held` holds, failing if `write` ends first.
async fn until_replied<T: std::fmt::Debug>(write: impl Future<Output = T>, held: &mut Held) {
    tokio::select! {
        written = write => panic!("The write ended first: {written:?}"),
        () = held.replied() => {}
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_ends_with_the_decision_of_the_replica_on_its_sequence() {
    let dir = tempdir().unwrap();
    let (server, room_id, _transport, storage, client) =
        setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, [1]).await;

    // Accepted: the replica applied the catch-up up to its sequence.
    assert_eq!(client.write(room_id.clone(), insert_task(2)).await, Ok(()));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(2, 1, true)));
    // An update of a row that does not exist.
    let updated = client.write(room_id.clone(), update_task(9)).await;
    assert_eq!(updated, Err(ClientError::RowMissing));
    assert_eq!(head(&client, &room_id).await, 3);

    // A row with the tag of another breaks the constraint, by its name.
    let tagged = server
        .create_room_with("room-b", "tagged", tagged_schema())
        .await;
    open_room(&client, &tagged).await.unwrap();
    server.commit_ops(&tagged, [insert_tagged(1, "a")]).await;
    let unique = Violation::Unique {
        constraint: "tasks_tag".to_string(),
    };
    let written = client.write(tagged.clone(), insert_tagged(2, "a")).await;
    assert_eq!(written, Err(ClientError::ConstraintViolation(unique)));
    let written = client.write(tagged.clone(), insert_tagged(3, "b")).await;
    assert_eq!(written, Ok(()));
    assert_eq!(tasks(&storage, &tagged).await, [1, 3]);
    assert_eq!(in_flight(&client, &tagged).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_lost_reply_is_retried_with_the_same_mutation_id_and_gets_its_sequence() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    for (op, decision) in [
        (insert_task(1), Ok(())),
        (update_task(9), Err(ClientError::RowMissing)),
    ] {
        transport.lose_next_reply(Commit);
        let before = commits(&transport).len();
        assert_eq!(client.write(room_id.clone(), op).await, decision);
        let sent = &commits(&transport)[before..];
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0, sent[1].0);
    }
    // Each write got one sequence.
    assert_eq!(head(&client, &room_id).await, 2);
    assert_eq!(server.commit(&room_id, [3]).await[0].seq.get(), 3);
    assert_eq!(tasks(&storage, &room_id).await, [1]);
}

#[tokio::test(start_paused = true)]
async fn a_refusal_after_an_uncertain_attempt_is_an_unknown_outcome() {
    let lost = || Err(TransportError("Lost".to_string()));
    for (first, uncertain) in [
        (Some(ErrorCode::Unavailable), true),
        (Some(ErrorCode::Timeout), true),
        (Some(ErrorCode::Internal), true),
        (None, true),
        (Some(ErrorCode::RoomLocked), false),
        (Some(ErrorCode::RequestTimeout), false),
        (Some(ErrorCode::BehindCompaction), false),
        (Some(ErrorCode::ClientNotRegistered), false),
    ] {
        let dir = tempdir().unwrap();
        let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        let mut commits_sent = 0;
        transport.answer(move |message| {
            if message.kind() != Commit {
                return None;
            }
            commits_sent += 1;
            Some(match (commits_sent, first) {
                (1, Some(code)) => Ok(refusal(message, code)),
                (1, None) => lost(),
                _ => Ok(refusal(message, ErrorCode::SchemaViolation)),
            })
        });
        let written = client.write(room_id.clone(), insert_task(1)).await;
        match uncertain {
            true => assert_eq!(written, Err(ClientError::OutcomeUnknown), "{first:?}"),
            false => assert!(
                matches!(written, Err(ClientError::Validation(_))),
                "{first:?}: {written:?}"
            ),
        }
        assert_eq!(commits(&transport).len(), 2);
    }

    // Nor is any other final error.
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let mut commits_sent = 0;
    transport.answer(move |message| {
        (message.kind() == Commit).then(|| {
            commits_sent += 1;
            let code = match commits_sent {
                1 => ErrorCode::Unavailable,
                _ => ErrorCode::RoomNotFound,
            };
            Ok(refusal(message, code))
        })
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(start_paused = true)]
async fn an_error_of_the_storage_after_an_uncertain_attempt_is_an_unknown_outcome() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (_server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    let failing = storage.clone();
    transport.answer_next(Commit, move |message| {
        failing.fail_next("get_head_seq", StorageError::Other("injected".to_string()));
        refusal(message, ErrorCode::Unavailable)
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));

    // Also in the registration that follows: reading the head when `Registered` comes fails.
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    let incarnation = server.incarnation(&room_id).await;
    let mut commits_sent = 0;
    transport.answer(move |message| match message {
        ClientMessage::Commit { .. } => {
            commits_sent += 1;
            let code = match commits_sent {
                1 => ErrorCode::Unavailable,
                _ => ErrorCode::ClientNotRegistered,
            };
            Some(Ok(refusal(message, code)))
        }
        ClientMessage::RegisterClient { .. } => {
            let injected = StorageError::Other("injected".to_string());
            storage.fail_next("get_head_seq", injected);
            Some(Ok(registered(
                message,
                incarnation,
                "tasks",
                tasks_schema(),
            )))
        }
        _ => None,
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(start_paused = true)]
async fn a_write_registers_in_the_epoch_of_the_room_before_its_first_attempt() {
    // The room was created again while the replica was open without registering.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, [1]).await;
    client.sync(room_id.clone()).await.unwrap();
    client.close_room(room_id.clone()).await.unwrap();
    server.recreate_room(&room_id).await;
    transport.offline();
    open_room(&client, &room_id).await.unwrap();
    transport.online();
    // Its registration discards the replica, and the write goes on in the new epoch.
    assert_eq!(client.write(room_id.clone(), insert_task(2)).await, Ok(()));
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 2, true)));
    assert_eq!(tasks(&storage, &room_id).await, [2]);
}

#[test]
fn each_rejection_of_the_replica_is_its_error() {
    let constraint = || "fk".to_string();
    for (reason, error) in [
        (RejectReason::MissingRow, ClientError::RowMissing),
        (
            RejectReason::Duplicate,
            ClientError::ConstraintViolation(Violation::Duplicate),
        ),
        (
            RejectReason::NotOwner,
            ClientError::ConstraintViolation(Violation::NotOwner),
        ),
        (
            RejectReason::Unique {
                constraint: constraint(),
            },
            ClientError::ConstraintViolation(Violation::Unique {
                constraint: constraint(),
            }),
        ),
        (
            RejectReason::ForeignKey {
                constraint: constraint(),
            },
            ClientError::ConstraintViolation(Violation::ForeignKey {
                constraint: constraint(),
            }),
        ),
        (
            RejectReason::Restrict {
                constraint: constraint(),
            },
            ClientError::ConstraintViolation(Violation::Restrict {
                constraint: constraint(),
            }),
        ),
    ] {
        assert_eq!(ClientError::from(reason), error);
    }
}

#[tokio::test(start_paused = true)]
async fn a_refusal_of_the_server_ends_the_write() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    // An operation with a column the schema of the room does not have.
    let written = client.write(room_id.clone(), insert_tagged(1, "a")).await;
    assert!(
        matches!(written, Err(ClientError::Validation(_))),
        "{written:?}"
    );
    for (code, refused) in [
        (ErrorCode::BadRequest, true),
        (ErrorCode::Forbidden, true),
        (ErrorCode::Unauthorized, false),
        (ErrorCode::RoomNotFound, false),
        (ErrorCode::SchemaNotFound, false),
        (ErrorCode::ProtocolVersionMismatch, false),
        (ErrorCode::RoomReset, false),
    ] {
        let before = commits(&transport).len();
        transport.answer_next(Commit, move |message| refusal(message, code));
        let written = client.write(room_id.clone(), insert_task(1)).await;
        match refused {
            true => assert!(
                matches!(written, Err(ClientError::Validation(_))),
                "{code:?}"
            ),
            false => assert!(matches!(written, Err(ClientError::Network(_))), "{code:?}"),
        }
        assert_eq!(commits(&transport).len(), before + 1, "{code:?}");
    }
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_write_without_a_commit_ack_ends_at_its_deadline() {
    for (reply, outcome) in [
        (Some(ErrorCode::Unavailable), ClientError::OutcomeUnknown),
        (None, ClientError::OutcomeUnknown),
        (
            Some(ErrorCode::RequestTimeout),
            ClientError::Network(String::new()),
        ),
        (
            Some(ErrorCode::RoomLocked),
            ClientError::Network(String::new()),
        ),
    ] {
        let dir = tempdir().unwrap();
        let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        let sent = Arc::new(Mutex::new(Vec::new()));
        let at = Arc::clone(&sent);
        transport.answer(move |message| {
            if message.kind() != Commit {
                return None;
            }
            at.lock().unwrap().push(Instant::now());
            Some(match reply {
                Some(code) => Ok(refusal(message, code)),
                None => Err(TransportError("Offline".to_string())),
            })
        });
        let start = Instant::now();
        let written = client.write(room_id.clone(), insert_task(1)).await;
        let deadline = start + Duration::from_secs(30);
        let ended = Instant::now();
        assert!(
            ended >= deadline && ended < deadline + Duration::from_secs(1),
            "{reply:?}"
        );
        match outcome {
            ClientError::Network(_) => assert!(
                matches!(written, Err(ClientError::Network(_))),
                "{reply:?}: {written:?}"
            ),
            outcome => assert_eq!(written, Err(outcome), "{reply:?}"),
        }
        // Retried with a backoff, and never after the deadline.
        let sent = sent.lock().unwrap().clone();
        assert!(
            sent.len() > 3 && sent.len() < 60,
            "{reply:?}: {}",
            sent.len()
        );
        assert!(sent.iter().all(|at| *at < deadline), "{reply:?}");
    }

    // Without network, a room open without registering retries its registration, and sends no
    // commit.
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    client.close_room(room_id.clone()).await.unwrap();
    transport.offline();
    open_room(&client, &room_id).await.unwrap();
    let registrations = transport.registrations().len();
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    assert!(transport.registrations().len() > registrations + 3);
    assert!(commits(&transport).is_empty());

    // A registration that never gets its reply ends at the deadline, without a commit.
    transport.online();
    let _held = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let start = Instant::now();
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    assert_eq!(Instant::now() - start, Duration::from_secs(30));
    assert!(commits(&transport).is_empty());

    // An attempt that never gets its reply is uncertain, and ends at the deadline.
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let _held = transport.hold_next_reply(Commit);
    let start = Instant::now();
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    assert_eq!(Instant::now() - start, Duration::from_secs(30));
    assert_eq!(commits(&transport).len(), 1);
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_server_that_asks_to_register_again_gets_the_same_mutation_id_after_it() {
    for code in [ErrorCode::ClientNotRegistered, ErrorCode::InvalidSequence] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, [1]).await;
        client.sync(room_id.clone()).await.unwrap();
        transport.answer_next(Commit, move |message| refusal(message, code));
        assert_eq!(client.write(room_id.clone(), insert_task(2)).await, Ok(()));
        let sent = commits(&transport);
        assert_eq!(sent.len(), 2, "{code:?}");
        assert_eq!(sent[0], sent[1], "{code:?}");
        let cursor = (Some(1), Some(server.incarnation(&room_id).await));
        assert_eq!(transport.registrations().len(), 2, "{code:?}");
        assert_eq!(transport.registrations().last(), Some(&cursor), "{code:?}");
        assert_eq!(head(&client, &room_id).await, 2);
    }

    // The server took the client out of its roster.
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let deregistration = ClientMessage::DeregisterClient {
        correlation_id: CorrelationId::new(1000),
        room_id: room_id.clone(),
        client_id: client.client_id(),
    };
    let token = token(&client.client_id(), &room_id);
    transport.send(&deregistration, &token).await.unwrap();
    assert_eq!(client.write(room_id.clone(), insert_task(1)).await, Ok(()));
    assert_eq!(transport.registrations().len(), 2);
    assert_eq!(commits(&transport).len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_commit_ack_with_more_syncs_up_to_its_sequence() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    // More than a catch-up holds.
    server.commit(&room_id, 1..=150).await;
    let written = client.write(room_id.clone(), update_task(500)).await;
    assert_eq!(written, Err(ClientError::RowMissing));
    assert_eq!(head(&client, &room_id).await, 151);
    assert_eq!(requests(&transport), [("sync", 100), ("ack", 151)]);
}

#[tokio::test(start_paused = true)]
async fn a_write_does_not_end_before_the_sync_that_applies_its_sequence() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    // Sequence 1 is an update of a row that does not exist, and the sync applies the next one
    // in another batch.
    server
        .commit_ops(&room_id, [update_task(9), insert_task(2)])
        .await;
    transport.sync_batch_size(1);
    transport.answer_next(Commit, |message| commit_ack(message, 1, &[], true, TASKS));
    let mut held = transport.hold_next_reply(Sync);
    let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
    until_replied(&mut write, &mut held).await;
    assert_eq!(in_flight(&client, &room_id).await, (1, 0));
    held.release();
    // The decision of the replica on sequence 1.
    assert_eq!(write.await, Err(ClientError::RowMissing));
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_sequence_applied_before_its_commit_ack_comes_takes_the_retained_decision() {
    for (lost, op, decision) in [
        (false, update_task(9), Err(ClientError::RowMissing)),
        (false, insert_task(1), Ok(())),
        (true, update_task(9), Err(ClientError::RowMissing)),
    ] {
        let server = TestServer::new();
        let room_id = server.create_room("room-a").await;
        // Two rejections, at or below the head the replica sees on opening.
        server
            .commit_ops(&room_id, [update_task(7), update_task(8)])
            .await;
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let client = open_client(dir.path(), engine(dir.path()), &transport).await;
        open_room(&client, &room_id).await.unwrap();
        if lost {
            // A reply lost, and the one to the retry delayed.
            transport.lose_next_reply(Commit);
        }
        let mut held = transport.hold_next_reply(Commit);
        let mut write = pin!(client.write(room_id.clone(), op.clone()));
        until_replied(&mut write, &mut held).await;
        // A sync applies the sequence of the write while its reply is held: only what comes
        // after the head the write saw is retained.
        let synced = tokio::select! {
            written = &mut write => panic!("{written:?}"),
            synced = client.sync(room_id.clone()) => synced,
        };
        synced.unwrap();
        assert_eq!(head(&client, &room_id).await, 3);
        let retained = usize::from(decision.is_err());
        assert_eq!(in_flight(&client, &room_id).await, (1, retained), "{op:?}");
        held.release();
        assert_eq!(write.await, decision, "{lost} {op:?}");
        assert_eq!(in_flight(&client, &room_id).await, (0, 0));
    }
}

#[tokio::test(start_paused = true)]
async fn the_head_seen_comes_from_every_reply_of_the_epoch() {
    for source in ["sync", "ack", "commit", "catch-up"] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        let ops = server.commit(&room_id, 1..=3).await;
        // A reply reports the head 9, above the head of the server.
        match source {
            "sync" => answer_next(&transport, Sync, move |correlation_id, room_id| {
                batch_to_nine(correlation_id, room_id, ops)
            }),
            "ack" => answer_next(
                &transport,
                ClientMessageKind::Ack,
                |correlation_id, room_id| {
                    let confirmed = ServerMessage::AckConfirmed {
                        correlation_id,
                        room_id: room_id.clone(),
                        ack_seq: SequenceNumber::new(3),
                        head_seq: SequenceNumber::new(9),
                    };
                    encode_message(&confirmed).unwrap()
                },
            ),
            _ => {}
        }
        client.sync(room_id.clone()).await.unwrap();
        if source == "commit" {
            // The ack of a write that waits for its sequence, and is dropped.
            transport.answer_next(Commit, |message| commit_ack(message, 9, &[], true, TASKS));
            let mut held = transport.hold_next_reply(Sync);
            let mut write = pin!(client.write(room_id.clone(), insert_task(8)));
            until_replied(&mut write, &mut held).await;
        }
        if source == "catch-up" {
            // The ack of a write with a catch-up up to 9, not applied because it announces
            // another version, and the write is dropped.
            transport.answer_next(Commit, |message| {
                let ops: Vec<_> = (4..=9)
                    .map(|seq| SequencedOperation::new(seq, insert_task(1)))
                    .collect();
                commit_ack(message, 4, &ops, false, ("tasks", 2))
            });
            let mut held = transport.hold_next_reply(ClientMessageKind::RegisterClient);
            let mut write = pin!(client.write(room_id.clone(), insert_task(8)));
            until_replied(&mut write, &mut held).await;
        }
        // A sequence above the head of the replica, but not above 9.
        transport.answer_next(Commit, |message| commit_ack(message, 5, &[], true, TASKS));
        let written = client.write(room_id.clone(), insert_task(7));
        let written = tokio::time::timeout(Duration::from_secs(60), written).await;
        assert_eq!(written, Ok(Err(ClientError::OutcomeUnknown)), "{source}");
    }

    // Not from a batch that does not validate, with a gap: the write waits for its sequence.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let ops = server.commit(&room_id, 1..=5).await;
    answer_next(&transport, Sync, move |correlation_id, room_id| {
        batch_to_nine(correlation_id, room_id, ops[1..].to_vec())
    });
    let synced = client.sync(room_id.clone()).await;
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");
    transport.answer_next(Commit, |message| commit_ack(message, 5, &[], true, TASKS));
    assert_eq!(client.write(room_id.clone(), insert_task(7)).await, Ok(()));

    // Not from a reply to a request of an earlier epoch: a sync held while the replica is
    // discarded gets a batch with the head 9.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let ops = server.commit(&room_id, 1..=3).await;
    answer_next(&transport, Sync, move |correlation_id, room_id| {
        batch_to_nine(correlation_id, room_id, ops)
    });
    let mut held = transport.hold_next_reply(Sync);
    let mut early = pin!(client.sync(room_id.clone()));
    until_replied(&mut early, &mut held).await;
    server.recreate_room(&room_id).await;
    client.sync(room_id.clone()).await.unwrap();
    held.release();
    early.await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 2, true)));
    // The ack of a write in the new epoch, with its operation.
    transport.answer_next(Commit, |message| {
        let ops = [SequencedOperation::new(1, insert_task(5))];
        commit_ack(message, 1, &ops, false, TASKS)
    });
    assert_eq!(client.write(room_id.clone(), insert_task(5)).await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn what_a_write_saw_is_never_below_the_head_of_its_replica() {
    // The room opens again, and a fabricated `Registered` reports a head below its cursor.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    client.close_room(room_id.clone()).await.unwrap();
    transport.offline();
    open_room(&client, &room_id).await.unwrap();
    let incarnation = server.incarnation(&room_id).await;
    transport.answer(move |message| match message {
        ClientMessage::RegisterClient { .. } => Some(Ok(registered(
            message,
            incarnation,
            "tasks",
            tasks_schema(),
        ))),
        ClientMessage::Commit { .. } => Some(Ok(commit_ack(message, 3, &[], false, TASKS))),
        _ => None,
    });
    // Sequence 3 is the head of the replica.
    let written = client.write(room_id.clone(), insert_task(4)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(start_paused = true)]
async fn a_write_waits_for_a_change_of_the_replica_in_progress() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, _, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    let injected = StorageError::Io(std::io::Error::other("injected"));
    storage.fail_next("apply_batch", injected);
    storage.slow_open(Duration::from_millis(300));
    let failed = client.sync(room_id.clone());
    let (client_ref, room) = (&client, room_id.clone());
    let meanwhile = async move {
        // While the storage room opens again, the room is out of the status.
        while client_ref.status(room.clone()).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        client_ref.write(room, insert_task(4)).await
    };
    let (failed, written) = tokio::join!(failed, meanwhile);
    assert!(matches!(failed, Err(ClientError::Storage(_))), "{failed:?}");
    assert_eq!(written, Ok(()));
    assert_eq!(head(&client, &room_id).await, 4);
}

#[tokio::test(start_paused = true)]
async fn a_write_with_its_commit_ack_retains_nothing() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    // A rejection after what the write saw, which a sync applies before its ack comes.
    server.commit_ops(&room_id, [update_task(9)]).await;
    transport.answer_next(Commit, |message| commit_ack(message, 2, &[], true, TASKS));
    let mut held = transport.hold_next_reply(Commit);
    let mut write = pin!(client.write(room_id.clone(), insert_task(5)));
    until_replied(&mut write, &mut held).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(in_flight(&client, &room_id).await, (1, 1));
    // With its ack, it waits for sequence 2, and nothing is retained.
    let mut synced = transport.hold_next_reply(Sync);
    held.release();
    until_replied(&mut write, &mut synced).await;
    assert_eq!(in_flight(&client, &room_id).await, (1, 0));
    server.commit(&room_id, [2]).await;
    synced.release();
    assert_eq!(write.await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_sequence_in_a_range_without_a_report_is_an_unknown_outcome() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, [1]).await;
    let mut held = transport.hold_next_reply(Commit);
    let mut write = pin!(client.write(room_id.clone(), insert_task(2)));
    until_replied(&mut write, &mut held).await;
    // A sync applies sequences 1 and 2 in a batch whose report is lost.
    files.fail_next("sync_data", "room_room-a.wal");
    let synced = client.sync(room_id.clone()).await;
    assert!(matches!(synced, Err(ClientError::Storage(_))), "{synced:?}");
    assert_eq!(head(&client, &room_id).await, 2);
    assert_eq!(in_flight(&client, &room_id).await, (1, 1));
    held.release();
    assert_eq!(write.await, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(start_paused = true)]
async fn a_write_keeps_what_it_retains_while_a_later_one_waits() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    // The first write is rejected, and a sync applies it before its ack comes.
    let mut first_held = transport.hold_next_reply(Commit);
    let mut first = pin!(client.write(room_id.clone(), update_task(9)));
    until_replied(&mut first, &mut first_held).await;
    client.sync(room_id.clone()).await.unwrap();
    // The second one saw sequence 1, and another sync applies its own.
    let mut second_held = transport.hold_next_reply(Commit);
    let mut second = pin!(client.write(room_id.clone(), insert_task(5)));
    until_replied(&mut second, &mut second_held).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(in_flight(&client, &room_id).await, (2, 1));
    first_held.release();
    assert_eq!(first.await, Err(ClientError::RowMissing));
    second_held.release();
    assert_eq!(second.await, Ok(()));
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_commit_ack_at_or_below_the_head_seen_is_an_unknown_outcome() {
    // At the head of the replica, which applied it.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    transport.answer_next(Commit, |message| commit_ack(message, 3, &[], false, TASKS));
    let written = client.write(room_id.clone(), insert_task(4)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));

    // Above the head of the replica, but not above the head of the server it saw.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=5).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    transport.answer_next(Commit, |message| commit_ack(message, 4, &[], true, TASKS));
    let written = client.write(room_id.clone(), insert_task(6)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    // It does not wait for it.
    assert_eq!(requests(&transport), []);
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

/// A reply to a commit, fabricated from it.
type Reply = Box<dyn FnOnce(&ClientMessage) -> Vec<u8> + Send>;

#[tokio::test(start_paused = true)]
async fn a_reply_that_does_not_answer_the_commit_leaves_the_attempt_uncertain() {
    let op = |seq| SequencedOperation::new(seq, insert_task(7));
    // An ack that, taken, would apply its operation and end the write.
    let ack = move |message: &ClientMessage, change: fn(&mut ServerMessage)| {
        changed(commit_ack(message, 1, &[op(1)], false, TASKS), change)
    };
    let replies: [Reply; 9] = [
        // An ack of another mutation, correlation or room.
        Box::new(move |message| {
            ack(message, |reply| {
                if let ServerMessage::CommitAck { mutation_id, .. } = reply {
                    *mutation_id = MutationId::from(7);
                }
            })
        }),
        Box::new(move |message| {
            ack(message, |reply| {
                if let ServerMessage::CommitAck { correlation_id, .. } = reply {
                    *correlation_id = CorrelationId::new(9999);
                }
            })
        }),
        Box::new(move |message| {
            ack(message, |reply| {
                if let ServerMessage::CommitAck { room_id, .. } = reply {
                    *room_id = RoomId::new("room-z").unwrap();
                }
            })
        }),
        // A refusal of another correlation, or of another room.
        Box::new(|message| {
            let ClientMessage::Commit { room_id, .. } = message else {
                panic!()
            };
            error_reply(
                CorrelationId::new(9999),
                room_id,
                ErrorCode::SchemaViolation,
            )
        }),
        Box::new(|message| {
            let (correlation_id, _) = request(message);
            let room_id = RoomId::new("room-z").unwrap();
            error_reply(correlation_id, &room_id, ErrorCode::SchemaViolation)
        }),
        // A catch-up with a gap, out of order, or that stops before the sequence without more.
        Box::new(move |message| commit_ack(message, 2, &[op(2)], false, TASKS)),
        Box::new(move |message| commit_ack(message, 2, &[op(2), op(1)], false, TASKS)),
        Box::new(move |message| commit_ack(message, 2, &[op(1)], false, TASKS)),
        // Bytes that do not decode.
        Box::new(|_| vec![1, 2, 3]),
    ];
    for (case, reply) in replies.into_iter().enumerate() {
        let dir = tempdir().unwrap();
        let (_server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        transport.answer_next(Commit, reply);
        // The retry gets the reply of the server: the write has its decision.
        assert_eq!(
            client.write(room_id.clone(), insert_task(1)).await,
            Ok(()),
            "{case}"
        );
        assert_eq!(commits(&transport).len(), 2, "{case}");
        assert_eq!(tasks(&storage, &room_id).await, [1], "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_ack_that_announces_another_schema_registers_again_before_applying() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, storage, client) =
        setup(dir.path(), engine(dir.path())).await;
    // The first attempt gets sequence 1, and the ack to the retry announces another version.
    transport.lose_next_reply(Commit);
    transport.answer_next(Commit, |message| {
        let ops = [SequencedOperation::new(1, insert_task(1))];
        commit_ack(message, 1, &ops, false, ("tasks", 2))
    });
    assert_eq!(client.write(room_id.clone(), insert_task(1)).await, Ok(()));
    // `Registered` has no change: the sync applies the operation with the local schema.
    assert_eq!(transport.registrations().len(), 2);
    assert_eq!(requests(&transport), [("sync", 0), ("ack", 1)]);
    assert_eq!(tasks(&storage, &room_id).await, [1]);
}

#[tokio::test(start_paused = true)]
async fn with_a_commit_ack_a_reset_a_close_or_failing_closed_end_the_write_unknown() {
    for case in ["reset", "close"] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        // The ack, fabricated, leaves the sequence to a sync, which is held.
        transport.answer_next(Commit, |message| commit_ack(message, 1, &[], true, TASKS));
        let mut held = transport.hold_next_reply(Sync);
        let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
        until_replied(&mut write, &mut held).await;
        match case {
            "reset" => server.recreate_room(&room_id).await,
            _ => client.close_room(room_id.clone()).await.unwrap(),
        }
        held.release();
        assert_eq!(write.await, Err(ClientError::OutcomeUnknown), "{case}");
    }

    // The ack announces another schema, and the registration that follows gets one that does
    // not evolve from the replica's.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let incarnation = server.incarnation(&room_id).await;
    transport.answer(move |message| match message {
        ClientMessage::Commit { .. } => Some(Ok(commit_ack(message, 1, &[], true, ("other", 1)))),
        ClientMessage::RegisterClient { .. } => Some(Ok(retyped_registered(message, incarnation))),
        _ => None,
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    assert_eq!(deregistrations(&transport), 1);
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));

    // The ack comes after the room failed closed.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    let mut held = transport.hold_next_reply(Commit);
    let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
    until_replied(&mut write, &mut held).await;
    fail_next_sync_closed(&transport, server.incarnation(&room_id).await);
    let synced = client.sync(room_id.clone()).await;
    assert_eq!(synced, Err(ClientError::FailedClosed(room_id.clone())));
    held.release();
    assert_eq!(write.await, Err(ClientError::OutcomeUnknown));
    assert!(tasks(&storage, &room_id).await.is_empty());
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn without_a_commit_ack_a_reset_a_close_or_failing_closed_end_the_write_without_effect() {
    for (case, uncertain) in [
        ("reset", false),
        ("close", false),
        ("fail closed", false),
        ("close", true),
    ] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        let incarnation = server.incarnation(&room_id).await;
        transport.answer_every(Commit, |message| refusal(message, ErrorCode::RoomLocked));
        if uncertain {
            transport.lose_next_reply(Commit);
        }
        // A refusal without effect, held.
        let mut held = transport.hold_next_reply(Commit);
        let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
        until_replied(&mut write, &mut held).await;
        match case {
            "reset" => {
                transport.online();
                server.recreate_room(&room_id).await;
                client.sync(room_id.clone()).await.unwrap();
                assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 2, true)));
            }
            "close" => client.close_room(room_id.clone()).await.unwrap(),
            _ => {
                fail_next_sync_closed(&transport, incarnation);
                let synced = client.sync(room_id.clone()).await;
                assert_eq!(synced, Err(ClientError::FailedClosed(room_id.clone())));
            }
        }
        let sent = commits(&transport).len();
        held.release();
        let written = write.await;
        match uncertain {
            true => assert_eq!(written, Err(ClientError::OutcomeUnknown), "{case}"),
            false => assert!(
                matches!(written, Err(ClientError::Network(_))),
                "{case}: {written:?}"
            ),
        }
        // No attempt after it.
        assert_eq!(commits(&transport).len(), sent, "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_batch_lost_its_report_is_an_unknown_outcome() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    // The catch-up with its sequence is written but not synced: the storage room opens again
    // with it, without its report.
    files.fail_next("sync_data", "room_room-a.wal");
    let written = client.write(room_id.clone(), update_task(9)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    assert_eq!(head(&client, &room_id).await, 1);

    // A write waiting for a sequence after the head the room opened again at goes on.
    client.sync(room_id.clone()).await.unwrap();
    server.commit(&room_id, [1]).await;
    server.commit_ops(&room_id, [update_task(8)]).await;
    transport.sync_batch_size(1);
    transport.answer_next(Commit, |message| commit_ack(message, 3, &[], true, TASKS));
    files.fail_next("sync_data", "room_room-a.wal");
    let written = client.write(room_id.clone(), insert_task(5)).await;
    // The decision of the replica on sequence 3, which the next sync applied.
    assert_eq!(written, Err(ClientError::RowMissing));
    assert_eq!(head(&client, &room_id).await, 3);
    assert_eq!(tasks(&storage, &room_id).await, [1]);
}

#[tokio::test(start_paused = true)]
async fn two_writes_at_once_end_with_the_decisions_of_their_sequences() {
    let dir = tempdir().unwrap();
    let (server, _, _, storage, client) = setup(dir.path(), engine(dir.path())).await;
    let room_id = server
        .create_room_with("room-b", "tagged", tagged_schema())
        .await;
    open_room(&client, &room_id).await.unwrap();
    let (first, second) = tokio::join!(
        client.write(room_id.clone(), insert_tagged(1, "a")),
        client.write(room_id.clone(), insert_tagged(2, "b")),
    );
    assert_eq!((first, second), (Ok(()), Ok(())));
    // The same tag: the first in the log has it.
    let (first, second) = tokio::join!(
        client.write(room_id.clone(), insert_tagged(3, "c")),
        client.write(room_id.clone(), insert_tagged(4, "c")),
    );
    let unique = Err(ClientError::ConstraintViolation(Violation::Unique {
        constraint: "tasks_tag".to_string(),
    }));
    let rows = tasks(&storage, &room_id).await;
    match rows.as_slice() {
        [1, 2, 3] => assert_eq!((first, second), (Ok(()), unique)),
        [1, 2, 4] => assert_eq!((first, second), (unique, Ok(()))),
        rows => panic!("{rows:?}"),
    }
    assert_eq!(in_flight(&client, &room_id).await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_write_to_a_room_that_is_closed_or_fails_closed_sends_nothing() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let other = RoomId::new("room-z").unwrap();
    let written = client.write(other.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::RoomClosed(other)));

    fail_next_sync_closed(&transport, server.incarnation(&room_id).await);
    let synced = client.sync(room_id.clone()).await;
    assert_eq!(synced, Err(ClientError::FailedClosed(room_id.clone())));
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::FailedClosed(room_id.clone())));
    client.close_room(room_id.clone()).await.unwrap();
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::RoomClosed(room_id.clone())));
    assert!(commits(&transport).is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_write_sends_nothing_after_its_deadline_while_the_applier_is_busy() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, [1]).await;
    // A sync holds the applier for 40 s while it writes its batch.
    files.slow(Duration::from_secs(40));
    let write = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let start = Instant::now();
        let written = client.write(room_id.clone(), insert_task(2)).await;
        (written, Instant::now() - start)
    };
    let (synced, (written, waited)) = tokio::join!(client.sync(room_id.clone()), write);
    synced.unwrap();
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    assert_eq!(waited, Duration::from_secs(30));
    assert!(commits(&transport).is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_registration_that_fails_the_room_closed_ends_the_write_without_effect() {
    for uncertain in [false, true] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        let incarnation = server.incarnation(&room_id).await;
        // The write registers again, and gets a schema that does not evolve.
        let mut commits_sent = 0;
        transport.answer(move |message| match message {
            ClientMessage::Commit { .. } => {
                commits_sent += 1;
                let code = match (commits_sent, uncertain) {
                    (1, true) => ErrorCode::Unavailable,
                    _ => ErrorCode::ClientNotRegistered,
                };
                Some(Ok(refusal(message, code)))
            }
            ClientMessage::RegisterClient { .. } => {
                Some(Ok(retyped_registered(message, incarnation)))
            }
            _ => None,
        });
        let written = client.write(room_id.clone(), insert_task(1)).await;
        match uncertain {
            true => assert_eq!(written, Err(ClientError::OutcomeUnknown)),
            false => assert!(
                matches!(written, Err(ClientError::Network(_))),
                "{written:?}"
            ),
        }
        let failed_closed = client.status(room_id.clone()).await.unwrap().failed_closed;
        assert!(failed_closed, "{uncertain}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_reopened_head_that_cannot_be_read_leaves_every_sequence_without_a_report() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let disk = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let (_server, room_id, transport, storage, client) =
        setup(dir.path(), TestEngine::new(disk)).await;
    let mut held = transport.hold_next_reply(Commit);
    let mut write = pin!(client.write(room_id.clone(), update_task(9)));
    until_replied(&mut write, &mut held).await;
    // The batch with the rejection of the write loses its report, and the head of the storage
    // room cannot be read once it opens again.
    files.fail_next("sync_data", "room_room-a.wal");
    storage.slow_open(Duration::from_millis(300));
    let (client_ref, room) = (&client, room_id.clone());
    let failing = async move {
        while client_ref.status(room.clone()).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let injected = StorageError::Other("injected".to_string());
        storage.fail_next("get_head_seq", injected);
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), failing);
    assert!(matches!(synced, Err(ClientError::Storage(_))), "{synced:?}");
    held.release();
    assert_eq!(write.await, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(start_paused = true)]
async fn a_write_waiting_for_its_sequence_syncs_with_a_backoff() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let syncs = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&syncs);
    transport.answer(move |message| match message {
        ClientMessage::Commit { .. } => Some(Ok(commit_ack(message, 1, &[], true, TASKS))),
        ClientMessage::Sync { .. } => {
            *counted.lock().unwrap() += 1;
            Some(Ok(refusal(message, ErrorCode::Unavailable)))
        }
        _ => None,
    });
    let written = client.write(room_id.clone(), insert_task(1));
    let written = tokio::time::timeout(Duration::from_secs(60), written).await;
    assert!(written.is_err(), "{written:?}");
    let syncs = *syncs.lock().unwrap();
    assert!(syncs > 5 && syncs < 100, "{syncs}");
}

#[tokio::test(start_paused = true)]
async fn the_backoff_is_random_up_to_its_longest_delay() {
    let dir = tempdir().unwrap();
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let config = ClientConfig {
        backoff_base: Duration::from_millis(100),
        backoff_max: Duration::from_millis(100),
        ..ClientConfig::new("ana")
    };
    let metadata = FileMetadata::new(dir.path());
    let client = Client::open(config, engine(dir.path()), metadata, transport.clone());
    let client = client.await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let at = Arc::clone(&sent);
    transport.answer_every(Commit, move |message| {
        at.lock().unwrap().push(Instant::now());
        refusal(message, ErrorCode::RoomLocked)
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    let sent = sent.lock().unwrap().clone();
    let delays: Vec<_> = sent.windows(2).map(|pair| pair[1] - pair[0]).collect();
    // About one every 50 ms, none apart by more than the longest delay.
    assert!(sent.len() > 300, "{}", sent.len());
    assert!(delays
        .iter()
        .all(|delay| *delay <= Duration::from_millis(100)));
    assert!(delays
        .iter()
        .any(|delay| *delay < Duration::from_millis(90)));

    // A backoff that does not grow from zero, or whose longest delay is shorter, is refused.
    for (base, max) in [(0, 100), (100, 50)] {
        let config = ClientConfig {
            backoff_base: Duration::from_millis(base),
            backoff_max: Duration::from_millis(max),
            ..ClientConfig::new("ana")
        };
        let dir = tempdir().unwrap();
        let metadata = FileMetadata::new(dir.path());
        let opened = Client::open(config, engine(dir.path()), metadata, transport.clone()).await;
        assert!(
            matches!(opened, Err(ClientError::Config(_))),
            "{base} {max}"
        );
    }
    // A deadline beyond the clock is too.
    let config = ClientConfig {
        write_timeout: Duration::MAX,
        ..ClientConfig::new("ana")
    };
    let dir = tempdir().unwrap();
    let metadata = FileMetadata::new(dir.path());
    let client = Client::open(config, engine(dir.path()), metadata, transport.clone());
    let client = client.await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert!(
        matches!(written, Err(ClientError::Config(_))),
        "{written:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_and_a_sync_of_a_room_that_is_not_registered_register_it_once() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    client.close_room(room_id.clone()).await.unwrap();
    transport.offline();
    open_room(&client, &room_id).await.unwrap();
    transport.online();
    let registrations = transport.registrations().len();
    // A sync waits for the registration of the write, held.
    let mut held = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
    until_replied(&mut write, &mut held).await;
    let mut sync = pin!(client.sync(room_id.clone()));
    let waited = tokio::time::timeout(Duration::from_secs(1), &mut sync).await;
    assert!(waited.is_err(), "{waited:?}");
    assert_eq!(transport.registrations().len(), registrations + 1);
    held.release();
    let (written, synced) = tokio::join!(write, sync);
    assert_eq!((written, synced), (Ok(()), Ok(())));
    assert_eq!(transport.registrations().len(), registrations + 1);
}

#[tokio::test(start_paused = true)]
async fn a_write_cut_at_any_point_leaves_nothing_in_flight_or_retained() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    for polls in 1.. {
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let storage = engine(dir.path());
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        open_room(&client, &room_id).await.unwrap();
        // A rejection after the head the write sees, which a sync applies while the write
        // waits for its slower reply.
        server.commit_ops(&room_id, [update_task(9)]).await;
        transport.slow(Duration::from_millis(2));
        transport.slow_next(Commit, Duration::from_millis(10));
        let id = i64::from(polls);
        let mut both = Box::pin(async {
            tokio::join!(
                client.write(room_id.clone(), insert_task(100 + id)),
                client.sync(room_id.clone()),
            )
        });
        let mut finished = false;
        for _ in 0..polls {
            let polled = std::future::poll_fn(|cx| Poll::Ready(both.as_mut().poll(cx)));
            if let Poll::Ready((written, synced)) = polled.await {
                assert_eq!((written, synced), (Ok(()), Ok(())));
                finished = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(both);
        assert_eq!(in_flight(&client, &room_id).await, (0, 0), "{polls}");

        // The next write goes on.
        let written = client.write(room_id.clone(), insert_task(200 + id)).await;
        assert_eq!(written, Ok(()), "{polls}");
        assert_eq!(in_flight(&client, &room_id).await, (0, 0), "{polls}");
        let key = PrimaryKey::single(200 + id);
        let row = storage.get(&room_id, "tasks", &key).await.unwrap();
        assert!(row.is_some(), "{polls}");
        client.shutdown().await.unwrap();
        if finished {
            break;
        }
    }
}
