//! Syncing the rooms of the client through their applier, against the server in process, where a
//! client of the server's own commits the operations.

mod common;

use common::*;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, MemoryMetadata, MetadataStore,
};
use zemdb_core::ClientMessageKind::{Ack, Sync};
use zemdb_core::*;
use zemdb_storage::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};

fn next(correlation_id: CorrelationId) -> CorrelationId {
    CorrelationId::new(correlation_id.get() + 1)
}

fn room_b() -> RoomId {
    RoomId::new("room-b").unwrap()
}

fn batch(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    head_seq: u64,
    ops: &[SequencedOperation],
    has_more: bool,
) -> Vec<u8> {
    encode_message(&ServerMessage::SyncBatch {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(head_seq),
        ops: ops.to_vec(),
        has_more,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: SchemaId::new("tasks").unwrap(),
    })
    .unwrap()
}

fn confirmed(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    ack_seq: u64,
    head_seq: u64,
) -> Vec<u8> {
    encode_message(&ServerMessage::AckConfirmed {
        correlation_id,
        room_id: room_id.clone(),
        ack_seq: SequenceNumber::new(ack_seq),
        head_seq: SequenceNumber::new(head_seq),
    })
    .unwrap()
}

#[tokio::test]
async fn a_sync_in_small_batches_reaches_the_head_and_acknowledges_it() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=5).await;
    transport.sync_batch_size(2);
    client.sync(room_id.clone()).await.unwrap();
    let synced = [("sync", 0), ("sync", 2), ("sync", 4), ("ack", 5)];
    assert_eq!(requests(&transport), synced);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(5, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5]);

    // At the head, it asks once and acknowledges the same head.
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(requests(&transport)[4..], [("sync", 5), ("ack", 5)]);
    let synced = client.sync(room_b()).await;
    assert_eq!(synced, Err(ClientError::RoomClosed(room_b())));
}

#[tokio::test]
async fn a_sync_registers_a_room_that_is_not_registered() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=3).await;
    let transport = server.transport();
    transport.offline();
    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    // Without metadata, the replica stays empty.
    let opened = open_room(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    let synced = client.sync(room_id.clone()).await;
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");

    // Two syncs at once register once.
    transport.online();
    let registrations = transport.registrations().len();
    let (synced, other) = tokio::join!(client.sync(room_id.clone()), client.sync(room_id.clone()));
    synced.unwrap();
    other.unwrap();
    assert_eq!(transport.registrations().len(), registrations + 1);
    assert_eq!(transport.registrations().last(), Some(&(None, None)));
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
}

#[tokio::test]
async fn a_reply_that_does_not_answer_the_sync_or_the_ack_is_not_used() {
    type Reply = fn(CorrelationId, &RoomId, &[SequencedOperation]) -> Vec<u8>;
    let cases: [(ClientMessageKind, &str, Reply); 21] = [
        (Sync, "a batch", |id, room, ops| {
            batch(id, room, 3, ops, false)
        }),
        // It asks for more all the same.
        (Sync, "a batch with more", |id, room, ops| {
            batch(id, room, 3, ops, true)
        }),
        (Sync, "a gap", |id, room, ops| {
            batch(id, room, 3, &ops[1..], false)
        }),
        (Sync, "operations out of order", |id, room, ops| {
            batch(id, room, 3, &[ops[0].clone(), ops[2].clone()], false)
        }),
        (Sync, "operations past its head", |id, room, ops| {
            batch(id, room, 2, ops, true)
        }),
        (Sync, "short of its head without more", |id, room, ops| {
            batch(id, room, 3, &ops[..2], false)
        }),
        (Sync, "more without operations", |id, room, _| {
            batch(id, room, 3, &[], true)
        }),
        (Sync, "an operation at sequence 0", |id, room, ops| {
            let zero = SequencedOperation::new(0, ops[0].op.clone());
            batch(id, room, 3, &[zero, ops[0].clone()], true)
        }),
        (Sync, "another correlation id", |id, room, ops| {
            batch(next(id), room, 3, ops, false)
        }),
        (Sync, "another room", |id, _, ops| {
            batch(id, &room_b(), 3, ops, false)
        }),
        (Sync, "another kind of reply", |id, room, _| {
            confirmed(id, room, 0, 3)
        }),
        (
            Sync,
            "a refusal of another correlation id",
            |id, room, _| error_reply(next(id), room, ErrorCode::InvalidSequence),
        ),
        (Sync, "a refusal of another room", |id, _, _| {
            error_reply(id, &room_b(), ErrorCode::InvalidSequence)
        }),
        (Sync, "bytes that do not decode", |_, _, _| b"zemd".to_vec()),
        (Ack, "a confirmation", |id, room, _| {
            confirmed(id, room, 3, 3)
        }),
        (Ack, "a confirmation of another sequence", |id, room, _| {
            confirmed(id, room, 2, 3)
        }),
        (Ack, "another correlation id", |id, room, _| {
            confirmed(next(id), room, 3, 3)
        }),
        (Ack, "another room", |id, _, _| {
            confirmed(id, &room_b(), 3, 3)
        }),
        (Ack, "another kind of reply", |id, room, _| {
            batch(id, room, 3, &[], false)
        }),
        (Ack, "a refusal of another correlation id", |id, room, _| {
            error_reply(next(id), room, ErrorCode::InvalidSequence)
        }),
        (Ack, "bytes that do not decode", |_, _, _| b"zemd".to_vec()),
    ];
    for (kind, case, reply) in cases {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        let ops = server.commit(&room_id, 1..=3).await;
        answer_next(&transport, kind, move |id, room| reply(id, room, &ops));
        let synced = client.sync(room_id.clone()).await;
        let (valid, applied) = match kind {
            Sync => (case.starts_with("a batch"), case.starts_with("a batch")),
            _ => (case == "a confirmation", true),
        };
        match valid {
            true => assert_eq!(synced, Ok(()), "{case}"),
            false => assert!(
                matches!(synced, Err(ClientError::Network(_))),
                "{case}: {synced:?}"
            ),
        }
        if case == "a batch with more" {
            let synced = [("sync", 0), ("sync", 3), ("ack", 3)];
            assert_eq!(requests(&transport), synced);
        } else if valid {
            assert_eq!(requests(&transport), [("sync", 0), ("ack", 3)], "{case}");
        }
        let head = if applied { 3 } else { 0 };
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(head, 1, true)), "{case}");
        let applied: Vec<i64> = (1..=i64::try_from(head).unwrap()).collect();
        assert_eq!(tasks(&storage, &room_id).await, applied, "{case}");
        assert_eq!(transport.registrations().len(), 1, "{case}");

        // The next sync goes on.
        client.sync(room_id.clone()).await.unwrap();
        assert_eq!(
            client.status(room_id.clone()).await,
            Ok(status(3, 1, true)),
            "{case}"
        );
    }
}

#[tokio::test]
async fn a_batch_asked_for_before_a_discard_is_not_applied() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    let old = server.incarnation(&room_id).await;
    server.commit(&room_id, 1..=3).await;
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (server_ref, client_ref, room) = (&server, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        // The room is created again: the next sync is not registered, registers with its cursor,
        // gets a reset and discards the replica.
        server_ref.recreate_room(&room).await;
        server_ref.commit(&room, [4]).await;
        client_ref.sync(room.clone()).await.unwrap();
        assert_eq!(client_ref.status(room).await, Ok(status(1, 2, true)));
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    late.unwrap();
    assert_eq!(tasks(&storage, &room_id).await, [4]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 2, true)));
    let registrations = [(None, None), (Some(0), Some(old)), (None, None)];
    assert_eq!(transport.registrations(), registrations);

    // Nor does a reply of the earlier epoch that would leave the replica waiting for a snapshot.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    answer_next(&transport, Sync, |id, room| {
        error_reply(id, room, ErrorCode::BehindCompaction)
    });
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (server_ref, client_ref, room) = (&server, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        server_ref.recreate_room(&room).await;
        client_ref.sync(room).await.unwrap();
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    assert!(matches!(late, Err(ClientError::Network(_))), "{late:?}");
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 2, true)));

    // Nor one that would leave the replica unregistered.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let old = server.incarnation(&room_id).await;
    answer_next(&transport, Sync, |id, room| {
        error_reply(id, room, ErrorCode::InvalidSequence)
    });
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (server_ref, client_ref, room) = (&server, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        server_ref.recreate_room(&room).await;
        client_ref.sync(room).await.unwrap();
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    late.unwrap();
    let registrations = [(None, None), (Some(0), Some(old)), (None, None)];
    assert_eq!(transport.registrations(), registrations);
}

#[tokio::test]
async fn a_batch_for_a_room_closed_meanwhile_is_not_applied() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (client_ref, room) = (&client, room_id.clone());
    let close = async move {
        held.replied().await;
        client_ref.close_room(room).await.unwrap();
        held.release();
    };
    let (late, ()) = tokio::join!(late, close);
    assert_eq!(late, Err(ClientError::RoomClosed(room_id.clone())));
    open_room(&client, &room_id).await.unwrap();
    let cursor = (Some(0), Some(server.incarnation(&room_id).await));
    assert_eq!(transport.registrations().last(), Some(&cursor));
    assert!(tasks(&storage, &room_id).await.is_empty());
}

#[tokio::test]
async fn a_delayed_batch_applies_only_what_the_replica_lacks() {
    // When it arrives, the replica holds part of it.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    let ops = server.commit(&room_id, 1..=4).await;
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (transport_ref, client_ref, room) = (&transport, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        answer_next(transport_ref, Sync, move |id, room| {
            batch(id, room, 2, &ops[..2], false)
        });
        client_ref.sync(room.clone()).await.unwrap();
        assert_eq!(client_ref.status(room).await, Ok(status(2, 1, true)));
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    late.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(4, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4]);
    // The late sync applies the rest of its batch, without asking again.
    let synced = [("sync", 0), ("sync", 0), ("ack", 2), ("ack", 4)];
    assert_eq!(requests(&transport), synced);

    // When it arrives, the replica holds all of it and more: its head is below the replica's,
    // and not below the request's, so the sync goes on without registering.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=2).await;
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (server_ref, client_ref, room) = (&server, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        server_ref.commit(&room, 3..=4).await;
        client_ref.sync(room.clone()).await.unwrap();
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    late.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(4, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4]);
    assert_eq!(transport.registrations().len(), 1);
    assert_eq!(requests(&transport).last(), Some(&("ack", 4)));
}

#[tokio::test]
async fn a_head_below_the_request_or_an_invalid_sequence_registers_again() {
    type Reply = fn(CorrelationId, &RoomId) -> Vec<u8>;
    let cases: [(ClientMessageKind, &str, Reply); 7] = [
        (Sync, "a head below the request", |id, room| {
            batch(id, room, 2, &[], false)
        }),
        (Sync, "an invalid sequence", |id, room| {
            error_reply(id, room, ErrorCode::InvalidSequence)
        }),
        (Sync, "an invalid sequence without the request", |_, _| {
            encode_message(&ServerMessage::Error {
                correlation_id: None,
                room_id: None,
                code: ErrorCode::InvalidSequence,
                message: "Fabricated".to_string(),
            })
            .unwrap()
        }),
        (Sync, "not registered", |id, room| {
            error_reply(id, room, ErrorCode::ClientNotRegistered)
        }),
        (Ack, "a head below the request", |id, room| {
            confirmed(id, room, 3, 2)
        }),
        (Ack, "an invalid sequence", |id, room| {
            error_reply(id, room, ErrorCode::InvalidSequence)
        }),
        (Ack, "not registered", |id, room| {
            error_reply(id, room, ErrorCode::ClientNotRegistered)
        }),
    ];
    for (kind, case, reply) in cases {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, 1..=3).await;
        // The sync is at 3; the ack, after a sync from 0.
        if kind == Sync {
            client.sync(room_id.clone()).await.unwrap();
        }
        answer_next(&transport, kind, reply);
        client.sync(room_id.clone()).await.unwrap();
        let cursor = (Some(3), Some(server.incarnation(&room_id).await));
        assert_eq!(
            transport.registrations(),
            [(None, None), cursor],
            "{kind:?} {case}"
        );
        assert_eq!(
            requests(&transport).last(),
            Some(&("ack", 3)),
            "{kind:?} {case}"
        );
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(3, 1, true)), "{kind:?} {case}");
        assert_eq!(
            tasks(&storage, &room_id).await,
            [1, 2, 3],
            "{kind:?} {case}"
        );
    }
}

#[tokio::test]
async fn a_server_that_keeps_asking_to_register_again_ends_the_sync() {
    type Reply = fn(&ClientMessage) -> Vec<u8>;
    let not_registered: Reply = |message| {
        let (id, room) = request(message);
        error_reply(id, &room, ErrorCode::ClientNotRegistered)
    };
    let cases: [(ClientMessageKind, &str, Reply); 4] = [
        (Sync, "not registered", not_registered),
        (Sync, "a head below the request", |message| {
            let (id, room) = request(message);
            batch(id, &room, 2, &[], false)
        }),
        (Ack, "not registered", not_registered),
        (Ack, "a head below the request", |message| {
            let ClientMessage::Ack {
                correlation_id,
                room_id,
                ack_seq,
                ..
            } = message
            else {
                panic!("{message:?}");
            };
            confirmed(*correlation_id, room_id, ack_seq.get(), ack_seq.get() - 1)
        }),
    ];
    for (kind, case, reply) in cases {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, 1..=3).await;
        client.sync(room_id.clone()).await.unwrap();
        transport.answer_every(kind, reply);
        let synced = client.sync(room_id.clone()).await;
        assert!(
            matches!(synced, Err(ClientError::Network(_))),
            "{kind:?} {case}: {synced:?}"
        );
        assert_eq!(transport.registrations().len(), 2, "{kind:?} {case}");
        transport.online();
        client.sync(room_id.clone()).await.unwrap();
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(3, 1, true)), "{kind:?} {case}");
    }
}

#[tokio::test]
async fn a_sync_or_an_ack_the_server_refuses_fails_and_changes_nothing() {
    let codes = [
        ErrorCode::Unavailable,
        ErrorCode::Internal,
        ErrorCode::Unauthorized,
    ];
    for kind in [Sync, Ack] {
        for code in codes {
            let dir = tempdir().unwrap();
            let (server, room_id, transport, _, client) =
                setup(dir.path(), engine(dir.path())).await;
            server.commit(&room_id, 1..=3).await;
            answer_next(&transport, kind, move |id, room| {
                error_reply(id, room, code)
            });
            let synced = client.sync(room_id.clone()).await;
            assert!(
                matches!(synced, Err(ClientError::Network(_))),
                "{kind:?} {code:?}: {synced:?}"
            );
            let head = if kind == Sync { 0 } else { 3 };
            let status_now = client.status(room_id.clone()).await;
            assert_eq!(status_now, Ok(status(head, 1, true)), "{kind:?} {code:?}");
            assert_eq!(transport.registrations().len(), 1, "{kind:?} {code:?}");
            client.sync(room_id.clone()).await.unwrap();
            let status_now = client.status(room_id.clone()).await;
            assert_eq!(status_now, Ok(status(3, 1, true)), "{kind:?} {code:?}");
        }
    }
}

#[tokio::test]
async fn behind_the_log_of_the_server_the_replica_registers_again_with_its_cursor() {
    // `Registered` tells whether it waits for a snapshot: here the log is whole, and the sync goes
    // on.
    for kind in [Sync, Ack] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, 1..=3).await;
        answer_next(&transport, kind, |id, room| {
            error_reply(id, room, ErrorCode::BehindCompaction)
        });
        client.sync(room_id.clone()).await.unwrap();
        let cursor = if kind == Sync { 0 } else { 3 };
        let incarnation = server.incarnation(&room_id).await;
        let registered = transport.registrations()[1..].to_vec();
        assert_eq!(registered, [(Some(cursor), Some(incarnation))], "{kind:?}");
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(3, 1, true)), "{kind:?}");
    }

    // A delayed reply, to a sync the replica moved past since, leaves it as it is.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    answer_next(&transport, Sync, |id, room| {
        error_reply(id, room, ErrorCode::BehindCompaction)
    });
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (client_ref, room) = (&client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        client_ref.sync(room).await.unwrap();
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    assert!(matches!(late, Err(ClientError::Network(_))), "{late:?}");
    server.commit(&room_id, 4..=5).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(5, 1, true)));
    assert_eq!(transport.registrations().len(), 1);
}

#[tokio::test]
async fn a_failed_write_of_a_batch_reopens_the_storage_room_and_the_next_sync_goes_on() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    // The batch is written but not synced, and the storage room refuses writes until it opens
    // again.
    files.fail_next("sync_data", "room_room-a.wal");
    let synced = client.sync(room_id.clone()).await;
    assert!(matches!(synced, Err(ClientError::Storage(_))), "{synced:?}");
    // Reopened from its files, it holds the batch.
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(3, 1, false))
    );
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);

    client.sync(room_id.clone()).await.unwrap();
    let cursor = (Some(3), Some(server.incarnation(&room_id).await));
    assert_eq!(transport.registrations().last(), Some(&cursor));
    assert_eq!(requests(&transport).last(), Some(&("ack", 3)));
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));
}

#[tokio::test]
async fn only_a_storage_error_after_which_the_room_may_have_changed_reopens_it() {
    for (case, reopens) in [
        ("Io", true),
        ("RoomFailed", true),
        ("SequenceMismatch", false),
        ("Other", false),
    ] {
        let dir = tempdir().unwrap();
        let storage = TestEngine::new(engine(dir.path()));
        let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
        server.commit(&room_id, 1..=3).await;
        let error = match case {
            "Io" => StorageError::Io(io::Error::other("injected")),
            "RoomFailed" => StorageError::RoomFailed {
                room_id: room_id.clone(),
                reason: "injected".to_string(),
            },
            "SequenceMismatch" => StorageError::SequenceMismatch {
                expected: SequenceNumber::new(1),
                actual: SequenceNumber::new(2),
            },
            _ => StorageError::Other("injected".to_string()),
        };
        storage.fail_next("apply_batch", error);
        // A reopen fails, and the room leaves the client.
        storage.fail_next("open_room", StorageError::RoomLocked(room_id.clone()));
        let synced = client.sync(room_id.clone()).await;
        assert!(
            matches!(synced, Err(ClientError::Storage(_))),
            "{case}: {synced:?}"
        );
        // The error of a reopen is the one reported.
        let locked =
            matches!(&synced, Err(ClientError::Storage(message)) if message.contains("locked"));
        assert_eq!(locked, reopens, "{case}: {synced:?}");
        let status_now = client.status(room_id.clone()).await;
        match reopens {
            true => assert_eq!(status_now, Err(ClientError::RoomClosed(room_id.clone()))),
            false => assert_eq!(status_now, Ok(status(0, 1, true)), "{case}"),
        }
        assert_eq!(deregistrations(&transport), 0, "{case}");
    }
}

#[tokio::test]
async fn a_reopen_that_finds_damage_discards_and_one_that_fails_to_close_leaves_the_room_out() {
    for damage in [true, false] {
        let dir = tempdir().unwrap();
        let storage = TestEngine::new(engine(dir.path()));
        let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
        let incarnation = server.incarnation(&room_id).await;
        server.commit(&room_id, 1..=3).await;
        storage.fail_next(
            "apply_batch",
            StorageError::Io(io::Error::other("injected")),
        );
        match damage {
            true => storage.fail_next("open_room", StorageError::SnapshotCorruption("x".into())),
            false => storage.fail_next("close_room", StorageError::Io(io::Error::other("x"))),
        }
        let synced = client.sync(room_id.clone()).await;
        assert!(
            matches!(synced, Err(ClientError::Storage(_))),
            "{damage}: {synced:?}"
        );
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(
            status_now,
            Err(ClientError::RoomClosed(room_id.clone())),
            "{damage}"
        );
        let saved = FileMetadata::new(dir.path()).room(&room_id).await.unwrap();
        assert_eq!(saved.is_none(), damage);

        // A discarded replica registers without a cursor; one left out opens with its cursor.
        if !damage {
            open_room(&client, &room_id).await.unwrap();
        }
        client.sync(room_id.clone()).await.unwrap();
        let registration = match damage {
            true => (None, None),
            false => (Some(0), Some(incarnation)),
        };
        assert_eq!(
            transport.registrations().last(),
            Some(&registration),
            "{damage}"
        );
        let epoch = if damage { 2 } else { 1 };
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(3, epoch, true)), "{damage}");
    }
}

#[tokio::test]
async fn a_batch_that_does_not_follow_the_head_is_not_applied_and_the_sync_goes_on() {
    // A memory engine loses the data of a room it closes: reopened after an error, the replica
    // is back at 0, and a batch asked for before does not follow its head.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = Client::open(
        ClientConfig::new("ana"),
        storage.clone(),
        MemoryMetadata::default(),
        transport.clone(),
    )
    .await
    .unwrap();
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    server.commit(&room_id, 4..=5).await;
    let mut held = transport.hold_next_reply(Sync);
    let late = client.sync(room_id.clone());
    let (storage_ref, client_ref, room) = (&storage, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        storage_ref.fail_next(
            "apply_batch",
            StorageError::Io(io::Error::other("injected")),
        );
        let synced = client_ref.sync(room.clone()).await;
        assert!(matches!(synced, Err(ClientError::Storage(_))), "{synced:?}");
        assert_eq!(client_ref.status(room).await, Ok(status(0, 1, false)));
        held.release();
    };
    let (late, ()) = tokio::join!(late, meanwhile);
    late.unwrap();
    let cursor = (Some(0), Some(server.incarnation(&room_id).await));
    assert_eq!(transport.registrations().last(), Some(&cursor));
    assert_eq!(client.status(room_id.clone()).await, Ok(status(5, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn a_sync_waits_for_a_change_of_the_replica_in_progress() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, _, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    storage.fail_next(
        "apply_batch",
        StorageError::Io(io::Error::other("injected")),
    );
    storage.slow_open(Duration::from_millis(300));
    let failed = client.sync(room_id.clone());
    let (client_ref, room) = (&client, room_id.clone());
    let meanwhile = async move {
        // While the storage room opens again, the room is out of the status.
        let reopening = async {
            while client_ref.status(room.clone()).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let reopening = tokio::time::timeout(Duration::from_secs(10), reopening).await;
        reopening.expect("the storage room did not open again");
        client_ref.sync(room).await
    };
    let (failed, synced) = tokio::join!(failed, meanwhile);
    assert!(matches!(failed, Err(ClientError::Storage(_))), "{failed:?}");
    synced.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));
}

#[tokio::test]
async fn a_registration_waits_for_a_change_of_the_replica_in_progress() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    let client = Arc::new(client);
    let sync = |client: &Arc<Client>| {
        let (client, room_id) = (Arc::clone(client), room_id.clone());
        tokio::spawn(async move { client.sync(room_id).await })
    };
    // A sync waits for its batch.
    let mut batch = transport.hold_next_reply(Sync);
    let applies = sync(&client);
    batch.replied().await;
    // Another one has to register again, and holds the registration lock until its
    // registration is refused.
    let (mut asked, mut registering) = (false, false);
    transport.answer(move |message| match message {
        ClientMessage::Sync { .. } if !std::mem::replace(&mut asked, true) => {
            let (id, room) = request(message);
            Some(Ok(error_reply(id, &room, ErrorCode::ClientNotRegistered)))
        }
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } if !std::mem::replace(&mut registering, true) => Some(Ok(error_reply(
            *correlation_id,
            room_id,
            ErrorCode::Unavailable,
        ))),
        _ => None,
    });
    let mut refused = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let fails = sync(&client);
    refused.replied().await;
    // A third one waits for the registration lock.
    let registers = sync(&client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The batch fails to apply, and the storage room opens again slowly.
    storage.fail_next(
        "apply_batch",
        StorageError::Io(io::Error::other("injected")),
    );
    storage.slow_open(Duration::from_millis(300));
    batch.release();
    let reopening = async {
        while client.status(room_id.clone()).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let reopening = tokio::time::timeout(Duration::from_secs(10), reopening).await;
    reopening.expect("the storage room did not open again");
    // The lock is released while the storage room opens: the registration waits for it.
    refused.release();
    let failed = fails.await.unwrap();
    assert!(matches!(failed, Err(ClientError::Network(_))), "{failed:?}");
    let applied = applies.await.unwrap();
    assert!(
        matches!(applied, Err(ClientError::Storage(_))),
        "{applied:?}"
    );
    registers.await.unwrap().unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));
}

#[tokio::test]
async fn a_registration_answered_after_the_replica_went_back_registers_again_with_its_head() {
    // A memory engine loses the data of a room it closes: reopened after an error, the replica
    // is back at 0, below the cursor of a registration in flight.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let incarnation = server.incarnation(&room_id).await;
    let transport = server.transport();
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = Client::open(
        ClientConfig::new("ana"),
        storage.clone(),
        MemoryMetadata::default(),
        transport.clone(),
    )
    .await
    .unwrap();
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    server.commit(&room_id, 4..=5).await;
    let client = Arc::new(client);
    let sync = |client: &Arc<Client>| {
        let (client, room_id) = (Arc::clone(client), room_id.clone());
        tokio::spawn(async move { client.sync(room_id).await })
    };
    // A sync waits for its batch, and another one, told to register again, for `Registered`.
    let mut batch = transport.hold_next_reply(Sync);
    let applies = sync(&client);
    batch.replied().await;
    answer_next(&transport, Sync, |id, room| {
        error_reply(id, room, ErrorCode::ClientNotRegistered)
    });
    let mut registered = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let registers = sync(&client);
    registered.replied().await;
    // The batch fails to apply, and the room opens again at 0.
    storage.fail_next(
        "apply_batch",
        StorageError::Io(io::Error::other("injected")),
    );
    batch.release();
    let applied = applies.await.unwrap();
    assert!(
        matches!(applied, Err(ClientError::Storage(_))),
        "{applied:?}"
    );
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(0, 1, false))
    );

    // `Registered` for the cursor 3 does not register the replica at 0, which registers again.
    registered.release();
    registers.await.unwrap().unwrap();
    let registrations = [
        (None, None),
        (Some(3), Some(incarnation)),
        (Some(0), Some(incarnation)),
    ];
    assert_eq!(transport.registrations(), registrations);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(5, 1, true)));
}

#[tokio::test]
async fn an_open_without_network_during_a_change_of_the_replica_opens_the_replica() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    let client = Arc::new(client);
    // A sync waits for its batch.
    let mut batch = transport.hold_next_reply(Sync);
    let applies = {
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        tokio::spawn(async move { client.sync(room_id).await })
    };
    batch.replied().await;
    // Another one leaves the replica unregistered: its registration is refused.
    let (mut asked, mut registering) = (false, false);
    transport.answer(move |message| match message {
        ClientMessage::Sync { .. } if !std::mem::replace(&mut asked, true) => {
            let (id, room) = request(message);
            Some(Ok(error_reply(id, &room, ErrorCode::ClientNotRegistered)))
        }
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } if !std::mem::replace(&mut registering, true) => Some(Ok(error_reply(
            *correlation_id,
            room_id,
            ErrorCode::Unavailable,
        ))),
        _ => None,
    });
    let refused = client.sync(room_id.clone()).await;
    assert!(
        matches!(refused, Err(ClientError::Network(_))),
        "{refused:?}"
    );
    // Without network, an open waits for its registration to fail.
    transport.offline();
    let mut offline = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let opens = {
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        tokio::spawn(async move { open_room(&client, &room_id).await })
    };
    offline.replied().await;
    // Meanwhile the batch fails to apply, and the storage room opens again slowly.
    storage.fail_next(
        "apply_batch",
        StorageError::Io(io::Error::other("injected")),
    );
    storage.slow_open(Duration::from_millis(300));
    batch.release();
    let reopening = async {
        while client.status(room_id.clone()).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let reopening = tokio::time::timeout(Duration::from_secs(10), reopening).await;
    reopening.expect("the storage room did not open again");
    // The registration fails while the storage room opens: the open waits for it, and the
    // replica opened unregistered, without the batch that failed.
    offline.release();
    opens.await.unwrap().unwrap();
    let applied = applies.await.unwrap();
    assert!(
        matches!(applied, Err(ClientError::Storage(_))),
        "{applied:?}"
    );
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(0, 1, false))
    );
}

#[tokio::test]
async fn a_reply_to_a_registration_of_a_replica_the_applier_discarded_changes_nothing() {
    // `Registered`, and a reset of the room created again.
    for reset in [false, true] {
        let dir = tempdir().unwrap();
        let storage = TestEngine::new(engine(dir.path()));
        let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
        let old = server.incarnation(&room_id).await;
        server.commit(&room_id, 1..=3).await;
        let client = Arc::new(client);
        let sync = |client: &Arc<Client>| {
            let (client, room_id) = (Arc::clone(client), room_id.clone());
            tokio::spawn(async move { client.sync(room_id).await })
        };
        // A sync waits for its batch.
        let mut batch = transport.hold_next_reply(Sync);
        let applies = sync(&client);
        batch.replied().await;
        // Another one has to register again, and waits for the reply.
        match reset {
            true => server.recreate_room(&room_id).await,
            false => answer_next(&transport, Sync, |id, room| {
                error_reply(id, room, ErrorCode::ClientNotRegistered)
            }),
        }
        let mut registered = transport.hold_next_reply(ClientMessageKind::RegisterClient);
        let registers = sync(&client);
        registered.replied().await;
        // The batch fails to apply, and the storage room opens again damaged: the applier
        // discards the replica.
        storage.fail_next(
            "apply_batch",
            StorageError::Io(io::Error::other("injected")),
        );
        storage.fail_next(
            "open_room",
            StorageError::SnapshotCorruption("injected".into()),
        );
        batch.release();
        let applied = applies.await.unwrap();
        assert!(
            matches!(applied, Err(ClientError::Storage(_))),
            "{applied:?}"
        );

        // The reply of the earlier epoch neither opens nor discards the empty replica, which
        // registers again without a cursor.
        registered.release();
        registers.await.unwrap().unwrap();
        let registrations = [(None, None), (Some(0), Some(old)), (None, None)];
        assert_eq!(transport.registrations(), registrations, "{reset}");
        let head = if reset { 0 } else { 3 };
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(head, 2, true)), "{reset}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sync_cut_while_it_applies_a_batch_leaves_the_batch_applied() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    let before = files.log().len();
    let written = async {
        let wal = |entry: &String| entry.starts_with("write_at room_room-a.wal");
        while !files.log()[before..].iter().any(wal) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    files.slow(Duration::from_millis(300));
    tokio::select! {
        _ = client.sync(room_id.clone()) => panic!("the write is slow"),
        () = written => {}
    }
    // The applier applies the whole batch, and the next sync goes on after it.
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(requests(&transport).last(), Some(&("ack", 3)));
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
}

#[tokio::test]
async fn a_sync_cut_at_any_point_leaves_a_head_the_applier_reached_whole() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=6).await;
    for polls in 1.. {
        let dir = tempdir().unwrap();
        let transport = server.transport();
        transport.sync_batch_size(2);
        let storage = engine(dir.path());
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        open_room(&client, &room_id).await.unwrap();
        let mut sync = Box::pin(client.sync(room_id.clone()));
        let mut finished = false;
        for _ in 0..polls {
            let polled = std::future::poll_fn(|cx| Poll::Ready(sync.as_mut().poll(cx)));
            if let Poll::Ready(synced) = polled.await {
                synced.unwrap();
                finished = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(sync);

        // Closing waits for the changes in progress: the storage room holds whole batches.
        client.close_room(room_id.clone()).await.unwrap();
        storage.open_room(&room_id, tasks_schema()).await.unwrap();
        let head = storage.get_head_seq(&room_id).await.unwrap().get();
        assert_eq!(head % 2, 0, "{polls}");
        let applied: Vec<i64> = (1..=i64::try_from(head).unwrap()).collect();
        assert_eq!(tasks(&storage, &room_id).await, applied, "{polls}");
        storage.close_room(&room_id).await.unwrap();

        // The next sync goes on from there.
        open_room(&client, &room_id).await.unwrap();
        client.sync(room_id.clone()).await.unwrap();
        assert_eq!(
            client.status(room_id.clone()).await,
            Ok(status(6, 1, true)),
            "{polls}"
        );
        client.shutdown().await.unwrap();
        if finished {
            break;
        }
    }
}
