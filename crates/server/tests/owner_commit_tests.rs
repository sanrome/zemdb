//! The server checks, before sequencing a commit, what needs no row: the owner the operation
//! declares is the client that sends it, and an immutable table only takes inserts. A
//! rejected commit is `SchemaViolation` and leaves no trace. What depends on the rows (whose a
//! row is, a repeated key, a `UNIQUE` or foreign key) is sequenced and left to the replicas.

mod common;

use common::{ask, commit, cursor, metrics, register, sync, TestServer};
use reqwest::StatusCode;
use tokio::sync::{broadcast, mpsc};
use zemdb_core::*;
use zemdb_server::{RoomCommand, RoomEvent, ServerError};

const POLLS: u16 = 0;
const VOTES: u16 = 1;
const NOTES: u16 = 2;
const BALLOTS: u16 = 3;

/// `polls(id, slug)` with `UNIQUE(slug)`; `votes(voter, poll, choice)` owned by `voter`, in
/// the key; `notes(id, author, body)` owned by `author`, outside the key; and `ballots`, like
/// `votes` but immutable.
fn ballots_schema() -> Schema {
    let votes = |name: &str| {
        TableSchema::builder(name)
            .primary_key("voter", DataType::String)
            .primary_key("poll", DataType::Int)
            .column("choice", DataType::Int)
            .owner("voter", OwnerIdentity::ClientId)
    };
    Schema::builder()
        .table(
            TableSchema::builder("polls")
                .primary_key("id", DataType::Int)
                .column("slug", DataType::String)
                .unique("polls_slug", ["slug"]),
        )
        .table(votes("votes"))
        .table(
            TableSchema::builder("notes")
                .primary_key("id", DataType::Int)
                .column("author", DataType::String)
                .nullable_column("body", DataType::String)
                .owner("author", OwnerIdentity::ClientId),
        )
        .table(votes("ballots").immutable())
        .build()
}

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

fn vote_key(voter: &str, poll: i64) -> PrimaryKey {
    PrimaryKey::composite([Value::from(voter), Value::Int(poll)])
}

fn vote(table: u16, voter: &str, poll: i64, choice: i64) -> Operation {
    let row = CompactRow::new(vec![voter.into(), poll.into(), choice.into()]);
    Operation::insert(table, vote_key(voter, poll), row, 1)
}

fn change_vote(table: u16, voter: &str, poll: i64, choice: i64) -> Operation {
    Operation::update(
        table,
        vote_key(voter, poll),
        vec![ColumnUpdate::new(2, choice)],
        1,
    )
}

fn note(id: i64, author: &str) -> Operation {
    let row = CompactRow::new(vec![id.into(), author.into(), "text".into()]);
    Operation::insert(NOTES, PrimaryKey::single(id), row, 1)
}

fn edit_note(id: i64, author: &str) -> Operation {
    let updates = vec![ColumnUpdate::new(1, author), ColumnUpdate::new(2, "edited")];
    Operation::update(NOTES, PrimaryKey::single(id), updates, 1)
}

fn delete_note(id: i64, author: &str) -> Operation {
    Operation::delete_with_owner(NOTES, PrimaryKey::single(id), author, 1)
}

fn poll(id: i64, slug: &str) -> Operation {
    let row = CompactRow::new(vec![id.into(), slug.into()]);
    Operation::insert(POLLS, PrimaryKey::single(id), row, 1)
}

/// A room with [`ballots_schema`] where `alice` and `bob` are registered.
struct Room {
    sender: mpsc::Sender<RoomCommand>,
    _parts: common::Parts,
    next_mutation: u8,
}

impl Room {
    async fn new() -> Self {
        let parts = common::build(|_| {});
        let schema_id = parts.register_schema("ballots", ballots_schema());
        let room_id = parts.create_room("ballot-room", &schema_id).await;
        let sender = parts
            .room_manager
            .get_or_spawn(&room_id, None)
            .await
            .unwrap();
        for id in ["alice", "bob"] {
            register(&sender, &client(id)).await;
        }
        Self {
            sender,
            _parts: parts,
            next_mutation: 0,
        }
    }

    async fn head(&self) -> u64 {
        metrics(&self.sender).await.head_seq.get()
    }

    /// Commits `op` from `client` as a new mutation, reporting the current head as its cursor.
    async fn commit(&mut self, client_id: &str, op: Operation) -> Result<u64, ServerError> {
        self.next_mutation += 1;
        let mutation = MutationId::new([self.next_mutation; 16]);
        self.commit_as(client_id, mutation, op).await
    }

    async fn commit_as(
        &self,
        client_id: &str,
        mutation: MutationId,
        op: Operation,
    ) -> Result<u64, ServerError> {
        let head = self.head().await;
        commit(&self.sender, &client(client_id), mutation, head, op)
            .await
            .map(|ack| ack.assigned_seq.get())
    }

    async fn subscribe(&self) -> broadcast::Receiver<RoomEvent> {
        ask(&self.sender, |reply| RoomCommand::SubscribeEvents { reply })
            .await
            .unwrap()
    }

    /// Commits `op` from `client_id` and asserts it is rejected with `SchemaViolation`
    /// without a trace: no new sequence, no event, the client's cursor where it was, and the
    /// mutation id not recorded, so the same id is a new mutation afterwards.
    async fn assert_rejected_without_trace(&mut self, client_id: &str, op: Operation) {
        let mut events = self.subscribe().await;
        let head = self.head().await;
        let before = cursor(&self.sender, &client(client_id)).await;
        self.next_mutation += 1;
        let mutation = MutationId::new([self.next_mutation; 16]);

        let result = self.commit_as(client_id, mutation, op.clone()).await;
        assert!(
            matches!(result, Err(ServerError::SchemaViolation(_))),
            "{op:?}: {result:?}"
        );
        assert_eq!(self.head().await, head, "{op:?}");
        assert!(events.try_recv().is_err(), "{op:?}");
        assert_eq!(cursor(&self.sender, &client(client_id)).await, before);

        let fresh = poll(1000 + i64::from(self.next_mutation), "fresh");
        let accepted = self.commit_as(client_id, mutation, fresh).await;
        assert_eq!(accepted.unwrap(), head + 1, "{op:?}");
    }
}

#[tokio::test]
async fn nobody_writes_in_the_name_of_another_owner() {
    let mut room = Room::new().await;
    let foreign = [
        // The owner in the key.
        vote(VOTES, "alice", 1, 2),
        change_vote(VOTES, "alice", 1, 3),
        Operation::delete(VOTES, vote_key("alice", 1), 1),
        // The owner outside the key.
        note(7, "alice"),
        edit_note(7, "alice"),
        delete_note(7, "alice"),
    ];
    for op in foreign {
        room.assert_rejected_without_trace("bob", op).await;
    }
}

#[tokio::test]
async fn the_owner_inserts_updates_and_deletes_its_rows() {
    let mut room = Room::new().await;
    let own = [
        vote(VOTES, "alice", 1, 2),
        change_vote(VOTES, "alice", 1, 3),
        Operation::delete(VOTES, vote_key("alice", 1), 1),
        note(7, "alice"),
        edit_note(7, "alice"),
        delete_note(7, "alice"),
    ];
    for (i, op) in own.into_iter().enumerate() {
        assert_eq!(room.commit("alice", op).await.unwrap(), i as u64 + 1);
    }
}

#[tokio::test]
async fn an_operation_that_does_not_declare_its_owner_is_rejected() {
    let mut room = Room::new().await;
    room.commit("alice", note(7, "alice")).await.unwrap();
    let undeclared = [
        // Outside the key, an update must set the owner column and a delete carry the owner.
        Operation::update(
            NOTES,
            PrimaryKey::single(7i64),
            vec![ColumnUpdate::new(2, "x")],
            1,
        ),
        Operation::delete(NOTES, PrimaryKey::single(7i64), 1),
        // Elsewhere, a delete carries no owner.
        Operation::delete_with_owner(VOTES, vote_key("alice", 1), "alice", 1),
        Operation::delete_with_owner(POLLS, PrimaryKey::single(1i64), "alice", 1),
    ];
    for op in undeclared {
        room.assert_rejected_without_trace("alice", op).await;
    }
}

#[tokio::test]
async fn declaring_oneself_owner_of_another_clients_row_is_left_to_the_replicas() {
    // The server holds no rows, so it cannot tell that note 7 is alice's: bob's operations
    // declare bob, are sequenced, and the replicas reject them.
    let mut room = Room::new().await;
    assert_eq!(room.commit("alice", note(7, "alice")).await.unwrap(), 1);
    assert_eq!(room.commit("bob", edit_note(7, "bob")).await.unwrap(), 2);
    assert_eq!(room.commit("bob", delete_note(7, "bob")).await.unwrap(), 3);
    assert_eq!(room.commit("bob", note(7, "bob")).await.unwrap(), 4);
}

#[tokio::test]
async fn a_retried_commit_is_acknowledged_with_its_original_sequence() {
    let mut room = Room::new().await;
    let mutation = MutationId::new([42; 16]);
    let first = room
        .commit_as("alice", mutation, vote(VOTES, "alice", 1, 2))
        .await
        .unwrap();
    room.commit("bob", vote(VOTES, "bob", 1, 2)).await.unwrap();

    let retry = room
        .commit_as("alice", mutation, vote(VOTES, "alice", 1, 2))
        .await;
    assert_eq!(retry.unwrap(), first);
    // A retry is recognized before the operation is checked: deduplication comes first.
    let retry = room
        .commit_as("alice", mutation, vote(VOTES, "bob", 1, 2))
        .await;
    assert_eq!(retry.unwrap(), first);
    assert_eq!(room.head().await, 2);
}

#[tokio::test]
async fn an_immutable_table_takes_only_inserts_and_repeated_keys_are_left_to_the_replicas() {
    let mut room = Room::new().await;
    assert_eq!(
        room.commit("alice", vote(BALLOTS, "alice", 1, 2))
            .await
            .unwrap(),
        1
    );
    room.assert_rejected_without_trace("alice", change_vote(BALLOTS, "alice", 1, 3))
        .await;
    room.assert_rejected_without_trace(
        "alice",
        Operation::delete(BALLOTS, vote_key("alice", 1), 1),
    )
    .await;
    // A second vote on the same key: the server cannot know the first one exists.
    let head = room.head().await;
    assert_eq!(
        room.commit("alice", vote(BALLOTS, "alice", 1, 3))
            .await
            .unwrap(),
        head + 1
    );
}

#[tokio::test]
async fn the_server_does_not_decide_unique_constraints() {
    let mut room = Room::new().await;
    assert_eq!(room.commit("alice", poll(1, "lunch")).await.unwrap(), 1);
    assert_eq!(room.commit("bob", poll(2, "lunch")).await.unwrap(), 2);
    let reader = client("reader");
    register(&room.sender, &reader).await;
    let synced = sync(&room.sender, &reader, 0, 10).await.unwrap();
    assert_eq!(synced.ops.len(), 2);
}

#[tokio::test]
async fn a_foreign_owner_is_a_schema_violation_over_http() {
    // Not `Forbidden`, which a client reads as a problem of its token.
    let server = TestServer::start(|_| {}).await;
    let schema_id = server.register_schema("ballots", ballots_schema());
    let room_id = server.create_room("http-ballots", &schema_id).await;
    let bob = client("bob");
    let (token, _) = server.register(&room_id, &bob).await;
    let commit = |op| ClientMessage::Commit {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: bob.clone(),
        mutation_id: MutationId::new([1; 16]),
        last_ack_seq: SequenceNumber::new(0),
        op,
    };

    let (status, reply) = server
        .post_message(
            &room_id,
            "commit",
            &token,
            &commit(vote(VOTES, "alice", 1, 2)),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::SchemaViolation,
                ..
            }
        ),
        "{reply:?}"
    );
    let (status, _) = server
        .post_message(
            &room_id,
            "commit",
            &token,
            &commit(vote(VOTES, "bob", 1, 2)),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_devices_of_a_person_write_as_the_person_before_the_first_hash() {
    let mut room = Room::new().await;
    for id in ["alice#phone", "alice#laptop", "bob#alice"] {
        register(&room.sender, &client(id)).await;
    }
    // Both devices of alice write her rows, also the rows the other one wrote.
    assert_eq!(
        room.commit("alice#phone", note(7, "alice")).await.unwrap(),
        1
    );
    assert_eq!(
        room.commit("alice#laptop", edit_note(7, "alice"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        room.commit("alice#laptop", vote(VOTES, "alice", 1, 2))
            .await
            .unwrap(),
        3
    );
    // No device writes as itself, and a device part that names someone else changes nothing.
    room.assert_rejected_without_trace("alice#phone", vote(VOTES, "alice#phone", 2, 1))
        .await;
    room.assert_rejected_without_trace("alice#phone", note(8, "alice#phone"))
        .await;
    room.assert_rejected_without_trace("bob#alice", vote(VOTES, "alice", 2, 1))
        .await;
    room.assert_rejected_without_trace("bob#alice", delete_note(7, "alice"))
        .await;
    let head = room.head().await;
    assert_eq!(
        room.commit("bob#alice", vote(VOTES, "bob", 2, 1))
            .await
            .unwrap(),
        head + 1
    );
}

#[tokio::test]
async fn the_devices_of_a_person_are_deduplicated_apart() {
    // The same mutation id from two devices of the same person is two mutations.
    let room = Room::new().await;
    for id in ["alice#phone", "alice#laptop"] {
        register(&room.sender, &client(id)).await;
    }
    let mutation = MutationId::new([7; 16]);
    let phone = room
        .commit_as("alice#phone", mutation, vote(VOTES, "alice", 1, 2))
        .await
        .unwrap();
    let laptop = room
        .commit_as("alice#laptop", mutation, vote(VOTES, "alice", 2, 2))
        .await
        .unwrap();
    assert_eq!((phone, laptop), (1, 2));
    let retry = room
        .commit_as("alice#phone", mutation, vote(VOTES, "alice", 1, 2))
        .await;
    assert_eq!(retry.unwrap(), phone);
}
