use super::*;
use crate::{Rejection, Violation};
use zemdb_core::{DataType, OwnerIdentity, ValidationError};

fn client() -> ClientId {
    ClientId::new("ana#phone").unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room-a").unwrap()
}

/// Whether `op` fits alone in a record of the log and in a response with the sequence `seq` and
/// `mutation_id`, as the server checks it.
fn fits(op: Operation, seq: u64, mutation_id: Option<MutationId>) -> bool {
    let op = SequencedOperation::new(SequenceNumber::new(seq), op);
    check_operation_size(&op, mutation_id).is_ok()
}

/// The largest `extra` below `high` for which `op(extra)` fits with the sequence of the worst case
/// and a mutation id, by bisection: `op(0)` fits, and `op(high)` does not.
fn border(op: impl Fn(usize) -> Operation, high: usize) -> usize {
    let worst = |extra| fits(op(extra), u64::MAX, Some(MutationId::from(0)));
    let (mut low, mut high) = (0, high);
    assert!(worst(low) && !worst(high));
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        match worst(middle) {
            true => low = middle,
            false => high = middle,
        }
    }
    low
}

/// An insert with a string of `extra` bytes: its response binds first, where the sequence counts.
fn wide(extra: usize) -> Operation {
    let row = CompactRow::new(vec![Value::from(1), Value::from("x".repeat(extra))]);
    Operation::insert(0, PrimaryKey::single(1), row, 0)
}

#[test]
fn an_operation_fits_with_the_sequence_of_the_worst_case() {
    let max = border(wide, 17 << 20);
    assert!(fitting(wide(max), &client(), &room_id()).is_ok());
    // A byte more fits with a short sequence, not with the worst one.
    assert!(fits(wide(max + 1), 1, Some(MutationId::from(0))));
    let message = match fitting(wide(max + 1), &client(), &room_id()) {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("too large"), "{message}");
}

#[test]
fn an_operation_fits_with_a_mutation_id() {
    // An update with many values, whose record in the log takes more than its response: the log
    // binds first, where the mutation id counts.
    let updates: Vec<_> = (1..=1000)
        .map(|column| ColumnUpdate::new(column, "y".repeat(16 << 10)))
        .collect();
    let many = |extra: usize| {
        let mut updates = updates.clone();
        updates.push(ColumnUpdate::new(1001, "y".repeat(extra)));
        Operation::update(0, PrimaryKey::single(1), updates, 0)
    };
    let max = border(many, 1 << 20);
    assert!(fitting(many(max), &client(), &room_id()).is_ok());
    assert!(fits(many(max + 1), u64::MAX, None));
    assert!(fitting(many(max + 1), &client(), &room_id()).is_err());
}

#[test]
fn an_operation_fits_in_the_commit_that_carries_it() {
    // The largest operation that fits alone fits in the commit of short ids, not in that of the
    // longest room id and a long client id.
    let max = border(wide, 17 << 20);
    assert!(fitting(wide(max), &client(), &room_id()).is_ok());
    let person = "p".repeat(200);
    let long_client = ClientId::new(format!("{person}#device")).unwrap();
    let long_room = RoomId::new("r".repeat(64)).unwrap();
    let message = match fitting(wide(max), &long_client, &long_room) {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("its commit encodes to"), "{message}");

    // The largest operation that fits in the commit of the long ids, by bisection on `fitting`,
    // encodes in a commit with the correlation id and the cursor of the worst case, and a byte
    // more does not.
    let accepted = |extra| fitting(wide(extra), &long_client, &long_room).is_ok();
    let (mut low, mut high) = (0, max);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        match accepted(middle) {
            true => low = middle,
            false => high = middle,
        }
    }
    let commit = |extra| ClientMessage::Commit {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: long_room.clone(),
        client_id: long_client.clone(),
        mutation_id: MutationId::from(u128::MAX),
        last_ack_seq: SequenceNumber::new(u64::MAX),
        op: wide(extra),
    };
    assert!(zemdb_core::encode_message(&commit(low)).is_ok());
    assert!(zemdb_core::encode_message(&commit(low + 1)).is_err());
}

#[test]
fn values_that_take_more_than_a_message_are_too_large_before_the_plan() {
    let half = || Value::from("x".repeat(8 << 20));
    assert_eq!(bounded([&half()]), Ok(()));
    let (one, two) = (half(), half());
    let message = match bounded([&one, &two]) {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("too large"), "{message}");
}

/// Table `members`, keyed by an int `ranking_id` and the owner column `member_id`; and
/// `rankings`, keyed by an int `ranking_id`, with its owner `creator` outside the key.
fn owned_schema() -> Schema {
    let members = TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Int)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId);
    let rankings = TableSchema::builder("rankings")
        .primary_key("ranking_id", DataType::Int)
        .column("creator", DataType::String)
        .owner("creator", OwnerIdentity::ClientId);
    Schema::builder().table(members).table(rankings).build()
}

#[tokio::test]
async fn an_operation_with_an_owner_other_than_the_client_is_not_sent() {
    let (schema, ana, room) = (owned_schema(), client(), room_id());
    let member = Row::new().with("ranking_id", 1).with("display_name", "Ana");
    let ranking = Key::new().with("ranking_id", 1);
    // A writer that would fill another owner: the operation it builds is valid for the schema,
    // and its owner is checked against the client before it is sent.
    let mut other = Writer::new(&ana);
    other.owner = Value::from("bob");
    for (writer, valid) in [(Writer::new(&ana), true), (other, false)] {
        let (inserted, _) = writer.insert(&schema, "members", &member).unwrap();
        let deleted = writer.delete(&schema, "rankings", &ranking).unwrap();
        for draft in [inserted, deleted] {
            let op = draft.seal(None, &room).await.unwrap();
            assert_eq!(schema.validate_operation(&op), Ok(()));
            match checked(&schema, op, &ana) {
                Ok(_) if valid => {}
                Err(ClientError::InvalidOperation(message)) if !valid => {
                    let other = "other than the owner of the client";
                    assert!(message.contains(other), "{message}");
                }
                other => panic!("{other:?}"),
            }
        }
    }
}

#[test]
fn the_precheck_takes_the_decision_and_the_errors_of_the_storage() {
    let room = room_id();
    let name = || "rankings_name".to_string();
    let constraint = Rejection::Constraint;
    let reasons = [
        (RejectReason::MissingRow, Rejection::RowMissing),
        (RejectReason::Duplicate, constraint(Violation::Duplicate)),
        (RejectReason::NotOwner, constraint(Violation::NotOwner)),
        (
            RejectReason::Unique { constraint: name() },
            constraint(Violation::Unique { constraint: name() }),
        ),
        (
            RejectReason::ForeignKey { constraint: name() },
            constraint(Violation::ForeignKey { constraint: name() }),
        ),
        (
            RejectReason::Restrict { constraint: name() },
            constraint(Violation::Restrict { constraint: name() }),
        ),
    ];
    assert_eq!(prechecked(Ok(None), &room), Ok(()));
    // The reason of a rejection is the one the replica gives on a sequence.
    for (reason, rejection) in reasons {
        let decided = match rejection.clone() {
            Rejection::Constraint(violation) => ClientError::ConstraintViolation(violation),
            Rejection::RowMissing => ClientError::RowMissing,
        };
        assert_eq!(ClientError::from(reason.clone()), decided);
        let failed = prechecked(Ok(Some(reason)), &room);
        assert_eq!(failed, Err(ClientError::PrecheckFailed(rejection)));
    }

    // A storage room that does not take the operation, or is closed.
    let no_table = StorageError::TableNotFound {
        room_id: room.clone(),
        table: "rankings".to_string(),
    };
    let invalid = ValidationError::EmptyUpdate("rankings".to_string());
    for err in [no_table, StorageError::SchemaValidation(invalid)] {
        let shown = err.to_string();
        let message = match prechecked(Err(err), &room) {
            Err(ClientError::InvalidOperation(message)) => message,
            other => panic!("{other:?}"),
        };
        assert!(message.ends_with(&shown), "{message}");
    }
    let closed = prechecked(Err(StorageError::RoomNotFound(room.clone())), &room);
    assert_eq!(closed, Err(ClientError::RoomClosed(room.clone())));

    // Any other error leaves the precheck without a decision.
    let others = [
        StorageError::Io(std::io::Error::other("The disk failed")),
        StorageError::RoomFailed {
            room_id: room.clone(),
            reason: "A compaction failed".to_string(),
        },
        StorageError::Serialization("A row".to_string()),
        StorageError::RoomAlreadyOpen(room.clone()),
        StorageError::RoomLocked(room.clone()),
    ];
    for err in others {
        assert_eq!(prechecked(Err(err), &room), Ok(()));
    }
}
