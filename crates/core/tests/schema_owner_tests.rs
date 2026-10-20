//! A table can declare an owner column, whose value identifies the client that owns each row,
//! and can be immutable. Both are validated when the table is built or deserialized, and are
//! part of the schema's identity.
//!
//! Every operation on a table with an owner declares it, and the server compares it with the
//! client that sends the operation; `validate_operation` rejects an operation that does not
//! declare it, a delete that declares one where none is expected, and an update or delete of
//! an immutable table. Neither needs any row: the checks that do are left to the replicas.

use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
use zemdb_core::*;

fn s(text: &str) -> String {
    text.to_string()
}

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

/// `polls(id Int, slug String)`, without an owner.
fn polls() -> TableBuilder {
    TableBuilder::new("polls")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column("slug", DataType::String)
}

/// `votes(voter String, poll Int, choice Int)`, keyed by `(voter, poll)` and owned by `voter`.
fn votes() -> TableBuilder {
    TableBuilder::new("votes")
        .table_id(1)
        .primary_key("voter", DataType::String)
        .primary_key("poll", DataType::Int)
        .column("choice", DataType::Int)
        .owner("voter", OwnerIdentity::ClientId)
}

/// `notes(id Int, author String, body String?, secret encrypted?, rank Int?, tag Bytes?)`,
/// owned by `author`, which is not part of the key.
fn notes() -> TableBuilder {
    TableBuilder::new("notes")
        .table_id(2)
        .primary_key("id", DataType::Int)
        .column("author", DataType::String)
        .nullable_column("body", DataType::String)
        .encrypted_column("secret", DataType::String)
        .nullable_column("rank", DataType::Int)
        .nullable_column("tag", DataType::Bytes)
        .owner("author", OwnerIdentity::ClientId)
}

fn schema(tables: impl IntoIterator<Item = TableBuilder>) -> Schema {
    tables
        .into_iter()
        .fold(Schema::builder(), SchemaBuilder::table)
        .build()
}

fn notes_with_owner(column: &str, identity: OwnerIdentity) -> Result<TableSchema, ValidationError> {
    notes().owner(column, identity).build()
}

fn vote(voter: &str, poll: i64, choice: i64) -> Operation {
    let row = RowBuilder::new()
        .set("voter", voter)
        .set("poll", poll)
        .set("choice", choice)
        .build();
    votes()
        .build()
        .unwrap()
        .to_operation_insert(&row, 1)
        .unwrap()
}

fn vote_key(voter: &str, poll: i64) -> PrimaryKey {
    PrimaryKey::composite([Value::from(voter), Value::Int(poll)])
}

fn note(id: i64, author: &str) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("author", author)
        .set("body", "text")
        .set("secret", vec![1u8, 2])
        .build();
    notes()
        .build()
        .unwrap()
        .to_operation_insert(&row, 1)
        .unwrap()
}

fn note_update(id: i64, fields: &[(&str, Value)]) -> Operation {
    let table = notes().build().unwrap();
    let updates = fields
        .iter()
        .map(|(column, value)| {
            ColumnUpdate::new(table.column_index(column).unwrap() as u16, value.clone())
        })
        .collect();
    Operation::update(2, PrimaryKey::single(id), updates, 1)
}

// ---------------------------------------------------------------------------------------------
// Definition
// ---------------------------------------------------------------------------------------------

#[test]
fn an_owner_column_that_does_not_exist_is_rejected() {
    assert_eq!(
        notes_with_owner("ghost", OwnerIdentity::ClientId).unwrap_err(),
        ValidationError::UnknownColumn {
            table: s("notes"),
            column: s("ghost"),
        }
    );
}

#[test]
fn an_owner_column_must_be_a_string_neither_nullable_nor_encrypted() {
    for column in ["secret", "body", "rank", "tag"] {
        assert_eq!(
            notes_with_owner(column, OwnerIdentity::ClientId).unwrap_err(),
            ValidationError::InvalidOwnerColumn {
                table: s("notes"),
                column: s(column),
            },
            "{column}"
        );
    }
    // The key columns qualify: they are never nullable nor encrypted.
    let owned_by_key = notes_with_owner("id", OwnerIdentity::ClientId);
    assert!(
        matches!(
            owned_by_key,
            Err(ValidationError::InvalidOwnerColumn { .. })
        ),
        "{owned_by_key:?}"
    );
    assert!(votes().build().is_ok());
}

#[test]
fn a_pseudonymous_owner_is_rejected_until_it_is_supported() {
    let table = TableBuilder::new("ballots")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column("voter", DataType::Bytes)
        .owner("voter", OwnerIdentity::Pseudonym)
        .build();
    assert_eq!(
        table.unwrap_err(),
        ValidationError::UnsupportedOwnerIdentity {
            table: s("ballots")
        }
    );
}

#[test]
fn an_invalid_owner_in_json_is_rejected() {
    let table = |owner: Json| {
        json!({
            "table_id": 0, "name": "votes", "primary_key": ["id"],
            "columns": [
                {"name": "id", "data_type": "Int", "nullable": false, "encrypted": false},
                {"name": "voter", "data_type": "String", "nullable": false, "encrypted": false}
            ],
            "owner": owner
        })
    };
    let parse = |owner: Json| serde_json::from_value::<TableSchema>(table(owner));
    assert!(parse(json!({"column": "voter", "identity": "ClientId"})).is_ok());
    for owner in [
        json!({"column": "ghost", "identity": "ClientId"}),
        json!({"column": "id", "identity": "ClientId"}),
        json!({"column": "voter", "identity": "Pseudonym"}),
        json!({"column": "voter", "identity": "Email"}),
        json!({"column": "voter"}),
        json!({"column": "voter", "identity": "ClientId", "signed": true}),
    ] {
        assert!(parse(owner.clone()).is_err(), "{owner}");
    }
}

/// The JSON of the integrity constraints proposal, with an owner in the key and an immutable
/// table.
fn votes_schema_json() -> Json {
    json!({
        "version": 1,
        "tables": [
            {
                "table_id": 0, "name": "polls", "primary_key": ["id"],
                "columns": [
                    {"name": "id", "data_type": "Uuid", "nullable": false, "encrypted": false},
                    {"name": "slug", "data_type": "String", "nullable": false, "encrypted": false}
                ],
                "unique": [{"name": "polls_slug", "columns": ["slug"]}],
                "foreign_keys": []
            },
            {
                "table_id": 1, "name": "votes", "primary_key": ["voter", "poll_id"],
                "columns": [
                    {"name": "voter", "data_type": "String", "nullable": false, "encrypted": false},
                    {"name": "poll_id", "data_type": "Uuid", "nullable": false, "encrypted": false},
                    {"name": "choice", "data_type": "Int", "nullable": false, "encrypted": false}
                ],
                "owner": {"column": "voter", "identity": "ClientId"},
                "immutable": true,
                "unique": [],
                "foreign_keys": [
                    {"name": "votes_poll", "columns": ["poll_id"], "references": "polls", "on_delete": "Restrict"}
                ]
            }
        ]
    })
}

#[test]
fn the_owner_and_immutability_round_trip_in_json_and_binary() {
    let schema: Schema = serde_json::from_value(votes_schema_json()).unwrap();
    let votes = schema.get_table_by_name("votes").unwrap();
    assert_eq!(
        votes.owner(),
        Some(&OwnerDef::new("voter", OwnerIdentity::ClientId))
    );
    assert_eq!(votes.owner_column_index(), Some(0));
    assert!(votes.is_immutable());
    let polls = schema.get_table_by_name("polls").unwrap();
    assert_eq!((polls.owner(), polls.is_immutable()), (None, false));

    let json = serde_json::to_value(&schema).unwrap();
    assert_eq!(
        json["tables"][1]["owner"],
        votes_schema_json()["tables"][1]["owner"]
    );
    assert_eq!(json["tables"][1]["immutable"], true);
    assert_eq!(json["tables"][0]["owner"], Json::Null);
    assert_eq!(serde_json::from_value::<Schema>(json).unwrap(), schema);

    let bytes = bincode::serialize(&schema).unwrap();
    let decoded: Schema = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded, schema);
    assert_eq!(
        decoded
            .get_table_by_name("votes")
            .unwrap()
            .owner_column_index(),
        Some(0)
    );
}

#[test]
fn a_table_without_owner_or_immutability_still_reads() {
    let mut json = votes_schema_json();
    let votes = json["tables"][1].as_object_mut().unwrap();
    votes.remove("owner");
    votes.remove("immutable");
    let schema: Schema = serde_json::from_value(json).unwrap();
    let votes = schema.get_table_by_name("votes").unwrap();
    assert_eq!((votes.owner(), votes.is_immutable()), (None, false));
}

#[test]
fn the_owner_and_immutability_are_part_of_the_schema_identity() {
    let plain = schema([polls()]);
    assert_ne!(schema([polls().immutable()]), plain);
    let owned = TableBuilder::new("polls")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column("slug", DataType::String)
        .owner("slug", OwnerIdentity::ClientId);
    assert_ne!(schema([owned]), plain);
    assert_eq!(schema([polls()]), plain);
}

#[test]
fn adding_a_column_keeps_the_owner_and_immutability() {
    let original = schema([polls(), votes().immutable(), notes()]);
    let column = || ColumnDef::new("comment", DataType::String).nullable(true);

    let mut plain = original.clone();
    plain.add_column("votes", column()).unwrap();
    let mut constrained = original.clone();
    constrained
        .add_column_with_constraints(
            "votes",
            column(),
            vec![UniqueConstraint::new("votes_comment", ["comment"])],
            Vec::new(),
        )
        .unwrap();
    let mut notes_evolved = original.clone();
    notes_evolved
        .add_column_with_constraints(
            "notes",
            ColumnDef::new("poll", DataType::Int).nullable(true),
            Vec::new(),
            vec![ForeignKey::new("notes_poll", ["poll"], "polls")],
        )
        .unwrap();

    for evolved in [&plain, &constrained] {
        let votes = evolved.get_table_by_name("votes").unwrap();
        assert_eq!(
            votes.owner(),
            Some(&OwnerDef::new("voter", OwnerIdentity::ClientId))
        );
        assert_eq!(votes.owner_column_index(), Some(0));
        assert!(votes.is_immutable());
    }
    let notes = notes_evolved.get_table_by_name("notes").unwrap();
    assert_eq!(notes.owner_column_index(), Some(1));
    assert!(!notes.is_immutable());
}

// ---------------------------------------------------------------------------------------------
// What `validate_operation` requires
// ---------------------------------------------------------------------------------------------

#[test]
fn an_immutable_table_only_takes_inserts() {
    let schema = schema([polls(), votes().immutable()]);
    let immutable = Err(ValidationError::ImmutableTable { table: s("votes") });

    assert_eq!(schema.validate_operation(&vote("alice", 1, 2)), Ok(()));
    let update = Operation::update(1, vote_key("alice", 1), vec![ColumnUpdate::new(2, 3i64)], 1);
    assert_eq!(schema.validate_operation(&update), immutable);
    let delete = Operation::delete(1, vote_key("alice", 1), 1);
    assert_eq!(schema.validate_operation(&delete), immutable);

    // The same operations are valid on the table when it is not immutable.
    let mutable = self::schema([polls(), votes()]);
    assert_eq!(mutable.validate_operation(&update), Ok(()));
    assert_eq!(mutable.validate_operation(&delete), Ok(()));

    // The helpers that compile operations validate them the same way.
    let table = schema.get_table_by_name("votes").unwrap();
    let fields = BTreeMap::from([(s("choice"), Value::Int(3))]);
    let immutable = ValidationError::ImmutableTable { table: s("votes") };
    assert_eq!(
        table
            .to_operation_update(vote_key("alice", 1), &fields, 1)
            .unwrap_err(),
        immutable
    );
    assert_eq!(
        table
            .to_operation_delete(vote_key("alice", 1), 1)
            .unwrap_err(),
        immutable
    );
    // And so do the validators by table name.
    assert_eq!(
        schema
            .validate_update("votes", &vote_key("alice", 1), &fields)
            .unwrap_err(),
        immutable
    );
    assert_eq!(
        schema
            .validate_delete("votes", &vote_key("alice", 1))
            .unwrap_err(),
        immutable
    );
}

#[test]
fn with_the_owner_outside_the_key_an_update_must_set_the_owner_column() {
    let schema = schema([notes()]);
    let missing = Err(ValidationError::MissingOwner { table: s("notes") });

    let without_owner = note_update(7, &[("body", Value::from("edited"))]);
    assert_eq!(schema.validate_operation(&without_owner), missing);
    let with_owner = note_update(
        7,
        &[
            ("author", Value::from("alice")),
            ("body", Value::from("edited")),
        ],
    );
    assert_eq!(schema.validate_operation(&with_owner), Ok(()));
    let owner_only = note_update(7, &[("author", Value::from("alice"))]);
    assert_eq!(schema.validate_operation(&owner_only), Ok(()));
    // The owner column is not nullable: a null owner declares none.
    let null_owner = note_update(7, &[("author", Value::Null)]);
    assert!(schema.validate_operation(&null_owner).is_err());

    let table = schema.get_table_by_name("notes").unwrap();
    let fields = BTreeMap::from([(s("body"), Value::from("edited"))]);
    assert_eq!(
        table
            .to_operation_update(PrimaryKey::single(7i64), &fields, 1)
            .unwrap_err(),
        ValidationError::MissingOwner { table: s("notes") }
    );
    assert_eq!(
        schema
            .validate_update("notes", &PrimaryKey::single(7i64), &fields)
            .unwrap_err(),
        ValidationError::MissingOwner { table: s("notes") }
    );
    let with_author = BTreeMap::from([(s("author"), Value::from("alice"))]);
    assert_eq!(
        schema.validate_update("notes", &PrimaryKey::single(7i64), &with_author),
        Ok(())
    );
}

#[test]
fn with_the_owner_outside_the_key_a_delete_must_carry_the_owner() {
    let schema = schema([notes()]);
    let key = || PrimaryKey::single(7i64);

    assert_eq!(
        schema.validate_operation(&Operation::delete(2, key(), 1)),
        Err(ValidationError::MissingOwner { table: s("notes") })
    );
    assert_eq!(
        schema.validate_operation(&Operation::delete_with_owner(2, key(), "alice", 1)),
        Ok(())
    );
    // The owner a delete carries is a value of the owner column.
    for owner in [Value::Null, Value::Int(1), Value::from(vec![1u8])] {
        let delete = Operation::delete_with_owner(2, key(), owner.clone(), 1);
        assert!(schema.validate_operation(&delete).is_err(), "{owner:?}");
    }
    assert_eq!(
        schema
            .get_table_by_name("notes")
            .unwrap()
            .to_operation_delete(key(), 1)
            .unwrap_err(),
        ValidationError::MissingOwner { table: s("notes") }
    );
    assert_eq!(
        schema.validate_delete("notes", &key()).unwrap_err(),
        ValidationError::MissingOwner { table: s("notes") }
    );
}

#[test]
fn a_delete_cannot_carry_an_owner_unless_the_owner_is_outside_the_key() {
    let schema = schema([polls(), votes()]);
    let in_key = Operation::delete_with_owner(1, vote_key("alice", 1), "alice", 1);
    assert_eq!(
        schema.validate_operation(&in_key),
        Err(ValidationError::UnexpectedOwner { table: s("votes") })
    );
    let no_owner = Operation::delete_with_owner(0, PrimaryKey::single(1i64), "alice", 1);
    assert_eq!(
        schema.validate_operation(&no_owner),
        Err(ValidationError::UnexpectedOwner { table: s("polls") })
    );
    assert_eq!(
        schema.validate_operation(&Operation::delete(1, vote_key("alice", 1), 1)),
        Ok(())
    );
    assert_eq!(
        schema.validate_operation(&Operation::delete(0, PrimaryKey::single(1i64), 1)),
        Ok(())
    );
}

// ---------------------------------------------------------------------------------------------
// The declared owner and the sending client
// ---------------------------------------------------------------------------------------------

#[test]
fn the_declared_owner_comes_from_the_key_the_row_the_delta_or_the_delete() {
    let schema = schema([polls(), votes(), notes()]);
    let owner = |op: &Operation| {
        schema
            .get_table_by_id(op.table_id())
            .unwrap()
            .declared_owner(op)
            .cloned()
    };
    let alice = Some(Value::from("alice"));

    // In the key.
    assert_eq!(owner(&vote("alice", 1, 2)), alice);
    let update = Operation::update(1, vote_key("alice", 1), vec![ColumnUpdate::new(2, 3i64)], 1);
    assert_eq!(owner(&update), alice);
    assert_eq!(owner(&Operation::delete(1, vote_key("alice", 1), 1)), alice);

    // Outside the key.
    assert_eq!(owner(&note(7, "alice")), alice);
    let update = note_update(
        7,
        &[("author", Value::from("alice")), ("body", Value::from("x"))],
    );
    assert_eq!(owner(&update), alice);
    let delete = Operation::delete_with_owner(2, PrimaryKey::single(7i64), "alice", 1);
    assert_eq!(owner(&delete), alice);

    // A table without an owner.
    let poll = Operation::delete(0, PrimaryKey::single(1i64), 1);
    assert_eq!(owner(&poll), None);
}

#[test]
fn only_the_declared_owner_may_send_an_operation() {
    let schema = schema([polls(), votes(), notes()]);
    let alice = client("alice");
    let operations = [
        vote("alice", 1, 2),
        Operation::update(1, vote_key("alice", 1), vec![ColumnUpdate::new(2, 3i64)], 1),
        Operation::delete(1, vote_key("alice", 1), 1),
        note(7, "alice"),
        note_update(7, &[("author", Value::from("alice"))]),
        Operation::delete_with_owner(2, PrimaryKey::single(7i64), "alice", 1),
    ];
    for op in &operations {
        assert_eq!(schema.validate_operation(op), Ok(()), "{op:?}");
        assert_eq!(schema.check_owner(op, &alice), Ok(()), "{op:?}");
        let table = schema.get_table_by_id(op.table_id()).unwrap().name();
        // The comparison is byte for byte: another case, another Unicode normalization or
        // surrounding text are other clients.
        for other in ["bob", "Alice", "alice2", "xalice"] {
            assert_eq!(
                schema.check_owner(op, &client(other)),
                Err(ValidationError::OwnerMismatch { table: s(table) }),
                "{other}: {op:?}"
            );
        }
    }

    let composed = client("jos\u{e9}");
    let decomposed = client("jose\u{301}");
    let by_composed = vote("jos\u{e9}", 1, 2);
    assert_eq!(schema.check_owner(&by_composed, &composed), Ok(()));
    assert!(schema.check_owner(&by_composed, &decomposed).is_err());

    // A table without an owner accepts any client.
    let poll = Operation::delete(0, PrimaryKey::single(1i64), 1);
    assert_eq!(schema.check_owner(&poll, &client("bob")), Ok(()));
}

// ---------------------------------------------------------------------------------------------
// Reassigning a room to another schema
// ---------------------------------------------------------------------------------------------

#[test]
fn schemas_with_the_same_constraints_owners_and_immutability_are_interchangeable() {
    let base = || {
        [
            polls().unique("polls_slug", ["slug"]),
            votes()
                .immutable()
                .foreign_key("votes_poll", ["poll"], "polls"),
        ]
    };
    let original = schema(base());
    assert!(original.has_same_constraints(&original.clone()));

    // Columns and tables without constraints do not matter.
    let mut wider = original.clone();
    wider
        .add_column(
            "polls",
            ColumnDef::new("title", DataType::String).nullable(true),
        )
        .unwrap();
    assert!(original.has_same_constraints(&wider));
    let mut with_table = original.clone();
    with_table
        .insert_table(
            TableBuilder::new("extra")
                .table_id(5)
                .primary_key("id", DataType::Int)
                .build()
                .unwrap(),
        )
        .unwrap();
    assert!(original.has_same_constraints(&with_table));

    let mut with_unique = original.clone();
    with_unique
        .add_column_with_constraints(
            "polls",
            ColumnDef::new("code", DataType::String).nullable(true),
            vec![UniqueConstraint::new("polls_code", ["code"])],
            Vec::new(),
        )
        .unwrap();
    let [polls_t, votes_t] = base();
    let different = [
        with_unique,
        schema([polls(), votes_t.clone()]),
        schema([
            polls_t.clone().unique("polls_other", ["slug"]),
            votes_t.clone(),
        ]),
        schema([
            polls_t.clone(),
            votes().foreign_key("votes_poll", ["poll"], "polls"),
        ]),
        schema([
            polls_t.clone(),
            TableBuilder::new("votes")
                .table_id(1)
                .primary_key("voter", DataType::String)
                .primary_key("poll", DataType::Int)
                .column("choice", DataType::Int)
                .immutable()
                .foreign_key("votes_poll", ["poll"], "polls"),
        ]),
        // The same names, but `polls` is another table id.
        schema([
            TableBuilder::new("polls")
                .table_id(3)
                .primary_key("id", DataType::Int)
                .column("slug", DataType::String)
                .unique("polls_slug", ["slug"]),
            votes_t.clone(),
        ]),
        // The same constraint over another column position.
        schema([
            TableBuilder::new("polls")
                .table_id(0)
                .primary_key("id", DataType::Int)
                .column("extra", DataType::String)
                .column("slug", DataType::String)
                .unique("polls_slug", ["slug"]),
            votes_t.clone(),
        ]),
        // The same positions under other names: the name is the reason a replica reports.
        schema([polls().unique("polls_renamed", ["slug"]), votes_t]),
        schema([
            polls_t.clone(),
            votes()
                .immutable()
                .foreign_key("votes_renamed", ["poll"], "polls"),
        ]),
        // The owner column renamed, at the same position.
        schema([
            polls_t.clone(),
            TableBuilder::new("votes")
                .table_id(1)
                .primary_key("elector", DataType::String)
                .primary_key("poll", DataType::Int)
                .column("choice", DataType::Int)
                .owner("elector", OwnerIdentity::ClientId)
                .immutable()
                .foreign_key("votes_poll", ["poll"], "polls"),
        ]),
        // The same foreign key over another column position.
        schema([
            polls_t.clone(),
            TableBuilder::new("votes")
                .table_id(1)
                .primary_key("voter", DataType::String)
                .column("choice", DataType::Int)
                .primary_key("poll", DataType::Int)
                .owner("voter", OwnerIdentity::ClientId)
                .immutable()
                .foreign_key("votes_poll", ["poll"], "polls"),
        ]),
        // The owner column with its name, at another position.
        schema([
            polls_t,
            TableBuilder::new("votes")
                .table_id(1)
                .column("choice", DataType::String)
                .column("poll", DataType::Int)
                .primary_key("voter", DataType::String)
                .primary_key("poll", DataType::Int)
                .owner("voter", OwnerIdentity::ClientId)
                .immutable()
                .foreign_key("votes_poll", ["poll"], "polls"),
        ]),
    ];
    for other in &different {
        assert!(!original.has_same_constraints(other), "{other:?}");
        assert!(!other.has_same_constraints(&original), "{other:?}");
    }

    // The same constraint, with the same name and column positions, on another table.
    let pair = |constrained: u16| {
        let table = |id: u16| {
            let table = TableBuilder::new(format!("t{id}"))
                .table_id(id)
                .primary_key("id", DataType::Int)
                .column("code", DataType::String);
            if id == constrained {
                table.unique("code_unique", ["code"])
            } else {
                table
            }
        };
        schema([table(0), table(1)])
    };
    assert!(!pair(0).has_same_constraints(&pair(1)));
    assert!(pair(0).has_same_constraints(&pair(0)));

    // A foreign key that references, by the same name, a table without constraints of its
    // own that has another id.
    let referencing = |polls_id: u16| {
        let polls = TableBuilder::new("polls")
            .table_id(polls_id)
            .primary_key("id", DataType::Int);
        schema([polls, votes().foreign_key("votes_poll", ["poll"], "polls")])
    };
    assert!(!referencing(0).has_same_constraints(&referencing(4)));
    assert!(referencing(4).has_same_constraints(&referencing(4)));

    // A table whose only constraint is a foreign key, over one column or another.
    let foreign_key_only = |column: &str| {
        let links = TableBuilder::new("links")
            .table_id(3)
            .primary_key("id", DataType::Int)
            .column("a", DataType::Int)
            .column("b", DataType::Int)
            .foreign_key("links_poll", [column], "polls");
        schema([polls(), links])
    };
    assert!(foreign_key_only("a").has_same_constraints(&foreign_key_only("a")));
    assert!(!foreign_key_only("a").has_same_constraints(&foreign_key_only("b")));
}
