//! A replica switches a room it holds to another schema only if the new one evolves from its
//! own: it extends it (the same tables, keys and columns, plus nullable columns and tables),
//! keeps every constraint, owner and immutable table as it was, and every constraint it adds to
//! an existing table includes a column that table did not have. Such a constraint finds that
//! column `NULL` in the rows already there, so no decision already taken changes.
//!
//! The ignored variant runs many more cases: `cargo test --release -- --ignored`.

mod common;

use common::*;
use zemdb_core::*;

fn rankings_columns() -> TableBuilder {
    TableSchema::builder("rankings")
        .primary_key("ranking_id", DataType::Uuid)
        .column("creator", DataType::String)
        .column("name", DataType::String)
}

fn rankings() -> TableBuilder {
    rankings_columns()
        .owner("creator", OwnerIdentity::ClientId)
        .unique("rankings_name", ["creator", "name"])
}

fn members_columns() -> TableBuilder {
    TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("member_id", DataType::String)
        .nullable_column("display_name", DataType::String)
}

fn members() -> TableBuilder {
    members_columns()
        .owner("member_id", OwnerIdentity::ClientId)
        .foreign_key("members_ranking", ["ranking_id"], "rankings")
}

fn votes_columns() -> TableBuilder {
    TableSchema::builder("votes")
        .primary_key("voter", DataType::String)
        .primary_key("ranking_id", DataType::Uuid)
        .column("candidate", DataType::String)
        .owner("voter", OwnerIdentity::ClientId)
}

fn votes() -> TableBuilder {
    votes_columns()
        .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
        .foreign_key("votes_candidate", ["ranking_id", "candidate"], "members")
        .immutable()
}

fn schema(tables: impl IntoIterator<Item = TableBuilder>) -> Schema {
    tables
        .into_iter()
        .fold(Schema::builder(), SchemaBuilder::table)
        .build()
}

fn base() -> Schema {
    schema([rankings(), members(), votes()])
}

/// `rounds(ranking_id, round_id, opened_by, title)`, a table the base does not have, with a
/// `UNIQUE` and foreign keys into two of its tables.
fn rounds(table_id: u16) -> TableSchema {
    TableSchema::builder("rounds")
        .table_id(table_id)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("round_id", DataType::Uuid)
        .column("opened_by", DataType::String)
        .column("title", DataType::String)
        .owner("opened_by", OwnerIdentity::ClientId)
        .unique("rounds_title", ["ranking_id", "title"])
        .foreign_key("rounds_ranking", ["ranking_id"], "rankings")
        .foreign_key("rounds_member", ["ranking_id", "opened_by"], "members")
        .build()
        .unwrap()
}

fn nullable(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef::new(name, data_type).nullable(true)
}

#[test]
fn a_schema_that_drops_or_changes_what_the_room_has_does_not_evolve_from_it() {
    let base = base();
    let refused: Vec<(&str, Schema)> = vec![
        ("a dropped table", schema([rankings(), members()])),
        (
            "a dropped column",
            schema([
                rankings(),
                TableSchema::builder("members")
                    .primary_key("ranking_id", DataType::Uuid)
                    .primary_key("member_id", DataType::String)
                    .owner("member_id", OwnerIdentity::ClientId)
                    .foreign_key("members_ranking", ["ranking_id"], "rankings"),
                votes(),
            ]),
        ),
        (
            "a retyped column",
            schema([
                rankings(),
                members().nullable_column("display_name", DataType::Int),
                votes(),
            ]),
        ),
    ];
    for (case, new) in refused {
        assert!(!new.extends(&base), "{case}");
        assert!(!new.evolves_from(&base), "{case}");
    }
}

#[test]
fn a_schema_that_changes_a_constraint_an_owner_or_an_immutable_table_does_not_evolve() {
    let base = base();
    let refused: Vec<(&str, Schema)> = vec![
        (
            "a dropped UNIQUE",
            schema([
                rankings_columns().owner("creator", OwnerIdentity::ClientId),
                members(),
                votes(),
            ]),
        ),
        (
            "a UNIQUE over other columns",
            schema([
                rankings_columns()
                    .owner("creator", OwnerIdentity::ClientId)
                    .unique("rankings_name", ["name"]),
                members(),
                votes(),
            ]),
        ),
        (
            "a renamed UNIQUE",
            schema([
                rankings_columns()
                    .owner("creator", OwnerIdentity::ClientId)
                    .unique("rankings_title", ["creator", "name"]),
                members(),
                votes(),
            ]),
        ),
        (
            "a dropped foreign key",
            schema([
                rankings(),
                members(),
                votes_columns()
                    .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
                    .immutable(),
            ]),
        ),
        (
            "foreign keys in another order",
            schema([
                rankings(),
                members(),
                votes_columns()
                    .foreign_key("votes_candidate", ["ranking_id", "candidate"], "members")
                    .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
                    .immutable(),
            ]),
        ),
        (
            "a foreign key over other columns",
            schema([
                rankings(),
                members(),
                votes_columns()
                    .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
                    .foreign_key("votes_candidate", ["ranking_id", "voter"], "members")
                    .immutable(),
            ]),
        ),
        (
            "another owner column",
            schema([
                rankings_columns()
                    .owner("name", OwnerIdentity::ClientId)
                    .unique("rankings_name", ["creator", "name"]),
                members(),
                votes(),
            ]),
        ),
        (
            "a dropped owner",
            schema([
                rankings_columns().unique("rankings_name", ["creator", "name"]),
                members(),
                votes(),
            ]),
        ),
        (
            "a table made mutable",
            schema([
                rankings(),
                members(),
                votes_columns()
                    .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
                    .foreign_key("votes_candidate", ["ranking_id", "candidate"], "members"),
            ]),
        ),
        (
            "a table made immutable",
            schema([rankings().immutable(), members(), votes()]),
        ),
    ];
    for (case, new) in refused {
        assert!(new.extends(&base), "{case}: only the constraints differ");
        assert!(!new.evolves_from(&base), "{case}");
    }

    // The same constraints in another order: the order decides which one a rejection names.
    let two_unique = |first: (&str, &str), second: (&str, &str)| {
        schema([
            rankings(),
            members()
                .unique(first.0, [first.1])
                .unique(second.0, [second.1]),
            votes(),
        ])
    };
    let older = two_unique(("members_a", "display_name"), ("members_b", "member_id"));
    let reordered = two_unique(("members_b", "member_id"), ("members_a", "display_name"));
    assert!(reordered.extends(&older));
    assert!(older.evolves_from(&older));
    assert!(!reordered.evolves_from(&older));

    // An owner added to a table that had none.
    let without_owner = schema([
        rankings(),
        members_columns().foreign_key("members_ranking", ["ranking_id"], "rankings"),
        votes(),
    ]);
    assert!(base.extends(&without_owner));
    assert!(!base.evolves_from(&without_owner));
}

#[test]
fn a_constraint_added_to_an_existing_table_must_include_a_column_it_did_not_have() {
    let base = base();
    let refused: Vec<(&str, Schema)> = vec![
        (
            "a UNIQUE over existing columns",
            schema([
                rankings(),
                members().unique("members_display", ["ranking_id", "display_name"]),
                votes(),
            ]),
        ),
        (
            "a UNIQUE over existing columns next to a new column",
            schema([
                rankings(),
                members()
                    .nullable_column("nick", DataType::String)
                    .unique("members_display", ["ranking_id", "display_name"]),
                votes(),
            ]),
        ),
        (
            "a foreign key over existing columns",
            schema([
                rankings(),
                members(),
                votes().foreign_key("votes_ranking", ["ranking_id"], "rankings"),
            ]),
        ),
    ];
    for (case, new) in refused {
        assert!(new.extends(&base), "{case}: only the constraints differ");
        assert!(!new.evolves_from(&base), "{case}");
    }

    // The same constraints, with a new column among theirs.
    let accepted = schema([
        rankings(),
        members()
            .nullable_column("nick", DataType::String)
            .unique("members_nick", ["ranking_id", "nick"]),
        votes()
            .nullable_column("ranking", DataType::Uuid)
            .foreign_key("votes_ranking", ["ranking"], "rankings"),
    ]);
    assert!(accepted.evolves_from(&base));
}

#[test]
fn columns_tables_and_constraints_that_include_them_evolve_from_the_schema() {
    let base = base();
    assert!(base.evolves_from(&base));
    assert!(base.evolves_from(&base.clone()));

    let mut evolved = base.clone();
    evolved
        .add_column("rankings", nullable("description", DataType::String))
        .unwrap();
    evolved
        .add_column_with_constraints(
            "members",
            nullable("nick", DataType::String),
            vec![UniqueConstraint::new(
                "members_nick",
                ["ranking_id", "nick"],
            )],
            Vec::new(),
        )
        .unwrap();
    evolved
        .add_column_with_constraints(
            "members",
            nullable("invited_by", DataType::String),
            Vec::new(),
            vec![ForeignKey::new(
                "members_inviter",
                ["ranking_id", "invited_by"],
                "members",
            )],
        )
        .unwrap();
    assert!(evolved.version() > base.version());
    assert!(evolved.evolves_from(&base));

    // A new table may bring constraints over its own columns, and foreign keys into the old
    // tables: the keys that reach `members` grow.
    let before = evolved.clone();
    evolved.insert_table(rounds(3)).unwrap();
    let members = evolved.get_table_id("members").unwrap();
    assert!(
        evolved.foreign_keys_referencing(members).count()
            > before.foreign_keys_referencing(members).count()
    );
    assert!(evolved.evolves_from(&before));
    assert!(evolved.evolves_from(&base));

    // None of them goes back.
    assert!(!base.evolves_from(&evolved));
    assert!(!before.evolves_from(&evolved));
}

/// One random step that a room's schema can take: a column without constraints, a column with
/// constraints that include it, or a new table with a foreign key into an existing one.
fn evolve(rng: &mut Rng, schema: &mut Schema, step: usize) {
    let names: Vec<String> = schema.tables().map(|t| t.name().to_string()).collect();
    let column = format!("added{step}");
    let single_key_targets: Vec<(String, DataType)> = schema
        .tables()
        .filter(|t| t.primary_key().len() == 1)
        .map(|t| {
            let key = t.get_column(&t.primary_key()[0]).unwrap();
            (t.name().to_string(), key.data_type())
        })
        .collect();
    match rng.below(3) {
        0 if !names.is_empty() => {
            let table = rng.pick(&names).clone();
            let added = schema.add_column(&table, nullable(&column, data_type(rng)));
            assert!(added.is_ok(), "{added:?}");
        }
        1 if !names.is_empty() => {
            let table = rng.pick(&names).clone();
            let mut data_type = data_type(rng);
            let mut unique = Vec::new();
            let mut foreign_keys = Vec::new();
            if rng.coin() {
                unique.push(UniqueConstraint::new(format!("u{step}"), [&column]));
            }
            if rng.coin() && !single_key_targets.is_empty() {
                let (target, key_type) = rng.pick(&single_key_targets).clone();
                data_type = key_type;
                foreign_keys.push(ForeignKey::new(format!("f{step}"), [&column], target));
            }
            let added = schema.add_column_with_constraints(
                &table,
                nullable(&column, data_type),
                unique,
                foreign_keys,
            );
            assert!(added.is_ok(), "{added:?}");
        }
        _ => {
            let table_id = (0..=u16::MAX)
                .find(|id| !schema.has_table_by_id(*id))
                .unwrap();
            let mut table = TableSchema::builder(format!("new{step}"))
                .table_id(table_id)
                .primary_key("id", DataType::Int)
                .nullable_column("label", DataType::String);
            if rng.coin() {
                table = table.unique(format!("u{step}"), ["label"]);
            }
            if !single_key_targets.is_empty() && rng.coin() {
                let (target, key_type) = rng.pick(&single_key_targets).clone();
                table = table.nullable_column("parent", key_type).foreign_key(
                    format!("f{step}"),
                    ["parent"],
                    target,
                );
            }
            if rng.coin() {
                table = table.immutable();
            }
            schema.insert_table(table.build().unwrap()).unwrap();
        }
    }
}

fn check_chains(seed: u64, cases: u64) {
    for_each_case(seed, cases, |rng| {
        let mut chain = vec![common::schema(rng)];
        for step in 0..rng.between(1, 6) {
            let mut next = chain.last().unwrap().clone();
            evolve(rng, &mut next, step);
            chain.push(next);
        }
        for (i, older) in chain.iter().enumerate() {
            assert!(older.evolves_from(older), "step {i} from itself");
            for (j, newer) in chain.iter().enumerate().skip(i + 1) {
                assert!(newer.evolves_from(older), "step {j} from step {i}");
                assert!(!older.evolves_from(newer), "step {i} from step {j}");
            }
        }
        // A room is reassigned only to a schema that extends its own with the same
        // constraints, owners and immutable tables: that schema evolves from it as well.
        for older in &chain {
            for newer in &chain {
                if newer.extends(older) && older.has_same_constraints(newer) {
                    assert!(newer.evolves_from(older));
                }
            }
        }
    });
}

#[test]
fn every_schema_of_a_chain_of_evolutions_evolves_from_all_the_earlier_ones() {
    check_chains(0x4e01, 300);
}

#[test]
#[ignore]
fn every_schema_of_a_chain_of_evolutions_evolves_from_all_the_earlier_ones_long() {
    check_chains(0x4e02, 20_000);
}
