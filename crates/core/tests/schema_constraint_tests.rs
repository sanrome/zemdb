//! `UNIQUE` constraints and foreign keys are part of the schema. A schema with constraints is
//! validated when it is built, deserialized or evolved, so no code path holds a constraint over
//! a missing, repeated or encrypted column, a `UNIQUE` that is just the primary key, a foreign
//! key that does not match the table it references, or two constraints with the same name.
//!
//! A constraint never judges operations sequenced before it existed: it is declared with the
//! schema, or with a new column that it includes.

use serde_json::{json, Value as Json};
use zemdb_core::schema::{MAX_CONSTRAINTS_PER_TABLE, MAX_CONSTRAINT_COLUMNS};
use zemdb_core::*;

fn s(text: &str) -> String {
    text.to_string()
}

/// `projects(id Uuid, slug String, secret encrypted, owner_ref String?)`.
fn projects() -> TableBuilder {
    TableBuilder::new("projects")
        .table_id(0)
        .primary_key("id", DataType::Uuid)
        .column("slug", DataType::String)
        .encrypted_column("secret", DataType::String)
        .nullable_column("note", DataType::String)
}

/// `tasks(project Uuid, num Int, title String, parent_num Int?)`, keyed by `(project, num)`.
fn tasks() -> TableBuilder {
    TableBuilder::new("tasks")
        .table_id(1)
        .primary_key("project", DataType::Uuid)
        .primary_key("num", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("parent_num", DataType::Int)
}

fn build(tables: impl IntoIterator<Item = TableBuilder>) -> Result<Schema, ValidationError> {
    tables
        .into_iter()
        .try_fold(Schema::builder(), SchemaBuilder::try_table)?
        .try_build()
}

// ---------------------------------------------------------------------------------------------
// Rules of a single table
// ---------------------------------------------------------------------------------------------

#[test]
fn a_constraint_over_an_unknown_column_is_rejected() {
    let unique = projects()
        .unique("projects_ghost", ["slug", "ghost"])
        .build();
    assert_eq!(
        unique.unwrap_err(),
        ValidationError::UnknownConstraintColumn {
            table: s("projects"),
            constraint: s("projects_ghost"),
            column: s("ghost"),
        }
    );
    let foreign = tasks()
        .foreign_key("tasks_ghost", ["ghost"], "projects")
        .build();
    assert_eq!(
        foreign.unwrap_err(),
        ValidationError::UnknownConstraintColumn {
            table: s("tasks"),
            constraint: s("tasks_ghost"),
            column: s("ghost"),
        }
    );
}

#[test]
fn a_constraint_that_repeats_a_column_is_rejected() {
    let unique = projects()
        .unique("projects_twice", ["slug", "slug"])
        .build();
    assert_eq!(
        unique.unwrap_err(),
        ValidationError::DuplicateConstraintColumn {
            table: s("projects"),
            constraint: s("projects_twice"),
            column: s("slug"),
        }
    );
    let foreign = tasks()
        .foreign_key("tasks_twice", ["project", "project"], "tasks")
        .build();
    assert_eq!(
        foreign.unwrap_err(),
        ValidationError::DuplicateConstraintColumn {
            table: s("tasks"),
            constraint: s("tasks_twice"),
            column: s("project"),
        }
    );
}

#[test]
fn a_constraint_over_an_encrypted_column_is_rejected() {
    // The ciphertext is random: neither a replica nor the server could compare it.
    let unique = projects().unique("projects_secret", ["secret"]).build();
    assert_eq!(
        unique.unwrap_err(),
        ValidationError::EncryptedConstraintColumn {
            table: s("projects"),
            constraint: s("projects_secret"),
            column: s("secret"),
        }
    );
    let foreign = projects()
        .foreign_key("projects_secret_ref", ["secret"], "projects")
        .build();
    assert_eq!(
        foreign.unwrap_err(),
        ValidationError::EncryptedConstraintColumn {
            table: s("projects"),
            constraint: s("projects_secret_ref"),
            column: s("secret"),
        }
    );
}

#[test]
fn a_unique_constraint_that_is_the_primary_key_is_rejected() {
    for columns in [["project", "num"], ["num", "project"]] {
        let err = tasks().unique("tasks_key", columns).build().unwrap_err();
        assert_eq!(
            err,
            ValidationError::UniqueIsPrimaryKey {
                table: s("tasks"),
                constraint: s("tasks_key"),
            },
            "{columns:?}"
        );
    }
    // Part of the key, or the key plus another column, is a real constraint.
    tasks().unique("tasks_num", ["num"]).build().unwrap();
    tasks()
        .unique("tasks_key_title", ["project", "num", "title"])
        .build()
        .unwrap();
}

#[test]
fn a_constraint_without_columns_is_rejected() {
    let empty: [&str; 0] = [];
    let unique = projects().unique("projects_none", empty).build();
    assert_eq!(
        unique.unwrap_err(),
        ValidationError::EmptyConstraint {
            table: s("projects"),
            constraint: s("projects_none"),
        }
    );
    let foreign = projects()
        .foreign_key("projects_none_ref", empty, "projects")
        .build();
    assert_eq!(
        foreign.unwrap_err(),
        ValidationError::EmptyConstraint {
            table: s("projects"),
            constraint: s("projects_none_ref"),
        }
    );
}

/// A table with `count` nullable `Int` columns `c0..` besides its key `id`.
fn wide(count: usize) -> TableBuilder {
    (0..count).fold(
        TableBuilder::new("wide")
            .table_id(0)
            .primary_key("id", DataType::Int),
        |table, i| table.nullable_column(format!("c{i}"), DataType::Int),
    )
}

#[test]
fn a_table_holds_at_most_the_maximum_number_of_constraints() {
    let with = |uniques: usize, foreign_keys: usize| {
        let table = (0..uniques).fold(wide(MAX_CONSTRAINTS_PER_TABLE + 1), |t, i| {
            t.unique(format!("u{i}"), [format!("c{i}")])
        });
        (0..foreign_keys).fold(table, |t, i| {
            t.foreign_key(format!("f{i}"), [format!("c{}", uniques + i)], "wide")
        })
    };
    // UNIQUE constraints and foreign keys count together.
    let half = MAX_CONSTRAINTS_PER_TABLE / 2;
    let at_limit = with(half, MAX_CONSTRAINTS_PER_TABLE - half);
    build([at_limit]).unwrap();

    for (uniques, foreign_keys) in [
        (MAX_CONSTRAINTS_PER_TABLE + 1, 0),
        (0, MAX_CONSTRAINTS_PER_TABLE + 1),
        (half, MAX_CONSTRAINTS_PER_TABLE + 1 - half),
    ] {
        let err = with(uniques, foreign_keys).build().unwrap_err();
        assert_eq!(
            err,
            ValidationError::TooManyConstraints {
                table: s("wide"),
                max: MAX_CONSTRAINTS_PER_TABLE,
            },
            "{uniques} + {foreign_keys}"
        );
    }
}

#[test]
fn a_constraint_holds_at_most_the_maximum_number_of_columns() {
    let names = |count: usize| (0..count).map(|i| format!("c{i}")).collect::<Vec<_>>();
    wide(MAX_CONSTRAINT_COLUMNS + 1)
        .unique("u", names(MAX_CONSTRAINT_COLUMNS))
        .build()
        .unwrap();

    let err = wide(MAX_CONSTRAINT_COLUMNS + 1)
        .unique("u", names(MAX_CONSTRAINT_COLUMNS + 1))
        .build()
        .unwrap_err();
    assert_eq!(
        err,
        ValidationError::TooManyConstraintColumns {
            table: s("wide"),
            constraint: s("u"),
            max: MAX_CONSTRAINT_COLUMNS,
        }
    );
    let err = wide(MAX_CONSTRAINT_COLUMNS + 1)
        .foreign_key("f", names(MAX_CONSTRAINT_COLUMNS + 1), "wide")
        .build()
        .unwrap_err();
    assert_eq!(
        err,
        ValidationError::TooManyConstraintColumns {
            table: s("wide"),
            constraint: s("f"),
            max: MAX_CONSTRAINT_COLUMNS,
        }
    );
}

#[test]
fn the_constraint_column_positions_are_precomputed_in_declaration_order() {
    let table = tasks()
        .unique("tasks_title", ["title", "project"])
        .unique("tasks_num", ["num"])
        .foreign_key("tasks_project", ["project"], "projects")
        .foreign_key("tasks_parent", ["project", "parent_num"], "tasks")
        .build()
        .unwrap();
    let names: Vec<&str> = table
        .unique_constraints()
        .iter()
        .map(UniqueConstraint::name)
        .collect();
    assert_eq!(names, ["tasks_title", "tasks_num"]);
    assert_eq!(table.unique_column_indices(), [vec![2, 0], vec![1]]);
    let names: Vec<&str> = table.foreign_keys().iter().map(ForeignKey::name).collect();
    assert_eq!(names, ["tasks_project", "tasks_parent"]);
    assert_eq!(table.foreign_key_column_indices(), [vec![0], vec![0, 3]]);
    assert_eq!(table.foreign_keys()[1].references(), "tasks");
    assert_eq!(table.foreign_keys()[1].on_delete(), OnDelete::Restrict);
}

// ---------------------------------------------------------------------------------------------
// Rules across tables
// ---------------------------------------------------------------------------------------------

#[test]
fn a_foreign_key_to_a_missing_table_is_rejected() {
    let expected = ValidationError::ForeignKeyTableNotFound {
        table: s("tasks"),
        constraint: s("tasks_project"),
        references: s("ghosts"),
    };
    let tasks = || tasks().foreign_key("tasks_project", ["project"], "ghosts");

    assert_eq!(build([projects(), tasks()]).unwrap_err(), expected);
    let tables = vec![
        projects().build().unwrap(),
        tasks().build().expect("checked by the schema"),
    ];
    assert_eq!(Schema::try_from_tables(tables).unwrap_err(), expected);

    let mut schema = build([projects()]).unwrap();
    let before = schema.clone();
    assert_eq!(
        schema.insert_table(tasks().build().unwrap()).unwrap_err(),
        expected
    );
    assert_eq!(schema, before, "a rejected table is not inserted");
    assert!(!schema.has_table_by_name("tasks"));
}

#[test]
fn a_foreign_key_that_does_not_match_the_referenced_key_is_rejected() {
    // `tasks` is keyed by (Uuid, Int).
    let fewer = projects().foreign_key("projects_task", ["id"], "tasks");
    assert_eq!(
        build([fewer, tasks()]).unwrap_err(),
        ValidationError::ForeignKeyArityMismatch {
            table: s("projects"),
            constraint: s("projects_task"),
            expected: 2,
            actual: 1,
        }
    );
    let more = tasks().foreign_key("tasks_project", ["project", "num"], "projects");
    assert_eq!(
        build([projects(), more]).unwrap_err(),
        ValidationError::ForeignKeyArityMismatch {
            table: s("tasks"),
            constraint: s("tasks_project"),
            expected: 1,
            actual: 2,
        }
    );
    let wrong_type = tasks().foreign_key("tasks_project", ["title"], "projects");
    assert_eq!(
        build([projects(), wrong_type]).unwrap_err(),
        ValidationError::ForeignKeyTypeMismatch {
            table: s("tasks"),
            constraint: s("tasks_project"),
            column: s("title"),
            expected: DataType::Uuid,
            actual: DataType::String,
        }
    );
    // The types are matched in order, not as a set.
    let swapped = tasks().foreign_key("tasks_self", ["num", "project"], "tasks");
    assert_eq!(
        build([swapped]).unwrap_err(),
        ValidationError::ForeignKeyTypeMismatch {
            table: s("tasks"),
            constraint: s("tasks_self"),
            column: s("num"),
            expected: DataType::Uuid,
            actual: DataType::Int,
        }
    );
}

#[test]
fn a_constraint_name_is_unique_in_the_whole_schema() {
    let expected = ValidationError::DuplicateConstraintName {
        constraint: s("dup"),
    };
    let cases = [
        // Two UNIQUE constraints of the same table.
        vec![projects().unique("dup", ["slug"]).unique("dup", ["note"])],
        // A UNIQUE constraint and a foreign key of the same table.
        vec![tasks().unique("dup", ["title"]).foreign_key(
            "dup",
            ["project", "parent_num"],
            "tasks",
        )],
        // Constraints of different tables.
        vec![
            projects().unique("dup", ["slug"]),
            tasks().foreign_key("dup", ["project"], "projects"),
        ],
    ];
    for tables in cases {
        assert_eq!(build(tables).unwrap_err(), expected);
    }

    let mut schema = build([projects().unique("dup", ["slug"])]).unwrap();
    let before = schema.clone();
    let tasks = tasks().unique("dup", ["title"]).build().unwrap();
    assert_eq!(schema.insert_table(tasks).unwrap_err(), expected);
    assert_eq!(schema, before);
}

#[test]
fn a_foreign_key_may_reference_its_own_table() {
    let schema =
        build([tasks().foreign_key("tasks_parent", ["project", "parent_num"], "tasks")]).unwrap();
    let referencing: Vec<(&str, usize)> = schema
        .foreign_keys_referencing(1)
        .map(|(table, index)| (table.name(), index))
        .collect();
    assert_eq!(referencing, [("tasks", 0)]);
}

#[test]
fn a_foreign_key_may_reference_a_table_with_a_larger_id() {
    // `projects` (id 0) references `tasks` (id 1), and `tasks` references `projects` back.
    let projects_to_tasks = || {
        projects()
            .nullable_column("first_task", DataType::Int)
            .foreign_key("projects_first_task", ["id", "first_task"], "tasks")
    };
    let tasks_to_projects = || tasks().foreign_key("tasks_project", ["project"], "projects");

    let built = build([projects_to_tasks(), tasks_to_projects()]).unwrap();
    let from_tables = Schema::try_from_tables(vec![
        projects_to_tasks().build().unwrap(),
        tasks_to_projects().build().unwrap(),
    ])
    .unwrap();
    let read: Schema = serde_json::from_str(&serde_json::to_string(&built).unwrap()).unwrap();
    assert_eq!(from_tables, built);
    assert_eq!(read, built);

    // Inserted one at a time, a table can only go in once the tables it references are there,
    // whatever their ids.
    let mut schema = Schema::new();
    let first_task = || projects_to_tasks().build().unwrap();
    assert!(matches!(
        schema.insert_table(first_task()),
        Err(ValidationError::ForeignKeyTableNotFound { .. })
    ));
    assert_eq!(schema.table_count(), 0);
    schema.insert_table(tasks().build().unwrap()).unwrap();
    schema.insert_table(first_task()).unwrap();
    let referencing: Vec<&str> = schema
        .foreign_keys_referencing(1)
        .map(|(table, index)| table.foreign_keys()[index].name())
        .collect();
    assert_eq!(referencing, ["projects_first_task"]);
}

#[test]
fn the_foreign_keys_that_reference_a_table_are_listed_by_table_id_then_declaration() {
    let schema = build([
        TableBuilder::new("c")
            .table_id(2)
            .primary_key("id", DataType::Int)
            .nullable_column("x", DataType::Int)
            .nullable_column("y", DataType::Int)
            .foreign_key("c_y", ["y"], "a")
            .foreign_key("c_x", ["x"], "a"),
        TableBuilder::new("a")
            .table_id(0)
            .primary_key("id", DataType::Int)
            .nullable_column("parent", DataType::Int)
            .foreign_key("a_parent", ["parent"], "a"),
        TableBuilder::new("b")
            .table_id(1)
            .primary_key("id", DataType::Int)
            .nullable_column("a", DataType::Int)
            .foreign_key("b_a", ["a"], "a"),
    ])
    .unwrap();
    let referencing: Vec<&str> = schema
        .foreign_keys_referencing(0)
        .map(|(table, index)| table.foreign_keys()[index].name())
        .collect();
    assert_eq!(referencing, ["a_parent", "b_a", "c_y", "c_x"]);
    assert_eq!(schema.foreign_keys_referencing(1).count(), 0);
    assert_eq!(schema.foreign_keys_referencing(7).count(), 0);
}

#[test]
fn constraints_are_part_of_the_schema_identity() {
    let plain = build([projects()]).unwrap();
    let constrained = build([projects().unique("projects_slug", ["slug"])]).unwrap();
    let renamed = build([projects().unique("projects_slug_2", ["slug"])]).unwrap();
    assert_ne!(plain, constrained);
    assert_ne!(constrained, renamed);
    assert_eq!(
        constrained,
        build([projects().unique("projects_slug", ["slug"])]).unwrap()
    );
}

// ---------------------------------------------------------------------------------------------
// Evolution
// ---------------------------------------------------------------------------------------------

fn evolving() -> Schema {
    build([
        projects().unique("projects_slug", ["slug"]),
        tasks().foreign_key("tasks_project", ["project"], "projects"),
    ])
    .unwrap()
}

#[test]
fn a_new_column_with_unique_and_foreign_keys_bumps_the_version_once() {
    let mut schema = evolving();
    let index = schema
        .add_column_with_constraints(
            "tasks",
            ColumnDef::new("code", DataType::String).nullable(true),
            vec![
                UniqueConstraint::new("tasks_code", ["code"]),
                UniqueConstraint::new("tasks_project_code", ["project", "code"]),
            ],
            vec![],
        )
        .unwrap();
    assert_eq!(index, 4);
    assert_eq!(schema.version(), 2);
    let tasks = schema.get_table_by_name("tasks").unwrap();
    let names: Vec<&str> = tasks
        .unique_constraints()
        .iter()
        .map(UniqueConstraint::name)
        .collect();
    assert_eq!(names, ["tasks_code", "tasks_project_code"]);
    assert_eq!(tasks.unique_column_indices(), [vec![4], vec![0, 4]]);

    let index = schema
        .add_column_with_constraints(
            "tasks",
            ColumnDef::new("blocked_by", DataType::Int).nullable(true),
            vec![],
            vec![ForeignKey::new(
                "tasks_blocked_by",
                ["project", "blocked_by"],
                "tasks",
            )],
        )
        .unwrap();
    assert_eq!(index, 5);
    assert_eq!(schema.version(), 3);
    let tasks = schema.get_table_by_name("tasks").unwrap();
    assert_eq!(tasks.foreign_keys().len(), 2);
    assert_eq!(tasks.foreign_key_column_indices(), [vec![0], vec![0, 5]]);
    let referencing: Vec<&str> = schema
        .foreign_keys_referencing(1)
        .map(|(table, index)| table.foreign_keys()[index].name())
        .collect();
    assert_eq!(referencing, ["tasks_blocked_by"]);

    // Both kinds in one call, on a table that already has a UNIQUE constraint: the version
    // goes up once, and the new constraints come after the existing ones.
    let index = schema
        .add_column_with_constraints(
            "projects",
            ColumnDef::new("parent", DataType::Uuid).nullable(true),
            vec![UniqueConstraint::new(
                "projects_parent_slug",
                ["parent", "slug"],
            )],
            vec![ForeignKey::new("projects_parent", ["parent"], "projects")],
        )
        .unwrap();
    assert_eq!(index, 4);
    assert_eq!(schema.version(), 4);
    let projects = schema.get_table_by_name("projects").unwrap();
    let names: Vec<&str> = projects
        .unique_constraints()
        .iter()
        .map(UniqueConstraint::name)
        .collect();
    assert_eq!(names, ["projects_slug", "projects_parent_slug"]);
    assert_eq!(projects.unique_column_indices(), [vec![1], vec![4, 1]]);
    assert_eq!(projects.foreign_key_column_indices(), [vec![4]]);
    let referencing: Vec<&str> = schema
        .foreign_keys_referencing(0)
        .map(|(table, index)| table.foreign_keys()[index].name())
        .collect();
    assert_eq!(referencing, ["projects_parent", "tasks_project"]);

    // The evolved schema reads back as itself.
    let read: Schema = serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(read, schema);
}

#[test]
fn a_constraint_added_with_a_column_must_include_it() {
    let column = || ColumnDef::new("code", DataType::String).nullable(true);
    let cases: Vec<(Vec<UniqueConstraint>, Vec<ForeignKey>, &str)> = vec![
        (
            vec![UniqueConstraint::new("tasks_title", ["title"])],
            vec![],
            "tasks_title",
        ),
        (
            vec![
                UniqueConstraint::new("tasks_code", ["code"]),
                UniqueConstraint::new("tasks_title", ["title", "project"]),
            ],
            vec![],
            "tasks_title",
        ),
        (
            vec![UniqueConstraint::new("tasks_code", ["code"])],
            vec![ForeignKey::new(
                "tasks_parent",
                ["project", "parent_num"],
                "tasks",
            )],
            "tasks_parent",
        ),
    ];
    for (unique, foreign_keys, constraint) in cases {
        let mut schema = evolving();
        let before = schema.clone();
        let err = schema
            .add_column_with_constraints("tasks", column(), unique, foreign_keys)
            .unwrap_err();
        assert_eq!(
            err,
            ValidationError::ConstraintOmitsNewColumn {
                table: s("tasks"),
                constraint: s(constraint),
                column: s("code"),
            }
        );
        assert_eq!(schema, before, "nothing changes");
        assert_eq!(schema.version(), 1);
        assert!(schema
            .get_table_by_name("tasks")
            .unwrap()
            .get_column("code")
            .is_none());
    }
}

#[test]
fn a_rejected_column_with_constraints_changes_nothing() {
    let column = || ColumnDef::new("code", DataType::String).nullable(true);
    let cases: Vec<(
        ColumnDef,
        Vec<UniqueConstraint>,
        Vec<ForeignKey>,
        ValidationError,
    )> = vec![
        (
            column(),
            vec![UniqueConstraint::new("projects_slug", ["code"])],
            vec![],
            ValidationError::DuplicateConstraintName {
                constraint: s("projects_slug"),
            },
        ),
        (
            column(),
            vec![],
            vec![ForeignKey::new("tasks_code", ["code"], "ghosts")],
            ValidationError::ForeignKeyTableNotFound {
                table: s("tasks"),
                constraint: s("tasks_code"),
                references: s("ghosts"),
            },
        ),
        (
            column(),
            vec![],
            vec![ForeignKey::new("tasks_code", ["code"], "projects")],
            ValidationError::ForeignKeyTypeMismatch {
                table: s("tasks"),
                constraint: s("tasks_code"),
                column: s("code"),
                expected: DataType::Uuid,
                actual: DataType::String,
            },
        ),
        (
            column().encrypted(true),
            vec![UniqueConstraint::new("tasks_code", ["code"])],
            vec![],
            ValidationError::EncryptedConstraintColumn {
                table: s("tasks"),
                constraint: s("tasks_code"),
                column: s("code"),
            },
        ),
        (
            ColumnDef::new("code", DataType::String),
            vec![UniqueConstraint::new("tasks_code", ["code"])],
            vec![],
            ValidationError::AddedColumnMustBeNullable {
                table: s("tasks"),
                column: s("code"),
            },
        ),
    ];
    for (column, unique, foreign_keys, expected) in cases {
        let mut schema = evolving();
        let before = schema.clone();
        let err = schema
            .add_column_with_constraints("tasks", column, unique, foreign_keys)
            .unwrap_err();
        assert_eq!(err, expected);
        assert_eq!(schema, before, "nothing changes");
    }
}

#[test]
fn the_constraint_limit_counts_the_existing_constraints() {
    let full = (0..MAX_CONSTRAINTS_PER_TABLE).fold(wide(MAX_CONSTRAINTS_PER_TABLE), |t, i| {
        t.unique(format!("u{i}"), [format!("c{i}")])
    });
    let mut schema = build([full]).unwrap();
    let before = schema.clone();
    let err = schema
        .add_column_with_constraints(
            "wide",
            ColumnDef::new("extra", DataType::Int).nullable(true),
            vec![UniqueConstraint::new("u_extra", ["extra"])],
            vec![],
        )
        .unwrap_err();
    assert_eq!(
        err,
        ValidationError::TooManyConstraints {
            table: s("wide"),
            max: MAX_CONSTRAINTS_PER_TABLE,
        }
    );
    assert_eq!(schema, before);
    // A column alone is still accepted, and keeps the constraints.
    schema
        .add_column(
            "wide",
            ColumnDef::new("extra", DataType::Int).nullable(true),
        )
        .unwrap();
    assert_eq!(
        schema
            .get_table_by_name("wide")
            .unwrap()
            .unique_constraints()
            .len(),
        MAX_CONSTRAINTS_PER_TABLE
    );
}

// ---------------------------------------------------------------------------------------------
// Format
// ---------------------------------------------------------------------------------------------

fn column_json(name: &str, data_type: &str, nullable: bool) -> Json {
    json!({"name": name, "data_type": data_type, "nullable": nullable, "encrypted": false})
}

fn votes_json() -> Json {
    json!({
        "version": 1,
        "tables": [
            {
                "table_id": 0, "name": "polls", "primary_key": ["id"],
                "columns": [column_json("id", "Uuid", false), column_json("slug", "String", false)],
                "unique": [{"name": "polls_slug", "columns": ["slug"]}],
                "foreign_keys": [],
                "owner": null,
                "immutable": false
            },
            {
                "table_id": 1, "name": "votes", "primary_key": ["voter", "poll_id"],
                "columns": [
                    column_json("voter", "String", false),
                    column_json("poll_id", "Uuid", false),
                    column_json("choice", "Int", false)
                ],
                "unique": [],
                "foreign_keys": [
                    {"name": "votes_poll", "columns": ["poll_id"], "references": "polls", "on_delete": "Restrict"}
                ],
                "owner": {"column": "voter", "identity": "ClientId"},
                "immutable": true
            }
        ]
    })
}

#[test]
fn the_documented_json_format_reads_and_writes_back_the_same() {
    let schema: Schema = serde_json::from_value(votes_json()).unwrap();
    let polls = schema.get_table_by_name("polls").unwrap();
    assert_eq!(polls.unique_constraints()[0].columns(), ["slug"]);
    let referencing: Vec<&str> = schema
        .foreign_keys_referencing(0)
        .map(|(table, index)| table.foreign_keys()[index].name())
        .collect();
    assert_eq!(referencing, ["votes_poll"]);
    assert_eq!(serde_json::to_value(&schema).unwrap(), votes_json());
}

#[test]
fn a_schema_without_constraint_fields_still_reads() {
    // A schema written before constraints existed (`schemas/{id}.json`, admin JSON).
    let old = json!({
        "version": 3,
        "tables": [{
            "table_id": 4, "name": "tasks", "primary_key": ["id"],
            "columns": [column_json("id", "Int", false), column_json("title", "String", true)]
        }]
    });
    let schema: Schema = serde_json::from_value(old).unwrap();
    let tasks = schema.get_table_by_id(4).unwrap();
    assert!(tasks.unique_constraints().is_empty());
    assert!(tasks.foreign_keys().is_empty());
    assert_eq!(schema.version(), 3);
    // Written again, it always carries both lists.
    let written = serde_json::to_value(&schema).unwrap();
    assert_eq!(written["tables"][0]["unique"], json!([]));
    assert_eq!(written["tables"][0]["foreign_keys"], json!([]));
}

#[test]
fn a_schema_with_constraints_round_trips_in_binary() {
    let schema = evolving();
    let frame = encode_message(&schema).unwrap();
    let back: Schema = decode_message(&frame).unwrap();
    assert_eq!(back, schema);
    assert_eq!(
        back.foreign_keys_referencing(0).count(),
        1,
        "the derived lists are rebuilt"
    );
}

#[test]
fn a_constraint_that_breaks_a_rule_is_rejected_when_read() {
    let cases: Vec<(&str, Json, &str)> = vec![
        (
            "/tables/0/unique",
            json!([{"name": "polls_ghost", "columns": ["ghost"]}]),
            "unknown column 'ghost'",
        ),
        (
            "/tables/1/foreign_keys",
            json!([{"name": "votes_poll", "columns": ["poll_id"], "references": "ghosts", "on_delete": "Restrict"}]),
            "does not exist in schema",
        ),
        (
            "/tables/1/foreign_keys",
            json!([{"name": "polls_slug", "columns": ["poll_id"], "references": "polls", "on_delete": "Restrict"}]),
            "used more than once",
        ),
        (
            "/tables/1/foreign_keys",
            json!([{"name": "votes_poll", "columns": ["poll_id"], "references": "polls", "on_delete": "Cascade"}]),
            "unknown variant",
        ),
        (
            "/tables/1/foreign_keys",
            json!([{"name": "votes_poll", "columns": ["poll_id"], "references": "polls"}]),
            "missing field `on_delete`",
        ),
    ];
    for (pointer, value, expected) in cases {
        let mut schema = votes_json();
        *schema.pointer_mut(pointer).unwrap() = value;
        let err = serde_json::from_value::<Schema>(schema).unwrap_err();
        assert!(err.to_string().contains(expected), "{pointer}: {err}");
    }
}

#[test]
fn an_unknown_field_in_a_column_or_a_constraint_is_rejected() {
    // A `"unique": true` inside a column would otherwise be ignored without notice.
    let cases = [
        ("/tables/0/columns/1/unique", json!(true)),
        ("/tables/0/unique/0/where", json!("slug <> ''")),
        ("/tables/1/foreign_keys/0/on_update", json!("Restrict")),
    ];
    for (pointer, value) in cases {
        let mut schema = votes_json();
        let (parent, field) = pointer.rsplit_once('/').unwrap();
        schema.pointer_mut(parent).unwrap()[field] = value;
        let err = serde_json::from_value::<Schema>(schema).unwrap_err();
        assert!(
            err.to_string().contains("unknown field"),
            "{pointer}: {err}"
        );
    }
    let column = json!({"name": "a", "data_type": "Int", "nullable": true, "encrypted": false, "unique": true});
    let err = serde_json::from_value::<ColumnDef>(column).unwrap_err();
    assert!(err.to_string().contains("unknown field"), "{err}");
}
