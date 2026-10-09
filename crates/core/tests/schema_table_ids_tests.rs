//! Table ids are part of the persisted data: operations in the WAL, snapshots and client
//! outboxes refer to tables by id. Reading a schema (admin JSON, schema files, protocol
//! messages) must therefore keep every id exactly as written, and reject a schema whose ids are
//! ambiguous instead of renumbering its tables.

use serde::Serialize;
use serde_json::{json, Value as Json};
use zemdb_core::*;

fn table_json(table_id: Option<u16>, name: &str) -> Json {
    let mut table = json!({
        "name": name,
        "primary_key": ["id"],
        "columns": [{"name": "id", "data_type": "Int", "nullable": false, "encrypted": false}]
    });
    if let Some(id) = table_id {
        table["table_id"] = json!(id);
    }
    table
}

fn from_json(schema: Json) -> Result<Schema, serde_json::Error> {
    serde_json::from_value(schema)
}

#[test]
fn table_zero_listed_after_another_table_keeps_its_id() {
    let schema = from_json(json!({
        "tables": [table_json(Some(1), "projects"), table_json(Some(0), "tasks")]
    }))
    .expect("distinct explicit ids are a valid schema");

    assert_eq!(schema.get_table_id("projects"), Some(1));
    assert_eq!(schema.get_table_id("tasks"), Some(0));
    assert_eq!(schema.get_table_by_id(0).unwrap().name(), "tasks");
}

#[test]
fn table_map_keyed_by_id_is_rejected() {
    // A schema is a list of tables that carry their own ids. A map keyed by id could disagree
    // with the ids inside it (or repeat a key), so it is not an accepted form.
    let res = from_json(json!({
        "tables_by_id": {"7": table_json(Some(2), "tasks")}
    }));
    let err = res.expect_err("only the `tables` list is accepted");
    assert!(err.to_string().contains("tables_by_id"), "{err}");

    let res = from_json(json!({
        "tables_by_id": {"1": table_json(Some(1), "projects"), "5": table_json(Some(0), "tasks")},
        "tables": []
    }));
    assert!(res.is_err(), "an unknown field must not be ignored");
}

#[test]
fn duplicate_table_ids_are_rejected() {
    let res = from_json(json!({
        "tables": [table_json(Some(3), "projects"), table_json(Some(3), "tasks")]
    }));
    let err = res.expect_err("two tables with id 3");
    assert!(err.to_string().contains("Table id 3"), "{err}");
}

#[test]
fn duplicate_table_names_are_rejected() {
    let res = from_json(json!({
        "tables": [table_json(Some(0), "tasks"), table_json(Some(1), "tasks")]
    }));
    assert!(res.unwrap_err().to_string().contains("already exists"));
}

#[test]
fn table_without_an_id_is_rejected_when_reading() {
    let res = from_json(json!({
        "tables": [table_json(Some(0), "projects"), table_json(None, "tasks")]
    }));
    let err = res.expect_err("reading a schema never assigns ids");
    assert!(err.to_string().contains("table_id"), "{err}");
}

#[test]
fn binary_schema_with_duplicate_table_ids_is_rejected() {
    #[derive(Serialize)]
    struct RawSchema {
        tables: Vec<TableSchema>,
    }
    let projects: TableSchema = serde_json::from_value(table_json(Some(2), "projects")).unwrap();
    let tasks: TableSchema = serde_json::from_value(table_json(Some(2), "tasks")).unwrap();
    let bytes = bincode::serialize(&RawSchema {
        tables: vec![projects, tasks],
    })
    .unwrap();

    assert!(bincode::deserialize::<Schema>(&bytes).is_err());
}

#[test]
fn valid_schema_round_trips_in_json_and_binary() {
    let schema = from_json(json!({
        "tables": [table_json(Some(4), "projects"), table_json(Some(0), "tasks")]
    }))
    .unwrap();

    let json = serde_json::to_value(&schema).unwrap();
    assert_eq!(
        json,
        json!({"tables": [table_json(Some(0), "tasks"), table_json(Some(4), "projects")]}),
        "tables are written as a list in ascending id order"
    );
    let from_json: Schema = serde_json::from_value(json).unwrap();
    assert_eq!(from_json, schema);

    let bytes = bincode::serialize(&schema).unwrap();
    let from_binary: Schema = bincode::deserialize(&bytes).unwrap();
    assert_eq!(from_binary, schema);
    assert_eq!(from_binary.get_table_id("projects"), Some(4));
}

fn table(name: &str) -> TableBuilder {
    TableSchema::builder(name).primary_key("id", DataType::Int)
}

#[test]
fn schema_builder_assigns_ids_only_to_tables_without_one() {
    let schema = Schema::builder()
        .table(table("first"))
        .table(table("explicit").table_id(10))
        .table(table("after_explicit"))
        .build();

    assert_eq!(schema.get_table_id("first"), Some(0));
    assert_eq!(schema.get_table_id("explicit"), Some(10));
    assert_eq!(schema.get_table_id("after_explicit"), Some(11));
    assert_eq!(schema.table_ids().collect::<Vec<_>>(), vec![0, 10, 11]);
}

#[test]
fn schema_builder_keeps_explicit_table_zero_after_other_tables() {
    let schema = Schema::builder()
        .table(table("projects").table_id(1))
        .table(table("tasks").table_id(0))
        .build();

    assert_eq!(schema.get_table_id("tasks"), Some(0));
    assert_eq!(schema.get_table_id("projects"), Some(1));
}

#[test]
fn schema_builder_rejects_an_explicit_id_already_taken() {
    let err = Schema::builder()
        .table(table("projects"))
        .try_table(table("tasks").table_id(0))
        .unwrap_err();
    assert_eq!(
        err,
        ValidationError::DuplicateTableId {
            table: "tasks".to_string(),
            table_id: 0,
        }
    );
}

#[test]
fn try_from_tables_rejects_repeated_ids_instead_of_renumbering() {
    let projects = table("projects").table_id(3).build().unwrap();
    let tasks = table("tasks").table_id(3).build().unwrap();

    let err = Schema::try_from_tables(vec![projects, tasks]).unwrap_err();
    assert_eq!(
        err,
        ValidationError::DuplicateTableId {
            table: "tasks".to_string(),
            table_id: 3,
        }
    );
}

#[test]
fn table_builder_without_an_id_cannot_build_a_standalone_table() {
    assert_eq!(
        table("tasks").build().unwrap_err(),
        ValidationError::MissingTableId {
            table: "tasks".to_string()
        }
    );
}

#[test]
fn schema_add_column_evolves_the_table_and_keeps_the_indexes() {
    let mut schema = Schema::builder()
        .table(table("projects"))
        .table(table("tasks"))
        .build();

    let idx = schema
        .add_column(
            "tasks",
            ColumnDef::new("done", DataType::Bool).nullable(true),
        )
        .unwrap();
    assert_eq!(idx, 1);
    let tasks = schema.get_table_by_id(1).unwrap();
    assert_eq!(tasks.name(), "tasks");
    assert!(tasks.get_column("done").is_some());
    assert!(schema
        .get_table_by_name("tasks")
        .unwrap()
        .get_column("done")
        .is_some());

    assert_eq!(
        schema.add_column("missing", ColumnDef::new("x", DataType::Int).nullable(true)),
        Err(ValidationError::TableNotFound("missing".to_string()))
    );
    assert_eq!(
        schema.add_column("tasks", ColumnDef::new("required", DataType::Int)),
        Err(ValidationError::AddedColumnMustBeNullable {
            table: "tasks".to_string(),
            column: "required".to_string(),
        })
    );
}
