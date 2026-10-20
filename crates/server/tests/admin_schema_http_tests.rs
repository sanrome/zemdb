//! `POST /admin/schemas` keeps every table id exactly as sent and answers 400 `BadRequest`
//! (JSON) to a schema whose table ids are ambiguous, without writing anything. A schema is a
//! `tables` list whose tables carry their own ids; a map keyed by id, which could disagree with
//! those ids, is not accepted. A schema id is declared once: posting it again is 409
//! `SchemaAlreadyExists`, and schemas evolve only through `POST /admin/schemas/{id}/columns`.
//!
//! A schema may declare constraints, owners and immutable tables when it is created, and a
//! new column may come with the constraints that include it. Both bodies reject unknown
//! fields, so nothing meant as a constraint is silently dropped.

mod common;

use common::{assert_json_error, TestServer};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value as Json};
use zemdb_core::protocol::encoded_len;
use zemdb_core::*;
use zemdb_server::actor::command::RoomCommand;

async fn start_server() -> TestServer {
    TestServer::start(|_| {}).await
}

fn table_json(table_id: u16, name: &str) -> Json {
    json!({
        "table_id": table_id,
        "name": name,
        "primary_key": ["id"],
        "columns": [{"name": "id", "data_type": "Int", "nullable": false, "encrypted": false}]
    })
}

async fn post_schema(server: &TestServer, schema_id: &str, schema: Json) -> reqwest::Response {
    server
        .admin(Method::POST, "/admin/schemas")
        .json(&json!({"schema_id": schema_id, "schema": schema}))
        .send()
        .await
        .unwrap()
}

async fn assert_rejected(server: &TestServer, schema_id: &str, schema: Json) {
    let resp = post_schema(server, schema_id, schema).await;
    assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    assert!(
        !server
            .data_dir
            .join("schemas")
            .join(format!("{schema_id}.json"))
            .exists(),
        "a rejected schema must not be written"
    );
}

#[tokio::test]
async fn table_zero_listed_after_another_table_keeps_its_id() {
    let server = start_server().await;

    let resp = post_schema(
        &server,
        "ordered",
        json!({"tables": [table_json(1, "projects"), table_json(0, "tasks")]}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = server
        .admin(Method::GET, "/admin/schemas/ordered")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["tables"][0]["table_id"], 0, "{body}");
    assert_eq!(body["tables"][0]["name"], "tasks", "{body}");
    assert_eq!(body["tables"][1]["table_id"], 1, "{body}");
    assert_eq!(body["tables"][1]["name"], "projects", "{body}");
}

#[tokio::test]
async fn table_map_keyed_by_id_is_bad_request() {
    let server = start_server().await;
    assert_rejected(
        &server,
        "mismatch",
        json!({"tables_by_id": {"7": table_json(2, "tasks")}}),
    )
    .await;
    assert_rejected(
        &server,
        "zero-moved",
        json!({"tables_by_id": {"1": table_json(1, "projects"), "5": table_json(0, "tasks")}}),
    )
    .await;
}

#[tokio::test]
async fn duplicate_table_ids_are_bad_request() {
    let server = start_server().await;
    assert_rejected(
        &server,
        "duplicate-ids",
        json!({"tables": [table_json(3, "projects"), table_json(3, "tasks")]}),
    )
    .await;
}

#[tokio::test]
async fn duplicate_table_names_are_bad_request() {
    let server = start_server().await;
    assert_rejected(
        &server,
        "duplicate-names",
        json!({"tables": [table_json(0, "tasks"), table_json(1, "tasks")]}),
    )
    .await;
}

#[tokio::test]
async fn table_without_an_id_is_bad_request() {
    let server = start_server().await;
    let mut table = table_json(0, "tasks");
    table.as_object_mut().unwrap().remove("table_id");
    assert_rejected(&server, "missing-id", json!({"tables": [table]})).await;
}

#[tokio::test]
async fn posting_an_existing_schema_id_again_is_a_conflict_and_changes_nothing() {
    let server = start_server().await;
    let original = json!({"tables": [table_json(0, "tasks"), table_json(1, "projects")]});
    let resp = post_schema(&server, "todo", original).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .json(&json!({"room_id": "room-a", "schema_id": "todo"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let room_id = RoomId::new("room-a").unwrap();
    let room_schema = || async {
        server
            .room_manager
            .ask(&room_id, |reply| RoomCommand::GetSchema { reply })
            .await
            .unwrap()
            .unwrap()
    };
    let (_, before) = room_schema().await;

    // The same tables with their ids swapped: rows of `tasks` already in the log would be read
    // as `projects`.
    let reordered = json!({"tables": [table_json(0, "projects"), table_json(1, "tasks")]});
    let resp = post_schema(&server, "todo", reordered).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["code"], "SchemaAlreadyExists", "{body}");

    let resp = server
        .admin(Method::GET, "/admin/schemas/todo")
        .send()
        .await
        .unwrap();
    let stored: Json = resp.json().await.unwrap();
    assert_eq!(stored["tables"][0]["name"], "tasks", "{stored}");
    assert_eq!(stored["tables"][1]["name"], "projects", "{stored}");

    let (schema_id, after) = room_schema().await;
    assert_eq!(schema_id, SchemaId::new("todo").unwrap());
    assert_eq!(after, before);
    assert_eq!(after.get_table_id("tasks"), Some(0));
}

fn column_json(name: &str, data_type: &str, nullable: bool) -> Json {
    json!({"name": name, "data_type": data_type, "nullable": nullable, "encrypted": false})
}

/// `polls` with `UNIQUE(slug)`, and `votes` owned by `voter`, immutable, with a foreign key to
/// `polls`.
fn votes_schema() -> Json {
    json!({
        "version": 1,
        "tables": [
            {
                "table_id": 0, "name": "polls", "primary_key": ["id"],
                "columns": [column_json("id", "Uuid", false), column_json("slug", "String", false)],
                "unique": [{"name": "polls_slug", "columns": ["slug"]}],
                "foreign_keys": []
            },
            {
                "table_id": 1, "name": "votes", "primary_key": ["voter", "poll_id"],
                "columns": [
                    column_json("voter", "String", false),
                    column_json("poll_id", "Uuid", false),
                    column_json("choice", "Int", false)
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

async fn get_schema(server: &TestServer, schema_id: &str) -> Json {
    let resp = server
        .admin(Method::GET, &format!("/admin/schemas/{schema_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

async fn add_column(server: &TestServer, schema_id: &str, body: Json) -> reqwest::Response {
    server
        .admin(Method::POST, &format!("/admin/schemas/{schema_id}/columns"))
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_schema_with_constraints_an_owner_and_an_immutable_table_is_created_and_read_back() {
    let server = start_server().await;
    let resp = post_schema(&server, "votes", votes_schema()).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let stored = get_schema(&server, "votes").await;
    let expected = votes_schema();
    let tables = ["unique", "foreign_keys", "owner", "immutable"];
    for (table, field) in [0, 1].into_iter().flat_map(|t| tables.map(|f| (t, f))) {
        let want = expected["tables"][table]
            .get(field)
            .cloned()
            .unwrap_or_else(|| {
                if field == "immutable" {
                    json!(false)
                } else {
                    Json::Null
                }
            });
        assert_eq!(
            stored["tables"][table][field], want,
            "{table} {field}: {stored}"
        );
    }
}

#[tokio::test]
async fn an_invalid_constraint_or_owner_is_bad_request_and_writes_nothing() {
    let server = start_server().await;
    let cases = [
        (
            "/tables/1/owner",
            json!({"column": "ghost", "identity": "ClientId"}),
        ),
        (
            "/tables/1/owner",
            json!({"column": "choice", "identity": "ClientId"}),
        ),
        (
            "/tables/1/owner",
            json!({"column": "voter", "identity": "Pseudonym"}),
        ),
        (
            "/tables/1/owner",
            json!({"column": "voter", "identity": "ClientId", "extra": 1}),
        ),
        (
            "/tables/0/unique",
            json!([{"name": "polls_ghost", "columns": ["ghost"]}]),
        ),
        ("/tables/1/foreign_keys/0/references", json!("ghosts")),
    ];
    for (i, (pointer, value)) in cases.into_iter().enumerate() {
        let mut schema = votes_schema();
        *schema.pointer_mut(pointer).unwrap() = value;
        assert_rejected(&server, &format!("invalid-{i}"), schema).await;
    }
    // A nullable owner column declares no owner.
    let mut schema = votes_schema();
    schema["tables"][1]["primary_key"] = json!(["poll_id"]);
    schema["tables"][1]["columns"][0]["nullable"] = json!(true);
    assert_rejected(&server, "nullable-owner", schema).await;
}

#[tokio::test]
async fn a_column_added_with_constraints_that_include_it_reaches_the_open_rooms() {
    let server = start_server().await;
    assert_eq!(
        post_schema(&server, "votes", votes_schema()).await.status(),
        StatusCode::CREATED
    );
    let resp = server
        .admin(Method::POST, "/admin/rooms")
        .json(&json!({"room_id": "open-room", "schema_id": "votes"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let room_id = RoomId::new("open-room").unwrap();

    let resp = add_column(
        &server,
        "votes",
        json!({
            "table_name": "polls",
            "column": column_json("code", "String", true),
            "unique": [{"name": "polls_code", "columns": ["code", "slug"]}],
            "foreign_keys": [{"name": "polls_parent", "columns": ["code"], "references": "votes_ref", "on_delete": "Restrict"}]
        }),
    )
    .await;
    // The foreign key references a table that does not exist: nothing changes.
    assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    assert_eq!(get_schema(&server, "votes").await["version"], 1);

    let resp = add_column(
        &server,
        "votes",
        json!({
            "table_name": "polls",
            "column": column_json("code", "String", true),
            "unique": [{"name": "polls_code", "columns": ["code", "slug"]}]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let evolved: Schema = resp.json().await.unwrap();
    assert_eq!(evolved.version(), 2);
    let polls = evolved.get_table_by_name("polls").unwrap();
    assert_eq!(polls.unique_constraints().len(), 2);
    assert_eq!(polls.unique_constraints()[1].columns(), ["code", "slug"]);

    let (_, in_room) = server
        .room_manager
        .ask(&room_id, |reply| RoomCommand::GetSchema { reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*in_room, evolved);
    assert_eq!(
        serde_json::from_value::<Schema>(get_schema(&server, "votes").await).unwrap(),
        evolved
    );
}

#[tokio::test]
async fn a_constraint_that_omits_the_new_column_is_bad_request_and_changes_nothing() {
    let server = start_server().await;
    assert_eq!(
        post_schema(&server, "votes", votes_schema()).await.status(),
        StatusCode::CREATED
    );
    let before = get_schema(&server, "votes").await;
    let resp = add_column(
        &server,
        "votes",
        json!({
            "table_name": "polls",
            "column": column_json("code", "String", true),
            "unique": [{"name": "polls_code", "columns": ["slug"]}]
        }),
    )
    .await;
    assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    assert_eq!(get_schema(&server, "votes").await, before);
}

#[tokio::test]
async fn an_unknown_field_in_an_admin_schema_body_is_bad_request() {
    let server = start_server().await;
    let resp = server
        .admin(Method::POST, "/admin/schemas")
        .json(&json!({"schema_id": "extra", "schema": votes_schema(), "replace": true}))
        .send()
        .await
        .unwrap();
    assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    assert!(!server.data_dir.join("schemas").join("extra.json").exists());

    assert_eq!(
        post_schema(&server, "votes", votes_schema()).await.status(),
        StatusCode::CREATED
    );
    let before = get_schema(&server, "votes").await;
    // A constraint under a name the endpoint does not know would otherwise be dropped.
    for field in ["constraints", "uniques", "owner", "immutable"] {
        let mut body = json!({
            "table_name": "polls",
            "column": column_json("code", "String", true)
        });
        body[field] = json!([{"name": "polls_code", "columns": ["code"]}]);
        let resp = add_column(&server, "votes", body).await;
        assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest").await;
    }
    assert_eq!(get_schema(&server, "votes").await, before);
}

/// `wide_{i}`: a key and 16 more columns, with as many constraints as a table may have, each
/// over 16 columns.
fn widest_table(table_id: u16) -> TableBuilder {
    let columns: Vec<String> = (0..16).map(|c| format!("column_{c}")).collect();
    let mut table = TableSchema::builder(format!("wide_{table_id}"))
        .table_id(table_id)
        .primary_key("id", DataType::Int);
    for column in &columns {
        table = table.column(column.as_str(), DataType::String);
    }
    for c in 0..zemdb_core::schema::MAX_CONSTRAINTS_PER_TABLE {
        let mut over = columns.clone();
        over.rotate_left(c % columns.len());
        table = table.unique(format!("wide_{table_id}_unique_{c}"), over);
    }
    table
}

#[tokio::test]
async fn a_schema_at_the_constraint_limits_reaches_clients_in_one_frame() {
    // A schema reaches clients whole in `Registered` and `Schema` frames, which the budget of
    // responses in flight reserves one frame for. Its wire encoding is smaller than the JSON
    // the admin API accepted it in, itself within one frame.
    let server = start_server().await;
    let schema = (0..40)
        .fold(Schema::builder(), |builder, id| {
            builder.table(widest_table(id))
        })
        .build();
    let json = serde_json::to_vec(&schema).unwrap();
    assert!(encoded_len(&schema).unwrap() < json.len() as u64);
    let resp = post_schema(&server, "wide", serde_json::from_slice(&json).unwrap()).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let room_id = server
        .create_room("wide-room", &SchemaId::new("wide").unwrap())
        .await;

    let alice = ClientId::new("alice").unwrap();
    let (token, registered) = server.register(&room_id, &alice).await;
    let ServerMessage::Registered { schema: sent, .. } = registered else {
        panic!("expected Registered, got {registered:?}");
    };
    assert_eq!(sent, schema);
    let request = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
    };
    let (status, reply) = server
        .post_message(&room_id, "schema", &token, &request)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        matches!(&reply, ServerMessage::Schema { schema: sent, .. } if *sent == schema),
        "{reply:?}"
    );
}
