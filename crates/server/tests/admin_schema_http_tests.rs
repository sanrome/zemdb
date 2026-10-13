//! `POST /admin/schemas` keeps every table id exactly as sent and answers 400 `BadRequest`
//! (JSON) to a schema whose table ids are ambiguous, without writing anything. A schema is a
//! `tables` list whose tables carry their own ids; a map keyed by id, which could disagree with
//! those ids, is not accepted. A schema id is declared once: posting it again is 409
//! `SchemaAlreadyExists`, and schemas evolve only through `POST /admin/schemas/{id}/columns`.

mod common;

use common::{assert_json_error, TestServer};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value as Json};
use zemdb_core::{RoomId, SchemaId};
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
