use super::*;
use crate::api::router::build_router;
use crate::config::ServerConfig;
use crate::fail_point;
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;
use crate::RoomManager;
use axum::body::Body;
use axum::http::{header, Request};
use std::time::Duration;
use tempfile::tempdir;
use tower::ServiceExt;
use zemdb_core::schema::TableSchema;
use zemdb_core::value::DataType;

const ADMIN_SECRET: &str = "control_plane_admin_secret_key_123";

async fn add_column_request(app: axum::Router, column: &str) -> StatusCode {
    let body = serde_json::json!({
        "table_name": "tasks",
        "column": {"name": column, "data_type": "Int", "nullable": true, "encrypted": false}
    });
    let request = Request::post("/admin/schemas/todo/columns")
        .header(header::AUTHORIZATION, format!("Bearer {ADMIN_SECRET}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.oneshot(request).await.unwrap().status()
}

async fn room_schema(manager: &RoomManager, room_id: &RoomId) -> Arc<Schema> {
    let (_, schema) = manager
        .ask(room_id, |reply| RoomCommand::GetSchema { reply })
        .await
        .unwrap()
        .unwrap();
    schema
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_column_additions_leave_every_room_with_the_latest_schema() {
    let dir = tempdir().unwrap();
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        admin_secret: ADMIN_SECRET.to_string(),
        ..ServerConfig::default()
    });
    let registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let schema_id = SchemaId::new("todo").unwrap();
    registry
        .register_schema(
            schema_id.clone(),
            Schema::builder()
                .table(TableSchema::builder("tasks").primary_key("id", DataType::Int))
                .build(),
        )
        .unwrap();
    let relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            config.max_snapshot_bytes,
        )
        .unwrap(),
    );
    let manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&registry),
        Arc::clone(&relay),
    ));
    let rooms = [
        RoomId::new("room-a").unwrap(),
        RoomId::new("room-b").unwrap(),
    ];
    for room_id in &rooms {
        manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await
            .unwrap();
        // The actor is running, so the reloads below reach it.
        room_schema(&manager, room_id).await;
    }
    let app = build_router(AppState::new(
        config,
        Arc::clone(&registry),
        Arc::clone(&manager),
        relay,
    ));

    // The first addition is durable and about to reload the rooms when a second addition runs
    // from start to finish, reloading them with the newer schema. The first reload then
    // arrives last: it must not take the rooms back to the schema without the second column.
    let (second_done_tx, second_done_rx) = std::sync::mpsc::channel();
    let second_app = app.clone();
    let runtime = tokio::runtime::Handle::current();
    fail_point::arm_hook(
        "schema_reload_before_rooms",
        &registry.schema_path(&schema_id),
        move || {
            runtime.spawn(async move {
                let status = add_column_request(second_app, "estimate").await;
                let _ = second_done_tx.send(status);
            });
            let second = tokio::task::block_in_place(|| {
                second_done_rx.recv_timeout(Duration::from_secs(10))
            });
            assert_eq!(second, Ok(StatusCode::OK));
        },
    );

    assert_eq!(
        add_column_request(app.clone(), "priority").await,
        StatusCode::OK
    );

    for room_id in &rooms {
        let schema = room_schema(&manager, room_id).await;
        let tasks = schema.get_table_by_name("tasks").unwrap();
        assert!(tasks.get_column("priority").is_some(), "{room_id}");
        assert!(tasks.get_column("estimate").is_some(), "{room_id}");
    }
}
