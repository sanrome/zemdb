use axum::http::StatusCode;
use futures::StreamExt;
use reqwest::header::AUTHORIZATION;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;
use zemdb_core::*;
use zemdb_server::{
    generate_client_token, serve_until_shutdown, AppState, ClientLeaseTracker, RoomCommand,
    RoomManager, SchemaRegistry, ServerConfig, SnapshotRelay,
};

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .build()
        .unwrap();
    Schema::from_tables(vec![table])
}

fn insert_op(schema: &Schema, id: i64) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", "task").build();
    schema.to_operation_insert("tasks", &row, 1000).unwrap()
}

async fn commit(
    sender: &tokio::sync::mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    mutation: u8,
    last_ack_seq: u64,
    op: Operation,
) {
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::Commit {
            client_id: client_id.clone(),
            mutation_id: MutationId::new([mutation; 16]),
            last_ack_seq: SequenceNumber::new(last_ack_seq),
            op,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_ends_sse_streams_and_persists_rosters() {
    let dir = tempdir().unwrap();
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    let schema_registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let snapshot_relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
        )
        .unwrap(),
    );
    let room_manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&schema_registry),
        Arc::clone(&snapshot_relay),
    ));
    let state = AppState::new(
        Arc::clone(&config),
        Arc::clone(&schema_registry),
        Arc::clone(&room_manager),
        snapshot_relay,
    );

    let schema = test_schema();
    let schema_id = SchemaId::new("todo").unwrap();
    schema_registry
        .register_schema(schema_id.clone(), schema.clone())
        .unwrap();
    let room_id = RoomId::new("room-shutdown").unwrap();
    room_manager
        .create_room(room_id.clone(), schema_id, None)
        .await
        .unwrap();

    // A client whose latest cursor is only in memory when the shutdown starts.
    let client_id = ClientId::new("writer").unwrap();
    let sender = room_manager.get_or_spawn(&room_id, None).await.unwrap();
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq: None,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
    commit(&sender, &client_id, 1, 0, insert_op(&schema, 1)).await;
    commit(&sender, &client_id, 2, 1, insert_op(&schema, 2)).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let (signal_tx, signal_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(serve_until_shutdown(
        listener,
        state,
        async move {
            signal_rx.await.ok();
        },
        Duration::from_secs(10),
    ));

    // An open SSE stream would keep the server alive forever unless shutdown ends it.
    let token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(300),
        &config.auth_secret,
    );
    let sse = reqwest::Client::new()
        .get(format!("{}/rooms/{}/events", base_url, room_id))
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(sse.status(), StatusCode::OK);
    let mut events = sse.bytes_stream();

    signal_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server did not shut down while an SSE stream was open")
        .unwrap()
        .unwrap();

    let end = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(_)) = events.next().await {}
    })
    .await;
    assert!(end.is_ok(), "the SSE stream must end on shutdown");

    assert!(sender.is_closed(), "room actors must be shut down");
    let roster = dir
        .path()
        .join("rooms")
        .join(room_id.as_str())
        .join(format!("meta_clients_{}.json", room_id.as_str()));
    let persisted = ClientLeaseTracker::open_or_create(roster).unwrap();
    let entry = persisted.get_client(&client_id).unwrap();
    assert_eq!(entry.last_ack_seq, SequenceNumber::new(1));
}
