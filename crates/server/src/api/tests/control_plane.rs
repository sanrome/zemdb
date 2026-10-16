use super::*;
use crate::api::router::build_router;
use crate::api::test_support::TestState;
use crate::fail_point;
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
    let state = TestState::new(dir.path(), |config| {
        config.admin_secret = ADMIN_SECRET.to_string();
    });
    let (registry, manager) = (&state.registry, &state.manager);
    let schema_id = SchemaId::new("todo").unwrap();
    registry
        .register_schema(
            schema_id.clone(),
            Schema::builder()
                .table(TableSchema::builder("tasks").primary_key("id", DataType::Int))
                .build(),
        )
        .unwrap();
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
        room_schema(manager, room_id).await;
    }
    let app = build_router(state.app_state());

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
        let schema = room_schema(manager, room_id).await;
        let tasks = schema.get_table_by_name("tasks").unwrap();
        assert!(tasks.get_column("priority").is_some(), "{room_id}");
        assert!(tasks.get_column("estimate").is_some(), "{room_id}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn room_deletion_finishes_when_the_request_is_dropped() {
    let dir = tempdir().unwrap();
    let state = TestState::new(dir.path(), |config| {
        config.admin_secret = ADMIN_SECRET.to_string();
    });
    let schema_id = SchemaId::new("todo").unwrap();
    state
        .registry
        .register_schema(
            schema_id.clone(),
            Schema::builder()
                .table(TableSchema::builder("tasks").primary_key("id", DataType::Int))
                .build(),
        )
        .unwrap();
    let room_id = RoomId::new("doomed").unwrap();
    state
        .manager
        .create_room(room_id.clone(), schema_id, None)
        .await
        .unwrap();

    // The request is dropped while the relay deletes the room's snapshot files.
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
    fail_point::arm_hook(
        "relay_purge_room_files",
        &dir.path().join("snapshots"),
        move || {
            let _ = reached_tx.send(());
            let _ = resume_rx.recv();
        },
    );
    let request = Request::delete("/admin/rooms/doomed")
        .header(header::AUTHORIZATION, format!("Bearer {ADMIN_SECRET}"))
        .body(Body::empty())
        .unwrap();
    tokio::select! {
        _ = build_router(state.app_state()).oneshot(request) => panic!("the purge did not stop"),
        _ = reached_rx => {}
    }
    resume_tx.send(()).unwrap();

    let room_dir = dir.path().join("rooms").join("doomed");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while room_dir.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!room_dir.exists());
    assert!(!state.manager.room_exists(&room_id).unwrap());
}

/// A `DELETE /admin/rooms/doomed` over TCP whose client disconnects while the relay purges the
/// room's snapshot: without a shutdown, the detached deletion finishes, and the next start of
/// the server finds neither the room nor its snapshot.
#[test]
fn room_deletion_whose_client_disconnects_over_tcp_removes_the_room_and_its_snapshot() {
    use crate::api::serve::{serve_until_shutdown, SHUTDOWN_GRACE_PERIOD};
    use crate::relay::LogBounds;
    use tokio::io::AsyncWriteExt;
    use zemdb_core::id::SequenceNumber;
    use zemdb_core::protocol::snapshot_envelope::{SnapshotCompression, SnapshotEnvelopeHeader};

    let dir = tempdir().unwrap();
    let room_dir = dir.path().join("rooms").join("doomed");
    let room_id = RoomId::new("doomed").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let state = TestState::new(dir.path(), |config| {
            config.admin_secret = ADMIN_SECRET.to_string();
        });
        let schema_id = SchemaId::new("todo").unwrap();
        state
            .registry
            .register_schema(
                schema_id.clone(),
                Schema::builder()
                    .table(TableSchema::builder("tasks").primary_key("id", DataType::Int))
                    .build(),
            )
            .unwrap();
        state
            .manager
            .create_room(room_id.clone(), schema_id, None)
            .await
            .unwrap();
        let body = vec![7u8; 1024];
        let header =
            SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
        let mut envelope = header.to_bytes().to_vec();
        envelope.extend_from_slice(&body);
        let bounds = LogBounds {
            tail_seq: SequenceNumber::new(1),
            head_seq: SequenceNumber::new(1),
            unrestorable_snapshot: None,
        };
        state
            .relay
            .stage_snapshot(
                &room_id,
                &crate::relay::Uploader::Admin,
                SequenceNumber::new(1),
                bytes::Bytes::from(envelope),
                std::future::ready(Ok(bounds)),
            )
            .await
            .unwrap();
        assert_eq!(
            state.relay.active_snapshot_seq(&room_id),
            Some(SequenceNumber::new(1))
        );

        // The deletion stops while the relay purges the room's snapshot files.
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        fail_point::arm_hook(
            "relay_purge_room_files",
            &dir.path().join("snapshots"),
            move || {
                let _ = reached_tx.send(());
                let _ = resume_rx.recv();
            },
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_until_shutdown(
            listener,
            state.app_state(),
            async move {
                let _ = stop_rx.await;
            },
            SHUTDOWN_GRACE_PERIOD,
        ));

        // The admin sends the DELETE and disconnects while the purge runs.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "DELETE /admin/rooms/doomed HTTP/1.1\r\nHost: zemdb\r\nAuthorization: Bearer \
             {ADMIN_SECRET}\r\nContent-Length: 0\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), reached_rx)
            .await
            .expect("the purge was not reached")
            .unwrap();
        drop(stream);
        resume_tx.send(()).unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while room_dir.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!room_dir.exists());
        stop_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the server did not shut down")
            .unwrap()
            .expect("orderly shutdown");
    });
    drop(runtime);

    // The next start of the server.
    let restarted = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    restarted.block_on(async {
        let state = TestState::new(dir.path(), |_| {});
        assert!(!state.manager.room_exists(&room_id).unwrap());
        assert_eq!(state.relay.active_snapshot_seq(&room_id), None);
    });
}
