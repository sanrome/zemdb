mod common;

use axum::http::StatusCode;
use common::{build, commit, make_test_op, register, tasks_schema, token};
use futures::StreamExt;
use std::time::Duration;
use zemdb_core::*;
use zemdb_server::ClientLeaseTracker;

#[tokio::test]
async fn shutdown_ends_sse_streams_and_persists_rosters() {
    let parts = build(|_| {});
    let schema_id = parts.register_schema("todo", tasks_schema());
    let room_id = parts.create_room("room-shutdown", &schema_id).await;

    // A client whose latest cursor is only in memory when the shutdown starts.
    let client_id = ClientId::new("writer").unwrap();
    let sender = parts
        .room_manager
        .get_or_spawn(&room_id, None)
        .await
        .unwrap();
    register(&sender, &client_id).await;
    for (mutation, last_ack_seq) in [(1u8, 0), (2, 1)] {
        commit(
            &sender,
            &client_id,
            MutationId::new([mutation; 16]),
            last_ack_seq,
            make_test_op(mutation.into()),
        )
        .await
        .unwrap();
    }

    let (addr, stop, server) = parts.serve_until_stopped().await;

    // An open SSE stream would keep the server alive forever unless shutdown ends it.
    let sse = reqwest::Client::new()
        .get(format!("http://{addr}/rooms/{room_id}/events"))
        .bearer_auth(token(&client_id, &room_id))
        .send()
        .await
        .unwrap();
    assert_eq!(sse.status(), StatusCode::OK);
    let mut events = sse.bytes_stream();

    stop.send(()).unwrap();

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
    let roster = parts
        .data_dir
        .join("rooms")
        .join(room_id.as_str())
        .join(format!("meta_clients_{}.json", room_id.as_str()));
    let persisted = ClientLeaseTracker::open_or_create(roster).unwrap();
    let entry = persisted.get_client(&client_id).unwrap();
    assert_eq!(entry.last_ack_seq, SequenceNumber::new(1));
}
