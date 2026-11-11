//! The renewal of the token of a room: the client asks the application for a new one when the
//! server refuses it (`Unauthorized`), and before the next request after the server forbade it
//! (`Forbidden`), and sends with it only a token of its own client id.

mod common;

use common::*;
use std::time::Duration;
use tokio::time::Instant;
use zemdb_client::{Client, ClientConfig, ClientError, MemoryMetadata};
use zemdb_core::ClientMessageKind::*;
use zemdb_core::*;
use zemdb_server::generate_client_token;
use zemdb_storage::MemoryStorageEngine;

/// A server with `room-a`, and a client of it over a memory engine with `tokens`, without the
/// room open.
async fn client_of(tokens: &TestTokens) -> (TestServer, RoomId, TestTransport, Client) {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = Client::open(
        ClientConfig::new("ana"),
        MemoryStorageEngine::new(),
        MemoryMetadata::default(),
        transport.clone(),
        tokens.clone(),
    );
    (server, room_id, transport, client.await.unwrap())
}

/// [`client_of`] with the room open and registered, and a provider that issues tokens valid for
/// an hour.
async fn setup() -> (TestServer, RoomId, TestTransport, TestTokens, Client) {
    let tokens = TestTokens::new();
    let (server, room_id, transport, client) = client_of(&tokens).await;
    open_room(&client, &room_id).await.unwrap();
    (server, room_id, transport, tokens, client)
}

/// A token of the client for `room_id` that already expired.
fn expired(client: &Client, room_id: &RoomId) -> String {
    generate_client_token(&client.client_id(), room_id, Duration::ZERO, AUTH_SECRET)
}

/// Gives the open and registered `room_id` a token that already expired, without sending
/// anything, and returns it.
async fn expire(client: &Client, room_id: &RoomId) -> String {
    let token = expired(client, room_id);
    client
        .open_room(room_id.clone(), token.clone())
        .await
        .unwrap();
    token
}

/// The tokens that the messages of `kind` went with through `transport`, in order.
fn sent_with(transport: &TestTransport, kind: ClientMessageKind) -> Vec<String> {
    let bearers = transport.bearers().into_iter();
    let of_kind = bearers.filter(|(sent, _)| *sent == kind);
    of_kind.map(|(_, token)| token).collect()
}

/// How many messages went through `transport` with `token`.
fn sent_count(transport: &TestTransport, token: &str) -> usize {
    let bearers = transport.bearers().into_iter();
    bearers.filter(|(_, sent)| sent == token).count()
}

/// An error with `code` for the request `correlation_id`, without its room.
fn error_without_room(correlation_id: CorrelationId, code: ErrorCode) -> Vec<u8> {
    encode_message(&ServerMessage::Error {
        correlation_id: Some(correlation_id),
        room_id: None,
        code,
        message: "Fabricated".to_string(),
    })
    .unwrap()
}

/// An error with `code` for `room_id`, without the correlation id of its request.
fn error_without_correlation(room_id: &RoomId, code: ErrorCode) -> Vec<u8> {
    encode_message(&ServerMessage::Error {
        correlation_id: None,
        room_id: Some(room_id.clone()),
        code,
        message: "Fabricated".to_string(),
    })
    .unwrap()
}

#[tokio::test]
async fn a_registration_with_an_expired_token_is_sent_again_with_a_new_one() {
    let tokens = TestTokens::new();
    let (_server, room_id, transport, client) = client_of(&tokens).await;
    let first = expired(&client, &room_id);
    client
        .open_room(room_id.clone(), first.clone())
        .await
        .unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    // The provider was asked for a token of the client for the room, and the registration went
    // again with it, as its bearer and inside it.
    assert_eq!(tokens.asked(), [(client.client_id(), room_id.clone())]);
    let renewed = tokens.given()[0].clone();
    assert_eq!(
        sent_with(&transport, RegisterClient),
        [first, renewed.clone()]
    );
    // The next request goes with it too.
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(sent_with(&transport, Sync), [renewed]);
    assert_eq!(tokens.calls(), 1);
}

#[tokio::test]
async fn a_sync_a_heartbeat_and_a_commit_with_an_expired_token_are_sent_again_with_a_new_one() {
    for kind in [Sync, Heartbeat, Commit] {
        let (server, room_id, transport, tokens, client) = setup().await;
        server.commit(&room_id, [1]).await;
        let first = expire(&client, &room_id).await;
        match kind {
            Sync => client.sync(room_id.clone()).await.unwrap(),
            Heartbeat => client.heartbeat(room_id.clone()).await.unwrap(),
            _ => client.write(room_id.clone(), insert_task(2)).await.unwrap(),
        }
        assert_eq!(tokens.calls(), 1, "{kind:?}");
        let renewed = tokens.given()[0].clone();
        let sent = sent_with(&transport, kind);
        assert_eq!(sent, [first.clone(), renewed.clone()], "{kind:?}");
        // The next request goes with it: the ack of the sync, and another heartbeat.
        client.heartbeat(room_id.clone()).await.unwrap();
        let heartbeats = sent_with(&transport, Heartbeat);
        assert_eq!(heartbeats.last(), Some(&renewed), "{kind:?}");
        if kind == Sync {
            assert_eq!(sent_with(&transport, Ack), std::slice::from_ref(&renewed));
        }
        assert_eq!(sent_count(&transport, &first), 1, "{kind:?}");
        assert_eq!(tokens.calls(), 1, "{kind:?}");
    }
}

#[tokio::test]
async fn an_ack_refused_as_unauthorized_is_sent_again_with_a_new_token() {
    let (server, room_id, transport, tokens, client) = setup().await;
    server.commit(&room_id, [1, 2]).await;
    answer_next(&transport, Ack, |id, room| {
        error_reply(id, room, ErrorCode::Unauthorized)
    });
    let first = sent_with(&transport, RegisterClient)[0].clone();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 1);
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_with(&transport, Ack), [first, renewed]);
    assert_eq!(requests(&transport), [("sync", 0), ("ack", 2), ("ack", 2)]);
}

#[tokio::test]
async fn a_chunk_of_an_upload_and_one_of_a_download_refused_as_unauthorized_are_sent_again() {
    // A client at 9, which the server designates once a client registers behind the log.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let (tokens, lagging_tokens) = (TestTokens::new(), TestTokens::new());
    let open = |tokens: &TestTokens| {
        let transport = server.transport();
        let client = Client::open(
            ClientConfig::new("ana"),
            MemoryStorageEngine::new(),
            MemoryMetadata::default(),
            transport.clone(),
            tokens.clone(),
        );
        async move { (transport, client.await.unwrap()) }
    };
    let (transport, client) = open(&tokens).await;
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=9).await;
    client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let (lagging_transport, lagging) = open(&lagging_tokens).await;
    open_room(&lagging, &room_id).await.unwrap();

    answer_next(&transport, UploadSnapshotChunk, |id, room| {
        error_reply(id, room, ErrorCode::Unauthorized)
    });
    let first = sent_with(&transport, RegisterClient)[0].clone();
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 1);
    let renewed = tokens.given()[0].clone();
    let uploads = sent_with(&transport, UploadSnapshotChunk);
    assert_eq!(uploads, [first, renewed]);
    let active = server.state.snapshot_relay.active_snapshot_seq(&room_id);
    assert_eq!(active, Some(SequenceNumber::new(9)));

    answer_next(&lagging_transport, RequestSnapshotChunk, |id, room| {
        error_reply(id, room, ErrorCode::Unauthorized)
    });
    let first = sent_with(&lagging_transport, RegisterClient)[0].clone();
    lagging.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(lagging_tokens.calls(), 1);
    let renewed = lagging_tokens.given()[0].clone();
    let chunks = sent_with(&lagging_transport, RequestSnapshotChunk);
    assert_eq!(chunks, [first, renewed.clone()]);
    lagging.sync(room_id.clone()).await.unwrap();
    assert_eq!(sent_with(&lagging_transport, Sync), [renewed]);
    let status_now = lagging.status(room_id.clone()).await.unwrap();
    assert_eq!(status_now.head_seq, SequenceNumber::new(9));
}

#[tokio::test(start_paused = true)]
async fn requests_refused_with_the_same_token_wait_for_one_renewal() {
    let (server, room_id, transport, tokens, client) = setup().await;
    server.commit(&room_id, [1]).await;
    // The provider takes a while: every request gets its refusal before the token comes.
    tokens.set(Issue::After(Duration::from_secs(1)));
    let first = expire(&client, &room_id).await;
    let (heartbeat, synced, written) = tokio::join!(
        client.heartbeat(room_id.clone()),
        client.sync(room_id.clone()),
        client.write(room_id.clone(), insert_task(2)),
    );
    heartbeat.unwrap();
    synced.unwrap();
    written.unwrap();
    assert_eq!(tokens.calls(), 1);
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_count(&transport, &first), 3);
    for kind in [Heartbeat, Sync, Commit] {
        let sent = sent_with(&transport, kind);
        assert_eq!(sent.last(), Some(&renewed), "{kind:?}");
    }
    assert_eq!(
        client.status(room_id.clone()).await.unwrap().head_seq.get(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn a_provider_that_does_not_answer_fails_the_requests_once_its_time_is_up() {
    let (_server, room_id, transport, tokens, client) = setup().await;
    tokens.set(Issue::Hang);
    let first = expire(&client, &room_id).await;
    let start = Instant::now();
    let (heartbeat, synced) = tokio::join!(
        client.heartbeat(room_id.clone()),
        client.sync(room_id.clone()),
    );
    // Both waited for the one renewal, which got no token after 60 s.
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");
    assert_eq!(tokens.calls(), 1);
    assert_eq!(sent_with(&transport, Heartbeat).last(), Some(&first));
    assert_eq!(sent_with(&transport, Sync), std::slice::from_ref(&first));
    // The next request that needs a token asks again.
    tokens.set(Issue::Valid(Duration::from_secs(3600)));
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_renewal_cut_with_its_write_leaves_the_others_to_ask_again() {
    // The write asks first, and the provider does not answer it; the heartbeat waits for that
    // renewal, which the deadline of the write cuts at 30 s.
    let (_server, room_id, transport, tokens, client) = setup().await;
    tokens.then(Issue::Hang);
    let first = expire(&client, &room_id).await;
    let start = Instant::now();
    let heartbeat = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let heartbeat = client.heartbeat(room_id.clone()).await;
        (heartbeat, start.elapsed())
    };
    let written = async {
        let written = client.write(room_id.clone(), insert_task(1)).await;
        (written, start.elapsed())
    };
    let ((written, written_at), (heartbeat, heartbeat_at)) = tokio::join!(written, heartbeat);
    // The commit refused as unauthorized had no effect, and nothing was sent after it.
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    assert_eq!(written_at, Duration::from_secs(30));
    assert_eq!(sent_with(&transport, Commit), [first]);
    // The heartbeat asked again once the write stopped, and went with the token.
    heartbeat.unwrap();
    assert_eq!(heartbeat_at, Duration::from_secs(30));
    assert_eq!(tokens.calls(), 2);
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_with(&transport, Heartbeat).last(), Some(&renewed));
}

#[tokio::test(start_paused = true)]
async fn a_write_refused_as_unauthorized_retries_with_the_new_token_and_the_same_mutation_id() {
    let (_server, room_id, transport, tokens, client) = setup().await;
    let first = expire(&client, &room_id).await;
    client.write(room_id.clone(), insert_task(1)).await.unwrap();
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_with(&transport, Commit), [first, renewed]);
    let sent = commits(&transport);
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].0, sent[1].0);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 1, true)));
}

#[tokio::test(start_paused = true)]
async fn a_write_without_a_new_token_ends_with_network_or_as_unknown() {
    // The provider fails: the write ends without effect (a token that the server refuses too,
    // in the next test).
    let (_server, room_id, transport, tokens, client) = setup().await;
    tokens.set(Issue::Fail);
    let first = expire(&client, &room_id).await;
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    assert_eq!(tokens.calls(), 1);
    assert_eq!(sent_with(&transport, Commit), [first]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    // After an attempt that may have been sequenced, its outcome is unknown.
    let (_server, room_id, transport, tokens, client) = setup().await;
    tokens.set(Issue::Fail);
    let mut attempts = 0;
    transport.answer(move |message| {
        if message.kind() != Commit {
            return None;
        }
        attempts += 1;
        let (id, room) = request(message);
        let code = match attempts {
            1 => ErrorCode::Unavailable,
            _ => ErrorCode::Unauthorized,
        };
        Some(Ok(error_reply(id, &room, code)))
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    assert_eq!(commits(&transport).len(), 2);
    assert_eq!(tokens.calls(), 1);
}

#[tokio::test]
async fn a_second_refusal_with_the_renewed_token_asks_for_no_other() {
    // With the real clock, and a limit of its own: a request that asked again would not end.
    let limit = Duration::from_secs(10);
    for kind in [Heartbeat, Commit] {
        let (_server, room_id, transport, tokens, client) = setup().await;
        tokens.set(Issue::Valid(Duration::ZERO));
        let first = expire(&client, &room_id).await;
        let done = match kind {
            Heartbeat => tokio::time::timeout(limit, client.heartbeat(room_id.clone())).await,
            _ => tokio::time::timeout(limit, client.write(room_id.clone(), insert_task(1))).await,
        };
        let done = done.expect("It asked for another token");
        assert!(
            matches!(done, Err(ClientError::Network(_))),
            "{kind:?}: {done:?}"
        );
        assert_eq!(tokens.calls(), 1, "{kind:?}");
        let renewed = tokens.given()[0].clone();
        assert_eq!(sent_with(&transport, kind), [first, renewed], "{kind:?}");
        assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    }
    // Also after the renewal before sending a request with a token that the server forbade.
    let (server, room_id, transport, tokens, client) = setup().await;
    let other_room = server.create_room("room-b").await;
    let forbidden = token(&client.client_id(), &other_room);
    client
        .open_room(room_id.clone(), forbidden.clone())
        .await
        .unwrap();
    let heartbeat = client.heartbeat(room_id.clone()).await;
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    tokens.set(Issue::Valid(Duration::ZERO));
    let heartbeat = tokio::time::timeout(limit, client.heartbeat(room_id.clone())).await;
    let heartbeat = heartbeat.expect("It asked for another token");
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    assert_eq!(tokens.calls(), 1);
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_with(&transport, Heartbeat), [forbidden, renewed]);
}

#[tokio::test(start_paused = true)]
async fn after_a_forbidden_token_the_next_request_asks_for_another_before_it_is_sent() {
    let (server, room_id, transport, tokens, client) = setup().await;
    let other_room = server.create_room("room-b").await;
    let forbidden = token(&client.client_id(), &other_room);
    for kind in [Heartbeat, Commit] {
        client
            .open_room(room_id.clone(), forbidden.clone())
            .await
            .unwrap();
        let calls = tokens.calls();
        // The request is refused, and not sent again: a write ends with `Validation`.
        match kind {
            Heartbeat => {
                let heartbeat = client.heartbeat(room_id.clone()).await;
                assert!(
                    matches!(heartbeat, Err(ClientError::Network(_))),
                    "{heartbeat:?}"
                );
            }
            _ => {
                let written = client.write(room_id.clone(), insert_task(1)).await;
                assert!(
                    matches!(written, Err(ClientError::Validation(_))),
                    "{written:?}"
                );
            }
        }
        assert_eq!(tokens.calls(), calls, "{kind:?}");
        assert_eq!(sent_with(&transport, kind).last(), Some(&forbidden));
        // The next request asks for a token first, and goes with it.
        let sent = sent_count(&transport, &forbidden);
        client.sync(room_id.clone()).await.unwrap();
        assert_eq!(tokens.calls(), calls + 1, "{kind:?}");
        let renewed = tokens.given().last().cloned();
        assert_eq!(sent_with(&transport, Sync).last(), renewed.as_ref());
        assert_eq!(sent_count(&transport, &forbidden), sent, "{kind:?}");
    }
    assert_eq!(commits(&transport).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_forbidden_token_without_a_new_one_goes_again_and_the_room_is_not_stopped() {
    let (server, room_id, transport, tokens, client) = setup().await;
    let other_room = server.create_room("room-b").await;
    let forbidden = token(&client.client_id(), &other_room);
    client
        .open_room(room_id.clone(), forbidden.clone())
        .await
        .unwrap();
    tokens.set(Issue::Fail);
    for calls in 0..2 {
        let heartbeat = client.heartbeat(room_id.clone()).await;
        assert!(
            matches!(heartbeat, Err(ClientError::Network(_))),
            "{heartbeat:?}"
        );
        assert_eq!(tokens.calls(), calls);
    }
    assert_eq!(
        sent_with(&transport, Heartbeat),
        [forbidden.clone(), forbidden]
    );
    tokens.set(Issue::Valid(Duration::from_secs(3600)));
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 2);
}

#[tokio::test]
async fn a_renewed_token_of_another_client_or_not_a_token_is_never_sent() {
    let other_client = ClientId::new("ana#tablet").unwrap();
    for issue in [
        Issue::ForClient(other_client),
        Issue::Text("not-a-token".to_string()),
        Issue::Text(String::new()),
    ] {
        let (_server, room_id, transport, tokens, client) = setup().await;
        tokens.set(issue.clone());
        let first = expire(&client, &room_id).await;
        let heartbeat = client.heartbeat(room_id.clone()).await;
        assert!(
            matches!(heartbeat, Err(ClientError::Network(_))),
            "{issue:?}: {heartbeat:?}"
        );
        assert_eq!(tokens.calls(), 1, "{issue:?}");
        let given = tokens.given()[0].clone();
        assert_eq!(sent_count(&transport, &given), 0, "{issue:?}");
        let heartbeats = sent_with(&transport, Heartbeat);
        assert_eq!(heartbeats.last(), Some(&first), "{issue:?}");
        // The next request that needs one asks again, and a token of the client is used.
        tokens.set(Issue::Valid(Duration::from_secs(3600)));
        client.heartbeat(room_id.clone()).await.unwrap();
        assert_eq!(tokens.calls(), 2, "{issue:?}");
        let renewed = tokens.given()[1].clone();
        let heartbeats = sent_with(&transport, Heartbeat);
        assert_eq!(heartbeats.last(), Some(&renewed), "{issue:?}");
    }
}

#[tokio::test]
async fn an_unauthorized_of_another_request_or_room_renews_nothing() {
    let (server, room_id, transport, tokens, client) = setup().await;
    let other_room = server.create_room("room-b").await;
    let (to_other_room, room) = (other_room.clone(), room_id.clone());
    for answer in [
        Box::new(move |id: CorrelationId, _: &RoomId| {
            let other = CorrelationId::new(id.get() + 1000);
            error_reply(other, &room, ErrorCode::Unauthorized)
        }) as Box<dyn FnOnce(CorrelationId, &RoomId) -> Vec<u8> + Send>,
        Box::new(move |id: CorrelationId, _: &RoomId| {
            error_reply(id, &to_other_room, ErrorCode::Unauthorized)
        }),
    ] {
        answer_next(&transport, Heartbeat, answer);
        let heartbeat = client.heartbeat(room_id.clone()).await;
        assert!(
            matches!(heartbeat, Err(ClientError::Network(_))),
            "{heartbeat:?}"
        );
        assert_eq!(tokens.calls(), 0);
    }
    assert_eq!(sent_with(&transport, Heartbeat).len(), 2);
    // Without its correlation id, or without its room, it is the refusal of the request.
    answer_next(&transport, Heartbeat, |_, room| {
        error_without_correlation(room, ErrorCode::Unauthorized)
    });
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 1);
    answer_next(&transport, Heartbeat, |id, _| {
        error_without_room(id, ErrorCode::Unauthorized)
    });
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 2);
    assert_eq!(sent_with(&transport, Heartbeat).len(), 6);
}

#[tokio::test]
async fn the_token_of_an_open_replaces_the_renewed_one_and_a_forbidden_one() {
    let (server, room_id, transport, tokens, client) = setup().await;
    expire(&client, &room_id).await;
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 1);
    let opened = token(&client.client_id(), &room_id);
    client
        .open_room(room_id.clone(), opened.clone())
        .await
        .unwrap();
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(sent_with(&transport, Heartbeat).last(), Some(&opened));
    // A token that the server forbade, replaced by an open: the next request goes with the new
    // one, without asking for another.
    let other_room = server.create_room("room-b").await;
    let forbidden = token(&client.client_id(), &other_room);
    client
        .open_room(room_id.clone(), forbidden.clone())
        .await
        .unwrap();
    let heartbeat = client.heartbeat(room_id.clone()).await;
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    let reopened = generate_client_token(
        &client.client_id(),
        &room_id,
        Duration::from_secs(900),
        AUTH_SECRET,
    );
    client
        .open_room(room_id.clone(), reopened.clone())
        .await
        .unwrap();
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(sent_with(&transport, Heartbeat).last(), Some(&reopened));
    assert_eq!(tokens.calls(), 1);
}

#[tokio::test]
async fn a_forbidden_reply_to_a_token_already_replaced_leaves_the_new_one() {
    // An open replaces the token while a heartbeat with the old one waits for its reply.
    let (server, room_id, transport, tokens, client) = setup().await;
    let other_room = server.create_room("room-b").await;
    let forbidden = token(&client.client_id(), &other_room);
    client
        .open_room(room_id.clone(), forbidden.clone())
        .await
        .unwrap();
    let opened = generate_client_token(
        &client.client_id(),
        &room_id,
        Duration::from_secs(900),
        AUTH_SECRET,
    );
    let mut held = transport.hold_next_reply(Heartbeat);
    let (client_ref, opened_ref, room_ref) = (&client, &opened, &room_id);
    let reopen = async move {
        held.replied().await;
        client_ref
            .open_room(room_ref.clone(), opened_ref.clone())
            .await
            .unwrap();
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(client.heartbeat(room_id.clone()), reopen);
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    // The forbidden token was the old one: the next request goes with the new one, without
    // asking for another.
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 0);
    assert_eq!(sent_with(&transport, Heartbeat), [forbidden, opened]);
}

/// [`tasks_schema`] with the column `title` of another type: it does not evolve from it.
fn retyped_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    Schema::builder().table(table).build()
}

/// Makes the next sync through `transport` fail the room closed: its batch announces another
/// schema, and the registration that follows gets one that does not evolve from the replica's.
fn fail_next_sync_closed(transport: &TestTransport, room_incarnation: u128) {
    transport.answer(move |message| {
        let (correlation_id, room_id) = request(message);
        let reply = match message {
            ClientMessage::Sync { .. } => ServerMessage::SyncBatch {
                correlation_id,
                room_id,
                head_seq: SequenceNumber::new(0),
                ops: Vec::new(),
                has_more: false,
                snapshot_wanted: false,
                schema_version: 1,
                schema_id: SchemaId::new("other").unwrap(),
            },
            ClientMessage::RegisterClient { .. } => ServerMessage::Registered {
                correlation_id,
                room_id,
                head_seq: SequenceNumber::new(0),
                tail_seq: SequenceNumber::new(0),
                schema_id: SchemaId::new("other").unwrap(),
                schema: retyped_schema(),
                active_snapshot_seq: None,
                room_incarnation,
            },
            _ => return None,
        };
        Some(Ok(encode_message(&reply).unwrap()))
    });
}

#[tokio::test(start_paused = true)]
async fn a_write_that_ends_while_it_renews_the_token_sends_nothing_more() {
    // The room closes, or fails closed, while the provider takes 5 s for the token of a commit
    // refused as unauthorized, or for one the server forbade before the first commit.
    for (case, forbidden) in [("close", false), ("fail", false), ("close", true)] {
        let (server, room_id, transport, tokens, client) = setup().await;
        let incarnation = server.incarnation(&room_id).await;
        let sent = if forbidden {
            let other_room = server.create_room("room-b").await;
            let forbidden = token(&client.client_id(), &other_room);
            client
                .open_room(room_id.clone(), forbidden.clone())
                .await
                .unwrap();
            let heartbeat = client.heartbeat(room_id.clone()).await;
            assert!(heartbeat.is_err(), "{heartbeat:?}");
            0
        } else {
            expire(&client, &room_id).await;
            1
        };
        tokens.set(Issue::After(Duration::from_secs(5)));
        let ends = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match case {
                "close" => client.close_room(room_id.clone()).await.unwrap(),
                _ => {
                    fail_next_sync_closed(&transport, incarnation);
                    let synced = client.sync(room_id.clone()).await;
                    assert!(
                        matches!(synced, Err(ClientError::FailedClosed(_))),
                        "{synced:?}"
                    );
                }
            }
        };
        let (written, ()) = tokio::join!(client.write(room_id.clone(), insert_task(1)), ends);
        assert!(
            matches!(written, Err(ClientError::Network(_))),
            "{case} {forbidden}: {written:?}"
        );
        assert_eq!(commits(&transport).len(), sent, "{case} {forbidden}");
        transport.settled().await;
        let bounds = server
            .state
            .room_manager
            .log_bounds(&room_id)
            .await
            .unwrap();
        assert_eq!(bounds.head_seq.get(), 0, "{case} {forbidden}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_sent_again_with_the_renewed_token_and_cut_by_the_deadline_is_uncertain() {
    let (server, room_id, transport, tokens, client) = setup().await;
    let first = expire(&client, &room_id).await;
    let mut refused = transport.hold_next_reply(Commit);
    let mut write = std::pin::pin!(client.write(room_id.clone(), insert_task(1)));
    tokio::select! {
        written = &mut write => panic!("The write ended first: {written:?}"),
        () = refused.replied() => {}
    }
    // The reply to the commit sent again does not come before the deadline.
    let _held = transport.hold_next_reply(Commit);
    refused.release();
    let start = Instant::now();
    let written = write.await;
    assert_eq!(start.elapsed(), Duration::from_secs(30));
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_with(&transport, Commit), [first, renewed]);
    // It was sequenced.
    let bounds = server
        .state
        .room_manager
        .log_bounds(&room_id)
        .await
        .unwrap();
    assert_eq!(bounds.head_seq.get(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_request_refused_with_another_token_does_not_take_the_failure_of_its_renewal() {
    // The heartbeat renews the token of the room and the provider does not answer it; meanwhile
    // an open gives the room another token, which the server refuses to a sync.
    let (_server, room_id, transport, tokens, client) = setup().await;
    tokens.then(Issue::Hang);
    answer_next(&transport, Heartbeat, |id, room| {
        error_reply(id, room, ErrorCode::Unauthorized)
    });
    let start = Instant::now();
    let synced = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        expire(&client, &room_id).await;
        let synced = client.sync(room_id.clone()).await;
        (synced, start.elapsed())
    };
    let (heartbeat, (synced, synced_at)) = tokio::join!(client.heartbeat(room_id.clone()), synced);
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    // The sync waited for that renewal, and then asked for a token of its own.
    synced.unwrap();
    assert_eq!(synced_at, Duration::from_secs(60));
    assert_eq!(tokens.calls(), 2);
    let renewed = tokens.given()[0].clone();
    assert_eq!(sent_with(&transport, Sync).last(), Some(&renewed));
}

#[tokio::test(start_paused = true)]
async fn an_open_while_the_token_is_renewed_keeps_its_token() {
    let (_server, room_id, transport, tokens, client) = setup().await;
    tokens.set(Issue::After(Duration::from_secs(5)));
    let first = expire(&client, &room_id).await;
    let opened = generate_client_token(
        &client.client_id(),
        &room_id,
        Duration::from_secs(900),
        AUTH_SECRET,
    );
    let open = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        client
            .open_room(room_id.clone(), opened.clone())
            .await
            .unwrap();
    };
    let (heartbeat, ()) = tokio::join!(client.heartbeat(room_id.clone()), open);
    heartbeat.unwrap();
    assert_eq!(tokens.calls(), 1);
    client.heartbeat(room_id.clone()).await.unwrap();
    let opened_twice = [first, opened.clone(), opened];
    assert_eq!(sent_with(&transport, Heartbeat), opened_twice);
}

#[tokio::test(start_paused = true)]
async fn a_renewed_token_of_another_room_is_renewed_again_before_the_next_request() {
    let (server, room_id, transport, tokens, client) = setup().await;
    let other_room = server.create_room("room-b").await;
    tokens.then(Issue::ForRoom(other_room));
    expire(&client, &room_id).await;
    // The token renewed is forbidden: the heartbeat fails.
    let heartbeat = client.heartbeat(room_id.clone()).await;
    assert!(
        matches!(heartbeat, Err(ClientError::Network(_))),
        "{heartbeat:?}"
    );
    assert_eq!(tokens.calls(), 1);
    let forbidden = tokens.given()[0].clone();
    // The next asks for another before it is sent.
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(tokens.calls(), 2);
    assert_eq!(sent_count(&transport, &forbidden), 1);
}

#[tokio::test(start_paused = true)]
async fn a_chunk_of_an_upload_is_not_sent_again_once_the_room_closes_while_it_renews() {
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let tokens = TestTokens::new();
    let open = || {
        let transport = server.transport();
        let client = Client::open(
            ClientConfig::new("ana"),
            MemoryStorageEngine::new(),
            MemoryMetadata::default(),
            transport.clone(),
            tokens.clone(),
        );
        async move { (transport, client.await.unwrap()) }
    };
    let (transport, client) = open().await;
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=9).await;
    client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let (_, lagging) = open().await;
    open_room(&lagging, &room_id).await.unwrap();
    // The designated client gets its chunk refused as unauthorized, and the room closes while
    // the provider takes 5 s.
    tokens.set(Issue::After(Duration::from_secs(5)));
    answer_next(&transport, UploadSnapshotChunk, |id, room| {
        error_reply(id, room, ErrorCode::Unauthorized)
    });
    let close = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        client.close_room(room_id.clone()).await.unwrap();
    };
    let (heartbeat, ()) = tokio::join!(client.heartbeat(room_id.clone()), close);
    assert!(heartbeat.is_err(), "{heartbeat:?}");
    assert_eq!(sent_with(&transport, UploadSnapshotChunk).len(), 1);
    assert_eq!(
        server.state.snapshot_relay.active_snapshot_seq(&room_id),
        None
    );
}
