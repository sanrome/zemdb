use super::*;
use crate::api::auth::generate_client_token;
use crate::api::router::build_router;
use crate::api::test_support::TestState;
use crate::fail_point;
use axum::body::Body;
use axum::http::Request;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use zemdb_core::id::{ClientId, MutationId, SchemaId, SequenceNumber};
use zemdb_core::mutation::{Operation, OperationKind};
use zemdb_core::protocol::codec::{
    decode_message, MAX_FRAME_SIZE, MAX_MESSAGE_SIZE, PROTOCOL_HEADER_LEN,
};
use zemdb_core::protocol::messages::{ErrorCode, SequencedOperation};
use zemdb_core::protocol::wal_frame::encode_wal_record;
use zemdb_core::schema::{Schema, TableSchema, MAX_COLUMNS};
use zemdb_core::value::{CompactRow, DataType, PrimaryKey, RowBuilder, Value};

const AUTH_SECRET: &str = "data_plane_cluster_secret_key_1234";

struct Fixture {
    dir: TempDir,
    app: axum::Router,
    manager: std::sync::Arc<crate::RoomManager>,
    relay: std::sync::Arc<crate::relay::SnapshotRelay>,
    room_id: RoomId,
    /// The incarnation of the room, which the snapshot chunks carry.
    incarnation: u128,
    client_id: ClientId,
    token: String,
}

/// Int columns of the `wide` table, besides its key and its string column.
const WIDE_INT_COLUMNS: usize = 5000;

fn schema() -> Schema {
    let tasks = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .nullable_column("title", DataType::String);
    let mut wide = TableSchema::builder("wide")
        .primary_key("id", DataType::Int)
        .nullable_column("text", DataType::String);
    for i in 0..WIDE_INT_COLUMNS {
        wide = wide.nullable_column(format!("c{i}"), DataType::Int);
    }
    Schema::builder().table(tasks).table(wide).build()
}

/// Insert into `tasks` with a title of `title_len` bytes.
fn task_with_title(id: i64, title_len: usize) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "x".repeat(title_len))
        .build();
    schema().to_operation_insert("tasks", &row, 1000).unwrap()
}

/// Insert into `wide` with a text of `text_len` bytes and every int column set to 0. Each
/// small int takes 2 bytes on the wire and 12 in a log record.
fn wide_row(id: i64, text_len: usize) -> Operation {
    let table_id = schema().get_table_by_name("wide").unwrap().table_id();
    let mut values = vec![Value::Int(id), Value::String("x".repeat(text_len).into())];
    values.extend(std::iter::repeat_n(Value::Int(0), WIDE_INT_COLUMNS));
    Operation::new(
        table_id,
        PrimaryKey::single(Value::Int(id)),
        1000,
        OperationKind::Insert {
            row: CompactRow::new(values),
        },
    )
}

impl Fixture {
    /// A router over a room with one registered client.
    async fn new() -> Self {
        Self::with(|_| {}).await
    }

    /// [`Fixture::new`] with the configuration adjusted by `configure`.
    async fn with(configure: impl FnOnce(&mut crate::config::ServerConfig)) -> Self {
        let dir = tempdir().unwrap();
        let state = TestState::new(dir.path(), |config| {
            config.auth_secret = AUTH_SECRET.to_string();
            configure(config);
        });
        let schema_id = SchemaId::new("tasks").unwrap();
        state
            .registry
            .register_schema(schema_id.clone(), schema())
            .unwrap();
        let room_id = RoomId::new("room").unwrap();
        let incarnation = state
            .manager
            .create_room(room_id.clone(), schema_id, None)
            .await
            .unwrap()
            .room_incarnation;
        let client_id = ClientId::new("writer").unwrap();
        let token =
            generate_client_token(&client_id, &room_id, Duration::from_secs(300), AUTH_SECRET);
        let fx = Self {
            app: build_router(state.app_state()),
            manager: state.manager,
            relay: state.relay,
            dir,
            room_id,
            incarnation,
            client_id,
            token,
        };

        fx.register().await;
        fx
    }

    /// Registers the client, which must succeed.
    async fn register(&self) {
        let (status, _, _) = self.post("register", &self.register_msg()).await;
        assert_eq!(status, StatusCode::OK);
    }

    fn register_msg(&self) -> ClientMessage {
        ClientMessage::RegisterClient {
            correlation_id: CorrelationId::new(1),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            auth_token: self.token.clone(),
            current_seq: None,
            known_incarnation: None,
        }
    }

    /// Posts `msg`, returning the response without reading its body.
    async fn send(&self, endpoint: &str, msg: &ClientMessage) -> Response {
        let request = Request::post(format!("/rooms/{}/{endpoint}", self.room_id))
            .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
            .body(Body::from(encode_message(msg).unwrap()))
            .unwrap();
        self.app.clone().oneshot(request).await.unwrap()
    }

    async fn post(
        &self,
        endpoint: &str,
        msg: &ClientMessage,
    ) -> (StatusCode, axum::http::HeaderMap, ServerMessage) {
        let response = self.send(endpoint, msg).await;
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, headers, decode_message(&body).unwrap())
    }

    fn commit(&self, n: u8, last_ack_seq: u64) -> ClientMessage {
        let row = RowBuilder::new().set("id", n as i64).build();
        self.commit_op(
            n,
            last_ack_seq,
            schema().to_operation_insert("tasks", &row, 1000).unwrap(),
        )
    }

    fn commit_op(&self, n: u8, last_ack_seq: u64, op: Operation) -> ClientMessage {
        ClientMessage::Commit {
            correlation_id: CorrelationId::new(n as u64),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            mutation_id: MutationId::new([n; 16]),
            last_ack_seq: SequenceNumber::new(last_ack_seq),
            op,
        }
    }

    fn sync(&self, from_seq: u64) -> ClientMessage {
        ClientMessage::Sync {
            correlation_id: CorrelationId::new(100 + from_seq),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            from_seq: SequenceNumber::new(from_seq),
            max_batch_size: 100,
        }
    }

    /// The first chunk of a download, anchored to `snapshot_hash` if given.
    fn first_chunk(&self, snapshot_hash: Option<[u8; 32]>) -> ClientMessage {
        ClientMessage::RequestSnapshotChunk {
            correlation_id: CorrelationId::new(3),
            room_id: self.room_id.clone(),
            chunk_index: 0,
            chunk_size: 0,
            snapshot_hash,
            room_incarnation: self.incarnation,
        }
    }

    /// Has the snapshot worker stage a snapshot at `seq` against `bounds`.
    async fn stage_snapshot(
        &self,
        seq: u64,
        bounds: impl std::future::Future<Output = Result<crate::relay::LogBounds, ServerError>>,
    ) -> [u8; 32] {
        let body = b"snapshot";
        let header = zemdb_core::protocol::snapshot_envelope::SnapshotEnvelopeHeader::for_body(
            zemdb_core::protocol::snapshot_envelope::SnapshotCompression::Raw,
            body.len() as u32,
            body,
        );
        let data = [&header.to_bytes()[..], body].concat();
        self.relay
            .stage_snapshot(
                &self.room_id,
                &Uploader::Admin,
                SequenceNumber::new(seq),
                data.into(),
                bounds,
            )
            .await
            .unwrap()
    }

    /// Posts a commit, expecting it to be acknowledged.
    async fn commit_ok(&self, msg: &ClientMessage) {
        let (status, _, reply) = self.post("commit", msg).await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
    }
}

/// Asserts an error frame with `code`, answered with HTTP `expected`.
fn assert_error(status: StatusCode, expected: StatusCode, code: ErrorCode, reply: &ServerMessage) {
    assert_eq!(status, expected, "{reply:?}");
    assert!(
        matches!(reply, ServerMessage::Error { code: got, .. } if *got == code),
        "{reply:?}"
    );
}

/// Asserts a `BadRequest` error frame with HTTP 400.
fn assert_bad_request(status: StatusCode, reply: &ServerMessage) {
    assert_error(
        status,
        StatusCode::BAD_REQUEST,
        ErrorCode::BadRequest,
        reply,
    );
}

#[tokio::test]
async fn commit_that_restarts_the_room_is_unavailable_with_retry_after() {
    let fx = Fixture::new().await;
    let (status, _, _) = fx.post("commit", &fx.commit(1, 0)).await;
    assert_eq!(status, StatusCode::OK);

    // The log append fails: the room stops to be recovered from disk.
    fail_point::arm(
        "warm_append_write",
        &fx.dir
            .path()
            .join("rooms")
            .join(fx.room_id.as_str())
            .join("segments")
            .join("active.wal"),
    );
    let (status, headers, reply) = fx.post("commit", &fx.commit(2, 1)).await;
    assert_error(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
        &reply,
    );
    assert_eq!(headers[header::RETRY_AFTER], "1");

    // Retrying is safe and succeeds once the room is back.
    let (status, _, reply) = fx.post("commit", &fx.commit(2, 1)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(2)
    ));
}

#[tokio::test]
async fn request_that_makes_the_room_panic_is_internal_and_the_room_recovers() {
    let fx = Fixture::new().await;
    let heartbeat = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(7),
        room_id: fx.room_id.clone(),
        client_id: fx.client_id.clone(),
    };

    fail_point::arm(
        "room_command_panic",
        &fx.dir.path().join("rooms").join(fx.room_id.as_str()),
    );
    let (status, headers, reply) = fx.post("heartbeat", &heartbeat).await;
    assert_error(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::Internal,
        &reply,
    );
    assert!(headers.get(header::RETRY_AFTER).is_none());

    // The next request reopens the room from disk. The roster is not saved after a panic, so
    // the client registers again (its registration may not have reached the disk).
    fx.register().await;
    let (status, _, reply) = fx.post("heartbeat", &heartbeat).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(
        matches!(reply, ServerMessage::HeartbeatAck { .. }),
        "{reply:?}"
    );
}

#[test]
fn retryable_errors_map_to_their_own_codes_and_statuses() {
    let unavailable = binary_error(
        None,
        None,
        ServerError::Unavailable("room restarting".to_string()),
    );
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.headers()[header::RETRY_AFTER], "1");

    let timeout = binary_error(None, None, ServerError::Timeout("slow room".to_string()));
    assert_eq!(timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(timeout.headers().get(header::RETRY_AFTER).is_none());
    assert_eq!(
        ServerError::Timeout(String::new()).to_error_code(),
        ErrorCode::Timeout
    );
}

#[tokio::test]
async fn commit_of_the_largest_frame_whose_log_record_does_not_fit_is_a_bad_request() {
    let fx = Fixture::new().await;
    fx.commit_ok(&fx.commit(1, 0)).await;

    // A commit request of exactly the largest frame: legal on the wire, but its log record
    // (fixed-width integers) is larger than the log's limit.
    let probe_len = 1 << 20;
    let overhead = encode_message(&fx.commit_op(2, 1, task_with_title(2, probe_len)))
        .unwrap()
        .len()
        - probe_len;
    let msg = fx.commit_op(2, 1, task_with_title(2, MAX_FRAME_SIZE - overhead));
    assert_eq!(encode_message(&msg).unwrap().len(), MAX_FRAME_SIZE);

    let (status, _, reply) = fx.post("commit", &msg).await;
    assert_bad_request(status, &reply);

    // Nothing changed and the room kept running: the next commit gets the next sequence.
    let (status, _, reply) = fx.post("commit", &fx.commit(3, 1)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(2)
    ));
}

#[tokio::test]
async fn commit_whose_small_ints_expand_past_the_log_limit_is_a_bad_request() {
    let fx = Fixture::new().await;

    // On the wire the operation fits comfortably in a request and in a reply...
    let probe = encode_message(&wide_row(1, 0)).unwrap().len();
    let op = wide_row(1, MAX_MESSAGE_SIZE as usize - 40_000 - probe);
    let wire_len = encode_message(&op).unwrap().len();
    assert!(wire_len < MAX_MESSAGE_SIZE as usize - 30_000);
    // ...but its small ints take 12 bytes each in the log record, which no longer fits.
    let mutation_id = MutationId::new([1; 16]);
    assert!(encode_wal_record(&SequencedOperation::new(1, op.clone()), Some(mutation_id)).is_err());

    let (status, _, reply) = fx.post("commit", &fx.commit_op(1, 0, op)).await;
    assert_bad_request(status, &reply);

    let (status, _, reply) = fx.post("commit", &fx.commit(2, 0)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(1)
    ));
}

#[tokio::test]
async fn register_rejects_another_message_kind_without_decoding_it() {
    let fx = Fixture::new().await;
    // A frame tagged as a Commit whose fields are garbage. Registration does not authenticate
    // the request before reading the message, so it must stop at the tag: decoding the rest
    // would answer "malformed" instead (and, with a crafted row, reserve its memory first).
    let mut frame = encode_message(&fx.commit(1, 0)).unwrap();
    frame.truncate(PROTOCOL_HEADER_LEN + 1);
    frame.extend_from_slice(&[0xFF; 64]);
    let request = Request::post(format!("/rooms/{}/register", fx.room_id))
        .body(Body::from(frame))
        .unwrap();
    let response = fx.app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let reply: ServerMessage = decode_message(&body).unwrap();
    assert_bad_request(status, &reply);
    assert!(
        matches!(&reply, ServerMessage::Error { message, room_id: Some(room), .. }
            if message.contains("Expected RegisterClient message") && *room == fx.room_id),
        "{reply:?}"
    );
}

/// Posts a registration frame of exactly `len` bytes (its token padded), without credentials.
async fn register_with_frame_of(fx: &Fixture, len: usize) -> (StatusCode, ServerMessage) {
    let register = |auth_token: String| ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: fx.room_id.clone(),
        client_id: fx.client_id.clone(),
        auth_token,
        current_seq: None,
        known_incarnation: None,
    };
    let probe = encode_message(&register("x".repeat(1024))).unwrap().len();
    let frame = encode_message(&register("x".repeat(1024 + len - probe))).unwrap();
    assert_eq!(frame.len(), len);
    let request = Request::post(format!("/rooms/{}/register", fx.room_id))
        .body(Body::from(frame))
        .unwrap();
    let response = fx.app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, decode_message(&body).unwrap())
}

#[tokio::test]
async fn registration_has_its_own_small_body_limit() {
    use crate::api::router::MAX_REGISTER_BODY_SIZE;
    let fx = Fixture::new().await;

    // At the limit the frame is read and decoded: the padded token is then rejected.
    let (status, reply) = register_with_frame_of(&fx, MAX_REGISTER_BODY_SIZE).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{reply:?}");

    // One byte more is rejected before it is buffered, as the general body limit does.
    let (status, reply) = register_with_frame_of(&fx, MAX_REGISTER_BODY_SIZE + 1).await;
    assert_error(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::BadRequest,
        &reply,
    );
}

#[tokio::test]
async fn commit_with_more_values_than_a_table_has_columns_is_rejected_when_decoded() {
    let fx = Fixture::new().await;
    // One Null more than any table can have columns: 64 KiB on the wire, rejected by the
    // decoder before the room sees it (the room would answer `SchemaViolation`).
    let table_id = schema().get_table_by_name("tasks").unwrap().table_id();
    let op = Operation::insert(
        table_id,
        PrimaryKey::single(Value::Int(1)),
        CompactRow::new(vec![Value::Null; MAX_COLUMNS + 1]),
        1000,
    );
    let (status, _, reply) = fx.post("commit", &fx.commit_op(1, 0, op)).await;
    assert_bad_request(status, &reply);
    assert!(
        matches!(&reply, ServerMessage::Error { message, .. } if message.contains("Malformed")),
        "{reply:?}"
    );

    // The room is untouched: the next commit gets the first sequence.
    let (status, _, reply) = fx.post("commit", &fx.commit(2, 0)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(1)
    ));
}

#[tokio::test]
async fn large_operations_are_split_across_catch_up_and_sync_batches() {
    let fx = Fixture::new().await;
    let six_mib = 6 * 1024 * 1024;
    for n in 1..=3u8 {
        fx.commit_ok(&fx.commit_op(n, n as u64 - 1, task_with_title(n as i64, six_mib)))
            .await;
    }

    // The catch-up of a commit from cursor 0 would carry 4 operations of 6 MiB: only as many
    // as fit in one frame are returned, flagged as partial.
    let (status, _, reply) = fx
        .post("commit", &fx.commit_op(4, 0, task_with_title(4, six_mib)))
        .await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    match reply {
        ServerMessage::CommitAck {
            assigned_seq,
            catchup_ops,
            has_more,
            ..
        } => {
            assert_eq!(assigned_seq, SequenceNumber::new(4));
            let seqs: Vec<u64> = catchup_ops.iter().map(|op| op.seq.get()).collect();
            assert_eq!(seqs, vec![1, 2]);
            assert!(has_more);
        }
        other => panic!("expected CommitAck, got {other:?}"),
    }

    // Sync pages through the same operations, at least one per batch.
    let mut from = 0;
    let mut pages = Vec::new();
    loop {
        let (status, _, reply) = fx.post("sync", &fx.sync(from)).await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
        let ServerMessage::SyncBatch { ops, has_more, .. } = reply else {
            panic!("expected SyncBatch, got {reply:?}");
        };
        assert!(!ops.is_empty());
        from = ops.last().unwrap().seq.get();
        pages.push(ops.iter().map(|op| op.seq.get()).collect::<Vec<_>>());
        if !has_more {
            break;
        }
    }
    assert_eq!(pages, vec![vec![1, 2], vec![3, 4]]);
}

#[tokio::test]
async fn responses_take_a_frame_of_the_clients_budget_before_they_are_built() {
    // A frame per client, and 1 MiB more.
    let fx = Fixture::with(|config| {
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64 + 1024 * 1024;
    })
    .await;
    let get_schema = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(2),
        room_id: fx.room_id.clone(),
    };
    // Unread responses hold what their encoding takes, so as many fit as the 1 MiB holds,
    // plus the one that reserved the last frame.
    let mut held = vec![fx.send("schema", &get_schema).await];
    let len = held[0].body().size_hint().exact().unwrap();
    while held.len() as u64 <= 1024 * 1024 / len {
        held.push(fx.send("schema", &get_schema).await);
    }
    assert!(held
        .iter()
        .all(|response| response.status() == StatusCode::OK));

    // Then nothing is: a snapshot chunk, before the relay reads it.
    let bounds = crate::relay::LogBounds {
        tail_seq: SequenceNumber::new(1),
        head_seq: SequenceNumber::new(1),
        unrestorable_through: None,
        room_incarnation: 0,
    };
    let hash = fx.stage_snapshot(1, std::future::ready(Ok(bounds))).await;
    let read = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_flag = std::sync::Arc::clone(&read);
    fail_point::arm_hook(
        "relay_read_chunk",
        &fx.dir.path().join("snapshots"),
        move || {
            read_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        },
    );
    let chunk = fx.first_chunk(Some(hash));
    for (endpoint, msg) in [
        ("schema", get_schema.clone()),
        ("register", fx.register_msg()),
        ("sync", fx.sync(0)),
        ("snapshot/chunk", chunk),
    ] {
        let (status, _, reply) = fx.post(endpoint, &msg).await;
        let expected = StatusCode::SERVICE_UNAVAILABLE;
        assert_error(status, expected, ErrorCode::Unavailable, &reply);
    }
    assert!(!read.load(std::sync::atomic::Ordering::SeqCst));
    // A commit, durable, is acknowledged without its catch-up, and so is its retry.
    for _ in 0..2 {
        let (status, _, reply) = fx.post("commit", &fx.commit(1, 0)).await;
        assert_eq!(status, StatusCode::OK);
        let ServerMessage::CommitAck {
            catchup_ops,
            has_more,
            ..
        } = reply
        else {
            panic!("expected CommitAck, got {reply:?}");
        };
        assert!(catchup_ops.is_empty() && has_more);
    }

    drop(held);
    let (status, _, reply) = fx.post("schema", &get_schema).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_waiting_for_its_room_holds_none_of_the_budget() {
    // One frame per client.
    let fx = Fixture::with(|config| {
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64;
    })
    .await;
    // The room's actor stalls, as on a slow fsync.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let room_dir = crate::actor::room::room_dir(fx.dir.path(), &fx.room_id);
    fail_point::arm_hook("room_shutdown", &room_dir, move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    let sender = fx.manager.get_or_spawn(&fx.room_id, None).await.unwrap();
    let (reply, _) = tokio::sync::oneshot::channel();
    sender.send(RoomCommand::Shutdown { reply }).await.unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    // A sync and a first chunk, which asks the room for its log range, wait for it.
    let chunk = |chunk_index, snapshot_hash| ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(3),
        room_id: fx.room_id.clone(),
        chunk_index,
        chunk_size: 0,
        snapshot_hash,
        room_incarnation: fx.incarnation,
    };
    let (sync, first_chunk) = (fx.sync(0), chunk(0, None));
    let waiting = futures::future::join(
        fx.send("sync", &sync),
        fx.send("snapshot/chunk", &first_chunk),
    );
    tokio::pin!(waiting);
    let wait = tokio::time::timeout(Duration::from_millis(200), &mut waiting).await;
    assert!(wait.is_err());

    // Without holding the client's part: an anchored chunk, which the relay answers without
    // the room, still gets its frame.
    let (status, _, reply) = fx.post("snapshot/chunk", &chunk(1, Some([0; 32]))).await;
    assert_error(
        status,
        StatusCode::CONFLICT,
        ErrorCode::SnapshotSuperseded,
        &reply,
    );
    release_tx.send(()).unwrap();
    waiting.await;
}

/// A sync answered `Unavailable` because the client's part of the budget is taken leaves no
/// trace: its cursor stays where the client's last commit left it, and once the held response
/// is released the same sync is served and only then moves it.
#[tokio::test]
async fn sync_without_budget_does_not_move_the_cursor() {
    // One frame per client.
    let fx = Fixture::with(|config| {
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64;
    })
    .await;
    for n in 1..=3u8 {
        fx.commit_ok(&fx.commit(n, u64::from(n) - 1)).await;
    }
    let cursor = || async {
        fx.manager
            .ask(&fx.room_id, |reply| RoomCommand::GetClientCursor {
                client_id: fx.client_id.clone(),
                reply,
            })
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(cursor().await, Some(SequenceNumber::new(2)));

    // An unread response holds the client's part.
    let get_schema = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(2),
        room_id: fx.room_id.clone(),
    };
    let held = fx.send("schema", &get_schema).await;
    assert_eq!(held.status(), StatusCode::OK);
    let (status, _, reply) = fx.post("sync", &fx.sync(3)).await;
    assert_error(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
        &reply,
    );
    assert_eq!(
        cursor().await,
        Some(SequenceNumber::new(2)),
        "a sync answered Unavailable moved the cursor"
    );

    drop(held);
    let (status, _, reply) = fx.post("sync", &fx.sync(3)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert_eq!(cursor().await, Some(SequenceNumber::new(3)));
}

#[test]
fn response_that_cannot_be_encoded_is_an_internal_error_frame() {
    let oversized = ServerMessage::SnapshotChunk {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room").unwrap(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: 1,
        snapshot_hash: [0; 32],
        data: vec![0u8; MAX_MESSAGE_SIZE as usize].into(),
    };
    let response = binary_response(StatusCode::OK, &oversized);
    let status = response.status();
    let body =
        futures::executor::block_on(axum::body::to_bytes(response.into_body(), 1 << 20)).unwrap();
    assert_error(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::Internal,
        &decode_message(&body).unwrap(),
    );
}

/// Posts the first chunk of a download, unanchored, with `meanwhile` run to completion right
/// after the relay picks the snapshot to serve, before it asks for the log range.
async fn first_chunk_while(
    fx: &std::sync::Arc<Fixture>,
    meanwhile: impl std::future::Future<Output = ()> + Send + 'static,
) -> ServerMessage {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let runtime = tokio::runtime::Handle::current();
    let snapshots = fx.dir.path().join("snapshots");
    fail_point::arm_hook("first_chunk_picked", &snapshots, move || {
        runtime.spawn(async move {
            meanwhile.await;
            let _ = done_tx.send(());
        });
        let done = tokio::task::block_in_place(|| done_rx.recv_timeout(Duration::from_secs(10)));
        assert!(done.is_ok());
    });
    fx.post("snapshot/chunk", &fx.first_chunk(None)).await.2
}

/// The first chunk checks its incarnation against the room it asks for the log range after
/// picking the snapshot: if the room was deleted and created again in between, it is
/// `RoomReset`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_chunk_of_a_room_created_again_after_its_snapshot_was_picked_is_room_reset() {
    let fx = std::sync::Arc::new(Fixture::new().await);
    fx.commit_ok(&fx.commit(1, 0)).await;
    fx.stage_snapshot(1, fx.manager.log_bounds(&fx.room_id))
        .await;
    let other = std::sync::Arc::clone(&fx);
    let reply = first_chunk_while(&fx, async move {
        let (manager, room_id) = (&other.manager, other.room_id.clone());
        manager.delete_room(&room_id).await.unwrap();
        let schema_id = SchemaId::new("tasks").unwrap();
        manager.create_room(room_id, schema_id, None).await.unwrap();
    })
    .await;
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::RoomReset,
                ..
            }
        ),
        "{reply:?}"
    );
}

/// The first chunk checks the snapshot it serves against the log range read after picking it:
/// one below a gap that a sync found in between is dropped instead of served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_chunk_does_not_serve_a_snapshot_below_a_gap_found_after_it_was_picked() {
    let fx = std::sync::Arc::new(Fixture::with(|config| config.ram_max_ops = 2).await);
    for n in 1..=6u8 {
        fx.commit_ok(&fx.commit(n, u64::from(n) - 1)).await;
    }
    fx.stage_snapshot(2, fx.manager.log_bounds(&fx.room_id))
        .await;
    let other = std::sync::Arc::clone(&fx);
    let reply = first_chunk_while(&fx, async move {
        let room = crate::actor::room::room_dir(other.dir.path(), &other.room_id);
        std::fs::remove_file(room.join("segments/segment_0000000000000003_0000000000000004.wal"))
            .unwrap();
        let (_, _, reply) = other.post("sync", &other.sync(2)).await;
        assert!(
            matches!(
                reply,
                ServerMessage::Error {
                    code: ErrorCode::BehindCompaction,
                    ..
                }
            ),
            "{reply:?}"
        );
    })
    .await;
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::RoomNotFound,
                ..
            }
        ),
        "{reply:?}"
    );
    assert_eq!(fx.relay.active_snapshot_seq(&fx.room_id), None);
}
