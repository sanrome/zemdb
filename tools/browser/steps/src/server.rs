//! The suite `server`: the test server builds the real router, and cuts the network between
//! the page and the router when asked to.

use crate::js::{describe, failed, scope, timer};
use js_sys::{Array, Promise, Reflect, Uint8Array};
use serde_json::json;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, ReadableStreamDefaultReader, Request, RequestInit, Response};
use zemdb_core::{
    decode_message, encode_message, ClientId, ClientMessage, CorrelationId, DataType, RoomId,
    Schema, ServerMessage, TableSchema,
};

/// The client of the steps.
const CLIENT: &str = "harness#page";

/// How long the cut of an open event stream may take to show, in milliseconds.
const CUT_WAIT_MS: i32 = 5000;

/// A request to the origin of the page, from the Worker.
async fn fetch(
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Result<Response, JsValue> {
    let init = RequestInit::new();
    init.set_method(method);
    let request_headers = Headers::new()?;
    for (name, value) in headers {
        request_headers.set(name, value)?;
    }
    init.set_headers(&request_headers);
    if let Some(body) = body {
        init.set_body(&Uint8Array::from(body));
    }
    let request = Request::new_with_str_and_init(path, &init)?;
    let scope = scope().map_err(JsValue::from)?;
    let response = JsFuture::from(scope.fetch_with_request(&request)).await?;
    Ok(response.unchecked_into())
}

async fn bytes(response: &Response) -> Result<Vec<u8>, JsError> {
    let buffer = response.array_buffer().map_err(failed("arrayBuffer"))?;
    let buffer = JsFuture::from(buffer)
        .await
        .map_err(failed("reading a body"))?;
    Ok(Uint8Array::new(&buffer).to_vec())
}

async fn text(response: &Response) -> Result<String, JsError> {
    Ok(String::from_utf8_lossy(&bytes(response).await?).into_owned())
}

/// `POST`s `body` as JSON to `path` of the harness; the body of a response with `expected`.
async fn harness(path: &str, body: serde_json::Value, expected: u16) -> Result<String, JsError> {
    let body = body.to_string();
    let response = fetch(
        "POST",
        path,
        &[("content-type", "application/json")],
        Some(body.as_bytes()),
    )
    .await
    .map_err(failed(path))?;
    let status = response.status();
    let text = text(&response).await?;
    if status != expected {
        return Err(JsError::new(&format!("{path}: HTTP {status}: {text}")));
    }
    Ok(text)
}

async fn set_network(up: bool) -> Result<(), JsError> {
    harness("/harness/network", json!({ "up": up }), 204).await?;
    Ok(())
}

fn room_of(run: &str) -> String {
    format!("room-{run}")
}

/// A token of the harness for the client of the steps in `room`.
async fn token(room: &str) -> Result<String, JsError> {
    harness(
        "/harness/token",
        json!({ "client_id": CLIENT, "room_id": room }),
        200,
    )
    .await
}

/// Registers the client of the steps in `room`; the error of `fetch` if the request fails.
async fn register(room: &str, token: &str) -> Result<Result<ServerMessage, JsError>, JsValue> {
    let message = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new(room).map_err(|err| JsError::new(&err.to_string()))?,
        client_id: ClientId::new(CLIENT).map_err(|err| JsError::new(&err.to_string()))?,
        auth_token: token.to_string(),
        current_seq: None,
        known_incarnation: None,
    };
    let frame = encode_message(&message).map_err(|err| JsError::new(&err.to_string()))?;
    let response = fetch(
        "POST",
        &format!("/rooms/{room}/register"),
        &[("content-type", "application/octet-stream")],
        Some(&frame),
    )
    .await?;
    let status = response.status();
    let body = bytes(&response).await?;
    Ok(decode_message::<ServerMessage>(&body)
        .map_err(|err| JsError::new(&format!("HTTP {status}: {err}"))))
}

/// The client of the steps registers in `room`.
async fn registers(room: &str, token: &str) -> Result<(), JsError> {
    let reply = register(room, token).await.map_err(failed("register"))??;
    match reply {
        ServerMessage::Registered { room_id, .. } if room_id.as_str() == room => Ok(()),
        reply => Err(JsError::new(&format!("register answered {reply:?}"))),
    }
}

/// The real router answers: a schema and a room created through the Control Plane, and the
/// client of the steps registered in the room with a token of the harness.
#[wasm_bindgen]
pub async fn server_register(run: String) -> Result<(), JsError> {
    set_network(true).await?;
    let schema_id = format!("tasks-{run}");
    let schema = Schema::builder()
        .table(
            TableSchema::builder("tasks")
                .primary_key("id", DataType::Int)
                .column("title", DataType::String),
        )
        .build();
    let schema = serde_json::to_value(schema).map_err(|err| JsError::new(&err.to_string()))?;
    harness(
        "/harness/admin/schemas",
        json!({ "schema_id": schema_id, "schema": schema }),
        201,
    )
    .await?;
    let room = room_of(&run);
    harness(
        "/harness/admin/rooms",
        json!({ "room_id": room, "schema_id": schema_id }),
        201,
    )
    .await?;
    registers(&room, &token(&room).await?).await
}

/// With the network down, an open event stream is cut and a request to the router fails as
/// without a network, while the harness still answers; with the network up again, the router
/// answers. Runs after `server_register`, in its room.
#[wasm_bindgen]
pub async fn server_offline(run: String) -> Result<(), JsError> {
    let room = room_of(&run);
    let token = token(&room).await?;
    registers(&room, &token).await?;
    let offline = offline(&room, &token).await;
    // The network comes back also when a check failed, for the steps that follow.
    set_network(true).await?;
    offline?;
    registers(&room, &token).await
}

async fn offline(room: &str, token: &str) -> Result<(), JsError> {
    let bearer = format!("Bearer {token}");
    let events = fetch(
        "GET",
        &format!("/rooms/{room}/events"),
        &[("authorization", &bearer)],
        None,
    )
    .await
    .map_err(failed("opening the event stream"))?;
    if events.status() != 200 {
        let status = events.status();
        return Err(JsError::new(&format!(
            "the event stream: HTTP {status}: {}",
            text(&events).await?
        )));
    }
    let body = events
        .body()
        .ok_or_else(|| JsError::new("the event stream has no body"))?;
    let reader: ReadableStreamDefaultReader = body.get_reader().unchecked_into();

    set_network(false).await?;
    stream_is_cut(&reader).await?;
    match register(room, token).await {
        Err(_) => Ok(()),
        Ok(reply) => Err(JsError::new(&format!(
            "with the network down, register got an answer: {}",
            match reply {
                Ok(reply) => format!("{reply:?}"),
                Err(err) => describe(&JsValue::from(err)),
            }
        ))),
    }
}

/// The open stream of `reader` fails, after the data that arrived before the cut, if any,
/// within [`CUT_WAIT_MS`].
async fn stream_is_cut(reader: &ReadableStreamDefaultReader) -> Result<(), JsError> {
    let timed_out = JsValue::from_str("timed out");
    loop {
        let timeout = timer(CUT_WAIT_MS, timed_out.clone())?;
        let read = Promise::race(&Array::of2(&reader.read().into(), &timeout.into()));
        let chunk = match JsFuture::from(read).await {
            Ok(chunk) => chunk,
            // The cut: a network error.
            Err(_) => return Ok(()),
        };
        if chunk == timed_out {
            return Err(JsError::new(
                "with the network down, the event stream stays open",
            ));
        }
        let done = Reflect::get(&chunk, &JsValue::from_str("done")).map_err(failed("read"))?;
        if done.is_truthy() {
            return Err(JsError::new(
                "with the network down, the event stream ends as if the server closed it",
            ));
        }
    }
}
