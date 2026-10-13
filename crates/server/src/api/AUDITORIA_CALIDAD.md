# Auditoría de calidad — `crates/server/src/api/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `serve.rs`, `extract.rs`, `data_plane.rs`, `control_plane.rs`, `auth.rs`, `relay.rs`, `sse.rs`, `router.rs`, `body_timeout.rs`, `mod.rs` y sus tests unitarios en `api/tests/` (`auth.rs`, `body_timeout.rs`, `control_plane.rs`, `data_plane.rs`, `serve.rs`, `sse.rs`).

## Resumen

- El módulo está bien escrito en general: los extractores validan en el borde, los handlers son lineales y `serve.rs`/`body_timeout.rs` son código sutil con una documentación que lo justifica. No hay código muerto relevante, ni referencias a códigos de auditoría, ni narrativa histórica en los comentarios.
- La mayor oportunidad es **la repetición en `data_plane.rs`**: seis handlers con el mismo esqueleto (`let-else` → closure `fail` → `ensure_payload_identity` en 6 líneas → `match ask_room(..)` con la rama `Err` idéntica). Dos helpers pequeños ahorran ~40 líneas sin cambiar ningún código HTTP ni cuerpo de error.
- Segunda oportunidad: **duplicación entre extractores de autenticación** (`extract.rs`) y la **construcción manual de `ServerMessage::Error`** en tres sitios (`data_plane.rs:52`, `extract.rs:314`, `router.rs:166`).
- Mejoras menores: `relay.rs` reimplementa `unexpected_message`, `sse.rs` repite `Some((Ok(event), rx))` en cada brazo, `is_body_read_timeout` es un `successors(..).any(..)`, `auth.rs` repite el cálculo de "ahora en segundos".
- **Ahorro estimado:** ~95–125 líneas en el código fuente (≈4–5 % de 2418) más ~40–60 en tests (≈5 % de 969). Ninguna sugerencia toca códigos HTTP, mensajes de error, la comparación en tiempo constante de `subtle`, ni el orden de las comprobaciones.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `serve.rs` | 533 | 398 | 86 | 4 | 45 | ~4 |
| `extract.rs` | 447 | 318 | 80 | 4 | 45 | ~20–25 |
| `data_plane.rs` | 407 | 360 | 20 | 0 | 27 | ~40–48 |
| `auth.rs` | 224 | 169 | 26 | 2 | 27 | ~8–10 |
| `relay.rs` | 194 | 168 | 15 | 0 | 11 | ~10–12 |
| `router.rs` | 175 | 134 | 23 | 5 | 13 | ~6–8 |
| `body_timeout.rs` | 169 | 125 | 30 | 0 | 14 | ~6–8 |
| `control_plane.rs` | 150 | 114 | 20 | 2 | 14 | ~0–1 |
| `sse.rs` | 103 | 76 | 17 | 3 | 7 | ~10–12 |
| `mod.rs` | 16 | 15 | 0 | 0 | 1 | 0 |
| **Subtotal fuente** | **2418** | **1877** | **317** | **20** | **204** | **~95–125** |
| `tests/data_plane.rs` | 516 | 448 | 8 | 19 | 41 | ~10–15 |
| `tests/control_plane.rs` | 122 | 111 | 0 | 4 | 7 | ~15–20 (fixture común) |
| `tests/serve.rs` | 108 | 91 | 1 | 6 | 10 | 0 |
| `tests/body_timeout.rs` | 84 | 72 | 2 | 3 | 7 | 0 |
| `tests/auth.rs` | 71 | 62 | 1 | 1 | 7 | 0 |
| `tests/sse.rs` | 68 | 62 | 3 | 0 | 3 | ~15 (fixture común) |
| **Subtotal tests** | **969** | **846** | **15** | **33** | **75** | **~40–50** |

## Comentarios

**Densidad y calidad.** La densidad es alta en `serve.rs` (16 % doc), `extract.rs` (18 %) y `body_timeout.rs` (18 %), y baja en `data_plane.rs` (5 %). En los tres primeros casi todo es *por qué* y contrato observable (códigos de rechazo, límites, cuándo se corta una conexión): **conservar**. No hay referencias a códigos de auditoría (C-xx, DEF-xx, M-xx) ni narrativa histórica.

**Qué recortar o reescribir:**

- **Tono de marketing en los doc-comments de handlers** de `data_plane.rs`: `:84` "Handshake endpoint delivering current head_seq and schema in 1 RTT", `:134` "Atomic commit with 1-RTT catchup deltas and monotonic sequencing", `:187` "Paginated batch synchronization with flow control", `:237` "Explicit client persistence acknowledgment triggering proactive log pruning", `:329` "On-demand schema retrieval during active DDL evolution", `:362` "Explicit client departure advancing retention window immediately". Varias superan los 100 caracteres (`:84`, `:134`, `:237`, `:362`) y describen más el sistema que el handler. Propuesta: una frase llana por handler (p. ej. `/// \`POST /rooms/:room_id/ack\`: records how far the client has persisted.`). Igual en `relay.rs:117` ("Handles multipart snapshot upload chunk streaming": no es multipart HTTP; confunde) y `control_plane.rs:52,104` ("Declares and registers a new schema template", "Explicitly provisions an isolated room"). Ahorro ~0 líneas, gana precisión.
- **Doc-comments que repiten la firma:** `router.rs:68` "Constructs a new AppState instance." (borrar); `auth.rs:13` "Axum extractor that validates administrative Bearer token authentication." podría decir lo que no dice la firma: que exige el prefijo `Bearer ` (a diferencia de `extract::bearer_token`, donde es opcional) y que responde JSON 401.
- **Reflujo de líneas cortadas a mitad de frase** (párrafos editados sin reflujar): `extract.rs:191-195` ("a frame of another protocol\n/// version is …"), `router.rs:94-97` ("for\n/// registration) and must arrive in full within the\n/// configured …"), `data_plane.rs:73-75` ("`Timeout` (see\n/// [`RoomManager::ask`]…)"). Ahorro ~3 líneas.
- **`extract.rs:265-267`**: tres líneas para justificar `#[allow(clippy::result_large_err)]`. Basta una: `// Same rejection type as every extractor here; runs once per request.` Ahorro 2 líneas.
- **`sse.rs:16-26`**: el doc de `room_events` es un bloque de 11 líneas sin separación de párrafos que mezcla autenticación, errores y la lista de eventos. Conviene un `///` en blanco entre "autenticación/errores" y "eventos emitidos" (suma 1 línea, mejora la lectura). La lista de eventos es contrato con los clientes: conservar.
- **Falta `//!` de módulo** en `data_plane.rs`, `auth.rs`, `control_plane.rs`, `router.rs` y `sse.rs` (sí lo tienen `serve.rs`, `extract.rs`, `relay.rs`, `body_timeout.rs`). Opcional; sumaría 1–2 líneas por archivo.
- **Estado:** ✅ aplicada (paso 1), salvo los `//!` de módulo: son opcionales, suman líneas y no corrigen nada, así que no se agregaron.

**Qué conservar (ejemplos):** `serve.rs:39-62` (orden de apagado, garantía de durabilidad, límites por conexión), `serve.rs:201-209` y `227-240` (por qué frames pequeños y la semántica de "idle"), `serve.rs:321-323` (`notify_one` con un único waiter), `body_timeout.rs:1-10` y `93-95` (algoritmo tipo `mod_reqtimeout` y por qué el timer se mueve como mucho una vez por espera), `router.rs:85-89` (por qué `register` tiene su propio límite de body), `auth.rs:72-73` y `124-127` (prefijo de longitud y expiry canónico: son razonamientos de seguridad), `control_plane.rs:80-82` y `95-96` (por qué la recarga de schema corre en su propia tarea), `relay.rs:179-180` (por qué `run_to_completion`), `extract.rs:66-73` (por qué no se acepta el token en la URL).

## Sugerencias

### Q1. Colapsar el esqueleto repetido de los handlers de `data_plane.rs`
- **Dónde:** `data_plane.rs:151-184`, `203-234`, `252-280`, `298-326`, `342-359`, `376-402` (y `100-131` en `register`).
- **Problema:** cada handler repite (a) `ensure_payload_identity(&auth.room_id, Some(&auth.client_id), &room_id, Some(&client_id))` en 6 líneas y (b) `match ask_room(..).await { Ok(x) => binary_response(StatusCode::OK, &Msg{..}), Err(err) => fail(room_id, err) }`, con la rama `Err` y el `StatusCode::OK` idénticos en los 7.
- **Propuesta:** dos helpers privados.
  ```rust
  impl AuthenticatedRoom {
      /// Checks the room and client named in the payload against the path and the token.
      pub(crate) fn ensure_payload(&self, room_id: &RoomId, client_id: Option<&ClientId>) -> Result<(), ServerError> {
          ensure_payload_identity(&self.room_id, Some(&self.client_id), room_id, client_id)
      }
  }

  /// 200 with the frame built from `result`, or the error frame of the request.
  fn respond<R>(
      correlation_id: CorrelationId,
      room_id: RoomId,
      result: Result<R, ServerError>,
      ok: impl FnOnce(RoomId, R) -> ServerMessage,
  ) -> Response {
      match result {
          Ok(value) => binary_response(StatusCode::OK, &ok(room_id, value)),
          Err(err) => binary_error(Some(correlation_id), Some(room_id), err),
      }
  }
  ```
  Cada handler queda (ej. `ack`):
  ```rust
  if let Err(err) = auth.ensure_payload(&room_id, Some(&client_id)) {
      return fail(room_id, err);
  }
  let result = ask_room(&state, &room_id, |reply| RoomCommand::Ack { client_id, ack_seq, reply }).await;
  respond(correlation_id, room_id, result, |room_id, head_seq| ServerMessage::AckConfirmed {
      correlation_id, room_id, ack_seq, head_seq,
  })
  ```
  (Se llama `respond` y no `reply` para no chocar con el parámetro `reply` de los closures de `RoomCommand`.) El orden de efectos no cambia (identidad → actor → respuesta), `room_id` se sigue moviendo sin clones y los mensajes/códigos son los mismos. `get_schema` y `register` usan `ensure_payload_identity` con `None` y siguen igual en esa parte. Variante si se prefiere (a verificar): mover `ensure_payload` como método de `AuthenticatedRoom` en `extract.rs`, junto a `ensure_payload_identity`.
- **Ahorro estimado:** ~35–45 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — los 7 handlers usan `StatusCode::OK` y la misma rama `Err(err) => fail(room_id, err)` (`data_plane.rs:130,183,233,279,325,358,401`); `correlation_id`/`mutation_id` ya se usan dos veces hoy (son `Copy`), así que capturarlos en `ok` compila. `ensure_payload` aplica a 5 handlers (commit, sync, ack, heartbeat, deregister).

### Q2. Extraer la autenticación común de `AuthenticatedRoom`, `EventStreamAuth` y `RelayAuth`
- **Dónde:** `extract.rs:88-99`, `122-134`, `161-183`, `441-447`.
- **Problema:** los tres extractores repiten `RoomPath::from_request_parts` + `AppState::from_ref` + `.ok_or_else(|| unauthorized(&room_id, "Missing Authorization header".to_string()))` + `verify_token_for_room(..).map_err(|err| binary_error(None, Some(room_id.clone()), err))`. `unauthorized` recibe un `String` pero solo se llama con ese literal.
- **Propuesta:**
  ```rust
  /// Client authenticated by `token` for `room_id`, or the binary rejection.
  fn authenticate_client(room_id: &RoomId, token: Option<&str>, state: &AppState) -> Result<ClientId, Response> {
      let token = token.ok_or_else(|| missing_token(room_id))?;
      verify_token_for_room(token, room_id, state).map_err(|err| binary_error(None, Some(room_id.clone()), err))
  }
  fn missing_token(room_id: &RoomId) -> Response {
      binary_error(None, Some(room_id.clone()), ServerError::Unauthorized("Missing Authorization header".to_string()))
  }
  ```
  `AuthenticatedRoom` y `EventStreamAuth` pasan a 3 líneas cada uno (`bearer_token(parts)` vs `bearer_token(parts).or_else(|| query_token(parts))`). `RelayAuth` usa `missing_token` y el mismo `map_err` en la rama de cliente; **su comparación `ct_eq` con el admin secret queda intacta**.
- **Ahorro estimado:** ~10–12 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — los tres extractores usan el mismo literal "Missing Authorization header" y el mismo `map_err`; `unauthorized` solo se llama en `extract.rs:93,128,166`; el orden (falta de token → 401 antes de verificar) y el `ct_eq` de `RelayAuth` no cambian.

### Q3. Un único constructor de `ServerMessage::Error`
- **Dónde:** `data_plane.rs:52-57`, `extract.rs:314-322` (`body_rejection`), `router.rs:166-174` (`unmatched`).
- **Problema:** los tres construyen a mano `ServerMessage::Error { correlation_id, room_id, code: err.to_error_code(), message: err.to_string() }`. `body_rejection` y `unmatched` no usan `binary_error` porque necesitan otro status.
- **Propuesta:** en `data_plane.rs`
  ```rust
  pub(crate) fn error_frame(correlation_id: Option<CorrelationId>, room_id: Option<RoomId>, err: &ServerError) -> ServerMessage {
      ServerMessage::Error { correlation_id, room_id, code: err.to_error_code(), message: err.to_string() }
  }
  ```
  y usarlo en los tres (`binary_response(status, &error_frame(None, room_id, &err))`). Es idéntico byte a byte; no se recomienda en cambio "llamar a `binary_error` y pisar el status" (como hace `AdminJson`), porque en la rama de fallo de codificación de `binary_response` el 500 quedaría pisado. `add_headers` solo actúa con 503, que ninguno de estos dos sitios produce, así que no hay diferencia de cabeceras.
- **Ahorro estimado:** ~8–10 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q4. `relay.rs`: reutilizar `unexpected_message` y no clonar `state` de más
- **Dónde:** `relay.rs:90-94`, `134-138`, `31`, `155`.
- **Problema:** `binary_error(None, Some(room_id), ServerError::BadRequest("Expected RequestSnapshotChunk message".to_string()))` es exactamente `unexpected_message(Some(room_id), "RequestSnapshotChunk")` (mismo formato `"Expected {expected} message"`, mismo status, mismos `None`). Además, en `upload_snapshot` y `upload_chunk` el `State(state)` no se usa después de crear la tarea, así que `let task_state = state.clone();` sobra: basta mover `state` al `async move`.
- **Propuesta:** `else { return unexpected_message(Some(room_id), "RequestSnapshotChunk"); };` (idem `UploadSnapshotChunk`), y usar `state` directamente en las dos tareas.
- **Ahorro estimado:** ~10 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1)

### Q5. `sse.rs`: un solo `Some((Ok(event), rx))` en el `unfold`
- **Dónde:** `sse.rs:37-66`.
- **Problema:** cada brazo construye el evento y repite `Some((Ok(event), rx))`.
- **Propuesta:**
  ```rust
  let event = match rx.recv().await {
      Ok(RoomEvent::HeadAdvanced(seq)) => Event::default().event("head_advanced").data(seq.to_string()),
      Ok(RoomEvent::SchemaReloaded(schema_id)) => Event::default().event("schema_reloaded").data(&schema_id),
      // An empty data line: browsers drop events without one.
      Ok(RoomEvent::SnapshotWanted) => Event::default().event("snapshot_wanted").data(""),
      Ok(RoomEvent::SnapshotAvailable(seq)) => Event::default().event("snapshot_available").data(seq.to_string()),
      Err(broadcast::error::RecvError::Lagged(_)) => Event::default().event("lagged").data("true"),
      Err(broadcast::error::RecvError::Closed) => return None,
  };
  Some((Ok::<_, Infallible>(event), rx))
  ```
  (`return None` dentro del bloque `async move` sale del bloque, igual que hoy.) De paso, nombrar el `15` de `sse.rs:72` como `const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);` (suma 1–2 líneas; constante mágica).
- **Ahorro estimado:** ~10–12 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1)

### Q6. `is_body_read_timeout` con `iter::successors`
- **Dónde:** `body_timeout.rs:79-88`.
- **Propuesta:**
  ```rust
  pub(crate) fn is_body_read_timeout(err: &(dyn Error + 'static)) -> bool {
      std::iter::successors(Some(err), |err| err.source()).any(|err| err.is::<BodyReadTimeout>())
  }
  ```
  Mismo recorrido de la cadena de `source()`, mismo resultado.
- **Ahorro estimado:** ~6 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1). El closure tiene que desestructurar la referencia (`|&err| err.source()`): con `|err|` el préstamo de `source()` queda atado al del closure y no compila.

### Q7. `auth.rs`: helper para la hora actual y comprobación de sala compartida
- **Dónde:** `auth.rs:96-99` y `173-176`; `auth.rs:212-217` y `extract.rs:432-437`.
- **Problema:** (a) `SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()` aparece dos veces. (b) La comprobación "Token room mismatch: expected {}, got {}" está duplicada entre `verify_client_token_bound` y `extract::verify_token_for_room`.
- **Propuesta:** (a) `fn unix_now_secs() -> u64`. (b) en `auth.rs`, `pub(crate) fn ensure_token_room(verified: &VerifiedClientToken, room_id: &RoomId) -> Result<(), ServerError>`, usado por `verify_client_token_bound` **después** de la comprobación de cliente (para conservar qué mensaje gana si fallan ambos) y por `verify_token_for_room`. No toca la verificación de firma ni el `ct_eq`.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q8. Argumentos de formato en línea
- **Dónde:** `serve.rs:95-100`, `auth.rs:206-209`, `213-216`, `extract.rs:433-436`.
- **Problema:** `format!("... within {:?}", grace)` y `format!("... expected {}, got {}", room_id, verified.room_id)` ocupan 4–6 líneas por rustfmt.
- **Propuesta:** `format!("Graceful shutdown did not finish within {grace:?}")` entra en una línea; `format!("Token room mismatch: expected {room_id}, got {}", verified.room_id)` ahorra 1–2 líneas por sitio. Mensajes idénticos.
- **Ahorro estimado:** ~5–6 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1)

### Q9. Menores
- `router.rs:107-108`: `.route("/rooms/:room_id", get(control_plane::get_room).delete(control_plane::delete_room))` en vez de dos `.route` al mismo path (axum 0.7 los fusiona igual). ~1 línea. **Riesgo:** bajo.
- `serve.rs:199`: `MAX_CONCURRENT_STREAMS` es `pub` pero nadie fuera de `serve.rs` lo usa (grep en `crates/`) ni se reexporta en `mod.rs`; podría ser privado. **A verificar** si se quiere documentar como parte de la API pública.
- `relay.rs:27-30`: `parse_snapshot_head_seq` podría devolver ya `SequenceNumber` y el handler usar `let-else`/`map`, ~1 línea. Baja.
- `extract.rs:441`: `unauthorized(room_id, message: String)` desaparece con Q2.
- **Ahorro estimado conjunto:** ~3–4 líneas · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1) en la ruta `/rooms/:room_id` y en `parse_snapshot_head_seq`. Hacer privado `MAX_CONCURRENT_STREAMS` es un cambio de API pública (paso 4), y `unauthorized` desaparece con Q2 (paso 2).

## Lo que conviene dejar como está

- **`serve.rs` completo.** `Activity`/`TrackedIo`/`TrackedBody`/`ProtocolDetection` son largos pero cada pieza resuelve un problema distinto (idle de HTTP/2, detección de protocolo antes de que hyper lo sepa, frames pequeños para medir el ritmo del cliente). Los doc-comments largos (`:39-62`, `:227-240`) son la única especificación de esos comportamientos y los tests de `tests/serve.rs` y `graceful_shutdown_tests.rs` los ejercitan. Las variantes `idle_for(SHUTDOWN_LINGER, true)` / `idle_for(header_read_timeout, false)` (`:278-282`) no se acortan con una tupla.
- **Comparaciones en tiempo constante** (`auth.rs:37-41`, `auth.rs:160-166`, `extract.rs:169-173`): aunque `unwrap_u8() == 1` se repite tres veces y un helper lo deduplicaría, por instrucción no se tocan. Los comentarios "Constant-time comparison…" también se quedan.
- **`RegisterMessage::from_request`** (`extract.rs:236-260`): la lista de campos aparece dos veces (patrón y struct), pero el `peek` previo es una decisión de seguridad (no decodificar el resto de un frame no autenticado) y un `let-else` no ahorra líneas reales.
- **`AdminJson`** (`extract.rs:362-380`): pisa el status de la respuesta JSON; es la forma más corta de conservar 413/415 con el cuerpo `BadRequest`.
- **`ask_room`** (`data_plane.rs:76-82`): wrapper de una línea, pero aplana `Result<Result<..>>` en seis sitios y documenta los errores del intercambio.
- **`router.rs`**: no anidar las rutas bajo `/rooms/:room_id` con `nest` para ahorrar el prefijo: cambiaría cómo actúan `method_not_allowed_fallback` y `fallback` (y por tanto los 404/405 binarios que comparan los tests de integración).
- **`control_plane.rs`**: el `room_exists` previo en `get_room` (`:124`) responde `RoomNotFound` sin pasar por `ask` (a verificar si además evita abrir el actor; en cualquier caso no es redundante a simple vista); los `Serialize` de los request structs se usan en `tests/server_integration_tests.rs`. Nada que recortar.
- **`body_timeout.rs::poll_deadline` y `DeadlineBody::poll_frame`**: el `if let` anidado de `:138-143` podría aplanarse con combinadores, pero quedaría menos legible para ahorrar 2 líneas.

## Tests

- **Fixture repetida en tres archivos:** `tests/data_plane.rs:78-112`, `tests/control_plane.rs:43-88` y `tests/sse.rs:14-31` construyen igual `ServerConfig` → `SchemaRegistry` → `SnapshotRelay` (con los mismos `Duration::from_secs(60)` y `max_snapshot_bytes`) → `RoomManager` → `AppState`. Un helper `fn test_state(dir: &Path, config: ServerConfig) -> AppState` (o que devuelva también `registry`/`manager`, que `control_plane` y `sse` necesitan) ahorraría ~15–20 líneas por archivo. Como los tests de cada archivo son módulos `#[path]` distintos, el helper requiere un módulo `#[cfg(test)]` compartido en el crate (no existe hoy): **a verificar** si compensa crearlo solo para esto.
- `tests/data_plane.rs` está bien factorizado (`Fixture::post`, `commit`, `sync`, `commit_ok`, `assert_bad_request`). El `assert_bad_request` (`:187-200`) podría generalizarse a `assert_error(status, code, reply)` porque otros tests comparan códigos a mano con `matches!` (`:223`, `:257`, `:408`, `:512`); ahorro ~10 líneas, prioridad baja.
- `tests/serve.rs`, `tests/body_timeout.rs`, `tests/auth.rs`: compactos, con tiempo pausado y helpers (`idles_within`, `drip`, `signed_token`); nada que recortar. `tests/auth.rs:63-70` ya usa tabla de casos.
