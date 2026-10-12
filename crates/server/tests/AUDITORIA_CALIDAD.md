# Auditoría de calidad — `crates/server/tests/` (parte A: HTTP e integración general)

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `server_integration_tests.rs`, `http_edge_tests.rs`, `http_error_codes_tests.rs`, `id_validation_http_tests.rs`, `auth_tests.rs`, `admin_schema_http_tests.rs`

## Resumen

- **No existe `tests/common/mod.rs`** (ni ningún `mod common`). Cinco de los seis archivos definen su propio `TestServer` + `start_server` con el mismo armado de `ServerConfig` → `SchemaRegistry` → `SnapshotRelay` → `RoomManager` → `AppState` → listener en `127.0.0.1:0` (44–70 líneas cada uno). Además `assert_binary_error`, el token de cliente, el `RegisterClient` y el frame `Heartbeat` están copiados casi literalmente entre `http_error_codes_tests.rs` e `id_validation_http_tests.rs`. Un módulo común (compartido también con la parte B: hay 14 construcciones de `SnapshotRelay::new` en `tests/`) es la oportunidad estructural más clara.
- **`server_integration_tests.rs` es el gran foco**: ~37 POST de `ClientMessage` armados a mano (`.post(format!("{}/rooms/{}/…")) .header(AUTHORIZATION…) .header(CONTENT_TYPE…) .body(encode_message(…)) .send().await.unwrap()`, 8–12 líneas cada uno), ~13 requests admin igual de verbosos, ~14 preludios casi idénticos «registrar schema + crear room» y 16 `generate_client_token(…, Duration::from_secs(300), &server.config.auth_secret)` de 6 líneas. El propio archivo ya tiene el helper `TestServer::post_message` (`:2004`), pero sólo lo usan los dos últimos tests.
- **Tests redundantes entre archivos**: tres tests de `id_validation_http_tests.rs` repiten exactamente (mismo endpoint, mismo input, mismo código esperado) casos que ya están en `http_error_codes_tests.rs`; no prueban validación de ids. Se pueden fusionar sin perder ningún caso adversario.
- **Ahorro estimado: ~750–1000 líneas netas de 4428 (≈17–23 %)**, ya descontando las ~120 líneas que sumaría `tests/common/mod.rs`. Casi todo sale de `server_integration_tests.rs` (~550–750, ≈25–33 % del archivo).
- `http_edge_tests.rs` y `auth_tests.rs` están bien escritos; sólo admiten retoques menores. Los comentarios del `//!` de `http_edge_tests.rs` y los «por qué» de los tests de tokens hay que conservarlos.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `server_integration_tests.rs` | 2279 | 2026 | 2 | 93 | 158 | ~550–750 |
| `http_edge_tests.rs` | 717 | 605 | 36 | 26 | 50 | ~30–40 |
| `http_error_codes_tests.rs` | 419 | 361 | 5 | 14 | 39 | ~60–75 |
| `id_validation_http_tests.rs` | 608 | 536 | 2 | 10 | 60 | ~150–200 |
| `auth_tests.rs` | 171 | 137 | 0 | 11 | 23 | ~15–25 |
| `admin_schema_http_tests.rs` | 234 | 208 | 5 | 2 | 19 | ~35–40 |
| `common/mod.rs` (nuevo) | — | — | — | — | — | +100–130 |
| **Total** | **4428** | **3873** | **50** | **156** | **349** | **~750–1000 netas** |

## Comentarios

- **Densidad baja en general y sin códigos de auditoría** (no hay `C-xx`, `DEF-xx`, `M-xx` ni referencias a GEMINI.md en estos seis archivos).
- **`server_integration_tests.rs`: 93 líneas de comentario, casi todas pasos numerados que repiten el assert de abajo.** Ej. `:115` `// 1. Missing Authorization header -> 401 Unauthorized`, `:128`, `:142`, `:157`, `:208`, `:225`, `:240`, `:252`, `:262`, `:372`, `:391`, `:429`, `:459`, `:829`/`:840`/`:880`, `:1152`/`:1171`/`:1189`/`:1206`/`:1222`/`:1238` («X without Authorization header -> 401»). Con los helpers de Q1–Q3 los pasos quedan en 1–3 líneas y el comentario sobra o se reduce a la parte no obvia (p. ej. `:1496` «EventSource compatibility» sí aporta). En el test de catchup, `:1893` y `:1900` («Catchup must contain sequences 3, 4, 5 and 6» / «Verify strict monotonicity…») duplican el mensaje del assert.
- **Narrativa histórica**: `server_integration_tests.rs:1406` el nombre `test_data_plane_dev_token_backdoor_eliminated` y `room-backdoor-test` (`:1414`) cuentan la historia de un fix; mejor `dev_token_is_not_accepted`. Igual en `auth_tests.rs:35` `// Dev backdoor elimination test` → `// "dev-token" is not a valid token`. `server_integration_tests.rs:1924` «Now client_gamma…» es narrativa de paso, aceptable.
- **`auth_tests.rs:95-96`** («A token in the previous … format… must no longer be accepted») es historia, pero es justamente lo que el test protege (que el formato viejo siga rechazado): conservar, se puede acortar a 1 línea.
- **Para conservar**: el `//!` de `http_edge_tests.rs:1-6` (por qué reloj real y no pausado), `http_edge_tests.rs:565-573` (dimensionado de `LARGE_ROWS` y `SHUTDOWN_DURING_RESPONSE`), `:688-689` y los comentarios de pacing (`:594`, `:624-625`, `:660`); `http_error_codes_tests.rs:158-159` (por qué el error va en la versión del server); `id_validation_http_tests.rs:115-117` (por qué `%2F` y no `..`), `:292`, `:531`/`:542`/`:555` (por qué el token en la URL sólo vale para SSE); `auth_tests.rs:132-133` y `:161-162` (por qué importan la frontera cliente/room y la expiración canónica); `server_integration_tests.rs:293`, `:905`, `:2151`, `:2187`.

## Sugerencias

### Q1. Usar en todo `server_integration_tests.rs` el helper `post_message` que ya existe (y hermanos)
- **Dónde:** `server_integration_tests.rs` — el helper está en `:2004-2022`; los POST de `ClientMessage` inline están en `:383, 408, 442, 472, 526, 542, 572, 584, 610, 649, 712, 731, 765, 843, 862, 985, 1057, 1105, 1163, 1181, 1198, 1214, 1230, 1283, 1302, 1354, 1374, 1395, 1433, 1452, 1770, 1790, 1810, 1833, 1875, 1936, 1968`.
  (El upload de snapshot de `:953-966` manda bytes crudos con `x-snapshot-head-seq` y queda fuera.)
- **Problema:** cada request repite 8–12 líneas (`.client.post(format!(…)).header(AUTHORIZATION, format!("Bearer {}", t)).header(CONTENT_TYPE, "application/octet-stream").body(encode_message(&m).unwrap()).send().await.unwrap()`) más 5–8 de `match decode_message(...) { … other => panic!(…) }`. `post_message` hace exactamente eso, pero vive al final del archivo y sólo lo usan `test_snapshot_demand_signals_over_http_and_sse` y `test_register_with_a_cursor_beyond_the_head…`.
- **Propuesta:** mover el `impl TestServer` de `:2003-2048` junto a `start()` y añadir una variante que devuelva la `reqwest::Response` cruda (para los casos 401/403 que no esperan un frame decodificable) con `token: Option<&str>`:
  ```rust
  async fn post_raw(&self, room: &RoomId, endpoint: &str, token: Option<&str>, msg: &ClientMessage) -> reqwest::Response
  // post_message = post_raw(..., Some(token), ...) + decode
  ```
  Antes (`:420-445`, 26 líneas) → después:
  ```rust
  let (status, ack) = server.post_message(&room_id, "commit", &token, &commit_msg1).await;
  assert_eq!(status, StatusCode::OK);
  assert!(matches!(ack, ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(1)), "{ack:?}");
  ```
  Ojo: varios POST de setup descartan la respuesta sin mirar el status (`:524-531`, `:540-555`, `:571-578`, `:584-598`); pasarlos por un helper que haga `assert_eq!(status, OK)` es más estricto (**a verificar** que todos respondan 200; deberían, porque los pasos siguientes dependen de ellos). El `CONTENT_TYPE` explícito no lo exige el server (los otros archivos no lo mandan), pero mantenerlo dentro del helper conserva el request idéntico.
- **Ahorro estimado:** ~250–300 líneas · **Riesgo:** bajo · **Prioridad:** alta

### Q2. Helpers de preludio en `server_integration_tests.rs`: `room_with_schema`, `token`, `register`
- **Dónde:** preludio «register_schema + create_room» en `:354-368`, `:491-505`, `:685-700`, `:805-820`, `:1030-1045`, `:1135-1148`, `:1249-1262`, `:1313-1332`, `:1406-1419`, `:1463-1476`, `:1721-1735`, `:2069-…`, `:2223-…`; `generate_client_token(&c, &r, Duration::from_secs(300), &server.config.auth_secret)` (6 líneas) 16 veces; `ClientMessage::RegisterClient { … }` + POST 14 veces.
- **Problema:** 12–15 líneas de preludio idénticas por test, más 6 por token y ~15 por registro.
- **Propuesta:**
  ```rust
  impl TestServer {
      /// Registers `create_test_schema()` as `schema_id` and creates `room_id` with it.
      async fn room_with_schema(&self, schema_id: &str, room_id: &str) -> (SchemaId, RoomId) { … }
      fn token(&self, client: &ClientId, room: &RoomId) -> String { /* 300 s */ }
      /// Registers `client` and returns its token and the `Registered` reply.
      async fn register(&self, room: &RoomId, client: &ClientId) -> (String, ServerMessage) { … }
  }
  ```
  `:2069-…` crea la room con `Some(small_segments)`: el helper debe aceptar el `lifecycle: Option<…>` (o ese test conserva su `create_room`).
  El `correlation_id` sólo se compara en `:2264` (`test_register_with_a_cursor_beyond_the_head…`, espera `7`); ese test conserva su mensaje armado a mano y el helper puede fijar uno para el resto. Los tests que prueban registros fallidos (`:372-389`, `:1423-1438`) siguen armando su mensaje a mano vía `post_raw`.
- **Ahorro estimado:** ~200–250 líneas · **Riesgo:** bajo · **Prioridad:** alta

### Q3. Helper `admin()` para el control plane en `server_integration_tests.rs`
- **Dónde:** `:115-153`, `:157-187`, `:209-268`, `:274-350`, `:1522-1549`, `:1569-1617`.
- **Problema:** `format!("Bearer {}", server.config.admin_secret)` + `.header(AUTHORIZATION, &auth_header)` + `.json(&…).send().await.unwrap()` repetido ~13 veces; los otros archivos ya usan `.bearer_auth(ADMIN_SECRET)`.
- **Propuesta:** `fn admin(&self, method: Method, path: &str) -> reqwest::RequestBuilder` que arme URL + `bearer_auth(admin_secret)`. Los casos 1–2 de `test_control_plane_schema_crud_and_auth` (`:114-139`, sin header / bearer inválido) quedan como una tabla `for auth in [None, Some("invalid_secret_token")]`. Los pasos 1–2 y 4–5 de `test_control_plane_room_lifecycle` (`:208-268`) son el mismo request dos veces con status distinto: 2 líneas cada uno.
- **Ahorro estimado:** ~80–100 líneas · **Riesgo:** bajo · **Prioridad:** alta

### Q4. Crear `tests/common/mod.rs` con el arranque del server y los helpers binarios compartidos
- **Dónde:** `server_integration_tests.rs:17-88`, `http_error_codes_tests.rs:20-73`, `id_validation_http_tests.rs:19-79`, `admin_schema_http_tests.rs:24-74`, `http_edge_tests.rs:37-112`; duplicados exactos: `assert_binary_error` (`http_error_codes_tests.rs:125-138` ≡ `id_validation_http_tests.rs:176-190`, difiere sólo el mensaje del assert), token de 300 s (`http_error_codes_tests.rs:83-89` ≡ `id_validation_http_tests.rs:155-162`), frame `Heartbeat` (`http_error_codes_tests.rs:140-147` ≡ `id_validation_http_tests.rs:462-469`), `RegisterClient` (`http_error_codes_tests.rs:103-122` ≈ `id_validation_http_tests.rs:444-460`).
- **Problema:** cinco copias del mismo cableado; cualquier cambio en `AppState::new`/`RoomManager::new` obliga a tocar todos los archivos (y los de la parte B).
- **Propuesta:**
  ```rust
  // tests/common/mod.rs
  #![allow(dead_code)] // cada archivo de tests/ es un crate y usa sólo una parte
  pub const ADMIN_SECRET: &str = "…"; pub const AUTH_SECRET: &str = "…";
  pub struct Parts { pub dir: TempDir, pub data_dir: PathBuf, pub config: Arc<ServerConfig>,
                     pub schema_registry: Arc<SchemaRegistry>, pub room_manager: Arc<RoomManager>, pub state: AppState }
  pub fn build(configure: impl FnOnce(&mut ServerConfig)) -> Parts { … }
  pub struct TestServer { pub base_url, pub client, pub parts… }
  pub async fn serve(parts: Parts) -> TestServer { /* axum::serve en 127.0.0.1:0 */ }
  pub fn tasks_schema() -> Schema; pub fn token(c, r) -> String;
  pub fn heartbeat(c, r) -> Vec<u8>; pub fn register_frame(c, r, token) -> Vec<u8>;
  pub async fn assert_binary_error(resp, status, code); pub async fn assert_json_error(resp, status, code);
  ```
  `http_edge_tests.rs` reutiliza sólo `build` (sirve con `serve_until_shutdown`, no con `axum::serve`, y llama a `config.validate()` — eso queda en su archivo). Los secretos por archivo (`"errors_…"`, `"schema_…"`, `"edge_…"`) no los compara ningún test (grep: sólo se usan vía la constante o `server.config.*`), así que se pueden unificar (tomar los de `http_edge_tests.rs`, que ya pasan `config.validate()`; si alguna vez se llama a `validate_secrets()` (`src/config.rs:288`), revisar que no coincidan con los defaults). `server_integration_tests.rs` fija campos que ya son el default o que no influyen (`host`, `port: 0` — el listener se bindea aparte —, `lease_timeout_secs`, etc.; **a verificar** contra `ServerConfig::default()` antes de quitarlos, en especial `dormant_after_secs: None` y `dedup_lru_capacity: 1000`). Coordinar con la parte B para que `snapshot_relay_tests.rs`, `commit_semantics_tests.rs`, `graceful_shutdown_tests.rs`, etc. usen el mismo módulo.
- **Ahorro estimado:** ~150–200 líneas brutas en estos seis archivos, ~+110 en `common/mod.rs` (más ahorro en la parte B) · **Riesgo:** bajo · **Prioridad:** alta

### Q5. Fusionar los tests de `id_validation_http_tests.rs` que no prueban ids y ya están en `http_error_codes_tests.rs`
- **Dónde / equivalencias exactas** (mismo endpoint, mismo body, mismo status y `ErrorCode`):
  - `id_validation_http_tests.rs:311-331` `token_for_another_room_gets_binary_forbidden` ≡ `http_error_codes_tests.rs:205-215` (primer caso de `valid_token_for_another_room_is_forbidden_on_every_extractor`).
  - `id_validation_http_tests.rs:334-351` `missing_token_gets_binary_unauthorized` ≡ `http_error_codes_tests.rs:175-181`.
  - `id_validation_http_tests.rs:354-370` `sse_with_a_token_for_another_room_is_forbidden` ≡ `http_error_codes_tests.rs:236-246`.
  - `id_validation_http_tests.rs:373-410` (casos «otra room en el payload» y «otro cliente») ≡ `http_error_codes_tests.rs:250-271`. El tercer caso (`:411-424`, `GetSchema` en `/heartbeat` → 400) **no** está en otro lado: moverlo a `payload_client_other_than_the_token_is_forbidden`.
  - También cercanos: `id_validation_http_tests.rs:472-487` (relay con token de otra room) ≈ `http_error_codes_tests.rs:217-234` (mismo extractor `RelayAuth`, distinto endpoint: upload vs chunk) — conservar ambos o juntarlos en una tabla de endpoints.
- **Problema:** ~110 líneas que repiten casos sin añadir cobertura y viven en un archivo cuyo tema es la validación de ids.
- **Propuesta:** borrar los duplicados exactos y mover el caso `wrong_kind` y, si se quiere, el de relay/upload a `http_error_codes_tests.rs`. Ningún caso adversario desaparece.
- **Ahorro estimado:** ~70–90 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q6. Tablas de casos para los 401/403 del data plane en `server_integration_tests.rs`
- **Dónde:** `:1135-1246` (`…_rejected_without_bearer`: 5 endpoints × ~17 líneas), `:1249-1310` (expirado/adulterado), `:1313-1403` (path/token/payload), `:1406-1460` (`dev-token`).
- **Problema:** el mismo bloque repetido con distinto mensaje/endpoint.
- **Propuesta:**
  ```rust
  let cases = [("commit", commit_msg), ("sync", sync_msg), ("ack", ack_msg), ("heartbeat", hb_msg), ("deregister", dereg_msg)];
  for (endpoint, msg) in &cases {
      let resp = server.post_raw(&room_id, endpoint, None, msg).await;
      assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{endpoint}");
  }
  ```
  Estos cuatro tests solapan parcialmente `http_error_codes_tests.rs` (allí con `heartbeat` y verificando además el `ErrorCode`), pero aquí recorren `commit/sync/ack/deregister`: conservar los casos, sólo compactar. Opcional: verificar también el frame con `assert_binary_error` (más estricto, no cambia lo que se prueba).
- **Ahorro estimado:** ~120 líneas (incluido en parte en Q1; neto adicional ~40–60) · **Riesgo:** bajo · **Prioridad:** media

### Q7. Unificar la lectura de SSE con `read_sse_until`
- **Dónde:** `server_integration_tests.rs:880-897` y `:1621-1638` leen **un solo chunk** con timeout de 2 s y buscan dos substrings; `read_sse_until` (`:2051-2066`) ya hace el bucle robusto.
- **Propuesta:** `let text = read_sse_until(&mut stream, "data: 1").await; assert!(text.contains("event: head_advanced"), "{text}");`. Ahorra ~10 líneas por sitio y elimina una posible intermitencia si el evento llega partido en dos chunks. Cambia el timeout de 2 a 5 s y acepta eventos repartidos en varios chunks (más tolerante; **a verificar** que no se quiera exigir que el evento llegue en un chunk).
- **Ahorro estimado:** ~20 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q8. Asserts verbosos del test de catchup
- **Dónde:** `server_integration_tests.rs:1885-1921`, `:1987-1997` y similares.
- **Problema:** `len()` + bucle con índice + tres `assert_eq!` por campo; en `:1992-1996` cinco `assert_eq!(catchup_ops[i].seq, …)` seguidos.
- **Propuesta:**
  ```rust
  let seqs: Vec<_> = catchup_ops.iter().map(|o| o.seq).collect();
  assert_eq!(seqs, (3..=6).map(SequenceNumber::new).collect::<Vec<_>>(), "catchup must be contiguous");
  ```
  y para el contenido, un helper local `fn same_op(a: &Operation, b: &Operation)` que compare `table_id/pk/kind`. Comparar la `Operation` completa (`Operation: PartialEq`, `core/src/mutation/op.rs:70`) sería aún más corto pero más estricto: **a verificar** que el server no reescriba ningún campo (p. ej. timestamp) antes de devolverla.
- **Ahorro estimado:** ~20–25 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q9. `id_validation_http_tests.rs`: helper `post` como el de `http_error_codes_tests.rs` y `assert_json_error`
- **Dónde:** POST binarios inline en `:281-308`, `:311-331`, `:373-425`, `:472-525`, `:525-566`, `:587-608`; el assert JSON «status 400 + `body["code"] == "BadRequest"`» en `:97-103`, `:146-153`, `:438-441`, `:579-583` (415 + `"BadRequest"`) y `admin_schema_http_tests.rs:96-100`.
- **Propuesta:** `server.post(path, bearer, body)` (ya existe en `http_error_codes_tests.rs:91-101`; con Q4 va a `common`) y `assert_json_error(resp, StatusCode::BAD_REQUEST, "BadRequest")`. En `:132-135` el `match decode_message…` inline es `assert_binary_error` sin el chequeo de `content-type` (usarlo es más estricto; **a verificar** que esa ruta responda `application/octet-stream`, como afirman los demás tests de errores binarios).
- **Ahorro estimado:** ~60–80 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q10. Test de `ClientLeaseTracker` dentro del archivo de integración HTTP
- **Dónde:** `server_integration_tests.rs:1641-1718` `test_client_lease_disconnected_to_dormant_timeout` (`#[test]` síncrono, sin server).
- **Problema:** es un test unitario de `actor::lease`; los casos «desconectado dentro del log sigue `Disconnected`» y «con `dormant_after` pasa a `Dormant`» ya están en `src/actor/tests/lease.rs:263` y `:319`. Lo único que suma son los asserts de `min_connected_ack_seq` en cada estado.
- **Propuesta:** **a verificar** si `src/actor/tests/lease.rs` cubre `min_connected_ack_seq` con clientes `Disconnected`/`Dormant`; si sí, eliminar este test; si no, mover esos asserts a los tests de `lease.rs` (es la carpeta de tests del módulo, no el código fuente). Mientras tanto, los `if let Some(entry) = tracker.get_client_mut(&alice) { … }` (`:1670`, `:1688`) deberían ser `.unwrap()` para que el test falle si alice no está, en lugar de saltarse el paso en silencio.
- **Ahorro estimado:** ~0–78 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q11. Retoques en `auth_tests.rs`
- **Dónde:** `generate_client_token(&ClientId::new(..).unwrap(), &RoomId::new(..).unwrap(), Duration::from_secs(60), secret)` en `:56-61`, `:70-75`, `:138-143`, `:152-157`; `token.split('.').map(str::to_string).collect()` en `:76`, `:145`, `:158/165`; `use base64::…` repetido dentro de dos funciones (`:67-68`, `:129-130`).
- **Propuesta:** `fn token(client: &str, room: &str) -> String` (secreto `"secret"`, 60 s) y `fn parts(t: &str) -> Vec<String>`; subir los `use base64` al tope. El primer test (`:8-37`) mezcla seis comprobaciones; está bien como «smoke», sólo cambiar el comentario de `:35`.
- **Ahorro estimado:** ~15–25 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q12. Retoques en `http_edge_tests.rs`
- **Dónde:** preámbulo HTTP/2 (`PRI * HTTP/2.0…` + SETTINGS vacío) duplicado en `:296-303` y `:702-708`; pacing `while unpaced >= 64 * 1024 { sleep(20 ms); unpaced -= 64 * 1024 }` en `:622-625` y `:666-669`; assert `matches!(… SyncBatch { ops, .. } if ops.len() == LARGE_ROWS as usize)` en `:631-634` y `:676-679`.
- **Propuesta:** `async fn send_h2_preface(stream: &mut TcpStream)`, `async fn pace(unpaced: &mut usize)` y `fn assert_large_sync_batch(body: &[u8])`. Con Q4, el armado de estado (`:50-89`) usa `common::build`.
- **Ahorro estimado:** ~30–40 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q13. Nombres de tests en `server_integration_tests.rs`
- **Dónde:** 18 de 20 tests empiezan con `test_` (p. ej. `:110`, `:354`, `:1406`), mientras el resto de `tests/` usa frases descriptivas (`register_token_checks_tell_authentication_from_authorization`).
- **Propuesta:** quitar el prefijo `test_` y renombrar los que narran historia (`…_backdoor_eliminated` → `dev_token_is_not_accepted`). No ahorra líneas; sólo coherencia.
- **Ahorro estimado:** 0 · **Riesgo:** bajo · **Prioridad:** baja

## Lo que conviene dejar como está

- **`http_edge_tests.rs` en general**: los tests de timeouts y de lectores lentos son largos porque controlan bytes y tiempos a mano (sockets crudos, `set_recv_buffer_size`, pacing por bytes y no por lecturas). Los helpers `assert_closed_after`, `get_not_found`, `post_dripping`, `timed_shutdown` ya están bien factorizados. El lector HTTP/1.1 manual de `:588-626` necesita parsear `content-length` a mano porque reqwest no deja controlar el ritmo de lectura del socket: no simplificar.
- **`TestServer` de `http_edge_tests.rs`** con `stop`/`server` en `Option` y `shutdown_after`/`wait_stopped`: refleja el ciclo de vida de `serve_until_shutdown` y no tiene equivalente en los otros archivos; no forzarlo dentro del `TestServer` común.
- **`admin_schema_http_tests.rs`**: los tests ya son cortos y usan `assert_rejected`; sólo le toca el arranque común.
- **`http_error_codes_tests.rs`**: buena forma (helpers `post`/`register`, `assert_binary_error`); es el modelo a seguir para los demás. Sólo cede el arranque y los helpers al módulo común.
- **Los tests largos de `server_integration_tests.rs` que recorren un escenario completo** (`test_commit_ack_catchup_ops_content_ordering_and_contiguity`, `test_snapshot_demand_signals_over_http_and_sse`, `test_snapshot_relay_chunked_transfer_and_blake3`): conviene mantenerlos como escenarios de punta a punta; con Q1–Q2 se acortan mucho sin partirlos.
- **Tests de tokens adversarios de `auth_tests.rs`** (`legacy_token_format_is_rejected`, `signature_binds_the_boundary_between_client_and_room`, `non_canonical_expiry_is_rejected`, `malformed_tokens_are_rejected`): cada uno cubre un ataque distinto; no fusionarlos.

## Tests

- **Cobertura solapada pero no idéntica** entre `server_integration_tests.rs` (`:1135-1460`, commit/sync/ack/deregister, sólo status), `http_error_codes_tests.rs` (heartbeat, status + `ErrorCode` + `content-type`) e `id_validation_http_tests.rs` (duplicados exactos, Q5). Una vez creado `common`, lo ideal es que la matriz «endpoint × falta de token / token de otra room / otro cliente» viva en `http_error_codes_tests.rs` como tabla y que `server_integration_tests.rs` sólo pruebe flujos felices y escenarios; pero eso mueve casos entre archivos, así que se deja como recomendación de orden, no de borrado.
- **Asserts débiles a reforzar mientras se compacta**: varios POST de setup en `server_integration_tests.rs` no verifican el status (`:524-531`, `:540-555`, `:571-578`, `:584-598`, `:860-877`); el helper de Q1 los haría fallar donde corresponde. Los `if let Some(entry)` de `:1670` y `:1688` pueden saltarse un paso en silencio (Q10).
- **`correlation_id` con valores dispersos** (1, 2, 3, 10+i, 30, 90…) en `server_integration_tests.rs`: el único assert que lo lee es `:2264`; en el resto fijarlo dentro de los helpers no cambia nada observable.
- **Intermitencia potencial**: la lectura de un solo chunk SSE en `:880` y `:1621` (Q7).
