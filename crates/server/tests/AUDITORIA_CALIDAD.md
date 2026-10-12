# Auditoría de calidad — `crates/server/tests/`

> El informe tiene dos partes. La **parte A** (HTTP e integración general) va primero. La **parte B** (actores, ciclo de vida, log, relay y WAL) está al final, en la sección [Parte B](#parte-b-actores-ciclo-de-vida-log-relay-y-wal). Las dos partes coinciden en lo mismo: hace falta un `tests/common/mod.rs` compartido con el arranque del server y la fábrica de salas. Ahorro conjunto estimado: ~1 300–1 800 líneas netas de 7 945 (≈16–23 %).

## Parte A: HTTP e integración general

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
- **Verificación (2.ª pasada):** ⚠️ corregida — `assert_binary_error` de `http_error_codes_tests.rs:125-138` e `id_validation_http_tests.rs` no son "duplicados exactos": difieren en el mensaje del `assert` ("Data Plane errors are binary frames" vs "error responses of the binary API are binary frames"); unificarlos es seguro pero cambia ese texto de fallo. `config.validate()` existe (`config.rs:309`); `tests/common/` aún no existe.

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

---

## Parte B: actores, ciclo de vida, log, relay y WAL

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `crates/server/tests/{room_actor_tests, room_lifecycle_resilience_tests, tiered_log_tests, snapshot_relay_tests, unified_wal_tests, commit_semantics_tests, graceful_shutdown_tests, dedup_cache_tests}.rs`

### Resumen

- La forma es muy despareja. `snapshot_relay_tests.rs`, `commit_semantics_tests.rs` y `dedup_cache_tests.rs` ya están bien escritos: helpers chicos, nombres descriptivos, un comportamiento por test. `room_actor_tests.rs` y `room_lifecycle_resilience_tests.rs` (casi la mitad del alcance) arman a mano cada `RoomCommand` con su `oneshot`, y eso son 10–12 líneas por llamada.
- La mayor ganancia sale de un `tests/common/mod.rs` con: `ask` genérico sobre `mpsc::Sender<RoomCommand>`, funciones `register/commit/sync/ack/cursor/metrics`, la fábrica de sala (registro + config + relay + manager + `get_or_spawn`), `make_test_op(seq)`, `envelope(len, fill)` y `seqs(&[SequencedOperation])`. Hoy hay 9 `SnapshotRelay::new(...)` copiados en 5 archivos del alcance, y `make_test_op` tiene dos copias idénticas más tres variantes inline.
- `tiered_log_tests.rs` y `unified_wal_tests.rs` tienen mucho bucle `for i in a..=b { log.append(...) }` (17 veces solo en tiered), paths `segment_…` armados a mano, aserciones posición por posición (`deltas[0]`, `deltas[4]`) y tres pares de tests casi iguales.
- `unified_wal_tests.rs` contiene dos tests que no tratan del WAL (`test_server_error_mapping`, `test_server_config_toml_and_env_overrides`). El primero repite el patrón de tabla que ya existe en `src/tests/error.rs:5`.
- **Ahorro estimado:** ~550–800 líneas netas de 3517 (≈16–23 %), contando ya las ~150–190 líneas nuevas de `common/mod.rs`. Ninguna sugerencia saca cobertura adversaria: los tests redundantes se fusionan y conservan sus aserciones únicas.

### Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---|
| `room_actor_tests.rs` | 894 | 764 | 0 | 52 | 78 | ~300–380 |
| `room_lifecycle_resilience_tests.rs` | 837 | 730 | 0 | 32 | 75 | ~220–290 |
| `tiered_log_tests.rs` | 570 | 402 | 0 | 66 | 102 | ~110–150 |
| `snapshot_relay_tests.rs` | 461 | 409 | 5 | 7 | 40 | ~15–25 |
| `unified_wal_tests.rs` | 315 | 253 | 0 | 14 | 48 | ~70–90 |
| `commit_semantics_tests.rs` | 241 | 203 | 6 | 3 | 29 | ~0–10 (pasa a ser la fuente de `common`) |
| `graceful_shutdown_tests.rs` | 153 | 139 | 0 | 2 | 12 | ~25–35 |
| `dedup_cache_tests.rs` | 46 | 33 | 1 | 3 | 9 | 0 |
| `common/mod.rs` (nuevo) | — | — | — | — | — | +150–190 |
| **Total** | **3517** | **2933** | **12** | **179** | **393** | **~550–800 netas** |

### Comentarios

Densidad baja en general (179 líneas de `//`, el 5 %), pero mal repartida. `tiered_log_tests.rs` tiene 66 y `room_actor_tests.rs` 52; la mayoría son pasos numerados que narran la línea siguiente.

**Recortar (repiten el código o son ruido):**
- `room_actor_tests.rs:65,69,83,93`: `// 1. Register schema`, `// 2. Add nullable column evolution`… Cada uno repite la llamada que tiene abajo. Lo mismo pasa con `:124,183` (`// Register client`), `:289` (`// Subscribe to SSE events`), `:335` (`// Verify metrics`), `:431` (`// Gracefully shutdown room actor`), `:472` (`// Commit op 6 successfully`) y `:804`.
- `room_actor_tests.rs:560-561`: `// 4. CRITICAL CHECK: … MUST STILL BE 0! Because the client has NOT sent its ACK yet!` repite el mensaje del `assert_eq!` (`:574`, "Cursor must NOT advance upon delivery…"). Lo mismo con `:577` contra el assert de `:599`, y `:614` (`// 7. NOW the client's confirmed cursor in the server is 5!`) contra el de `:627`. Alcanza con el mensaje del assert.
- `tiered_log_tests.rs`: `// Run background maintenance` (`:145`), `// Append 10 operations` (`:391`), `// Verify both sealed segments exist on disk` (`:260`), `// 1. Create first segment 1..=5` / `// 2. Create second…` (`:452,458`), `// Verify segments directory contains…` (`:100`). Todos sobran en cuanto existan `append_range` y `segment_path` (Q5).
- `room_lifecycle_resilience_tests.rs:154,168,209,253` y los pasos `// 3. Upload chunk 0` / `// 4. Upload chunk 1` (`:600,639`), que desaparecen con el helper de Q3.
- `unified_wal_tests.rs:25,32,43` (`// 1. Initial creation`, `// 2. Append…`, `// 3. Re-open…`) y `:296` (`// Clean up env vars`).

**Desactualizados o incorrectos (corregir):**
- `room_actor_tests.rs:165`: `// 0. Commit from unregistered client rejected with Unauthorized`. El assert de `:178-181` espera `ServerError::ClientNotRegistered`, no `Unauthorized`.
- `room_actor_tests.rs:822`: `// … far into the future (e.g. u64::MAX or 999)`. Solo se prueba 999. Si quedara solo `u64::MAX`, el test cambiaría.
- `tiered_log_tests.rs:404`: el nombre `test_tiered_log_async_maintenance_spawn_blocking` y el comentario `:426` ("delegates Zstd compression to spawn_blocking") describen un detalle de implementación que el test no observa (ver Q6).

**Conservar (explican el *por qué* o una precondición no obvia):**
- `room_actor_tests.rs:654-656` (por qué TTL por defecto y segmentos chicos), `:670-673` y `:721-723` (cómo el ancla de retención protege los deltas 6..=20). Son la especificación del test.
- `room_lifecycle_resilience_tests.rs:452-454` (por qué el cliente reporta cursor 0 en los 1005 commits) y `:747-749` (el snapshot es válido y solo el hash está mal, así que es un error del cliente).
- `commit_semantics_tests.rs:183-184`, `:203`; `graceful_shutdown_tests.rs:86,113`; `dedup_cache_tests.rs:12-13`. Todos cortos y con intención.
- `tiered_log_tests.rs:31` (drop abrupto = crash), `:311-312` (rotación + ventana deslizante), `:511-515` y `:521-523` (qué tier sirve cada rango).

### Sugerencias

**Q1. `tests/common/mod.rs` con un `ask` genérico y helpers de `RoomCommand`**
- **Dónde:** `room_actor_tests.rs`: 35 `oneshot::channel()`, que empiezan en `:45` (`register_client_helper`), `:125`, `:167`, `:190`, `:207`, `:223`, `:240`, `:278`, `:290`, `:315`, `:336`, `:354`, `:368`, `:417`, `:448`, `:458`, `:474`, `:515`, `:532`, `:547`, `:562`, `:578`, `:588`, `:603`, `:615`, `:675`, `:688`, `:724`, `:737`, `:757`, `:807`, `:823`, `:843`, `:858` y `:875`. `room_lifecycle_resilience_tests.rs`: 10 (`:156`, `:169`, `:217`, `:229`, `:441`, `:457`, `:472`, `:491`, `:540`, `:551`). `snapshot_relay_tests.rs:72,88`; `graceful_shutdown_tests.rs:33,89`; `commit_semantics_tests.rs:87,114,137`.
- **Problema:** cada envío repite `let (tx, rx) = oneshot::channel(); sender.send(RoomCommand::X { …, reply: tx }).await.unwrap(); rx.await.unwrap()`, que rustfmt deja en 8–12 líneas. `commit_semantics_tests.rs` (`TestRoom::register/commit/metrics`), `graceful_shutdown_tests.rs` (`commit`) y `room_actor_tests.rs` (`register_client_helper`) ya inventaron su propia versión parcial, cada uno por su lado.
- **Propuesta:** todas las respuestas de `RoomCommand` salvo `Shutdown` son `oneshot::Sender<Result<T, ServerError>>` (`src/actor/command.rs:90-160`), así que alcanza un único genérico más funciones finas:
  ```rust
  pub async fn ask<T>(
      tx: &mpsc::Sender<RoomCommand>,
      cmd: impl FnOnce(oneshot::Sender<Result<T, ServerError>>) -> RoomCommand,
  ) -> Result<T, ServerError> {
      let (reply, rx) = oneshot::channel();
      tx.send(cmd(reply)).await.unwrap();
      rx.await.unwrap()
  }
  pub async fn register(tx: &Sender, client: &ClientId) -> RegisterResponse { … }
  pub async fn commit(tx: &Sender, client: &ClientId, mutation: MutationId, last_ack: u64, op: Operation) -> Result<CommitResponse, ServerError> { … }
  pub async fn sync(tx: &Sender, client: &ClientId, from: u64, max: u32) -> Result<SyncBatchResponse, ServerError> { … }
  pub async fn cursor(tx, client) -> Option<SequenceNumber>; ack(tx, client, seq) -> Result<…>; metrics(tx) -> RoomMetrics
  ```
  Antes/después (`room_actor_tests.rs:822-839`, 18 líneas → 5):
  ```rust
  let err = ack(&sender, &alice, 999).await.unwrap_err();
  assert!(matches!(err, ServerError::InvalidSequence { expected, actual }
      if expected == SequenceNumber::new(3) && actual == SequenceNumber::new(999)));
  ```
  Usá funciones libres sobre `&Sender`, no métodos de un `TestRoom`: `room_actor_tests.rs:301-327` clona el `Sender` dentro de `tokio::spawn`. No reemplazar por `RoomManager::ask` (`src/actor/manager.rs:552`): ese agrega timeout y reenvío a una sala respawneada, y cambiaría lo que observan estos tests.
- **Requisito:** CI corre `cargo clippy --workspace --all-targets -- -D warnings` (`.github/workflows/ci.yml:42`). Cada archivo de `tests/` es un crate distinto y usa solo una parte de `common`, así que `common/mod.rs` necesita `#![allow(dead_code)]` (o `#[allow(dead_code)]` por ítem).
- **Ahorro estimado:** ~350–450 líneas en el alcance (sobre todo `room_actor` y `lifecycle`) · **Riesgo:** bajo · **Prioridad:** alta

**Q2. Fábrica de sala compartida (registro + config + relay + manager + `get_or_spawn`)**
- **Dónde:** `room_actor_tests.rs:104-120`, `:143-160`, `:257-275`, `:385-398`, `:492-509`, `:633-667`, `:778-795`; `room_lifecycle_resilience_tests.rs:111-132`, `:183-205`, `:408-429`; `commit_semantics_tests.rs:47-83`; `graceful_shutdown_tests.rs:49-84`; `snapshot_relay_tests.rs:36-73`.
- **Problema:** unas 13 copias de las mismas 15–22 líneas: `tempdir` → `SchemaRegistry::new(dir/schemas)` → `register_schema` → `ServerConfig { data_dir, ..Default }` → `SnapshotRelay::new(…, 60s, default max)` → `RoomManager::new` → `get_or_spawn`/`create_room`. `room_actor_tests.rs:11-20` (`create_test_relay`) ya extrajo una parte, pero `:645-652` lo vuelve a copiar con otro TTL.
- **Propuesta:** generalizar `commit_semantics_tests.rs::spawn_room` en `common` como `TestRoom::spawn(policy: Option<RoomLifecyclePolicy>) -> TestRoom { _dir, manager: Arc<RoomManager>, relay: Arc<SnapshotRelay>, sender, schema, schema_id, room_id, data_dir }`. Tiene que exponer `relay` y `manager`, porque `room_actor_tests.rs:707-718` llama a `relay.stage_snapshot` con `manager.log_bounds`. Para el test de recuperación (`room_actor_tests.rs:400-488`) basta `fn manager(&dir) -> RoomManager` reutilizable. Los nombres de sala/esquema distintos por test no importan, porque cada test tiene su propio `tempdir`. Ojo: `commit_semantics_tests.rs:79` arma `segments_dir` con `"commit-room"`, así que el path tiene que derivarse de `room_id`.
- **A verificar:** `room_actor_tests.rs:645-652` usa TTL de relay de 300 s en vez de 60 s. El snapshot se lee enseguida, así que probablemente es indistinto, pero conviene dejar el parámetro.
- **Ahorro estimado:** ~150–200 líneas · **Riesgo:** bajo · **Prioridad:** alta

**Q3. Subida multiparte de `room_lifecycle_resilience_tests.rs`: un helper `post_frame` y una aserción de ack**
- **Dónde:** `room_lifecycle_resilience_tests.rs:576-778` (el test ocupa 268 líneas); también `:286-325` y `:340-376`.
- **Problema:** las cuatro construcciones de `ClientMessage::UploadSnapshotChunk` (`:577-586`, `:601-610`, `:640-649`, `:677-686`) solo cambian `correlation_id`, `chunk_index` y el rango de datos. Hay seis `client.post(format!(…)).header(AUTHORIZATION…).header(CONTENT_TYPE…).body(encode_message(..)).send().await.unwrap()` de 12–15 líneas cada uno, y tres `match ack { SnapshotUploadChunkAck {..} => …, other => panic! }` casi iguales.
- **Propuesta:** dentro de `impl LifecycleTestServer`, `async fn post_frame(&self, path: &str, token: Option<&str>, msg: &ClientMessage) -> reqwest::Response`, con el mismo estilo que `snapshot_relay_tests.rs:148-160`. En el test, un closure `chunk(i, range) -> ClientMessage` y una tabla:
  ```rust
  for (i, range, staged) in [(0, 0..CHUNK, false), (1, CHUNK..2 * CHUNK, false), (2, 2 * CHUNK..len, true)] {
      let resp = server.post_frame("snapshot/upload-chunk", Some(&token), &chunk(i, range)).await;
      assert_eq!(resp.status(), StatusCode::OK);
      assert!(matches!(decode(resp).await,
          ServerMessage::SnapshotUploadChunkAck { chunk_index, total_chunks: 3, staged: s, .. } if chunk_index == i && s == staged));
  }
  ```
  Esto también agrega la verificación de `total_chunks == 3` en los chunks 1 y 2, que hoy solo se hace en el 0: es más estricto, no menos. Para los commits HTTP del test de reactivación perezosa (`:305-324`, `:345-358`) sirve el mismo `post_frame`.
- **Ahorro estimado:** ~110–140 líneas · **Riesgo:** bajo · **Prioridad:** alta

**Q4. `tiered_log_tests.rs` y `unified_wal_tests.rs`: helpers `append_range`, `seqs` y `segment_path`**
- **Dónde:** `tiered_log_tests.rs`: 17 bucles `for i in a..=b { log.append(make_test_op(i), None).unwrap(); }` (empiezan en `:60`, `:96`, `:130`, `:182`, `:190`, `:196`, `:249`, `:255`, `:307`, `:392`, `:418`, `:453`, `:459`, `:468`, `:515`, `:556`; `:27` además verifica `head_seq`); paths de segmento armados a mano en `:102`, `:136-139`, `:150-153`, `:261-268`; aserciones por índice o por `enumerate` en `:43-50`, `:66-69`, `:73-76`, `:111-113`, `:162-164`, `:208-220`, `:286-292`, `:318-327`, `:396-400`, `:430-434`, `:530-540`, `:567-569`. En `unified_wal_tests.rs`: `:33-40`, `:68-74`, `:108-113`, `:139-144`.
- **Problema:** mucho texto para afirmaciones simples. Además, las aserciones por índice (`deltas[0]`, `deltas[4]`) son más débiles que comparar la secuencia completa.
- **Propuesta:** `src/log/tests/tiered_log.rs:28` (`assert_contiguous`) y `:287` (`append_range`) ya tienen estos helpers para los unitarios, y `commit_semantics_tests.rs:158` tiene `seqs`. Replicarlos en `common`:
  ```rust
  pub fn append_range(log: &mut TieredLog, seqs: RangeInclusive<u64>) { for s in seqs { log.append(make_test_op(s), None).unwrap(); } }
  pub fn seqs(ops: &[SequencedOperation]) -> Vec<u64> { ops.iter().map(|o| o.seq.get()).collect() }
  pub fn segment_path(dir: &Path, first: u64, last: u64) -> PathBuf { dir.join("segments").join(format!("segment_{first:016}_{last:016}.wal")) }
  ```
  (para el `.wal.zst` de `:150-153`: `segment_path(..).with_extension("wal.zst")`)
  y `assert_eq!(seqs(&deltas), (6..=10).collect::<Vec<_>>())` en lugar de `len` + `[0]` + `[4]`. Las aserciones quedan más estrictas porque verifican todos los elementos.
- **Ahorro estimado:** ~70–90 líneas · **Riesgo:** bajo · **Prioridad:** media

**Q5. `test_tiered_log_hot_buffer_o1_range_query` como tabla de casos**
- **Dónde:** `tiered_log_tests.rs:332-379`.
- **Problema:** siete consultas `get_range(from, ReadBudget::ops(n))` con el mismo patrón y un comentario por caso.
- **Propuesta:**
  ```rust
  let cases: [(u64, usize, Vec<u64>); 7] = [
      (5, 5, vec![]),            // antes de min_seq: dejaría hueco
      (9, 5, (10..=14).collect()),
      (10, 3, (11..=13).collect()),
      (15, 10, (16..=20).collect()),
      (20, 5, vec![]), (25, 5, vec![]), (10, 0, vec![]),
  ];
  for (from, n, expected) in cases {
      let got = buffer.get_range(SequenceNumber::new(from), &mut ReadBudget::ops(n));
      assert_eq!(seqs(&got), expected, "from {from}, budget {n}");
  }
  ```
  Hay que conservar el comentario del caso `from=5`, que explica por qué vuelve vacío.
- **Ahorro estimado:** ~25 líneas · **Riesgo:** bajo · **Prioridad:** media

**Q6. Fusionar tres pares de tests casi idénticos en `tiered_log_tests.rs`**
- **Dónde y por qué:**
  - `test_tiered_log_async_maintenance_spawn_blocking` (`:404-436`) contra `test_tiered_log_cold_compression_and_read` (`:116-165`): política equivalente (solo cambia `warm_disk_ttl`: 10 ms contra 5 ms), mismos 5 ops, `force_rotate_warm`, espera más allá de `warm_disk_ttl`, `run_maintenance` con `warm_compressed_count == 1` y lectura de 1..=5 desde frío. El segundo verifica además los archivos. La única diferencia es `tokio::time::sleep` contra `std::thread::sleep`, que no cambia lo que se observa.
  - `test_tiered_log_fast_path_off_by_one_boundary` (`:543-570`) contra `test_tiered_log_hot_buffer_sliding_window_no_zero_eviction` (`:296-329`): los dos usan `ram_max_ops: 5`, agregan 1..=6 y piden desde el cursor 1, esperando 2..=6. Solo cambia el límite (5 contra 10). Alcanza con agregar al segundo `assert_eq!(log.fetch_deltas(SequenceNumber::new(1), 5)…, ((2..=6), false))`.
  - `test_tiered_log_hierarchical_cache_inversion` (`:381-402`) es una lectura dentro del hot buffer, ya cubierta por `test_tiered_log_hot_buffer_fast_read` (`:55-84`), que hace lo mismo con más casos. El nombre ("cache inversion") no describe lo que prueba.
- **Propuesta:** quedarse con un test por comportamiento y mover a él las aserciones que solo tenía el otro. No se pierde ningún caso.
- **Ahorro estimado:** ~65–75 líneas · **Riesgo:** bajo · **Prioridad:** media

**Q7. `unified_wal_tests.rs`: sacar lo que no es WAL y tabular la configuración**
- **Dónde:** `test_server_error_mapping` (`:260-315`) y `test_server_config_toml_and_env_overrides` (`:202-258`).
- **Problema:** (a) el mapeo de errores son ocho bloques de 3–8 líneas. `src/tests/error.rs:5-57` (`each_error_maps_to_its_code_and_status`) ya es una tabla `(err, code, status)`, pero no incluye `SchemaViolation`, `BehindCompaction`, `RoomNotFound`, `InvalidSequence`, `RoomLocked` ni `SnapshotSuperseded`; `Unauthorized` y `Timeout` están en las dos. (b) Los ocho `set_var` y los ocho `remove_var` (`:223-230`, `:297-304`) son listas paralelas, y si el test falla en el medio los `remove_var` no se ejecutan.
- **Propuesta:** (a) agregar las seis filas que faltan a la tabla de `src/tests/error.rs` (unas 30 líneas allá, con rustfmt) y borrar el test de integración, que queda totalmente cubierto. Si se prefiere no tocar `src/tests`, convertirlo en tabla ahí mismo, con el mismo `for (err, code, status) in cases`. (b) `const ENV: [(&str, &str); 8] = [("ZEMDB_PORT", "9999"), …];` con un `for (k, v) in ENV { std::env::set_var(k, v) }` antes y un `remove_var` después. El override por closure (`apply_overrides_from`, `src/config.rs:415`) es privado, así que no está disponible desde un test de integración. Cuando el resto de la suite se ordene, conviene mover los dos tests a un archivo propio (`config_and_errors_tests.rs`) o a los unitarios. Hoy el nombre del archivo engaña.
- **Ahorro estimado:** ~45–60 líneas en el alcance (−55 en el archivo, +~30 en `src/tests/error.rs`, −14 por la tabla de env) · **Riesgo:** bajo · **Prioridad:** media

**Q8. `unified_wal_tests.rs`: fixture de log escrito y `file_len`**
- **Dónde:** `:106-114`, `:137-145`, `:178-182` (bloque `{ let (mut log, _) = open…; append… }`); `:116`, `:147`, `:185` (`dir.path().join("segments").join("active.wal")`); `:148`, `:157`, `:165`, `:186`, `:198` (`std::fs::metadata(&p).unwrap().len()`).
- **Propuesta:** `fn write_log(dir, n) -> PathBuf /* active.wal */`, que agregue los ops 1..=n con `MutationId::from_u128(i)` y devuelva el path, más `fn file_len(p) -> u64`. Las variables intermedias `let seq = …; let op = make_test_op(seq.get())` (`:34-36`, `:69-71`) sobran. Ojo: `test_unified_wal_dedup_idempotency` usa `i * 42` como id y `test_unified_wal_append_and_recovery_roundtrip` usa `i + 1000`; el helper tiene que aceptar la función de id o esos dos tests deben quedar como están.
- **Ahorro estimado:** ~25 líneas · **Riesgo:** bajo · **Prioridad:** baja

**Q9. Envelope de snapshot y `make_test_op` duplicados**
- **Dónde:** `snapshot_relay_tests.rs:127-134` (`envelope`) contra las copias inline de `room_actor_tests.rs:702-706`, `room_lifecycle_resilience_tests.rs:565-569` y `:744-750`. `make_test_op` en `tiered_log_tests.rs:11-15` y `unified_wal_tests.rs:14-18` (idénticos), más las variantes inline de `room_lifecycle_resilience_tests.rs:385-387`, `:786-788` y `:832-834`.
- **Propuesta:** moverlos a `common`. `:565-569` es literalmente `envelope(300 * 1024, 0x42)`. En `:744-750` el cuerpo `"test"` no importa: el test solo depende de que el hash `[0xFF; 32]` no coincida, así que sirve `envelope(SNAPSHOT_HEADER_LEN + 4, b't')`. En `room_actor_tests.rs:702` cualquier envelope sirve.
- **Ahorro estimado:** ~20 líneas · **Riesgo:** bajo · **Prioridad:** baja

**Q10. Detalles de `snapshot_relay_tests.rs` y `graceful_shutdown_tests.rs`**
- `snapshot_relay_tests.rs:216-219`, `:239-242`, `:261-267`, `:293-296`, `:347-350`: `assert_eq!(server.upload(..).await.status(), StatusCode::OK)` ocupa 4–7 líneas con rustfmt. Un `async fn upload_ok(&self, seq, data)` lo deja en 1. Del mismo modo, el par `assert_eq!(resp.status(), X); assert_eq!(error_code(resp).await, Y);` (`:199-200`, `:210-211`, `:222-223`, `:231-232`, `:374-375`, `:459-460`) se resume en `expect_http_error(resp, X, Y).await`, simétrico al `expect_error` de `:186`. ~15–20 líneas.
- `graceful_shutdown_tests.rs:49-73` repite la fábrica de Q2 (sin `get_or_spawn`) y `:89-100` reimplementa `register` + `commit` de Q1. ~25–30 líneas.
- `room_lifecycle_resilience_tests.rs:20-24`: campos `pub` en un struct privado de test. `:30-31,37`: `host: "127.0.0.1"` y `dormant_after_secs: None` son los valores por defecto (`src/config.rs:130,244`), y `port: 0` no se usa: el listener se bindea aparte en `:69` y `config.port` solo se lee en `src/main.rs:40,45`. Se pueden borrar las tres líneas y el `pub` de los campos. Unas 3–5 líneas.
- **Riesgo:** bajo · **Prioridad:** baja

**Q11. Test de colisión de flock simple, incluido en el de estrés**
- **Dónde:** `room_lifecycle_resilience_tests.rs:379-404` contra `:780-837`.
- **Problema:** el test de estrés hace exactamente lo mismo (append que toma el flock, luego `open_or_create` → `RoomLocked` con el mismo mensaje) con 12 hilos, y además verifica la liberación al hacer `drop`. El test simple no aporta ningún caso propio.
- **Propuesta:** borrarlo o fusionarlo. Si se quiere conservar el caso "un solo intento" como documentación, que quede como el primer `assert` del test de estrés antes de lanzar los hilos (3 líneas). En los dos tests, el `match` de tres brazos se puede reemplazar por `assert!(matches!(res, Err(ServerError::RoomLocked(ref m)) if m.contains("active.wal locked by another process")), "{res:?}")`. `WarmDiskLog` deriva `Debug` (`src/log/warm_disk.rs:29`), así que `{res:?}` compila.
- **Ahorro estimado:** ~25–35 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Lo que conviene dejar como está

- **`commit_semantics_tests.rs`**: es el modelo a seguir (`TestRoom` con métodos de dominio, políticas con nombre, `wait_for_tail_above` con tiempo pausado y nombres que describen el comportamiento). Solo cambia de lugar si sus helpers pasan a `common`.
- **`dedup_cache_tests.rs`**: 46 líneas, tres tests mínimos con el *por qué* comentado (`:12-13`). No hay nada que recortar.
- **`snapshot_relay_tests.rs:404-422` (`wrong_kind_frame_of_len`)**: el cálculo de overhead con una sonda de 1 MiB parece rebuscado, pero es la única forma de obtener un frame de exactamente `MAX_FRAME_SIZE` bytes sin acoplar el test al formato del codec. Su doc-comment es justo.
- **`room_actor_tests.rs:632-774` (ancla de retención)**: aun con Q1 y Q2 va a seguir siendo largo, porque el escenario necesita 20 ops, snapshot, ack de Alice, registro y sync de Bob. Los comentarios de `:654-656`, `:670-673` y `:721-723` son la especificación del test y no se deben resumir.
- **`room_lifecycle_resilience_tests.rs:258-377` (reactivación perezosa)**: las dos fases con servidores distintos sobre el mismo directorio son el punto del test. Solo hay que compactar los POST (Q3), no la estructura.
- **`graceful_shutdown_tests.rs:101-152`**: los `timeout`, el SSE abierto y la lectura del roster persistido son todos aserciones necesarias.
- **Los `matches!`/`match` con mensaje de `panic!("expected X, got {other:?}")`**: donde el valor no implementa `Debug` o el mensaje ayuda a diagnosticar, el `match` explícito está justificado.

### Tests

- **Coordinación con la parte A:** la mayoría de los archivos de la otra parte (`admin_schema_http_tests.rs:24-61`, `http_edge_tests.rs:37-91`, `http_error_codes_tests.rs:20-62`, `id_validation_http_tests.rs:19-66`, `server_integration_tests.rs:17-70`) definen su propio `TestServer` con el mismo armado de registro, relay, manager, router y listener que `room_lifecycle_resilience_tests.rs:19-90` y `snapshot_relay_tests.rs:20-121`. Conviene un único `common::TestServer::start(configure: impl FnOnce(&mut ServerConfig))`, con la firma de `http_edge_tests.rs:50`, que es la más flexible, más un `post_frame`. Así la fábrica HTTP y la de sala de Q2 quedan en el mismo módulo.
- **Nombres:** `room_actor`, `lifecycle`, `tiered_log` y `unified_wal` usan el prefijo `test_…` y nombres de implementación (`…_o1_range_query`, `…_spawn_blocking`, `…_elimination_of_race_condition`). Los archivos más nuevos (`snapshot_relay`, `commit_semantics`, `dedup_cache`) usan frases de comportamiento. Unificar al estilo nuevo no ahorra líneas, pero mejora la lectura. Prioridad baja.
- **Ubicación:** `test_schema_registry_crud_and_evolution` (`room_actor_tests.rs:57-101`) no toca el actor. Encaja mejor junto a los unitarios de `src/tests/schema_registry.rs`, que hoy no cubren el rechazo de una columna no nullable ni la recarga. Lo mismo pasa con los dos tests de `WarmDiskLog` dentro de `room_lifecycle_resilience_tests.rs` (`:379`, `:780`), que son tests de log.
- **Sin pérdida de cobertura:** Q6, Q7(a) y Q11 eliminan tests solo cuando otro test (o la tabla unitaria) ya verifica exactamente lo mismo, y trasladan las aserciones únicas. Las pruebas adversarias (CRC alterado, escritura cortada, EOF con ceros, flock con 12 hilos, ack o commit futuros, hash BLAKE3 incorrecto, límites de frame) quedan intactas en contenido.
