# Plan de Remediación — Fase 3.8

**Fecha:** 14 de Octubre de 2026
**Origen:** [`docs/audits/2026-10-post-fase3_7-audit.md`](../audits/2026-10-post-fase3_7-audit.md) (DEF-115 a DEF-142, re-verificados en `698e8d6`)
**Tipo:** Documento vivo (plan + seguimiento). El detalle de cada defecto (dónde, escenario, evidencia) está en la auditoría; acá van el orden, las decisiones, la solución y el test exigido.

---

## 0. Cómo usar este documento

- Los defectos están agrupados en **lotes** ordenados por prioridad de ejecución. Cada lote es una unidad de trabajo revisable (uno o pocos commits).
- Cada ítem tiene: severidad, estado, solución y test exigido.
- Al cerrar un ítem se actualiza su estado aquí y se anota el commit.

**Estados:** ⬜ Pendiente · 🟡 Parcial · ✅ Hecho · ⏸ Diferido · ❌ Descartado

**Reglas de trabajo** (las mismas de las Fases 3.6 y 3.7, ver `GEMINI.md`):
1. Tests según la estructura de dos niveles de `GEMINI.md`, con al menos un test negativo que reproduzca el fallo y que falle sin el fix.
2. Sin códigos de auditoría (`DEF-xx`) en código, tests ni docstrings.
3. Cada lote cierra con `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings` y `cargo test --workspace` en verde.
4. Los tests que faltaban (DEF-131, 132, 133, 141) van en el lote que toca el código que protegen, no al final: así cubren también los cambios de ese lote.

---

## 1. Criterios del orden

1. **El servidor desplegable, primero.** El único Alto (DEF-116) es un DoS de memoria que puede disparar cualquier miembro de cualquier sala.
2. **Pérdida de datos confirmados, antes que rendimiento.** DEF-115 no afecta hoy a nadie (storage no tiene consumidor), pero es irreparable cuando ocurre y es requisito de la Fase 4.
3. **Los cambios del protocolo van antes de la Fase 4.** DEF-118, 123, 126 y 127 cambian mensajes o códigos de error; hacerlos antes de que exista el SDK evita versionar el protocolo (contexto V1, sin retrocompatibilidad, el byte de versión sigue en `0x01` como en la Fase 3.7).
4. **Costo del actor antes que casos raros.** El roster con fsync por Ack (DEF-117) aparece en régimen normal; los huecos del log (DEF-119, 120) requieren un segmento dañado o un borrado fallido.
5. **La documentación y la CI, al final**, para que reflejen los cambios de los lotes anteriores.

**Requisito para empezar la Fase 4:** Lotes 2 y 4 cerrados (storage del cliente y contrato del protocolo).

---

## 2. Decisiones de diseño

Se continúa la numeración del plan de la Fase 3.7 (que termina en D52). Se confirman o cambian antes de arrancar el lote correspondiente.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **D53** | **Respuestas acotadas en tiempo y en memoria.** (a) **Plazo de progreso por respuesta**, no por conexión: cada `TrackedBody` espera al cliente desde que se crea y desde cada frame que entrega hasta que hyper le pide el siguiente (hyper lo pide solo mientras tiene lugar en sus buffers, en HTTP/1.1, o en el control de flujo de HTTP/2). Una escritura al socket bloqueada sin progreso también espera al cliente (clave `SOCKET`), haya o no requests en curso: cubre al cliente HTTP/2 que concede ventana pero no lee el socket, con lo que h2 acumularía el cuerpo en sus buffers. El último byte de cada respuesta va en un frame propio: hyper suelta el cuerpo apenas pasa el último frame a h2, así que para entonces el cliente ya concedió ventana para el resto. Además, un cuerpo con presupuesto entrega copias de sus frames, así que lo que h2 o hyper retienen después de soltarlo (como mucho un par de frames por stream HTTP/2, aunque el cliente achique la ventana, o el buffer de escritura de hyper en HTTP/1.1) no es una porción que mantenga viva la codificación entera fuera del presupuesto. Si alguna espera de la conexión llega a `response_stall_timeout_secs` (`ZEMDB_RESPONSE_STALL_TIMEOUT_SECS`, 60 s por defecto, de 5 s a 1 h), se cierra la conexión. Aplica a HTTP/1.1 y HTTP/2. Un cuerpo sin nada que enviar (SSE sin eventos, `Pending`) no espera al cliente, así que un SSE sano nunca se corta, sea cual sea el keep-alive. El tiempo del handler y la lectura del body no cuentan (tienen sus propios límites). Por conexión se podía engañar: en HTTP/2, un stream chico que avanza o un PING del cliente sostenían streams grandes sin leer. Con frames de 64 KiB, el ritmo mínimo es de ~1,1 KiB/s. (b) **Presupuesto global de bytes de respuesta en vuelo** (`max_inflight_response_bytes`, `ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES`, 256 MiB por defecto, de `MAX_FRAME_SIZE` a 64 GiB): un semáforo en `AppState` del que `SyncBatch`, `CommitAck` y `SnapshotChunk` toman su largo exacto **después de codificarse**, sin esperar (`try_acquire_many_owned`); antes de codificar no se conoce el tamaño. El permiso viaja en las extensions de la respuesta y `serve_connection` lo mueve al `TrackedBody`, que lo libera al soltarse. Sin presupuesto, `/sync` y los chunks responden `Unavailable` (503, `Retry-After: 1`); un commit ya es durable, así que se confirma sin catch-up y con `has_more: true`. (c) `MAX_CONCURRENT_STREAMS` baja de 100 a 32. (d) El doc de `serve_until_shutdown`, `ARCHITECTURE.md` §7.3 y el README describen el plazo nuevo. Residuo aceptado: un atacante que lee justo al ritmo mínimo con muchos streams puede ocupar el presupuesto y degradar `/sync` a 503, pero no causar un OOM. Alternativa descartada: solo el plazo de (a); acota el tiempo pero no la memoria. | 1 | Adoptada |
| **D54** | **Tope de streams SSE.** (a) Como mucho 2 streams por `(sala, client_id)` (`STREAMS_PER_CLIENT`): un tercero **cierra el más viejo** del mismo cliente en vez de responder 429, para que un cliente que cambió de red no quede bloqueado por streams semiabiertos (D53 no corta un SSE inactivo). Un atacante igual queda en 2 por token. (b) En total, `max_sse_streams` (`ZEMDB_MAX_SSE_STREAMS`, `Option<usize>`; sin configurar, `max(1, max_connections / 2)`, de 1 a `max_connections`); superado, `RateLimited` (429). (c) `SseStreams` en `AppState`: un `Mutex<HashMap<(RoomId, ClientId), VecDeque<…>>>` con el total, en lugar del `DashMap` previsto (las aperturas son raras y un solo lock deja el reemplazo y el tope atómicos). El lugar se toma después de autenticar y antes de suscribirse (suscribirse puede crear el actor), y el stream más viejo se cierra recién cuando la suscripción tuvo éxito (`SseSlot::replace_older`), así que un stream que no arranca no desplaza a ninguno; cada cliente tiene como mucho un reemplazo arrancando a la vez (otro recibe `RateLimited`), así que su cola tiene como mucho 3 streams y el total pasa el tope como mucho en uno por cliente que reemplaza. Un `HEAD` a `/events` (axum lo atiende con el handler de `GET`) también reemplaza al más viejo. Lo libera un guard RAII que vive en el stream, y la entrada se borra con su último stream, así que el mapa queda acotado por el tope global más uno por cliente que reemplaza. El stream reemplazado termina cuando se suelta su `oneshot::Sender`. En HTTP/2 cada stream cuenta por separado. | 1 | Adoptada |
| **D55** | **Storage abre una sala solo con un esquema que cubre sus datos.** En `replay_wal_file` (WAL activo y `.wal.compacting`), una operación sobre una tabla que no está en el esquema es `StorageError::SchemaMismatch { room_id, table_id }` (variante nueva), no se saltea. En `load_snapshot`, una tabla del snapshot con filas que no está en el esquema también es `SchemaMismatch` (antes `fill_missing_tables` la conservaba); una tabla vacía que el esquema no tiene se descarta, porque el snapshot guarda todas las tablas del esquema con el que se compactó, aunque estén vacías. Así se exige un esquema que cubra los datos sin cambiar el formato en disco. El error sale al cargar el snapshot o al recorrer los WAL, antes de truncar un torn write, plegar un WAL rotado o reescribir un archivo, así que los datos quedan en disco para reabrir con el esquema correcto. Antes solo pueden pasar cosas inocuas: crear el directorio de datos, el `.lock` y el `.wal` si faltan, el `.snap` vacío si falta y borrar los `.snap.tmp.*` de compactaciones interrumpidas. Las columnas no se chequean: una fila escrita con más columnas que el esquema conserva sus valores. Solo aplica al motor de disco: el de memoria empieza vacío al reabrir. Alternativa descartada: persistir el `SchemaId` en el header del `.snap`; el cliente puede tener un esquema más nuevo con el mismo id, así que el id solo no alcanza, y los `table_id` del snapshot ya dan la información necesaria. | 2 | Adoptada |
| **D56** | **El roster se escribe cuando hace falta, no en cada cambio.** (a) `prune_to_retention_floor` escribe el roster solo si algún segmento termina por debajo del piso (`TieredLog::has_prunable_below(floor)`); si no, el cursor queda dirty para el tick rápido, y la poda de RAM del `HotBuffer` hasta el piso se hace igual. (b) `register_client` y `deregister_client` solo cambian memoria y marcan dirty (ya no devuelven `Result`). Se escribe en el tick rápido (1 s), al parar la sala o antes de cualquier poda que borre segmentos, que es la invariante que importa (D6 de la Fase 3.6, que también pedía escribir en el momento el registro y la baja; D56 deja eso sin efecto). Confirmado antes del lote: ninguna otra invariante depende de escribir el registro en el momento (la designación de quién sube el snapshot vive en memoria, la poda por TTL o cuota no usa los cursores y nada fuera del actor lee el roster). Costo aceptado: un registro perdido en un crash o un panic da `ClientNotRegistered` y el cliente se registra de nuevo (D50), pero mientras tanto su cursor no frena la poda, así que puede volver detrás del log y necesitar un snapshot. Una baja perdida deja un cliente que vence a `Disconnected`, que frena la poda proactiva hasta que el TTL o la cuota lo dejan detrás del log (o `dormant_after`), como cualquier cliente que se va sin darse de baja. | 3 | Adoptada |
| **D57** | **Dedup hidratado sin decodificar operaciones.** Al sellar un segmento se escribe `segment_{a}_{b}.ids` con `write_atomic`: el rango del segmento (`a` y `b`), cada `(mutation_id, seq)` en 24 bytes little-endian y un CRC32 de todo, así que un sidecar solo vale para su segmento. Los ids del segmento activo se guardan en memoria al escribirlo (y se recuperan al abrir), así que sellar no relee `active.wal`; si escribir el sidecar falla se registra un warning y el segmento queda sin él. La compresión a cold lo conserva (el nombre no depende del tier) y la poda lo borra antes que el segmento, así que un crash en el medio deja un segmento sin sidecar, nunca al revés. `read_newest` lee los `.ids` y decodifica el segmento solo si el sidecar falta, está dañado o es de otro segmento. `SegmentIndex::scan` los ignora y la cuota no los cuenta (24 bytes por operación). El `HotBuffer` se rehidrata solo con las operaciones de `active.wal`. Alternativa descartada: leer solo los primeros 17 bytes del payload de cada frame; el `mutation_id` quedaría sin verificar por el CRC del payload y un id dañado daría un falso duplicado. | 3 | Adoptada |
| **D58** | **Encarnación de la sala.** `create_room` genera un `room_incarnation: u128` aleatorio y lo guarda en `meta_room.json`. `Registered` lo devuelve y `RegisterClient` lleva `known_incarnation: Option<u128>`. Si un cliente se registra con cursor y una encarnación distinta (o sin encarnación), el servidor responde con un código nuevo, `RoomReset` (409, permanente para ese estado local): el cliente tiene que descartar su base local y registrarse sin cursor, como `Bootstrapping`. Sin cursor, la encarnación no se exige. Alternativa descartada: tratar al cliente como `Bootstrapping` sin avisarle; conservaría filas de la sala vieja y podría mezclarlas con el snapshot nuevo. Implementación: `Uuid::new_v4().as_u128()`, también cuando `RoomManager::get_or_spawn` con esquema crea el `meta_room.json`; reasignar el esquema o reabrir la conserva. `register` lo revisa antes que el head, sin tocar el roster. | 4 | Adoptada |
| **D59** | **Dedup por cliente.** La clave del dedup y lo que se guarda en el WAL pasa a ser `blake3(client_id ‖ mutation_id)` truncado a 16 bytes, con `client_id` prefijado por su longitud. El formato del frame no cambia (sigue siendo `Option<[u8; 16]>`). Un reintento del mismo cliente sigue siendo idempotente y un id repetido por otro cliente se secuencia como mutación nueva. El protocolo documenta igual que `MutationId` debe ser aleatorio de 128 bits. Implementación: `dedup_key` en `server/src/dedup.rs` (la longitud como `u64` little-endian, los 16 primeros bytes del hash); `commit` la usa para el dedup, el chequeo de tamaño y el WAL, así que los `.ids` de D57 también la guardan. | 4 | Adoptada |
| **D60** | **Versión de esquema en las respuestas.** El registry guarda `version: u64` en `schemas/{id}.json` (1 al crearse, +1 por cada `add_column`). `CommitAck`, `SyncBatch`, `HeartbeatAck` y `Registered` llevan `schema_version`. Un cliente que ve una versión mayor que la suya pide `GetSchema` antes de aplicar el batch. SSE sigue avisando, pero deja de ser la única señal. Implementación: la versión es parte del `Schema` de core (`version()`; `add_column` suma 1 solo si tiene éxito) y viaja en su serialización (`{"version", "tables"}`), así que `schemas/{id}.json` y el esquema de `Registered` la llevan sin un campo aparte. Un JSON sin `version` vale 1, la versión 0 es un error de deserialización y `register_schema` rechaza con `BadRequest` un esquema nuevo con otra versión (`Schema::FIRST_VERSION`). La regla del cliente quedó como "una versión distinta de la suya", no "mayor": una sala reasignada a otro esquema (`RoomManager::get_or_spawn` con esquema) puede bajarla. `RESPONSE_ENVELOPE_ALLOWANCE` suma un varint. | 4 | Adoptada |
| **D61** | **Una subida en curso ancla la retención.** El piso de poda pasa a ser `min(min_ack, snapshot usable, subida en curso no inactiva)`, con la seq de la subida que expone el relay. Si al completar la subida su seq quedó fuera del rango retenido, la respuesta es `SnapshotSuperseded` (reintentable), no `BadRequest`. Implementación: `SnapshotRelay::upload_in_progress` reemplaza a `has_upload_in_progress` y devuelve la seq de la subida por chunks no inactiva, o 0 (no se poda nada) mientras hay una subida de un solo request o el lock del relay está tomado; el cierre por inactividad y la designación siguen mirando solo si hay alguna. `LogBounds::check` recibe el error: `BadRequest` al empezar, `SnapshotSuperseded` al completar. | 4 | Adoptada |
| **D62** | **Un snapshot por debajo de un hueco deja de ser usable.** Un hueco en el log solo aparece por daño externo (un segmento corrupto o borrado, o DEF-120), así que la solución se limita al camino de error que ya existe. En la rama de hueco de `RoomActor::sync`, si el snapshot usable es `<= from_seq`, el actor lo guarda en `unrestorable_snapshot` (en memoria): restaurarlo lleva al mismo hueco. `usable_snapshot_seq` lo ignora, así que deja de anunciarse y de anclar la poda, la demanda se enciende y una subida nueva (con una seq mayor) lo reemplaza. Al reabrir la marca se pierde y el próximo cliente que choca con el hueco la vuelve a poner. Alternativa descartada (la propuesta original): `unreadable_through` en `TieredLog`, calculado al abrir y al leer, con `fetch_deltas` mutable; más código y estado en el log para un caso que solo existe con daño en disco. Tampoco se borra el snapshot del relay: ya no se usa y lo reemplaza la próxima subida o el TTL. | 5 | Adoptada |
| **D63** | **El hueco de secuencia se reporta antes que cualquier error de contenido.** `validate_batch` revisa en dos pasadas: primero la continuidad de todo el batch (`SequenceMismatch` ante un hueco, aunque el batch traiga además una tabla desconocida o una fila inválida) y después tabla y esquema de cada operación. Un hueco significa que el batch no corresponde al estado local y el cliente tiene que resincronizar; el error de contenido es secundario. Antes el orden era por operación (tabla, secuencia, fila), así que una operación inválida anterior al hueco lo tapaba. Documentado en el doc de `StorageEngine::apply_batch` y en `ARCHITECTURE.md` §8. | 2 | Adoptada |
| **D64** | **`create_dir_all_synced` se duplica en storage, no se mueve a core.** Core no hace E/S de archivos (es el crate puro del protocolo, también para wasm), storage ya tiene su `sys::sync_dir` y el servidor no depende de storage. El helper vive en `disk/compactor.rs` junto a `sync_parent`, es bloqueante y `open_room` lo llama por `run_blocking`; cada sync del padre de un directorio creado pasa por el fail point `sync_dir`. | 2 | Adoptada |
| **D65** | **`DiskStorageEngine::new` valida sus opciones.** Devuelve `Result<Self, StorageError>`: `compaction_ratio` tiene que ser finito y mayor que 0 y `zstd_level` estar en `zstd::compression_level_range()` (el rango real de la librería, con los niveles negativos rápidos); si no, `StorageError::InvalidOptions` (variante nueva). Se valida en el constructor porque los campos de `DiskStorageOptions` son públicos. El doc del builder que decía "1-22" se corrigió. | 2 | Adoptada |
| **D66** | **El servidor nunca asigna `u64::MAX`.** Así `head_seq + 1` (el tail de un log podado entero) siempre existe y `SequenceNumber::next` y el `+ 1` de `TieredLog::append` no cambian. Un commit con el head en `u64::MAX - 1` es `Internal` antes de escribir nada, y un log que dice tener `u64::MAX` (solo un archivo dañado) es `WalCorruption` al abrir; en `log_meta.json` se comprueba al leerlo, antes de que `SegmentIndex::scan` borre los segmentos que terminan por debajo. En storage, `validate_batch` usa `checked_add`: sin siguiente número, `SequenceMismatch`. El replay del WAL ya era seguro: salta las operaciones `<= head_seq` antes de calcular `head_seq + 1`, así que nunca suma sobre `u64::MAX`. | 5 | Adoptada |
| **D67** | **Comprobar si un archivo existe propaga el error.** `durable::exists(path)` (fail point `exists` y `try_exists`) reemplaza a `Path::exists` en los sitios de la auditoría: `cold_disk` (1), `warm_disk` (3) y `manager` (4; `room_exists` pasa a devolver `Result<bool>`). `schema_registry` usa `fs::metadata(&path)?.is_file()` (sigue los symlinks, como antes). En el manager las comprobaciones corren en `blocking_io`, y `delete_room` llama directamente a `remove_dir_all` y traduce `NotFound` a `RoomNotFound`. Sin `disallowed-methods` en clippy: los tests usan `.exists()` decenas de veces y storage tampoco lo tiene. El `is_dir` de `create_dir_all_synced` queda: un error ahí aparece al crear el directorio. | 5 | Adoptada |
| **D68** | **CI de MSRV, clippy en wasm32 y rustdoc.** Un job en ubuntu con una matriz de dos toolchains, cada uno con su caché: con 1.85.0, `cargo build --workspace --lib --bins`; con 1.88.0, `cargo test --workspace` (verificado antes de adoptarlo: el MSRV declarado, `rust-version` y el README, no cambia; los toolchains se fijan solo en la CI). El job wasm suma `cargo clippy --target wasm32-unknown-unknown -p zemdb-core -p zemdb-storage -p zemdb-client -- -D warnings`. Un job `Docs` corre `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`; los links a ítems privados (8, no 7: el Lote 1 sumó `STREAMS_PER_CLIENT` en `sse.rs`) pierden los corchetes en lugar de volver públicos los ítems. | 6 | Adoptada |

---

## 3. Resumen

| Orden | Lote | Ítems | Prioridad | Progreso |
|---|---|---|---|---|
| 1 | Borde HTTP acotado | DEF-116, 124 | Inmediata | ✅ 2/2 |
| 2 | Storage: esquema y durabilidad | DEF-115, 129, 130, 133, 141 | Alta (requisito de Fase 4) | ✅ 5/5 |
| 3 | Costo del roster y de abrir salas | DEF-117, 121, 122, 131 | Alta | ✅ 4/4 |
| 4 | Contrato del protocolo | DEF-118, 123, 127, 126 | Media (requisito de Fase 4) | ✅ 4/4 |
| 5 | Huecos del log y robustez del servidor | DEF-119, 120, 128, 125, 132, 143 | Media | ✅ 6/6 |
| 6 | Tests, documentación y CI | DEF-134, 140, 135, 136, 137, 138, 142, 139 | Baja | ✅ 8/8 |

**Avance total:** 29 de 29 ítems (DEF-143 surgió en la revisión del Lote 2).

**Por qué este orden, en una línea por lote:**
1. Es el único Alto y afecta al único binario desplegable; lo dispara cualquier miembro con un token válido.
2. Es la única pérdida de datos confirmados de la auditoría. El arreglo es chico y no cambia el formato en disco.
3. Aparece en régimen normal (Acks, reconexiones, salas que se reabren) y acerca al actor a los 504.
4. Cambia mensajes y códigos de error: tiene que entrar antes de escribir el SDK de la Fase 4.
5. Requiere disparadores poco frecuentes (segmentos dañados, borrados fallidos, cancelaciones) y ninguno pierde commits.
6. La documentación tiene que reflejar los cambios de los lotes 1, 4 y 5. Incluye lo que dejó desactualizado el refactor de calidad (`2a5f7b3..698e8d6`).

---

## Lote 1 — Borde HTTP acotado

> Objetivo: que ningún cliente, autenticado o no, pueda retener memoria o conexiones del servidor sin plazo.

#### DEF-116 · Alto · ✅ Hecho
**Problema.** Un cliente que no lee la respuesta (sin `WINDOW_UPDATE` en HTTP/2, o sin leer el socket en HTTP/1.1) deja la request en curso para siempre, con la conexión y el cuerpo codificado retenidos: hasta ~1,6 GB por conexión HTTP/2.
**Solución.** D53.
**Hecho.** `Activity` guarda desde cuándo espera cada respuesta (`waiting`, por request, y la escritura bloqueada bajo `SOCKET`), y `Activity::stalled` resuelve cuando la espera más vieja pasa el plazo; `serve_connection` cierra entonces la conexión. `TrackedBody::poll_frame` marca la espera cuando entrega un frame y la levanta cuando no tiene nada que entregar o terminó. La revisión encontró que el último frame retenido por h2 mantenía viva la codificación entera fuera del presupuesto y sin plazo; de ahí el último byte aparte, la copia de los frames con presupuesto y la espera del socket. El presupuesto es `AppState::response_budget` con `ResponseBudget` en las extensions; `budgeted_or_unavailable` lo usan `/sync` y `/snapshot/chunk`, y `commit` cae a un `CommitAck` sin catch-up. Tres settings nuevos en `ServerConfig`, validados y con variables de entorno. Diferencias con el plan: el plazo es por respuesta y no por conexión, el presupuesto cubre también `CommitAck`, y la variable es `ZEMDB_RESPONSE_STALL_TIMEOUT_SECS`. Los tests de integración usan el reloj real (como el resto de `http_edge_tests.rs`: con el reloj pausado los timeouts del servidor saltan mientras se esperan bytes del socket) con el plazo mínimo de 5 s; los de `Activity`, el reloj pausado. Comprobado con mutantes: sin la rama de plazo en `serve_connection` fallan los dos tests de desconexión; con el presupuesto que nunca se agota falla el de presupuesto; tomando la espera más reciente en lugar de la más vieja falla el de streams que se tapan; sin registrar la escritura bloqueada falla `a_blocked_write_stalls_even_with_a_request_in_progress`, y sin copiar los frames, `budgeted_bodies_hand_copies_with_the_last_byte_alone`. La CI de `d49269b` falló por timing en runners cargados, no por el código: el test HTTP/2 mandaba los `/sync` en secuencia y el plazo cerraba la conexión antes del último (ahora van en paralelo y se toleran los que no llegan a recibir headers); el HTTP/1.1 contaba desde el envío del request el tiempo de armar la respuesta (ahora espera los headers, como el HTTP/2); y el de presupuesto daba 5 s para volver a atender un `/sync` de ~10,5 MB (ahora 30 s). Comprobado con 4 binarios en paralelo.
**Test exigido.**
- HTTP/2 (`http2_client_that_stops_reading_is_disconnected`): 4 `/sync` de ~12,6 MB a la vez, sin leer (se quedan los que recibieron headers, al menos uno), con `max_connections=1` y el header timeout en 300 s para que solo el plazo pueda liberar la conexión; al vencer el plazo otra conexión es atendida y los cuerpos quedan cortados.
- Lo mismo en HTTP/1.1 (`http1_client_that_stops_reading_is_disconnected`).
- Un lector lento que avanza no se corta: `slow_reader_that_keeps_reading_outlasts_the_stall_timeout` lee ~1 MB/s una respuesta que dura más que el plazo, y `a_response_taken_frame_by_frame_does_not_stall` toma un frame cada medio plazo con reloj pausado. El ejemplo del plan (1 KiB cada pocos segundos) queda debajo del mínimo de un frame por plazo.
- Un stream SSE sin eventos durante más que el plazo no se corta (`open_sse_stream_outlives_every_timeout`, `a_response_with_nothing_to_send_does_not_stall`); no depende del keep-alive.
- Una respuesta trabada no queda tapada por otra que avanza (`one_stalled_response_is_not_hidden_by_another_that_progresses`), y una escritura bloqueada vence el plazo aunque haya una request en curso (`a_blocked_write_stalls_even_with_a_request_in_progress`).
- Un cuerpo con presupuesto entrega frames que no apuntan a la codificación, con el último byte solo; uno sin presupuesto no copia (`budgeted_bodies_hand_copies_with_the_last_byte_alone`).
- `responses_over_the_budget_in_flight_wait_for_held_bodies`: con el presupuesto mínimo y un cuerpo de ~10,5 MB retenido, otro `/sync` responde `Unavailable` con `Retry-After`, un commit se confirma sin catch-up y con `has_more`, y al soltarse el cuerpo retenido el siguiente `/sync` se atiende.

#### DEF-124 · Bajo · ✅ Hecho
**Problema.** Un solo miembro puede ocupar todas las conexiones del servidor con streams SSE.
**Solución.** D54.
**Hecho.** `SseStreams` y `SseSlot` en `api/sse.rs`; `room_events` toma el lugar antes de suscribirse y termina el stream con el apagado o con el reemplazo. Diferencias con el plan: el tercer stream de un cliente reemplaza al más viejo en lugar de recibir `RateLimited`, y el registro usa un `Mutex` en lugar de un `DashMap`. Comprobado con mutantes: sin reemplazo ni tope fallan los dos tests de integración y los dos unitarios; reemplazando al abrir en lugar de después de suscribirse falla `a_stream_that_fails_to_start_replaces_none`, y sin el límite de un reemplazo arrancando, `a_client_has_one_replacement_starting_at_a_time`.
**Test exigido.** Un tercer stream del mismo cliente en la misma sala cierra el más viejo y los otros dos siguen recibiendo eventos (`a_third_sse_stream_of_a_client_ends_its_oldest`); con `max_sse_streams = 2`, un stream de otra sala recibe `RateLimited`, una request normal es atendida y, al cerrar un stream, se libera su lugar (`sse_streams_over_their_limit_are_refused_and_leave_connections_for_requests`). Unitarios de `SseStreams`: el reemplazo ocurre recién cuando el stream nuevo arranca, no suma al total ni libera al soltarse el stream reemplazado, un stream que no arranca no reemplaza a ninguno, un cliente tiene un solo reemplazo arrancando a la vez, y el tope se mantiene hasta que cierra un stream vigente.

---

## Lote 2 — Storage: esquema y durabilidad

> Objetivo: que reabrir una sala nunca descarte operaciones confirmadas y que todo lo confirmado sobreviva a un corte de energía, antes de que exista el cliente de la Fase 4.

#### DEF-115 · Medio · ✅ Hecho
**Problema.** Reabrir una sala con un esquema al que le falta una tabla descarta en silencio sus operaciones del WAL, y la compactación las pierde para siempre.
**Solución.** D55.
**Hecho.** `ensure_known_table` en `disk/recovery.rs`, usado por `replay_wal_file` (que ahora recibe el `room_id`) y por `load_snapshot`; variante `StorageError::SchemaMismatch`. Documentado en el doc de `StorageEngine::open_room` y en `ARCHITECTURE.md` §8. Diferencias con el plan: los tests son solo del motor de disco, porque en el de memoria reabrir una sala la deja vacía y el escenario no existe; y la revisión llevó a aceptar las tablas vacías del snapshot que el esquema no tiene, que no tienen datos que perder. Comprobado con mutantes: salteando de nuevo las operaciones de tablas desconocidas fallan los tests del WAL activo y del rotado; conservando las tablas desconocidas del snapshot falla el del snapshot; y exigiendo también las tablas vacías falla el de la tabla vacía.
**Test exigido.** En `disk_wal_resilience_tests.rs`:
- `reopening_with_a_table_missing_from_the_wal_is_rejected`: el escenario de la auditoría (abrir con `{0, 1}`, un batch con una fila en cada tabla, cerrar, reabrir con `{0}`) da `SchemaMismatch { table_id: 1 }` y deja el `.snap` y el `.wal` byte a byte iguales; al reabrir con `{0, 1}` el head es 2 y la fila de la tabla 1 está.
- `reopening_with_a_table_missing_from_the_snapshot_is_rejected`: lo mismo después de compactar (WAL vacío).
- `reopening_with_a_table_missing_from_the_rotated_wal_is_rejected`: con las filas solo en un `.wal.compacting` dejado por una compactación interrumpida, el error es el mismo y ningún archivo cambia (el WAL rotado no se pliega).
- `an_empty_table_of_the_snapshot_does_not_have_to_be_in_the_schema`: una sala compactada con `{0, 1}` sin filas en la tabla 1 reabre con `{0}`.
- `reopening_with_a_new_table_keeps_the_data`: reabrir con una tabla nueva conserva las filas del snapshot y del WAL y acepta escrituras en la tabla nueva.

#### DEF-133 · Medio · ✅ Hecho
**Problema.** Ningún test cubre un hueco de secuencia dentro de un mismo batch: validar solo el primer op deja la suite en verde.
**Solución.** Solo el test (el código ya valida cada op). Va en este lote porque protege `validate_batch`, cuya falla dejaría una sala que no vuelve a abrir, el mismo tipo de daño que DEF-115. Al final el lote sí toca `validate_batch` (D63).
**Hecho.** Casos nuevos en el helper genérico `sequence_gap_is_rejected` (`tests/common/mod.rs`), que corren `test_disk_apply_batch_sequence_mismatch` y `test_sequence_mismatch_rejected`. Comprobado con el mutante de la auditoría (revisar solo la secuencia del primer op): falla.
**Test exigido.** `[insert(1), insert(3)]` devuelve `SequenceMismatch { expected: 2, actual: 3 }`; después de todos los casos el head sigue en 0 y la fila 1 no está, antes y después de cerrar y reabrir la sala, en los dos motores.

#### DEF-141 · Bajo · ✅ Hecho
**Problema.** El refactor de tests perdió la única cobertura de que `validate_batch` revisa la secuencia antes que el esquema: el helper compartido `sequence_gap_is_rejected` usa una fila válida.
**Solución.** D63: el orden es un contrato, y `validate_batch` revisa la secuencia de todo el batch antes que el contenido.
**Hecho.** `validate_batch` en dos pasadas (`memory/state.rs`), documentado en el doc de `StorageEngine::apply_batch` y en `ARCHITECTURE.md` §8. Diferencia con el plan: además del caso de una fila inválida con seq 42 (que el orden viejo ya cumplía y que protege contra mover `validate_operation` antes), un caso con una fila inválida en seq 1 seguida de seq 3, que solo cumplen las dos pasadas. Comprobado con un mutante que vuelve al orden por operación: falla.
**Test exigido.** En `sequence_gap_is_rejected`, en los dos motores: una fila inválida con seq 42 y `[fila inválida en 1, insert(3)]` dan `SequenceMismatch`, y la misma fila inválida en seq 1 sola da `SchemaValidation` (control de que la fila es inválida).

#### DEF-129 · Bajo · ✅ Hecho
**Problema.** `open_room` crea `data_dir` sin sincronizar su directorio padre.
**Solución.** D64: `create_dir_all_synced` duplicado en storage y usado en `open_room`. En wasm32 no aplica (el motor de disco no existe ahí).
**Hecho.** `disk/compactor.rs::create_dir_all_synced`, llamado por `open_room` con `run_blocking`. Como en el servidor, el fail point `sync_dir` inyecta una falla en lugar de registrar los directorios. Un `AlreadyExists` solo se acepta si el camino es un directorio (la copia del servidor tiene el mismo detalle; queda para el Lote 5, que toca su E/S). Un sync que falla no se reintenta en una apertura posterior, que encuentra el directorio creado; es la misma regla de GEMINI para un `fsync` fallido. Comprobado con mutantes: sin sincronizar falla el test del helper, y con `open_room` usando `create_dir_all` falla el suyo.
**Test exigido.** En `disk/tests/compactor.rs`: `create_dir_all_synced_creates_missing_components` (crea `a/b` y es idempotente) y `create_dir_all_synced_syncs_the_parent_of_every_new_directory` (armando el fail point en el padre de cada directorio nuevo, el helper falla). En `tests/disk.rs`: `open_room_syncs_the_data_directory_it_creates` (con el fail point en el padre del `data_dir`, `open_room` falla).

#### DEF-130 · Bajo · ✅ Hecho
**Problema.** `compaction_ratio` acepta NaN, 0 y negativos, y entonces se compacta en cada batch.
**Solución.** D65.
**Hecho.** `DiskStorageEngine::new` devuelve `Result`; los 32 sitios que lo llaman (todos en tests) pasaron a `.unwrap()`. Un ratio positivo minúsculo (`1e-300`) se acepta aunque en la práctica se comporte como 0; no se fija un piso. Comprobado con un mutante sin validación: falla.
**Test exigido.** `engine_options_that_cannot_work_are_rejected` (`compaction_lifecycle_tests.rs`): NaN, 0, -1 e infinito como ratio y un nivel por debajo y por encima del rango de zstd dan `InvalidOptions`; las opciones por defecto y un ratio de 0,5 con nivel 19 se aceptan.

---

## Lote 3 — Costo del roster y de abrir salas

> Objetivo: que un Ack, un registro o la reapertura de una sala no cuesten fsync ni lecturas proporcionales al tamaño del roster o de la historia.

#### DEF-117 · Medio · ✅ Hecho
**Problema.** Casi todo Ack reescribe el roster completo con 2 fsync aunque la poda no borre nada (~8,8 ms por Ack).
**Solución.** D56 (a).
**Hecho.** `has_prunable_below` en `TieredLog` y la condición en `prune_to_retention_floor`. Diferencia con el plan: en lugar de un contador de escrituras, el test comprueba que el archivo del roster no existe (con el registro ya sin escritura, cualquier escritura lo crearía). Comprobado con un mutante que siempre escribe: falla.
**Test exigido.**
- `acks_that_prune_no_segment_do_not_write_the_roster` (con el reloj pausado): 50 commits con Acks dentro de `active.wal` no escriben el roster.
- `pruning_on_ack_persists_cursor_before_deleting_segments`, el que ya existía, sigue en verde.

#### DEF-121 · Medio · ✅ Hecho
**Problema.** Cada registro reescribe el roster completo con 2 fsync; una tormenta de reconexiones cuesta O(N²).
**Solución.** D56 (b).
**Hecho.** `register_client` y `deregister_client` sin escritura ni `Result`; `save` absorbió a `write_roster`. Los tests que probaban que una escritura fallida dejaba el registro intacto se reemplazaron (ya no hay escritura en el registro): `interrupted_roster_save_keeps_previous_roster` ahora prueba que un `persist_if_dirty` fallido deja el roster anterior en disco y el cambio pendiente. Otros cinco tests suponían que el registro ya estaba en disco y se ajustaron; el del panic en `data_plane` ahora registra de nuevo al cliente, porque después de un panic el roster no se guarda.
**Test exigido.**
- `membership_changes_are_written_only_when_flushed`: registrar 1.000 clientes no crea el archivo; un `persist_if_dirty` los escribe todos de una vez; una baja tampoco escribe hasta el siguiente.
- `registration_lost_in_a_crash_is_made_again`: después de un panic (que no guarda el roster, como un crash) el cliente recibe `ClientNotRegistered` y se registra de nuevo.
- Un registro seguido de una poda que borra segmentos: lo cubre `pruning_on_ack_persists_cursor_before_deleting_segments`, que registra sin escribir y comprueba que el roster en disco tiene al cliente después de la poda.

#### DEF-122 · Medio · ✅ Hecho
**Problema.** Reabrir una sala decodifica operaciones completas solo para hidratar el dedup (24× más lento con operaciones de 20 KB), y el `HotBuffer` se llena con operaciones viejas.
**Solución.** D57.
**Hecho.** `mutation_ids_path`, `encode_mutation_ids` y `decode_mutation_ids` en `segment_index.rs`; `WarmDiskLog` guarda los ids del segmento activo y escribe el sidecar al sellar; `read_newest` y `segment_mutations` en `tiered_log.rs` (se eliminó `RamFill`); la poda borra el sidecar antes que el segmento. `open_tolerates_a_corrupt_newest_segment` ahora borra el sidecar del segmento dañado, para seguir probando el camino que lo decodifica, y `reopening_loads_into_ram_only_what_fits` usa un `active.wal` más grande que la RAM, para que la apertura tenga que recortar. La revisión llevó a guardar el rango del segmento en el sidecar (un archivo de 4 bytes en cero pasaba el CRC como una lista vacía). Comprobado con mutantes: sin escribir el sidecar, sin la decodificación de respaldo, sin borrar el sidecar en la poda, sin recuperar los ids de `active.wal` al abrir y sin verificar el rango, falla cada test correspondiente.
**Test exigido.** En `log/tests/tiered_log.rs`:
- `reopen_reads_the_mutation_ids_of_sealed_segments_without_decoding_them`: con diez segmentos ya comprimidos a cold, reabrir con una ventana de 7 no lee ningún segmento (`IoEvent::SegmentRead`), recupera los 7 ids correctos y el `HotBuffer` tiene solo 51..=53 (`active.wal`). Como los segmentos son cold, prueba también que los `.ids` sobreviven a la compresión.
- `reopen_decodes_a_segment_whose_mutation_ids_are_missing_or_invalid`: con el sidecar de un segmento reemplazado por el de otro (CRC válido), otro en cero y un tercero borrado, la apertura decodifica esos tres segmentos y recupera los mismos ids.
- `ids_written_before_a_reopen_reach_the_sidecar`: un segmento que se sella después de reabrir incluye en su sidecar los ids escritos antes.
- `pruning_a_segment_deletes_its_mutation_ids`.
- `opening_fills_ram_only_from_active_wal`, y en `actor/tests/room.rs` `reopening_hydrates_dedup_without_reading_segments` (antes `reopening_reads_only_the_segments_needed_for_ram_and_dedup`), que ahora espera 0 lecturas y sigue comprobando que el dedup reconoce los reintentos.

#### DEF-131 · Medio · ✅ Hecho
**Problema.** Ningún test cubre que un cliente `Bootstrapping` no frene la poda.
**Solución.** Solo los tests. Van en este lote porque D56 cambia cuándo y cómo se persiste el roster que define ese estado.
**Hecho.** `bootstrapping_clients_do_not_hold_back_pruning` (`lease.rs`) y `a_bootstrapping_client_does_not_hold_back_pruning` (`room.rs`). Los dos fallan con el mutante de la auditoría.
**Test exigido.**
- Unitario en `actor/tests/lease.rs`: `active` con `Some(20)`, `newbie` con `None` (tail 10); `newbie` es `Bootstrapping` y `min_connected_ack_seq() == Some(20)`.
- En `actor/tests/room.rs`: con un `Bootstrapping` presente, el Ack de un `Connected` poda los segmentos.
- Comprobar que fallan con el mutante de la auditoría.

---

## Lote 4 — Contrato del protocolo

> Objetivo: que el contrato que va a implementar el SDK de la Fase 4 no permita divergencias silenciosas: ni cursores de otra sala, ni ids de otro cliente, ni esquemas desactualizados, ni subidas que fallan sin poder reintentarse.

#### DEF-118 · Medio · ✅ Hecho
**Problema.** Una sala recreada con el mismo id acepta el cursor de la sala borrada.
**Solución.** D58. Actualizar `ARCHITECTURE.md` (§ de onboarding y tabla de códigos de error).
**Hecho.** `RoomMetadata::room_incarnation`, `RoomActor::open`/`spawn` la reciben, `ServerError::RoomReset` (409) y `ErrorCode::RoomReset`. `ARCHITECTURE.md` §6 (*Room incarnation*), §7.1, §7.2 y la tabla de códigos. Los helpers de registro con cursor de los tests de la sala pasan la encarnación del `Fixture`. Tests: `recreated_room_resets_the_cursors_of_the_deleted_one` (integración HTTP: la sala nueva llega a 3, así que el cursor 5 con la encarnación vieja o sin encarnación prueba además que `RoomReset` va antes que `InvalidSequence`, la segunda rama de la auditoría; no entra al roster; sin cursor queda en 0; con la encarnación nueva, en 3) y `room_incarnation_survives_a_schema_change_and_a_restart` (`actor/tests/manager.rs`). Diferencia con el test exigido: registrado sin cursor, `old` no queda `Bootstrapping` sino `Connected` en 0, porque la sala recreada no está podada (sería `Bootstrapping` con el log podado, como cualquier cliente nuevo); el test comprueba que su cursor pasa a 0. Comprobado con mutantes: sin el chequeo en `register` y generando una encarnación nueva al reasignar el esquema, falla el test correspondiente.
**Test exigido.**
- El escenario de la auditoría: `old` se registra con `Some(5)` y la encarnación vieja en la sala recreada y recibe `RoomReset`; registrado sin cursor queda `Bootstrapping`.
- Con la encarnación correcta, el registro con cursor funciona igual que hoy.
- La encarnación sobrevive a un reinicio del servidor (está en `meta_room.json`).

#### DEF-123 · Bajo · ✅ Hecho
**Problema.** El dedup es por sala: un `MutationId` que ya usó otro cliente se confirma con la seq ajena y la operación no se aplica.
**Solución.** D59.
**Hecho.** `dedup_key` y su uso en `RoomActor::commit`; doc de `MutationId` y `ARCHITECTURE.md` §5.1. `the_same_mutation_id_from_two_clients_is_two_mutations` (`actor/tests/room.rs`) cubre los dos puntos: con segmentos de 2 operaciones, las dos primeras quedan en un segmento sellado (ids en su `.ids`) y la tercera en `active.wal`; los reintentos devuelven su seq antes y después de reabrir. Falla con el mutante que usa el `MutationId` como clave (el código anterior).
**Test exigido.**
- Dos clientes con el mismo `MutationId` obtienen seqs distintas y las dos operaciones se aplican.
- El reintento de un mismo cliente sigue devolviendo su seq original, también después de reabrir la sala (dedup hidratado desde el WAL y desde los `.ids` de D57).

#### DEF-127 · Bajo · ✅ Hecho
**Problema.** Un cambio de esquema solo se avisa por SSE; ninguna respuesta del protocolo lo señala.
**Solución.** D60. Documentar en `ARCHITECTURE.md` la regla del SDK (ver una versión mayor → `GetSchema` antes de aplicar).
**Hecho.** `Schema::version`, `schema_version` en `CommitAck`, `SyncBatch` y `HeartbeatAck`, y el chequeo de `register_schema`. Regla del SDK en `ARCHITECTURE.md` §3.1 (*Schema Version*) y en el doc de los mensajes. Tests: `schema_version_counts_every_added_column_and_survives_a_restart` y `new_schema_must_start_at_version_one` (`tests/schema_registry.rs`); `add_column_waits_for_a_concurrent_add_column_on_the_same_schema` ahora exige versión 3 (dos `add_column` concurrentes no pierden un incremento); `schema_add_column_evolves_the_table_and_keeps_the_indexes` (core) comprueba que un cambio rechazado no la sube; `error_codes_keep_their_wire_indexes` fija `RoomReset` en el índice 20 y la tabla de `tests/error.rs` lo mapea a 409; `valid_schema_round_trips_in_json_and_binary` rechaza la versión 0; `schema_evolution_cascades_to_active_room` (integración) comprueba que `Registered` trae la versión 1 y, después del `add_column`, `HeartbeatAck`, `SyncBatch` y `CommitAck` traen la 2. Comprobado con mutantes: sin el incremento y con una versión fija en las respuestas, fallan. No detectable: quitar el varint de `RESPONSE_ENVELOPE_ALLOWANCE`, porque el margen ya tiene 12 bytes de holgura (tag de variante y longitud del room id acotados por arriba); `largest_accepted_operation_fits_alone_in_any_response` codifica las respuestas con `schema_version: u64::MAX` y pasa.
**Test exigido.**
- `schema_version` empieza en 1 y sube con cada `add_column`, también después de reiniciar el registry.
- Después de un `add_column`, `HeartbeatAck`, `SyncBatch` y `CommitAck` de una sala con ese esquema llevan la versión nueva.

#### DEF-126 · Bajo · ✅ Hecho
**Problema.** Una subida por chunks no ancla la retención y puede fallar al completarse con `BadRequest`, que no se reintenta.
**Solución.** D61.
**Hecho.** `upload_in_progress` en `relay/upload.rs`, el piso de `prune_to_retention_floor` y `LogBounds::check` con el error como parámetro. `ARCHITECTURE.md` §5.2 (*Snapshot Retention Anchor*), §6 y la aceptación del relay. Tests: `snapshot_upload_in_progress_anchors_pruning_until_it_goes_idle` (`actor/tests/room.rs`): una subida en 4 que pasa `UPLOAD_IDLE_TIMEOUT` sin chunks no frena la poda; una en 10, con commits y Acks hasta 16, deja la cola en 11 o menos y se completa. `multipart_upload_checks_the_log_range_when_it_starts_and_completes` (relay, antes `multipart_completion_rechecks_the_log_range`) ahora espera `BadRequest` al empezar y `SnapshotSuperseded` al completar, y los dos tests de `has_upload_in_progress` comprueban la seq que devuelve. El escenario de la auditoría se corrió en otro orden (primero la subida inactiva) porque un snapshot activo también ancla y taparía la segunda parte. Comprobado con mutantes: sin la subida en el piso, sin el filtro de inactividad, con `BadRequest` al completar, con `SnapshotSuperseded` al empezar y sin contar la subida de un solo request, falla cada test correspondiente.
**Test exigido.**
- El escenario de la auditoría (subida en S=4, commits y Acks hasta 10): la poda no pasa de 4 mientras la subida está activa, y la subida se completa.
- Una subida inactiva (vencida) deja de anclar.
- Si el rango igual se movió (TTL o cuota), la respuesta es `SnapshotSuperseded`.

---

## Lote 5 — Huecos del log y robustez del servidor

> Objetivo: que un hueco en el log siempre termine en un snapshot nuevo, que un borrado fallido no lo cree, y que los errores de I/O y las cancelaciones no dejen salas a medias.

#### DEF-119 · Medio · ✅ Hecho
**Problema.** Un snapshot "usable" por debajo de un hueco del log apaga la demanda de snapshots, y los clientes quedan en bucle hasta que el snapshot vence.
**Solución.** D62.
**Hecho.** `unrestorable_snapshot` en `RoomActor`, marcado en la rama de hueco de `sync` y filtrado en `usable_snapshot_seq`. `ARCHITECTURE.md` §5.2 (*Damaged Segments*). Test: `snapshot_below_a_gap_stops_being_usable` (`actor/tests/room.rs`): con un snapshot en 2 y el segmento 3–4 borrado, un cliente en 2 recibe `BehindCompaction`; después el `HeartbeatAck` ya no ofrece el snapshot y el writer (cursor 6) queda designado; un snapshot en 6 se ofrece, también después de que otro `sync` choque con el hueco. Diferencia con el plan: el tercer test (recalcular al abrir) no aplica con D62 reducida. Comprobado con mutantes: sin el filtro de `usable_snapshot_seq`, con `<` en lugar de `<=` y sin la condición `<= from_seq`, falla.
**Test exigido.**
- El escenario de la auditoría (`audit_gap_after_usable_snapshot_never_turns_demand_on`): después del `BehindCompaction` por hueco, el snapshot viejo deja de anunciarse, la demanda se enciende y un `Connected` es designado.
- Un snapshot nuevo por encima del hueco vuelve a ser usable y se sirve.
- Al reabrir la sala con el hueco en el índice, `unreadable_through` se recalcula.

#### DEF-120 · Bajo · ✅ Hecho
**Problema.** Un segmento warm que no se pudo borrar reaparece después de podar su cold y hace retroceder `tail_seq`.
**Solución.** `SegmentIndex::scan` descarta (y borra) los segmentos con `end_seq <= pruned_through_seq`; `record_compression` guarda la ruta del warm que no se pudo borrar y el próximo `expire`/`prune_segments` reintenta el borrado.
**Hecho.** `SegmentIndex::scan(dir, pruned_through_seq)` borra esos segmentos y sus `.ids`; `TieredLog::undeleted_warm` guarda el warm que no se pudo borrar y `expire` lo reintenta en cada pasada. La compresión borra el warm con `durable::remove_file_if_exists`, que tiene el fail point `remove_file`. `ARCHITECTURE.md` §5.2 (Tier 3). Test: `warm_copy_that_could_not_be_removed_is_removed_later` (`log/tests/tiered_log.rs`), con las dos partes. Comprobado con mutantes: sin el filtro de `scan` y sin guardar la ruta, falla.
**Test exigido.** Con un fail point que hace fallar el `remove_file` del warm: comprimir, podar el cold, reabrir; `tail_seq` no retrocede y el warm ya no está. Sin reinicio, el siguiente mantenimiento borra el warm.

#### DEF-128 · Bajo · ✅ Hecho
**Problema.** `Path::exists()` trata un error de I/O como "no existe" en el log y el manager del servidor.
**Solución.** D67.
**Hecho.** `durable::exists` en los sitios con `exists()` (en `delete_room`, `remove_dir_all` directo) y `fs::metadata` en `schema_registry`. `ARCHITECTURE.md` §5.2 (*Atomic Server Metadata*). Tests: `failure_to_check_for_active_wal_fails_the_open` (`log/tests/tiered_log.rs`) y `failure_to_check_for_meta_room_does_not_create_the_room_again` (`actor/tests/manager.rs`). Comprobado con el mutante que toma el error por un archivo inexistente (el comportamiento anterior): fallan los dos.
**Test exigido.** Con el fail point devolviendo error: abrir una sala con `active.wal` falla en vez de abrir con el log vacío, y `create_room` no sobrescribe `meta_room.json`.

#### DEF-125 · Bajo · ✅ Hecho
**Problema.** Un `DELETE /admin/rooms/:id` cancelado a mitad del purge deja la sala viva pero sin snapshot.
**Solución.** El handler corre `delete_room` en `tokio::spawn` y espera el `JoinHandle` (como `add_column`); el doc de `RoomManager::delete_room` aclara que no es seguro cancelarla.
**Hecho.** `control_plane::delete_room` con `tokio::spawn` (un `JoinError` es `Internal`) y el doc de `RoomManager::delete_room`. `ARCHITECTURE.md` §7.1 corrige además la línea de `DELETE /admin/rooms`, que decía "Archives or purges". Test: `room_deletion_finishes_when_the_request_is_dropped` (`api/tests/control_plane.rs`), que suelta la request en un hook nuevo, `relay_purge_room_files`, dentro de `purge_room`. Falla con el handler anterior.
**Test exigido.** El handler se descarta durante el purge (el test de la auditoría, ahora sobre el handler HTTP): el borrado termina igual, el directorio desaparece y `room_exists` es `false`.

#### DEF-132 · Medio · ✅ Hecho
**Problema.** Ningún test cubre que la poda por TTL o cuota se detenga en el primer segmento conservado.
**Solución.** Solo el test. Va en este lote porque D62 y DEF-120 cambian la poda y el índice de segmentos.
**Hecho.** `quota_pruning_stops_at_the_first_segment_it_keeps` (`log/tests/tiered_log.rs`). Falla con `break` → `continue`.
**Test exigido.** El de la auditoría: corromper `segment_1_5.wal`, mantenimiento, `max_room_disk_bytes = 1`, mantenimiento otra vez; `segment_6_10.wal.zst` sigue existiendo. Comprobar que falla con `break` → `continue`.

#### DEF-143 · Bajo · ✅ Hecho
**Problema.** `validate_batch` (`storage/src/memory/state.rs`) y el replay del WAL (`storage/src/disk/recovery.rs`) calculan `head_seq + 1` sin control: con el head en `u64::MAX - 1` un batch válido entra en pánico en debug, y con el head en `u64::MAX` el número da la vuelta a 0 en release. A ese head solo se llega con un snapshot falsificado o un archivo dañado. Lo encontró la revisión del Lote 2; ya existía.
**Solución.** `checked_add`: sin siguiente número, el batch es `SequenceMismatch` y el replay lo reporta como corrupción. El servidor tiene la misma cuenta en `TieredLog::append` (`head_seq.get() + 1`) y en `SequenceNumber::next` (`core/src/id.rs`).
**Hecho.** D66. Tests: `batch_past_the_last_sequence_number_is_a_mismatch` (`storage/src/memory/tests/state.rs`, con el head en `u64::MAX` y en `u64::MAX - 1` con dos operaciones; el código anterior entra en pánico por overflow), `log_claiming_the_last_sequence_number_fails_to_open` (en `log_meta.json`, sin borrar el segmento que hay, y en el nombre de un segmento) y `commit_that_would_take_the_last_sequence_number_is_refused`. Diferencia con el plan: el replay del WAL no cambió (ver D66). Comprobado con mutantes: sin cada guarda del servidor, falla su test.
**Test exigido.** Unitario con el head en `u64::MAX`: un batch da error, sin pánico.

---

## Lote 6 — Tests, documentación y CI

> Objetivo: tests que prueben lo que dicen, documentación que coincida con el código después de los lotes anteriores, y una CI que verifique lo que el proyecto declara.

#### DEF-134 · Bajo · ✅ Hecho
**Problema.** Ningún test comprueba que la compresión a cold corra fuera del worker. El refactor de calidad borró el test que lo prometía sin probarlo, pero el hueco sigue.
**Solución.** Un fail point en `compress_warm_segment_sync` registra el `ThreadId`; un test nuevo, con runtime `current_thread`, comprueba que es distinto del suyo.
**Hecho.** Hook `compress_warm_segment` en `compress_warm_segment_sync` (con el `arm_hook` existente). Test: `compression_runs_off_the_runtime_thread` (`log/tests/tiered_log.rs`), que corre el mantenimiento en el runtime `current_thread` de `run_maintenance_sync`. Falla con el mutante que comprime inline.
**Test exigido.** El propio test, que tiene que fallar si se comprime inline (el mutante de la auditoría).

#### DEF-140 · Bajo · ✅ Hecho
**Problema.** `control_plane_room_lifecycle_overrides` compara la política efectiva contra una config igual a los defaults de la política, así que no detecta que la config del servidor se ignore (debilitado por los fixtures compartidos del refactor).
**Solución.** Arrancar el test con `TestServer::start(|c| { c.lease_timeout_secs = 60; c.snapshot_demand_ttl_secs = 60; })` y comparar contra los literales.
**Hecho.** `control_plane_room_lifecycle_overrides` arranca con `lease_timeout_secs = 60` y `snapshot_demand_ttl_secs = 60` y compara contra los literales. Falla con `RoomLifecyclePolicy::default()` en `RoomManager::new`.
**Test exigido.** El propio test, que tiene que fallar si `RoomManager` usa `RoomLifecyclePolicy::default()` en lugar de `config.default_lifecycle_policy()`.

#### DEF-135 · Bajo · ✅ Hecho
**Problema.** `ROADMAP.md` contradice el código (HMAC-SHA256, `/snapshot-chunk`, `Dormant` → `BehindCompaction`, `CryptoConcurrencyBounds` pendiente).
**Solución.** Corregir esas líneas y marcar §4.3 como diseño histórico que remite a `ARCHITECTURE.md`. Agregar la Fase 3.8 al encabezado y al cronograma.
**Hecho.** `ROADMAP.md`: la verificación de tokens es BLAKE3 con clave derivada; los endpoints del relay son `/snapshot/upload`, `/snapshot/upload-chunk` y `/snapshot/chunk`; `CryptoConcurrencyBounds` sale de la Fase 4, donde figuraba como pendiente (existe en `zemdb-core` y la Fase 2.8 ya lo daba por hecho); un `Dormant` que vuelve con el cursor detrás queda `Bootstrapping` y restaura el snapshot del relay. §4.3 lleva una nota de diseño histórico que remite a `ARCHITECTURE.md`, y la Fase 3.8 está en el encabezado, el resumen y el cronograma.
**Test exigido.** Ninguno (documentación). Revisión cruzada contra el código.

#### DEF-136 · Bajo · ✅ Hecho
**Problema.** `ARCHITECTURE.md:355` promete portabilidad total de snapshots sin la salvedad de Zstd en wasm32.
**Solución.** Agregar la limitación en §8, con referencia a la Fase 4 (DEF-54, diferido).
**Hecho.** `ARCHITECTURE.md` §8 (*Universal Canonical Snapshot Envelope*): wasm32 escribe snapshots sin comprimir y rechaza los Zstd, con la Fase 4 como plazo; deja de decir "100% portability".
**Test exigido.** Ninguno.

#### DEF-137 · Bajo · ✅ Hecho
**Problema.** Quedan afirmaciones sobre la línea de caché en `core/src/value/row.rs:12` y `storage/src/disk/format.rs:14`.
**Solución.** Dejar solo los tamaños. De paso, cerrar los residuos de los reportes de calidad que siguen abiertos: `mutation/op.rs:60` ("zero heap pointers"), `wal_frame.rs:66` ("little-endian" para el magic) y los tres comentarios desactualizados en tests.
**Hecho.** `row.rs` y `format.rs` dicen solo los tamaños. Los demás residuos ya estaban corregidos: `op.rs` dice "88 bytes" sin "zero heap pointers", el magic de `wal_frame.rs` es `[0xBA, 0x7C]` sin "little-endian", y los comentarios desactualizados de tests que listaban los informes de calidad (`room_actor_tests` con `Unauthorized` y "u64::MAX or 999", el de `spawn_blocking` en `tiered_log_tests`, y los de `storage/tests`) ya no existen. La auditoría no los nombra; son los del informe de calidad de `server/tests`, ya corregidos.
**Test exigido.** Ninguno.

#### DEF-138 · Bajo · ✅ Hecho
**Problema.** La doc pública enlaza a 7 ítems privados (uno nuevo del refactor: `compactor::compact_room_cow` en `disk/mod.rs`).
**Solución.** Quitar los corchetes (o hacer públicos los ítems si corresponde) y agregar a la CI `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
**Hecho.** D68. Los 8 links quedan como nombres en backticks; `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` pasa limpio.
**Test exigido.** El paso nuevo de la CI en verde.

#### DEF-142 · Bajo · ✅ Hecho
**Problema.** Documentación que el refactor dejó desactualizada: el doc de `RoomSnapshotPayload` (`storage/src/memory/state.rs`) dice que lo serializa la compactación; `ARCHITECTURE.md:26` lista `as_slice()`, que ya no existe; en `server/src/actor/room.rs` el comentario de `panic_message` quedó arriba de `room_dir`; `ROADMAP.md:73` menciona `with_default_origin`.
**Solución.** Corregir el doc de `RoomSnapshotPayload` y quitarle el `Serialize` que ya no se usa; `as_slice()` → `values()`; devolver el comentario a `panic_message`; anotar en el ROADMAP que `with_default_origin` se eliminó.
**Hecho.** `RoomSnapshotPayload` solo deriva `Deserialize`, con el doc corregido (la compactación escribe con `RoomSnapshotRef`); `cargo build --workspace --all-targets` compila. `ARCHITECTURE.md` cita `values()` en lugar de `as_slice()`; el comentario volvió a `panic_message`; `ROADMAP.md` anota que `with_default_origin` y los campos de origen de `SequencedOperation` se eliminaron.
**Test exigido.** Ninguno. `cargo build --workspace --all-targets` verifica que nada serialice `RoomSnapshotPayload`.

#### DEF-139 · Bajo · ✅ Hecho
**Problema.** La CI no verifica el MSRV declarado ni corre clippy sobre wasm32.
**Solución.** Un job con 1.85 (`cargo build --workspace --lib --bins`) y otro con 1.88 (`cargo test --workspace`); en el job wasm, `cargo clippy --target wasm32-unknown-unknown -p zemdb-core -p zemdb-storage -p zemdb-client -- -D warnings`.
**Hecho.** D68. Verificado localmente: `cargo +1.85.0 build --workspace --lib --bins` y `cargo +1.88.0 test --workspace` (715 tests) pasan, y clippy en wasm32 está limpio. El README aclara que la CI comprueba las dos versiones.
**Test exigido.** Los jobs nuevos en verde. Si los lotes anteriores subieron el MSRV real, actualizar `rust-version` y el README en el mismo commit.

---

## Fuera de este plan

Siguen abiertos y diferidos, sin cambios respecto de sus planes:
- **DEF-85** (ventana de dedup limitada al log retenido más la LRU). D57 y D59 no la amplían. Un cliente sigue sin poder reconocer su propio commit en un sync porque `SequencedOperation` no lleva `mutation_id`; conviene decidirlo al diseñar la outbox de la Fase 4.
- **DEF-34** (`reload_schema` en storage). D55 lo hace más estricto pero no lo implementa.
- **DEF-81** (contenido de snapshots subidos por miembros maliciosos) y **DEF-54** (Zstd en wasm32).
- **Residuo de DEF-95:** una conexión SSE semiabierta (sin FIN) retiene su permiso hasta que TCP falla: el keep-alive manda pocos bytes y tarda días en llenar el buffer del socket, recién entonces D53 la corta. D54 lo acota: como mucho 2 por cliente y sala, y el cliente que vuelve a conectarse reemplaza la más vieja.
