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
| **D55** | **Storage abre una sala solo con un esquema que cubre sus datos.** En `replay_wal_file`, una operación sobre una tabla que no está en el esquema es `StorageError::SchemaMismatch { table_id }` (variante nueva), no se saltea. En `recover_room`, un snapshot con una tabla que no está en el esquema también es `SchemaMismatch`. Como el snapshot guarda todas las tablas del esquema con el que se compactó (aunque estén vacías), esto alcanza para exigir un superconjunto sin cambiar el formato en disco. Alternativa descartada: persistir el `SchemaId` en el header del `.snap`; el cliente puede tener un esquema más nuevo con el mismo id, así que el id solo no alcanza, y los `table_id` del snapshot ya dan la información necesaria. | 2 | Propuesta |
| **D56** | **El roster se escribe cuando hace falta, no en cada cambio.** (a) `prune_to_retention_floor` persiste el roster solo si algún segmento termina por debajo del piso (`TieredLog::has_prunable_below(floor)`); si no, el cursor queda dirty para el tick rápido. (b) `register_client` y `deregister_client` marcan el roster dirty en vez de escribirlo. Se escribe en el tick rápido (1 s) o antes de cualquier poda que borre segmentos, que es la invariante que importa (D27: el roster en disco nunca queda detrás del log retenido). Un cliente cuyo registro se perdió en un crash recibe `ClientNotRegistered` y se registra de nuevo (D50), y uno cuya baja se perdió reaparece como `Disconnected` y pasa a `Dormant`, que no frena la poda. **A confirmar antes del lote:** recorrer los usos del roster y comprobar que ninguna otra invariante depende de la escritura síncrona del registro; si alguna depende, se mantiene la escritura síncrona solo para el alta de un cliente nuevo, y el re-registro de uno presente marca dirty. | 3 | Propuesta |
| **D57** | **Dedup hidratado sin decodificar operaciones.** Al sellar un segmento (rotación), se escribe al lado `segment_{a}_{b}.ids`: los pares `(mutation_id, seq)` del segmento, con CRC32, escritos con `write_atomic`. La compresión a cold lo conserva y la poda lo borra junto con el segmento. `read_newest` lee primero los `.ids` y solo decodifica el segmento entero si el sidecar falta o está dañado (por ejemplo, segmentos de antes del cambio). `active.wal` se sigue leyendo entero porque ya se inspecciona al abrir. El `HotBuffer` se rehidrata solo con las operaciones de `active.wal`, no con segmentos sellados. Alternativa descartada: leer solo los primeros 17 bytes del payload de cada frame; el `mutation_id` quedaría sin verificar por el CRC del payload y un id dañado daría un falso duplicado. | 3 | Propuesta |
| **D58** | **Encarnación de la sala.** `create_room` genera un `room_incarnation: u128` aleatorio y lo guarda en `meta_room.json`. `Registered` lo devuelve y `RegisterClient` lleva `known_incarnation: Option<u128>`. Si un cliente se registra con cursor y una encarnación distinta (o sin encarnación), el servidor responde con un código nuevo, `RoomReset` (409, permanente para ese estado local): el cliente tiene que descartar su base local y registrarse sin cursor, como `Bootstrapping`. Sin cursor, la encarnación no se exige. Alternativa descartada: tratar al cliente como `Bootstrapping` sin avisarle; conservaría filas de la sala vieja y podría mezclarlas con el snapshot nuevo. | 4 | Propuesta |
| **D59** | **Dedup por cliente.** La clave del dedup y lo que se guarda en el WAL pasa a ser `blake3(client_id ‖ mutation_id)` truncado a 16 bytes, con `client_id` prefijado por su longitud. El formato del frame no cambia (sigue siendo `Option<[u8; 16]>`). Un reintento del mismo cliente sigue siendo idempotente y un id repetido por otro cliente se secuencia como mutación nueva. El protocolo documenta igual que `MutationId` debe ser aleatorio de 128 bits. | 4 | Propuesta |
| **D60** | **Versión de esquema en las respuestas.** El registry guarda `version: u64` en `schemas/{id}.json` (1 al crearse, +1 por cada `add_column`). `CommitAck`, `SyncBatch`, `HeartbeatAck` y `Registered` llevan `schema_version`. Un cliente que ve una versión mayor que la suya pide `GetSchema` antes de aplicar el batch. SSE sigue avisando, pero deja de ser la única señal. | 4 | Propuesta |
| **D61** | **Una subida en curso ancla la retención.** El piso de poda pasa a ser `min(min_ack, snapshot usable, subida en curso no inactiva)`, con la seq de la subida que expone el relay. Si al completar la subida su seq quedó fuera del rango retenido, la respuesta es `SnapshotSuperseded` (reintentable), no `BadRequest`. | 4 | Propuesta |
| **D62** | **El log recuerda su rango ilegible.** `TieredLog` guarda en memoria `unreadable_through: Option<SequenceNumber>`, el fin del último hueco conocido: se calcula al abrir (huecos del índice de segmentos) y se actualiza cuando una lectura encuentra un hueco. `is_usable_snapshot` exige además `seq >= unreadable_through`. En la rama de hueco de `handle_sync`, si el snapshot activo no lo supera, se descarta del relay antes de `request_snapshot()`, así que la demanda se enciende y el ancla deja de retener el hueco. | 5 | Propuesta |

---

## 3. Resumen

| Orden | Lote | Ítems | Prioridad | Progreso |
|---|---|---|---|---|
| 1 | Borde HTTP acotado | DEF-116, 124 | Inmediata | ✅ 2/2 |
| 2 | Storage: esquema y durabilidad | DEF-115, 129, 130, 133, 141 | Alta (requisito de Fase 4) | ⬜ 0/5 |
| 3 | Costo del roster y de abrir salas | DEF-117, 121, 122, 131 | Alta | ⬜ 0/4 |
| 4 | Contrato del protocolo | DEF-118, 123, 127, 126 | Media (requisito de Fase 4) | ⬜ 0/4 |
| 5 | Huecos del log y robustez del servidor | DEF-119, 120, 128, 125, 132 | Media | ⬜ 0/5 |
| 6 | Tests, documentación y CI | DEF-134, 140, 135, 136, 137, 138, 142, 139 | Baja | ⬜ 0/8 |

**Avance total:** 2 de 28 ítems.

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

#### DEF-115 · Medio · ⬜ Pendiente
**Problema.** Reabrir una sala con un esquema al que le falta una tabla descarta en silencio sus operaciones del WAL, y la compactación las pierde para siempre.
**Solución.** D55.
**Test exigido.**
- El escenario de la auditoría (abrir con `{a, b}`, cerrar, reabrir con `{a}`) falla con `SchemaMismatch` y no modifica ningún archivo; al reabrir con `{a, b}` están todas las filas.
- Lo mismo cuando la tabla solo está en el snapshot (WAL vacío después de compactar).
- Reabrir con un esquema más amplio (una tabla nueva) sigue funcionando.
- En los dos motores, con el patrón genérico `<E: StorageEngine>`.

#### DEF-133 · Medio · ⬜ Pendiente
**Problema.** Ningún test cubre un hueco de secuencia dentro de un mismo batch: validar solo el primer op deja la suite en verde.
**Solución.** Solo el test (el código ya valida cada op). Va en este lote porque protege `validate_batch`, que el lote no toca pero cuya falla dejaría una sala que no vuelve a abrir, el mismo tipo de daño que DEF-115.
**Test exigido.** `apply_batch(vec![insert(1), insert(3)])` devuelve `SequenceMismatch`, no aplica nada y la sala reabre, en los dos motores. Comprobar que falla con el mutante de la auditoría.

#### DEF-141 · Bajo · ⬜ Pendiente
**Problema.** El refactor de tests perdió la única cobertura de que `validate_batch` revisa la secuencia antes que el esquema: el helper compartido `sequence_gap_is_rejected` usa una fila válida.
**Solución.** Primero decidir si el orden es un contrato. Propuesta: sí, porque un hueco es la señal que lleva al cliente a pedir un resync, y no debe quedar tapada por un error de validación del mismo batch. Si se confirma, documentarlo en el doc de `apply_batch` de `StorageEngine`; si no, descartar el ítem.
**Test exigido.** Si se confirma: un segundo caso en `sequence_gap_is_rejected` con una fila inválida y seq 42 que espere `SequenceMismatch`, en los dos motores. Comprobar que falla al mover `validate_operation` antes del chequeo de secuencia.

#### DEF-129 · Bajo · ⬜ Pendiente
**Problema.** `open_room` crea `data_dir` sin sincronizar su directorio padre.
**Solución.** Mover `create_dir_all_synced` de `server/src/durable.rs` a `zemdb-core` (o duplicarlo en storage si core no debe tener E/S) y usarlo en `open_room`. En wasm32 no aplica.
**Test exigido.** Unitario del helper: crea los componentes que faltan y sincroniza el padre de cada uno (con un fail point que registra los directorios sincronizados, como los del servidor).

#### DEF-130 · Bajo · ⬜ Pendiente
**Problema.** `compaction_ratio` acepta NaN, 0 y negativos, y entonces se compacta en cada batch.
**Solución.** `DiskStorageEngine::new` valida `ratio.is_finite() && ratio > 0.0` y `zstd_level` dentro del rango de zstd; si no, `StorageError::InvalidOptions` (variante nueva).
**Test exigido.** NaN, 0, -1 e infinito se rechazan; el valor por defecto y uno válido se aceptan.

---

## Lote 3 — Costo del roster y de abrir salas

> Objetivo: que un Ack, un registro o la reapertura de una sala no cuesten fsync ni lecturas proporcionales al tamaño del roster o de la historia.

#### DEF-117 · Medio · ⬜ Pendiente
**Problema.** Casi todo Ack reescribe el roster completo con 2 fsync aunque la poda no borre nada (~8,8 ms por Ack).
**Solución.** D56 (a).
**Test exigido.**
- Con un contador de escrituras del roster (fail point en `write_roster`): 50 commits con Acks dentro de `active.wal` no escriben el roster.
- Un Ack que permite borrar un segmento escribe el roster antes de borrarlo (el test de esa invariante que ya existe sigue en verde).

#### DEF-121 · Medio · ⬜ Pendiente
**Problema.** Cada registro reescribe el roster completo con 2 fsync; una tormenta de reconexiones cuesta O(N²).
**Solución.** D56 (b), después de confirmar la invariante que pide la decisión.
**Test exigido.**
- Con el contador de escrituras: registrar 1.000 clientes no escribe el roster más de una vez por tick.
- Un crash (reapertura sin pasar por el tick) después de registrar a un cliente nuevo: el cliente recibe `ClientNotRegistered` y se puede registrar de nuevo.
- Un registro seguido de una poda que borra segmentos: el roster en disco contiene al cliente antes del borrado.

#### DEF-122 · Medio · ⬜ Pendiente
**Problema.** Reabrir una sala decodifica operaciones completas solo para hidratar el dedup (24× más lento con operaciones de 20 KB), y el `HotBuffer` se llena con operaciones viejas.
**Solución.** D57.
**Test exigido.**
- Con sidecars, la apertura no decodifica ningún frame de segmentos sellados (contador o fail point en `decode_segment`), y el dedup reconoce un `mutation_id` de un segmento sellado.
- Con un `.ids` borrado o dañado, la apertura cae a decodificar el segmento y el dedup sigue funcionando.
- Los `.ids` sobreviven a la compresión a cold y se borran con la poda.
- Después de reabrir, el `HotBuffer` solo tiene las operaciones de `active.wal`.

#### DEF-131 · Medio · ⬜ Pendiente
**Problema.** Ningún test cubre que un cliente `Bootstrapping` no frene la poda.
**Solución.** Solo los tests. Van en este lote porque D56 cambia cuándo y cómo se persiste el roster que define ese estado.
**Test exigido.**
- Unitario en `actor/tests/lease.rs`: `active` con `Some(20)`, `newbie` con `None` (tail 10); `newbie` es `Bootstrapping` y `min_connected_ack_seq() == Some(20)`.
- En `actor/tests/room.rs`: con un `Bootstrapping` presente, el Ack de un `Connected` poda los segmentos.
- Comprobar que fallan con el mutante de la auditoría.

---

## Lote 4 — Contrato del protocolo

> Objetivo: que el contrato que va a implementar el SDK de la Fase 4 no permita divergencias silenciosas: ni cursores de otra sala, ni ids de otro cliente, ni esquemas desactualizados, ni subidas que fallan sin poder reintentarse.

#### DEF-118 · Medio · ⬜ Pendiente
**Problema.** Una sala recreada con el mismo id acepta el cursor de la sala borrada.
**Solución.** D58. Actualizar `ARCHITECTURE.md` (§ de onboarding y tabla de códigos de error).
**Test exigido.**
- El escenario de la auditoría: `old` se registra con `Some(5)` y la encarnación vieja en la sala recreada y recibe `RoomReset`; registrado sin cursor queda `Bootstrapping`.
- Con la encarnación correcta, el registro con cursor funciona igual que hoy.
- La encarnación sobrevive a un reinicio del servidor (está en `meta_room.json`).

#### DEF-123 · Bajo · ⬜ Pendiente
**Problema.** El dedup es por sala: un `MutationId` que ya usó otro cliente se confirma con la seq ajena y la operación no se aplica.
**Solución.** D59.
**Test exigido.**
- Dos clientes con el mismo `MutationId` obtienen seqs distintas y las dos operaciones se aplican.
- El reintento de un mismo cliente sigue devolviendo su seq original, también después de reabrir la sala (dedup hidratado desde el WAL y desde los `.ids` de D57).

#### DEF-127 · Bajo · ⬜ Pendiente
**Problema.** Un cambio de esquema solo se avisa por SSE; ninguna respuesta del protocolo lo señala.
**Solución.** D60. Documentar en `ARCHITECTURE.md` la regla del SDK (ver una versión mayor → `GetSchema` antes de aplicar).
**Test exigido.**
- `schema_version` empieza en 1 y sube con cada `add_column`, también después de reiniciar el registry.
- Después de un `add_column`, `HeartbeatAck`, `SyncBatch` y `CommitAck` de una sala con ese esquema llevan la versión nueva.

#### DEF-126 · Bajo · ⬜ Pendiente
**Problema.** Una subida por chunks no ancla la retención y puede fallar al completarse con `BadRequest`, que no se reintenta.
**Solución.** D61.
**Test exigido.**
- El escenario de la auditoría (subida en S=4, commits y Acks hasta 10): la poda no pasa de 4 mientras la subida está activa, y la subida se completa.
- Una subida inactiva (vencida) deja de anclar.
- Si el rango igual se movió (TTL o cuota), la respuesta es `SnapshotSuperseded`.

---

## Lote 5 — Huecos del log y robustez del servidor

> Objetivo: que un hueco en el log siempre termine en un snapshot nuevo, que un borrado fallido no lo cree, y que los errores de I/O y las cancelaciones no dejen salas a medias.

#### DEF-119 · Medio · ⬜ Pendiente
**Problema.** Un snapshot "usable" por debajo de un hueco del log apaga la demanda de snapshots, y los clientes quedan en bucle hasta que el snapshot vence.
**Solución.** D62.
**Test exigido.**
- El escenario de la auditoría (`audit_gap_after_usable_snapshot_never_turns_demand_on`): después del `BehindCompaction` por hueco, el snapshot viejo deja de anunciarse, la demanda se enciende y un `Connected` es designado.
- Un snapshot nuevo por encima del hueco vuelve a ser usable y se sirve.
- Al reabrir la sala con el hueco en el índice, `unreadable_through` se recalcula.

#### DEF-120 · Bajo · ⬜ Pendiente
**Problema.** Un segmento warm que no se pudo borrar reaparece después de podar su cold y hace retroceder `tail_seq`.
**Solución.** `SegmentIndex::scan` descarta (y borra) los segmentos con `end_seq <= pruned_through_seq`; `record_compression` guarda la ruta del warm que no se pudo borrar y el próximo `expire`/`prune_segments` reintenta el borrado.
**Test exigido.** Con un fail point que hace fallar el `remove_file` del warm: comprimir, podar el cold, reabrir; `tail_seq` no retrocede y el warm ya no está. Sin reinicio, el siguiente mantenimiento borra el warm.

#### DEF-128 · Bajo · ⬜ Pendiente
**Problema.** `Path::exists()` trata un error de I/O como "no existe" en el log y el manager del servidor.
**Solución.** `try_exists()?` (o `metadata()` propagando el error) en los 9 sitios de la auditoría, con un fail point que reemplaza su resultado, como el de storage (DEF-108). Evaluar un `disallowed-methods` para `Path::exists` en un `clippy.toml` del servidor, como el de storage.
**Test exigido.** Con el fail point devolviendo error: abrir una sala con `active.wal` falla en vez de abrir con el log vacío, y `create_room` no sobrescribe `meta_room.json`.

#### DEF-125 · Bajo · ⬜ Pendiente
**Problema.** Un `DELETE /admin/rooms/:id` cancelado a mitad del purge deja la sala viva pero sin snapshot.
**Solución.** El handler corre `delete_room` en `tokio::spawn` y espera el `JoinHandle` (como `add_column`); el doc de `RoomManager::delete_room` aclara que no es seguro cancelarla.
**Test exigido.** El handler se descarta durante el purge (el test de la auditoría, ahora sobre el handler HTTP): el borrado termina igual, el directorio desaparece y `room_exists` es `false`.

#### DEF-132 · Medio · ⬜ Pendiente
**Problema.** Ningún test cubre que la poda por TTL o cuota se detenga en el primer segmento conservado.
**Solución.** Solo el test. Va en este lote porque D62 y DEF-120 cambian la poda y el índice de segmentos.
**Test exigido.** El de la auditoría: corromper `segment_1_5.wal`, mantenimiento, `max_room_disk_bytes = 1`, mantenimiento otra vez; `segment_6_10.wal.zst` sigue existiendo. Comprobar que falla con `break` → `continue`.

---

## Lote 6 — Tests, documentación y CI

> Objetivo: tests que prueben lo que dicen, documentación que coincida con el código después de los lotes anteriores, y una CI que verifique lo que el proyecto declara.

#### DEF-134 · Bajo · ⬜ Pendiente
**Problema.** Ningún test comprueba que la compresión a cold corra fuera del worker. El refactor de calidad borró el test que lo prometía sin probarlo, pero el hueco sigue.
**Solución.** Un fail point en `compress_warm_segment_sync` registra el `ThreadId`; un test nuevo, con runtime `current_thread`, comprueba que es distinto del suyo.
**Test exigido.** El propio test, que tiene que fallar si se comprime inline (el mutante de la auditoría).

#### DEF-140 · Bajo · ⬜ Pendiente
**Problema.** `control_plane_room_lifecycle_overrides` compara la política efectiva contra una config igual a los defaults de la política, así que no detecta que la config del servidor se ignore (debilitado por los fixtures compartidos del refactor).
**Solución.** Arrancar el test con `TestServer::start(|c| { c.lease_timeout_secs = 60; c.snapshot_demand_ttl_secs = 60; })` y comparar contra los literales.
**Test exigido.** El propio test, que tiene que fallar si `RoomManager` usa `RoomLifecyclePolicy::default()` en lugar de `config.default_lifecycle_policy()`.

#### DEF-135 · Bajo · ⬜ Pendiente
**Problema.** `ROADMAP.md` contradice el código (HMAC-SHA256, `/snapshot-chunk`, `Dormant` → `BehindCompaction`, `CryptoConcurrencyBounds` pendiente).
**Solución.** Corregir esas líneas y marcar §4.3 como diseño histórico que remite a `ARCHITECTURE.md`. Agregar la Fase 3.8 al encabezado y al cronograma.
**Test exigido.** Ninguno (documentación). Revisión cruzada contra el código.

#### DEF-136 · Bajo · ⬜ Pendiente
**Problema.** `ARCHITECTURE.md:355` promete portabilidad total de snapshots sin la salvedad de Zstd en wasm32.
**Solución.** Agregar la limitación en §8, con referencia a la Fase 4 (DEF-54, diferido).
**Test exigido.** Ninguno.

#### DEF-137 · Bajo · ⬜ Pendiente
**Problema.** Quedan afirmaciones sobre la línea de caché en `core/src/value/row.rs:12` y `storage/src/disk/format.rs:14`.
**Solución.** Dejar solo los tamaños. De paso, cerrar los residuos de los reportes de calidad que siguen abiertos: `mutation/op.rs:60` ("zero heap pointers"), `wal_frame.rs:66` ("little-endian" para el magic) y los tres comentarios desactualizados en tests.
**Test exigido.** Ninguno.

#### DEF-138 · Bajo · ⬜ Pendiente
**Problema.** La doc pública enlaza a 7 ítems privados (uno nuevo del refactor: `compactor::compact_room_cow` en `disk/mod.rs`).
**Solución.** Quitar los corchetes (o hacer públicos los ítems si corresponde) y agregar a la CI `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
**Test exigido.** El paso nuevo de la CI en verde.

#### DEF-142 · Bajo · ⬜ Pendiente
**Problema.** Documentación que el refactor dejó desactualizada: el doc de `RoomSnapshotPayload` (`storage/src/memory/state.rs`) dice que lo serializa la compactación; `ARCHITECTURE.md:26` lista `as_slice()`, que ya no existe; en `server/src/actor/room.rs` el comentario de `panic_message` quedó arriba de `room_dir`; `ROADMAP.md:73` menciona `with_default_origin`.
**Solución.** Corregir el doc de `RoomSnapshotPayload` y quitarle el `Serialize` que ya no se usa; `as_slice()` → `values()`; devolver el comentario a `panic_message`; anotar en el ROADMAP que `with_default_origin` se eliminó.
**Test exigido.** Ninguno. `cargo build --workspace --all-targets` verifica que nada serialice `RoomSnapshotPayload`.

#### DEF-139 · Bajo · ⬜ Pendiente
**Problema.** La CI no verifica el MSRV declarado ni corre clippy sobre wasm32.
**Solución.** Un job con 1.85 (`cargo build --workspace --lib --bins`) y otro con 1.88 (`cargo test --workspace`); en el job wasm, `cargo clippy --target wasm32-unknown-unknown -p zemdb-core -p zemdb-storage -p zemdb-client -- -D warnings`.
**Test exigido.** Los jobs nuevos en verde. Si los lotes anteriores subieron el MSRV real, actualizar `rust-version` y el README en el mismo commit.

---

## Fuera de este plan

Siguen abiertos y diferidos, sin cambios respecto de sus planes:
- **DEF-85** (ventana de dedup limitada al log retenido más la LRU). D57 y D59 no la amplían. Un cliente sigue sin poder reconocer su propio commit en un sync porque `SequencedOperation` no lleva `mutation_id`; conviene decidirlo al diseñar la outbox de la Fase 4.
- **DEF-34** (`reload_schema` en storage). D55 lo hace más estricto pero no lo implementa.
- **DEF-81** (contenido de snapshots subidos por miembros maliciosos) y **DEF-54** (Zstd en wasm32).
- **Residuo de DEF-95:** una conexión SSE semiabierta (sin FIN) retiene su permiso hasta que TCP falla: el keep-alive manda pocos bytes y tarda días en llenar el buffer del socket, recién entonces D53 la corta. D54 lo acota: como mucho 2 por cliente y sala, y el cliente que vuelve a conectarse reemplaza la más vieja.
