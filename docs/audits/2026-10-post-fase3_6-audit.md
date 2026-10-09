# Auditoría Post-Fase 3.6

**Fecha:** 9 de Octubre de 2026
**Commit auditado:** `e93d46e` (`main`)
**Alcance:** `crates/core`, `crates/storage`, `crates/server`, `crates/client`, `README.md`, `ARCHITECTURE.md`, `ROADMAP.md`, `GEMINI.md`, `docs/`
**Plan de resolución:** [`docs/proposals/2026-10-fase3_7-remediation-plan.md`](../proposals/2026-10-fase3_7-remediation-plan.md)

---

## 1. Metodología

- **6 auditores en paralelo**, cada uno con una perspectiva distinta y en modo solo lectura:
  1. Durabilidad y recuperación (`storage`, WAL y log del servidor).
  2. Concurrencia y ciclo de vida de actores (`server/src/actor`).
  3. Correctitud del dominio y del protocolo (`core`).
  4. Seguridad y entradas hostiles (API HTTP, relay, decodificación).
  5. Rendimiento y uso de recursos.
  6. Calidad de tests y coherencia con la documentación.
- **Sin forzar hallazgos:** un área sin problemas se reporta como tal. Cada hallazgo tiene que traer un escenario de falla que se pueda seguir en el código.
- **Consolidación:** se unificaron 2 duplicados (el CRC del frame WAL, reportado por dominio y por durabilidad; y `room_meta`, reportado por rendimiento y por concurrencia). Las referencias a líneas se revisaron contra el código; las que estaban mal se corrigieron.
- **Estados de verificación:**
  - **Confirmado (coordinador):** comprobado por el coordinador en el código o reproducido.
  - **Confirmado:** el auditor recorrió el camino completo en el código.
  - **Plausible:** el mecanismo existe, pero el impacto depende del despliegue o de un disparador poco frecuente.
- **IDs:** se continúa la numeración `DEF-xx` del [plan de la Fase 3.6](../proposals/2026-10-fase3_6-remediation-plan.md), que termina en DEF-86. Esta auditoría va de **DEF-87 a DEF-113**.

### Estado del build en el commit auditado

| Chequeo | Resultado |
|---|---|
| `cargo fmt --all -- --check` | Sin diferencias |
| `cargo clippy --workspace --all-targets -- -D warnings` | Sin warnings |
| `cargo test --workspace` | 555 tests en verde (core 101, storage 100, server 353, client 1), en 4 corridas, sin tests inestables |
| Build `wasm32-unknown-unknown` (core, storage, client) | Compila |

---

## 2. Hechos de contexto que condicionan las severidades

1. **`zemdb-server` no depende de `zemdb-storage`.** Los defectos de storage afectan al cliente de la Fase 4, que es la próxima fase.
2. **`zemdb-client` es un stub.** Lo que depende de un cliente real (aplicar snapshots, bootstrap) hoy no tiene consumidor.
3. **El squashing (`core/src/mutation/`) solo se usa en la outbox del cliente**, y la outbox offline está diferida para después de la v0.1 (ROADMAP §7, "Borradores Offline Explícitos"). Fuera de los tests de core no hay ningún consumidor. Los 4 hallazgos de squashing (DEF-88, 94, 105 y 106) se marcan como **latentes**: la severidad describe el daño que causarían cuando exista la outbox, no el impacto actual.
4. **El servidor es el único binario desplegable hoy.** Lo que afecta al servidor (DEF-89, 90, 95) tiene impacto inmediato si se expone.

---

## 3. Resumen

| Severidad | Cantidad | IDs |
|---|---|---|
| 🔴 Alto | 4 | DEF-87, 88 (latente), 89, 90 |
| 🟠 Medio | 11 | DEF-91, 92, 93, 94 (latente), 95, 96, 97, 98, 99, 100, 101 |
| 🟡 Bajo | 12 | DEF-102, 103, 104, 105 (latente), 106 (latente), 107, 108, 109, 110, 111, 112, 113 |
| **Total** | **27** | |

**Lo más importante:** una pérdida silenciosa de batches ya confirmados en el storage (DEF-87), un DoS por memoria sin autenticación en `/register` (DEF-89) y un log del servidor que no tiene ningún límite en bytes (DEF-90). La concurrencia de los actores salió prácticamente limpia.

### Matriz

| ID | Sev. | Verificación | Crate | Resumen |
|---|---|---|---|---|
| DEF-87 | Alto | Confirmado (coordinador, reproducido) | storage | `write_all` + `sync_data` sin `flush()`: tokio descarta el error de la escritura de fondo y el batch se confirma sin estar en disco. |
| DEF-88 | Alto · latente | Confirmado (coordinador) | core | El squashing convierte `Delete → Insert → Delete` en nada: la fila sigue viva en el servidor. |
| DEF-89 | Alto | Confirmado (coordinador) | server, core | `/register` decodifica el mensaje completo antes de autenticar; una `CompactRow` de ~16 M `Null` reserva ~0,4–0,5 GB. |
| DEF-90 | Alto | Confirmado | server | El log del servidor se acota por cantidad de operaciones, nunca por bytes; lecturas y compresión cargan segmentos enteros en RAM. |
| DEF-91 | Medio | Confirmado (requiere corrupción del medio) | core, storage, server | El CRC del frame WAL no cubre la longitud: un bit flip a mitad del log se toma como torn write y se truncan frames durables. |
| DEF-92 | Medio | Plausible | core, storage, server | Un torn write que no deja un prefijo intacto se clasifica como corrupción y la sala no vuelve a abrir. |
| DEF-93 | Medio | Confirmado el bypass; impacto plausible | storage | `apply_snapshot` no valida filas, tablas ni el `head_seq` declarado. |
| DEF-94 | Medio · latente | Confirmado | core | El squashing decide la precedencia por reloj de pared: si el reloj retrocede, un `Delete` posterior se descarta. |
| DEF-95 | Medio | Plausible | server | Sin timeouts de lectura ni límite de conexiones (slowloris, body por goteo). |
| DEF-96 | Medio | Confirmado (coordinador) | server | El caché de dedup reserva ~280 KiB por sala aunque esté vacío. |
| DEF-97 | Medio | Confirmado | server | Leer un rango de un segmento decodifica todo lo anterior: ponerse al día cuesta O(S²/B). |
| DEF-98 | Medio | Confirmado | storage | La deserialización de snapshots corre en el worker de tokio; el motor en memoria serializa con el lock tomado. |
| DEF-99 | Medio | Plausible | server | La compresión de segmentos no tiene tope por pasada y congela al actor (504). |
| DEF-100 | Medio | Confirmado (coordinador) | docs, server | La documentación promete "HTTP/2 over TLS"; el servidor habla HTTP/1.1 en texto plano. |
| DEF-101 | Medio | Confirmado | storage (tests) | `test_cow_compaction_concurrent_writes_unblocked` no prueba que el escritor no se bloquee. |
| DEF-102 | Bajo | Confirmado | server | `room_meta` crece sin límite; la recarga de esquemas recorre todas las salas históricas. |
| DEF-103 | Bajo | Plausible | server | El roster nunca olvida clientes `Dormant` y se reescribe en JSON pretty con 2 fsync por tick. |
| DEF-104 | Bajo | Confirmado | server | `shutdown_all` cierra las salas de a una y puede agotar los 10 s de gracia. |
| DEF-105 | Bajo · latente | Confirmado | core | El LWW por timestamp no se cumple por columna después de mezclar updates. |
| DEF-106 | Bajo · latente | Confirmado | core | `TableBuffer::apply` acepta deltas que su propio `Deserialize` rechaza; un `Update` inválido contamina un `Insert` válido. |
| DEF-107 | Bajo | Plausible | storage | Ventana sin `flock` durante la rotación de la compactación (requiere 2 procesos sobre el mismo `data_dir`). |
| DEF-108 | Bajo | Plausible | storage | `Path::exists()` devuelve `false` ante un error de I/O y se trunca el snapshot real. |
| DEF-109 | Bajo | Confirmado | storage (tests) | `test_disk_concurrent_readers_and_writers` no verifica a los lectores. |
| DEF-110 | Bajo | Confirmado (reproducido) | server (tests) | Tests que fallan si hay variables `ZEMDB_*` en el entorno. |
| DEF-111 | Bajo | Confirmado | server (tests) | Tests de integración que usan internals expuestos como `pub` solo para testear (regla de `GEMINI.md`). |
| DEF-112 | Bajo | Confirmado | storage (tests) | Faltan puntos de fallo en la fase 3 de la compactación. |
| DEF-113 | Bajo | Confirmado | docs | Documentación desactualizada (magic, micro-WAL, ROADMAP, versión de Rust, cantidad de tests, merge por campo). |

---

## 4. Hallazgos

### 🔴 Alto

#### DEF-87 · Alto · Un error de escritura se pierde y el batch se confirma como durable
- **Dónde:** `crates/storage/src/disk/mod.rs:534-537` (`apply_batch`), `mod.rs:497` (`close_room`), `crates/storage/src/disk/compactor.rs:101`, `:285`, `:308` y `:316`, `crates/storage/src/disk/recovery.rs:277`, `:325` y `:338`. Todos son `sync_data()`/`sync_all()` sobre un `tokio::fs::File` sin `flush()` previo.
- **Qué pasa:** en tokio, `write_all` (hasta 2 MiB) copia los datos a un buffer, lanza la escritura al pool bloqueante y devuelve `Ok`. `sync_data`/`sync_all` esperan esa escritura con `complete_inflight()`, que guarda su error en `last_write_err` sin devolverlo, y después hacen un `fsync` que da `Ok`. Solo `flush()` (o la próxima escritura) devuelve el error guardado.
- **Reproducción (tokio 1.53.1):** con un handle abierto solo para lectura, `write_all(b"hello")` → `Ok`, `sync_data()` → `Ok`, y recién `flush()` → `Err(EBADF)`. El archivo queda con 0 bytes.
- **Escenario:**
  1. Con el disco casi lleno, `apply_batch(seq N)` escribe parte del frame y la escritura de fondo falla con ENOSPC.
  2. `sync_data` devuelve `Ok`: se aplica N en memoria (los lectores lo ven) y se devuelve `Ok(N)`.
  3. Al reabrir, recovery trata el frame parcial como torn write y lo trunca. El batch N, ya confirmado, se pierde.
- **Variante con pérdida permanente (compactación):** ENOSPC es justamente lo que deja un `.wal.compacting` huérfano. El error de `append_synced` al absorberlo se descarta, `truncate_wal` vacía el WAL activo y la fase 2 falla: esos registros ya no están en ningún archivo. Tras un crash se pierden en silencio, o recovery falla con "WAL sequence gap" y la sala no abre más.
- **Arreglo sugerido:** `flush().await?` antes de cada sync, centralizado en un helper para que no se pueda olvidar.

#### DEF-88 · Alto · latente · El squashing borra un `Delete` real
- **Dónde:** `crates/core/src/mutation/squash.rs:115-127` (Regla 4: `Insert` pendiente + `Delete` → `Purged`) y `squash.rs:130-135` (Regla 5: `Delete` pendiente + `Insert` → `Insert`, y se pierde el rastro del `Delete`). `crates/core/src/mutation/buffer.rs:137-141` saca la entrada purgada.
- **Qué pasa:** la aniquilación mutua da por hecho que el `Insert` pendiente creó la fila. Pero en el servidor y en las réplicas `Insert` es un upsert (`crates/storage/src/memory/state.rs`, `table.insert(pk, row)`), y la Regla 5 ya borró la evidencia de que antes hubo un `Delete`.
- **Escenario:**
  1. La fila K existe en el servidor.
  2. El cliente encola `Delete(K)`: queda un `Delete` pendiente.
  3. Encola `Insert(K)`: la Regla 5 lo convierte en un `Insert` pendiente.
  4. Encola `Delete(K)`: la Regla 4 devuelve `Purged` y el buffer queda vacío.
  - **Resultado:** no se envía nada y K sigue viva en el servidor y en todas las réplicas. Enviadas en orden, las tres operaciones habrían borrado K.
  - Pasa lo mismo con un `Insert` que sobrescribe una fila existente seguido de un `Delete`.
- **Por qué es latente:** solo lo usa la outbox offline, diferida para después de la v0.1.

#### DEF-89 · Alto · Una request sin autenticación reserva ~0,5 GB al decodificarse
- **Dónde:** `crates/server/src/api/data_plane.rs:86-90` (`register` no tiene extractor de auth y decodifica `BinaryMessage<ClientMessage>` completo antes de mirar la variante o el token), `crates/server/src/api/extract.rs:213`, `crates/core/src/protocol/codec.rs:102` (`with_limit` cuenta bytes leídos, no memoria reservada) y `crates/core/src/value/row.rs:102` (`CompactRow` es un `Vec<Value>`).
- **Qué pasa:** cada `Value::Null` ocupa 1 byte en el wire y 24 bytes en memoria. Un cuerpo de 16 MiB se convierte en un `Vec` de ~16,7 M elementos que crece por duplicación hasta ~400–540 MB.
- **Ataque:**
  1. `POST /rooms/<id-válido>/register` sin `Authorization`, con un `ClientMessage::Commit` cuyo `Insert` trae ~16,7 M `Null`.
  2. El cuerpo entra en el límite de 16 MiB; la decodificación reserva ~0,5 GB.
  3. Recién después el handler responde 400 ("Expected RegisterClient").
  4. Con N conexiones que completan el cuerpo a la vez, el proceso muere por OOM.
- **Alcance adicional:** cualquier poseedor de un token puede hacer lo mismo en `/commit` y `/sync`, porque la validación de esquema ocurre en el actor, después de decodificar.
- **Nota del coordinador:** acotar cada fila a `MAX_COLUMNS` (65.535) no alcanza para los endpoints autenticados. Un `Commit` con ~256 operaciones de 65.535 `Null` cada una entra en 16 MiB y vuelve a reservar ~0,4 GB. La amplificación ×24 es propia del formato, así que hace falta además un tope global (ver el plan, decisión D44).

#### DEF-90 · Alto · El log del servidor se acota por operaciones, nunca por bytes
- **Dónde:**
  - Ventana de RAM: `crates/server/src/log/hot_buffer.rs:94-96` (solo cuenta entradas y TTL).
  - Rotación de segmentos: `crates/server/src/log/tiered_log.rs:214` (`active_ops_count() >= ram_max_ops`).
  - Lecturas de archivo completo: `crates/server/src/log/warm_disk.rs:187-188` (`vec![0u8; active_len]`) y `warm_disk.rs:231` (`std::fs::read` del segmento).
  - Compresión: `crates/server/src/log/cold_disk.rs:52-53` (lee el segmento entero y lo comprime en memoria) y `cold_disk.rs:69` (lo verifica descomprimiéndolo y decodificándolo entero).
  - Clonado antes de recortar: `hot_buffer.rs:72-76` clona hasta `limit` operaciones y recién después `crates/server/src/actor/room.rs:654` (`fit_in_response`) recorta la respuesta a 16 MiB.
- **Qué pasa:** cada operación puede pesar hasta ~16 MiB. El HotBuffer, `active.wal` y cada segmento guardan `ram_max_ops` operaciones (1.000 por defecto, hasta 100.000) sin ningún tope en bytes.
- **Escenario:** un esquema con una columna `Bytes` y adjuntos de 1 MiB, con 1.000 commits en 5 minutos (`ram_ttl`):
  - El HotBuffer retiene ~1 GiB por sala.
  - Cada `/sync` desde un cursor anterior clona ~1 GiB en el hilo del actor para devolver solo 16 MiB.
  - Un sync que cae en `active.wal` reserva otro GiB.
  - La compresión del segmento usa 3–4 GiB transitorios.
  - Con 10 salas así, OOM. En el peor caso (16 MiB × 1.000) son ~16 GiB por sala.

### 🟠 Medio

#### DEF-91 · Medio · El CRC del frame WAL no cubre la longitud
- **Dónde:** `crates/core/src/protocol/wal_frame.rs:167-179` (parse del header: `payload_len` no está cubierto por ningún CRC), `wal_frame.rs:236` (`classify_checksum_mismatch` solo mira si hay un magic en `frame_len`) y `wal_frame.rs:310` (payload truncado → torn write). Consumidores: `crates/storage/src/disk/recovery.rs:324` (`set_len`) y `crates/server/src/log/warm_disk.rs:278`.
- **Escenario:** WAL con F1, F2 y F3; un bit flip sube en 1 el `payload_len` de F2.
  1. El payload leído incluye el primer byte de F3 y el CRC no coincide.
  2. Los bytes en el nuevo `frame_len` empiezan con `0x7C`, no con el magic, y se clasifica como torn write.
  3. `set_len(len(F1))` borra F2 y F3, que ya tenían fsync. No salta el chequeo de gap porque no queda nada después.
  4. Si la longitud apunta más allá del EOF se cae en "Truncated WAL batch payload", con el mismo resultado.
  5. En el servidor, el head retrocede y se reasignan números de secuencia ya entregados.
- **Por qué importa:** contradice la garantía de `ARCHITECTURE.md` de no descartar en silencio la cola cuando hay frames durables detrás.

#### DEF-92 · Medio · Plausible · Un torn write que no es prefijo se trata como corrupción
- **Dónde:** `crates/core/src/protocol/wal_frame.rs:258` (`classify_zeroed_header`) y el absorb sin sync intermedio en `crates/storage/src/disk/compactor.rs:247-316`.
- **Qué pasa:** solo se acepta como torn write lo que deja un prefijo intacto. Sin fsync, POSIX no garantiza el orden en que las páginas llegan al disco.
- **Escenario:** un batch de varias páginas; tras un crash la página del header quedó en ceros y la siguiente sí llegó. `classify_zeroed_header(false)` devuelve `WalCorruption` y `open_room` falla para siempre, aunque solo se perdió un batch nunca confirmado. Lo mismo puede pasar en un `.wal.compacting` a medio absorber, aunque el WAL activo todavía tiene todos los registros.

#### DEF-93 · Medio · `apply_snapshot` no valida el contenido del snapshot
- **Dónde:** `crates/storage/src/memory/mod.rs:326-356` y `crates/storage/src/disk/mod.rs:666-700`. El relay solo valida el envelope (`crates/server/src/relay/upload.rs`).
- **Qué pasa:** cualquier miembro de la sala puede subir un snapshot; el relay comprueba el header `ZMSN` y el CRC (decisión D18). `apply_snapshot` deserializa `RoomSnapshotPayload` y reemplaza las tablas sin `validate_compact_row`, sin rechazar `table_id` desconocidos y sin contrastar `payload.head_seq` con el `snapshot_head_seq` que declaró el relay.
- **Escenario:** se declara `snapshot_head_seq = S`, pero el payload trae `head_seq = S + 1000` y filas con tipos inválidos o con una PK distinta de la clave. El cliente que arranca desde él lo aplica sin error: queda con filas que el servidor nunca aceptaría y se saltea en silencio las operaciones S+1..S+1000.
- **Relación con DEF-81:** DEF-81 (diferido) cubre que el contenido sea *verdadero*; esto cubre que sea *válido*, que D18 dejaba a cargo del cliente.

#### DEF-94 · Medio · latente · El squashing depende del reloj de pared
- **Dónde:** `crates/core/src/mutation/squash.rs:73-132` (las 5 reglas comparan `timestamp`).
- **Qué pasa:** dentro de la outbox de un solo cliente, el orden real es el orden de llegada a `apply`, pero la precedencia se decide por el timestamp. Contradice `ARCHITECTURE.md` §10.2 ("Not used for causal precedence or local optimistic rebase"), aunque `ARCHITECTURE.md:58` documenta lo contrario ("Strict Last-Write-Wins"). Ya figuraba en la auditoría de la Fase 2.
- **Escenario:** `Insert(K, ts=1000)`, ajuste de NTP hacia atrás, `Delete(K, ts=990)`: la Regla 4 devuelve `Discarded` y el borrado del usuario se pierde en silencio. Con dos `Update` del mismo campo gana el valor anterior.

#### DEF-95 · Medio · Plausible · Sin timeouts de lectura ni límite de conexiones
- **Dónde:** `crates/server/src/api/serve.rs:48` (`axum::serve` sin configurar) y `crates/server/src/api/router.rs:123` (la única capa es `DefaultBodyLimit`).
- **Qué pasa:** axum 0.7 arma el builder de hyper-util sin `Timer`, así que no se aplica el `header_read_timeout` de hyper. Tampoco hay timeout del body ni límite de conexiones.
- **Ataque:** slowloris (miles de conexiones que mandan las cabeceras byte a byte) agota los file descriptors. Un body de `/register` enviado por goteo retiene hasta 16 MiB por request, sin autenticación y sin plazo.
- **Por qué es plausible:** depende de si hay un reverse proxy adelante que imponga esos límites.

#### DEF-96 · Medio · El caché de dedup reserva ~280 KiB por sala vacía
- **Dónde:** `crates/server/src/dedup.rs:14-18`.
- **Qué pasa:** `LruCache::new(cap)` (lru 0.12) hace `HashMap::with_capacity(cap)`. Con la capacidad por defecto de 10.000 son 16.384 buckets por actor de sala, se usen o no.
- **Escenario:** 10.000 salas activas con poca actividad ocupan ~2,7 GiB solo en mapas vacíos. Con `ZEMDB_DEDUP_LRU_CAPACITY=100000` son ~2,8 MiB por sala.

#### DEF-97 · Medio · Leer un rango de un segmento cuesta O(S²/B)
- **Dónde:** `crates/server/src/log/warm_disk.rs:306` (`decode_segment` verifica el CRC y deserializa cada batch anterior a `from_seq` para descartarlo), `crates/server/src/log/cold_disk.rs:128-135` (descomprime el archivo entero en cada lectura), `crates/server/src/log/tiered_log.rs:640-641`.
- **Qué pasa:** ponerse al día atravesando un segmento de S operaciones en lotes de B cuesta S/B lecturas completas del archivo (y S/B descompresiones si es frío) y O(S²/B) decodificaciones.
- **Escenario:** con `ram_max_ops=100000` y `/sync` de 1.000, un segmento de ~20 MB se lee 100 veces (~2 GB de E/S). Un cliente con `max_batch_size=1` hace que cada request lea y decodifique un segmento entero para devolver una sola operación. El catch-up de cada commit de un cliente atrasado (límite 100) dispara lo mismo.

#### DEF-98 · Medio · La deserialización de snapshots bloquea el worker de tokio
- **Dónde:** `crates/storage/src/disk/mod.rs:684` (`apply_snapshot`), `crates/storage/src/disk/recovery.rs:258` (`recover_room`), `crates/storage/src/memory/mod.rs:335`. Además, `memory/mod.rs:304-316` serializa el snapshot entero con el `RwLock` de la sala tomado, y `disk/mod.rs:675` copia el snapshot comprimido con `to_vec()`.
- **Qué pasa:** la descompresión va a `spawn_blocking`, pero `bincode::deserialize` (que construye millones de nodos `imbl`) corre en el worker async.
- **Escenario:** un snapshot de 200 MiB descomprimido bloquea el worker entre cientos de ms y segundos. En un runtime current-thread (típico en el cliente y en wasm) se congela todo: red, UI y otros streams.

#### DEF-99 · Medio · Plausible · La compresión de segmentos no tiene tope por pasada
- **Dónde:** `crates/server/src/log/tiered_log.rs:340-345` (comprime todos los segmentos vencidos, en secuencia), `crates/server/src/actor/room.rs:282-283` (el tick lento hace `await` dentro del `select!` del actor) y `room.rs:277` (la misma pasada al apagar por inactividad).
- **Escenario:** una sala con cientos de segmentos warm (por ejemplo, acumulados hasta la cuota porque un cliente `Disconnected` frena la poda) se reabre después de más de `warm_disk_ttl`. En el primer tick lento todos vencieron y se comprimen uno tras otro, con varios fsync cada uno. Mientras tanto el actor no atiende comandos: commits, syncs y heartbeats devuelven `Timeout` (504). Si pasa al apagarse por inactividad, los requests que la reabren esperan en `wait_for_previous_actor` y también terminan en 504.

#### DEF-100 · Medio · La documentación promete HTTP/2 sobre TLS
- **Dónde:** `ARCHITECTURE.md:259-262`, `README.md:16` y `:32`.
- **Qué pasa:** `axum = "0.7"` usa las features por defecto, sin `http2`; el crate `h2` no está en `Cargo.lock`. `crates/server/src/api/serve.rs:48` sirve sobre un `TcpListener` plano y no hay TLS (`rustls` aparece solo como dev-dependency de reqwest). El servidor habla HTTP/1.1 en texto plano.
- **Por qué importa:** quien despliegue confiando en la documentación expone tokens en texto plano.

#### DEF-101 · Medio · Un test no prueba lo que dice su nombre
- **Dónde:** `crates/storage/tests/compaction_lifecycle_tests.rs:191` (`test_cow_compaction_concurrent_writes_unblocked`).
- **Qué pasa:** lanza la compactación y el escritor con `tokio::join!` sin garantizar que se solapen, y no tiene ningún assert sobre bloqueo. Ningún test pausa en `compaction.staging` para comprobar que una escritura termina mientras la compactación está en curso. La promesa principal de A-03 ("compactación sin bloquear al escritor") no tiene un test que detecte una regresión.

### 🟡 Bajo

#### DEF-102 · Bajo · `room_meta` crece sin límite
- **Dónde:** `crates/server/src/actor/manager.rs:143`, con inserciones en `:341` y `:480` y un único `remove` en `:497` (`delete_room`). `reload_schema_for_rooms` (`:578`) recorre todas las salas vistas y toma el spawn lock de cada una, en secuencia.
- **Escenario:** un servidor de larga vida que sirve millones de salas distintas retiene una entrada por sala para siempre, en contra del objetivo del apagado por inactividad. Cada `POST /admin/schemas/{id}/columns` hace O(salas históricas) adquisiciones de lock.

#### DEF-103 · Bajo · Plausible · El roster nunca olvida clientes `Dormant`
- **Dónde:** `crates/server/src/actor/lease.rs:268` (solo `deregister_client` borra entradas) y `lease.rs:121-130` (`write_roster` serializa todo con `to_string_pretty` y hace 2 fsync). Se llama desde `room.rs:916` en cada tick rápido con cambios.
- **Escenario:** si los IDs de cliente rotan (reinstalaciones, pestañas o dispositivos que nunca se desregistran), una sala activa con miles de entradas históricas reescribe y sincroniza un JSON de cientos de KB por segundo.

#### DEF-104 · Bajo · `shutdown_all` cierra las salas de a una
- **Dónde:** `crates/server/src/actor/manager.rs:429-434` y `crates/server/src/api/serve.rs` (timeout total de 10 s, D8).
- **Escenario:** SIGTERM con ~1.000 salas activas; a ~10 ms de fsync por sala se pasan los 10 s, `main` sale con `exit(1)` y las salas restantes no guardan su roster. Se pierde hasta ~1 s de avance de cursores y el `last_seen` queda viejo. Los commits no se pierden.

#### DEF-105 · Bajo · latente · El LWW por timestamp no se cumple por columna
- **Dónde:** `crates/core/src/mutation/squash.rs:97-101` (Regla 2) y `squash.rs:135-150` (Regla 5, merge de un `Insert` más viejo).
- **Escenario:** `U1(ts=100, A=x)`, `U2(ts=90, B=old)` (se mezcla y la entrada queda con ts=100), `U3(ts=95, B=new)`: como 95 < 100, B queda en "old" aunque 95 > 90. La entrada guarda un solo timestamp para todas sus columnas.
- **Nota:** desaparece si se resuelve DEF-94 quitando la precedencia por timestamp.

#### DEF-106 · Bajo · latente · `TableBuffer::apply` acepta lo que su `Deserialize` rechaza
- **Dónde:** `crates/core/src/mutation/buffer.rs:123-150` (`apply`) frente a `buffer.rs:82-93` (`Deserialize` exige deltas estrictamente ascendentes); `crates/core/src/mutation/op.rs:79`, `:107` y `:197` (`sort_by_key` no elimina duplicados).
- **Escenario A:** `UpdateBuilder::new(t, pk).set(1, "a").set(1, "b").build()` produce deltas `[1, 1]` y `apply` da `Ok`. Al persistir el buffer y volver a leerlo, falla con "strictly ascending" y se pierde toda la outbox persistida de esa tabla.
- **Escenario B:** un `Insert` pendiente válido en una tabla de 3 columnas recibe un `Update(col 99)`. La Regla 1 agranda la fila a 100 valores y el servidor rechaza el `Insert` completo (`CompactRowArityMismatch`). Enviadas por separado, solo habría fallado el `Update`.

#### DEF-107 · Bajo · Plausible · Ventana sin `flock` durante la rotación de la compactación
- **Dónde:** `crates/storage/src/disk/compactor.rs:178-226` (`rename(wal → compacting)` y después `open_fresh_wal`, que abre con `truncate(true)` antes de tomar el lock).
- **Escenario:** un segundo proceso abre la sala en esa ventana, crea `room_X.wal` y toma el lock. El primero trunca ese archivo, falla el lock y `roll_back_rotation` renombra `.wal.compacting` encima. El segundo proceso sigue confirmando batches en un inodo desvinculado, que se pierden al reiniciar. Requiere 2 procesos sobre el mismo `data_dir`.

#### DEF-108 · Bajo · Plausible · `exists()` falso ante un error de I/O trunca el snapshot
- **Dónde:** `crates/storage/src/disk/recovery.rs:197` y `:267-277`.
- **Qué pasa:** `Path::exists()` devuelve `false` ante cualquier error de `stat` (EIO transitorio, FS de red). En ese caso se abre el snapshot con `create` + `truncate(true)` y se escribe uno vacío. Si el WAL estaba vacío tras una compactación, la sala abre vacía en silencio.

#### DEF-109 · Bajo · Un test no verifica a los lectores
- **Dónde:** `crates/storage/tests/compaction_lifecycle_tests.rs:164` y `:176` (`test_disk_concurrent_readers_and_writers`).
- **Qué pasa:** `if let Ok(Some(_)) = ...` descarta los errores de lectura y `let _ = reader_handle.await.unwrap()` descarta el contador. Un lector que fallara siempre igual pasaría.

#### DEF-110 · Bajo · Tests que dependen de `ZEMDB_*` del entorno
- **Dónde:** `crates/server/tests/unified_wal_tests.rs:159-273` (`with_env` y los tests de variables) y `crates/server/src/tests/config.rs`.
- **Qué pasa:** `load_with_env` lee todas las `ZEMDB_*` del proceso. Con `ZEMDB_LEASE_TIMEOUT_SECS=30` exportada (el README pide exportar variables `ZEMDB_*` para levantar el servidor), el test falla (`left: 30s, right: 120s`). Además modifica el entorno del proceso sin restaurarlo.

#### DEF-111 · Bajo · Tests de integración que usan internals
- **Dónde:** `crates/server/tests/tiered_log_tests.rs`, `unified_wal_tests.rs` y `room_lifecycle_resilience_tests.rs`.
- **Qué pasa:** usan `zemdb_server::log::HotBuffer`, `WarmDiskLog` y `dedup::DedupLruCache`. `lib.rs` mantiene `pub mod dedup`/`pub mod log` solo por estos tests, contra la regla de `GEMINI.md`. El plan de la Fase 3.6 los clasificaba como unitarios en `server/src/log/tests/`, y la reestructuración quedó pendiente.

#### DEF-112 · Bajo · Faltan puntos de fallo en la fase 3 de la compactación
- **Dónde:** `crates/storage/src/disk/compactor.rs:146-160`.
- **Qué pasa:** no hay punto de fallo en el `rename` del snapshot, en `sync_parent` ni en el borrado de `.wal.compacting`. El propio plan de la Fase 3.6 lo reconoce como no cubierto.

#### DEF-113 · Bajo · Documentación desactualizada
- **Magic "RM":** `README.md:17` y `docs/audits/2026-09-post-fase3-audit.md:78` dicen "RM"; el código usa "ZM" (`crates/core/src/protocol/codec.rs:6`).
- **Micro-WAL descrito como existente:** `ARCHITECTURE.md:75` y `:390`, `README.md:32`, `ROADMAP.md` (`:180`, `:281`, `:334`, `:645-646`, `:725-726`). El propio `ARCHITECTURE.md` §5.2 dice que se eliminó.
- **ROADMAP con estado contradictorio:**
  - El encabezado (`:4` y `:6`) dice "20 de Septiembre" y "Fases 2 a 5 Planificadas", contra `:634` ("FASE 3 … [COMPLETADA]").
  - `:145` marca el server como "[PENDIENTE - FASE 3]".
  - `:597` dice que `SequencedOperation` lleva `client_id` y `mutation_id`; solo tiene `{seq, op}` (`crates/core/src/protocol/messages.rs:58`).
  - `:437` (A-10) describe "Dormant tras 90 s"; hoy se deriva del cursor y `dormant_after` es `None` por defecto.
  - `:677` marca el build wasm32 como pendiente, pero la CI ya lo corre.
  - No menciona la Fase 3.6.
- **Versión de Rust:** `README.md:49` dice 1.78+; `uuid 1.26.1` (dependencia normal) exige 1.85, y las dev-dependencies (`icu_*` vía reqwest) 1.88. No hay `rust-version` en `Cargo.toml`.
- **Cantidad de tests:** `README.md:68` dice "100+"; son 555, uno de ellos el placeholder `it_works` de `crates/client`.
- **Merge por campo:** `ARCHITECTURE.md:51` dice que el servidor hace un "field-level merge"; el servidor no tiene estado de filas, la mezcla ocurre en cada réplica al aplicar en orden de secuencia.
- **"L1 cache-line aligned":** `README.md:13` lo dice de `PrimaryKey`; no hay `repr(align(64))`, el tipo solo cabe en 64 B. Los tamaños de 24 y 40 B sí se cumplen (`crates/core/tests/types_memory_tests.rs`).

---

## 5. Observaciones que no cuentan como hallazgos

- **SSE no revalida el token:** solo se verifica al conectar. Un stream sigue recibiendo señales después de que el token vence o el cliente se da de baja. Solo expone números de secuencia, ids de esquema y avisos de snapshot.
- **Tokens en query string y admin secret como Bearer en el relay:** decisiones de diseño documentadas (D15).
- **Sin group commit:** un fsync por commit es una decisión documentada (D4), no un bug.

---

## 6. Áreas revisadas sin hallazgos

- **Concurrencia de actores:** spawn lock por sala y doble chequeo (nunca hay dos actores para la misma sala), carreras entre `create_room` y `delete_room`, sala que se apaga mientras llega un request, recuperación ante panics (`catch_unwind`), respuestas oneshot descartadas, recarga ordenada de esquemas, locks retenidos a través de `.await`, backpressure (mpsc de 1024 con `ACTOR_TIMEOUT`, broadcast de 256 con `Lagged`), tareas huérfanas.
- **Seguridad:** path traversal (`RoomId`/`SchemaId` validados en todos los orígenes), endpoints admin (`AdminAuth` antes de leer el body), tokens (BLAKE3 con clave derivada, comparación en tiempo constante, chequeo cruzado de sala y cliente), relay multipart (aritmética de `UploadLayout`, `chunk_index`, BLAKE3 antes de promover, descargas ancladas por hash, lock por sala), descompresión zstd acotada, `unwrap`/`expect`/`panic` no alcanzables desde input externo, el data plane nunca crea salas.
- **Dominio:** `Value` (Eq, Ord y Hash consistentes con NaN y ±0.0), `PrimaryKey`, `CompactRow`, ids, validación de esquemas (ids de tabla estrictos, `deny_unknown_fields`, `MAX_COLUMNS`, `add_column`), `validate_operation`, `merge_sorted_column_updates` con entradas válidas (O(M+N)), codec "ZM" v0x01, `limits.rs`, envelope `ZMSN`, idempotencia del actor (orden dedup → retención → esquema → tamaño).
- **Durabilidad:** orden de `apply_snapshot` (stage, rename, sync del directorio, truncado del WAL), fases 1 a 3 de la compactación, fold de `.wal.compacting` en recovery, saltear registros con seq ≤ head y detectar gaps, `PendingWrite` y estado de sala fallida, header `ZEM1`. En el servidor: `warm_disk` (std I/O con `flush`/`sync_data`), `cold_disk` (tmp con sync, rename y `sync_dir`), poda con `log_meta.json` atómico, `durable::write_atomic`.
- **Rendimiento:** índice de segmentos, `get_range` del HotBuffer O(1), ticks de mantenimiento, scans por cursor (lotes de 64, clon O(1) de `imbl`), compactación CoW fuera del runtime async, descarga del relay por chunk, sin fugas de file descriptors.
- **Tests:** sin tests inestables en 4 corridas; los tiempos usan reloj pausado o sleeps de cota inferior.

## 7. Auditorías previas

- **Siguen corregidos:** C-01, C-02, C-05, C-06, C-07, C-09, A-07, A-14, A-17, M-04, M-05, M-08, M-09, B-04, DEF-04, DEF-06, DEF-27, DEF-29, DEF-42, DEF-48, DEF-55, DEF-65, DEF-67 y DEF-68. Los tests que exige el plan de la Fase 3.6 existen.
- **A-10:** sigue resuelto, pero con otro mecanismo (estado derivado del cursor, D24); su texto quedó desactualizado.
- **Pendientes declarados:** A-16 (el crate cliente es un stub) y los diferidos a Fase 4: DEF-34, 54, 81, 82, 84 y 85.
- **Reabierto:** la dependencia del reloj en el squashing (auditoría de la Fase 2, "Contradicción de orden temporal") sigue sin corregirse y vuelve como DEF-94.
