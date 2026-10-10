# Plan de Remediación — Fase 3.7

**Fecha:** 9 de Octubre de 2026
**Origen:** [`docs/audits/2026-10-post-fase3_6-audit.md`](../audits/2026-10-post-fase3_6-audit.md) (DEF-87 a DEF-113)
**Tipo:** Documento vivo (plan + seguimiento). El detalle de cada defecto (dónde, escenario, evidencia) está en la auditoría; acá van el orden, las decisiones, la solución y el test exigido.

---

## 0. Cómo usar este documento

- Los defectos están agrupados en **lotes** ordenados por prioridad de ejecución. Cada lote es una unidad de trabajo revisable (uno o pocos commits).
- Cada ítem tiene: severidad, estado, solución y test exigido.
- Al cerrar un ítem se actualiza su estado aquí y se anota el commit.

**Estados:** ⬜ Pendiente · 🟡 Parcial · ✅ Hecho · ⏸ Diferido · ❌ Descartado

**Reglas de trabajo** (las mismas de la Fase 3.6, ver `GEMINI.md`):
1. Tests según la estructura de dos niveles de `GEMINI.md`, con al menos un test negativo que reproduzca el fallo y que falle sin el fix.
2. Sin códigos de auditoría (`DEF-xx`) en código, tests ni docstrings.
3. Cada lote cierra con `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings` y `cargo test --workspace` en verde.

---

## 1. Criterios del orden

1. **Pérdida de datos ya confirmados, primero.** Es lo único que no se puede reparar después.
2. **Lo que afecta al servidor va antes que lo que afecta a consumidores futuros.** El servidor es el único binario desplegable hoy; storage lo va a usar el cliente de la Fase 4.
3. **Los cambios de formato en disco van antes de la Fase 4.** Así ninguna base de datos de cliente necesita migración (contexto V1, sin retrocompatibilidad).
4. **Dependencias técnicas.** El Lote 3 (header del frame con CRC propio) va antes del Lote 4, que saltea registros leyendo solo el header.
5. **Lo latente, cuando no bloquea nada.** El squashing solo lo usa la outbox offline, diferida para después de la v0.1. Se arregla igual, pero sin urgencia, y es obligatorio cerrarlo antes de habilitar la outbox.
6. **La documentación, al final**, para que refleje los cambios de formato, transporte y límites de los lotes anteriores.

**Requisito para empezar la Fase 4:** Lotes 1, 3 y 5 cerrados (son los que tocan el storage del cliente y su formato en disco).

---

## 2. Decisiones de diseño

Se continúa la numeración del plan de la Fase 3.6 (que termina en D42). Todas están **propuestas**: se confirman o cambian antes de arrancar el lote correspondiente.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **D43** | **Sync en storage siempre con flush previo.** Un helper único hace `flush().await?` y después `sync_data`/`sync_all` sobre un `tokio::fs::File`; todos los sitios de storage pasan por él. Para que no se pueda volver a olvidar, `clippy.toml` agrega `disallowed-methods` para `tokio::fs::File::sync_data` y `sync_all`, con un `#[allow]` solo dentro del helper. Alternativa descartada: pasar a `std::fs::File` dentro de `spawn_blocking` (cambio mucho más grande para el mismo resultado). | 1 | Adoptada |
| **D44** | **Decodificación acotada en tres capas.** (a) `/register` mira el tag de variante antes de decodificar el cuerpo y solo decodifica `RegisterClient`; cualquier otra variante es 400 sin decodificar filas. (b) `Deserialize` propio para `CompactRow`, `PrimaryKey` y los deltas de `Update`, que rechaza más de `MAX_COLUMNS` elementos antes de reservar memoria (sin confiar en el `size_hint`). (c) Presupuesto de 1 M de `Value`s por mensaje (`MAX_MESSAGE_VALUES`), aplicado solo cuando el servidor decodifica mensajes de clientes (`decode_client_message`), no a las respuestas: un contador thread-local (la decodificación es síncrona) que los visitors de (b) descuentan una vez por secuencia; un mensaje que lo supera es `BadRequest`. Así un request no puede reservar más de ~24 MiB en `Value`s, sea cual sea su forma. (a) cierra el ataque sin credenciales. Con los mensajes actuales, (b) ya acota los endpoints autenticados (un `Commit` lleva una sola operación, como mucho 131.070 valores); (c) cubre mensajes futuros con varias operaciones. | 2 | Adoptada |
| **D45** | **Borde HTTP con límites propios y transporte documentado como es.** Loop de accept propio con hyper-util (`auto`: HTTP/1.1 y HTTP/2 sin TLS). Límites configurables con `ZEMDB_*` y TOML: headers en 10 s; body con plazo por tasa mínima (base de 60 s desde el fin de los headers, más 1 s por cada `ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC` bytes recibidos, 32 KiB/s por defecto, como `mod_reqtimeout`); 10.000 conexiones simultáneas (backpressure en el accept); 100 streams por conexión HTTP/2. Un request en curso, incluida la escritura de su respuesta, nunca se corta por estos límites, y SSE tampoco. Un body que no llega a tiempo responde `ErrorCode::RequestTimeout` (408, nuevo y reintentable; el protocolo sigue en `0x01`). Al apagar, las conexiones sin request en curso se cierran en el acto. Transporte: **TLS se termina en un reverse proxy** (ejemplo con Caddy en el README). TLS nativo con rustls: descartado para la v0.1. | 2 | Adoptada |
| **D46** | **Frame WAL v2.** Header de 18 bytes: `magic(2) + payload_len(4) + payload_crc(4) + ops_count(4) + header_crc(4)`; `header_crc` cubre los 14 bytes anteriores. Clasificación del final del log: ante un header inválido, un payload truncado o un CRC de payload incorrecto, se busca hacia adelante un frame completo válido (magic + CRC del header + CRC del payload). Si existe, es **corrupción** y no se trunca nada; si no, es **torn write** y se trunca. La búsqueda solo corre ante una anomalía. Aplica a los dos consumidores de `wal_frame` (WAL de storage y segmentos del servidor). Sin retrocompatibilidad: los archivos del formato anterior no se leen (V1, sin despliegues que migrar). | 3 | Propuesta |
| **D47** | **El log del servidor se acota también en bytes.** (a) `ram_max_bytes` para el HotBuffer y `segment_max_bytes` para rotar `active.wal`, además de los límites por operaciones (lo que se alcance primero), sobrescribibles por sala como el resto de la política (D32). (b) `fetch_deltas` y `get_range` reciben un presupuesto en bytes (el de la respuesta) y dejan de clonar al llenarlo; `fit_in_response` queda como red de seguridad. (c) Lectura de segmentos en streaming (`BufReader`), salteando los registros anteriores a `from_seq` con solo leer su header (seguro gracias a D46). (d) Compresión, verificación y lectura de segmentos fríos con streams de zstd, sin cargar el archivo entero. (e) Como mucho N segmentos comprimidos por tick lento (el resto queda para el siguiente), con el mismo tope en el apagado por inactividad. (f) Dedup con `LruCache::unbounded()` y tope aplicado a mano con `pop_lru`. Los valores por defecto se fijan al implementar. | 4 | Propuesta |
| **D48** | **`apply_snapshot` valida antes de reemplazar.** Cada fila se valida contra el esquema del payload (tipos, aridad, PK de la fila igual a la clave, tablas declaradas en el esquema), y la API recibe un `expected_head_seq` (el `snapshot_head_seq` que anunció el relay): si no coincide con `payload.head_seq`, error y la sala queda intacta. Que el esquema del snapshot sea el registrado en el servidor lo verifica el SDK (Fase 4); que el contenido sea verdadero sigue siendo DEF-81. La deserialización pasa al mismo `spawn_blocking` que la descompresión, idealmente en streaming. | 5 | Propuesta |
| **D49** | **Lock de sala estable en storage.** Un archivo `room_{id}.lock` que nunca se renombra, tomado al abrir la sala y liberado al cerrarla (como `lock_log` en el servidor). El WAL nuevo de la rotación se crea sin `truncate`. | 5 | Propuesta |
| **D50** | **Ciclo de vida de salas del servidor.** (a) La entrada de `room_meta` se quita cuando termina el actor (bajo el spawn lock, comparando `actor_id`); la recarga de esquemas recorre solo las salas vivas, y las que se están creando siguen leyendo el registry bajo el spawn lock, como hoy. (b) `shutdown_all` cierra las salas en paralelo con concurrencia acotada. (c) Roster en JSON compacto; las entradas `Dormant` se olvidan tras un TTL largo configurable. Un cliente olvidado que vuelve recibe `ClientNotRegistered` (409) y se registra de nuevo (D27). | 6 | Propuesta |
| **D51** | **Squashing de la outbox por orden de llegada.** (a) La operación entrante siempre gana: el timestamp no decide nada dentro del buffer (queda como metadato). Esto alinea el código con `ARCHITECTURE.md` §10.2 y obliga a reescribir `ARCHITECTURE.md:58-60` (§4.1: "Strict Last-Write-Wins", "Old Insert into Newer Update"). (b) `Insert` + `Delete` → `Delete`; se elimina la aniquilación mutua. Como `Insert` es un upsert y un `Delete` de una fila inexistente es un no-op en storage, mandar el `Delete` siempre es correcto; el costo es una operación extra en el caso de crear y borrar estando offline. Purgar cuando el SDK sepa que la fila no existía queda como optimización futura. (c) `TableBuffer::apply` rechaza deltas repetidos o desordenados (el mismo chequeo que su `Deserialize`), `UpdateBuilder::set` sobre una columna ya seteada reemplaza el valor en vez de duplicarlo, y el buffer valida cada operación contra el `TableSchema` antes de hacer squash. | 7 | Propuesta |
| **D52** | **Tests independientes del entorno del proceso.** La configuración se lee a través de un lector de variables inyectable; los tests le pasan un mapa y no tocan `std::env`. | 8 | Propuesta |

---

## 3. Resumen

| Orden | Lote | Ítems | Prioridad | Progreso |
|---|---|---|---|---|
| 1 | Durabilidad del storage | DEF-87, 108, 112, 114 | Inmediata | ✅ 4/4 |
| 2 | Entrada hostil y borde HTTP | DEF-89, 95, 100 | Inmediata | ✅ 3/3 |
| 3 | Integridad del formato WAL | DEF-91, 92 | Alta (requisito de Fase 4) | ⬜ 0/2 |
| 4 | Memoria y E/S del log del servidor | DEF-90, 97, 99, 96 | Alta | ⬜ 0/4 |
| 5 | Storage listo para el cliente | DEF-93, 98, 107, 101 | Media (requisito de Fase 4) | ⬜ 0/4 |
| 6 | Ciclo de vida de salas del servidor | DEF-102, 104, 103 | Baja | ⬜ 0/3 |
| 7 | Squashing de la outbox | DEF-88, 94, 105, 106 | Latente (antes de habilitar la outbox) | ⬜ 0/4 |
| 8 | Tests y documentación | DEF-109, 110, 111, 113 | Baja | ⬜ 0/4 |

**Avance total:** 7 de 28 ítems (DEF-114 surgió al implementar el Lote 1).

**Por qué este orden, en una línea por lote:**
1. Es la única pérdida silenciosa de datos ya confirmados, y el arreglo son pocas líneas.
2. Es un DoS sin credenciales sobre el único componente desplegable.
3. Cambia el formato en disco: tiene que entrar antes de que existan bases de clientes, y el Lote 4 depende de él.
4. Es el problema de memoria más grande del servidor, pero hace falta una carga con operaciones grandes para dispararlo.
5. Sin consumidor hasta la Fase 4, pero es su requisito.
6. Son defectos de servidores de larga vida o con muchas salas; ninguno pierde commits.
7. Hoy no tiene consumidor. Es independiente del resto, así que se puede adelantar o hacer en paralelo sin conflictos.
8. La documentación tiene que reflejar lo que cambian los lotes 2 a 5.

---

## Lote 1 — Durabilidad del storage

> Objetivo: que ningún batch se confirme sin estar en disco y que un error de I/O nunca termine borrando un archivo durable.

#### DEF-87 · Alto · ✅ Hecho
**Problema.** `write_all` + `sync_data`/`sync_all` sin `flush()` sobre `tokio::fs::File`: el error de la escritura de fondo se descarta y el batch se confirma sin estar en disco. Afecta a `apply_batch`, `close_room`, la compactación (incluida la absorción de `.wal.compacting`, donde la pérdida es permanente) y recovery.
**Solución.** D43: helper único con `flush` antes del sync en los 9 sitios, y `disallowed-methods` en `clippy.toml`.
**Hecho.** `sync_file` (`disk/compactor.rs`) en los 9 sitios y `crates/storage/clippy.toml`, que solo aplica a `zemdb-storage`. `WalWriter::write_record`/`write_batch` (API pública sin usos internos) también hacen `flush` después de escribir. Punto de fallo de test `read_only_handle`, que reproduce el fallo real de la escritura de fondo.
**Test exigido.**
- Unitario del helper: un handle abierto solo para lectura, `write_all` y el helper deben devolver error (sin el flush, `sync_data` devuelve `Ok`; ya está reproducido con tokio 1.53.1).
- `apply_batch` con una escritura de fondo que falla (por ejemplo, un punto de fallo que deja el handle del WAL en solo lectura antes de escribir): devuelve error, el batch no es visible en memoria y la sala queda fallida hasta reabrirse.
- Lo mismo para la absorción de `.wal.compacting`: el WAL activo no se trunca si la escritura falló.

#### DEF-108 · Bajo · ✅ Hecho
**Problema.** `Path::exists()` devuelve `false` ante un error de `stat`, y recovery termina truncando el snapshot real.
**Solución.** `try_exists()?` en recovery, y crear el snapshot inicial con `create_new(true)` en vez de `create` + `truncate(true)`, así un snapshot existente nunca se trunca.
**Test exigido.** Un error en la comprobación de existencia (punto de fallo) hace fallar `open_room` y deja el snapshot intacto.
**Hecho.** `try_exists()?`, con un punto de fallo que reemplaza su resultado (el test falla si se vuelve a `exists()`). En vez de `create_new`, el snapshot inicial pasó a crearse de forma atómica (DEF-114), así que no se trunca nunca.

#### DEF-112 · Bajo · ✅ Hecho
**Problema.** La fase 3 de la compactación (rename del snapshot, sync del directorio y borrado de `.wal.compacting`) no tiene puntos de fallo.
**Solución.** Puntos de fallo `compaction.phase3_rename`, `compaction.phase3_sync` y `compaction.cleanup`.
**Test exigido.** Para cada uno: la compactación falla, se reabre la sala y no se pierde ni se duplica ningún batch.
**Hecho.** El código ya se comportaba así; no hubo que cambiarlo. Los tests escriben batches antes y después del fallo (incluidos un borrado y un update posteriores), reabren o compactan de nuevo y verifican el contenido exacto.

#### DEF-114 · Medio · ✅ Hecho (nuevo, implementación del Lote 1)
**Problema.** El snapshot vacío de una sala nueva se creaba directamente sobre `room_{id}.snap`. Si la escritura, el `flush` o el sync fallaban (por ejemplo, disco lleno), o había un crash antes de que los datos llegaran a disco, quedaba un `.snap` de 0 bytes o con el header incompleto, y desde ahí `open_room` fallaba siempre hasta borrarlo a mano. Antes de DEF-87 el error de escritura se tragaba y el problema aparecía recién en la apertura siguiente.
**Solución.** Se escribe en un `*.snap.tmp.<uuid>` (el mismo que recovery ya limpia al abrir), se sincroniza, se renombra a `.snap` y se sincroniza el directorio, con el mismo camino que usan la compactación y el plegado de `.wal.compacting`. El rename no puede pisar un snapshot real: solo corre si `try_exists` dijo que no existe, con el lock exclusivo del WAL tomado.
**Test exigido.** Un fallo al escribir el snapshot inicial hace fallar `open_room` sin dejar ningún `.snap` ni temporal, y la apertura siguiente arranca vacía; un crash entre el temporal y el rename deja solo el temporal, que la apertura siguiente limpia.

---

## Lote 2 — Entrada hostil y borde HTTP

> Objetivo: que ningún request, autenticado o no, pueda reservar memoria desproporcionada ni retener recursos sin plazo; y que la documentación del transporte diga la verdad.

#### DEF-89 · Alto · ✅ Hecho
**Problema.** `/register` decodifica el mensaje completo antes de autenticar; una `CompactRow` de ~16 M `Null` reserva ~0,5 GB. Con un token, lo mismo vale para `/commit` y `/sync`.
**Solución.** D44 (a), (b) y (c).
**Test exigido.**
- `/register` con la variante `Commit` y un cuerpo inválido después del tag responde 400 "Expected RegisterClient" (prueba que no se decodifica el cuerpo).
- Decodificar una `CompactRow`, una `PrimaryKey` o un `Update` con `MAX_COLUMNS + 1` elementos falla.
- Un mensaje de cliente con más de 1 M de valores se rechaza por el presupuesto. (Con los mensajes actuales no se puede armar por HTTP, porque un `Commit` lleva una sola operación; se prueba decodificando con el presupuesto una lista de operaciones.)
- Un mensaje legítimo grande (por ejemplo, valores `Bytes` cerca del límite de 16 MiB) sigue pasando.

**Hecho.** Extractor `RegisterMessage` que lee el tag de variante; `deserialize_columns` acota filas, claves y deltas; presupuesto en `core/value/decode_budget.rs`, cobrado por secuencia; `/register`, el único endpoint sin autenticación, acepta bodies de hasta 64 KiB (`MAX_REGISTER_BODY_SIZE`). La decodificación de storage quedó con el mismo costo que antes (bench) y el formato no cambió (fuzz diferencial contra la versión anterior).

#### DEF-95 · Medio · ✅ Hecho
**Problema.** Sin timeout de headers ni de body, y sin límite de conexiones: slowloris y bodies por goteo agotan file descriptors y memoria.
**Solución.** D45 (límites).
**Test exigido.** Con tiempo pausado: una conexión que no completa los headers se cierra al vencer el timeout; un body enviado por goteo recibe un error al vencer el suyo; un stream SSE abierto no se corta por esos timeouts; al superar el límite de conexiones, las nuevas esperan o se rechazan sin afectar a las abiertas.
**Hecho.** Loop propio en `api/serve.rs` y plazo del body en `api/body_timeout.rs` (D45). Surgieron en la verificación y quedaron cubiertos con tests: respuestas grandes a lectores lentos que el primer watchdog cortaba (HTTP/1.1 y HTTP/2), un apagado que esperaba a conexiones a medio abrir, y un plazo fijo del body que cortaba a clientes lentos legítimos. Queda sin cubrir, como antes del lote: en HTTP/1.1, un cliente que deja de leer una respuesta retiene la conexión, porque hyper no tiene timeout de escritura.

#### DEF-100 · Medio · ✅ Hecho
**Problema.** La documentación promete "HTTP/2 over TLS"; el servidor habla HTTP/1.1 en texto plano.
**Solución.** D45 (transporte): corregir `ARCHITECTURE.md` §7.2 y el README (HTTP/1.1, TLS en el reverse proxy, con un ejemplo mínimo de despliegue). Si se activa h2c, documentarlo como opcional.
**Test exigido.** Ninguno si solo cambia la documentación. Si se activa h2c: un test de integración con `reqwest` en modo `http2_prior_knowledge`.
**Hecho.** README y `ARCHITECTURE.md` describen HTTP/1.1 y h2c sin TLS detrás de un proxy con TLS, con una sección de despliegue (Caddy, `keepalive` menor que el timeout de headers, límites por IP, `ulimit -n`) y la tabla de variables nuevas.

---

## Lote 3 — Integridad del formato WAL

> Objetivo: que una corrupción a mitad del log nunca se confunda con un torn write (y viceversa), en storage y en el servidor. Requisito de la Fase 4 y del Lote 4.

#### DEF-91 · Medio · ⬜ Pendiente
**Problema.** El CRC del frame no cubre `payload_len`: un bit flip en la longitud de un frame del medio se clasifica como torn write y se truncan frames durables. En el servidor, además, se reasignan secuencias ya entregadas.
**Solución.** D46 (header con CRC propio y búsqueda hacia adelante antes de truncar).
**Test exigido.** En los dos consumidores (recovery de storage y `warm_disk` del servidor): WAL con 3 frames y un bit flip en la longitud del segundo → error de corrupción y el archivo queda sin cambios. Variante con la longitud apuntando más allá del EOF, con el mismo resultado.

#### DEF-92 · Medio · ⬜ Pendiente
**Problema.** Un torn write que no deja un prefijo intacto (header en ceros seguido de bytes no nulos) se clasifica como corrupción y la sala no vuelve a abrir.
**Solución.** D46: si después de la anomalía no hay ningún frame válido, es torn write.
**Test exigido.** Header en ceros seguido de basura sin frames válidos después → se trunca y la sala abre con los batches anteriores. Lo mismo para un `.wal.compacting` a medio absorber. Header en ceros seguido de un frame válido → corrupción.

---

## Lote 4 — Memoria y E/S del log del servidor

> Objetivo: que la memoria y la E/S del log dependan del tamaño de lo que se pide, no del tamaño de los segmentos ni de la cantidad de salas.

#### DEF-90 · Alto · ⬜ Pendiente
**Problema.** HotBuffer, rotación de segmentos, lecturas y compresión se acotan solo por cantidad de operaciones; con operaciones grandes, una sala retiene y clona del orden de GiB.
**Solución.** D47 (a), (b), (c) y (d).
**Test exigido.**
- El HotBuffer desaloja por bytes aunque no llegue a `ram_max_ops`, y `active.wal` rota al llegar a `segment_max_bytes`.
- `fetch_deltas` con operaciones grandes devuelve como mucho lo que entra en la respuesta y no clona de más (se verifica con la cantidad devuelta y `has_more`).
- Compresión y lectura fría en streaming: ida y vuelta de un segmento con operaciones grandes.
- Los tests de continuidad entre tiers (`fetch_deltas` sin huecos) siguen en verde.

#### DEF-97 · Medio · ⬜ Pendiente
**Problema.** Leer un rango decodifica y verifica todo lo anterior a `from_seq`, y en frío descomprime el archivo entero: ponerse al día cuesta O(S²/B).
**Solución.** D47 (c) y (d).
**Test exigido.** Leer el final de un segmento decodifica solo los frames pedidos (contado con `io_probe` o un contador de decodificaciones); el resultado es igual al de la lectura completa.

#### DEF-99 · Medio · ⬜ Pendiente
**Problema.** Una pasada de compresión sin tope congela al actor y los requests de la sala terminan en 504.
**Solución.** D47 (e).
**Test exigido.** Con N+k segmentos vencidos, un tick lento comprime como mucho N, el actor atiende un comando entre ticks y los siguientes ticks terminan el resto.

#### DEF-96 · Medio · ⬜ Pendiente
**Problema.** `LruCache::new` reserva el mapa completo (~280 KiB por sala con la capacidad por defecto) aunque esté vacío.
**Solución.** D47 (f).
**Test exigido.** Un caché nuevo no reserva la capacidad máxima, y al superar la capacidad se desaloja lo menos usado (los tests actuales de dedup siguen en verde).

---

## Lote 5 — Storage listo para el cliente

> Objetivo: que el motor de storage sea seguro de usar desde el SDK de la Fase 4 (snapshots de otros clientes, runtime de un solo hilo, varios procesos). Requisito de la Fase 4.

#### DEF-93 · Medio · ⬜ Pendiente
**Problema.** `apply_snapshot` reemplaza las tablas sin validar filas, tablas ni el `head_seq` declarado.
**Solución.** D48 (validación y `expected_head_seq`).
**Test exigido.** En los dos motores: un snapshot con una fila de tipo inválido, con aridad incorrecta, con la PK distinta de la clave, con una tabla desconocida o con `head_seq` distinto del esperado se rechaza y la sala queda igual que antes.

#### DEF-98 · Medio · ⬜ Pendiente
**Problema.** La deserialización de snapshots corre en el worker de tokio; el motor en memoria serializa con el lock de la sala tomado; `apply_snapshot` copia el snapshot comprimido.
**Solución.** D48 (deserialización en `spawn_blocking`). En `MemoryStorageEngine::create_snapshot`, clonar las tablas (O(1) con `imbl`) y soltar el lock antes de serializar, como ya hace el motor de disco. Evitar la copia de `to_vec()`.
**Test exigido.** Los tests de snapshots y recovery siguen en verde. Un test con un runtime current-thread en el que otra tarea avanza mientras se aplica un snapshot grande, si se puede hacer estable; si no, queda justificado en la revisión.

#### DEF-107 · Bajo · ⬜ Pendiente
**Problema.** Durante la rotación de la compactación no hay ningún `room_X.wal` con lock: un segundo proceso puede abrir la sala y sus batches se pierden.
**Solución.** D49.
**Test exigido.** Con la compactación pausada en `compaction.rotate_renamed`, abrir la misma sala desde otro motor falla por el lock.

#### DEF-101 · Medio · ⬜ Pendiente
**Problema.** `test_cow_compaction_concurrent_writes_unblocked` no fuerza el solapamiento ni verifica que el escritor no se bloquee.
**Solución.** Reescribirlo: `arm_pause("compaction.staging")`, un `apply_batch` que termina antes del `release()`, y verificar ese batch después de reabrir.
**Test exigido.** El propio test, que tiene que fallar si `apply_batch` espera a la compactación (por ejemplo, con un timeout corto sobre la escritura mientras la compactación está pausada).

---

## Lote 6 — Ciclo de vida de salas del servidor

> Objetivo: que un servidor de larga vida con muchas salas no acumule memoria por salas cerradas y se apague dentro del período de gracia.

#### DEF-102 · Bajo · ⬜ Pendiente
**Problema.** `room_meta` retiene una entrada por cada sala abierta alguna vez, y la recarga de esquemas toma el spawn lock de todas.
**Solución.** D50 (a).
**Test exigido.** Después del apagado por inactividad la sala no tiene entrada en `room_meta`; una recarga de esquema con una sala reabriéndose en paralelo igual le llega (el caso que hoy cubre el spawn lock).

#### DEF-104 · Bajo · ⬜ Pendiente
**Problema.** `shutdown_all` cierra las salas de a una y puede pasarse de los 10 s de gracia.
**Solución.** D50 (b).
**Test exigido.** Con una sala pausada en su apagado, las demás terminan igual; el roster de todas queda guardado.

#### DEF-103 · Bajo · ⬜ Pendiente
**Problema.** El roster nunca olvida clientes `Dormant` y se reescribe en JSON pretty.
**Solución.** D50 (c).
**Test exigido.** Un cliente `Dormant` se olvida al vencer el TTL y, si vuelve, recibe `ClientNotRegistered` y puede registrarse de nuevo; el roster se escribe en JSON compacto y se sigue leyendo bien.

---

## Lote 7 — Squashing de la outbox

> Objetivo: que la outbox, cuando exista, envíe siempre un resultado equivalente a enviar las operaciones en orden. Hoy no tiene consumidor (outbox diferida post-v0.1); se puede adelantar o hacer en paralelo con cualquier otro lote.

#### DEF-88 · Alto · latente · ⬜ Pendiente
**Problema.** `Delete → Insert → Delete` (o un `Insert` que sobrescribe seguido de un `Delete`) termina en un buffer vacío y la fila sigue viva en el servidor.
**Solución.** D51 (b).
**Test exigido.** `Delete(K) → Insert(K) → Delete(K)` deja un `Delete(K)` pendiente; `Insert(K) → Delete(K)` también. Los tests de `crates/core/tests/squashing_rules_tests.rs` que esperan `Purged` se actualizan a la nueva regla.

#### DEF-94 · Medio · latente · ⬜ Pendiente
**Problema.** La precedencia depende del reloj de pared: si retrocede, un `Delete` posterior se descarta.
**Solución.** D51 (a), y reescribir `ARCHITECTURE.md:58-60`.
**Test exigido.** `Insert(ts=1000)` seguido de `Delete(ts=990)` deja un `Delete` pendiente; dos `Update` del mismo campo con timestamps decrecientes dejan el valor del último en llegar.

#### DEF-105 · Bajo · latente · ⬜ Pendiente
**Problema.** El LWW por timestamp no se cumple por columna después de mezclar updates.
**Solución.** Desaparece con D51 (a): ya no hay LWW por timestamp en el buffer.
**Test exigido.** `U1(ts=100, A=x)`, `U2(ts=90, B=old)`, `U3(ts=95, B=new)` deja `A=x, B=new`.

#### DEF-106 · Bajo · latente · ⬜ Pendiente
**Problema.** `apply` acepta deltas que el `Deserialize` del buffer rechaza (se pierde la outbox persistida), y un `Update` inválido contamina un `Insert` válido.
**Solución.** D51 (c).
**Test exigido.** `apply` con deltas repetidos o desordenados devuelve error; `UpdateBuilder::set` dos veces sobre la misma columna produce un solo delta con el último valor; `Insert` válido + `Update(col 99)` rechaza el `Update` y deja el `Insert` intacto; serializar y deserializar el buffer después de cualquier secuencia aceptada por `apply` siempre funciona.

---

## Lote 8 — Tests y documentación

> Objetivo: tests que prueben lo que dicen y documentación que coincida con el código después de los lotes anteriores.

#### DEF-109 · Bajo · ⬜ Pendiente
**Problema.** `test_disk_concurrent_readers_and_writers` descarta los errores y el contador de los lectores.
**Solución.** `unwrap()` del resultado de cada lectura y verificar el contador o el valor leído.
**Test exigido.** El propio test, que tiene que fallar si un lector recibe error.

#### DEF-110 · Bajo · ⬜ Pendiente
**Problema.** Tests de configuración que fallan si hay `ZEMDB_*` en el entorno y que modifican el entorno del proceso.
**Solución.** D52.
**Test exigido.** `ZEMDB_LEASE_TIMEOUT_SECS=30 cargo test --workspace` en verde.

#### DEF-111 · Bajo · ⬜ Pendiente
**Problema.** `tiered_log_tests.rs`, `unified_wal_tests.rs` y `room_lifecycle_resilience_tests.rs` usan internals expuestos como `pub` solo para testear.
**Solución.** Moverlos a unitarios en `server/src/log/tests/` (y `actor/tests/` donde corresponda) y pasar `log` y `dedup` a `pub(crate)`. Cierra la reestructuración de tests que quedó pendiente en la Fase 3.6.
**Test exigido.** Se ejecuta exactamente la misma cantidad de tests antes y después del movimiento.

#### DEF-113 · Bajo · ⬜ Pendiente
**Problema.** Documentación desactualizada (ver la auditoría para las líneas).
**Solución.**
- `README.md`: magic "ZM", sin micro-WAL, versión de Rust real, cantidad de tests sin número fijo (o la real), "L1 cache-line aligned" reemplazado por "cabe en 64 B".
- `Cargo.toml`: `rust-version = "1.85"` en `[workspace.package]`; README: 1.85+ para compilar, 1.88+ para correr los tests.
- `ARCHITECTURE.md`: sin micro-WAL (§5.1 y el árbol de archivos de §9.2), merge por campo en las réplicas y no en el servidor (§4), y lo que cambien los lotes 2 a 5 (transporte, formato del frame v2, límites en bytes, validación de snapshots).
- `ROADMAP.md`: encabezado y fecha, tabla de crates, `SequencedOperation`, A-10, build wasm32 hecho, micro-WAL marcado como reemplazado por `active.wal`, y referencia a las Fases 3.6 y 3.7.
- `docs/audits/2026-09-post-fase3-audit.md` (M-03): "RM" → "ZM".
**Test exigido.** Ninguno (documentación). Revisión cruzada de cada afirmación cambiada contra el código.
