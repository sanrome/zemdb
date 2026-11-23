# Navegador (Rust) — Plan de la sub-fase 4.5

**Fecha:** 19 de Noviembre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lote 1 (el arnés del navegador) hecho el 23 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda; el paso S1, en curso. Lote 0 (el contrato) cerrado el 23 de Noviembre de 2026, con la revisión cruzada en verde en su sexta ronda y DC46 en su principio, con su protocolo para el paso D (§1.3). El dueño del proyecto adoptó DC40 y DC43 el 22 de Noviembre de 2026, y eligió ese día la pestaña líder para DC41, de donde sale DC46; adoptó las demás el 23 (DC46, en su principio: su protocolo, en el paso D). La CI de `main`, en verde sobre `91850bf`.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.5. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).
**Parte de:** el núcleo de 4.2 ([plan](2026-10-fase4_2-nucleo-replica.md)), el worker y el transporte de 4.3 ([plan](2026-11-fase4_3-transporte-worker.md)), la fachada de 4.4 ([plan](2026-11-fase4_4-api-rust.md)) y la capa de archivos de 4.1 (DC17, [plan](2026-10-fase4_1-storage-servidor.md)). Depende de 4.3, que cerró el 19 de Noviembre de 2026 con sus Lotes 11b y 11c (`496da6c`), mientras se escribía este plan: ningún lote de 4.5 la espera.

---

## 0. Resumen

- **Qué.** El SDK de Rust en el navegador:
  - el cliente compilado a wasm32 con la feature `wasm`, con un runtime propio (las tareas, los plazos, el reloj y el azar del navegador);
  - el motor en disco de hoy sobre el *Origin Private File System* (DC8, DC17);
  - el transporte del navegador, con `fetch` también para el stream de eventos;
  - una pestaña líder por directorio del cliente, que abre sus archivos y atiende a las demás pestañas (INV-K13);
  - el `CryptoEngine` del SDK sobre WebCrypto, con las claves de la aplicación en el Worker (INV-K9);
  - el runtime del Worker, que recibe los comandos de la página, los ejecuta sobre la fachada de 4.4 y publica las respuestas y los valores de `watch`;
  - los metadatos durables antes de servirse (DEF-189, INV-K16);
  - las pruebas en navegadores headless, con el criterio de aceptación: la réplica sobrevive a recargar la página.
- **Dónde.** En `zemdb-storage` (la capa del OPFS y el motor en disco en wasm32) y en `zemdb-client` (lo demás), los dos en el alcance auditado: cada lote, con su revisión de delta. Las pruebas del navegador, en un arnés nuevo (`tools/browser/`, DC44), con su job de la CI. El servidor no cambia.
- **El modelo de amenaza.** No cambia de forma: DC11 ya pone en el alcance el navegador, el origen como la aplicación y la concurrencia entre pestañas, y en (3) extiende al OPFS el supuesto del disco. §2 de `INVARIANTES.md` precisa ese supuesto (lo que en el OPFS hace las veces de sincronizar un directorio: `move()`, crear y borrar entradas) y lo que el SDK le pide al navegador para coordinar las pestañas (los Web Locks, su cola y los handles exclusivos, y el `BroadcastChannel` entre ellas), como §2 ya confía en el sistema operativo para `flock`; dice que las demás pestañas del origen, que usan el SDK a través de la líder, también son la aplicación; y dice dónde está el `CryptoEngine` en el navegador. Por eso no pide una auditoría amplia (§1.4): las garantías nuevas se auditan en la revisión de cierre (§3). Si el dueño del proyecto lo ve como un cambio del modelo, la revisión de cierre se hace con el método de una auditoría amplia.
- **Qué no.** El paquete de TypeScript, el codec de objetos de JavaScript con las representaciones de DC14 (INV-K14), `persist()` desde la página, Vite y la app de referencia en el navegador (4.6); CORS en el servidor; un lock por sala; un `CryptoEngine` en JavaScript cargado en el Worker; partir en pasos el trabajo de CPU largo del Worker (todo eso, al backlog si hace falta).
- **Decisiones nuevas:** DC38 a DC46 (§1), adoptadas por el dueño del proyecto: DC40 y DC43 el 22 de Noviembre de 2026, y las demás el 23 (DC46, en su principio: su protocolo, en el paso D). Precisan decisiones de la fase:
  - DC8 y DC17 (la implementación del OPFS, DC39; la coordinación entre pestañas, DC41 y DC46);
  - DC27 y §A.5 (el stream del navegador por `fetch`, y no por `EventSource`, DC40);
  - DC28 (en wasm32, los syncs del worker de a uno y sus transferencias de a una, DC40);
  - DC14 y DC18 (el enum de comandos, DC42, con las demás pestañas, DC46);
  - DC32 (un error nuevo, `InUse`, DC41; los errores de las demás pestañas, en el paso D de DC46);
  - DC34 (en el navegador, el `CryptoEngine` es el del SDK, con las claves de la aplicación, DC43).
- **Spikes:** S1 en Firefox (DC8) y S3 en Safari y Firefox (DC17), pendientes de 4.0 y 4.1, y S5, nuevo (§1.2). Los de Safari y Firefox necesitan la Mac del dueño del proyecto o un runner de macOS: §1.2 dice cuándo.
- **Cómo cierra:** con una revisión de cierre (`docs/METODOLOGIA.md` §6), con tres revisores (§3), con las pruebas del navegador en verde en Chromium en la CI y la página de aceptación corrida en Safari y Firefox.

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC8 (el OPFS), DC9 (`ruzstd` para el `.snap` del navegador), DC11 (el navegador en el modelo de amenaza), DC14 (los principios del límite del Worker), DC17 (la capa de archivos), DC18 (el enum de comandos, que aparece en el runtime del Worker) y DC34 (el cifrado). Esta sub-fase agrega las siguientes:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC38** | La feature `wasm` y el runtime del cliente: tareas, plazos, reloj, azar y hora del navegador, sin `Send` en wasm32 | 2, 4, 7, 8 | Adoptada (23 de Noviembre de 2026) |
| **DC39** | El motor del navegador: `DiskStorageEngine` sobre `OpfsFiles`, una capa de archivos del OPFS | 2, 3, 4, 8 | Adoptada (23 de Noviembre de 2026), con la condición de S1 en Firefox, S3 en Safari y Firefox y la pregunta 1 de S5 (§1.2) |
| **DC40** | El transporte del navegador: `fetch`, también para el stream de eventos, con el tope de lo que lee y de sus streams | 9 | Adoptada (22 de Noviembre de 2026) |
| **DC41** | Una pestaña líder por directorio del cliente, con un Web Lock, y el Worker que la página termina en `pagehide` | 10, 12, 15 | Adoptada (23 de Noviembre de 2026); la pestaña líder, elegida el 22; con la pregunta 2 de S5 |
| **DC42** | El runtime del Worker: un despachador de comandos serializables, y su punto de entrada con un codec intercambiable | 6, 12 | Adoptada (23 de Noviembre de 2026) |
| **DC43** | Las claves en el Worker: un `CryptoEngine` del SDK sobre WebCrypto, AES-GCM con un `CryptoKey` por sala | 11, 12 | Adoptada (22 de Noviembre de 2026) |
| **DC44** | Las pruebas en el navegador: el arnés, la página que se recarga sola, Playwright, los tests unitarios en wasm32 y la CI | 1, 3, 15 | Adoptada (23 de Noviembre de 2026), con Safari en un runner de macOS (§1.2) |
| **DC45** | DEF-189: los metadatos, durables antes de servirse | 2, 5, 8 | Adoptada (23 de Noviembre de 2026), con la pregunta 1 de S5 |
| **DC46** | Las demás pestañas: usan el SDK a través de la pestaña líder, por un `BroadcastChannel`, con el relevo cuando la líder se va | 6, 13, 14, 15 | Adoptada en su principio (23 de Noviembre de 2026); su protocolo, en el paso D, con la pregunta 6 de S5 |

### DC38 · La feature `wasm` y el runtime del cliente
- **La feature.** `wasm` compila en wasm32 el cliente entero (el núcleo, el worker, la fachada y los metadatos sobre la capa de archivos), con `wasm-bindgen`, `wasm-bindgen-futures`, `js-sys` y `web-sys`. `native` sigue por defecto y solo tiene efecto fuera de wasm32; `wasm`, solo en wasm32. Sin `wasm`, en wasm32, el crate sigue vacío, como hoy. La CI suma `cargo check --target wasm32-unknown-unknown -p zemdb-client --no-default-features --features wasm` (§4.5 del plan de la fase) y su clippy, en el job de WebAssembly.
- **El runtime, en un módulo interno** (`rt`), con lo que el cliente toma hoy de Tokio y del sistema:
  - lanzar una tarea que corre hasta su fin aunque nadie la espere (de lo que dependen el aplicador de DC21, los metadatos y el worker), con un handle para esperarla y para cortarla (el corte del worker, DC28);
  - dormir hasta un instante, un plazo sobre un future y el instante monotónico (DC11, (4));
  - el trabajo de CPU (`spawn_blocking`, el hash de un snapshot);
  - el azar (los `MutationId`, el id de dispositivo y el *jitter*) y la hora de pared en milisegundos (el `timestamp` de una operación).

  En nativo es Tokio y el sistema, sin ningún cambio de comportamiento: la simulación sigue controlando todos los plazos con el reloj pausado. En wasm32:
  - las tareas, con `wasm_bindgen_futures::spawn_local`;
  - los timers, con `setTimeout` del ámbito global (el del Worker, o el de la página); un timer que se dispara antes del instante pedido vuelve a dormir hasta él, así que un plazo nunca se cumple antes;
  - el instante, con `performance.now()`;
  - el trabajo de CPU, en el lugar (no hay pool);
  - el azar, con `crypto.getRandomValues`, por `getrandom` con su feature `wasm_js`. `uuid` no compila en wasm32 sin elegir su fuente: el cliente y el storage sacan el azar de `getrandom` en wasm32, o activan su feature `js`;
  - la hora, con `Date.now()`.
- **Un lint** (`clippy::disallowed_methods`, con un `clippy.toml` en `crates/client` y en `crates/storage`) deja fuera de `rt` y de `sys` lo que en wasm32 entra en pánico o no corre: `std::time::Instant::now`, `SystemTime::now`, `tokio::spawn`, `spawn_blocking` y los timers de Tokio. Vale solo al compilar para wasm32 (fuera de wasm32, los crates lo permiten con `cfg_attr`), así que no toca los tests ni el servidor, que siguen con Tokio.
- **El storage** suma lo mismo para sus tareas (`open_room`, `apply_snapshot`, `delete_room` y la compactación, hoy con `tokio::spawn`) y el azar de sus nombres temporales, en su módulo `sys`, donde ya está `run_blocking`: la tarea, en el Lote 2, que la usa para las promesas del OPFS; las del motor y el azar, en el 4.
- **Sin `Send` en wasm32.** Como `StorageEngine` (`EngineConcurrencyBounds`) y `CryptoEngine` (`CryptoConcurrencyBounds`) hoy, con `async_trait(?Send)`, los traits que implementa la plataforma o la aplicación (`TransportClient`, `TokenProvider`, `MetadataStore`, `RoomFiles` y `RoomFile`) no piden `Send` ni `Sync` en wasm32, donde los objetos de JavaScript no lo son, y los streams tampoco (`EventBlocks`, y `ChangeStream`, que pasa a un `LocalBoxStream`: el pendiente de la nota del Lote 6 de 4.4). En nativo no cambia nada.
- **Lo que no cambia:** `tokio::sync` (que el storage ya usa en wasm32) y `select!`; el formato, el protocolo y lo que hace cada paso del núcleo y del worker.
- **Límite:** en wasm32 todo corre en el hilo del Worker. El trabajo de CPU largo (una compactación, la instalación de un snapshot o su hash: unos segundos en una sala de un millón de filas, S2) demora los demás comandos mientras dura; la página no se frena.
- **Alternativas descartadas:**
  - el runtime de Tokio en el Worker: no corre en el bucle de eventos del navegador (un Worker que bloquea en `block_on` no deja que se resuelvan las promesas que espera), y su reloj es `std::time::Instant`, que en wasm32-unknown-unknown entra en pánico;
  - envolver los objetos de JavaScript en un `SendWrapper` para conservar `Send`: esconde que todo corre en un hilo y entra en pánico si algo los mueve de hilo; el storage ya eligió `?Send`.

### DC39 · El motor del navegador (DC8, DC17)
- **`OpfsFiles`,** una implementación de `RoomFiles` en `zemdb-storage`, en wasm32, sobre el OPFS; y `DiskStorageEngine<OpfsFiles>`, el motor en disco de hoy compilado en wasm32, con el mismo formato, la misma recuperación, la compactación, la instalación de snapshots y sus locks (DC17). `MemoryStorageEngine` sigue, para una aplicación que no quiere persistir.
- **Los bindings.** `web-sys` no trae `move()` de un archivo, y trae `navigator.locks` solo con `--cfg=web_sys_unstable_apis`, que obligaría a todo el que compile el crate: lo que falte va con bindings propios (`#[wasm_bindgen] extern`), que compilan con `forbid(unsafe_code)`.
- **La raíz:** el directorio del OPFS del origen (`navigator.storage.getDirectory()`, en el Worker), y bajo él las rutas que usa el motor, separadas por `/`: el `data_dir` del motor es un directorio relativo a esa raíz. Una ruta con `..`, o que sale de la raíz, se rechaza. Los metadatos del cliente (`FileMetadata`) van en el mismo directorio que el motor, como en nativo (§11 de `ARCHITECTURE.md`).
- **Los archivos:** un `FileSystemSyncAccessHandle` por archivo abierto, en el modo por defecto (`readwrite`), exclusivo también para leer: es el único modo de Safari, y Chrome tiene otros (`read-only`, `readwrite-unsafe`) que no se usan (S1).
  - `open_locked` y `create_locked` dan `None` cuando otro handle tiene el archivo, con el error que da cada navegador al abrir un handle (S1: `NoModificationAllowedError` en Chrome, `InvalidStateError` en Safari, donde puede salir también por otras causas, que el paso S1 mira; Firefox, en ese paso). En otro lugar, esos errores son errores.
  - `read_at` da exactamente lo pedido o falla: repite una lectura corta (el requisito de DC17).
  - `write_at` escribe todo; `size`, `set_len`, `sync_data` y `sync_all` son `getSize`, `truncate` y `flush`.
  - Renombrar un archivo abierto cierra el handle, hace `move()` y lo reabre sobre el mismo `FileSystemFileHandle` (DC17, S3). Si la reapertura falla, el handle queda muerto: toda operación siguiente falla, y nunca escribe en otro archivo (DC17).
- **El directorio:** `exists`, `list` (las entradas del directorio, por prefijo), `rename` (`move()` sobre un destino que se reemplaza, sin handles abiertos en ninguno de los dos), `remove` (`removeEntry`) y `create_dir_all`; `sync_dir` no hace nada, y su contrato pasa a decir que, donde no hace nada, solo se garantizan durables los cambios de las operaciones que salieron bien (si hace durable uno que ya ocurrió, lo dice el método de DC45). `create_locked` verifica que el archivo no exista y después lo crea, porque el OPFS no tiene un *create-new* atómico: es seguro bajo el lock de la sala y con una pestaña por directorio (DC17, DC41).
- **Una operación soltada termina** (INV-L7, DC17). Las operaciones del OPFS que son promesas (obtener un handle de archivo o de directorio, `createSyncAccessHandle`, `move()`, `removeEntry`, listar) siguen en el navegador aunque se suelte el future que las espera, así que la capa las corre en una tarea propia (la de `sys`, DC38) que retiene el handle, y su lock, hasta que la promesa termina: la próxima operación sobre ese archivo la espera. Las síncronas (`read`, `write`, `flush`, `truncate`, `getSize`) terminan en la misma llamada.
- **Los errores:** cada `DOMException` pasa a un `io::ErrorKind` (`NotFoundError` a `NotFound`, la cuota llena a `StorageFull`), con su nombre en el mensaje.
- **El motor en wasm32:** sus tareas, con la de DC38; su trabajo de CPU, en el lugar; el `.snap`, comprimido con `ruzstd` `Fastest` (DC9), y `zstd_level`, validado contra lo que da en wasm32 (hoy, con `zstd::compression_level_range()`); los nombres temporales, con el azar de DC38; y el módulo de los *fail points*, que hoy no compila en wasm32, compila ahí, donde fuera de los tests no hace nada, como en nativo (sus pausas, que esperan con `Instant`, quedan para los tests nativos).
- **Supuestos** (§2 de `INVARIANTES.md`). En el OPFS, sincronizar un directorio no hace nada, así que lo que en nativo hace durable `sync_dir` tiene que serlo cuando su promesa resuelve:
  - lo escrito con `flush()` sobrevive como lo sincronizado con `fsync` (ya está, S1);
  - un `move()` resuelto es atómico y durable, como un `rename` seguido de sincronizar el directorio;
  - una entrada creada (un archivo, con `getFileHandle` y `create`, o un directorio, con `getDirectoryHandle` y `create`) o borrada (`removeEntry`) cuya promesa resolvió es durable, en el orden en que resolvieron sus promesas, como un cambio de directorio sincronizado.

  De eso dependen la instalación de un snapshot, la rotación del WAL, el directorio de datos, el WAL y el `.lock` que crea la apertura de una sala, los borrados de `delete_room` (INV-S8), y los guardados y los borrados de los metadatos del cliente (INV-K16). Son supuestos, como `flush()`: S5 los prueba donde se puede, cortando el navegador, que no es un corte de energía.
- **Condición:** si S1 o S3 en Firefox, o S3 en Safari, contestan que no (por ejemplo, un navegador sin `move()` de un archivo), DC39, y con ella DC8, se revisan antes del Lote 2. Lo mismo si S5 (1) contesta que un `move()`, una creación o un `removeEntry` que se rechazan pueden haber tenido efecto: en nativo, el `sync_dir` siguiente hace durable un cambio así (el de la apertura de una sala, para el WAL que creó una apertura anterior; el de `delete_room`, para un borrado), y en el OPFS DC39 tiene que decir qué hace el motor con él. Por ejemplo, si un `removeEntry` del `.snap` o de `.wal.compacting` se rechaza después de borrar, `delete_room` lo da por hecho al reintentar (un `NotFound` es éxito), y el archivo podría reaparecer después de un corte, en contra de INV-S8: el `.snap`, con la sala que se dio por borrada; `.wal.compacting`, sin su `.snap` (un estado que no es un prefijo). El WAL activo no: al reintentar, `lock_room_files` lo vuelve a crear y `delete_room` lo borra otra vez.
- **Límites:** la réplica vive en la memoria del Worker (en wasm32, hasta 4 GiB, y el navegador puede dar menos), dos veces mientras se instala un snapshot (INV-K15); en wasm32 una reserva que falla deja la instancia inservible, como un pánico (DC42). Sin `persist()`, el navegador puede borrar el OPFS del origen: una pérdida total (§2).
- **Alternativas descartadas:** IndexedDB (DC8); un motor del navegador escrito aparte (DC17).

### DC40 · El transporte del navegador
- **`FetchTransport`,** un `TransportClient` del cliente con la feature `wasm`, sobre el `fetch` del ámbito global (el del Worker), con `web-sys` y sin otra dependencia. Cada mensaje es el cuerpo de un `POST` a su endpoint (`endpoint`), con el token en `Authorization: Bearer`, sin cookies (`credentials: "omit"`), sin caché y sin seguir redirecciones, como el nativo. La URL del servidor, en el mismo origen que la página o con CORS (abajo).
- **El tope de lo que lee** (DEF-195, INV-K12). Una respuesta que anuncia más que `MAX_FRAME_SIZE` (por `Content-Length`), o que trae más, falla sin leer el resto: el cuerpo se lee de su `ReadableStream`, bloque a bloque, y se cancela al pasarse. No se usa `arrayBuffer()`, que lee todo.
- **Lo que reserva una respuesta** (la otra mitad de DEF-195). Una respuesta se decodifica sin el presupuesto de valores, y reserva hasta unas 24 veces su tamaño (INV-K12): con `MAX_FRAME_SIZE` (16 MiB), unos 384 MiB. Entra en el Worker de a una, pero varias a la vez, de salas que se ponen al día juntas, pueden no entrar, y en wasm32 una reserva que falla deja la instancia inservible (un crash, DC42: nada se pierde, y puede repetirse al reabrir). Por eso, en wasm32, los syncs del worker de todas las salas van de a uno, y sus transferencias (una bajada con la instalación de su snapshot, que puede ocupar dos veces la réplica, DC39, o una subida), de a una, con permisos separados, para que una bajada larga no frene los syncs de las otras salas. Un sync puede esperar al de otra sala, y la subida del designado, a una bajada de otra sala, más que los 60 s en que el servidor espera que empiece: entonces el servidor designa a otro, o de nuevo al mismo si no hay otro (§A.8): una demora acotada (INV-K7). Precisa DC28. Los permisos son fijos, sin configuración: con más de uno no se gana casi nada, porque todo corre en un hilo, y con varios (por ejemplo, seis) volvería el riesgo de memoria (seis respuestas de hasta 384 MiB) y, con HTTP/1.1, los syncs ocuparían las conexiones de las escrituras. Lo que queda es de la aplicación, como en la API de Rust: sus escrituras y sus `sync` en varias salas a la vez, y los comandos del despachador en vuelo. Va al §4 de `INVARIANTES.md`, como riesgo aceptado: el crash se repite al reabrir solo si vuelven a coincidir respuestas así, o si la réplica no entra en la memoria.
- **Los plazos** (INV-K7), con un `AbortController` por pedido. `fetch` no deja ver la conexión ni el avance del envío del cuerpo, así que hay un solo plazo hasta los headers, el de inactividad (`idle_timeout`, 60 s por defecto, como el nativo), y el mismo entre los bloques del cuerpo de la respuesta de un pedido: `connect_timeout` no se usa en el navegador. Un stream abierto no tiene plazo, como dice el trait: su silencio lo vigila el worker (45 s, DC26). Mandar el cuerpo cuenta como espera: el plazo tiene que alcanzar para subir un chunk de snapshot (4 MiB, DC22) en el enlace más lento que la aplicación espera (unos 0,6 Mbit/s con 60 s). Un pedido cortado no reemplaza nada, porque las conexiones son del navegador: una conexión HTTP/2 que deja de contestar sigue hasta que el navegador la cierra, y los pedidos que van por ella vencen su plazo; el SDK no puede abrir otra (un límite, que el nativo no tiene).
- **El stream de eventos, también por `fetch`,** y no por `EventSource` con `?token=`, como decían §4.5 y §A.5 del plan de la fase: `GET /rooms/{room_id}/events` con el token en el header, y el cuerpo, bloque a bloque, como `EventBlocks`, con el parser del núcleo (64 KiB por evento, DC27). Por qué:
  1. `EventSource` no deja leer el código ni el cuerpo de una negativa, así que el worker no distinguiría `EventsDisabled`, `Unauthorized`, `Forbidden` y `RateLimited`, de los que dependen DC24, DC29 y DC30 (INV-K17);
  2. se reconecta solo, fuera del backoff del worker y sin renovar el token;
  3. el navegador junta cada evento entero, sin tope, antes de entregarlo (INV-K12, INV-K15);
  4. el token en la URL queda en los logs, y el servidor lo acepta ahí solo por `EventSource` (`ARCHITECTURE.md` §7.2). El `?token=` del servidor sigue, para otros clientes.
- **El tope de streams.** La conexión la elige el navegador. Con HTTP/1.1, el stream de cada sala en `live` ocupa una de las pocas conexiones por origen (seis en los navegadores comunes), compartidas entre pestañas. Con seis salas con un `watch` (DC36), los pedidos se quedarían sin conexión: escrituras inciertas y clientes que no se ponen al día (INV-K7).
  - Por eso el transporte de un cliente abre a lo sumo `max_streams` streams a la vez (2 por defecto, configurable). Con varios clientes en el origen, o con otras conexiones largas de la aplicación, la aplicación lo baja; con HTTP/2, lo puede subir.
  - Uno de más se niega en el lugar, con el frame de `RateLimited`. El worker lo reintenta con su backoff, y la sala sigue como en `periodic` (su heartbeat compara su head, DC24); `status` la muestra en `live`, con el stream cerrado (`stream_open` en falso).
  - Con HTTP/2, el stream comparte la conexión de los pedidos, y mientras el worker aplica un evento no lo lee (INV-K15): el navegador frena lo que recibe (S5, pregunta 4).
  - La alternativa, que el dueño del proyecto puede preferir: HTTP/2 entre el navegador y el proxy como supuesto de §2, sin tope.
- **CORS:** el servidor no tiene CORS. La aplicación lo sirve en su mismo origen, por el proxy que termina TLS (§2), o con un proxy que agregue los headers. CORS en el servidor queda fuera de 4.5 (al backlog, si la app real lo necesita). Las pruebas sirven la página y el servidor desde un mismo origen (DC44).
- **Alternativas descartadas:**
  - `reqwest` en wasm32, que envuelve `fetch` sin dejar cortar entre bloques ni leer con un tope; `gloo-net` (§A.1 del plan de la fase), otra capa sin el tope;
  - `XMLHttpRequest` para los pedidos, que en un Worker informa el avance del envío (`upload.onprogress`), pero junta el cuerpo de la respuesta entero (el tope se aplica sobre lo que ya juntó) y sería otra API además de la del stream: queda como alternativa si el plazo que cuenta el envío resulta un problema;
  - `EventSource`, por lo de arriba.

### DC41 · Una pestaña líder por directorio (INV-K13)
- **El lock.** En wasm32, `FileMetadata::open` toma, antes de tocar ningún archivo (también antes de crear su directorio), un Web Lock exclusivo (`navigator.locks.request`) cuyo nombre sale de la ruta normalizada de su directorio del OPFS, y lo conserva hasta su `close` (el de `shutdown`), hasta que se suelta el store o hasta que termina el Worker. La unidad es el directorio: `FileMetadata` retiene `client.lock` mientras está abierto, y en el OPFS su handle es exclusivo, así que a lo sumo una pestaña tiene un cliente sobre ese directorio, con todas sus salas (INV-K13). Los metadatos van en el directorio del motor (DC39), así que el lock cubre los dos. Un lock por sala pediría unos metadatos sin `client.lock` en el navegador: no en la v0.1.
- **La segunda pestaña.** Sin la opción de esperar, abrir el store falla enseguida con `ClientError::InUse`, nuevo (precisa DC32; su código para JavaScript, en 4.6), sin tocar ningún archivo. Con la opción del store del navegador (en su constructor), espera en la cola del lock, y abre cuando la otra pestaña lo suelta: es la que usa el runtime del Worker, que mientras tanto atiende a su página a través de la pestaña que lo tiene, la líder (DC46). Nunca reintenta los archivos (INV-K13). El contrato de `MetadataStore::open` pasa a decir cómo informa un store tomado: con un error propio, que solo da el store (un `io::Error` que envuelve un tipo del cliente, reconocido con `downcast`; hoy es un `WouldBlock`, que otro paso podría dar), y `Client::open` lo pasa a `InUse`, también en nativo, cuando otro cliente, de este proceso o de otro, tiene `client.lock` (hoy, `Storage`). Para eso, la capa nativa da `None` solo con el error de un lock tomado, comparado por su código (`raw_os_error()`) con el de `fs2::lock_contended_error()` (`EWOULDBLOCK` en Unix, `ERROR_LOCK_VIOLATION` en Windows) y no por su `kind()`, y cualquier otro error al tomar el lock es un error, no `InUse`. En nativo no hay espera.
- **`pagehide`.** La página termina el Worker (en 4.6, su paquete; en 4.5, el arnés de pruebas). Terminarlo es un crash (INV-K11): lo aplicado es durable, y el navegador suelta los handles y el Web Lock. Una página que vuelve del *back/forward cache* arranca otro Worker. Si la que se va es la líder, la reemplaza la primera de la cola (DC46). El `shutdown` ordenado, con el último `Ack` de cada sala (DC28), es para cuando la aplicación cierra el cliente: en `pagehide` no hay tiempo de esperarlo. La página también termina el Worker ante su evento `error`, el de un pánico o de una reserva que falla (DC42).
- **El orden al terminar** (S5, pregunta 2): si el navegador suelta el Web Lock antes que los handles, la pestaña que lo toma puede encontrar `client.lock` todavía tomado; abrir falla entonces con `InUse`, y la aplicación vuelve a abrir, lo que toma el lock primero. Si S5 lo muestra, DC41 se revisa antes del Lote 10 (el requisito 8 de §1.3); también si una promesa del OPFS que el Worker dejó en vuelo (un `move()`, un `removeEntry`) puede terminar después de que otra pestaña tomó el directorio.
- **Alternativas descartadas:** reintentar el handle (INV-K13 lo prohíbe: compite con la otra pestaña archivo por archivo); `readwrite-unsafe` (solo Chrome, y rompe la exclusión); solo la primera pestaña con el SDK, y las demás con `InUse` (la primera versión de esta decisión: la segunda pestaña no podía ni leer, y abrir una aplicación en dos pestañas es lo común); un `SharedWorker` (no tiene `FileSystemSyncAccessHandle`, así que igual haría falta una pestaña que abra los archivos).

### DC42 · El runtime del Worker (DC14, DC18)
- **El despachador,** un módulo del cliente (`src/dispatch/`) que no depende del navegador: corre y se prueba también en nativo. Es API pública del cliente: la usa el punto de entrada del Worker, y el de 4.6 puede vivir en otro crate. Recibe un `Command` y da un `Reply`, y publica `Notice`s, todos datos serializables (DC14) sobre los tipos de la fachada (`Row`, `Key`, `Scan`, `RoomStatus`, `Freshness`, `ClientError`). Es el enum de comandos que DC18 dejaba para el runtime del Worker:
  - el cliente: abrirlo, con una configuración propia del despachador y serializable (lo de `ClientConfig` que elige la aplicación; las opciones del transporte, como `idle_timeout` y `max_streams`; y en el navegador, el directorio del OPFS, la URL del servidor y la opción de esperar la pestaña, del store de DC41), `client_id` y `shutdown`;
  - las salas: `room`, `close`, `discard`, `status`, `sync` y `set_freshness`, por el id de la sala;
  - cada comando lleva, además, el id de la pestaña que lo manda, para atender a varias (DC46); y hay un comando más, el aviso de que una pestaña se fue, que termina sus suscripciones y suelta sus salas;
  - las tablas: `get`, `scan`, `insert`, `update` y `delete`, por el id de la sala y el nombre de la tabla;
  - `watch`, `watch_scan` y `unwatch`, con un id de suscripción que da el despachador (DC14: nada que no se serializa cruza el límite; los handles y los streams quedan del lado del Worker);
  - la clave de una sala (DC43) y el token que pide el Worker (abajo).

  Cada comando es una llamada a la fachada de 4.4, no al núcleo: la fachada suma el dueño, el cifrado y las validaciones de DC32. Precisa la nota de DC18 en el plan de 4.2, que decía que el runtime traduce cada mensaje en una llamada a un método del núcleo.
- **Concurrencia.** Cada comando corre en una tarea propia (DC38) y su respuesta lleva su id: una escritura que espera su `CommitAck` (hasta el plazo de DC6) no frena las lecturas, y las respuestas salen en el orden en que terminan. Como en la API de Rust, dos comandos en vuelo no tienen orden entre sí. Cada escritura es una sola llamada a la fachada, que el despachador nunca repite (INV-K2, INV-Y4).
- **`watch`.** Por suscripción, una tarea lee su `ChangeStream` y publica cada valor como un `Notice`, y no publica el siguiente hasta que la página confirma el anterior: a lo sumo un valor en vuelo por suscripción, y el stream guarda el último (INV-K10, INV-K15). Termina con el stream, con `unwatch` o con el cliente.
- **El token.** El `TokenProvider` del navegador le pide el token a la página con un `Notice` y espera su respuesta (un comando con el id del pedido), con el plazo de DC30 (60 s): el backend que emite los tokens es de la aplicación.
- **El límite con la página.** Un punto de entrada en el Worker (`wasm-bindgen`) recibe cada mensaje, lo decodifica con un codec, lo despacha y publica las respuestas y los avisos con `postMessage`. El codec es intercambiable:
  - en 4.5, uno binario: los mismos tipos con `bincode`, en un `ArrayBuffer` transferible, exacto para todo `Value`, que usan las pruebas;
  - en 4.6, el que convierte a objetos de JavaScript con las representaciones de DC14 (INV-K14), sobre el mismo despachador.

  La clave de una sala (un `CryptoKey`, DC43) viaja al lado del mensaje, no adentro. Un mensaje que no decodifica tiene una respuesta de error si trae su id, y si no se ignora; nunca un pánico.
- **Un pánico.** En wasm32, un pánico es un *trap* que sale como una excepción de JavaScript: el Worker sigue vivo, con sus handles y su Web Lock, y la instancia queda inservible. Por eso el punto de entrada instala un *panic hook* que cierra el Worker (`close()`), y la página lo termina ante su evento `error` (DC41): queda como un crash (INV-K11), que suelta el lock (INV-K13). Una reserva que falla no pasa por el *panic hook* (en Rust estable aborta sin llamarlo: en wasm32, un `unreachable`), pero llega igual a la página como una excepción no atrapada, que dispara su evento `error`: en ese caso, solo la página lo termina.
- **El ciclo de vida.** El cliente se abre una vez, y su store toma el lock de DC41; `shutdown` cierra todo, como el de la fachada, y responde; después, todo comando es `RoomClosed`, como en la fachada después de `shutdown`.
- **Alternativas descartadas:** proxies de funciones entre la página y el Worker (como Comlink), otra dependencia de JavaScript que no fija qué cruza el límite; el codec de objetos de JavaScript ya en 4.5 (es INV-K14, de 4.6); los comandos de una sala en orden (una escritura de 30 s frenaría sus lecturas).

### DC43 · Las claves de cifrado en el Worker (precisa DC34; INV-K9)
- **`WebCryptoEngine`,** un `CryptoEngine` del cliente con la feature `wasm`: AES-GCM con una clave de 256 bits por sala, un `CryptoKey` de WebCrypto que da la aplicación desde la página, por *structured clone*, así que puede ser no extraíble y nunca entra a la memoria del wasm. Cada valor, con un nonce de 96 bits al azar (`crypto.getRandomValues`) y los datos adicionales de DC34 como `additionalData`: el supuesto de §2, un `CryptoEngine` que autentica los datos adicionales, vale por construcción.
- **Precisa DC34:** en el navegador, el `CryptoEngine` no es de la aplicación sino del SDK, con las claves de la aplicación, que sigue manejándolas (§2). Lo demás de DC34 (los datos adicionales, `NULL` cifrado, el texto descifrado validado) no cambia, y la fachada tampoco.
- **El texto cifrado:** `versión (1 byte, 1) || nonce (12 bytes) || cifrado con su tag (16 bytes)`, documentado en `ARCHITECTURE.md` para que una aplicación nativa pueda escribir y leer lo mismo con su propio `CryptoEngine`. Una versión que no conoce no descifra.
- **Una sala sin clave,** con la fachada de hoy: escribir una columna cifrada es `Crypto` (el `CryptoEngine` falló al cifrar, DC34), y leerla es `Decryption` (no descifra, como con otra clave), sin mandar nada. Una clave que no es AES-GCM de 256 bits, o que no permite cifrar y descifrar, se rechaza al darla (`Config`). La aplicación puede cambiar la clave de una sala: lo cifrado con otra da `Decryption` (la rotación es de la aplicación, §2).
- **El claro** de una columna cifrada vive en la memoria del Worker y sale de él solo hacia la aplicación (§2): su página, en la respuesta a una lectura o en un valor de `watch`, o el Worker de la pestaña que lo pidió, por el canal de esa pestaña (DC46; INV-K9).
- **Límite:** con nonces al azar, una clave no debería cifrar más de unos 2³² valores (el de AES-GCM); para una aplicación es lejano, y la rotación es suya.
- **Alternativas descartadas:** un `CryptoEngine` en JavaScript de la aplicación, cargado en el Worker (más flexible, pero necesita su propio canal para las claves; puede sumarse después de la v0.1); una vuelta a la página por cada valor (un `scan` de mil filas con una columna cifrada serían mil idas y vueltas); la clave en bytes dentro del wasm (extraíble, en la memoria del módulo); un error propio para la sala sin clave (cambia `CryptoError` y la fachada por una diferencia que `Decryption` ya cubre).

### DC44 · Las pruebas en el navegador
- **El arnés,** en `tools/browser/`, un workspace propio fuera del de la raíz como `tools/perf/` (con su `exclude`), con su `Cargo.lock` y su `package.json` (con las versiones fijas):
  1. un crate de pruebas compilado a wasm32 con `wasm-bindgen` (su CLI, de la misma versión que el crate, que es la misma en el lock del arnés y en el de la raíz; la CI instala también `wasm-bindgen-test-runner` de esa versión), cuyas funciones exportadas son los pasos de las pruebas y corren en un Worker dedicado;
  2. un servidor de pruebas en Rust: el router de `zemdb-server` (`build_router`, como la simulación), los archivos de la página y unas rutas de prueba (crear el esquema y la sala por el Control Plane, emitir tokens con `generate_client_token`, apagar el router para probar sin red, y respuestas armadas para el tope y los plazos del transporte), en un solo origen de `localhost` (un contexto seguro, sin CORS; habla HTTP/1.1);
  3. una página que corre los pasos en orden, guarda su avance en `sessionStorage` y se recarga sola entre pasos, abre una segunda página para las pruebas de pestañas y termina el Worker, y deja el resultado en la página.

  Playwright (Node) corre la página sin ventana en la CI; una persona la abre en cualquier navegador y lee el resultado, y un runner de macOS la corre en Safari (§1.2).
- **Los tests unitarios en wasm32.** Lo interno que una prueba de integración no alcanza (el handle muerto del OPFS, que pide hacer fallar una reapertura) va en tests unitarios del crate, `#[cfg(test)]`, con `wasm-bindgen-test` en un Worker dedicado (`run_in_dedicated_worker`), que corre `wasm-bindgen-test-runner` con el Chrome y el chromedriver del runner de la CI. `GEMINI.md` no deja hacer `pub` lo interno para probarlo.
- **El criterio de aceptación** (§4.5 del plan de la fase): una sala con filas que llegaron del servidor, la página recargada, y la sala que abre sin red desde sus metadatos y lee las mismas filas; después, con red, se pone al día. Además: el Worker terminado en medio de un lote, y la sala que reabre en un prefijo (INV-K1, INV-S3); dos pestañas (INV-K13); el cifrado (INV-K9); y todo eso por `postMessage`, a través del runtime del Worker.
- **La CI:** un job `browser` en `ubuntu-latest`, en cada push y pull request como los demás: compila el wasm y el servidor de pruebas, instala Chromium con Playwright y corre la página, corre los tests unitarios en wasm32, y pasa el fmt y el clippy del arnés. Firefox también, con Playwright, si S1 en Firefox sale bien. Safari, en un runner de `macos-latest` con `safaridriver`, antes de la revisión de cierre (§1.2, el paso S2); WebKit con Playwright en Linux, si hace falta, como un motor de más, que no reemplaza a Safari.
- **Lo que no depende del navegador** (el despachador, `rt` en nativo, DEF-189) se prueba en la suite de siempre.
- **Alternativas descartadas:** `wasm-bindgen-test` también para la integración (va por WebDriver: no recarga la página en medio de una prueba ni abre dos pestañas); un proxy en Node delante del servidor, en lugar del servidor de pruebas en Rust, que arma el servidor real en proceso como la simulación.

### DC45 · DEF-189: los metadatos, durables antes de servirse (INV-K16)
- **La regla.** `FileMetadata` hace durables los metadatos de una sala antes de servirlos (`room`) la primera vez desde que se abrió el store, y después de un guardado o un borrado que falló, en cualquier paso: así nada se escribe bajo un guardado que no es durable (INV-K16), también si después de la falla el store se cerró y se volvió a abrir sin un crash, lo que haría olvidarla. Si no puede, la lectura falla (un error, nunca metadatos que faltan, como hoy), así que la sala no abre.
- **En nativo:** sincroniza su directorio (al abrir el store, y antes de servir después de una falla). Vale con los metadatos en cualquier directorio, también fuera del directorio del motor, que §11 de `ARCHITECTURE.md` excluía sin que el código lo impidiera.
- **En el OPFS,** donde sincronizar el directorio no hace nada: un guardado que resolvió es durable por el supuesto de `move()` (DC39), y uno que falló pudo haber movido el archivo sin que sea durable (un `move()` rechazado con efecto incierto, que el supuesto no cubre; S5, pregunta 1). Por eso, antes de servir, vuelve a guardar lo que lee (temporal, `flush()` y `move()`), y repite un borrado que falló. Repetir un borrado cubre solo uno rechazado sin efecto: uno rechazado que borró (`remove_room` toma un `NotFound` como éxito) depende de la respuesta de S5 (1), como en DC39.
- **Cómo sabe cuál:** un método nuevo de `RoomFiles` (Lote 2, con su nota en DC17) dice si `sync_dir` hace durable un cambio de directorio que ya ocurrió: en nativo, sí (también en Windows, donde lo hace el journal de NTFS: el riesgo aceptado de §4 de `INVARIANTES.md`); en el OPFS, no. En nativo, eso supone que un `sync_dir` que sale bien hace durable lo que uno anterior que falló no hizo: en Linux, después de un EIO, no está garantizado (el riesgo aceptado de §4, en la fila del EIO en Linux). El Lote 5 prueba las dos ramas en nativo, con una capa de prueba que dice que no.
- **Alternativa descartada:** atar los metadatos al directorio del motor (que la API arme los dos juntos): cambia `ZemdbClient::open` y deja INV-K16 colgada de un efecto de la apertura del motor.

### DC46 · Las demás pestañas, a través de la líder (precisa DC41 y DC42; INV-K13)
- **El principio.** Cada pestaña arranca su Worker, y su página le habla siempre a él, igual en todas. El Worker que tiene el lock del directorio (DC41) es la *líder*: abre el cliente y atiende los comandos de todas las pestañas. Los demás son *seguidores*: no abren ningún archivo, le reenvían a la líder los comandos de su página por `BroadcastChannel`, con el codec binario de DC42, y le devuelven a su página las respuestas y los valores de `watch`. Solo la líder tiene la réplica en memoria y abre streams (el tope de DC40 es suyo).
- **Lo que garantiza:**
  - ninguna pestaña abre un archivo sin el lock del directorio (INV-K13);
  - una escritura o un `discard` de un seguidor se ejecuta a lo sumo una vez, en la líder a la que se mandó, y nunca después de que el seguidor lo dio por terminado. El seguidor da por terminado un comando en vuelo solo cuando la líder se fue, y eso lo dice el lock de la pestaña de ella, que se suelta cuando su Worker termina, y con él su cola. Entonces la escritura es incierta (`OutcomeUnknown`, el caso (e) de INV-K2);
  - cuando la líder se va, la primera de la cola del lock la reemplaza, y lo de cada pestaña (sus claves, sus salas y sus `watch`) pasa a la líder nueva: los `watch` siguen, sin quedar colgados ni repetir un valor (INV-K10);
  - un seguidor no le manda nada a una líder con otra versión del protocolo, otra persona u otro servidor: su página recibe `InUse` (INV-K8);
  - lo que la líder retiene por cada pestaña que atiende, y lo que retiene cada seguidor, está acotado (INV-K15).
- **Límites:** cada pestaña carga el wasm del SDK aunque no abra nada (unos 500 KB, S1); un comando de un seguidor suma un salto entre Workers (S5, pregunta 6); un relevo deja inciertas las escrituras en vuelo de los seguidores, como un crash las de la líder; el claro de una columna cifrada viaja entre los Workers de las pestañas, que son la aplicación (§2, INV-K9); con una líder congelada en segundo plano, las demás esperan (S5, pregunta 6).
- **El protocolo** (los canales, cómo se encuentra a la líder y cómo se sabe que se fue, el relevo, el token, las claves, los errores, las salas de varias pestañas) se diseña en el paso D (§2): solo documentos, con su propia revisión cruzada, después del paso S1 y antes del Lote 13, con los requisitos de §1.3, que salieron de las rondas séptima a novena de la revisión cruzada del Lote 0.
- **Alternativas descartadas:** la elección y el reenvío en la página, en el paquete de 4.6 (el lock lo tendría la página, y una página en el *back/forward cache* podría retenerlo; además, el relevo quedaría fuera del código auditado); una réplica por pestaña (cada pestaña sería otro dispositivo, con su propio almacenamiento y sus propias bajadas).

### 1.1. Lo que hereda

| # | De dónde | Qué | Cómo se resuelve | Lote |
|---|---|---|---|---|
| 1 | Plan de la fase, §4.5 | La feature `wasm` del cliente y su chequeo en la CI | DC38 | 7, 8 |
| 2 | §4.5 y §A.5 | El transporte del navegador (`fetch` y `EventSource` con `?token=`) | DC40: `fetch` también para el stream | 9 |
| 3 | §4.5, DC8 y DC17 | El motor de DC8, con los requisitos de la implementación del OPFS de §1 del plan de 4.1 | DC39 | 2, 3, 4 |
| 4 | §4.5, DC14 y DC18 | El runtime del Worker, y el enum de comandos que DC18 deja para él | DC42 | 6, 12 |
| 5 | §4.5 e INV-K9 | Cómo le llegan al Worker las claves o el `CryptoEngine` de la aplicación | DC43 (el paso desde la página es de 4.6) | 11, 12 |
| 6 | §4.5, INV-K13 e INV-K11 | Una sola pestaña abre cada sala (`navigator.locks`, S1), y el Worker suelta sus archivos en `pagehide` | DC41 y DC46: una pestaña líder por directorio, que atiende a las demás, y el Worker terminado en `pagehide` | 10, 13, 14, 15 |
| 7 | DC8 | S1 en Firefox | §1.2, paso S1 | — |
| 8 | DC17 | S3 en Safari y Firefox | §1.2, paso S1 | — |
| 9 | DC9 | La medición del codificador `ruzstd` en Safari | §1.2, paso S2 | — |
| 10 | Auditoría de 4.2 (DEF-189) e INV-K16 | En nativo, los metadatos fuera del directorio del motor; en el OPFS, un guardado durable antes de escribir bajo él | DC45, con el supuesto de DC39 y S5 en el OPFS | 5, 8 |
| 11 | Auditoría de 4.2 (DEF-195) | El tamaño máximo de una respuesta del transporte del navegador, y si 24 veces ese tamaño entra en la memoria del Worker | DC40: el tope de lo que lee, y en wasm32 los syncs del worker de a uno y sus transferencias de a una | 9 |
| 12 | Nota del Lote 6 de 4.4 | `ChangeStream` usa un `BoxStream`, que pide `Send` | DC38 | 8 |
| 13 | Cierre de 4.1 (DEF-184, DEF-185) | La capa de prueba del OPFS no rechaza un segundo handle, y el handle muerto no se ejercita en el motor | Con el motor sobre el OPFS real y la capa de prueba ajustada | 4 |
| 14 | §A.1 y §A.2 del plan de la fase | `transport/wasm.rs` con `gloo-net`, y la feature `wasm` con `gloo-net` | DC40 y DC38: `web-sys`, sin `gloo-net` | 8, 9 |
| 15 | Plan de la fase, §4.5 | El criterio de aceptación: la réplica sobrevive a recargar la página, en un navegador headless | DC44 | 15 |
| 16 | Plan de la fase, §7 | Las herramientas de JavaScript en la CI (npm, Playwright) | DC44 | 1 |

### 1.2. Spikes

`docs/METODOLOGIA.md` §3: el código va fuera del repo y no se mergea; la respuesta queda en la decisión, con lo medido y dónde.

- **S1 en Firefox** (DC8): las cinco preguntas de S1 (§4.0 del plan de la fase), y en Safari, cuándo da `InvalidStateError` al abrir un handle, además de cuando otro lo tiene (DC39). El criterio de DC8: si (1) no se cumple, DC8 y DC39 se revisan.
- **S3 en Safari y Firefox** (DC17): las cuatro preguntas de S3 (§1 del plan de 4.1; la 4, que un `FileSystemFileHandle` sigue valiendo después de su `move()`, es la que usa DC17 para renombrar un archivo abierto). Si un navegador no tiene `move()` de un archivo, o no reemplaza el destino, DC39 se revisa.
- **S5 (nuevo; DC39, DC40, DC41, DC45 y DC46):**
  1. Un temporal escrito con `flush()` y movido con `move()` sobre un archivo existente; un archivo y un directorio creados; un archivo borrado con `removeEntry`. Si se corta el navegador (`SIGKILL`) justo después de que la promesa resuelve, ¿al reabrir el perfil se ve el cambio, entero? Cortándolo en medio de una serie de esas operaciones, ¿se ven en el orden en que resolvieron, y un `move()` deja siempre el archivo viejo o el nuevo, nunca ninguno ni una mezcla? Un `move()`, una creación o un `removeEntry` que se rechazan, ¿pueden haber tenido efecto? (Los supuestos y la condición de DC39; DC45.)
  2. Al terminar un Worker que tiene handles y un Web Lock (`terminate()`, cerrar la pestaña, recargarla, y un pánico seguido de `close()`), ¿el navegador suelta los handles antes que el lock? Una pestaña que espera el lock, ¿puede encontrar un handle tomado cuando lo recibe? Una promesa del OPFS que el Worker dejó en vuelo (un `move()`, un `removeEntry`), ¿puede terminar después de que otra pestaña tomó el directorio? (DC41.)
  3. En el Worker, `navigator.locks.request` con `ifAvailable` y sin él: ¿la segunda pestaña recibe `null` enseguida, o espera hasta que la primera termina? (DC41.)
  4. Un `fetch` desde el Worker con el cuerpo leído de su `ReadableStream`: ¿llega bloque a bloque, sin que el navegador junte la respuesta? ¿Se corta con `AbortController` en medio del cuerpo? Si el Worker deja de leer, ¿el navegador deja de recibir? (DC40.)
  5. En una pestaña en segundo plano, ¿cuánto se demora un `setTimeout` de 30 s en el Worker dedicado, y cuánto tarda la página en contestarle un mensaje (un pedido de token)? (INV-K7, el heartbeat; DC46.)
  6. Entre los Workers de dos pestañas, por un `BroadcastChannel`: ¿llegan los mensajes de uno en orden, también un `CryptoKey` no extraíble, y cuánto tarda una ida y vuelta? Al cerrarse la pestaña que tiene el Web Lock, ¿cuánto tarda el primero de la cola en recibirlo? Una pestaña en segundo plano que tiene un Web Lock, ¿se congela (Chrome en Android, Safari en iOS), y mientras tanto la cola le da el lock a otra? ¿Un `AbortSignal` saca de la cola un pedido que espera? ¿Una página prerenderizada recibe sus Web Locks recién cuando se muestra? (DC46.)

  En Chromium, sin ventana, con Playwright y un perfil persistente que se corta con `SIGKILL`, sin la Mac; en Safari y Firefox, en la Mac.
- **La medición del codificador `ruzstd` en Safari** (DC9): el programa del Lote 6 de 4.1 (el `cdylib`, la página que mide y el generador nativo del payload), en Safari en un runner de `macos-latest`. El hardware no es el de DC9, así que se anota cuál es, y los tiempos no se comparan uno a uno. Si ese programa no está, se rearma con lo que describe DC9.

**Cuándo y dónde corren Safari y Firefox:**
- **Paso S1, después del Lote 0 y antes del Lote 2** (la capa del OPFS): S1 en Firefox, S3 en Safari y Firefox, y S5 en Safari y Firefox. Una respuesta negativa reabre DC8 o DC39, o cambia los errores que mapea la capa, así que va antes de escribirla. Puede ir en paralelo con el Lote 1 (el arnés), que no depende de ellos. S5 en Chromium va en el mismo paso.
- **Paso S2, después del Lote 15 y antes de la revisión de cierre:** la página de aceptación del arnés (DC44) en Safari y en Firefox, y la medición de `ruzstd` en Safari. Es la condición de DC8 para cerrar 4.5.
- **Dónde, sin la Mac** (el dueño del proyecto ya no tiene Safari, y Safari no corre en Linux):
  - Firefox, en Linux: con Playwright en este entorno, si se puede bajar, o en un runner de `ubuntu-latest`, que lo trae instalado.
  - Safari, en un runner de `macos-latest`, con `safaridriver`, desde un workflow en la rama de trabajo de la sesión, que no se mergea (el código de un spike no entra a `main`). En un runner no hay ventanas de nadie, así que también se puede cortar Safari (S1 (2), S5 (1)).
  - WebKit con Playwright en Linux es el mismo motor que Safari, pero no Safari: sirve de motor de más, no lo reemplaza.

### 1.3. Requisitos de diseño de DC46 (para el paso D)

Salieron de las rondas séptima a novena de la revisión cruzada del Lote 0, sobre versiones detalladas de DC46 que no llegaron a verde. El paso D los resuelve, con su revisión cruzada:
1. **Encontrar a la líder.** Una pestaña que se abre después del anuncio de la líder no lo recibe (un `BroadcastChannel` no repite), y dos pueden preguntar a la vez. La versión del protocolo y la configuración (la persona y el servidor) van en la pregunta, la respuesta y el anuncio, con un formato que no cambie entre versiones, para que una líder vieja conteste y su seguidor reciba `InUse` y no `Offline`.
2. **Saber que la líder se fue,** sin que un anuncio viejo engañe: el lock de la pestaña de ella, tomado antes de su primer mensaje, sin número de relevo. El `shutdown` de la líder tiene que soltarlo y no mandar nada después de avisar: el fin de los `watch` que produce cerrar el cliente va por otro canal, sin orden con el aviso. Recibir el lock del directorio termina el seguimiento de la anterior.
3. **Nada se ejecuta después de que su pestaña lo dio por terminado.** Los comandos que un seguidor manda quedan en la cola del Worker de la líder, y una líder ocupada en un trabajo de CPU largo (DC39) o congelada los ejecuta cuando vuelve a correr. Por eso el seguidor da por terminado un comando solo cuando la líder se fue (DC46); un *ping*, si hace falta, solo informa. Una líder congelada deja esperando a las demás: el límite, la condición sobre S5 (6) y la salida posible de terminar el Worker en el evento `freeze`.
4. **Lo que deja una pestaña que se va:** sus suscripciones, sus salas y los comandos suyos que la líder termina después (un `room` o un `watch` que se abre cuando la pestaña ya no está tiene que soltarse).
5. **El relevo:** qué devuelve cada comando en vuelo (una escritura y un `discard`, `OutcomeUnknown`; los que se pueden repetir); el orden de la restauración de cada pestaña (sus claves, sus salas, el modo, sus `watch`) y lo que se manda otra vez después; un `close` o un `unwatch` en vuelo; el modo de una sala, que no puede depender del orden en que restauran las pestañas; un `watch` que no queda colgado ni repite un valor (la confirmación de punta a punta, un valor que no se publica por igual al último, un error repetido); y la líder nueva, que restaura lo de su propia página.
6. **El token:** a qué página se lo pide la líder, con el `TokenProvider` de hoy, que no sabe qué pestaña lo pidió (la renovación es una por sala), y qué pasa si esa página está en segundo plano (S5, pregunta 5).
7. **Las claves:** una que llega tarde, con un `watch` en `Decryption` (DC35: un error no cambia el último valor emitido, así que volver a leer el mismo valor no emite nada); con código nuevo del núcleo para marcar las suscripciones de la sala, o volviendo a suscribir sin bajar el modo `live` (INV-K18). Claves distintas para una sala en pestañas distintas.
8. **La apertura en el relevo:** el handle de `client.lock` que todavía tiene el Worker que terminó (S5, pregunta 2), con un plazo, en el Lote 10 (que lo necesita para sus propios tests), y la excepción que eso sería en INV-K13 y DC41; qué pasa con el lock del directorio después de un error al abrir.
9. **Los errores:** `Offline` sin líder, frente a DC36 y a la fachada de hoy ("las lecturas nunca son `Offline`", y lleva una sala); `OutcomeUnknown` para un `discard`; `InUse` y el `shutdown` de un seguidor en el enunciado de INV-K2; y los archivos de 4.4 que cambian con eso.
10. **La memoria:** un canal por pestaña para lo que va a una sola (un `scan` grande no se copia en todas); lo que retiene un seguidor; los pedidos de lock que se cancelan con un `AbortSignal`.
11. **Lo que una pestaña le hace a las demás:** `close` por pestaña, que termina sus `watch` en esa sala; `discard` y `set_freshness`, del cliente (DC37, DC36).

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC38 a DC46 | Ajustes: §2, §4, S1 a S8, L7, W1 y W2 sobre el OPFS, y K1 a K13 y K15 a K17 en el navegador |✅ |
| S1 | Spikes: S1 en Firefox, S3 en Safari y Firefox, S5 | Fuera del repo | DC8, DC17, DC39 a DC41, DC45, DC46 | — | ⬜ |
| 1 | El arnés del navegador | `tools/browser` (pruebas) y CI | DC44 | — | ✅ |
| 2 | La capa del OPFS: el directorio | storage | DC39, DC38, DC45 | S1 a S8 y L7 sobre el OPFS | ⬜ |
| 3 | La capa del OPFS: los archivos | storage | DC39, DC44 | S1 a S8 y L7 sobre el OPFS | ⬜ |
| 4 | El motor en disco en wasm32 | storage | DC39, DC38 | S1 a S8, L7, W1 y W2 sobre el OPFS | ⬜ |
| 5 | Los metadatos, durables antes de servirse | client | DC45 | K16 | ⬜ |
| 6 | El despachador del runtime del Worker | client | DC42, DC46 | K2, K10, K15 | ⬜ |
| 7 | El runtime del cliente detrás de `rt` | client | DC38 | — (sin cambio de comportamiento) | ⬜ |
| 8 | El cliente en wasm32: la feature `wasm` | client | DC38, DC39, DC45 | K1 a K8, K10, K11 y K16 en el navegador | ⬜ |
| 9 | El transporte del navegador | client | DC40 | K7, K12, K15, K17 | ⬜ |
| 10 | Una pestaña por directorio | client y storage | DC41 | K13, K11, K17 | ⬜ |
| 11 | Las claves en el Worker | client | DC43 | K9 | ⬜ |
| 12 | El punto de entrada del Worker | client | DC42, DC43, DC41 | K9, K11, K12, K15 | ⬜ |
| D | El protocolo entre pestañas (el diseño de DC46) | Documentos | DC46 | K2, K8, K9, K10, K13, K15 | ⬜ |
| 13 | Las demás pestañas, a través de la líder | client | DC46 | K2, K8, K9, K10, K13, K15 | ⬜ |
| 14 | El relevo de la líder | client | DC46 | K2, K9, K10, K11, K13, K15 | ⬜ |
| 15 | La aceptación en el navegador | `tools/browser` (pruebas) | DC44, DC41, DC46 | K1, K4, K9, K11, K13 | ⬜ |
| S2 | Safari y Firefox, y `ruzstd` en Safari | Fuera del repo | DC8, DC9, DC44 | — | ⬜ |
| 16 | Revisión de cierre y cierre | Documentos (y lo que pida la revisión) | Todas | Las de §3.11 que toca 4.5, y S1 a S8, L7, W1 y W2 sobre el OPFS | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirman antes del Lote 1.
- **4.3 cerró** el 19 de Noviembre de 2026, así que el orden ya no la espera. Este plan se escribió con 4.3 abierta, y por eso agrupa así los lotes: los que no tocan el núcleo, el worker ni el transporte, el 1 (el arnés), el paso S1 (en paralelo con el 1), el 2 al 4 (el storage), el 5 (`src/metadata.rs`, que los Lotes 11b y 11c de 4.3 no tocan) y el 6 (un módulo nuevo del cliente, sobre la fachada, con su propia configuración: no toca `config.rs`, que cambia el Lote 11c).
- y los que sí: el 7 (`rt` cambia `client.rs`, `worker.rs`, `events.rs`, `metadata.rs` y `http_transport.rs`) y todo lo que depende de él.
- El 1 va antes del 2, que se prueba en el navegador; el paso S1, antes del 2; el 2 antes del 3, y el 3 antes del 4; el 2 antes del 5 (el método de la capa); el 4 y el 5 antes del 8 (los metadatos sobre el OPFS); el 7 antes del 8; el 8 antes del 9 al 12; el 10 y el 11 antes del 12 (el punto de entrada abre el store, que toma el lock, y recibe las claves); el 6 antes del 12; el paso D, después del paso S1 y antes del 13 (solo documentos); el 12 antes del 13, y el 13 antes del 14; el 9 al 14 antes del 15; el paso S2 después del 15, y el 16 al final.
- El 1 y el 15 son pruebas y herramientas, sin producción (salvo arreglos de lo que encuentren, que van como desvíos en su nota o como lotes nuevos).
- El 16 cierra con la revisión de cierre (§3); sus rupturas se arreglan en lotes nuevos de 4.5.

**Tamaño.** Unas 200 líneas de producción el Lote 2, 220 el 3 (la capa nativa, con el mismo contrato, tiene unas 300 sin comentarios), 120 el 4, 60 el 5, 280 el 6 (si pasa de 300, se parte: los comandos y el despachador; las suscripciones y el token), 120 el 7 (mecánico), 200 el 8, 280 el 9, 100 el 10, 150 el 11 y 180 el 12; las del 13 y el 14, las estima el paso D. El arnés del 1 y las pruebas del 15 no son producción.

**Chequeos de cierre de cada lote:** los de `docs/METODOLOGIA.md` §4 (regla 4), con el de wasm32 del cliente con `--no-default-features --features wasm` desde el Lote 8; las versiones largas en release (regla 5) de los crates tocados; desde el Lote 1, las pruebas del navegador (`tools/browser`) en Chromium y los tests unitarios en wasm32, con el fmt y el clippy del arnés, en los lotes que tocan algo que corre en el navegador; las mutaciones inversas en una copia del repo; y la revisión de delta (§5, paso 4).

**Pruebas.**
- **En el navegador** (DC44): lo que depende del OPFS, de `fetch`, de los Web Locks, de WebCrypto, de `postMessage` y del ciclo de vida de la página, en el arnés, en Chromium en la CI; lo interno que el arnés no alcanza, en tests unitarios en wasm32. Cada lote suma sus pasos. Lo que depende del navegador (los errores de cada uno, `move()`, los locks) tiene también su caso en Safari y Firefox en el paso S2.
- **En nativo:** el despachador (Lote 6), con la fachada sobre el servidor real en proceso y el transporte de prueba de `crates/client/tests/common/`; DEF-189 (Lote 5), con la capa de archivos que hace fallar el `sync_dir` (los tests de la auditoría de 4.2); y en los Lotes 4, 7 y 8, la suite entera, con la simulación, sin tocar sus expectativas: no cambian ningún comportamiento en nativo (`InUse` en nativo es del Lote 10).
- **La capa del OPFS:** las preguntas de S3 y los requisitos de DC17, como pasos del arnés sobre la capa real; la capa de prueba nativa (`opfs_like.rs`) se ajusta a lo que el OPFS real mostró (DEF-184).

**Checklist propio.** Los Lotes 2 a 14 recorren este checklist, y además: el de crash-safety de 4.1 (§2 de su [plan](2026-10-fase4_1-storage-servidor.md)), en los Lotes 2 a 4; el checklist del cliente de 4.2 (§2 de su [plan](2026-10-fase4_2-nucleo-replica.md)), para lo que cambie del núcleo; y el de 4.4 (§2 de su [plan](2026-11-fase4_4-api-rust.md)), para lo que use la fachada.
- En nativo, nada cambia de comportamiento: la suite entera y la simulación con el reloj pausado pasan sin tocar sus expectativas (Lotes 4, 7 y 8).
- Fuera de `rt` y de `sys`, nada de lo que en wasm32 entra en pánico o no corre (DC38, con su lint).
- Ningún pánico alcanzable en wasm32 desde una respuesta, un evento o un archivo (INV-K12), ni desde un mensaje de la página (DC42): deja la instancia inservible.
- Ninguna reserva por lo que dice una respuesta, un archivo o un mensaje antes de verificarlo contra su tope (INV-K12, INV-W1).
- Toda operación del OPFS que es una promesa termina aunque se suelte su future, y retiene su handle hasta entonces (INV-L7, DC39).
- Nada que no se serializa cruza el límite del Worker, salvo el `CryptoKey` de una sala (DC42, DC43).
- El claro de una columna cifrada no sale del Worker salvo hacia la aplicación: su página, o el Worker de la pestaña que lo pidió, por el canal de esa pestaña (INV-K9, DC46).
- Ningún archivo del directorio se toca antes de tener el lock de la pestaña (INV-K13).
- Una escritura o un `discard` de una pestaña se ejecuta a lo sumo una vez, en la líder a la que se mandó, y nunca después de que su pestaña lo dio por terminado (DC46, INV-K2).

### Lote 0 · Contrato · ✅ Hecho (23 de Noviembre de 2026)
**Objetivo:** el contrato contra el que se revisa cada lote de 4.5. Solo documentos.

**Qué se hace:**
- **Este plan,** con DC38 a DC46 (DC40 y DC43, adoptadas), lo que hereda (§1.1), los spikes (§1.2) y la revisión de cierre (§3).
- **`INVARIANTES.md`, versión 48** (la 46 y la 47 las tomaron los Lotes 11b y 11c de 4.3, en paralelo):
  - **§2:** en el disco local, los supuestos del OPFS de DC39 (`move()`, y crear y borrar entradas, durables cuando resuelven; S5); en la aplicación, la página y el Worker, los comandos de la página (DC42), los tokens que le pide el Worker y las claves como `CryptoKey` para el `CryptoEngine` del SDK sobre WebCrypto (DC43); el navegador, en lo que el SDK le pide (los Web Locks, su cola y los handles exclusivos, DC41, y el `BroadcastChannel`, DC46); las demás pestañas del origen, que también son la aplicación (DC46); en el reloj, los timers del navegador, que pueden demorarse en segundo plano.
  - **§3.2, INV-L7, W1 y W2:** las invariantes del storage valen igual para el motor en disco sobre el OPFS (DC39), con los supuestos de §2, 🆕 (Lotes 2 a 4); en INV-L7, las operaciones del OPFS que son promesas.
  - **§3.11:** su introducción, que el mismo núcleo corre en el navegador (DC38); INV-K2, el despachador que no repite una escritura, la escritura de otra pestaña, que se ejecuta a lo sumo una vez en la líder, y el caso (e) de una escritura incierta cuando la líder se va (Lotes 6, 13 y 14); INV-K7, los plazos del transporte del navegador, el tope de sus streams, los timers en segundo plano, el sync que espera al de otra sala y la subida del designado que espera a una bajada (Lotes 8 y 9); INV-K9, el `CryptoEngine` del SDK sobre WebCrypto, y el claro que sale solo hacia la aplicación: su página, o la pestaña que lo pidió (Lotes 11 a 15); INV-K8, el seguidor que no le reenvía nada a una líder de otra persona (Lote 13); INV-K10, `watch` a través del runtime del Worker y en las demás pestañas, con el relevo (Lotes 6, 13 y 14); INV-K11, el Worker que la página termina en `pagehide` y ante un pánico, y el relevo de la líder (Lotes 10, 12, 14 y 15); INV-K12, el transporte del navegador con su tope (DEF-195) y el pánico que cierra el Worker (Lotes 9 y 12); INV-K13, el título y el enunciado por directorio, `InUse` y la espera, las demás pestañas a través de la líder, y lo que la sostiene (DC41, DC46, Lotes 10 y 13 a 15); INV-K15, el runtime del Worker (un valor en vuelo por suscripción, lo de cada pestaña que atiende la líder y lo que retiene un seguidor), el stream por `fetch` y, en wasm32, los syncs de a uno y las transferencias de a una (Lotes 6, 9 y 12 a 14); INV-K16, DC45 (Lotes 5 y 8); INV-K17, el transporte del navegador que distingue las negativas y niega un stream de más con una que se reintenta, e `InUse`, que no es de una sala (Lotes 9 y 10).
  - **§4:** lo que el cliente decodifica e instala a la vez en el Worker (DC40); el riesgo de un EIO en Linux, también en el `sync_dir` de un directorio (DC45); y el token en la query de `/events`, que el SDK del navegador no usa.
  - El resumen, con el lote de 4.5 que implementa cada parte, también en K1, K3 a K6 y K8; §1.6, con las decisiones hasta DC46.
- **El plan de la fase:** DC38 a DC46 en la tabla de decisiones (DC40 y DC43, adoptadas); las notas en DC8 (la condición de S1 en Firefox y la coordinación entre pestañas, DC39, DC41 y DC46), DC14 y DC18 (el enum de comandos, DC42), DC17 (la capa del OPFS, el contrato de `sync_dir` y el método nuevo, DC39 y DC45), DC27 (el stream del navegador por `fetch`, DC40), DC28 (los syncs y las transferencias del worker en wasm32, DC40), DC32 (`InUse`, DC41) y DC34 (el `CryptoEngine` del navegador, DC43); 4.5 en curso, con el enlace a este plan (encabezado, §4, §4.5 y §8); en §4.5, el stream por `fetch` y la pestaña por directorio; en §A.1, §A.2 y §A.5, `gloo-net` y `EventSource`, con DC38 y DC40.
- **`ROADMAP.md`:** 4.5 en curso, con el enlace a este plan, y su fecha.
- `ARCHITECTURE.md` no cambia: describe comportamiento implementado. Desde el Lote 2, cada lote suma lo suyo (§8 para el motor del navegador, §11 para el cliente en el navegador, §3.1 para el formato del texto cifrado de DC43).

**Entrega:** `docs: plan Fase 4.5 with its batches and the browser decisions, and adjust the client contract for it (Fase 4.5, Lote 0)`.

**Avance de la revisión cruzada.**
1. **Primera ronda: 4 bloqueantes, 14 ajustes y unos 20 menores, corregidos.**
   - **Los bloqueantes:**
     - los supuestos del OPFS no alcanzaban: en el OPFS no hay `sync_dir`, y el motor lo usa también para las entradas que crea (el WAL y el `.lock`) y para los borrados de `delete_room` (INV-S8); ahora §2 y DC39 suponen durables las entradas creadas y borradas cuya promesa resolvió, en orden, y S5 lo prueba;
     - con HTTP/1.1, seis salas con un `watch` dejaban a los pedidos sin conexión (INV-K7), y el arnés habla HTTP/1.1: ahora DC40 tiene un tope de streams, con una negativa que el worker reintenta;
     - DC43 contradecía DC34 sin decirlo (el `CryptoEngine` era "de la aplicación", y la fachada da `Decryption`, no `Crypto`, al leer una sala sin clave): ahora DC43 precisa DC34, sin tocar la fachada;
     - el Lote 2 usaba la tarea de `sys`, que traía el 3, y `files` no compila en wasm32: ahora el 2 trae la tarea y `files` en wasm32.
   - **Los ajustes:** los plazos de DC40 (un solo plazo hasta los headers, porque `fetch` no deja ver la conexión); la otra mitad de DEF-195 (lo que reserva una respuesta en la memoria del Worker); `uuid` en wasm32; los bindings que faltan en `web-sys` (`move()` y los Web Locks); un pánico en wasm32, que no termina el Worker; DC45, que olvidaba una falla al reabrir el store y no cubría un `move()` rechazado; el lint, solo en wasm32; la configuración propia del despachador; cómo se prueba el handle muerto (tests unitarios en wasm32); el claro en los valores de `watch`; el tamaño del lote de la capa del OPFS, que se parte en dos (directorio; archivos); las marcas del resumen en L7, W1, W2, K1, K3 a K6 y K8; por qué no hace falta una auditoría amplia (§0); y las notas del plan de la fase (la pestaña por directorio en §4.5, y la de `EventSource` en DC27 en lugar de DC8).
   - **Los menores:** `zstd_level` en `disk/mod.rs`; `CryptoConcurrencyBounds`; Firefox todavía sin S1; las listas de invariantes de los lotes; el `exclude` del arnés; los `Send` y los `JoinHandle` del cliente en el Lote 7, y tests propios de `rt`; el título de INV-K13 y el nombre del lock; los modos de Chrome; `createSyncAccessHandle` en INV-L7; los mensajes de la página, fuera de INV-K12; el riesgo del token en la query; un timer que se dispara antes; `block_on`; el corte del navegador, que no es uno de energía; `InvalidStateError` en Safari; `XMLHttpRequest` como alternativa; la conexión HTTP/2 que deja de contestar; `InUse` también en nativo; el nombre del despachador (`dispatch`); el estilo; la fecha de `ROADMAP.md`.
2. **Segunda ronda, acotada a las correcciones de la primera: sin bloqueantes, 8 ajustes y unos 20 menores, corregidos.** La renumeración de los lotes quedó consistente.
   - **Los ajustes:** los directorios creados y el borrado de los metadatos en el supuesto del OPFS; cómo sabe `FileMetadata` si `sync_dir` hace algo (un método de `RoomFiles`, DC45); una reserva que falla no pasa por el *panic hook*, y la que cierra el Worker es la página; el tope de streams es por cliente, no por origen, y qué muestra `status` de una sala que espera lugar; las transferencias de varias salas a la vez, que ahora también van de a una en wasm32, y lo que queda de la aplicación; los tests unitarios del storage en wasm32, que hoy no compilan (el Lote 3 los prepara); quién toma el Web Lock (`FileMetadata::open`, en wasm32) y cómo se informa un store tomado; y una promesa del OPFS que un Worker terminado deja en vuelo (S5, pregunta 2).
   - **Los menores:** las listas de invariantes de los Lotes 8 y 12 y del resumen de K2, K10 y K11; el lint del storage en el Lote 4; el `cfg` de `native` en `files/mod.rs`; un stream abierto sin plazo; la configuración del despachador, con las opciones del transporte; "otro cliente" en lugar de "otro proceso"; la redacción de §2, de DC45 y de la fila de §4 ("nada confirmado", y cuándo se repite el crash); la cita de DC39 en INV-K16; el módulo de los *fail points* en wasm32; la frase de 4.5 en `ROADMAP.md` (y sus dos espacios al final de la fecha, el salto de línea de Markdown, que ya estaban y que `git diff --check` marca); §A.5 del plan de la fase; el resumen de B4; el estilo; el permiso del worker en `client.rs` y su nota en DC28; la cita del checklist; y el despachador como API pública, por sus tests de integración.
3. **Tercera ronda, acotada a las correcciones de la segunda: sin bloqueantes, 3 ajustes y 16 menores, corregidos.**
   - **Los ajustes:** el contrato de `sync_dir` frente al método nuevo, y lo que hace el motor sobre el OPFS con un cambio que deja una operación que falló (ahora, una condición de DC39 con S5); lo que muestra `status` de una sala sin lugar para su stream (el error del worker se borra con el paso siguiente que sale bien, y no lleva el mensaje del frame: queda lo estable); y dos textos que decían que el punto de entrada toma el Web Lock, que toma el store.
   - **Los menores:** DC45 y `TestFiles` en el Lote 2; las columnas de DC41 y DC44; el 2 antes del 5; cómo se fuerza el handle muerto en el Lote 3; el lock que se suelta con el store; `None` de la capa nativa solo con el lock tomado, y el test que pasa a `InUse`; las notas de DC32 y DC41 en el plan de la fase; la subida del designado que espera a una bajada de otra sala; "de a uno" y "de a una", con permisos separados; las listas del Lote 0 y de §1.1; el Lote 13 en el resumen de K9 y K13; la promesa en vuelo de un Worker terminado en DC41; INV-K16 y DC45 en los términos del método, con el supuesto de un `sync_dir` después de uno que falló; el estilo; y la versión de `wasm-bindgen` en los dos locks.
4. **Cuarta ronda, acotada a las correcciones de la tercera: sin bloqueantes, 2 ajustes y 11 menores, corregidos.**
   - **Los ajustes:** la condición de DC39 dependía de una pregunta que S5 (1) no hacía, y le faltaba el borrado (un `removeEntry` rechazado con efecto, en `delete_room` y en los metadatos): ahora S5 (1) pregunta por los tres, DC39 los nombra y DC45 dice qué cubre repetir un borrado; y el lock tomado de la capa nativa no es `WouldBlock` en Windows (`fs2` da `ERROR_LOCK_VIOLATION`): ahora es el error de `fs2::lock_contended_error()`.
   - **Los menores:** "hasta que se suelta el store"; el Lote 10 toca también el storage, con el `RoomLocked` del motor y `opfs_like.rs` en sus interacciones; la fila de §4 del EIO en Linux, también para el `sync_dir` de un directorio; el objetivo del Lote 9; el store de DC41 en el Lote 12; el Lote 13 en K9 y K13; la subida del designado en la lista de INV-K7; "solo se garantizan durables"; "de nuevo al mismo si no hay otro"; la capa de prueba del Lote 5; y la nota de DC17, con el contrato de `sync_dir`.
5. **Quinta ronda, acotada a las correcciones de la cuarta: sin bloqueantes, 1 ajuste y 10 menores, corregidos.**
   - **El ajuste:** el ejemplo de la condición de DC39 era falso: al reintentar `delete_room`, `lock_room_files` vuelve a crear el WAL activo y `delete_room` lo borra otra vez; los que da por borrados con un `NotFound` son el `.snap`, `.wal.compacting` y la copia de una absorción (los temporales, que ya no lista, no los reintenta). Ahora el ejemplo es el `.snap` y `.wal.compacting`.
   - **Los menores:** el `removeEntry` en las interacciones del Lote 4; el encabezado y las interacciones del Lote 10 (`create_locked` en `compactor.rs`), y su test negativo; el error del lock tomado, comparado por su código; la salvedad de DC45 en INV-K16; la fila del EIO en Linux, con los metadatos en su mitigación; la nota de DC17; la lista del Lote 0; y los paréntesis de la tabla de decisiones.
6. **Sexta ronda, acotada a las correcciones de la quinta: sin bloqueantes, 1 ajuste y 8 menores, corregidos; verde con el ajuste corregido.**
   - **El ajuste:** el test negativo del Lote 10 no probaba el criterio: una capa de prueba que envuelve a la nativa devuelve su error antes de `try_lock_exclusive`, y el código de hoy lo pasaba igual. Ahora se parte en dos: un *fail point* en la toma del lock de la capa nativa, con su test en el storage, y el de `Client::open` con `TestFiles`.
   - **Los menores:** quién borra el WAL al reintentar; lo que reaparece es el archivo, y `.wal.compacting` rompe la otra cláusula de INV-S8; los temporales, que no se listan otra vez; el snapshot temporal que `create_locked` deja al fallar al tomar su lock; la fila del EIO en Linux, con las invariantes del motor y la razón de aceptarlo; DC28 en §0 y en la lista del Lote 0; un error propio del store tomado, en lugar de `WouldBlock`; y el estilo de las tablas de decisiones.
7. **Después de la sexta ronda,** el dueño del proyecto eligió la pestaña líder para DC41: con la primera versión, la segunda pestaña no podía ni leer. De ahí DC46 y los Lotes 13 (las demás pestañas, a través de la líder) y 14 (el relevo); la aceptación pasó al Lote 15 y el cierre al 16. Las notas de las rondas anteriores usan la numeración de entonces. La séptima ronda de la revisión cruzada está acotada a ese cambio.
8. **Séptima ronda, acotada a la pestaña líder: 1 bloqueante, 7 ajustes y 14 menores, corregidos.**
   - **El bloqueante:** DC46 no decía cómo un seguidor encuentra a la líder (una pestaña que abre después del anuncio no lo recibe: un `BroadcastChannel` no repite), ni cómo sabe que se fue, ni de dónde salía el número de relevo. Ahora un seguidor pregunta quién es la líder hasta que contesta, y la sigue mientras el lock de la pestaña de ella está tomado: así se entera de que se fue, y un anuncio viejo no lo engaña, sin número de relevo.
   - **Los ajustes:** el relevo completo (las claves, las salas con su modo y los `watch`, en ese orden, también para la página de la líder nueva; una apertura que falla; el `shutdown` de la líder como una partida; un primer valor igual al último, que no se publica); el caso (e) de INV-K2, lo que devuelve cada comando en un relevo y `Offline` sin líder; el claro entre los Workers de las pestañas (INV-K9); lo que una pestaña le hace a las demás (`close` por pestaña, `discard` y el modo del cliente); otra versión u otra configuración en otra pestaña (`InUse`); la memoria (un canal por pestaña, la confirmación de punta a punta, lo que retiene un seguidor); y una líder que no corre (un plazo, y una pregunta más en S5 (6)).
   - **Los menores:** las notas de DC40 y DC43 en el plan de la fase, que ya están adoptadas; la nota de DC8; §1.6 hasta DC46; lo nuevo de §2 en los resúmenes; las listas de los lotes; lo que entrega un `BroadcastChannel` y la cola de los Web Locks, en §2; el `AbortSignal` para dejar la cola; el lock de cada pestaña antes de su primer mensaje y sus ids al azar; los ids de la página, que no cambian con un relevo; las claves de varias pestañas; lo de DC46 en *Cómo intentar romperla*; el estilo; y las fechas.
9. **Octava ronda, acotada a las correcciones de la séptima: 1 bloqueante, 7 ajustes y 15 menores, corregidos; DC46 se reescribió entera.**
   - **El bloqueante:** un `watch` quedaba colgado después de un relevo sin cambios: el seguidor no publicaba un valor igual al último, su página no confirmaba, y la líder no publicaba el siguiente. Ahora un valor que no se publica se confirma enseguida.
   - **Los ajustes:** el `shutdown` de la líder suelta el lock de su pestaña y deja de contestar, y recibir el lock del directorio termina el seguimiento de la anterior; un *ping* en lugar de un plazo por comando, que cortaba comandos largos de una líder viva, y el token pedido a la página de la pestaña que mandó el comando; lo que se manda otra vez, después de restaurar la pestaña, y una clave nueva que vuelve a leer los `watch` de su sala; el reintento del handle de `client.lock`, con el Web Lock tomado, como única excepción de INV-K13; `Offline` con la sala opcional y `OutcomeUnknown` para un `discard`; una líder nueva incompatible después de un relevo; y "a lo sumo una vez" para las escrituras y los `discard`.
   - **Los menores:** `close` por pestaña y sus `watch`; el modo que eligió cada pestaña; cuándo se anuncia la líder; el token; el canal de la pestaña antes de preguntar; INV-K15 y §2; los límites (la líder en segundo plano, una página prerenderizada); el `shutdown` de un seguidor; el estado de la revisión; las notas del plan de la fase y del Lote 0; INVARIANTES; §3; los archivos y los tests de los Lotes 13 y 14; la redacción.
10. **Novena ronda, acotada a las correcciones de la octava: 1 bloqueante y 8 ajustes.** El bloqueante: los comandos que un seguidor da por terminados cuando la líder no contesta el *ping* siguen en la cola del Worker de ella, que los ejecuta al volver a correr (una escritura incierta saldría después). Los ajustes: lo que el *ping* no separa; la partida de la líder contra el fin de los `watch`, por canales sin orden; el reintento de `client.lock`, sin plazo y en el lote equivocado; el enunciado de INV-K2 (`InUse`, el `shutdown` de un seguidor); el token con el `TokenProvider` de hoy; la clave nueva que vuelve a leer los `watch`, que necesita código del núcleo; `Offline` frente a DC36; el modo de una sala después de un relevo.

   Como cada ronda sobre DC46 encontraba casos nuevos de un protocolo entre pestañas, el dueño del proyecto eligió el 23 de Noviembre de 2026 cerrar el Lote 0 con DC46 en su principio (lo que garantiza y su mecanismo básico) y diseñar el protocolo en el paso D, solo de documentos, con su propia revisión cruzada, después del paso S1 (S5 contesta lo que más lo condiciona), con los hallazgos de las rondas séptima a novena como sus requisitos (§1.3). Con eso, DC46, `INVARIANTES.md` y los Lotes 13 y 14 se redujeron al principio; el reintento de `client.lock`, `Offline` sin líder, el *ping* y el resto del detalle salieron del contrato y quedaron como requisitos.

### Lote 1 · El arnés del navegador (`tools/browser`, pruebas, y la CI) · ✅ Hecho (23 de Noviembre de 2026)
**Objetivo:** poder probar en un navegador headless, en la CI, lo que hagan los lotes siguientes, con recargas, dos pestañas y el Worker terminado, y correr tests unitarios en wasm32 (DC44).

**Archivos:** `tools/browser/` (su `Cargo.toml`, con su `[workspace]`, el crate de pruebas en wasm32, el servidor de pruebas, la página y su script, el runner de Playwright, `package.json` y su lock, y un `README.md`); el `exclude` del `Cargo.toml` de la raíz; `.github/workflows/ci.yml` (el job `browser`).

**Qué se hace:** las piezas de DC44; el runner, que levanta el servidor de pruebas en un puerto libre, abre la página en Chromium sin ventana y falla si algún paso falla, con su salida; la configuración de `wasm-bindgen-test-runner` para los tests unitarios en wasm32; y el job de la CI, con las versiones fijas de `wasm-bindgen` y de Playwright. Los pasos de este lote prueban el arnés: un paso que guarda algo en el OPFS y otro, después de recargar, que lo lee; una segunda página del mismo contexto; un Worker terminado. Se escriben con `web-sys`, sin el storage, que llega en el Lote 2.

**Tests exigidos:**
- Un paso que falla hace fallar la corrida, con el nombre del paso: el runner, contra un paso que falla a propósito, falla (y el paso se saca).
- Lo guardado en el OPFS antes de una recarga se lee después; una página nueva de otro contexto no lo ve.
- La segunda página corre en el mismo contexto (el mismo OPFS); terminar el Worker de la primera suelta sus handles.
- Un test unitario en wasm32 de prueba, en el crate de pruebas del arnés, corre en un Worker dedicado, y uno que falla hace fallar el job.

**Interacciones a revisar:** el servidor de pruebas arma el servidor real como la simulación (`build_router` sobre un `data_dir` temporal) y no deja tareas sueltas al terminar; la CI de siempre no cambia (el arnés está fuera del workspace); el tiempo del job.

**Documentos:** `tools/browser/README.md`; el plan.

**Entrega:** `test(client): the harness of the tests in a browser (Fase 4.5, Lote 1)`.

**Hecho** (23 de Noviembre de 2026).
- **Qué entró.**
  - **El workspace del arnés** (`tools/browser/`), fuera del de la raíz (su `exclude`).
    - Su `Cargo.lock` tiene `wasm-bindgen` 0.2.129, como el de la raíz. `build.sh` compara la versión de los dos locks con la del CLI.
    - Su `package.json` fija Playwright 1.63.0, con su lock.
  - **El crate de pasos** (`steps/`), en wasm32 con `wasm-bindgen`.
    - Cada función exportada es un paso: recibe el id de la corrida y falla con un error que dice qué falló.
    - Un pánico hace fallar su paso, con el mensaje del pánico.
    - Sus tests unitarios (`src/tests/`) corren en un Worker dedicado, con `wasm-bindgen-test-runner`.
  - **El servidor de pruebas** (`server/`). Sirve un solo origen en `127.0.0.1`, con HTTP/1.1. Tiene:
    - el router de `zemdb-server` sobre un directorio temporal, armado como en la simulación;
    - los archivos de la página;
    - las rutas del arnés: el Control Plane con el secreto de admin, los tokens y el corte de la red. Con la red cortada, un pedido al router cierra su conexión sin respuesta, y una respuesta en curso (un stream de eventos) se corta.

    Se detiene cuando se cierra su entrada (es decir, cuando termina el runner que lo arrancó) o con Ctrl+C: cierra las conexiones, apaga las salas y borra su directorio. Sale con error si una conexión entró en pánico.
  - **La página** (`www/`). Corre en orden las acciones de las suites de su URL, que pueden ser:
    - un paso en su Worker;
    - recargar la página;
    - un paso en una segunda página del mismo contexto;
    - terminar su Worker;
    - cerrar la segunda página.

    Guarda su avance en `sessionStorage` y deja el resultado en la página. Cada carga de una página nombra a sus Workers con un id al azar: con él, un paso sabe si corre en una página que se recargó o en la segunda.
  - **El runner** (`run.mjs`).
    - Levanta el servidor en un puerto libre, corre las suites por defecto en Chromium sin ventana, y después la suite `isolation` en otro contexto, con el mismo id.
    - Falla si:
      - un paso falla (con su nombre);
      - un paso corrió en la segunda página sin que el contexto abriera una;
      - la página tomó otro id que el de su URL;
      - el servidor no se detiene a tiempo y con éxito.
    - `--self-check` prueba el propio runner.
  - **El job `browser` de la CI**, en `ubuntu-latest`. Corre:
    - el fmt y el clippy del arnés y los tests del servidor de pruebas;
    - el build, y Chromium con Playwright;
    - el runner y su self-check;
    - los tests unitarios en wasm32, con el chromedriver del runner.
- **Tests.**
  - **Las suites:**
    - `reload`: lo escrito en el OPFS antes de recargar se lee después, en otra carga de la página;
    - `isolation`: otro contexto no lo ve;
    - `tabs`: la segunda página ve el mismo OPFS y no puede abrir el archivo que tiene la primera; terminado el Worker de la primera, lo abre;
    - `server`:
      - un esquema y una sala, creados por el Control Plane;
      - el registro de un cliente con un token del arnés;
      - el corte de la red: un stream abierto se corta y un pedido falla como sin red; con la red de vuelta, el router contesta.
  - **El self-check:** las suites `fails`, `fails_in_peer` y `panics`, cuyos pasos fallan a propósito (en la primera página y en la segunda) o entran en pánico, hacen fallar la corrida con el nombre del paso y su error.
  - **Un test unitario en wasm32,** en un Worker dedicado con el OPFS. Que uno que falla haga fallar `cargo test`, con código 1, se comprobó una vez, y ese test se sacó.
  - **Dos tests nativos de `page_file`:** ninguna ruta sale de `www/`.
- **Mutaciones inversas: las 18 quedan muertas, salvo una equivalente.**
  - **La primera tanda, 10.** Mueren estas:
    - el corte que termina limpio una respuesta;
    - las respuestas sin corte;
    - el servidor que no se detiene;
    - `terminate`, que no termina el Worker;
    - los pasos de la segunda página, en el Worker de la primera;
    - la página que no se recarga;
    - la aislación en el mismo contexto;
    - el runner que da por buena una página que falla;
    - el Worker que da todo por bueno.

    La equivalente es el chequeo de la red al empezar un pedido al router: el `select!` que espera el corte también lo ve.
  - **Las que sobrevivieron a la revisión de delta, 5, y ahora mueren:**
    - la recarga cambiada por terminar el Worker;
    - un Worker de la misma página como segunda página;
    - la segunda página que da todo por bueno;
    - la página que no toma el id de su URL, en un contexto compartido;
    - el servidor que sirve cualquier ruta.
  - **Las de las correcciones, 3:**
    - un pánico en una ruta del arnés;
    - el Worker sin el listener de sus errores;
    - de la segunda ronda, otro Worker de la misma página como segunda página, con un nombre nuevo, que ahora el runner rechaza.
- **Interacciones revisadas.**
  - **El servidor se arma como la simulación,** con `build_router` sobre un directorio temporal. Lo sirve un loop propio, sin lo que suma `serve_until_shutdown`:
    - los límites de las conexiones;
    - los plazos de los headers y de una respuesta trabada;
    - h2c;
    - la reserva de una respuesta hasta el fin de su cuerpo;
    - el barrido de los snapshots vencidos.

    El README lo lista.
  - **No deja tareas sueltas.**
    - Las conexiones van en un `JoinSet`, que se drena con un plazo de 5 s y después se aborta.
    - Después vienen `shutdown_all` y el drop del runtime, que espera las tareas bloqueantes.
    - El runner verifica que el servidor salga en menos de 15 s y con código 0. La revisión midió 56 ms, con un stream abierto.
  - **La CI de siempre no cambia:** el job es nuevo, el lock de la raíz no cambia y `cargo metadata` de la raíz lista los mismos cuatro crates.
  - **El tiempo del job:** se ve en la CI.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia con sus propias mutaciones).
  - **Primera ronda: ningún Alto, 4 Medios y 10 Bajos.**
    - **Los Medios, corregidos:**
      - una recarga cambiada por terminar el Worker pasaba: ahora `reload_read` compara la carga de la página;
      - una segunda página que fuera otro Worker de la misma página pasaba: ahora `tabs_taken` compara la página que tiene el archivo;
      - el self-check no cubría un paso que falla en la segunda página: ahora corre `fails_in_peer`;
      - un pánico perdía su mensaje: un paso `async` que entra en pánico no rechaza su promesa, y su trap es un error del Worker. Ahora el Worker hace fallar los pasos en vuelo con el mensaje del pánico, y el self-check lo prueba con `panics`.
    - **Los Bajos, corregidos:**
      - el plazo del `stop` del runner, que demoraba 15 s cada corrida;
      - una frase falsa del README sobre `--no-sandbox`;
      - los tests unitarios, a `src/tests/` como pide `GEMINI.md`;
      - las rutas de la página, en una función con sus tests;
      - el runner, que no comparaba el id de la corrida;
      - un pánico del servidor, que no hacía fallar la corrida;
      - el fmt del job, que revisaba también la raíz;
      - el cache de los navegadores de Playwright;
      - lo que el servidor no hace de `serve_until_shutdown`, y el aviso de `nohup`, en el README.
    - **El Bajo que queda:** que el runner falle si el servidor no se detiene no tiene una prueba permanente. Lo prueba la mutación del servidor que no se detiene.
  - **Segunda ronda, acotada a las correcciones: verde.** Ningún Alto ni Medio, y nada que las correcciones rompieran.
    - **Sus tres Bajos.** Uno se corrigió: otro Worker de la misma página, con un nombre nuevo, pasaba por segunda página. Ahora el runner exige que el contexto haya abierto una segunda página cuando un paso corrió en ella.
    - **Dos quedan sin prueba permanente,** como el de la primera ronda, aunque funcionan probados a mano:
      - la salida con error del servidor ante el pánico de una conexión;
      - el `unhandledrejection` del Worker.
    - **Uno no tiene consecuencia:** sin el `preventDefault`, el error llega también a la página, que encuentra el paso ya resuelto.
- **Desvíos.**
  - **El paso que falla a propósito queda,** con los de `fails_in_peer` y `panics`, como el self-check del runner. Así se comprueba en cada corrida de la CI. El plan decía sacarlo.
  - **Las respuestas armadas para el tope y los plazos del transporte** (DC44 (2)) van con el Lote 9, que prueba el transporte con ellas.
  - **La suite `server`,** además de los tests exigidos: prueba las rutas del servidor de pruebas de DC44.
  - **El origen es `127.0.0.1`, no `localhost`:** también es un contexto seguro, sin la resolución a `::1`.
  - **Los tests unitarios en wasm32, acá:** el chromedriver del entorno (147) no es el de su Chrome (153), y no se pudo bajar otro. Corrieron con ese chromedriver, con `--disable-build-check` (`CHROMEDRIVER_REMOTE`). En la CI, con el del runner.
  - **El runner acepta `--browser firefox|webkit`,** pero solo corre Chromium. Firefox entra en la CI después de S1, como dice DC44.
- **Chequeos.**
  - **Del arnés:**
    - fmt y clippy (los pasos en wasm32 y el servidor, con `-D warnings`) y los tests del servidor;
    - el build;
    - el runner (8 pasos) y su self-check, en Chromium 157 (Playwright 1.63);
    - los tests unitarios en wasm32, en Chrome 153.
  - **De la raíz,** aunque el lote no toca sus crates: fmt, clippy, doc con `-D warnings`, wasm32 con clippy, y la suite del workspace (1647 tests, sin fallas). Los `--ignored` en release no corresponden, porque no cambia ningún crate.
  - **La CI:** en el commit del lote.

### Lote 2 · La capa del OPFS: el directorio (`zemdb-storage`) · ⬜
**Objetivo:** la parte de `OpfsFiles` que trabaja sobre el directorio, con la tarea que retiene una promesa soltada (DC39, DC38), y el método de la capa de DC45.

**Archivos:** `crates/storage/src/lib.rs` (`files` en wasm32), `files/mod.rs` (`native` solo fuera de wasm32; los traits sin `Send` en wasm32, DC38; y el método que dice si `sync_dir` hace durable un cambio que ya ocurrió, DC45, en la capa nativa, la de prueba y la del OPFS, y en `TestFiles`, de `crates/client/tests/common/`; con el contrato de `sync_dir` reescrito, DC39), `files/opfs.rs` (nuevo), `sys.rs` (lanzar una tarea, DC38), `crates/storage/Cargo.toml` (`web-sys`, `js-sys`, `wasm-bindgen` y `wasm-bindgen-futures`, solo en wasm32), los pasos del arnés y `ARCHITECTURE.md` §8 (*File Layer*).

**Qué se hace:** la raíz y las rutas, los bindings que faltan, los errores, la tarea de `sys` y las operaciones del directorio de DC39 (`exists`, `list`, `rename`, `remove`, `create_dir_all`, `sync_dir`), sobre archivos sin handles abiertos. Lo que S1 y S3 contestaron en cada navegador decide qué error es cuál.

**Tests exigidos** (pasos del arnés, sobre la capa real):
- Cada operación del directorio, y su resultado después de recargar.
- Renombrar o borrar un archivo con un handle abierto (abierto con `web-sys` en el paso) falla sin cambiar nada (S3); renombrar reemplaza el destino.
- Una ruta con `..`, o fuera de la raíz, se rechaza sin tocar nada.
- Una operación que es una promesa, soltada: termina, y la siguiente sobre el mismo archivo la espera.
- Un `NotFound` y la cuota llena salen con su `ErrorKind`.

**Interacciones a revisar:** los requisitos de DC17 (§1 del plan de 4.1); lo que el motor da por hecho del directorio (la instalación de snapshots, los borrados de `delete_room`, la búsqueda de temporales); la capa nativa y las de prueba, que no cambian de comportamiento.

**Documentos:** `ARCHITECTURE.md` §8; `INVARIANTES.md`: el estado de L7 sobre el OPFS.

**Entrega:** `feat(storage): the directory of the file layer over the Origin Private File System (Fase 4.5, Lote 2)`.

### Lote 3 · La capa del OPFS: los archivos (`zemdb-storage`) · ⬜
**Objetivo:** el resto de `OpfsFiles`: abrir y crear con su lock, la E/S posicional, renombrar el archivo abierto y el handle muerto (DC39).

**Archivos:** `crates/storage/src/files/opfs.rs` y sus tests unitarios en wasm32; lo que esos tests necesitan, porque los de hoy no compilan en wasm32 (usan `#[tokio::test]`, `zstd`, el motor en disco y los *fail points*): los módulos de tests nativos de `crates/storage/src/` fuera de wasm32, `wasm-bindgen-test` en `crates/storage/Cargo.toml` (solo para los tests en wasm32), el runner (`.cargo/config.toml`) y el comando de la CI (`cargo test --target wasm32-unknown-unknown -p zemdb-storage --lib`); los pasos del arnés y `ARCHITECTURE.md` §8.

**Qué se hace:** `open_locked`, `create_locked`, `create` y `open_read`, con un `FileSystemSyncAccessHandle` cada uno; `read_at`, `write_at`, `size`, `set_len`, `sync_data` y `sync_all`; renombrar el archivo abierto; el handle muerto.

**Tests exigidos:**
- **Pasos del arnés:** leer y escribir en un offset, el largo, truncar, `flush`, y lo escrito después de recargar; `open_locked` y `create_locked` dan `None` con otro handle sobre el archivo, también desde un Worker de otra página; `create_locked` sobre uno que existe es `AlreadyExists` y no lo trunca; renombrar el archivo abierto lo sigue, y el handle sigue sirviendo; `read_at` más allá del fin es `UnexpectedEof`, nunca una lectura corta.
- **Tests unitarios en wasm32:** el handle muerto (la reapertura después del `move()` que falla, forzada con un mecanismo de `opfs.rs` bajo `cfg(test)`: los *fail points* compilan en wasm32 recién en el Lote 4): toda operación siguiente falla, y no escribe en ningún archivo.

**Interacciones a revisar:** los requisitos de DC17; la búsqueda de frames y el replay, que no admiten una lectura corta; la rotación y su vuelta atrás.

**Documentos:** `ARCHITECTURE.md` §8; `INVARIANTES.md`: el estado de L7 sobre el OPFS.

**Entrega:** `feat(storage): the files of the file layer over the Origin Private File System (Fase 4.5, Lote 3)`.

### Lote 4 · El motor en disco en wasm32 (`zemdb-storage`) · ⬜
**Objetivo:** `DiskStorageEngine<OpfsFiles>` en wasm32, sin cambiar nada en nativo (DC38, DC39).

**Archivos:** `crates/storage/src/sys.rs` (el azar), `disk/mod.rs` (las tareas, y `zstd_level` y su validación), `disk/compactor.rs` (los nombres temporales), `disk/recovery.rs`, `lib.rs` (el motor en disco y los *fail points* en wasm32), `fail_point.rs`, `disk/tests` (fuera de wasm32), `crates/storage/Cargo.toml` (`getrandom`, o la fuente de `uuid`, en wasm32), el `clippy.toml` del storage (el lint de DC38), `disk/tests/opfs_like.rs` (DEF-184 y DEF-185), los pasos del arnés y `ARCHITECTURE.md` §8.

**Qué se hace:** las tareas del motor (`open_room`, `apply_snapshot`, `delete_room`, `compact_room` y la compactación automática) con la tarea de `sys`; el motor, la recuperación y la compactación compilados en wasm32; el `.snap` con `ruzstd` `Fastest`; los nombres temporales al azar; el módulo de los *fail points*, también en wasm32 (DC39); el lint del storage. En la capa de prueba nativa, el segundo handle rechazado (DEF-184) y los dos tests del handle muerto de la revisión de cierre de 4.1 (DEF-185).

**Tests exigidos:**
- **En el navegador:** una sala que aplica lotes, se recarga la página y reabre con todo lo aplicado; el Worker terminado en medio de un lote, y la sala que reabre en un prefijo del log; una compactación, también terminada en el medio, y la sala que reabre igual; `apply_snapshot` de un snapshot nativo (Zstd) y de uno propio (`ruzstd`), también después de recargar; `delete_room`, también terminado en el medio; una segunda página que abre la misma sala recibe `RoomLocked`.
- **En nativo:** la suite del storage entera, sin cambios; DEF-184 y DEF-185 en la capa de prueba.

**Interacciones a revisar:** INV-S1, S3, S4, S5 y S8, cuyos mecanismos (las tareas propias, el lock del WAL) cambian de runtime en wasm32; INV-L7 (las tareas del motor retienen los locks, D86 y D89); el decodificador y el codificador `ruzstd` (INV-W1, W2); la compactación automática, que se lanza sin que nadie la espere; un `move()`, una creación o un `removeEntry` que se rechazan con efecto en el OPFS (S5, pregunta 1; DC39).

**Documentos:** `ARCHITECTURE.md` §8 (el motor en wasm32); `INVARIANTES.md`: el estado de S1 a S8, L7, W1 y W2 sobre el OPFS.

**Entrega:** `feat(storage): the disk engine in WebAssembly, over the Origin Private File System (Fase 4.5, Lote 4)`.

### Lote 5 · Los metadatos, durables antes de servirse (`zemdb-client`) · ⬜
**Objetivo:** DC45, que arregla DEF-189 (INV-K16).

**Archivos:** `crates/client/src/metadata.rs` y sus tests; `crates/client/tests/common/mod.rs` (la capa de prueba cuyo `sync_dir` no hace durable un cambio que ya ocurrió); `ARCHITECTURE.md` §11 (*Durable metadata*).

**Qué se hace:** DC45, con sus dos ramas, según lo que la capa dice de su `sync_dir` (el método del Lote 2); en el OPFS, se prueba otra vez en el Lote 8.

**Tests exigidos:**
- Los dos de la auditoría de 4.2 (`replica.patch`), traídos con otro nombre y sin códigos: con los metadatos fuera del directorio del motor y el `sync_dir` del guardado de antes de `reload_schema` que falla, la réplica ya no diverge después del crash; y con los metadatos en el directorio del motor, sigue igual.
- Después de un guardado que falló, `room` sincroniza el directorio antes de servir; si la sincronización falla, `room` falla, y la apertura de la sala con él.
- Un guardado que falló, el store cerrado y vuelto a abrir sin crash: la apertura sincroniza el directorio.
- El guardado de `Registered` en una réplica vacía, con su `sync_dir` que falla (el caso sin test de INV-K16).
- Un borrado que falló, igual que un guardado.
- Con una capa de prueba cuyo `sync_dir` no hace durable un cambio que ya ocurrió: después de un guardado que falló, `room` vuelve a guardar lo que lee antes de servirlo, y se repite un borrado que falló.

**Interacciones a revisar:** la apertura de la sala del storage, que también sincroniza el directorio; el descarte (DC19, DC37), que borra los metadatos; las operaciones de `FileMetadata`, que corren en orden en su tarea.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: INV-K16 en nativo.

**Entrega:** `fix(client): the metadata are durable before they are served (Fase 4.5, Lote 5)`.

### Lote 6 · El despachador del runtime del Worker (`zemdb-client`) · ⬜
**Objetivo:** el despachador de DC42, en nativo, sobre la fachada de 4.4.

**Archivos:** un módulo nuevo del cliente, público (`src/dispatch/`, DC42: los comandos, su configuración, el despachador, las suscripciones y el `TokenProvider` que pide a la página), `lib.rs`, sus tests (`crates/client/tests/dispatch_tests.rs`) y `ARCHITECTURE.md` §11.

**Qué se hace:** los comandos, las respuestas y los avisos de DC42, serializables, con su configuración propia y el id de la pestaña de cada comando (DC46); el despachador, con una tarea por comando, las suscripciones con su confirmación, el pedido de token y el ciclo de vida. Sin el punto de entrada del Worker ni el codec (Lote 12).

**Tests exigidos:**
- Cada comando, contra el servidor real en proceso, da lo mismo que la llamada de la fachada.
- Una escritura que espera su `CommitAck` no frena una lectura de la misma sala; las respuestas llevan el id de su comando.
- Una escritura es una sola llamada a la fachada: el transporte de prueba ve un solo `MutationId`, también con reintentos.
- `watch`: un valor en vuelo por suscripción hasta la confirmación; con muchos cambios sin confirmar, la página recibe después el último; `unwatch` y el cierre de la sala la terminan.
- El token que pide el Worker: con la respuesta de la página, la sala abre; sin respuesta dentro del plazo, como sin token (DC30).
- Después de `shutdown`, todo comando es `RoomClosed`.
- Comandos de dos pestañas (dos ids): cada respuesta y cada valor de `watch` van a la suya, y el aviso de que una se fue termina sus suscripciones, no las de la otra.
- Ida y vuelta con `bincode` de cada comando, respuesta y aviso, con valores al azar de cada tipo (`NaN` con payload, `-0.0`, los extremos de `i64`).

**Interacciones a revisar:** la fachada con el worker (DC36), cuyo modo depende de los `watch` (INV-K18); el descarte a pedido con suscripciones y escrituras en vuelo (DC37); el cierre del cliente con tareas de comandos en vuelo (INV-K11).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K10 y K15 en el despachador.

**Entrega:** `feat(client): the dispatcher of the commands of the browser worker (Fase 4.5, Lote 6)`.

### Lote 7 · El runtime del cliente detrás de `rt` (`zemdb-client`) · ⬜
**Objetivo:** que el cliente tome sus tareas, plazos, reloj, azar y hora de un módulo propio (DC38), en nativo, sin cambiar nada. Después de que cierre 4.3.

**Archivos:** `crates/client/src/rt.rs` (nuevo), `client.rs` (también las cotas `Send` de su tarea propia), `worker.rs` (sus `JoinHandle`), `events.rs`, `metadata.rs`, `http_transport.rs`, `api/write.rs`, `dispatch/`, y el `clippy.toml` del cliente (el lint de DC38; el del storage va en el Lote 4).

**Qué se hace:** cada uso de `tokio::spawn`, `spawn_blocking`, `tokio::time`, `Instant`, `SystemTime` y `Uuid::new_v4` pasa por `rt`, que en nativo es exactamente lo de hoy; el lint de DC38, que vale al compilar para wasm32.

**Tests exigidos:**
- La suite del cliente entera, con la simulación y su reloj pausado, sin tocar ninguna expectativa.
- Tests unitarios de `rt`: una tarea sigue hasta su fin después de soltar su handle; cortarla la detiene en su próximo `.await`; un plazo y un `sleep_until` con el reloj pausado.
- El lint, con un `tokio::spawn` fuera de `rt`, falla en el chequeo de wasm32 (se prueba una vez y se saca).

**Interacciones a revisar:** el reloj pausado de la simulación, que tiene que seguir controlando cada plazo; las tareas que corren hasta su fin (DC21); el corte del worker (DC28).

**Documentos:** el plan.

**Entrega:** `refactor(client): the tasks, the timers, the clock and the randomness of the client behind a module of its own (Fase 4.5, Lote 7)`.

### Lote 8 · El cliente en wasm32: la feature `wasm` (`zemdb-client`) · ⬜
**Objetivo:** el cliente entero en wasm32 (DC38), con el motor del OPFS y sus metadatos (DC39, DC45).

**Archivos:** `crates/client/Cargo.toml` (la feature, y `getrandom` o la fuente de `uuid` en wasm32), `lib.rs`, `rt.rs` (la parte de wasm32), los traits sin `Send` en wasm32 (`transport.rs`, `token.rs`, `metadata.rs`), `api/watch.rs` (`ChangeStream`), `.github/workflows/ci.yml` (el chequeo de wasm32), los pasos del arnés y `ARCHITECTURE.md` §11.

**Qué se hace:** DC38 en wasm32; `FileMetadata` sobre `OpfsFiles`, con la parte del OPFS de DC45; el chequeo de la CI.

**Tests exigidos** (pasos del arnés, con un transporte de prueba del crate de pruebas que responde con frames armados con `zemdb-core`):
- El núcleo y la fachada sobre el motor del OPFS: una sala que se registra y aplica un lote; la página recargada; la sala abre sin red desde sus metadatos y lee las mismas filas (el criterio de aceptación sin el servidor real, que llega en el 9).
- Los plazos: el heartbeat del worker en su intervalo, el plazo de una escritura, el backoff; detener el worker corta sus tareas, que terminan.
- Una tarea del aplicador soltada por su llamador termina (DC21).
- Un `watch` emite en wasm32 (`ChangeStream` sin `Send`).
- Un guardado de metadatos y una recarga: abre con lo guardado; después de un guardado que falló, `room` vuelve a guardar antes de servir (DC45).
- **En nativo:** la suite entera, sin cambios.

**Interacciones a revisar:** todo lo que corre en una tarea propia en el núcleo (DC21) y en el worker (DC28); los timers del navegador (S5, pregunta 5); INV-K16 en el OPFS (el guardado con `move()`); la memoria del Worker.

**Documentos:** `ARCHITECTURE.md` §11 (el cliente en wasm32); `INVARIANTES.md`: K1 a K8, K10, K11 y K16 en el navegador.

**Entrega:** `feat(client): the client in WebAssembly, with the wasm feature (Fase 4.5, Lote 8)`.

### Lote 9 · El transporte del navegador (`zemdb-client`) · ⬜
**Objetivo:** `FetchTransport` (DC40), con el stream por `fetch`, el tope de lo que lee (DEF-195), el tope de sus streams, y en wasm32 los syncs de a uno y las transferencias de a una, con permisos separados.

**Archivos:** `crates/client/src/fetch_transport.rs` (nuevo), `lib.rs`, `client.rs` y `worker.rs` (los permisos de los syncs y de las transferencias del worker en wasm32), los pasos del arnés y sus rutas de prueba, y `ARCHITECTURE.md` §11.

**Qué se hace:** DC40.

**Tests exigidos** (pasos del arnés, contra el servidor de pruebas):
- Cada mensaje llega a su endpoint con su token, y su respuesta vuelve entera.
- Una respuesta que anuncia más que `MAX_FRAME_SIZE`, o que trae más sin anunciarlo, falla sin leer el resto (DEF-195).
- Un pedido que calla más que el plazo se corta; uno que avanza despacio, no.
- El stream: los eventos llegan bloque a bloque al parser; una negativa (`EventsDisabled`, `Unauthorized`, `Forbidden`, `RateLimited`) llega con su frame y el worker reacciona a cada una como en nativo (DC24, DC29, DC30); un evento de más de 64 KiB cierra el stream.
- Más salas en `live` que `max_streams`: las de más siguen como en `periodic`, y `status` las muestra en `live`, con el stream cerrado; los pedidos de todas avanzan (con HTTP/1.1, el del arnés); y una que suelta su stream deja entrar a otra.
- Dos salas que se ponen al día a la vez: sus syncs del worker van de a uno, y sus bajadas también; una bajada larga no frena el sync de otra sala.
- El criterio de aceptación con el servidor real: escribir, recargar, leer sin red, y ponerse al día con red, también en `live`.

**Interacciones a revisar:** el parser del núcleo (DC27), el worker y su stream (DC26, DC28), la renovación del token (DC30), los plazos del núcleo (DC6) y los del transporte, el modo de una sala de la fachada (DC36, INV-K18), la subida del designado, que puede esperar a una bajada de otra sala (DC22, §A.8), y el control de flujo de un stream que no se lee (S5, pregunta 4).

**Documentos:** `ARCHITECTURE.md` §11 (el transporte del navegador); `INVARIANTES.md`: K7, K12, K15 y K17 en el transporte del navegador, y el riesgo de §4.

**Entrega:** `feat(client): the transport of the browser over fetch, also for the event stream (Fase 4.5, Lote 9)`.

### Lote 10 · Una pestaña por directorio (`zemdb-client` y `zemdb-storage`) · ⬜
**Objetivo:** DC41 (INV-K13).

**Archivos:** `crates/client/src/metadata.rs` (el Web Lock en `FileMetadata::open` en wasm32, la opción de esperar en su constructor, y el error propio de un store tomado), `client.rs` y `error.rs` (`InUse`), `crates/storage/src/files/native.rs` (`None` solo con el lock tomado, con un *fail point* en la toma del lock), `crates/storage/src/files/mod.rs` (el contrato de `create_locked`: un error también puede dejar el archivo creado), `crates/storage/src/files/tests/native.rs`, la expectativa de `a_second_client_over_the_same_directory_fails_until_the_first_shuts_down` (`crates/client/tests/client_tests.rs`), que pasa de `Storage` a `InUse`, los pasos del arnés y `ARCHITECTURE.md` §11.

**Qué se hace:** DC41, también `InUse` en nativo, sin el punto de entrada del Worker (Lote 12), que lo usa.

**Tests exigidos:**
- **Pasos del arnés, con dos páginas del mismo contexto:** la segunda recibe `InUse` sin tocar ningún archivo (una capa que cuenta las operaciones, alrededor de la del OPFS, no ve ninguna); con la opción de esperar, la segunda abre cuando la primera hace `shutdown`, y cuando su Worker termina; recargar la página que tiene el cliente: la página nueva lo abre; dos directorios distintos se abren a la vez en dos páginas.
- **En nativo:** un segundo cliente sobre un directorio tomado recibe `InUse`. En el storage, un error inyectado al tomar el lock (el *fail point* nuevo, en `files/tests/native.rs`) es un `Err`, no `None`: con el código de hoy, que da `None` con cualquier error, el test falla. En el cliente, con `TestFiles` que falla en el `open_locked` de `client.lock`, `Client::open` da `Storage`, no `InUse`.

**Interacciones a revisar:** `FileMetadata` y `client.lock`; el `RoomLocked` del motor en disco, que sale del mismo `None` de la capa nativa (`lock_room_files`, y `create_locked` en la rotación y en el snapshot temporal, en `compactor.rs`), y la capa de prueba `opfs_like.rs`, que delega en ella; el snapshot temporal que `create_locked` creó antes de fallar al tomar su lock, que `stage_file` hoy deja si sale por el error (tiene que borrarlo, como con `None`); `shutdown` (DC28) y el último `Ack`; el orden en que el navegador suelta los handles y el lock (S5, pregunta 2).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K13, K11 y K17.

**Entrega:** `feat(client): one tab at a time opens a directory of the client in the browser (Fase 4.5, Lote 10)`.

### Lote 11 · Las claves en el Worker (`zemdb-client`) · ⬜
**Objetivo:** `WebCryptoEngine` (DC43).

**Archivos:** `crates/client/src/webcrypto.rs` (nuevo), `lib.rs`, los pasos del arnés, `ARCHITECTURE.md` §3.1 (el formato del texto cifrado) y §11.

**Qué se hace:** DC43.

**Tests exigidos** (pasos del arnés):
- Ida y vuelta de cada tipo de valor de una columna cifrada, por la fachada, con una clave no extraíble.
- Un texto cifrado de otra celda, de otra sala o con otra clave es `Decryption`; uno con una versión que no conoce, también.
- Una sala sin clave: escribir una columna cifrada es `Crypto` y leerla es `Decryption`, sin mandar nada; una clave que no es AES-GCM de 256 bits es `Config`.
- Dos cifrados del mismo valor dan textos distintos (el nonce al azar).
- Ningún pedido lleva el claro (la marca en el texto en claro, como en la simulación de 4.4).
- Un texto cifrado de prueba fijo se descifra (el formato no cambia sin que un test falle).

**Interacciones a revisar:** el orden de una escritura de la fachada (DC32) y su paso del cifrado (DC34); el texto descifrado validado contra el tipo de la columna (INV-K12).

**Documentos:** `ARCHITECTURE.md` §3.1 y §11; `INVARIANTES.md`: K9.

**Entrega:** `feat(client): the crypto engine of the browser over WebCrypto (Fase 4.5, Lote 11)`.

### Lote 12 · El punto de entrada del Worker (`zemdb-client`) · ⬜
**Objetivo:** el límite con la página de DC42: el punto de entrada en el Worker, el codec binario, la clave al lado del mensaje y el pánico que cierra el Worker.

**Archivos:** `crates/client/src/dispatch/` (el punto de entrada en wasm32, el codec y el *panic hook*), los pasos del arnés (la página que habla con el Worker por `postMessage` y lo termina ante su error) y `ARCHITECTURE.md` §11.

**Qué se hace:** el punto de entrada, que abre el cliente con el motor de DC39, sus metadatos, el transporte de DC40, el store de DC41, con su opción de esperar, y el `CryptoEngine` de DC43; el codec binario; el `CryptoKey` al lado del mensaje; el *panic hook*.

**Tests exigidos** (pasos del arnés):
- Cada comando por `postMessage`, con la respuesta correlacionada por su id.
- Un mensaje que no decodifica: con su id, una respuesta de error; sin él, se ignora; el Worker sigue.
- `watch` por el límite: los valores llegan con la confirmación de la página, uno a la vez.
- La clave de una sala por el límite: un `CryptoKey` no extraíble cifra y descifra en el Worker.
- El token que pide el Worker a la página.
- Un pánico forzado en el Worker (un comando de prueba del crate de pruebas): el Worker se cierra, la otra página toma el directorio, y la sala reabre con lo aplicado.
- Una reserva que falla (un comando de prueba que llama a `std::alloc::handle_alloc_error`): la página termina el Worker ante su error, y la otra página toma el directorio.

**Interacciones a revisar:** el despachador (Lote 6); el lock de la pestaña (Lote 10); el cierre del Worker con comandos en vuelo (INV-K11).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K9, K11, K12 y K15 en el límite.

**Entrega:** `feat(client): the entry point of the browser worker, with its binary codec (Fase 4.5, Lote 12)`.

### Paso D · El protocolo entre pestañas (el diseño de DC46) · ⬜
**Objetivo:** el protocolo de DC46, con los requisitos de §1.3, antes de escribir su código. Solo documentos.

**Qué se hace:** DC46 con su protocolo; `INVARIANTES.md` (K2, K8, K9, K10, K13, K15 y lo que toque de K17) con su detalle; los Lotes 10, 13 y 14, con sus archivos y sus tests; y las precisiones de otras decisiones que pida (DC30, DC32, DC35, DC36, DC41). Con las respuestas de S5 (preguntas 2, 5 y 6).

**Cierra con:** una revisión cruzada, como la del Lote 0, hasta verde; y el dueño del proyecto confirma DC46 con su protocolo.

**Entrega:** `docs: the protocol between the tabs of the browser client (Fase 4.5, paso D)`.

### Lote 13 · Las demás pestañas, a través de la líder (`zemdb-client`) · ⬜
**Objetivo:** DC46, salvo el relevo, con el protocolo del paso D: el Worker que no tiene el lock encuentra a la líder y le reenvía los comandos de su página, y la líder los atiende y le devuelve las respuestas y los valores de `watch`.

**Archivos:** `crates/client/src/dispatch/` (el modo de seguidor y lo que pida el protocolo), `crates/client/Cargo.toml` (las features de `web-sys` del `BroadcastChannel`), los pasos del arnés (con dos y tres páginas) y `ARCHITECTURE.md` §11; los demás, los que fije el paso D.

**Qué se hace:** DC46, sin el relevo (Lote 14). Si pasa de unas 300 líneas, se parte.

**Tests exigidos:** los que fije el paso D, y al menos (pasos del arnés):
- con dos páginas, la segunda lee, escribe y mira con `watch` a través de la primera, sin abrir ningún archivo (la capa que cuenta las operaciones no ve ninguna suya);
- una página que se abre después de que la líder se anunció la encuentra;
- con tres, cada respuesta y cada valor van a su página, y ninguna recibe lo de otra;
- una escritura de un seguidor es un solo `MutationId` en el servidor;
- un seguidor con otra persona, otro servidor u otra versión del protocolo recibe `InUse` y no le manda nada a la líder;
- cerrar el seguidor libera en la líder lo que era suyo.

**Interacciones a revisar:** el despachador con comandos de varias pestañas (Lote 6); el lock de la pestaña (Lote 10); el codec binario entre Workers y el de la página (Lote 12 y 4.6); el token (DC30); el tope de streams y los permisos del worker, que son de la líder (DC40); el modo de una sala y sus `watch` en la fachada (DC36, INV-K18); el descarte a pedido (DC37).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K8, K9, K10, K13 y K15.

**Entrega:** `feat(client): the other tabs use the client through the tab that holds its directory (Fase 4.5, Lote 13)`.

### Lote 14 · El relevo de la líder (`zemdb-client`) · ⬜
**Objetivo:** que, cuando la líder se va, otra pestaña la reemplace, con el protocolo del paso D, sin que un comando se ejecute dos veces ni después de darse por terminado, y sin que un `watch` quede colgado (DC46).

**Archivos:** `crates/client/src/dispatch/`, los pasos del arnés y `ARCHITECTURE.md` §11; los demás, los que fije el paso D.

**Qué se hace:** el relevo de DC46.

**Tests exigidos:** los que fije el paso D, y al menos (pasos del arnés):
- la líder se va (su página se recarga o se cierra, su Worker se termina, un pánico, su `shutdown`), y la primera de la cola la reemplaza: los seguidores siguen leyendo y escribiendo;
- dos relevos seguidos;
- una escritura de un seguidor en vuelo durante el relevo termina como `OutcomeUnknown` y no sale nunca después, también con una líder ocupada que vuelve a correr: el servidor ve a lo sumo un `MutationId`;
- los `watch` de los seguidores siguen después del relevo, también los de una columna cifrada, sin repetir un valor ni quedar colgados;
- en la líder no queda nada de lo que un seguidor dio por terminado.

**Interacciones a revisar:** el orden en que el navegador suelta el lock y los handles de la líder que se va (S5, pregunta 2); sus escrituras en vuelo (INV-K11, INV-K2); el `watch` que se vuelve a suscribir (INV-K10); el worker de cada sala en la líder nueva, que arranca como al abrir (DC36); las claves (INV-K9); los tokens de las salas que se vuelven a abrir (DC30).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K9, K10, K11, K13 y K15.

**Entrega:** `feat(client): another tab takes over the directory when the one that holds it goes away (Fase 4.5, Lote 14)`.

### Lote 15 · La aceptación en el navegador (`tools/browser`, pruebas) · ⬜
**Objetivo:** el criterio de aceptación de 4.5 (DC44), de punta a punta, por `postMessage`.

**Qué se hace:** los escenarios de DC44 por el runtime del Worker, contra el servidor de pruebas: la réplica que sobrevive a recargar la página y abre sin red; el Worker terminado en medio de un lote; dos pestañas, la segunda a través de la líder, y el relevo; una columna cifrada; dos clientes del navegador (dos contextos) que convergen con uno nativo (INV-K4); y la página terminada en `pagehide` (INV-K11, DC41). Firefox en la CI, si el paso S1 salió bien.

**Tests exigidos:** los escenarios, en Chromium en la CI; en Firefox, si entra.

**Interacciones a revisar:** las de los Lotes 8 a 14 juntas; la duración del job.

**Documentos:** `INVARIANTES.md`: lo *Automatizable* de K1, K4, K9, K11 y K13; el plan.

**Entrega:** `test(client): the acceptance of the client in a browser (Fase 4.5, Lote 15)`.

### Lote 16 · Revisión de cierre y cierre · ⬜
**Objetivo:** cerrar 4.5 (§3), después del paso S2.

**Qué se hace:** la revisión de cierre; lo que encuentre; los documentos (el plan de la fase, `INVARIANTES.md`, `ROADMAP.md`).

---

## 3. Revisión de cierre

Al cerrar 4.5 (Lote 16), después del paso S2, con tres revisores (agentes aparte, de contexto fresco y solo lectura), cada uno con las tres condiciones de `INVARIANTES.md` §1.4 sobre lo que la sub-fase tocó y los mecanismos con los que interactúa, con el arnés del navegador para reproducir lo que encuentre:
1. **El motor del navegador:** INV-S1 a S8, L7, W1, W2 y K16 sobre el OPFS, con los supuestos de §2 y lo que respondieron S3 y S5.
2. **El cliente en el navegador:** INV-K1 a K8, K11, K12, K15 y K17 en wasm32, con el runtime de DC38 y el transporte de DC40.
3. **El Worker y las pestañas:** INV-K2, K8, K9, K10, K11, K13 y K15 en el runtime del Worker, en su límite y entre pestañas, con DC41 a DC43 y DC46, y K16 en nativo (DC45).

Sus rupturas se arreglan en lotes nuevos de 4.5, cada uno con su revisión de delta, antes de cerrar.

---

## 4. Seguimiento

**Avance:** Lote 0 (el contrato) cerrado el 23 de Noviembre de 2026, con la CI de `main` en verde sobre `91850bf` y todas sus decisiones adoptadas; sigue el paso S1, con el Lote 1 en paralelo.

| Paso | Qué | Estado |
|---|---|---|
| 1 | Este plan, con DC38 a DC46 | ✅ |
| 2 | El contrato: `INVARIANTES.md` versión 48, el plan de la fase y `ROADMAP.md` | ✅ |
| 3 | La revisión cruzada del Lote 0 | ✅ Verde en su sexta ronda, después de 4 bloqueantes, 27 ajustes y unos 80 menores en las seis, todos corregidos. La séptima a la novena, acotadas a la pestaña líder (DC46), con 1 bloqueante y 7 u 8 ajustes cada una: el dueño del proyecto eligió el 23 de Noviembre de 2026 dejar DC46 en su principio y pasar su protocolo al paso D, con esos hallazgos como requisitos (§1.3) |
| 4 | Las *Propuestas*, con el dueño del proyecto | ✅ El 22 de Noviembre de 2026 adoptó DC40 (el stream por `fetch`, el tope de streams y los permisos del worker, fijos) y DC43 (WebCrypto), eligió la pestaña líder para DC41 (de ahí DC46) y aceptó `InUse` en nativo y la revisión de cierre sin auditoría amplia. El 23 adoptó las demás: DC38, DC39 (con su condición), DC41, DC42, DC44 (con Safari en un runner de macOS, porque ya no tiene Safari), DC45 y DC46 en su principio |
| 5 | La CI de `main` | ✅ En verde sobre `91850bf` (23 de Noviembre de 2026) |
