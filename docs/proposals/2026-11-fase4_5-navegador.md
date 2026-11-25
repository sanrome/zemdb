# Navegador (Rust) — Plan de la sub-fase 4.5

**Fecha:** 19 de Noviembre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: el paso S1 (los spikes) hecho el 23 de Noviembre de 2026, en Chromium, Firefox y Safari (§1.2.1): revisa DC39 (un `move()` sobre un archivo existente no es atómico en Firefox) y DC41 (el Web Lock llega antes de que se suelten los handles), las dos adoptadas por el dueño del proyecto el 24 de Noviembre de 2026, y suma notas en DC40, DC44, DC45 y DC46. Lote 1 (el arnés del navegador) hecho el 23 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda; el Lote 6a (los comandos y el despachador del runtime del Worker), hecho el 24 de Noviembre de 2026, con el 6b (las suscripciones y el token) por hacer; el Lote 7 (el runtime del cliente detrás de `rt`), hecho el 24 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda; la CI de `main`, en verde sobre `2dab234`. Lote 0 (el contrato) cerrado el 23 de Noviembre de 2026, con la revisión cruzada en verde en su sexta ronda y DC46 en su principio, con su protocolo para el paso D (§1.3). El dueño del proyecto adoptó DC40 y DC43 el 22 de Noviembre de 2026, y eligió ese día la pestaña líder para DC41, de donde sale DC46; adoptó las demás el 23 (DC46, en su principio: su protocolo, en el paso D).
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
- **El modelo de amenaza.** No cambia de forma: DC11 ya pone en el alcance el navegador, el origen como la aplicación y la concurrencia entre pestañas, y en (3) extiende al OPFS el supuesto del disco. §2 de `INVARIANTES.md` precisa ese supuesto (lo que en el OPFS hace las veces de sincronizar un directorio: un `move()` a un nombre libre, y crear y borrar entradas) y lo que el SDK le pide al navegador para coordinar las pestañas (los Web Locks, su cola y los handles exclusivos, y el `BroadcastChannel` entre ellas), como §2 ya confía en el sistema operativo para `flock`; dice que las demás pestañas del origen, que usan el SDK a través de la líder, también son la aplicación; y dice dónde está el `CryptoEngine` en el navegador. Por eso no pide una auditoría amplia (§1.4): las garantías nuevas se auditan en la revisión de cierre (§3). Si el dueño del proyecto lo ve como un cambio del modelo, la revisión de cierre se hace con el método de una auditoría amplia.
- **Qué no.** El paquete de TypeScript, el codec de objetos de JavaScript con las representaciones de DC14 (INV-K14), `persist()` desde la página, Vite y la app de referencia en el navegador (4.6); CORS en el servidor; un lock por sala; un `CryptoEngine` en JavaScript cargado en el Worker; partir en pasos el trabajo de CPU largo del Worker (todo eso, al backlog si hace falta).
- **Decisiones nuevas:** DC38 a DC46 (§1), adoptadas por el dueño del proyecto: DC40 y DC43 el 22 de Noviembre de 2026, y las demás el 23 (DC46, en su principio: su protocolo, en el paso D). Precisan decisiones de la fase:
  - DC8 y DC17 (la implementación del OPFS, DC39; la coordinación entre pestañas, DC41 y DC46);
  - DC27 y §A.5 (el stream del navegador por `fetch`, y no por `EventSource`, DC40);
  - DC28 (en wasm32, los syncs del worker de a uno y sus transferencias de a una, DC40);
  - DC14 y DC18 (el enum de comandos, DC42, con las demás pestañas, DC46);
  - DC32 (un error nuevo, `InUse`, DC41; los errores de las demás pestañas, en el paso D de DC46);
  - DC34 (en el navegador, el `CryptoEngine` es el del SDK, con las claves de la aplicación, DC43).
- **Spikes:** S1 en Firefox (DC8) y S3 en Safari y Firefox (DC17), pendientes de 4.0 y 4.1, y S5, nuevo (§1.2). Los respondió el paso S1, con Firefox en un runner de `ubuntu-latest` y Safari en uno de `macos-latest` (§1.2.1), y revisaron DC39 y DC41.
- **Cómo cierra:** con una revisión de cierre (`docs/METODOLOGIA.md` §6), con tres revisores (§3), con las pruebas del navegador en verde en Chromium en la CI y la página de aceptación corrida en Safari y Firefox.

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC8 (el OPFS), DC9 (`ruzstd` para el `.snap` del navegador), DC11 (el navegador en el modelo de amenaza), DC14 (los principios del límite del Worker), DC17 (la capa de archivos), DC18 (el enum de comandos, que aparece en el runtime del Worker) y DC34 (el cifrado). Esta sub-fase agrega las siguientes:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC38** | La feature `wasm` y el runtime del cliente: tareas, plazos, reloj, azar y hora del navegador, sin `Send` en wasm32 | 2, 4, 7, 8 | Adoptada (23 de Noviembre de 2026) |
| **DC39** | El motor del navegador: `DiskStorageEngine` sobre `OpfsFiles`, una capa de archivos del OPFS | 2, 3, 4, 8 | Adoptada (23 de Noviembre de 2026); revisada con el paso S1 (§1.2.1): las generaciones, adoptadas el 24 de Noviembre de 2026 |
| **DC40** | El transporte del navegador: `fetch`, también para el stream de eventos, con el tope de lo que lee y de sus streams | 9 | Adoptada (22 de Noviembre de 2026) |
| **DC41** | Una pestaña líder por directorio del cliente, con un Web Lock, y el Worker que la página termina en `pagehide` | 10, 12, 15 | Adoptada (23 de Noviembre de 2026); la pestaña líder, elegida el 22; revisada con el paso S1 (§1.2.1): la espera de los handles después del lock, adoptada el 24 de Noviembre de 2026 |
| **DC42** | El runtime del Worker: un despachador de comandos serializables, y su punto de entrada con un codec intercambiable | 6, 12 | Adoptada (23 de Noviembre de 2026) |
| **DC43** | Las claves en el Worker: un `CryptoEngine` del SDK sobre WebCrypto, AES-GCM con un `CryptoKey` por sala | 11, 12 | Adoptada (22 de Noviembre de 2026) |
| **DC44** | Las pruebas en el navegador: el arnés, la página que se recarga sola, Playwright, los tests unitarios en wasm32 y la CI | 1, 3, 15 | Adoptada (23 de Noviembre de 2026), con Safari en un runner de macOS (§1.2) |
| **DC45** | DEF-189: los metadatos, durables antes de servirse | 2, 5, 8 | Adoptada (23 de Noviembre de 2026); S5 (1), respondida (§1.2.1) |
| **DC46** | Las demás pestañas: usan el SDK a través de la pestaña líder, por un `BroadcastChannel`, con el relevo cuando la líder se va | 6, 13, 14, 15 | Adoptada en su principio (23 de Noviembre de 2026); su protocolo, en el paso D, con S5 (6) respondida (§1.2.1) |

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
- **Un lint** (`clippy::disallowed_methods`, con un `clippy.toml` en `crates/client` y en `crates/storage`) deja fuera de `rt` y de `sys` lo que en wasm32 entra en pánico o no corre: `std::time::Instant::now`, `SystemTime::now`, `tokio::spawn`, `spawn_blocking` y los timers de Tokio. Vale solo al compilar para wasm32 (fuera de wasm32, los crates lo permiten con `cfg_attr`), así que no toca los tests ni el servidor, que siguen con Tokio. *Precisado en el Lote 7:* en el cliente vale también en nativo, fuera de sus tests unitarios (`cfg_attr(test, ..)`), para que la CI vea un uso nuevo fuera de `rt` antes de que el crate compile en wasm32; sus tests de integración y su ejemplo lo permiten con un `allow` cada uno. El storage hace lo mismo en el Lote 4.
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
  - `open_locked` y `create_locked` dan `None` cuando otro handle tiene el archivo, con el error que da cada navegador al abrir un handle: `NoModificationAllowedError` en Chromium y Firefox, e `InvalidStateError` en Safari, que lo da solo por eso (§1.2.1). En las demás operaciones (`open_read`, `create`, `remove`, `rename`), un archivo tomado da un `io::Error` propio de la capa, *tomado*, que envuelve un tipo de `zemdb-storage` y se reconoce con `downcast`. El error del navegador que lo indica depende de la operación (§1.2.1):
    - `createSyncAccessHandle` y `move()` dan `NoModificationAllowedError` en Chromium y Firefox, e `InvalidStateError` en Safari;
    - `removeEntry` da `NoModificationAllowedError` en los tres, y en Safari solo por un archivo tomado o con un stream abierto.

    Ese error lo reintenta la apertura de DC41 durante su plazo, después de recibir el lock (en un relevo, al recargar la página, o ante una operación soltada del mismo Worker). En otro momento es un error como cualquiera.
  - `read_at` da exactamente lo pedido o falla: repite una lectura corta (el requisito de DC17).
  - `write_at` escribe todo; `size`, `set_len`, `sync_data` y `sync_all` son `getSize`, `truncate` y `flush`.
  - Renombrar un archivo abierto cierra el handle, hace `move()` y lo reabre sobre el mismo `FileSystemFileHandle` (DC17, S3). Si la reapertura falla, el handle queda muerto: toda operación siguiente falla, y nunca escribe en otro archivo (DC17). `move()` va siempre con la forma `move(directorio, nombre)`, la única de Safari. En Safari, abrir un `FileSystemFileHandle` cuya entrada se borró, o se movió por otro handle, la vuelve a crear vacía (§1.2.1): la capa solo reabre el handle que acaba de mover, bajo el lock de la sala.
- **El directorio:** `exists`, `list` (las entradas del directorio, por prefijo), `rename` (sin handles abiertos en el origen ni en el destino, y sin `move()` sobre un archivo existente: abajo, *Las generaciones*), `remove` (`removeEntry`) y `create_dir_all`; `sync_dir` no hace nada, y su contrato pasa a decir que, donde no hace nada, solo se garantizan durables los cambios de las operaciones que salieron bien (si hace durable uno que ya ocurrió, lo dice el método de DC45). `create_locked` verifica que el archivo no exista y después lo crea, porque el OPFS no tiene un *create-new* atómico: es seguro bajo el lock de la sala y con una pestaña por directorio (DC17, DC41).
- **Las generaciones** (con el paso S1; adoptadas el 24 de Noviembre de 2026). En Firefox, un `move()` sobre un archivo existente no es atómico ante un corte: con uno en vuelo, el destino queda vacío o no está, y se pierde también lo que dejaron los anteriores (S5 (1)). En cambio, un `move()` a un nombre libre no se dañó en ninguno de los 20 cortes del seguimiento (§1.2.1). Por eso la capa del OPFS nunca mueve sobre un archivo existente, en ningún navegador.
  - **Los nombres.** Cada nombre del motor es, en el OPFS, una serie de generaciones: el nombre y, después de su primer reemplazo, el nombre con un sufijo de generación creciente, que ningún nombre del motor ni de los metadatos puede tener. Un nombre vale lo de su generación más nueva, y para la capa existe si existe alguna generación.
  - **Renombrar.** `rename` y el `rename` del handle abierto (en la rotación del WAL y en su vuelta atrás) siguen estos pasos:
    1. borran las generaciones viejas del origen, si las hay. Si eso falla, el `rename` falla sin cambiar el valor de ningún nombre. En el `rename` del handle, este paso va antes de cerrar el handle, así un `rename` que falla deja el WAL usable, como en nativo;
    2. mueven la generación más nueva del origen a la generación siguiente del destino, o al nombre si el destino no existe: siempre a un nombre libre;
    3. borran las generaciones viejas del destino, sin esperar que salga bien.

    Una vez resuelto el `move()`, `rename` da `Ok`, porque el valor ya es el nuevo. Una generación vieja que no se pudo borrar (por un `NotFound`, o porque estaba abierta) no cambia el valor, y se borra en el próximo `rename` o `remove` de ese nombre. Así, un `rename` que falla no cambió nada, como espera el motor (`apply_snapshot`, INV-S5).
  - **Borrar.** `remove` borra las generaciones de la más vieja a la más nueva. Un corte o un error en el medio deja el nombre con su valor o sin ninguno, nunca con uno anterior.
  - **Lo demás.** `exists`, `open_locked`, `create` (que trunca la más nueva) y `open_read` usan la generación más nueva. `create_locked` da `AlreadyExists` si existe alguna. `list` da cada nombre una sola vez, sin el sufijo.
  - **Resolver un nombre.** El OPFS no lista por prefijo, así que cada operación que resuelve un nombre (`exists`, las aperturas, `create`, `create_locked`, `rename` y `remove`) recorre las entradas del directorio en esa misma operación, y de ahí saca sus generaciones y la siguiente. Las compara por el nombre exacto, una vez quitado el sufijo, y no por prefijo, porque `room_{id}.wal` es prefijo de `room_{id}.wal.compacting`.
    - **El costo** es proporcional a todas las entradas del directorio, con las de todas las salas del cliente y sus metadatos, así que abrir N salas cuesta del orden de N² entradas recorridas. Con las decenas de salas por cliente que se esperan es poco; si hiciera falta, un índice del directorio es una optimización de la capa.
  - **Las tareas y los locks.** Todos los pasos de un reemplazo corren en la tarea de su operación (INV-L7). Las dos capas de un directorio, la del motor y la de los metadatos, no se cruzan: cada una renombra solo sus nombres, bajo su lock (el de la sala, o `client.lock`). Nada limpia el directorio al abrirlo: una generación vieja que quedó solo ocupa lugar hasta el próximo reemplazo de su nombre.
  - **Los errores de `move()`.** La capa no usa el error de `move()` para saber si falta el origen o el directorio del destino, porque en Safari y en Firefox puede ser `UnknownError` (§1.2.1). Lo sabe por el listado.

  Una generación nueva está entera, porque el motor escribe el origen con `flush()` antes de renombrarlo (DC17). Un corte deja el nombre en su generación vieja o en la nueva, como un `rename` en nativo. El motor y los metadatos no cambian: el reemplazo atómico lo da la capa, con el supuesto de §2 sobre el `move()` a un nombre libre.

  Esto precisa lo que eligió el dueño del proyecto el 23 de Noviembre de 2026: no confiar en un `move()` sobre un archivo existente. Las generaciones no validan el destino al reabrir ni lo completan desde un temporal: nunca lo dejan expuesto. Las alternativas descartadas, abajo, dicen por qué.
- **Una operación soltada termina** (INV-L7, DC17). Las operaciones del OPFS que son promesas (obtener un handle de archivo o de directorio, `createSyncAccessHandle`, `move()`, `removeEntry`, listar) siguen en el navegador aunque se suelte el future que las espera, así que la capa las corre en una tarea propia (la de `sys`, DC38) que retiene el handle, y su lock, hasta que la promesa termina: la próxima operación sobre ese archivo la espera. Las síncronas (`read`, `write`, `flush`, `truncate`, `getSize`) terminan en la misma llamada.
- **Los errores:** cada `DOMException` pasa a un `io::ErrorKind` (`NotFoundError` a `NotFound`, la cuota llena a `StorageFull`), con su nombre en el mensaje.
- **El motor en wasm32:** sus tareas, con la de DC38; su trabajo de CPU, en el lugar; el `.snap`, comprimido con `ruzstd` `Fastest` (DC9), y `zstd_level`, validado contra lo que da en wasm32 (hoy, con `zstd::compression_level_range()`); los nombres temporales, con el azar de DC38; y el módulo de los *fail points*, que hoy no compila en wasm32, compila ahí, donde fuera de los tests no hace nada, como en nativo (sus pausas, que esperan con `Instant`, quedan para los tests nativos).
- **Supuestos** (§2 de `INVARIANTES.md`). En el OPFS, sincronizar un directorio no hace nada, así que lo que en nativo hace durable `sync_dir` tiene que serlo cuando su promesa resuelve:
  - lo escrito con `flush()` sobrevive como lo sincronizado con `fsync` (ya está, S1);
  - un `move()` a un nombre libre resuelto es atómico y durable, como un `rename` seguido de sincronizar el directorio (sobre un archivo existente, no: *Las generaciones*);
  - una entrada creada (un archivo, con `getFileHandle` y `create`, o un directorio, con `getDirectoryHandle` y `create`) o borrada (`removeEntry`) cuya promesa resolvió es durable, en el orden en que resolvieron sus promesas, como un cambio de directorio sincronizado.

  De eso dependen la instalación de un snapshot, la rotación del WAL, el directorio de datos, el WAL y el `.lock` que crea la apertura de una sala, los borrados de `delete_room` (INV-S8), y los guardados y los borrados de los metadatos del cliente (INV-K16). Son supuestos, como `flush()`. S5 los probó en Chromium, Firefox y Safari cortando el navegador, que no es un corte de energía (§1.2.1).
- **La condición del paso S1** (S1 en Firefox, S3 en Safari y Firefox, y S5 (1)).
  - S1 y S3 contestaron que sí en los tres navegadores. Safari, además, reemplaza un destino que tiene un handle abierto (S3 (2)); la capa no lo usa, porque el motor nunca renombra sobre un archivo abierto (DC17).
  - Ninguna operación rechazada tuvo efecto (§1.2.1).
  - El `move()` sobre un archivo existente de Firefox revisó esta decisión: de ahí *Las generaciones*.
  - **Queda sin probar** un rechazo después de un error de E/S, porque no se pudo provocar. Si uno tuviera efecto, en el OPFS ningún `sync_dir` lo haría durable. Por ejemplo, si un `removeEntry` del `.snap` o de `.wal.compacting` se rechaza después de borrar, `delete_room` lo da por hecho al reintentar (para él, un `NotFound` es éxito), y el archivo podría reaparecer después de un corte, en contra de INV-S8:
    - el `.snap`, con la sala que ya se dio por borrada;
    - `.wal.compacting`, sin su `.snap`, que es un estado que no es un prefijo.

    Va al §4 de `INVARIANTES.md`, como riesgo aceptado.
- **Límites:** la réplica vive en la memoria del Worker (en wasm32, hasta 4 GiB, y el navegador puede dar menos), dos veces mientras se instala un snapshot (INV-K15); en wasm32 una reserva que falla deja la instancia inservible, como un pánico (DC42). Sin `persist()`, el navegador puede borrar el OPFS del origen: una pérdida total (§2).
- **Alternativas descartadas:**
  - IndexedDB (DC8);
  - un motor del navegador escrito aparte (DC17);
  - al reabrir, completar desde un temporal entero un destino que falta o no valida. Es la opción que eligió el dueño del proyecto, en la forma en que se formuló. La capa no sabe qué temporal corresponde a qué destino, porque los temporales de los snapshots tienen un nombre al azar. Tampoco sabe si está entero uno de nombre fijo (`.wal.compacting.tmp`, `.meta.tmp`), ni cómo validar cada formato. Además, en Firefox el destino roto puede faltar o tener 0 bytes;
  - borrar el destino antes del `move()`: en Firefox también lo dejó vacío (§1.2.1).

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
  - Con HTTP/2, el stream comparte la conexión de los pedidos.
  - **Un stream que el worker no lee.** El worker no lee el stream mientras aplica un evento (INV-K15), durante un trabajo de CPU largo (DC38), ni mientras Safari suspende su Worker en segundo plano (S5 (6)). Lo que hace el navegador mientras tanto lo midió S5 (4) (§1.2.1):
    - con HTTP/1.1, Chromium frena lo que recibe a los 12 a 15 MB, Firefox recién a unos 4 GiB, y Safari no frena;
    - con HTTP/2, medido solo en Chromium, frena a los 12,6 MB.

    Lo que el navegador junta queda en su propia memoria, fuera de la del Worker y del tope de lo que lee el SDK. El volumen lo pone el servidor, con los eventos de la sala. Va al §4 de `INVARIANTES.md`, como riesgo aceptado.
  - La alternativa, que el dueño del proyecto puede preferir: HTTP/2 entre el navegador y el proxy como supuesto de §2, sin tope.
- **CORS:** el servidor no tiene CORS. La aplicación lo sirve en su mismo origen, por el proxy que termina TLS (§2), o con un proxy que agregue los headers. CORS en el servidor queda fuera de 4.5 (al backlog, si la app real lo necesita). Las pruebas sirven la página y el servidor desde un mismo origen (DC44).
- **Alternativas descartadas:**
  - `reqwest` en wasm32, que envuelve `fetch` sin dejar cortar entre bloques ni leer con un tope; `gloo-net` (§A.1 del plan de la fase), otra capa sin el tope;
  - `XMLHttpRequest` para los pedidos, que en un Worker informa el avance del envío (`upload.onprogress`), pero junta el cuerpo de la respuesta entero (el tope se aplica sobre lo que ya juntó) y sería otra API además de la del stream: queda como alternativa si el plazo que cuenta el envío resulta un problema;
  - `EventSource`, por lo de arriba.

### DC41 · Una pestaña líder por directorio (INV-K13)
- **El lock.** En wasm32, `FileMetadata::open` toma, antes de tocar ningún archivo (también antes de crear su directorio), un Web Lock exclusivo (`navigator.locks.request`) cuyo nombre sale de la ruta normalizada de su directorio del OPFS, y lo conserva hasta su `close` (el de `shutdown`), hasta que se suelta el store o hasta que termina el Worker. La unidad es el directorio: `FileMetadata` retiene `client.lock` mientras está abierto, y en el OPFS su handle es exclusivo, así que a lo sumo una pestaña tiene un cliente sobre ese directorio, con todas sus salas (INV-K13). Los metadatos van en el directorio del motor (DC39), así que el lock cubre los dos. Un lock por sala pediría unos metadatos sin `client.lock` en el navegador: no en la v0.1.
- **La segunda pestaña.** Sin la opción de esperar, abrir el store falla enseguida con `ClientError::InUse`, nuevo (precisa DC32; su código para JavaScript, en 4.6), sin tocar ningún archivo. Con la opción del store del navegador (en su constructor), espera en la cola del lock, y abre cuando la otra pestaña lo suelta: es la que usa el runtime del Worker, que mientras tanto atiende a su página a través de la pestaña que lo tiene, la líder (DC46). Nunca reintenta los archivos (INV-K13), salvo los handles que el Worker que terminó todavía no soltó, después de recibir el lock (abajo, *El orden al terminar*). El contrato de `MetadataStore::open` pasa a decir cómo informa un store tomado: con un error propio, que solo da el store (un `io::Error` que envuelve un tipo del cliente, reconocido con `downcast`; hoy es un `WouldBlock`, que otro paso podría dar), y `Client::open` lo pasa a `InUse`, también en nativo, cuando otro cliente, de este proceso o de otro, tiene `client.lock` (hoy, `Storage`). Para eso, la capa nativa da `None` solo con el error de un lock tomado, comparado por su código (`raw_os_error()`) con el de `fs2::lock_contended_error()` (`EWOULDBLOCK` en Unix, `ERROR_LOCK_VIOLATION` en Windows) y no por su `kind()`, y cualquier otro error al tomar el lock es un error, no `InUse`. En nativo no hay espera.
- **`pagehide`.** La página termina el Worker (en 4.6, su paquete; en 4.5, el arnés de pruebas). Terminarlo es un crash (INV-K11): lo aplicado es durable, y el navegador suelta los handles y el Web Lock. Una página que vuelve del *back/forward cache* arranca otro Worker. Si la que se va es la líder, la reemplaza la primera de la cola (DC46). El `shutdown` ordenado, con el último `Ack` de cada sala (DC28), es para cuando la aplicación cierra el cliente: en `pagehide` no hay tiempo de esperarlo. La página también termina el Worker ante su evento `error`, el de un pánico o de una reserva que falla (DC42).
- **El orden al terminar** (con el paso S1; adoptado el 24 de Noviembre de 2026).
  - **Lo que midió S5 (§1.2.1):**
    - el navegador suelta el Web Lock de un Worker que termina antes que sus handles, en los tres navegadores, y suelta los handles de a poco: la pestaña que recibe el lock encuentra archivos tomados durante 10 a 99 ms;
    - en Chromium, un `move()` que el Worker terminado dejó en vuelo puede aterrizar después de que la otra pestaña recibió el lock, hasta unos 25 ms después; en Firefox y Safari no aterrizó ninguno (80 iteraciones en cada uno);
    - en el seguimiento, en Chromium, ninguno aterrizó después de que la otra pestaña tomó el handle de `client.lock` (160 iteraciones).
  - **Lo que hace la apertura.** Con el Web Lock tomado, ninguna otra instancia viva tiene el directorio. Un handle tomado solo puede ser del Worker que terminó, o de una operación soltada de este mismo Worker (INV-L7), y las dos lo van a soltar. Por eso, en wasm32, durante un plazo de unos segundos desde que recibe el lock:
    - `FileMetadata::open` espera el handle de `client.lock`, y lo vuelve a pedir cada pocos milisegundos. No toca ningún otro archivo hasta tenerlo.
    - Después, ante `RoomLocked` o ante el error *tomado* de la capa (DC39), el cliente vuelve a intentar la apertura entera de una sala y la operación de los metadatos (leerlos, o repetir un guardado), porque el navegador puede seguir soltando los demás handles en cualquier orden. Pasa con los temporales de una compactación o de una instalación cortadas, que la recuperación borra; con las lecturas de la recuperación; y con el temporal de un guardado de los metadatos cortado.

    Si vence el plazo, la apertura del store da `InUse` (y la de una sala, el error que dé). Si la apertura del store falla, el store suelta el Web Lock, para que no se trabe la cola.
  - Son las únicas veces que el SDK reintenta un handle (INV-K13), y resuelven el requisito 8 de §1.3. El Lote 10 lo prueba con los pasos del arnés: con muchos handles abiertos en la página que se va, y con su Worker terminado en medio de una compactación y en medio de un guardado de los metadatos.
  - Que una operación en vuelo termine antes de que el navegador suelte el handle de `client.lock` es un supuesto de §2, con lo que midió el seguimiento en Chromium.
- **Alternativas descartadas:** reintentar los handles sin el lock del directorio (INV-K13 lo prohíbe: compite con la otra pestaña archivo por archivo; con el lock, solo se espera al Worker que terminó); `readwrite-unsafe` (solo Chrome, y rompe la exclusión); solo la primera pestaña con el SDK, y las demás con `InUse` (la primera versión de esta decisión: la segunda pestaña no podía ni leer, y abrir una aplicación en dos pestañas es lo común); un `SharedWorker` (no tiene `FileSystemSyncAccessHandle`, así que igual haría falta una pestaña que abra los archivos).

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
- **La CI:** un job `browser` en `ubuntu-latest`, en cada push y pull request como los demás: compila el wasm y el servidor de pruebas, instala Chromium con Playwright y corre la página, corre los tests unitarios en wasm32, y pasa el fmt y el clippy del arnés. Firefox también, con Playwright, desde el Lote 2 (S1 en Firefox salió bien, §1.2.1). El Firefox de Playwright es un build propio, no el Firefox 160 que midió el paso S1. Safari, en un runner de `macos-latest`, antes de la revisión de cierre (§1.2, el paso S2): abierto con `open -a Safari` y no con `safaridriver`, porque las sesiones de `safaridriver` tienen un almacenamiento efímero (§1.2.1); WebKit con Playwright en Linux, si hace falta, como un motor de más, que no reemplaza a Safari.
- **Lo que no depende del navegador** (el despachador, `rt` en nativo, DEF-189) se prueba en la suite de siempre.
- **Alternativas descartadas:** `wasm-bindgen-test` también para la integración (va por WebDriver: no recarga la página en medio de una prueba ni abre dos pestañas); un proxy en Node delante del servidor, en lugar del servidor de pruebas en Rust, que arma el servidor real en proceso como la simulación.

### DC45 · DEF-189: los metadatos, durables antes de servirse (INV-K16)
- **La regla.** `FileMetadata` hace durables los metadatos de una sala antes de servirlos (`room`) la primera vez desde que se abrió el store, y después de un guardado o un borrado que falló, en cualquier paso: así nada se escribe bajo un guardado que no es durable (INV-K16), también si después de la falla el store se cerró y se volvió a abrir sin un crash, lo que haría olvidarla. Si no puede, la lectura falla (un error, nunca metadatos que faltan, como hoy), así que la sala no abre.
- **En nativo:** sincroniza su directorio (al abrir el store, y antes de servir después de una falla). Vale con los metadatos en cualquier directorio, también fuera del directorio del motor, que §11 de `ARCHITECTURE.md` excluía sin que el código lo impidiera.
- **En el OPFS,** donde sincronizar el directorio no hace nada: un guardado que resolvió es durable por el reemplazo de la capa (DC39, *Las generaciones*). Uno que falló pudo haber tenido efecto sin que sea durable: S5 no vio ningún rechazo con efecto, pero no pudo provocar uno después de un error de E/S (§1.2.1). Por eso, antes de servir, vuelve a guardar lo que lee (temporal, `flush()` y el `rename` de la capa), y repite un borrado que falló. Repetir un borrado cubre solo uno rechazado sin efecto: uno rechazado que borró (`remove_room` toma un `NotFound` como éxito) es el riesgo aceptado de DC39.
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
- **Límites:** cada pestaña carga el wasm del SDK aunque no abra nada (unos 500 KB, S1); un comando de un seguidor suma un salto entre Workers (menos de 1 ms de mediana entre los Workers de dos pestañas, S5 (6)); un relevo deja inciertas las escrituras en vuelo de los seguidores, como un crash las de la líder; el claro de una columna cifrada viaja entre los Workers de las pestañas, que son la aplicación (§2, INV-K9); con una líder congelada en segundo plano, las demás esperan. Safari suspende a los ~4 minutos el Worker de una pestaña en segundo plano sin soltar su Web Lock, y Chromium conserva el lock de una pestaña congelada (S5 (6), §1.2.1).
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
| 7 | DC8 | S1 en Firefox | §1.2.1, respondido en el paso S1 | — |
| 8 | DC17 | S3 en Safari y Firefox | §1.2.1, respondido en el paso S1 | — |
| 9 | DC9 | La medición del codificador `ruzstd` en Safari | §1.2.1, adelantada al paso S1 | — |
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

  En Chromium, sin ventana, con Playwright y un perfil persistente que se corta con `SIGKILL`, sin la Mac; en Safari y Firefox, en la Mac. (Así se planeó; cómo corrió de verdad, en §1.2.1.)
- **La medición del codificador `ruzstd` en Safari** (DC9): el programa del Lote 6 de 4.1 (el `cdylib`, la página que mide y el generador nativo del payload), en Safari en un runner de `macos-latest`. El hardware no es el de DC9, así que se anota cuál es, y los tiempos no se comparan uno a uno. Si ese programa no está, se rearma con lo que describe DC9.

**Cuándo y dónde corren Safari y Firefox:**
- **Paso S1, después del Lote 0 y antes del Lote 2** (la capa del OPFS): S1 en Firefox, S3 en Safari y Firefox, y S5 en Safari y Firefox. Una respuesta negativa reabre DC8 o DC39, o cambia los errores que mapea la capa, así que va antes de escribirla. Puede ir en paralelo con el Lote 1 (el arnés), que no depende de ellos. S5 en Chromium va en el mismo paso. Hecho el 23 de Noviembre de 2026 (§1.2.1).
- **Paso S2, después del Lote 15 y antes de la revisión de cierre:** la página de aceptación del arnés (DC44) en Safari y en Firefox (la medición de `ruzstd` en Safari se adelantó al paso S1, §1.2.1). Es la condición de DC8 para cerrar 4.5.
- **Dónde, sin la Mac** (el dueño del proyecto ya no tiene Safari, y Safari no corre en Linux):
  - Firefox, en Linux: con Playwright en este entorno, si se puede bajar, o en un runner de `ubuntu-latest`, que lo trae instalado.
  - Safari, en un runner de `macos-latest` (al final, abierto con `open -a Safari` y no con `safaridriver`, §1.2.1), desde un workflow en la rama de trabajo de la sesión, que no se mergea (el código de un spike no entra a `main`). En un runner no hay ventanas de nadie, así que también se puede cortar Safari (S1 (2), S5 (1)).
  - WebKit con Playwright en Linux es el mismo motor que Safari, pero no Safari: sirve de motor de más, no lo reemplaza.

### 1.2.1. Las respuestas del paso S1

Se corrieron el 23 de Noviembre de 2026, en tres navegadores:
- **Chromium 153** (Chrome for Testing), con ventana bajo Xvfb, en Ubuntu 24.04, con un Xeon de 2,1 GHz y 4 núcleos. Para congelar una página se usó CDP, el protocolo de depuración de Chrome.
- **Firefox 160**, en `ubuntu-latest` (Ubuntu 26.04), con ventana bajo `xvfb-run`.
- **Safari 27.2**, en `macos-latest` (macOS 26.7, un M1 virtual de 3 núcleos), abierto con `open -a Safari` en la sesión gráfica del runner.

Un corte es un `SIGKILL` del navegador entero, y en Safari también de sus procesos de WebKit; después se reabre el mismo perfil. No es un corte de energía: la caché de páginas del sistema sobrevive. Las pestañas son pestañas reales; en Safari las abre el runner, porque Safari bloquea los pop-ups. El código del spike está en la rama de trabajo de la sesión (`spikes/fase4_5-s1/`, con cinco corridas en los runners, la última del seguimiento), sin mergear.

- **S1 en Firefox.**
  1. Sí. Desde Rust (`wasm-bindgen` 0.2.129 y `web-sys` 0.3.106), en un Worker de módulo, funcionan:
     - escribir y leer en un offset, también una lectura corta al final;
     - `flush`, `truncate`, `getSize` y `close`;
     - reabrir con el mismo handle o con otro, y leer después de recargar la página.

     6,1 MB se escriben en 10 ms y se leen en 4 ms.
  2. Sí. Lo escrito con `flush`, sin `close`, se lee después de `terminate()`, de cerrar la pestaña y de un corte.
  3. Un segundo handle falla enseguida con `NoModificationAllowedError`: en el mismo Worker, desde otra pestaña o en paralelo.
     - Se libera unos 15 ms después de cerrar la pestaña que lo tenía.
     - No hay modos compartidos: la opción `mode` se ignora.
     - `navigator.locks` existe en el Worker.
  4. `persisted()` da `false`, y la cuota es de 10 GiB.
     - `persist()` no resuelve sin el usuario; probablemente queda en un pedido de permiso. Es para 4.6, que lo llama desde la página con un plazo.
     - En el Worker no existe.
  5. El tamaño del wasm no se midió, porque no depende del navegador.

  El criterio (1) de DC8 se cumple en los tres navegadores.
- **Los errores de cada navegador.**
  - **Al abrir un handle con otro handle tomado**, o con un stream de escritura abierto: Chromium y Firefox dan `NoModificationAllowedError`, y Safari, `InvalidStateError`. Safari da `InvalidStateError` solo en ese caso. No lo da:
    - por una entrada borrada o movida;
    - por un directorio padre borrado;
    - ni después de `close()`.
  - **Al abrir o crear una entrada**, en los tres:
    - una que falta da `NotFoundError`;
    - un nombre con `/`, `..` o `.`, o vacío, da `TypeError`;
    - un archivo donde va un directorio da `TypeMismatchError`.

    Crear dentro de un directorio borrado da `NotFoundError` en Chromium y `UnknownError` en Firefox. En Safari, un archivo da `UnknownError`, y un directorio vuelve a crear el directorio borrado.
  - **Al mover:**
    - un origen que falta da `NotFoundError` en Chromium y Firefox, y `UnknownError` en Safari;
    - un directorio de destino que falta da `NotFoundError` en Chromium, y `UnknownError` en Firefox y Safari;
    - un origen con un stream de escritura abierto falla en Chromium y Firefox, pero Safari lo mueve.
  - **Además, en Safari:**
    - solo existe la forma `move(dir, nombre)`;
    - si la entrada de un `FileSystemFileHandle` se borró, o se movió por otro handle, `createSyncAccessHandle` la vuelve a crear vacía.
- **S3 en Safari y Firefox.**
  1. Sí en los dos, también entre directorios. Safari solo con `move(dir, nombre)`.
  2. **Mover un archivo abierto** falla sin cambiar nada: `NoModificationAllowedError` en Firefox, `InvalidStateError` en Safari. **Moverlo sobre un destino abierto** falla en Firefox; Safari, en cambio, lo reemplaza, y el handle abierto sigue leyendo el contenido viejo.
  3. No: `removeEntry` de un archivo abierto falla con `NoModificationAllowedError` y no lo borra.
  4. Sí.

  Todos tienen `move()` de un archivo, y todos reemplazan el destino.
- **S5.**
  1. **Un corte justo después de que la promesa resuelve** deja el cambio entero en los tres navegadores. Se probaron cinco operaciones: un `move()`, crear un archivo, crear un directorio, `removeEntry`, y `flush` sin `close`. Fueron 3 cortes por operación, 15 por navegador. El corte lo hace el servidor, al que el Worker le pide con un `XMLHttpRequest` síncrono, antes de contestarle.
     - **Un corte al azar en medio de una serie:**
       - en Chromium y Safari (40 cortes en cada uno, de los que unos pocos cayeron después de que la serie había terminado), lo que se ve es siempre un prefijo, en el orden en que resolvieron las promesas, y el destino de cada `move()` es siempre un archivo entero, el viejo o el nuevo;
       - **en Firefox, no.** En 12 de 60 cortes, siempre con un `move()` sobre un archivo existente en vuelo, el destino quedó vacío o no estaba, y con él se perdió lo que habían dejado los `move()` que ya habían resuelto. El temporal más nuevo seguía entero, y lo demás (crear y borrar) era un prefijo exacto.
     - **Los rechazos:** ninguno de los que se pudieron provocar tuvo efecto, de 16 a 19 clases en cada navegador (un archivo tomado, una entrada que falta, un nombre inválido, un tipo que no coincide). Uno después de un error de E/S no se pudo provocar.
     - **Un `move()` sobre un directorio vacío:** Firefox lo reemplaza, Safari mueve el archivo adentro, y Chromium da `InvalidModificationError`.
  2. **El Web Lock llega antes de que se suelten los handles, en los tres navegadores, y los handles se sueltan de a poco.** Se terminó la pestaña que tenía el lock y 50 handles, de cuatro formas: `terminate()`, cerrarla, recargarla, y un error seguido de `close()`. Con cada una, la que esperaba encontró handles tomados en estas iteraciones:
     - de 18 a 20 de 20, en Chromium y Firefox;
     - de 7 a 15 de 20, en Safari.

     Al recibir el lock seguían tomados entre 1 y 50 de los 50, y se liberaron de 10 a 99 ms después.
     - **En Chromium, un `move()` en vuelo de un Worker terminado aterriza después de que la otra pestaña tomó el lock**, de 17 a 25 ms después. Pasó en 10 de 20 `terminate()` y en 4 de 20 `close()` del Worker. Al cerrar o recargar la pestaña, nunca. Los `removeEntry` en vuelo ya habían aterrizado siempre. En Firefox y Safari no aterrizó nada después del lock, en 80 iteraciones en cada uno.
  3. Con `ifAvailable`, la segunda pestaña recibe `null` enseguida en los tres, en menos de 10 ms. Sin `ifAvailable`, espera, y recibe el lock de 0 a 6 ms después de que se suelta.
  4. **El cuerpo de un `fetch`:** en los tres, con HTTP/1.1, llega bloque a bloque, cada bloque de 0 a 7 ms después de que se envía. `abort()` rechaza la lectura pendiente con `AbortError` y cierra la conexión. Con HTTP/2, que se midió solo en Chromium, pasa lo mismo, con un `RST_STREAM`.
     - **Si el Worker deja de leer:** Chromium frena lo que recibe, y el servidor se traba a los 12 a 15 MB (con HTTP/2, a los 12,6 MB). Firefox frena recién a unos 4 GiB (4,3 GB). Safari no frena: recibe 4,6 y 6,6 GB en 8 s, hasta que falla el `fetch`.
  5. **Una pestaña en segundo plano, unos 390 s:**
     - en Chromium y Firefox, el `setTimeout` de 30 s del Worker se demora menos de 1 ms, y la página contesta en menos de 2 ms. Chromium frena a uno por minuto los timers de la página, pero no los del Worker.
     - **Safari suspende el Worker dedicado a los ~240 s**, y cuando la pestaña vuelve, sus timers se disparan de 124 a 147 s tarde.
  6. **Entre los Workers de dos pestañas,** por un `BroadcastChannel`, en los tres:
     - los mensajes llegan en orden (se mandaron 5.000, también de 64 KiB);
     - un `CryptoKey` no extraíble llega usable, y sigue sin poder exportarse;
     - una ida y vuelta tarda menos de 1 ms de mediana.

     **El lock, al cerrarse la pestaña que lo tiene:** el primero de la cola lo recibe de 2 a 15 ms después en Chromium y Firefox, y de 24 a 117 ms después en Safari. Un `AbortSignal` saca de la cola un pedido que espera, en los tres.

     **Una pestaña congelada:**
     - en Chromium, congelada con CDP, conserva el lock hasta que se descongela;
     - **Safari suspende la pestaña en segundo plano con el lock tomado, y la que espera no lo recibe** (se esperó 210 s);
     - en Firefox no se vio una suspensión en 6,5 minutos;
     - en Android e iOS no se probó.

     **El prerender:** en Chromium, una página prerenderizada recibe sus Web Locks recién cuando se muestra, de 17 a 31 ms después. Firefox y Safari no prerenderizan.
- **El codificador `ruzstd` en Safari** (DC9). El programa original no estaba, y se rearmó:
  - un `cdylib` sin `wasm-bindgen`, de 266 KB, que llama a `encode_snapshot_envelope`;
  - el generador nativo del payload: el snapshot de `DiskStorageEngine` de 756.000 filas, de 104,3 MB.

  No es el mismo contenido que el de DC9: el `zstd` nativo de nivel 3 lo comprime a 10,3 MB, no a 8,8 MB.

  | | Hardware | Comprimir (6 corridas: 2 cargas de 3) | Resultado | Decodificar el envelope nativo | Decodificar el suyo |
  |---|---|---|---|---|---|
  | Safari 27.2 | el M1 virtual del runner | de 0,59 a 0,67 s (en otro runner, de 0,65 a 0,92 s) | 18,5 MB | de 0,24 a 0,30 s | de 0,18 a 0,19 s |
  | Chromium 153 | un Xeon con la misma descripción que el de DC9 | de 0,99 a 1,16 s | 18,5 MB | de 0,38 a 0,44 s | de 0,27 a 0,32 s |

  El `zstd` nativo decodifica a los mismos bytes lo que comprimió el navegador. Los tiempos se pueden comparar con los de DC9; los tamaños no, porque el payload es otro.
- **`safaridriver`:** sus sesiones tienen un almacenamiento propio y efímero: ven el OPFS vacío, y no ven el de Safari. Por eso la página de aceptación corre en Safari con `open -a Safari` (DC44, paso S2).
- **El seguimiento.** Corrió después de que el dueño del proyecto eligiera no confiar en `move()`, con 20 cortes por serie y dos reaperturas por corte.
  - **En Firefox:**
    - un `move()` de un temporal a un nombre libre no dañó nada en ninguno de los 20 cortes: ni el archivo movido, ni su temporal, ni las generaciones anteriores;
    - borrar el destino antes del `move()` no alcanza: 3 de 20 cortes lo dejaron con 0 bytes;
    - sobre un archivo existente, 7 de 20 cortes dejaron el destino roto, sin el archivo o con 0 bytes. En los 7, el temporal más nuevo estaba entero, de una generación igual o posterior a la del último `move()` resuelto, y nunca se dañó otro archivo.

    En Chromium, como control, los dos primeros casos salieron limpios.
  - **En Chromium, DC41:** la pestaña que recibe el lock pide el handle de `client.lock` cada vez que termina un listado del directorio, y lista el directorio de seguido hasta tenerlo y durante 1 s después. Se probaron 160 iteraciones: cuatro formas de terminar la otra pestaña, con 1 handle y con 50.
    - Ninguna operación en vuelo de la pestaña que terminó aterrizó después de que se tomó el handle.
    - Algunos `move()` aterrizaron después del lock. Las ventanas de listado en que se vieron terminan a los 13 ms del lock como máximo, y una vez a los 191 ms, con el handle a los 191,1 ms.
    - Cerrar los handles antes de `terminate()` no lo evita del todo: pasó en 1 de 20.
    - El handle salió al primer pedido en todas las iteraciones, de 1,8 a 18 ms después del lock, y una vez a los 191 ms. El reintento que propone DC41 no llegó a ejercitarse.
    - Los otros 49 handles no se miraron.
- **Lo que no se midió:**
  - un rechazo después de un error de E/S;
  - HTTP/2 en Firefox y Safari;
  - la congelación en Firefox, y en Android e iOS;
  - en Firefox, un corte entre el `move()` a una generación nueva y el `removeEntry` de la vieja (lo cubre el supuesto de orden de §2);
  - un corte de energía;
  - S1 (5).

**Lo que cambia:**
- **DC39** (adoptada el 24 de Noviembre de 2026). La capa del OPFS reemplaza un archivo con generaciones, sin `move()` sobre uno existente. Usa siempre la forma `move(directorio, nombre)`, toma los errores de cada navegador de esta sección, y la condición queda respondida.
- **DC41** (adoptada el 24 de Noviembre de 2026). Con el lock tomado, la apertura espera durante un plazo los handles que el Worker que terminó todavía no soltó, y si falla, suelta el lock. Resuelve el requisito 8 de §1.3.
- **DC40.** Firefox y Safari no frenan lo que reciben como Chromium; va al §4 de `INVARIANTES.md`.
- **DC44.** Safari corre con `open -a Safari`, y Firefox entra al job `browser` desde el Lote 2, con el Firefox de Playwright.
- **DC45.** Un guardado es durable por las generaciones, y la repetición antes de servir se queda.
- **DC46 y el paso D** reciben la suspensión de Safari con el lock tomado (el requisito 3 de §1.3), el prerender y las latencias entre pestañas.
- **Para 4.6:** en Firefox `persist()` no resuelve sin el usuario, así que la página tiene que llamarlo con un plazo.

### 1.3. Requisitos de diseño de DC46 (para el paso D)

Salieron de las rondas séptima a novena de la revisión cruzada del Lote 0, sobre versiones detalladas de DC46 que no llegaron a verde. El paso D los resuelve, con su revisión cruzada:
1. **Encontrar a la líder.** Una pestaña que se abre después del anuncio de la líder no lo recibe (un `BroadcastChannel` no repite), y dos pueden preguntar a la vez. La versión del protocolo y la configuración (la persona y el servidor) van en la pregunta, la respuesta y el anuncio, con un formato que no cambie entre versiones, para que una líder vieja conteste y su seguidor reciba `InUse` y no `Offline`.
2. **Saber que la líder se fue,** sin que un anuncio viejo engañe: el lock de la pestaña de ella, tomado antes de su primer mensaje, sin número de relevo. El `shutdown` de la líder tiene que soltarlo y no mandar nada después de avisar: el fin de los `watch` que produce cerrar el cliente va por otro canal, sin orden con el aviso. Recibir el lock del directorio termina el seguimiento de la anterior.
3. **Nada se ejecuta después de que su pestaña lo dio por terminado.** Los comandos que un seguidor manda quedan en la cola del Worker de la líder, y una líder ocupada en un trabajo de CPU largo (DC39) o congelada los ejecuta cuando vuelve a correr. Por eso el seguidor da por terminado un comando solo cuando la líder se fue (DC46); un *ping*, si hace falta, solo informa. Una líder congelada deja esperando a las demás: el límite, la condición sobre S5 (6) y la salida posible de terminar el Worker en el evento `freeze`. S5 (6) midió que Safari suspende a los ~4 minutos el Worker de una pestaña en segundo plano, sin soltar su lock (§1.2.1).
4. **Lo que deja una pestaña que se va:** sus suscripciones, sus salas y los comandos suyos que la líder termina después (un `room` o un `watch` que se abre cuando la pestaña ya no está tiene que soltarse).
5. **El relevo:** qué devuelve cada comando en vuelo (una escritura y un `discard`, `OutcomeUnknown`; los que se pueden repetir); el orden de la restauración de cada pestaña (sus claves, sus salas, el modo, sus `watch`) y lo que se manda otra vez después; un `close` o un `unwatch` en vuelo; el modo de una sala, que no puede depender del orden en que restauran las pestañas; un `watch` que no queda colgado ni repite un valor (la confirmación de punta a punta, un valor que no se publica por igual al último, un error repetido); y la líder nueva, que restaura lo de su propia página.
6. **El token:** a qué página se lo pide la líder, con el `TokenProvider` de hoy, que no sabe qué pestaña lo pidió (la renovación es una por sala), y qué pasa si esa página está en segundo plano (S5, pregunta 5).
7. **Las claves:** una que llega tarde, con un `watch` en `Decryption` (DC35: un error no cambia el último valor emitido, así que volver a leer el mismo valor no emite nada); con código nuevo del núcleo para marcar las suscripciones de la sala, o volviendo a suscribir sin bajar el modo `live` (INV-K18). Claves distintas para una sala en pestañas distintas.
8. **La apertura en el relevo:** los handles que todavía tiene el Worker que terminó (el de `client.lock`, y los de las salas y los de los metadatos) quedan resueltos en DC41 con el paso S1: la espera con un plazo, en el Lote 10. DC41 decide además que el store suelta el lock si la apertura falla. Al paso D le queda qué hace la pestaña en el relevo después de ese error: si vuelve a la cola, y qué les dice a sus seguidores.
9. **Los errores:** `Offline` sin líder, frente a DC36 y a la fachada de hoy ("las lecturas nunca son `Offline`", y lleva una sala); `OutcomeUnknown` para un `discard`; `InUse` y el `shutdown` de un seguidor en el enunciado de INV-K2; y los archivos de 4.4 que cambian con eso.
10. **La memoria:** un canal por pestaña para lo que va a una sola (un `scan` grande no se copia en todas); lo que retiene un seguidor; los pedidos de lock que se cancelan con un `AbortSignal`.
11. **Lo que una pestaña le hace a las demás:** `close` por pestaña, que termina sus `watch` en esa sala; `discard` y `set_freshness`, del cliente (DC37, DC36). Hoy (Lote 6a), `close` es de todas las pestañas, como en la fachada, y `crates/client/tests/dispatch_tests.rs` lo fija (`each_reply_goes_to_its_tab_and_a_tab_that_left_closes_only_the_rooms_no_other_opened`, y el `close` de `the_calls_that_open_close_or_discard_a_room_run_in_the_order_they_were_dispatched`): el paso D los cambia, con el `close` por pestaña.

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC38 a DC46 | Ajustes: §2, §4, S1 a S8, L7, W1 y W2 sobre el OPFS, y K1 a K13 y K15 a K17 en el navegador |✅ |
| S1 | Spikes: S1 en Firefox, S3 en Safari y Firefox, S5 | Fuera del repo | DC8, DC17, DC39 a DC41, DC45, DC46 | — | ✅ |
| 1 | El arnés del navegador | `tools/browser` (pruebas) y CI | DC44 | — | ✅ |
| 2 | La capa del OPFS: el directorio | storage | DC39, DC38, DC45 | S1 a S8 y L7 sobre el OPFS | ⬜ |
| 3 | La capa del OPFS: los archivos | storage | DC39, DC44 | S1 a S8 y L7 sobre el OPFS | ⬜ |
| 4 | El motor en disco en wasm32 | storage | DC39, DC38 | S1 a S8, L7, W1 y W2 sobre el OPFS | ⬜ |
| 5 | Los metadatos, durables antes de servirse | client | DC45 | K16 | ⬜ |
| 6a | El despachador del runtime del Worker: los comandos y el despachador | client | DC42, DC46 | K2, K15 | ✅ |
| 6b | El despachador del runtime del Worker: las suscripciones y el token | client | DC42, DC46 | K10, K15 | ⬜ |
| 7 | El runtime del cliente detrás de `rt` | client | DC38 | — (sin cambio de comportamiento) | ✅ |
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

**Tamaño.** Unas 260 líneas de producción el Lote 2 (con las generaciones de DC39), 220 el 3 (la capa nativa, con el mismo contrato, tiene unas 300 sin comentarios), 120 el 4, 60 el 5, 280 el 6 (si pasa de 300, se parte: los comandos y el despachador; las suscripciones y el token; salieron unas 480, y se partió: 299 el 6a), 120 el 7 (mecánico; salieron 161), 200 el 8, 280 el 9, 100 el 10, 150 el 11 y 180 el 12; las del 13 y el 14, las estima el paso D. El arnés del 1 y las pruebas del 15 no son producción.

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
- Ningún archivo del directorio se toca antes de tener el lock de la pestaña (INV-K13), ni, después de recibirlo, antes del handle de `client.lock` (DC41).
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
  - **La CI de `main`,** en verde sobre `27e7320`, con sus nueve jobs. El de `browser` tardó 4 min 40 s en frío; lo que más tardó fue instalar `wasm-bindgen-cli`, 1 min 36 s.

### Paso S1 · Los spikes (fuera del repo) · ✅ Hecho (23 de Noviembre de 2026)
**Qué corrió** (§1.2): S1 en Firefox, S3 en Safari y Firefox, S5 en los tres navegadores y la medición de `ruzstd` en Safari.
- **Dónde:** Firefox en un runner de `ubuntu-latest`, Safari en uno de `macos-latest`, y Chromium en el entorno de la sesión.
- **El código,** en la rama de trabajo de la sesión (`spikes/fase4_5-s1/`), sin mergear.
- **Las respuestas,** en §1.2.1.
- **Un spike de seguimiento,** después de que el dueño del proyecto eligiera no confiar en `move()`:
  - en Firefox, midió el `move()` a un nombre libre y el borrado antes del `move()`;
  - en Chromium, midió si una operación en vuelo aterriza después del handle de `client.lock`.

**Qué cambia.**
- **Revisa** DC39 (las generaciones) y DC41 (la espera de los handles después del lock), que el dueño del proyecto adoptó el 24 de Noviembre de 2026, con la revisión cruzada en verde.
- **Suma notas:**
  - en DC40, DC44, DC45 y DC46 y en §1.3;
  - en los Lotes 2 y 3 (las generaciones, y Firefox en el job `browser`), 4 (la instalación de un snapshot, cortada), 8 y 10 (la espera);
  - en el checklist propio;
  - en el plan de la fase (DC8, DC9, DC17, DC39, DC41, DC44 y DC45, sus pendientes y su avance) y en el de 4.1 (S3).
- **`INVARIANTES.md`, versión 49:** §2, §3.2, INV-K13, INV-K16 y dos filas de §4.
- **La revisión cruzada** de este cambio, que es solo de documentos, la hizo un agente aparte, de contexto fresco y solo lectura, que contrastó los números con los datos crudos y las generaciones con el código del motor. **Salió en verde en su cuarta ronda.**
  1. **Primera ronda: 2 bloqueantes, 10 ajustes y unos 25 menores, corregidos.**
     - **Los bloqueantes:**
       - *Las generaciones* no decían qué da un `rename` cuyo borrado de las viejas falla. Propagar ese error habría sido un rechazo con efecto, en contra de `apply_snapshot` y de INV-S5. Tampoco decían en qué orden borra `remove`. Ahora `rename` da `Ok` una vez resuelto el `move()`, y `remove` borra de la más vieja a la más nueva.
       - Esperar solo `client.lock` no cubría los demás handles, que el navegador suelta de a poco. Ahora la espera, con su plazo, cubre también las salas y los metadatos.
     - **Los ajustes:**
       - la limpieza al abrir el directorio, que se sacó;
       - `create_locked`, `list` y el rename del handle con las generaciones, también en el Lote 3;
       - lo que eligió el dueño del proyecto, ahora precisado, con su forma literal en las alternativas;
       - los números de §1.2.1 y de DC41: de 10 a 99 ms, el reintento que no se ejercitó, y que solo los `move()` aterrizaron tarde;
       - la tabla de errores de `move()`;
       - el caso de `.wal.compacting` en el riesgo aceptado;
       - el Web Lock, que se suelta si la apertura falla;
       - el supuesto de §2, sin enunciarlo de más;
       - los estados del plan de la fase;
       - la fila del stream en §4, y DC40.
  2. **Segunda ronda, acotada a las correcciones: 3 ajustes y 9 menores, corregidos.** Los ajustes:
     - la limpieza al abrir, que todavía estaba en el Lote 2;
     - el error *tomado* de la capa, para que la espera reintente también los temporales, las lecturas de la recuperación y el guardado de los metadatos;
     - cómo se resuelve un nombre: se recorre todo el directorio y se compara por el nombre exacto.
  3. **Tercera ronda: 1 ajuste y 4 menores, corregidos.** El ajuste: el error *tomado* va por operación, porque en Safari `removeEntry` da `NoModificationAllowedError`.
  4. **Cuarta ronda: verde.**

**Entrega:** `docs: the answers of the spikes of Fase 4.5, and the decisions they revise (Fase 4.5, Paso S1)`.

### Lote 2 · La capa del OPFS: el directorio (`zemdb-storage`) · ⬜
**Objetivo:** la parte de `OpfsFiles` que trabaja sobre el directorio, con la tarea que retiene una promesa soltada (DC39, DC38), y el método de la capa de DC45.

**Archivos:** `crates/storage/src/lib.rs` (`files` en wasm32), `files/mod.rs` (`native` solo fuera de wasm32; los traits sin `Send` en wasm32, DC38; y el método que dice si `sync_dir` hace durable un cambio que ya ocurrió, DC45, en la capa nativa, la de prueba y la del OPFS, y en `TestFiles`, de `crates/client/tests/common/`; con el contrato de `sync_dir` reescrito y el error *tomado* en el de `open_read`, `create`, `remove` y `rename`, DC39), `files/opfs.rs` (nuevo), `sys.rs` (lanzar una tarea, DC38), `crates/storage/Cargo.toml` (`web-sys`, `js-sys`, `wasm-bindgen` y `wasm-bindgen-futures`, solo en wasm32), los pasos del arnés, `.github/workflows/ci.yml` (Firefox en el job `browser`) y `ARCHITECTURE.md` §8 (*File Layer*).

**Qué se hace:** la raíz y las rutas, los bindings que faltan, los errores, la tarea de `sys` y las operaciones del directorio de DC39 (`exists`, `list`, `rename`, `remove`, `create_dir_all`, `sync_dir`), sobre archivos sin handles abiertos, con las generaciones de un nombre (DC39). Los errores de cada navegador son los de §1.2.1, con el error *tomado* de `remove` y `rename` (DC39). Firefox entra en el job `browser` (DC44).

**Tests exigidos** (pasos del arnés, sobre la capa real):
- Cada operación del directorio, y su resultado después de recargar.
- Renombrar un archivo con un handle abierto en el origen, o borrarlo, falla con el error *tomado* y sin cambiar nada (S3), con el handle abierto con `web-sys` en el paso. Renombrar reemplaza el destino. Un destino abierto no hace fallar el `rename`, porque el `move()` va a un nombre libre. El motor, de todos modos, nunca renombra sobre un archivo abierto (DC17).
- Las generaciones:
  - un `rename` sobre un nombre que existe no mueve sobre él;
  - el nombre vale su generación más nueva en cada operación, también después de recargar;
  - `list` da los nombres sin el sufijo;
  - un nombre con dos generaciones (las que deja un corte, fabricadas en el paso) vale la más nueva, y el próximo `rename` o `remove` borra la vieja;
  - `remove` borra de la más vieja a la más nueva;
  - un `rename` cuyo borrado de las viejas falla, con una abierta en el paso, da `Ok` con el valor nuevo;
  - el origen de un `rename` con generaciones viejas queda sin ninguna.
- Una ruta con `..`, o fuera de la raíz, se rechaza sin tocar nada.
- Una operación que es una promesa, soltada: termina, y la siguiente sobre el mismo archivo la espera.
- Un `NotFound` y la cuota llena salen con su `ErrorKind`.

**Interacciones a revisar:** los requisitos de DC17 (§1 del plan de 4.1); lo que el motor da por hecho del directorio (la instalación de snapshots, los borrados de `delete_room`, la búsqueda de temporales); la capa nativa y las de prueba, que no cambian de comportamiento.

**Documentos:** `ARCHITECTURE.md` §8; `INVARIANTES.md`: el estado de L7 sobre el OPFS.

**Entrega:** `feat(storage): the directory of the file layer over the Origin Private File System (Fase 4.5, Lote 2)`.

### Lote 3 · La capa del OPFS: los archivos (`zemdb-storage`) · ⬜
**Objetivo:** el resto de `OpfsFiles`: abrir y crear con su lock, la E/S posicional, renombrar el archivo abierto y el handle muerto (DC39).

**Archivos:** `crates/storage/src/files/opfs.rs` y sus tests unitarios en wasm32; lo que esos tests necesitan, porque los de hoy no compilan en wasm32 (usan `#[tokio::test]`, `zstd`, el motor en disco y los *fail points*): los módulos de tests nativos de `crates/storage/src/` fuera de wasm32, `wasm-bindgen-test` en `crates/storage/Cargo.toml` (solo para los tests en wasm32), el runner (`.cargo/config.toml`) y el comando de la CI (`cargo test --target wasm32-unknown-unknown -p zemdb-storage --lib`); los pasos del arnés y `ARCHITECTURE.md` §8.

**Qué se hace:** `open_locked`, `create_locked`, `create` y `open_read`, con un `FileSystemSyncAccessHandle` cada uno y sobre la generación más nueva de su nombre (`create_locked`, `AlreadyExists` si existe alguna; `open_read` y `create`, el error *tomado* de DC39 con otro handle sobre el archivo); `read_at`, `write_at`, `size`, `set_len`, `sync_data` y `sync_all`; renombrar el archivo abierto, con las generaciones del destino (DC39); el handle muerto.

**Tests exigidos:**
- **Pasos del arnés:** leer y escribir en un offset, el largo, truncar, `flush`, y lo escrito después de recargar; `open_locked` y `create_locked` dan `None` con otro handle sobre el archivo, también desde un Worker de otra página; `create_locked` sobre uno que existe es `AlreadyExists` y no lo trunca; renombrar el archivo abierto lo sigue, también sobre un nombre que existe (a su generación siguiente), y el handle sigue sirviendo; cada apertura usa la generación más nueva de su nombre, y `create_locked` da `AlreadyExists` con cualquier generación; `open_read` y `create` dan el error *tomado* con otro handle sobre el archivo; `read_at` más allá del fin es `UnexpectedEof`, nunca una lectura corta.
- **Tests unitarios en wasm32:** el handle muerto (la reapertura después del `move()` que falla, forzada con un mecanismo de `opfs.rs` bajo `cfg(test)`: los *fail points* compilan en wasm32 recién en el Lote 4): toda operación siguiente falla, y no escribe en ningún archivo.

**Interacciones a revisar:** los requisitos de DC17; la búsqueda de frames y el replay, que no admiten una lectura corta; la rotación y su vuelta atrás.

**Documentos:** `ARCHITECTURE.md` §8; `INVARIANTES.md`: el estado de L7 sobre el OPFS.

**Entrega:** `feat(storage): the files of the file layer over the Origin Private File System (Fase 4.5, Lote 3)`.

### Lote 4 · El motor en disco en wasm32 (`zemdb-storage`) · ⬜
**Objetivo:** `DiskStorageEngine<OpfsFiles>` en wasm32, sin cambiar nada en nativo (DC38, DC39).

**Archivos:** `crates/storage/src/sys.rs` (el azar), `disk/mod.rs` (las tareas, y `zstd_level` y su validación), `disk/compactor.rs` (los nombres temporales), `disk/recovery.rs`, `lib.rs` (el motor en disco y los *fail points* en wasm32), `fail_point.rs`, `disk/tests` (fuera de wasm32), `crates/storage/Cargo.toml` (`getrandom`, o la fuente de `uuid`, en wasm32), el `clippy.toml` del storage (el lint de DC38, también en nativo fuera de los tests unitarios, como en el cliente desde el Lote 7), `disk/tests/opfs_like.rs` (DEF-184 y DEF-185), los pasos del arnés y `ARCHITECTURE.md` §8.

**Qué se hace:** las tareas del motor (`open_room`, `apply_snapshot`, `delete_room`, `compact_room` y la compactación automática) con la tarea de `sys`; el motor, la recuperación y la compactación compilados en wasm32; el `.snap` con `ruzstd` `Fastest`; los nombres temporales al azar; el módulo de los *fail points*, también en wasm32 (DC39); el lint del storage. En la capa de prueba nativa, el segundo handle rechazado (DEF-184) y los dos tests del handle muerto de la revisión de cierre de 4.1 (DEF-185).

**Tests exigidos:**
- **En el navegador:** una sala que aplica lotes, se recarga la página y reabre con todo lo aplicado; el Worker terminado en medio de un lote, y la sala que reabre en un prefijo del log; una compactación, también terminada en el medio, y la sala que reabre igual; `apply_snapshot` de un snapshot nativo (Zstd) y de uno propio (`ruzstd`), también después de recargar; `delete_room`, también terminado en el medio; el Worker terminado en medio de la instalación de un snapshot, y la sala que reabre con el viejo o con el nuevo; una segunda página que abre la misma sala recibe `RoomLocked`.
- **En nativo:** la suite del storage entera, sin cambios; DEF-184 y DEF-185 en la capa de prueba.

**Interacciones a revisar:** INV-S1, S3, S4, S5 y S8, cuyos mecanismos (las tareas propias, el lock del WAL) cambian de runtime en wasm32; INV-L7 (las tareas del motor retienen los locks, D86 y D89); el decodificador y el codificador `ruzstd` (INV-W1, W2); la compactación automática, que se lanza sin que nadie la espere; un rechazo con efecto en el OPFS (el riesgo aceptado de DC39); las generaciones bajo la instalación de un snapshot, la rotación y su vuelta atrás, y la limpieza de temporales de la recuperación.

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

### Lote 6 · El despachador del runtime del Worker (`zemdb-client`) · partido en 6a (hecho) y 6b (⬜)
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

**Partido.** Entero, con las suscripciones y el token, salía de unas 480 líneas de producción (sin comentarios ni líneas en blanco), así que se parte como dice §2 (*Tamaño*):
- **6a, los comandos y el despachador:** los comandos del cliente, de las salas y de las tablas, con su configuración y el id de la pestaña; las respuestas; la tarea por comando, con el turno de las llamadas que abren, cierran o descartan una sala; el ciclo de vida; y el aviso de que una pestaña se fue, que suelta sus salas. Sus tests exigidos: el primero, el segundo, el tercero, el de `shutdown`, el de dos pestañas en lo que es de las respuestas y las salas, y la ida y vuelta con `bincode` de los comandos y las respuestas.
- **6b, las suscripciones y el token:** `watch`, `watch_scan`, `unwatch` y la confirmación de un valor, con los avisos de sus valores y de su fin; el `TokenProvider` que le pide el token a la página, con su aviso y su respuesta; y lo que el aviso de que una pestaña se fue termina de eso. Sus tests exigidos: el de `watch`, el del token, el de dos pestañas en lo que es de los valores de `watch` y de las suscripciones que termina el aviso, y la ida y vuelta de los avisos. Unas 200 líneas.

#### Lote 6a · Los comandos y el despachador · ✅ Hecho (24 de Noviembre de 2026)
**Hecho** (24 de Noviembre de 2026).
- **Qué entró.**
  - **El módulo `dispatch`** (`crates/client/src/dispatch/`), público, sin depender del navegador:
    - `command.rs`: `DispatchConfig` (el `ClientConfig`, la URL del servidor, el directorio del cliente, `idle_timeout`, `max_streams` y la opción de esperar la pestaña), `Command` (la pestaña, el id y su `Request`), `Request` (`Open`, `ClientId`, `Shutdown`, `Room` con un `RoomCall`, `Table` con un `TableCall`, y `Left`), `Answer`, `Reply` y `Message`, que en el 6a tiene solo las respuestas. Todos con `serde`.
    - `mod.rs`: el trait `Platform`, que abre la fachada sobre lo que nombra la configuración (en el 6a, con el `TokenProvider` de la plataforma); y `Dispatcher`, con `dispatch(Command)`, que corre cada comando que llega al cliente en una tarea propia de Tokio y publica su `Reply` con la función que recibe. Los que no llegan al cliente (`ClientId`, y uno de una sala antes de abrir o después de apagar) responden en el momento.
    - **El turno de una sala:** las llamadas que la abren, la cierran o la descartan, y los cierres del aviso de una pestaña que se fue, corren de a una por sala, en el orden en que se despacharon: cada una toma, al despacharse, el fin de la anterior de su sala, y lo espera. Las pestañas de cada sala cambian solo en su turno. Las lecturas, las escrituras y las demás llamadas no esperan. El aviso toma el turno de las salas que tiene la pestaña y de las que tienen un turno en vuelo (que puede ser de la pestaña), y espera cada uno por su cuenta, así que no acopla las salas: una llamada lenta de una sala no frena las de otras.
  - **La fachada:** `ZemdbClient::handle(room_id)`, `pub(crate)`: el `RoomHandle` de una sala por su id sin abrirla, que alcanza la sala como el cliente la tiene en cada llamada (como uno de `room`). `room` es `ZemdbClient::room`, y cada otra llamada de una sala o de una tabla es una llamada sobre ese handle.
  - **`ClientConfig`,** serializable, para ir en `DispatchConfig`: `ZemdbClient::open` lo verifica. **`ClientError::Config`** dice también que es el error de un comando que el estado del cliente del despachador no deja.
  - **299 líneas de producción**, sin comentarios, líneas en blanco ni la declaración del módulo de tests unitarios (3 líneas): 81 en `command.rs`, 213 en `mod.rs` y 5 en `lib.rs`, `config.rs` y `api/client.rs`.
- **Tests** (`crates/client/tests/dispatch_tests.rs`), con un `Platform` de prueba (la fachada sobre el motor en disco, envuelto en el de prueba que falla a pedido, el servidor real en proceso y el transporte de prueba, con los heartbeats sin respuesta) y una página de prueba que recibe las respuestas:
  - `every_command_gives_what_its_call_of_the_facade_gives` (el primer test exigido): un guion de 36 comandos (abrir, sincronizar, `status`, lecturas, escrituras con el pre-chequeo, errores de validación, `set_freshness` de una sala cerrada, `close`, `discard`, abrir de nuevo, una sala que el servidor no tiene, `shutdown` y lo que viene después), por el despachador sobre un servidor y por la fachada sobre otro igual: da lo mismo y manda lo mismo, en el mismo orden, comando por comando.
  - `a_write_that_waits_for_its_commit_ack_does_not_hold_back_a_read_of_its_room` (el segundo): con la respuesta del commit retenida, una lectura y un `status` que salen después responden antes, con el id de su comando y a su pestaña, y el `status` cuenta la escritura en vuelo.
  - `a_write_is_one_call_of_the_facade_with_one_mutation_id_also_with_retries` (el tercero): con la respuesta del primer intento perdida, dos intentos con el mismo `MutationId` y una secuencia; con el segundo rechazado para siempre, `OutcomeUnknown`, y nada sale después; un `update` y un `delete`, un commit cada uno.
  - `after_shutdown_every_command_on_a_room_is_room_closed` (el de `shutdown`): cada comando de una sala, abierta o no, es `RoomClosed` sin mandar nada, empezando por un `set_freshness`, que la fachada toma también apagada; la escritura en vuelo termina como en la fachada; `Open` es `Config`, `ClientId` sigue dando el id, y un segundo `Shutdown` es el de la fachada. Además, `a_shutdown_replies_once_the_client_shut_down` (con el último `Ack` del worker retenido, no responde hasta soltarlo) y `a_shutdown_before_the_open_is_done_and_no_open_follows_it`.
  - `each_reply_goes_to_its_tab_and_a_tab_that_left_closes_only_the_rooms_no_other_opened` (el de dos pestañas, en lo del 6a): cada respuesta va a la pestaña de su comando; el aviso de una pestaña que se fue cierra las salas que solo ella abrió, y no las que abrió también otra; una sala que una pestaña cerró o descartó, y otra abrió de nuevo, es solo de la segunda, se vaya primero cualquiera de las dos.
  - `every_command_and_reply_goes_through_bincode_unchanged` (la ida y vuelta): cada `Request`, cada `Answer` y cada `ClientError`, con valores al azar de cada tipo, con `bincode` de enteros fijos y variables: lo decodificado es igual y vuelve a dar los mismos bytes (un `Float` conserva sus bits). `the_values_at_random_hold_the_edges_of_each_type` comprueba que el generador da un `NaN` con payload, `-0.0` y los extremos de `i64`.
  - **El turno de una sala:** `a_tab_that_left_does_not_close_a_room_that_another_tab_opens_after_it` y su versión con varios hilos (`..._with_threads`), de la primera ronda de la revisión de delta: con el cierre de la pestaña que se fue retenido en el último `Ack` del worker, el `room` de otra pestaña espera ese cierre, y la sala queda abierta y se lee; `the_calls_that_open_close_or_discard_a_room_run_in_the_order_they_were_dispatched`, con varios hilos, diez veces: un `room` y un `close` o un `discard` mandados juntos dejan la sala cerrada y sin pestañas (la que la abre después es la única), y un `room` con el aviso de que su pestaña se fue, también; `a_tab_that_left_closes_a_room_that_an_open_of_its_own_still_in_flight_opens`, en una sala que no tenía pestañas; `a_tab_that_left_closes_its_room_before_an_open_sent_right_after_it` (el aviso y un `room` de otra pestaña mandados juntos: el aviso cierra la sala y el `room` la abre otra vez, en ese orden), de la tercera ronda; `a_slow_open_does_not_hold_back_the_calls_of_other_rooms_through_a_tab_that_left`, de la segunda ronda (con un `room` lento, el aviso de una pestaña que no abrió nada no frena los `room` y `close` de otras ocho salas); `a_tab_that_left_releases_each_of_its_rooms_without_waiting_for_a_slow_one` (cinco rondas, para que en alguna la sala lenta venga antes que las otras: el aviso las suelta sin esperarla); `two_tabs_that_leave_together_leave_their_rooms_to_the_tab_that_opens_them_next`, con 40 salas y varios hilos; y `the_reads_writes_and_other_calls_of_a_room_do_not_wait_for_its_close`. En `crates/client/src/tests/dispatch.rs`: `a_room_keeps_its_turn_only_while_a_call_of_it_is_in_flight` y `a_room_leaves_the_tabs_of_the_rooms_once_no_tab_holds_it`.
  - **El aviso de una pestaña que se fue:** `a_tab_that_left_closes_its_rooms_and_does_not_discard_them` (sin red, la sala abre otra vez desde sus metadatos, con sus filas y su época) `a_tab_that_left_reports_a_close_that_failed_and_closes_the_others` (uno de los cierres que fallan es su respuesta, y las demás salas cierran igual; una sala que el cliente cerró por otro lado no es un error) y `an_open_that_failed_does_not_count_its_tab`.
  - Además: `the_client_opens_once_and_an_open_that_failed_may_be_tried_again` (antes de abrir, `RoomClosed`, `ClientId` es `Config` y `Left` es `Done`; una apertura que falla deja abrir otra; de dos a la vez, la segunda es `Config`), `a_shutdown_that_comes_while_the_client_opens_shuts_down_the_client_it_opens` (el `shutdown` espera a la apertura en vuelo y apaga el cliente que abre, que suelta el directorio) y `a_command_that_panics_has_no_reply_and_the_dispatcher_goes_on`.
- **Mutaciones inversas** (en una copia, cada una con algún test que falla): repetir una escritura incierta; correr los comandos de a uno; responder a otra pestaña; llamar a la fachada después de `shutdown`; que el aviso cierre las salas de otras pestañas, o ninguna; que `shutdown` no espere a la apertura; no rechazar una segunda apertura; que `close` y `discard` no suelten las pestañas de la sala; que abrir una sala no cuente su pestaña; `discard` como `close`; un campo de la configuración sin serializar; marcar el apagado en la tarea; no dejar reintentar una apertura fallida; no llamar a `set_freshness`; que un turno no espere al anterior; que el aviso no tome el turno de la sala; `discard` fuera del turno; que una apertura fallida cuente su pestaña; que el aviso espere sus salas de a una; que queden los turnos que terminaron, o los conjuntos vacíos de pestañas; que el aviso pare en el primer error; y que una llamada tome su turno en su tarea y no al despacharse. De la revisión de delta, las que habían sobrevivido: en la primera ronda, que el aviso descarte en lugar de cerrar; abrir después de un `shutdown` sin cliente; que `shutdown` responda antes del de la fachada; un `status` que sincroniza; un `shutdown` sin cliente que falla; y que el aviso informe `RoomClosed`; en la segunda, que el aviso no tome el turno de una sala sin pestañas; que las demás llamadas esperen el turno; y un aviso antes de abrir que falla; en la tercera, que el aviso tome sus turnos en su tarea y no al despacharse.
- **Interacciones revisadas.**
  - **La fachada con el worker (DC36, INV-K18):** `room` arranca el worker en el modo de la sala, y `set_freshness`, también de una sala cerrada, vale para su próxima apertura: el guion lo compara con la fachada por el `status` (el modo del worker). Lo que depende de los `watch` es del 6b.
  - **El descarte a pedido con escrituras en vuelo (DC37):** `a_discard_ends_a_write_of_the_dispatcher_in_flight_as_the_facade_ends_it`, con el `CommitAck` que llega después: `OutcomeUnknown`, y la escritura llegó una vez al log. Las suscripciones, en el 6b.
  - **El cierre del cliente con tareas de comandos en vuelo (INV-K11):** en el test de `shutdown`, una escritura en vuelo termina como en la fachada, y los comandos que llegan después no tocan el cliente; una apertura en vuelo, en `a_shutdown_that_comes_while_the_client_opens_shuts_down_the_client_it_opens`; y el último `Ack` del worker, que el `shutdown` espera antes de responder.
  - **El cierre de una sala con la apertura de otra pestaña** (de la primera ronda de la revisión de delta): el turno de la sala. **Una llamada lenta de una sala con las de otras** (de la segunda): el aviso espera cada sala por su cuenta.
- **Desvíos y precisiones.**
  - **Partido** en 6a y 6b (arriba).
  - **`config.rs`:** el plan decía que el Lote 6 no lo tocaba, porque lo cambiaba el Lote 11c de 4.3, ya cerrado. `ClientConfig` pasa a ser serializable y va entero en `DispatchConfig`, en lugar de una copia de sus campos.
  - **Los errores sin sala.** `RoomClosed` lleva una sala, así que "después de `shutdown`, todo comando es `RoomClosed`" vale para los comandos de una sala, antes de abrir y después de apagar. `ClientId` antes de abrir y `Open` repetido son `Config` (sin un error nuevo, que precisaría DC32; su docstring lo dice); después de apagar, `ClientId` da el id, como la fachada, y `Shutdown` es el de la fachada otra vez. Una apertura que falla deja abrir otra: "el cliente se abre una vez" cuenta las que salen bien.
  - **El orden de las llamadas de una sala que la abren, la cierran o la descartan:** DC42 dice que dos comandos en vuelo no tienen orden entre sí; para esas, y para los cierres del aviso de una pestaña que se fue, el orden es el del despacho, por sala, para que el aviso no cierre una sala que otra pestaña abre después (la revisión de delta). El turno es un `oneshot` por llamada, encadenado al despacharla; una sala guarda el fin del último solo mientras tiene uno en vuelo (los que terminaron salen con la llamada siguiente que toma un turno), y sale de las pestañas de las salas cuando ninguna la tiene.
  - **El aviso de que una pestaña se fue** es un `Request::Left` cuya pestaña es la que se fue (quién lo manda es del paso D). "Suelta sus salas" es: cierra las que abrió solo ella, en el turno de cada sala, también una que abría un `room` suyo en vuelo; `close` y `discard` son de todas las pestañas, como en la fachada, y sueltan la sala de todas. Lo que hace otro comando de la pestaña que termina después del aviso es el requisito 4 de §1.3, del paso D.
  - **Un pánico:** un comando que entra en pánico termina sin respuesta, en nativo (lo dice el docstring de `Dispatcher`), y los turnos que tomó o esperaba terminan con él; en wasm32, el *panic hook* del Lote 12 cierra el Worker. Atraparlo pedía un error nuevo para la respuesta.
  - **Cada comando tiene su respuesta,** también `Left`, salvo uno que entra en pánico (arriba).
  - **La clave de una sala** (DC43) no entra: el `CryptoKey` va al lado del mensaje, y su `CryptoEngine` es del Lote 11; su comando, en el 12.
  - **Lo que valida lo deserializado:** `RoomId`, al decodificarse; filas, claves y scans, la fachada contra el esquema; `ClientConfig`, `ZemdbClient::open`; el directorio, la URL y las opciones del transporte y del store, la plataforma (Lotes 9, 10 y 12).
  - **Los tests unitarios** (`crates/client/src/tests/dispatch.rs`) miran lo que retiene el despachador, que no se ve desde la API pública. Una apertura que termina después del `shutdown` no cuenta su pestaña, lo que no se ve desde afuera: después del apagado, ningún comando la usa.
- **La revisión de delta** la hizo un agente aparte, de contexto fresco y solo lectura, que probó en su propia copia con sus propias mutaciones. Corrió los tests del despachador más de 130 veces por ronda, también bajo carga. **Salió en verde en su tercera ronda.**
  1. **Primera ronda: ningún Alto, 2 Medios y 5 Bajos, corregidos.**
     - **Los Medios:**
       - Un `Left` cerraba una sala que otra pestaña estaba abriendo, porque la soltaba al despacharse y la cerraba después. Se reprodujo reteniendo el último `Ack`. De ahí sale el turno de cada sala.
       - Ningún test distinguía que `Left` cierra y no descarta (INV-K3). Ahora la sala vuelve a abrir sin red, con sus filas.
     - **Los Bajos:**
       - un comando que entra en pánico queda sin respuesta (ahora está documentado);
       - entradas viejas en el registro de pestañas, que el turno resuelve;
       - tests para las mutaciones que sobrevivían: un `Open` después de `shutdown`, un `shutdown` sin cliente, el `shutdown` que responde después de la fachada, lo que manda cada transporte y los errores de `Left`;
       - el docstring y los mensajes de `Config`;
       - dos imprecisiones de `ARCHITECTURE.md`.
  2. **Segunda ronda, acotada a los arreglos: 1 Medio y 3 Bajos, corregidos.**
     - **El Medio lo trajo el turno.** Un `Left` tomaba el turno de todas las salas y los esperaba de a uno, así que una llamada lenta de una sala frenaba el `room`, el `close` y el `discard` de las demás. Ahora espera cada sala por separado, y solo las suyas y las que tienen un turno en vuelo.
     - **Los Bajos:**
       - tests para un `room` en vuelo de la pestaña que se va, para las lecturas y las escrituras durante un `close`, y para un `Left` antes de abrir;
       - `turns` y `rooms`, que crecían y ahora están acotados;
       - los docs del pánico.
  3. **Tercera ronda: verde, con 2 Bajos, corregidos:**
     - un test de que `Left` toma sus turnos al despacharse;
     - "el primero" de los `close` que fallaron, que es uno cualquiera.

  Quedan sin test dos cosas que no se ven desde la API: el `rooms.clear()` de `shutdown`, y una espera de más de `Left` en las salas que tiene alguna pestaña, que no rompe nada.
- **Chequeos:**
  - fmt, clippy del workspace y de wasm32 con `-D warnings`, doc con `-D warnings`, y el build a wasm32;
  - la suite del workspace, con 1674 tests y sin fallas;
  - los `--ignored` del cliente en release, con 7 tests y sin fallas;
  - los tests del despachador, más de 130 veces por ronda de la revisión, también bajo carga.

  En dos corridas de la suite del cliente, hechas mientras corrían las mutaciones, fallaron una vez dos tests del transporte que el lote no toca: uno depende de una pausa de 1,5 s, y el otro, de que nadie tome un puerto recién soltado. Los dos pasaron solos, y en la suite entera sin otra carga.
- **La CI de `main`,** en verde sobre `2dab234`.

#### Lote 6b · Las suscripciones y el token (`zemdb-client`) · ⬜
**Objetivo:** lo que queda de DC42 en el despachador: `watch`, con un valor en vuelo por suscripción, y el token que el Worker le pide a la página.

**Archivos:** `crates/client/src/dispatch/` (las suscripciones y el `TokenProvider` de la página), sus tests (`crates/client/tests/dispatch_tests.rs`) y `ARCHITECTURE.md` §11.

**Qué se hace:** `Request::Watch` (`watch` y `watch_scan`), `Unwatch`, `Confirm` y `Token`; los avisos (`Notice`: un valor, el fin de una suscripción y el pedido de un token) en `Message`; una tarea por suscripción, que no lee el valor siguiente de su stream hasta la confirmación del anterior; el `TokenProvider` que publica el pedido y espera la respuesta, con el plazo de 60 s del cliente, y que `Platform::open` recibe; y, con el aviso de que una pestaña se fue, el fin de sus suscripciones y de los pedidos de token a ella.

**Tests exigidos:** el de `watch`, el del token, el de dos pestañas en lo de los valores de `watch` y las suscripciones que termina el aviso, y la ida y vuelta de los avisos (los del Lote 6); además, el modo del worker que sale de las suscripciones del despachador (INV-K18) y una suscripción que termina con un descarte a pedido (DC37).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K10 y K15 en el despachador.

**Entrega:** `feat(client): the subscriptions and the tokens of the dispatcher of the browser worker (Fase 4.5, Lote 6b)`.

### Lote 7 · El runtime del cliente detrás de `rt` (`zemdb-client`) · ✅ Hecho (24 de Noviembre de 2026)
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

**Hecho** (24 de Noviembre de 2026).
- **Qué entró.**
  - **El módulo `rt`** (`crates/client/src/rt.rs`), interno, con todo lo que el cliente tomaba de Tokio y del sistema. En nativo cada función es la de Tokio o la del sistema, sin nada en el medio:
    - **las tareas:** `spawn`, que da un `Task`, el handle de una tarea que corre hasta su fin aunque nadie la espere (soltarlo la deja seguir), con `abort` (el corte, en su próximo `.await`) e `is_finished`. Esperarlo da lo que dio la tarea, o un `TaskError` si entró en pánico o se cortó, con el mismo texto que el `JoinError` de Tokio;
    - **el trabajo de CPU:** `spawn_blocking`, que también da un `Task` (en nativo, en el pool bloqueante de Tokio);
    - **los plazos y el reloj:** `now`, el instante monotónico, de tipo `rt::Instant` (en nativo, el de Tokio, que controla el reloj pausado); `sleep_until`; `sleep`; y `timeout` y `timeout_at`, que dan un `Elapsed` propio. `sleep` y `timeout` cuentan desde la llamada, como los de Tokio;
    - **el azar:** `random_uuid`, un UUID versión 4 como número, como se sacaban los `MutationId`, el id de dispositivo y el *jitter*;
    - **la hora de pared:** `wall_millis`, en milisegundos desde 1970, con el 0 y el `u64::MAX` de antes;
    - **las cotas de una tarea:** `MaybeSend`, que en nativo es `Send`.
  - **La forma, pensada para el Lote 8.** Fuera de `rt`, nadie nombra un tipo del runtime de Tokio:
    - `Task`, `TaskError` y `Elapsed` son tipos propios, que en wasm32 pueden ser un `spawn_local` con su corte, y `setTimeout`;
    - `rt::Instant` se usa solo con `+`, `checked_add`, `saturating_duration_since` y las comparaciones, que un tipo propio sobre `performance.now()` puede dar;
    - las tareas, el trabajo de CPU y lo que dan piden `MaybeSend`, que en wasm32 no pedirá nada.

    La parte de wasm32 no entra: el crate sigue vacío en wasm32 hasta el Lote 8.
  - **Cada uso migrado** (fuera de los tests):

    | Archivo | Antes | Ahora |
    |---|---|---|
    | `client.rs` | `tokio::spawn` (la tarea del aplicador, en `change`, con sus cotas `Send`) | `rt::spawn`, con `MaybeSend` |
    | | `spawn_blocking` ×2 (el hash de un snapshot bajado y el de uno subido) | `rt::spawn_blocking` |
    | | `timeout` ×2 (el token, en `renew` y `first_token`); `timeout_at` ×4, `sleep_until` ×2 y `sleep` ×1 (la escritura); `sleep` ×1 (los reintentos de un chunk) | `rt::timeout`, `rt::timeout_at`, `rt::sleep_until` y `rt::sleep` |
    | | `Instant::now` ×5 (el plazo de la escritura, su chequeo, sus dos backoffs y `upload_failed`); `elapsed` ×1 (`uploads`) | `rt::now`; `rt::now().saturating_duration_since(..)`, la cuenta del `elapsed` de Tokio |
    | | `Uuid::new_v4` ×2 (`jitter` y el `MutationId`) | `rt::random_uuid` |
    | `worker.rs` | `tokio::spawn` ×5 (la tarea del worker, la aplicación de un evento, la transferencia, el sync y la apertura del stream) y sus 13 `JoinHandle` | `rt::spawn` y `rt::Task` |
    | | `sleep_until` ×2, `timeout` ×1 (`LAST_ACK`) e `Instant::now` ×7 | `rt::sleep_until`, `rt::timeout` y `rt::now` |
    | `events.rs` | `timeout_at` ×1 (`SILENCE`) e `Instant::now` ×2 | `rt::timeout_at` y `rt::now` |
    | `metadata.rs` | `tokio::spawn` ×1 (`FileMetadata::run`, con sus cotas `Send`) y `Uuid::new_v4` ×2 (el id de dispositivo) | `rt::spawn`, con `MaybeSend`, y `rt::random_uuid` |
    | `http_transport.rs` | `tokio::spawn` ×1 y `spawn_blocking` ×1 (el reemplazo del cliente HTTP), `timeout` ×3, `Instant::now` ×2 y `elapsed` ×1 (`Progress`) | `rt::spawn`, `rt::spawn_blocking`, `rt::timeout`, `rt::now` y `rt::now().saturating_duration_since(..)` |
    | `api/write.rs` | `SystemTime::now` (el `timestamp` de una operación) | `rt::wall_millis` |
    | `dispatch/mod.rs` | `tokio::spawn` ×1 (`Shared::spawn`) | `rt::spawn` (el cambio mínimo: sus cotas siguen en `Send`) |

    En total: 9 `tokio::spawn`, 3 `spawn_blocking`, 6 `timeout`, 5 `timeout_at`, 4 `sleep_until`, 2 `sleep`, 16 `Instant::now`, 2 `elapsed`, 4 `Uuid::new_v4` y 1 `SystemTime::now`. Quedan `tokio::sync` y `select!`, que DC38 deja.
  - **El lint** (`crates/client/clippy.toml`): `clippy::disallowed_methods`, con 17 entradas, cada una con su razón y su reemplazo en `rt`. Son `tokio::task::spawn` (que es también `tokio::spawn`), `spawn_blocking`, `sleep`, `sleep_until`, `timeout`, `timeout_at`, `interval`, `interval_at`, `Instant::now` y `elapsed` de Tokio y de `std`, `SystemTime::now` y `elapsed`, `Uuid::new_v4`, y `std::thread::spawn` y `std::thread::sleep`. Vale para todo el código del crate, también en nativo: `lib.rs` lo permite solo en sus tests unitarios (`cfg_attr(test, ..)`), y `rt.rs` siempre (`#![allow]`).
  - **161 líneas de producción**, sin comentarios, líneas en blanco ni la declaración del módulo de tests unitarios (3 líneas): 84 en `rt.rs`, y 77 que cambian en los demás archivos, por 87 que salen. Son 24 por 28 en `client.rs`, 28 por 34 en `worker.rs`, 9 por 9 en `http_transport.rs`, 6 por 5 en `metadata.rs`, 4 por 4 en `events.rs`, 2 por 5 en `api/write.rs`, 2 por 2 en `dispatch/mod.rs` y 2 en `lib.rs`. El plan estimaba 120. Aparte, `clippy.toml` (17 entradas) y una línea en cada test de integración y en el ejemplo (en los desvíos).
- **Tests** (`crates/client/src/tests/rt.rs`, 14), unitarios porque `rt` es interno:
  - `a_task_runs_until_its_end_after_its_handle_is_dropped` (el primero exigido): la tarea sigue, y pasa por un `.await` después de que se soltó su handle.
  - `cutting_a_task_stops_it_at_its_next_await_and_drops_what_it_holds` (el segundo): cortada mientras espera, no sigue aunque llegue lo que espera, y suelta lo que tiene. Además, `a_task_that_ended_gives_what_it_gave_also_once_cut` y `a_task_that_panicked_gives_an_error`. Los dos de un error comparan su texto con el del `JoinError` de Tokio, que es lo que el worker muestra.
  - Con el reloj pausado (el tercero):
    - `a_sleep_until_wakes_at_its_deadline_and_never_before`: pendiente 1 ms antes, despierta en el plazo, y el reloj llega solo hasta ahí;
    - `a_deadline_gives_the_future_that_ends_before_it_and_cuts_the_one_that_does_not` (`timeout_at`, y lo que el future tenía suelto al cortarse);
    - `a_timeout_counts_from_its_call` y `a_sleep_counts_from_its_call`;
    - `a_duration_whose_end_does_not_fit_in_the_clock_never_comes`: un `sleep` y un `timeout` de `Duration::MAX` siguen pendientes diez años después, y un `timeout` así da lo que da su future;
    - `the_instant_is_that_of_the_clock_of_tokio`;
    - `the_timers_count_by_the_clock_of_tokio`, que adelanta el reloj una hora antes de medir cada timer: un plazo que mirara el reloj del sistema se cumpliría enseguida.
  - `the_work_of_the_cpu_runs_out_of_the_thread_of_the_runtime`, `a_random_uuid_is_of_version_4_and_at_random` (mil distintos, de versión 4 y de la variante RFC 4122) y `the_wall_clock_is_the_time_of_the_system_in_milliseconds`.
  - **La suite del cliente entera, sin tocar ninguna expectativa:** los tests de integración y el ejemplo solo suman el `allow` del lint (en los desvíos), y la simulación, con su reloj pausado, pasa igual.
  - **El lint**, en nativo (el test exigido; el de wasm32 pasa al Lote 8, en los desvíos):
    - el clippy del workspace (`--all-targets`) y el del cliente sin `native` (`--no-default-features --all-targets`) pasan, con `-D warnings`: la producción no llama nada de la lista fuera de `rt`;
    - un `tokio::spawn` en `Shared::spawn` (`dispatch/mod.rs`) lo hace fallar con `use of a disallowed method tokio::task::spawn`. Se probó una vez y se sacó; también, una vez, un `tokio::spawn` en `Worker::start`;
    - una llamada de cada entrada fuera de `rt` lo hace fallar con todas: las 15 primeras, en la copia de las mutaciones, y `std::thread::spawn` y `std::thread::sleep`, en `dispatch/mod.rs`. Así, cada ruta de `clippy.toml` resuelve en nativo (también `tokio::spawn`, por el reexport);
    - sin el `cfg_attr` de `lib.rs`, fallan los tests unitarios del crate, que usan Tokio: el `cfg_attr` es lo que los deja.
- **Mutaciones inversas: las 14 mueren** (en una copia, cada una sola, con los tests de `rt` y los de la suite que la tocan).
  - **Las tareas:**
    - que soltar el handle corte la tarea: el primer test de `rt`, 23 del despachador (sus comandos no responden) y 6 del worker (el último ack);
    - que el corte no corte: el segundo test de `rt`, y 5 del worker (detenerlo corta su sync y su transferencia);
    - que `is_finished` dé siempre `false`: `a_task_that_ended_gives_what_it_gave_also_once_cut`, y 5 del worker (su `running` después de que un paso lo detiene);
    - `spawn_blocking` en el hilo del runtime: `the_work_of_the_cpu_runs_out_of_the_thread_of_the_runtime`.
  - **El reloj y los plazos:**
    - `rt::now` con el reloj del sistema: 6 tests de `rt` y 3 de `Progress`, en el transporte HTTP. La simulación no termina: se cortó a los 17 minutos, y sin la mutación tarda 28 s;
    - el plazo de `rt::timeout` con el reloj del sistema: `the_timers_count_by_the_clock_of_tokio` y un test de `Progress` (otros dos no terminan). La simulación pasa: ningún plazo de `rt::timeout` (el token, `LAST_ACK` y los del transporte HTTP) decide lo que ella mira;
    - `sleep_until` 100 ms antes, `sleep` de la mitad y `timeout_at` 100 ms después: de dos a cuatro tests de `rt` cada una. La simulación pasa con las tres, porque mira la convergencia y no cuándo vence cada plazo;
    - `timeout` que cuenta desde el primer `poll`: `a_timeout_counts_from_its_call`.
  - **El azar y la hora:**
    - un `random_uuid` fijo: su test, y 6 de las escrituras (todas con el mismo `MutationId`);
    - `wall_millis` en 0: su test. Nada más mira el `timestamp`, que es informativo.
  - **Las dos cuentas de `elapsed`, al revés:** la de la pausa de las subidas, con `a_replica_whose_upload_failed_uploads_none_for_a_minute`; la de `Progress`, con dos tests del transporte HTTP.

  Ninguna de las 14 sobrevivió: los tests de `rt` se escribieron antes, para ellas.
  - **Las de la primera ronda de la revisión de delta:**
    - un `Display` fijo de `TaskError` sobrevivía, y pidió un test: ahora los dos tests de un error comparan su texto, y mueren los dos;
    - un `sleep` y un `timeout` acotados a un año: muere `a_duration_whose_end_does_not_fit_in_the_clock_never_comes`;
    - sin el `#[track_caller]` de `spawn`, sobrevive: dónde se lanzó una tarea solo se ve con un *panic hook*, que es global a los tests, o con `tokio_unstable`. No cambia lo que hace la tarea.
- **Interacciones revisadas.**
  - **El reloj pausado de la simulación.** `rt::now`, `sleep`, `sleep_until`, `timeout` y `timeout_at` son los de Tokio, así que el reloj pausado sigue controlando cada plazo: la simulación y los tests con `start_paused` pasan sin cambios, y mueren las mutaciones de un reloj del sistema, de un plazo que cuenta desde el primer `poll` y de un `sleep_until` que duerme menos. `sleep` y `timeout` calculan su plazo en la llamada, como los de Tokio, y no en el primer `poll`, como lo habría hecho un `async fn`.
  - **Las tareas que corren hasta su fin** (DC21): el aplicador (`change`), `FileMetadata::run`, el reemplazo del cliente HTTP y las tareas del despachador siguen aunque su llamador deje de esperarlas. Soltar un `Task` no corta su tarea, como soltar un `JoinHandle`, y el primer test lo fija.
  - **El corte del worker** (DC28): `cut` es `Task::abort`, que corta en el próximo `.await` como `JoinHandle::abort`, y el worker sigue esperando cada tarea cortada antes de su último ack; `is_running` sigue leyendo `is_finished`. El trabajo de `spawn_blocking` no se corta una vez empezado, como hoy (su docstring lo dice).
  - **El lint con los `[lints]` del workspace:** `disallowed_methods` es de `clippy::style`, que avisa por defecto, y con el `-D warnings` de la CI es un error. El `allow` del atributo del crate, solo con `cfg(test)`, le gana a los niveles de la línea de comandos, y los `[lints]` del workspace no lo nombran. El `clippy.toml` vale para todos los targets del paquete: los tests unitarios pasan por el `cfg_attr` de `lib.rs`, pero los de integración y el ejemplo no. La biblioteca que compilan los tests de integración no lleva `cfg(test)`, así que el lint la revisa también ahí.
- **Desvíos y precisiones.**
  - **El lint vale también en nativo,** y no solo al compilar para wasm32, como decía DC38. `lib.rs` lo permite solo en sus tests unitarios (`cfg_attr(test, ..)`), así que el clippy nativo de la CI ya rechaza un uso nuevo fuera de `rt`: también en `http_transport.rs`, que no compila en wasm32, y en lo que el Lote 6b suma en `dispatch/`. Es más estricto que DC38, y en wasm32 vale desde que el crate compile ahí (Lote 8). Sale de la primera ronda de la revisión de delta: con el `cfg_attr(not(target_arch = "wasm32"), ..)` de antes, el crate lo permitía en nativo y estaba vacío en wasm32, así que el lint no vigilaba nada hasta el Lote 8.
  - **El test del lint en el chequeo de wasm32** no se puede hacer en este lote: el crate es `#![cfg(not(target_arch = "wasm32"))]`, vacío en wasm32 hasta el Lote 8. Se probó en nativo (arriba), y pasa a los tests exigidos del Lote 8. Ahí no puede ser con un `tokio::spawn`: en wasm32, Tokio va solo con `sync`, como en el storage, así que un `tokio::spawn` no compila, y el test probaría el compilador y no el lint. Va con `std::time::Instant::now`, `SystemTime::now` o `Uuid::new_v4`, que compilan en wasm32 y solo el lint rechaza. Las entradas de Tokio de `clippy.toml` no resuelven en wasm32: clippy avisa que no nombran una función alcanzable, y `-D warnings` no vuelve ese aviso un error. Van a necesitar `allow-invalid = true` o un cfg.
  - **Los tests de integración y el ejemplo,** crates aparte que leen el mismo `clippy.toml` y no pasan por el `cfg_attr` de `lib.rs`, llevan cada uno un `#![allow(clippy::disallowed_methods)]` (22 archivos, una línea cada uno). Sin él, el clippy nativo de la CI (`--all-targets`) fallaba en ellos, que usan Tokio en todas partes. DC38 decía que el `cfg_attr` del crate bastaba para no tocar los tests.
  - **Más entradas que las de DC38:** los `elapsed` (de Tokio, de `std::time::Instant` y de `SystemTime`), que llaman a `now`; `interval` e `interval_at`; `Uuid::new_v4`, que en wasm32 no compila sin elegir su fuente; y, de la revisión de delta, `std::thread::spawn` y `std::thread::sleep`, que en wasm32-unknown-unknown entran en pánico. El cliente no los usa en producción (`std::thread::panicking` no está en la lista).
  - **`#[track_caller]`** en `spawn`, `spawn_blocking`, `sleep`, `sleep_until`, `timeout` y `timeout_at`, como en Tokio (de la revisión de delta): el pánico sin runtime y el lugar donde se lanzó la tarea apuntan a quien llama, y no a `rt`.
  - **`rt::sleep`,** además del `sleep_until` del plan: los dos `sleep` del cliente siguen siendo `tokio::time::sleep`. Un `sleep` o un `timeout` cuyo fin no cabe en el reloj no vence nunca, como en Tokio, que lo pone a unos treinta años: sus docstrings lo dicen, y un test lo fija.
  - **Los dos `elapsed`** pasan a `rt::now().saturating_duration_since(..)`, que es lo que hace el `elapsed` de Tokio.
  - **`random_uuid` sigue siendo un UUID versión 4,** y no 128 bits al azar: los `MutationId`, los ids de dispositivo y el *jitter* salen como antes.
  - **`http_transport.rs`,** que es solo nativo (`reqwest`), pasa también por `rt`, como dice el plan.
  - **El despachador,** con el cambio mínimo (el `tokio::spawn` de `Shared::spawn`), por el Lote 6b, que se hizo en paralelo: este lote entra primero a `main`, y el 6b pasa a `rt` lo que suma en `dispatch/`, que el lint nativo ya no deja fuera de `rt`.
  - **`ARCHITECTURE.md` e `INVARIANTES.md`** no cambian: el lote no cambia ningún comportamiento.
- **La revisión de delta** la hizo un agente aparte, de contexto fresco y solo lectura, que probó en su propia copia con sus propias mutaciones. Comparó cada función de `rt` con el código de Tokio 1.53.1 (cuándo se calcula un plazo, el de un fin que no cabe, soltar el handle, el corte, `is_finished`, el texto del error, el `elapsed`), y la tabla y las líneas de la nota con el delta. **Salió en verde en su segunda ronda.**
  1. **Primera ronda: ningún Alto, 2 Medios y 4 Bajos, corregidos.** En nativo el comportamiento no cambiaba.
     - **Los Medios:**
       - El lint no vigilaba nada: el crate lo permitía en nativo y estaba vacío en wasm32, así que un `tokio::spawn` nuevo en `dispatch/` no daba ningún aviso, mientras el Lote 6b sumaba tareas ahí. Ahora vale también en nativo, fuera de los tests unitarios (en los desvíos, y una precisión en DC38).
       - La prueba del lint que pasaba al Lote 8 no figuraba en el Lote 8, y con un `tokio::spawn` no probaría el lint: en wasm32 no compila. Ahora es un test exigido del Lote 8, con lo que compila en wasm32.
     - **Los Bajos:** `#[track_caller]` en las seis funciones, como en Tokio; un test del texto de `TaskError` (la mutación de un `Display` fijo sobrevivía); `std::thread::spawn` y `std::thread::sleep` en el lint; y lo que pasa con un fin que no cabe en el reloj, en los docstrings y en un test.
  2. **Segunda ronda, acotada a los arreglos: verde, con 2 Bajos de documentación, corregidos:** el docstring del módulo `rt`, que decía que el lint valía solo al compilar para wasm32; y DC38 y el Lote 4, que lo seguían diciendo para el storage. Inyectó un uso fuera de `rt` en `http_transport.rs`, `events.rs` y `client.rs`, también sin `native`, y el clippy nativo falla con cada uno; los tests unitarios siguen permitidos.

  Queda sin test el `#[track_caller]` de `spawn`: solo se ve con un *panic hook*, que es global al binario de los tests unitarios.
- **Chequeos:**
  - fmt; clippy del workspace con `-D warnings`, y el del cliente sin `native` (`--no-default-features --all-targets`); el build y el clippy de wasm32 de `zemdb-core`, `zemdb-storage` y `zemdb-client`, con `-D warnings` (el cliente, vacío en wasm32); y doc con `-D warnings` (también el del cliente con `--document-private-items`, por los docs de `rt`);
  - la suite del workspace, con 1688 tests y sin fallas, después de los arreglos de la primera ronda de la revisión de delta (1687 antes);
  - los `--ignored` del cliente en release, con 7 tests y sin fallas;
  - la prueba del lint, arriba.

  Todos, salvo la prueba del lint, se repitieron en `main` con el lote integrado y los arreglos de las dos rondas, con los mismos números.

  En la primera corrida de la suite, con otro agente compilando en la misma máquina, falló una vez `the_heartbeats_of_the_worker_leave_behind_an_http2_connection_that_stops_answering`: su última escritura, con un plazo de 200 ms, dio `OutcomeUnknown`. Es el test de la pausa de 1,5 s que ya registra la nota del 6a. Pasó cinco veces solo, tres veces con su binario entero, y en la segunda corrida de la suite.

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
- **El lint de DC38 en el chequeo de wasm32** (el del Lote 7, que ahí no se podía hacer; se prueba una vez y se saca): una llamada fuera de `rt` a `std::time::Instant::now`, a `SystemTime::now` o a `Uuid::new_v4` (este, si `uuid` tiene su fuente en wasm32), que compilan en wasm32 y solo el lint rechaza, hace fallar el clippy de wasm32 con `-D warnings`. Un `tokio::spawn` no sirve: en wasm32, Tokio va solo con `sync`, y no compilaría. Las entradas de Tokio del `clippy.toml` del cliente no resuelven en wasm32 (clippy avisa que no nombran una función alcanzable, y `-D warnings` no vuelve ese aviso un error): necesitan `allow-invalid = true` o un cfg.
- **En nativo:** la suite entera, sin cambios.

**Interacciones a revisar:** todo lo que corre en una tarea propia en el núcleo (DC21) y en el worker (DC28); los timers del navegador (S5, pregunta 5); INV-K16 en el OPFS (el guardado con el `rename` de la capa, sus generaciones); la memoria del Worker.

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

**Qué se hace:** DC41, con la espera, durante un plazo desde el Web Lock, de los handles que el Worker que terminó todavía no soltó (el de `client.lock`, y después los de las salas y los de los metadatos, ante `RoomLocked` y el error *tomado* de la capa, que dan los Lotes 2 y 3), y también `InUse` en nativo, sin el punto de entrada del Worker (Lote 12), que lo usa.

**Tests exigidos:**
- **Pasos del arnés, con dos páginas del mismo contexto:** la segunda recibe `InUse` sin tocar ningún archivo (una capa que cuenta las operaciones, alrededor de la del OPFS, no ve ninguna); con la opción de esperar, la segunda abre cuando la primera hace `shutdown`, y cuando su Worker termina; recargar la página que tiene el cliente: la página nueva lo abre; dos directorios distintos se abren a la vez en dos páginas. Con la opción de esperar, la segunda abre aunque el handle de `client.lock` y los de las salas sigan tomados un rato después de recibir el lock (el paso lo provoca terminando el Worker de la primera con muchos handles abiertos, en medio de una compactación y en medio de un guardado de los metadatos). Si no se sueltan en el plazo, la apertura del store da `InUse` y suelta el lock, y la de una sala da su propio error.
- **En nativo:** un segundo cliente sobre un directorio tomado recibe `InUse`. En el storage, un error inyectado al tomar el lock (el *fail point* nuevo, en `files/tests/native.rs`) es un `Err`, no `None`: con el código de hoy, que da `None` con cualquier error, el test falla. En el cliente, con `TestFiles` que falla en el `open_locked` de `client.lock`, `Client::open` da `Storage`, no `InUse`.

**Interacciones a revisar:** `FileMetadata` y `client.lock`; el `RoomLocked` del motor en disco, que sale del mismo `None` de la capa nativa (`lock_room_files`, y `create_locked` en la rotación y en el snapshot temporal, en `compactor.rs`), y la capa de prueba `opfs_like.rs`, que delega en ella; el snapshot temporal que `create_locked` creó antes de fallar al tomar su lock, que `stage_file` hoy deja si sale por el error (tiene que borrarlo, como con `None`); `shutdown` (DC28) y el último `Ack`; el orden en que el navegador suelta los handles y el lock, y las operaciones en vuelo de un Worker terminado (S5 (2), §1.2.1).

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

**Interacciones a revisar:** el orden en que el navegador suelta el lock y los handles de la líder que se va (S5 (2), §1.2.1; la espera de DC41); sus escrituras en vuelo (INV-K11, INV-K2); el `watch` que se vuelve a suscribir (INV-K10); el worker de cada sala en la líder nueva, que arranca como al abrir (DC36); las claves (INV-K9); los tokens de las salas que se vuelven a abrir (DC30).

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
