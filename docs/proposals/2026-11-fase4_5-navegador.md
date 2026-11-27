# Navegador (Rust) — Plan de la sub-fase 4.5

**Fecha:** 19 de Noviembre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: el paso S1 (los spikes) hecho el 23 de Noviembre de 2026, en Chromium, Firefox y Safari (§1.2.1): revisa DC39 (un `move()` sobre un archivo existente no es atómico en Firefox) y DC41 (el Web Lock llega antes de que se suelten los handles), las dos adoptadas por el dueño del proyecto el 24 de Noviembre de 2026, y suma notas en DC40, DC44, DC45 y DC46. Lote 1 (el arnés del navegador) hecho el 23 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda; el Lote 6a (los comandos y el despachador del runtime del Worker), hecho el 24 de Noviembre de 2026, y el 6b (las suscripciones y el token), hecho el mismo día, con su revisión de delta en verde en su cuarta ronda; el Lote 7 (el runtime del cliente detrás de `rt`), hecho el 24 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda; el Lote 2 (la capa del OPFS: el directorio, con las generaciones de DC39, y Firefox en el job `browser`), hecho el 26 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda y Firefox en verde en la CI (el run 45732770653); el Lote 3 (la capa del OPFS: los archivos, con los tests unitarios del storage en wasm32), hecho el 27 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda y Firefox en verde en la CI (el run 46021628394); el Lote 4 (el motor en disco en wasm32, sobre el OPFS, con DEF-184 y DEF-185 de 4.1), hecho el 27 de Noviembre de 2026, con la revisión de delta y Firefox en la CI pendientes; la CI de `main`, en verde sobre `9692686` (el run 46030117421). Lote 0 (el contrato) cerrado el 23 de Noviembre de 2026, con la revisión cruzada en verde en su sexta ronda y DC46 en su principio, con su protocolo para el paso D (§1.3). El dueño del proyecto adoptó DC40 y DC43 el 22 de Noviembre de 2026, y eligió ese día la pestaña líder para DC41, de donde sale DC46; adoptó las demás el 23 (DC46, en su principio: su protocolo, en el paso D).
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
- **Un lint** (`clippy::disallowed_methods`, con un `clippy.toml` en `crates/client` y en `crates/storage`) deja fuera de `rt` y de `sys` lo que en wasm32 entra en pánico o no corre: `std::time::Instant::now`, `SystemTime::now`, `tokio::spawn`, `spawn_blocking` y los timers de Tokio. Vale solo al compilar para wasm32 (fuera de wasm32, los crates lo permiten con `cfg_attr`), así que no toca los tests ni el servidor, que siguen con Tokio. *Precisado en el Lote 7:* en el cliente vale también en nativo, fuera de sus tests unitarios (`cfg_attr(test, ..)`), para que la CI vea un uso nuevo fuera de `rt` antes de que el crate compile en wasm32; sus tests de integración y su ejemplo lo permiten con un `allow` cada uno. El storage hace lo mismo en el Lote 4. *Precisado en el Lote 4:* el `clippy.toml` del storage tiene las mismas entradas, con su reemplazo en `sys` donde lo hay; las de Tokio y de `uuid` llevan `allow-invalid`, porque en wasm32 sus rutas no resuelven (Tokio, solo con `sync`; `uuid`, que no es una dependencia ahí); y la capa nativa, que corre en el pool bloqueante y no compila en wasm32, lo permite en su módulo.
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
- **Los errores:** cada `DOMException` pasa a un `io::ErrorKind` (`NotFoundError` a `NotFound`, la cuota llena a `StorageFull`), con su nombre en el mensaje. *Precisado en el Lote 3:* la cuota llena da `StorageFull` donde el navegador lanza `QuotaExceededError`; un conteo de bytes del navegador mayor que lo pedido, como el que da Chromium 153 por una escritura sobre la cuota (§1.2.1), es un error (`Other`), con el conteo en el mensaje; una escritura que terminaría más allá de 2^53, donde los offsets del navegador dejan de ser exactos, es `InvalidInput`, antes de llegar al navegador; y una que no escribe nada, como la de Firefox 155 en el offset `2^53 - 1`, es `WriteZero`.
- **El motor en wasm32:** sus tareas, con la de DC38; su trabajo de CPU, en el lugar; el `.snap`, comprimido con `ruzstd` `Fastest` (DC9), y `zstd_level`, validado contra lo que da en wasm32 (hoy, con `zstd::compression_level_range()`); los nombres temporales, con el azar de DC38; y el módulo de los *fail points*, que hoy no compila en wasm32, compila ahí, donde fuera de los tests no hace nada, como en nativo (sus pausas, que esperan con `Instant`, quedan para los tests nativos). *Precisado en el Lote 4:* `zstd_level` toma en wasm32 solo 1, el nivel que `Fastest` aproxima, que es ahí su valor por defecto (`ruzstd` no implementa otro, y lo ignoraría); un nivel que vale en nativo, también el 3 por defecto, es `InvalidOptions`. Los nombres temporales son ahí 32 dígitos hexadecimales de 128 bits de `crypto.getRandomValues`, y en nativo, el UUID de antes. Las pausas bloqueantes y los puntos de hilo, para los tests nativos.
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
  - **Observado en el Lote 3, no en el paso S1, en Chromium 153** (el de Playwright 1.63, el de la CI): una escritura del `FileSystemSyncAccessHandle` sobre la cuota del origen no lanza `QuotaExceededError`, sino que da como escritos 4294967288 bytes (−8 como entero sin signo), más que los que se le dieron. Deja escrita una parte, hasta casi la cuota, y el `flush()` siguiente sale bien. Una escritura en el offset `2^53 - 1` da el mismo conteo, sin escribir nada. El headless shell de Chromium 157 de este entorno lanza `QuotaExceededError` en los dos casos, y sobre la cuota también deja escrita una parte. La capa rechaza un conteo mayor que lo pedido (DC39, la nota del Lote 3).
  - **Observado en el Lote 3 en Firefox 155** (el build de Playwright 1.63, en la CI, los runs 46018624452 y 46021628394): la escritura en el offset `2^53 - 1` no lanza `QuotaExceededError`: da 0 bytes escritos, que la capa toma por `WriteZero`. La cuota llena en Firefox no se probó: el arnés solo la fija por CDP, que Playwright no tiene para Firefox.
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
4. **Lo que deja una pestaña que se va:** sus suscripciones, sus salas y los comandos suyos que la líder termina después (un `room` o un `watch` que se abre cuando la pestaña ya no está tiene que soltarse). Hoy (Lote 6b), el aviso termina las suscripciones de la pestaña y sus pedidos de token, y un `Watch` que la pestaña manda después abre una suscripción, como un `room` abre la sala.
5. **El relevo:** qué devuelve cada comando en vuelo (una escritura y un `discard`, `OutcomeUnknown`; los que se pueden repetir); el orden de la restauración de cada pestaña (sus claves, sus salas, el modo, sus `watch`) y lo que se manda otra vez después; un `close` o un `unwatch` en vuelo; el modo de una sala, que no puede depender del orden en que restauran las pestañas; un `watch` que no queda colgado ni repite un valor (la confirmación de punta a punta, un valor que no se publica por igual al último, un error repetido); y la líder nueva, que restaura lo de su propia página.
6. **El token:** a qué página se lo pide la líder, con el `TokenProvider` de hoy, que no sabe qué pestaña lo pidió (la renovación es una por sala), y qué pasa si esa página está en segundo plano (S5, pregunta 5). Hoy (Lote 6b), el despachador se lo pide a una pestaña de la sala: la del `room` que corre en el turno de la sala, o la de menor id de las que la abrieron, nunca una cuyo aviso de que se fue está en vuelo; el aviso termina los pedidos a la pestaña, sin token (el paso D lo revisa).
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
| 2 | La capa del OPFS: el directorio | storage | DC39, DC38, DC45 | S1 a S8 y L7 sobre el OPFS | ✅ |
| 3 | La capa del OPFS: los archivos | storage | DC39, DC44 | S1 a S8 y L7 sobre el OPFS | ✅ |
| 4 | El motor en disco en wasm32 | storage | DC39, DC38 | S1 a S8, L7, W1 y W2 sobre el OPFS | ✅ |
| 5 | Los metadatos, durables antes de servirse | client | DC45 | K16 | ⬜ |
| 6a | El despachador del runtime del Worker: los comandos y el despachador | client | DC42, DC46 | K2, K15 | ✅ |
| 6b | El despachador del runtime del Worker: las suscripciones y el token | client | DC42, DC46 | K10, K15 | ✅ |
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

**Tamaño.** Unas 260 líneas de producción el Lote 2 (con las generaciones de DC39; salieron 380, de las que 44 son las aperturas que todavía no abren, que reemplaza el 3: un desvío de la regla, en su nota), 220 el 3 (la capa nativa, con el mismo contrato, tiene unas 300 sin comentarios; salieron 351 netas, sobre lo del 2 sin el relleno que reemplaza, con las de las dos rondas de su revisión de delta: un desvío, en su nota), 120 el 4 (salieron 128 nuevas y 51 que salen, 77 netas), 60 el 5, 280 el 6 (si pasa de 300, se parte: los comandos y el despachador; las suscripciones y el token; salieron unas 480, y se partió: 299 el 6a y 293 el 6b), 120 el 7 (mecánico; salieron 161), 200 el 8, 280 el 9, 100 el 10, 150 el 11 y 180 el 12; las del 13 y el 14, las estima el paso D. El arnés del 1 y las pruebas del 15 no son producción.

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
    - el runner (8 pasos) y su self-check, en el Chromium que Playwright toma en este entorno (*precisión del Lote 3:* un headless shell, un build 157, y no el Chromium 153.0.8010.12 de Playwright 1.63, que usa la CI);
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

### Lote 2 · La capa del OPFS: el directorio (`zemdb-storage`) · ✅ Hecho (26 de Noviembre de 2026)
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

**Hecho** (26 de Noviembre de 2026).
- **Qué entró.**
  - **`OpfsFiles`** (`crates/storage/src/files/opfs.rs`, nuevo, solo en wasm32), la capa sobre el OPFS del origen desde el Worker dedicado, en su parte del directorio:
    - **la raíz y las rutas:** una ruta es relativa a la raíz del OPFS (`navigator.storage.getDirectory()`), con sus nombres separados por `/`; los nombres vacíos y `.` no cuentan. Se rechazan con `InvalidInput` antes de la primera promesa, así que no tocan nada: una ruta absoluta o con `..`; una con un nombre con `~`, con `\` o con un carácter NUL; la de un archivo que termina en `/` o en `.`, que no nombra ningún archivo; y el prefijo de un listado con `~`. Así no llegan al navegador los nombres que se sabe que no toma: `/`, `.`, `..` y el vacío, en los tres (el paso S1), y `\`, en Chromium (el sondeo de la primera ronda de la revisión de delta); NUL no lo probó nadie, y se rechaza por precaución. Un navegador puede rechazar todavía otro nombre con un `TypeError`, que sale como `InvalidInput`. Cada operación baja desde la raíz con `getDirectoryHandle` (con `create` en `create_dir_all`);
    - **los bindings:** `move(directorio, nombre)`, que `web-sys` no tiene, en un tipo propio (`MovableFile`, con `js_name = FileSystemFileHandle`) y con `catch`, que compila con el `forbid(unsafe_code)` del crate. Lo demás es de `web-sys` (`getDirectoryHandle`, `getFileHandle`, `removeEntry` y `keys()`) y de `js-sys` (el iterador asíncrono);
    - **los errores:** cada error del navegador pasa a un `io::Error` con su nombre en el mensaje (`{la llamada}: {nombre}: {mensaje}`): `NotFoundError` a `NotFound`, `QuotaExceededError` a `StorageFull`, un `TypeError` a `InvalidInput`, `NotAllowedError` y `SecurityError` a `PermissionDenied`, y los demás a `Other`. El de un archivo tomado es el error *tomado*, según la llamada (§1.2.1): `NoModificationAllowedError` o `InvalidStateError` para `move()`, como para `createSyncAccessHandle`, y `NoModificationAllowedError` para `removeEntry`;
    - **las operaciones del directorio, con las generaciones de DC39.** Cada una lista el directorio (`keys()`) y saca de ahí las generaciones de sus nombres, por el nombre exacto:
      - `exists`: si hay alguna generación. Un directorio que falta da `false`, como `try_exists` en nativo, y cualquier otro error es un error;
      - `list`: los nombres con el prefijo, sin el sufijo, cada uno una vez y en orden;
      - `rename`: primero busca los directorios del origen y del destino (con un solo handle si son el mismo), así que uno que falta lo hace fallar sin tocar nada; después, los tres pasos de DC39. Borra las viejas del origen, y si no puede, falla sin cambiar el valor de ningún nombre; mueve la más nueva a la generación siguiente del destino, o al nombre si el destino no existe, siempre una entrada libre; y borra las viejas del destino sin fallar (una que queda se registra con `tracing::debug!`). Sabe por el `NotFoundError` de `getDirectoryHandle` si falta el directorio del destino, y por el listado si falta el origen (`NotFound`), y no por el error de `move()`;
      - `remove`: de la más vieja a la más nueva. Salta una generación que ya no está (un `NotFound`), y da `NotFound` solo si no borró ninguna; cualquier otro error lo corta;
      - `create_dir_all`; y `sync_dir`, que solo valida la ruta;
      - `sync_dir_covers_earlier_changes`, que da `false`;
    - **una operación soltada termina:** cada operación corre, con todos sus pasos, en una tarea propia (`sys::spawn`). Las operaciones de la capa corren de a una en el Worker, con un turno (un `tokio::sync::Mutex` estático) que el llamador toma antes de lanzar la tarea y que la tarea retiene hasta que termina: la siguiente espera a la que su llamador soltó, y una soltada antes de su turno no corre;
    - **las aperturas** (`open_locked`, `create_locked`, `create` y `open_read`) dan `Unsupported`, y `OpfsFile` no tiene valores (su único campo es `Infallible`): las reemplaza el Lote 3.
  - **Lo que la capa decide sin el navegador** (`files/opfs_names.rs`, nuevo, que compila en wasm32 y en los tests nativos):
    - partir una ruta en sus nombres, y el prefijo de un listado, rechazando lo de arriba;
    - la entrada de una generación (`nombre~N`, con `N` en decimal canónico desde 1; la generación 0 es el nombre), y la generación de una entrada;
    - las generaciones de un nombre entre las entradas, de la más vieja a la más nueva, y la siguiente;
    - el listado sin sufijos;
    - el `ErrorKind` de cada nombre de error, y los errores de un archivo tomado de `move()` y de `removeEntry`.
  - **El contrato** (`files/mod.rs` y `lib.rs`):
    - `files` compila en wasm32, con `native` solo fuera y `opfs` solo en wasm32;
    - `RoomFiles` y `RoomFile` piden `EngineConcurrencyBounds` (en nativo, `Send + Sync`, como antes) y van con `async_trait(?Send)` en wasm32;
    - el contrato de `sync_dir`, reescrito: hace durables los cambios de las operaciones que salieron bien, y donde no hace nada solo esos se garantizan; si hace durable uno que ya ocurrió, lo dice el método nuevo;
    - `sync_dir_covers_earlier_changes` (DC45), sin implementación por defecto: `true` en `NativeFiles`; `false` en `OpfsFiles`; y en las dos capas de prueba, la del storage (`disk/tests/opfs_like.rs`) y `TestFiles` (`crates/client/tests/common/`), lo que dice la nativa, sobre la que corren;
    - `FileTaken`, el error *tomado*: un `io::Error` de tipo `ResourceBusy` que lo envuelve, con `FileTaken::error` para armarlo y `FileTaken::is` para reconocerlo con `downcast`. Lo nombran los contratos de `create`, `open_read`, `rename` y `remove`.
  - **La tarea de `sys`** (`sys::spawn`, solo en wasm32): corre un future con `wasm_bindgen_futures::spawn_local` hasta su fin aunque nadie lo espere, y le da su resultado a un `oneshot::Receiver` mientras alguien lo espere.
  - **`crates/storage/Cargo.toml`:** `wasm-bindgen` 0.2.129, `wasm-bindgen-futures` 0.4.79, `js-sys` 0.3.106 y `web-sys` 0.3.106 (con seis features), solo en wasm32. El `Cargo.lock` de la raíz suma solo esas dependencias de `zemdb-storage`: los paquetes ya estaban.
  - **El arnés:** el crate de pasos depende de `zemdb-storage` por ruta y de `futures` (por `now_or_never`); la suite `files`, nueva y entre las de por defecto (abajo); y su `README.md` (Firefox y las suites).
  - **La CI:** Firefox en el job `browser`, que pasa a llamarse `Browser (Chromium and Firefox)`. Instala el Chromium y el Firefox de Playwright, y corre el runner y su self-check con cada uno. La clave del caché de los navegadores nombra a los dos, para que el caché de antes, solo con Chromium, no se reuse.
  - **380 líneas de producción**, sin comentarios, líneas en blanco ni la declaración de los módulos de tests unitarios (las tres líneas de `#[cfg(test)]`, `#[path]` y `mod tests;`, en todos los archivos por igual), contadas después de los arreglos de la primera ronda de la revisión de delta: 244 en `opfs.rs`, 92 en `opfs_names.rs`, 36 que entran en `mod.rs` por 5 que salen, 11 en `sys.rs`, 3 en `native.rs` y 1 que sale de `lib.rs` (386 nuevas y 6 que salen). El plan estimaba 260. La primera versión de la nota decía 363, con las tres líneas de la declaración de los tests de `mod.rs` contadas: eran 360. Los arreglos sumaron 20 (las rutas, el prefijo, `remove`, y los directorios de `rename` antes del primer paso).
    - De ellas, 44 son las aperturas que todavía no abren (`OpfsFile` sin valores y los cuatro `Unsupported`), que reemplaza el Lote 3: sin ellas, 336.
    - Lo demás de más está en lo que la estimación no contaba: el error *tomado* y el contrato (unas 40), y el turno, la tarea, el error de cada llamada y las rutas que no se toman.
    - Si hubiera que partirlo, el corte sería la base (el contrato, `FileTaken`, el método de DC45, la tarea, las rutas, los errores, el turno, `exists`, `list`, `create_dir_all` y `sync_dir`), unas 210, y las generaciones (`rename`, `remove` y su parte de `opfs_names.rs`), unas 170. No se partió: es un desvío (abajo).
- **Tests.**
  - **Nativos** (12, unitarios del storage, porque lo que prueban es interno):
    - de `opfs_names.rs` (`files/tests/opfs_names.rs`, 10):
      - `a_path_is_split_into_its_names_from_the_root`;
      - `a_path_or_a_prefix_that_the_layer_does_not_take_is_refused`: `..` en cualquier lugar, una ruta absoluta, `~`, `\` o NUL en un directorio o en el archivo, la ruta de un archivo que termina en `/` o en `.` (o vacía), y un prefijo con `~`;
      - `the_first_generation_of_a_name_is_the_name_itself`: ida y vuelta, también con `u64::MAX`;
      - `an_entry_that_no_generation_has_is_not_one`: `~0`, `~01`, `~+1`, `~`, `~x`, `~1~2`, `~1` y un número que no cabe;
      - `the_generations_of_a_name_are_those_of_exactly_that_name_from_the_oldest`: `room_a.wal` frente a `room_a.wal.compacting`, `room_a.wa` y `room_ab.wal`;
      - `a_replacement_goes_to_a_free_entry`, también sin ninguna después de `u64::MAX`;
      - `a_listing_gives_each_name_once_without_its_generations`;
      - `each_error_of_the_browser_has_its_kind`, con la cuota llena;
      - `a_taken_file_is_the_error_that_each_browser_gives_it`, de una mutación que sobrevivía (abajo);
      - `no_name_of_the_engine_or_of_the_metadata_has_the_mark_of_a_generation`, con los nombres reales: los de `DiskStorageEngine` (`.lock`, `.snap` y `.wal`), `compacting_wal_path`, `absorbing_wal_path` y el temporal de un snapshot, con el id de sala más largo y todos sus caracteres, y los de `FileMetadata`, escritos en el test porque son del cliente. Cada uno es su propia generación 0;
    - de `mod.rs` (`src/tests/files.rs`, 2): `the_error_of_a_taken_file_is_recognized_and_no_other_is` (otro `ResourceBusy`, o un `Other` con el nombre del error, no lo es) y `sync_dir_covers_the_earlier_changes_on_the_file_system_of_the_process`.
  - **Pasos del arnés** (la suite `files`, con una recarga, sobre la capa real). Los archivos los hace el paso con `web-sys`, porque la capa todavía no abre, y cada paso compara las entradas exactas que deja cada operación, con sus generaciones:
    - `files_write`, cada operación del directorio: `create_dir_all`; `exists`, también de un archivo en un directorio que falta; `list`; `sync_dir`; `rename` a un nombre libre (al nombre mismo), sobre uno que existe (a `b~1` y después a `b~2`, borrando la vieja, y nunca sobre él) y a otro directorio (de `data/x`, con `x` y `x~1`, a `data/deep/x`, con `x` y `x~2`: queda solo `deep/x~3`, con lo de `x~1`); y `remove`. Después fabrica lo que deja un corte: `g` y `g~3`; `h`, `h~1` y `h~2`; y `p.wal` con `p.wal.compacting~1`. `list` da cada nombre una vez, sin el sufijo;
    - `files_after_reload`, después de recargar: las entradas de antes y lo que tienen, también en `deep`; `exists` de cada nombre; y `list`. Después, `g` renombrado mueve `g~3` y borra `g`; `h` sobre `b` deja solo `b~3`, con lo de `h~2`, y nada de `h`; `remove(p.wal)` deja `p.wal.compacting~1`; y `remove` de un nombre con dos generaciones borra las dos;
    - `files_taken`, con un handle de `web-sys` abierto en el paso:
      - con el origen abierto, `rename` y `remove` dan el error *tomado* (`FileTaken::is` y `ResourceBusy`) y no cambian nada;
      - con el destino abierto, `rename` da `Ok`, con el valor nuevo en `t~1`, y la vieja, abierta, queda;
      - con la generación vieja del origen abierta, `rename` falla sin cambiar nada;
      - `remove` con la más vieja abierta falla enseguida, y el nombre conserva su valor; con la más nueva abierta, borra antes las viejas y falla en ella, que queda con su valor;
      - cerrados los handles, los `remove` siguientes las borran, también la vieja de `t`;
    - `files_refused`: `create_dir_all` con `..` después de un nombre que se podría crear, absoluto, con `~` y con `\` después de un nombre que se podría crear; `rename` hacia y desde `..` (con un origen que tiene una generación vieja, que no se borra) y de un nombre con `~`; `remove`, `exists`, `list` y `sync_dir` con rutas así; `remove` de `sub/`, con `sub` un directorio vacío, y `exists` de `r/` y de `r/.`, con `r` un archivo; y `list` con el prefijo `r~`. Todos dan `InvalidInput`, y ni la raíz del OPFS ni el directorio de la corrida cambian;
    - `files_dropped`: un `rename` sondeado una vez y soltado (`now_or_never`), con generaciones viejas en su origen, termina, y el `exists` siguiente lo espera; un `remove` soltado mientras espera su turno, detrás de otro `rename` soltado, no corre nunca; y (de la revisión de delta) un `rename` soltado sobre un destino con 200 generaciones, cuyo `exists` siguiente espera también los 200 borrados que vienen después del `move()`: queda solo `d~200`;
    - `files_errors`: `rename` de un origen que falta, `remove` de uno que falta, `list` de un directorio que falta y `rename` a un directorio que falta dan `NotFound`, los dos últimos con `NotFoundError` en el mensaje (y el último no toca nada: la generación vieja de su origen queda); un `remove` de un nombre con 300 generaciones, mientras el paso borra `v~250` por su cuenta una vez que la capa empezó a borrar, sale bien y las borra todas; `exists` debajo de un archivo es un error (`TypeMismatchError`), no un archivo que falta; y `sync_dir_covers_earlier_changes` da `false`.
  - **La cuota llena no se pudo provocar en el navegador.** En este lote la capa no escribe, y el arnés no tiene cómo fijar la cuota del origen en los dos navegadores (el Chromium de Playwright lo permite por CDP, que Firefox no tiene). El `ErrorKind` de `QuotaExceededError` lo prueba `each_error_of_the_browser_has_its_kind`, en nativo, y los pasos prueban el nombre de un `DOMException` como llega del navegador, con `NotFoundError`. El Lote 3, que escribe, puede volver a intentarlo.
  - **La suite del workspace, sin tocar ninguna expectativa:** la capa nativa y las de prueba solo suman el método.
- **Mutaciones inversas: 48 corridas, en una copia, cada mutación sola: 31 contra los pasos del arnés en Chromium y 17 contra los tests nativos. Mueren todas salvo una, que pidió un test y ahora muere.**
  - **Contra los pasos:**
    - un `move()` sobre la generación más nueva del destino, que el Chromium acepta y reemplaza: `files_write` (las entradas);
    - mover la generación más vieja del origen: `files_after_reload` (`k` tiene lo de `g`);
    - las generaciones por prefijo: `files_write`;
    - `remove` de la más nueva a la más vieja: `files_taken` (el nombre pierde su valor);
    - un `rename` que falla si no pudo borrar las viejas del destino: `files_taken`;
    - no borrar las viejas del origen, o borrarlas después del `move()`: `files_after_reload`;
    - borrar las viejas del destino antes del `move()`: `files_taken` (con el origen abierto, `t` desaparece). La primera forma de esta mutación armaba la promesa del `move()` antes de los borrados, y moría por un rechazo sin manejar: se rehízo con la promesa después, y muere por el valor;
    - no rechazar `..`, una ruta absoluta o `~`, y un `sync_dir` que no valida su ruta: `files_refused`;
    - la operación en el future de su llamador, sin tarea, y sin el turno: `files_dropped` (el `exists` no ve el `rename`); el turno tomado dentro de la tarea: `files_dropped` (el `remove` soltado corre);
    - `list` con las entradas crudas, y con nombres repetidos: `files_write`;
    - `NotFoundError` a `Other`: `files_write` (`exists` en un directorio que falta); el nombre del error fuera del mensaje: `files_errors`;
    - el error *tomado* que no se arma, `removeEntry` o `move()` sin `NoModificationAllowedError`, y `FileTaken::is` que no lo reconoce: `files_taken`;
    - `sync_dir_covers_earlier_changes` en `true`: `files_errors`;
    - `exists` que toma cualquier error por un archivo que falta: `files_errors` (debajo de un archivo); y que da un error con un directorio que falta: `files_write`;
    - `remove` de un nombre que falta que sale bien: `files_errors`;
    - `create_dir_all` sin `create`, y la generación siguiente igual a la más nueva: `files_write`;
    - **la que sobrevivía:** `move()` sin `InvalidStateError`, el error de Safari, que ni el Chromium ni el Firefox de la CI dan. Pidió un test: las listas de los errores de un archivo tomado pasaron a `opfs_names.rs`, y `a_taken_file_is_the_error_that_each_browser_gives_it` las fija. Safari las prueba en el paso S2.
  - **Contra los tests nativos,** cada una con un test que falla:
    - las generaciones por prefijo, sin ordenar, de un nombre con `~`, la 0 con sufijo y una no canónica;
    - la siguiente igual a la más nueva, y el listado repetido;
    - no rechazar `..`, una ruta absoluta o `~`;
    - `NotFoundError` y `QuotaExceededError` a `Other`;
    - `move()` sin `InvalidStateError`, y `removeEntry` con él;
    - `FileTaken::is` que reconoce cualquier error que envuelve algo, y el error *tomado* de otro tipo;
    - la capa nativa que dice que su `sync_dir` no cubre los cambios anteriores.
  - **Las de los arreglos de la primera ronda de la revisión de delta** (9, en otra copia, cada una sola, contra los pasos en Chromium, y las de `opfs_names.rs` también contra los tests nativos): mueren todas.
    - el turno suelto apenas resuelve el `move()`, con los borrados del paso 3 en otra tarea: `files_dropped` (quedan `d~10`…`d~200`);
    - el handle del origen tomado del directorio del destino, las generaciones del destino listadas en el directorio del origen, y el `move()` al directorio del origen: `files_write` (el `rename` a otro directorio);
    - el directorio del destino buscado después del paso 1: `files_errors` (se borra la generación vieja del origen);
    - un `remove` que corta en una generación que ya no está: `files_errors`;
    - la ruta de un archivo que termina en `/` tomada por su último directorio, un nombre con `\` y un prefijo con `~`: `files_refused` y `a_path_or_a_prefix_that_the_layer_does_not_take_is_refused`.
- **Interacciones revisadas.**
  - **Los requisitos de DC17** (§1 del plan de 4.1). Renombrar y borrar por ruta, sin handles abiertos en el origen ni en el destino: con las generaciones, un destino abierto ya no hace fallar el `rename` (el `move()` va a un nombre libre), y un origen abierto da el error *tomado* sin cambiar nada. Lo demás (`read_at` exacto, `create_locked` sin un *create-new* atómico y el handle muerto) es del Lote 3.
  - **Lo que el motor da por hecho del directorio** (`crates/storage/src/disk/`, que todavía no compila en wasm32):
    - **la instalación de un snapshot y la compactación:** el `rename` del temporal sobre `.snap`, y el de la copia de una absorción sobre `.wal.compacting`, que existen, van a su generación siguiente, y el valor es el nuevo una vez resuelto el `move()`. Un `rename` que falla no cambió ningún valor, como espera `apply_snapshot` (INV-S5). El temporal nunca tiene generaciones viejas, porque lo crea `create_locked`;
    - **los borrados de `delete_room`:** `remove` de `.snap`, `.wal.compacting`, la copia de una absorción y los temporales, con un `NotFound` como éxito. Borra todas las generaciones de cada nombre, de la más vieja, así que un borrado que falla en el medio deja el nombre con su valor, y el reintento lo termina (INV-S8). `room_{id}.wal` y `room_{id}.wal.compacting` no se confunden;
    - **la búsqueda de temporales** (la recuperación y `delete_room`, con `list(data_dir, "room_{id}.snap.tmp.")`): da cada temporal una vez, sin sufijo; y `exists` de `.wal.compacting` (la compactación y la recuperación) mira todas sus generaciones;
    - **los metadatos** (`FileMetadata`): su guardado es `create` del temporal, `rename` sobre el archivo y `sync_dir`, y su borrado, `remove` del archivo y del temporal, con los mismos casos. Su lectura (`open_read`) es del Lote 3;
    - **`create_dir_all` y `sync_dir` de `open_room`,** también del padre de un `data_dir` de un solo nombre: la ruta vacía, que es la raíz.
  - **La capa nativa y las de prueba no cambian de comportamiento:** suman el método, que en las tres da `true`, y los traits siguen pidiendo `Send + Sync` en nativo. El clippy y la suite del workspace pasan sin tocar nada.
  - **INV-L7:** todos los pasos de una operación (los listados, los borrados y el `move()` de un reemplazo) corren en su tarea, con el turno, hasta el fin: ningún paso queda en el future del llamador después de su primer `poll`.
- **Desvíos y precisiones.**
  - **Las aperturas, en este lote,** dan `Unsupported`, y `OpfsFile` es un tipo sin valores, con los métodos de `RoomFile` vacíos (`match self.never {}`): el plan no decía cómo quedaban. El motor sobre la capa es del Lote 4, así que nada las llama.
  - **El sufijo** es `~` seguido de la generación en decimal canónico, desde 1. Una entrada con `~` que no es una generación de la capa (`a~0`, `a~01`, `a~+1`, `a~b~1`) no es de ningún nombre: ninguna operación la lista ni la borra. La capa rechaza `~` en cualquier nombre de una ruta, también en los directorios, así que tampoco el directorio que elige la aplicación puede tenerlo. Ningún nombre real lo tiene (el test de arriba).
  - **El turno es uno por Worker, de todas las operaciones de la capa,** y no uno por nombre: el plan lo dejaba abierto. Es más simple, y una operación espera también a las de otro `OpfsFiles` del mismo Worker (por ejemplo, las de un motor soltado con una operación en vuelo). El costo es que las promesas de dos salas no se solapan; si hiciera falta, un turno por nombre (dos en un `rename`, tomados en orden) es una optimización de la capa. El llamador toma el turno antes de lanzar la tarea, así que una operación soltada antes de su turno no corre nunca.
  - **La tarea de `sys` es solo de wasm32.** En nativo no hace falta en este lote: `NativeFiles` sigue en el pool bloqueante, y las tareas del motor siguen con `tokio::spawn` hasta el Lote 4, que les da su forma en los dos. Da un `oneshot::Receiver`: si la tarea no llega a mandar su resultado, la capa da un error, nunca un pánico.
  - **Una ruta absoluta se rechaza,** como una que sale de la raíz: las rutas de la capa son relativas a la raíz del OPFS. Se parten por `/` como texto, igual en wasm32 y en los tests nativos de Windows.
  - **`exists` en un directorio que falta da `false`,** y `list` incluye los directorios, como en nativo (`try_exists` y `read_dir`).
  - **Los errores que DC39 no nombra:** un `TypeError` (un nombre inválido) a `InvalidInput`, y `NotAllowedError` y `SecurityError` a `PermissionDenied`; `TypeMismatchError` y `UnknownError`, a `Other`.
  - **El error *tomado* es de tipo `ResourceBusy`,** y `FileTaken::error` es público, para que otra capa, como la de prueba del cliente en el Lote 10, lo pueda dar.
  - **`rename` sobre sí mismo** mueve la generación más nueva a la siguiente. El motor no lo hace.
  - **La capa de prueba del storage** (`opfs_like.rs`) solo suma el método: sus negativas siguen siendo `Other`, y no el error *tomado*. El Lote 4 la ajusta (DEF-184).
  - **Los tests unitarios en wasm32:** ninguno nuevo. Lo interno que no depende del navegador va en tests nativos (`opfs_names.rs`), y lo que depende de él, en los pasos; los del storage en wasm32 los prepara el Lote 3. Los del arnés corrieron con el chromedriver del entorno (147, con Chrome 153) y `--disable-build-check`, como en el Lote 1.
  - **El tamaño es un desvío de la regla 1 de `docs/METODOLOGIA.md` §4** (unas 300 líneas de producción como máximo, y si se pasa, se parte): son 380. No se parte porque 44 son el relleno de las aperturas, que el Lote 3 reemplaza, y porque la revisión de delta ya se hizo sobre el lote entero: partirlo ahora sería revisar dos veces lo mismo. El corte que tendría está arriba, en *Qué entró*.
  - **Firefox corrió en la CI, no acá.** El proxy del entorno no deja bajar el de Playwright (un 403 de `cdn.playwright.dev` y de `playwright.download.prss.microsoft.com`). El coordinador lanzó la CI a mano. El lote corregido, con los arreglos de las dos rondas de la revisión de delta, pasó en Firefox en el run 45732770653 (el 154, sobre `c09047f`). Antes de la revisión pasó en el run 45727060096 (el 153, sobre `2e49aca`: este lote antes de sus arreglos). Los dos son un `workflow_dispatch` de `ci.yml` en la rama de trabajo, con los spikes de esa rama, y en los dos pasaron los 9 jobs; en el de `browser`, el Firefox 155 de Playwright pasó los 14 pasos y el self-check. Ahí Firefox es más lento: `files_write` tardó 906 ms (57 en Chromium), y `server_offline`, 15 s.
  - **`rename` usa un solo handle cuando el origen y el destino están en el mismo directorio** (de la revisión de delta, por lo lento de Firefox), y no guarda el de la raíz entre operaciones. Sigue listando el directorio dos veces, una por nombre: usar el mismo listado para los dos es una optimización posible, que el Lote 4 mide.
  - **Lo que no se prueba, de la revisión de delta: tres ramas sin test, leídas a mano.**
    - Un `js_sys::Error` sin nombre (un `TypeError`): la capa rechaza antes los nombres que se sabe que un navegador no toma, y no se conoce otro con el que provocarlo; su caso de `error` es una red de seguridad. El test unitario de `error` pediría tests del storage en wasm32, que prepara el Lote 3.
    - Un error del listado en `exists`, después de obtener el directorio: solo se provoca borrando el directorio en medio del listado, y entonces `false` también sería una respuesta correcta.
    - Un `remove` cuyas generaciones listadas ya no están, todas: tiene que dar `NotFound`, y la mutación que da `Ok` sobrevive. Provocarlo pide borrarlas todas entre el listado y el primer `removeEntry`.
  - **El paso 1 de `rename` sigue cortando en un `NotFound`** de una generación vieja del origen que ya no está, a diferencia de `remove`: un `rename` que falla no cambia ningún valor, y saltarla pediría otro test de carrera.
  - **Los pasos conocen el formato de las entradas** (`nombre~N`), porque fabrican lo que deja un corte: es el formato en el OPFS, que documenta `ARCHITECTURE.md` §8, y no un detalle interno.
  - **`INVARIANTES.md`, versión 52:** INV-L7 sobre el OPFS, ✅ en las operaciones del directorio (en su texto y en el resumen), y en §3.2, el reemplazo y el borrado de la capa, probados en la CI en Chromium y Firefox. S1 a S8 siguen 🆕 sobre el OPFS hasta que el motor corra ahí (Lote 4).
- **La revisión de delta** la hizo un agente aparte, de contexto fresco y solo lectura, que probó en su propia copia con sus propias mutaciones. **Salió en verde en su segunda ronda.**
  1. **Primera ronda: ningún Alto, 1 Medio y 9 Bajos, corregidos o anotados.** Firefox, verificado en la CI sobre el lote de antes de los arreglos (el run 45727060096, arriba). El revisor corrió 23 mutaciones: sobrevivieron 7 contra los pasos y 3 contra los tests nativos, estas tres equivalentes.
     - **El Medio:** que la operación siguiente espere también los últimos pasos de una soltada no lo probaba ningún paso: una mutación que suelta el turno apenas resuelve el `move()`, antes de borrar las viejas del destino, pasaba los seis. Ahora `files_dropped` termina con el test del revisor, un `rename` soltado sobre un destino con 200 generaciones, y la mutación muere.
     - **Los Bajos:**
       - **B1,** un `rename` entre directorios sin test, con tres mutaciones que sobrevivían (el handle del origen, el listado del destino y el `move()`, en el directorio equivocado): un caso en `files_write`, de `data/x` a `data/deep/x`, y mueren las tres;
       - **B2,** `remove` cortaba en una generación vieja que ya no estaba, y podía dar `NotFound` con la más nueva todavía ahí: ahora la salta, y da `NotFound` solo si no borró ninguna. Se provoca en `files_errors`, borrando una generación desde el paso mientras la capa borra;
       - **B3,** las rutas: `remove("x/")` borraba un directorio vacío, `exists("f/")` y `exists("f/.")` daban `true` con `f` un archivo, y `create_dir_all("n/b\\c")` creaba `n` antes de fallar. Ahora se rechazan la ruta de un archivo que termina en `/` o en `.`, un nombre con `\` o NUL, y un prefijo con `~`, antes de tocar nada, con su test nativo, sus casos en `files_refused` y el docstring del módulo;
       - **B4,** dos mutaciones que sobreviven y no se pueden provocar (un `js_sys::Error` sin nombre y un error del listado en `exists`): anotadas en los desvíos;
       - **B5,** `rename` busca los dos directorios antes del paso 1, así que uno a un directorio que falta no toca nada; `files_errors` lo exige;
       - **B6,** `ARCHITECTURE.md` decía que la capa sabe por el listado que falta el directorio del destino: lo sabe por el `NotFoundError` de `getDirectoryHandle`;
       - **B7,** el docstring de `sys::sync_dir`, que decía que en WebAssembly no hay sistema de archivos;
       - **B8,** la nota: las líneas recontadas con un mismo criterio, el tamaño como desvío explícito, y Firefox con el resultado del run;
       - **B9,** Firefox, más lento: un solo handle del directorio en un `rename` dentro de un directorio, y la medición de abrir decenas de salas en Firefox, en el Lote 4.
     - **El turno, para los lotes que siguen,** en las precisiones de los Lotes 3 y 10.
  2. **Segunda ronda, acotada a los arreglos: verde, con ningún Alto ni Medio y 4 Bajos, corregidos o anotados.** El revisor corrió 14 mutaciones: mueren 13, y sobrevive la del B4. Verificó también la CI de `main`, en los runs 151 y 152.
     - **B1,** Firefox sobre el lote corregido: el run 45727060096 era de antes de los arreglos. El coordinador lo volvió a lanzar sobre el lote cerrado, en el run 45732770653, en verde en sus 9 jobs, y la nota, `INVARIANTES.md` y `ARCHITECTURE.md` lo citan, junto al de antes de la revisión.
     - **B2,** una afirmación de más: el plan decía que la capa rechaza antes todo nombre que un navegador no toma. Ahora el plan, el docstring de `opfs.rs` y `ARCHITECTURE.md` nombran los que se conocen (`/`, `.`, `..` y el vacío, del paso S1, y `\`, en Chromium, del sondeo de la primera ronda), dicen que NUL se rechaza por precaución, sin probar, y que un navegador puede rechazar otro nombre con un `TypeError`, que sale como `InvalidInput`.
     - **B3,** el paso del B2 de la primera ronda podía colgarse hasta el plazo de 60 s si el `remove` fallaba antes de borrar `v`. Ahora corre con un `select`: si el `remove` termina antes de que el paso borre `v~250`, el paso falla enseguida y dice con qué terminó. Con un `remove` que falla antes de borrar, el paso falla en 128 ms con ese mensaje.
     - **B4,** la rama de `remove` cuyas generaciones listadas ya no están, todas, sin test: anotada en los desvíos, con las dos de la primera ronda, como leída a mano.
     - **La coherencia del paso 1 de `rename`,** que corta en un `NotFound` de una generación vieja del origen: queda así, anotada en los desvíos.
- **Chequeos:**
  - repetidos después de los arreglos de las dos rondas de la revisión de delta: fmt de la raíz; clippy del workspace con `-D warnings`; el build y el clippy de wasm32 de `zemdb-core`, `zemdb-storage` y `zemdb-client`, con `-D warnings`; y doc con `-D warnings` (también el del storage en wasm32, que la CI no corre);
  - los tests del storage (340, sin fallas, 22 ignorados), después de las dos rondas, y la suite del workspace (1726, sin fallas, 57 ignorados), después de la primera: la segunda no toca código nativo. Antes de la revisión de delta, en una corrida con las mutaciones del arnés corriendo en la misma máquina, falló una vez `the_heartbeats_of_the_worker_leave_behind_an_http2_connection_that_stops_answering`, el de la pausa de 1,5 s que ya registran las notas del 6a y del 7: pasó tres veces solo, y con su binario entero;
  - los `--ignored` del cliente en release, con 7 tests y sin fallas, antes de los arreglos, que no tocan el código nativo;
  - **del arnés:** fmt, el clippy de los pasos en wasm32 (`--all-targets`, con `-D warnings`), `build.sh`, y el runner (14 pasos) y su self-check, en el Chromium que Playwright toma en este entorno (*precisión del Lote 3:* un headless shell, un build 157, y no el Chromium 153.0.8010.12 de Playwright 1.63, que usa la CI), también después de los arreglos; y, antes de ellos, que no tocan el servidor de pruebas, el clippy del servidor, sus tests (2), `npm ci` y el test unitario en wasm32, en Chrome 153;
  - **los pasos nuevos, repetidos:** el runner entero, diez veces seguidas antes de la revisión de delta, cinco veces seguidas después de los arreglos de la primera ronda (con el clippy y la suite del workspace corriendo en la misma máquina) y tres después de los de la segunda, todas en verde;
  - **la CI con Firefox:** el lote corregido, en el run 45732770653; antes de la revisión de delta, el run 45727060096; los dos en verde en sus 9 jobs (arriba). La CI de `main`, en verde sobre `acd89b6` (el run 151) y sobre `b9f9548` (el 152);
  - **en `main`, con el lote aplicado:** fmt; los dos clippy de la raíz (el del workspace y el del cliente sin features por defecto); el build y el clippy de wasm32; doc con `-D warnings`; la suite del workspace (1725 pasan y 57 ignorados, con una falla: `a_panic_in_a_file_operation_stops_the_room_and_the_next_request_reopens_it`, de `zemdb-server`, que no depende de `zemdb-storage`, con un `Timeout` del actor bajo la carga de la suite; pasó quince veces sola, y la suite del servidor entera, 639 tests, pasó después); los `--ignored` del cliente en release (7); y del arnés, su fmt, sus dos clippy, los tests del servidor de pruebas (2), la compilación del test unitario en wasm32, `build.sh`, `npm ci`, el runner (14 pasos) y su self-check, en Chromium.

### Lote 3 · La capa del OPFS: los archivos (`zemdb-storage`) · ✅ Hecho (27 de Noviembre de 2026)
**Objetivo:** el resto de `OpfsFiles`: abrir y crear con su lock, la E/S posicional, renombrar el archivo abierto y el handle muerto (DC39).

**Archivos:** `crates/storage/src/files/opfs.rs` y sus tests unitarios en wasm32; lo que esos tests necesitan, porque los de hoy no compilan en wasm32 (usan `#[tokio::test]`, `zstd`, el motor en disco y los *fail points*): los módulos de tests nativos de `crates/storage/src/` fuera de wasm32, `wasm-bindgen-test` en `crates/storage/Cargo.toml` (solo para los tests en wasm32), el runner (`.cargo/config.toml`) y el comando de la CI (`cargo test --target wasm32-unknown-unknown -p zemdb-storage --lib`); los pasos del arnés y `ARCHITECTURE.md` §8.

**Qué se hace:** `open_locked`, `create_locked`, `create` y `open_read`, con un `FileSystemSyncAccessHandle` cada uno y sobre la generación más nueva de su nombre (`create_locked`, `AlreadyExists` si existe alguna; `open_read` y `create`, el error *tomado* de DC39 con otro handle sobre el archivo); `read_at`, `write_at`, `size`, `set_len`, `sync_data` y `sync_all`; renombrar el archivo abierto, con las generaciones del destino (DC39); el handle muerto.

**Tests exigidos:**
- **Pasos del arnés:** leer y escribir en un offset, el largo, truncar, `flush`, y lo escrito después de recargar; `open_locked` y `create_locked` dan `None` con otro handle sobre el archivo, también desde un Worker de otra página; `create_locked` sobre uno que existe es `AlreadyExists` y no lo trunca; renombrar el archivo abierto lo sigue, también sobre un nombre que existe (a su generación siguiente), y el handle sigue sirviendo; cada apertura usa la generación más nueva de su nombre, y `create_locked` da `AlreadyExists` con cualquier generación; `open_read` y `create` dan el error *tomado* con otro handle sobre el archivo; `read_at` más allá del fin es `UnexpectedEof`, nunca una lectura corta.
- **Tests unitarios en wasm32:** el handle muerto (la reapertura después del `move()` que falla, forzada con un mecanismo de `opfs.rs` bajo `cfg(test)`: los *fail points* compilan en wasm32 recién en el Lote 4): toda operación siguiente falla, y no escribe en ningún archivo.

**Interacciones a revisar:** los requisitos de DC17; la búsqueda de frames y el replay, que no admiten una lectura corta; la rotación y su vuelta atrás.

**Precisiones del Lote 2 (el turno de la capa).** El turno es un lock hoja: dentro de `run` solo se esperan promesas del OPFS, y nunca un `run` dentro de otro, así que el `rename` del handle abierto no puede llamar a `OpfsFiles::rename` desde su tarea, sino a los pasos sin turno. Un handle toma primero su permiso y después el turno. Un `OpfsFile` que sale de una apertura soltada cierra su `FileSystemSyncAccessHandle` en `Drop`, porque `sys::spawn` descarta el resultado si nadie lo espera: si no, el archivo quedaría tomado hasta que termine el Worker.

**Documentos:** `ARCHITECTURE.md` §8; `INVARIANTES.md`: el estado de L7 sobre el OPFS.

**Entrega:** `feat(storage): the files of the file layer over the Origin Private File System (Fase 4.5, Lote 3)`.

**Hecho** (27 de Noviembre de 2026).
- **Qué entró.**
  - **Los archivos de `OpfsFiles`** (`crates/storage/src/files/opfs.rs`, solo en wasm32), que reemplazan las aperturas en `Unsupported` y el `OpfsFile` sin valores del Lote 2:
    - **las aperturas:** `open_locked`, `create_locked`, `create` y `open_read` abren cada una un `FileSystemSyncAccessHandle`, en el modo por defecto, sobre la generación más nueva de su nombre, en la tarea de la capa y con su turno, como las del directorio. La generación la elige `opfs_names::opened`, con lo que pide cada apertura (`Opening`): `open_locked` crea el nombre mismo si no tiene ninguna; `create_locked` da `AlreadyExists` si tiene alguna, sin abrirla ni truncarla, y si no, crea el nombre: verifica y después crea, porque el OPFS no tiene un *create-new* atómico; `create` vacía la más nueva (`truncate(0)`); y `open_read` da `NotFound` sin ninguna. Otro handle sobre el archivo (el error de `createSyncAccessHandle`, `NoModificationAllowedError` o `InvalidStateError`, en `TAKEN_BY_OPEN`, nuevo en `opfs_names.rs`, del que `TAKEN_BY_MOVE` pasa a ser un alias) es `None` en `open_locked` y `create_locked`, y el error *tomado* en `create` y `open_read`. Un directorio que falta es `NotFound`, y una ruta que la capa no toma, `InvalidInput`, antes de la primera promesa;
    - **la E/S,** con las llamadas síncronas del handle: `read_at` da exactamente lo pedido: una de largo 0 da nada, en cualquier offset, como en nativo (de la segunda ronda de la revisión de delta); una lectura que termina más allá del largo del archivo (`getSize`) es `UnexpectedEof` antes de reservar nada, y una que da menos se repite, y es `UnexpectedEof` si el archivo termina antes, nunca una lectura corta; `write_at` escribe todo, repitiendo una escritura que escribe menos (y `WriteZero` si una no escribe nada), y una que terminaría más allá de 2^53, donde los offsets del navegador dejan de ser exactos, es `InvalidInput` antes de escribir nada; un conteo del navegador mayor que lo pedido es un error con el conteo, nunca una lectura o una escritura hecha (de la revisión de delta); `size`, `set_len`, `sync_data` y `sync_all` son `getSize`, `truncate` y `flush`. Las repeticiones son funciones de `opfs_names.rs` (`read_exact` y `write_all`) que reciben la lectura o la escritura, para probarlas en nativo con una que da menos o que cuenta de más;
    - **el `rename` del archivo abierto,** con los pasos de `OpfsFiles::rename`, que pasan a funciones sin el turno (`directories`, `replace_first`, `move_entry` y `replace_last`), y que usan los dos `rename`. Busca los directorios del origen y del destino; da el paso 1 (borra las generaciones del nombre del archivo que no son la abierta) antes de cerrar el handle, así un `rename` que falla ahí deja el archivo abierto, como en nativo; cierra el handle, mueve el archivo a la generación siguiente del destino, una entrada libre, lo reabre sobre el mismo `FileSystemFileHandle`, que lo siguió, y da el paso 3. Un `move()` que falla reabre el archivo donde estaba, si el listado todavía tiene ahí su entrada, y el `rename` da el error del `move()`; si no la tiene (un `move()` rechazado con efecto, después del cual Safari crearía de nuevo el origen, vacío, al reabrirlo), el archivo queda muerto (de la revisión de delta);
    - **el handle muerto:** si la reapertura falla, el `rename` da su error y el archivo queda sin `FileSystemSyncAccessHandle`. Toda operación siguiente falla (`... is closed: a rename could not reopen it`) sin llamar al navegador, así que no escribe en ningún archivo, y un `rename` siguiente no mueve nada;
    - **el contrato de `RoomFile`,** como en `NativeFile`: las operaciones de un archivo corren de a una y en orden, cada una con el permiso del archivo (un `tokio::sync::Mutex`); el `rename`, en una tarea propia con el permiso y después el turno, y su error, si nadie lo esperó, lo guarda el archivo y lo da la operación siguiente, que no corre (con un `Outcome` que, soltado, guarda también el error que el `rename` ya había mandado); y lo que el archivo comparte con su `rename` en vuelo (`Open`: el `FileSystemFileHandle`, el handle de acceso, dónde está y el error que nadie esperó, en un `Rc<RefCell<..>>` que nunca se toma a través de un `await`) cierra el `FileSystemSyncAccessHandle` en su `Drop`, cuando lo sueltan el archivo y su última operación. El de una apertura soltada lo suelta la tarea, antes de que corra la operación siguiente;
    - **el mecanismo de los tests unitarios** (en `opfs.rs`, bajo `cfg(test)`): tres *puntos* de una operación, donde un test arma una acción, un future que la operación espera en su tarea, una sola vez, sobre una entrada: `open`, entre obtener el handle de la entrada y abrirlo; y `rename.closed` y `rename.moved`, antes y después del `move()` del `rename` de un archivo abierto. Fuera de los tests, `point` no hace nada.
  - **Los tests unitarios del storage en wasm32,** que no existían:
    - los módulos de tests nativos de `crates/storage/src/` que compilaban en wasm32 (los de `snapshot.rs`, `files/mod.rs`, `files/opfs_names.rs` y los tres de `memory/`, con el `indexes` de `memory/state.rs` que solo usan ellos) pasan a `cfg(all(test, not(target_arch = "wasm32")))`; los del motor, de la capa nativa y de los *fail points* ya estaban fuera de wasm32 con su módulo;
    - `wasm-bindgen-test` 0.3.79, la versión del lock del arnés, en las dev-dependencies del storage solo para wasm32. El `Cargo.lock` de la raíz suma sus paquetes, que ya estaban en el del arnés: `wasm-bindgen-test`, su macro y su `shared`, `minicov`, `cast`, `libm`, `num-traits`, `oorandom` y `autocfg`;
    - el runner, en `.cargo/config.toml` de la raíz (nuevo), solo para `wasm32-unknown-unknown`: en nativo no cambia nada, y el del arnés, en `tools/browser/.cargo/`, dice lo mismo;
    - el comando, `cargo test --target wasm32-unknown-unknown -p zemdb-storage --lib`, en el job `browser` de la CI, desde la raíz y con el chromedriver del runner, como el del crate de pasos; y su clippy, `cargo clippy -p zemdb-storage --target wasm32-unknown-unknown --lib --profile test -- -D warnings`, en el job de WebAssembly (`--all-targets` compilaría también los tests de integración, que son nativos).
  - **`crates/storage/Cargo.toml`:** cuatro features más de `web-sys` (`FileSystemFileHandle`, `FileSystemGetFileOptions`, `FileSystemReadWriteOptions` y `FileSystemSyncAccessHandle`), además de la dev-dependency.
  - **El arnés:** la suite `handles` (`steps/src/handles.rs`, nueva, entre las de por defecto) y la suite `quota`, que el runner corre solo en Chromium, en un tercer contexto con la cuota chica (abajo); `within`, en `js.rs`, que hace fallar un paso que no termina en su plazo; los helpers de `files.rs`, compartidos con `handles.rs`; y su `README.md` (las suites y los tests unitarios del storage).
  - **351 líneas de producción netas**, contadas como en el Lote 2 (sin comentarios, líneas en blanco ni la declaración de los módulos de tests unitarios) y sin las 24 del mecanismo de los tests, bajo `cfg(test)`: en `opfs.rs` entran 280 y salen 50, de las que 44 son el relleno del Lote 2 (las aperturas en `Unsupported` y el `OpfsFile` sin valores) y 6 del `rename` por ruta, que pasan a los pasos factorizados; en `opfs_names.rs` entran 78 y sale 1. Son 358 nuevas y 7 que salen, sobre las 336 del Lote 2 sin su relleno. De ellas, 26 son de la primera ronda de la revisión de delta (el conteo de más, el largo antes de reservar, el fin de 2^53, el listado después de un `move()` que falla) y 2 de la segunda (la lectura de largo 0): antes eran 323. El lote cambia además 7 atributos `cfg` de los módulos de tests. El plan estimaba 220.
    - Lo de más está en lo que la estimación no contaba: el contrato de `RoomFile` como lo cumple `NativeFile` (el permiso, el error que nadie esperó con su `Outcome`, y el estado compartido con su `Drop`: unas 50), los pasos del reemplazo factorizados para los dos `rename` (unas 50, de las que salen 15 del `rename` por ruta) y lo que pasó a `opfs_names.rs` para probarlo en nativo (la generación de cada apertura y las repeticiones de la lectura y la escritura: unas 20 más que en `opfs.rs`, por sus firmas).
    - **Es un desvío de la regla 1 de `docs/METODOLOGIA.md` §4** (unas 300 como máximo, y si se pasa, se parte), de 51 líneas. No se partió: el `rename` del archivo abierto necesita las aperturas para probarse, y el corte dejaría el handle muerto, que es su caso, sin el resto. Si hubiera que partirlo, el corte sería las aperturas y la E/S, con el estado del archivo, su permiso y su `Drop` (unas 230), y el `rename` del archivo abierto, con los pasos factorizados, el handle muerto y el error que nadie esperó (unas 120).
- **Tests.**
  - **Unitarios en wasm32** (8, en `files/tests/opfs.rs`, en un Worker dedicado del Chrome 153 de este entorno):
    - `each_error_of_the_browser_is_an_io_error_of_its_kind_with_its_name`, el que dejó pendiente el Lote 2: un `DOMException` de cada nombre (`NotFoundError`, `QuotaExceededError`), un `TypeError`, que no es un `DOMException`, y un valor que no es un error, con su `ErrorKind` y su mensaje; y el error *tomado* solo si la llamada nombra el error (`InvalidStateError` lo es para `createSyncAccessHandle` y no para `removeEntry`);
    - `a_rename_that_cannot_reopen_the_file_leaves_it_dead_and_writes_nowhere`: en `rename.moved`, el test abre un handle del navegador sobre el destino, así la reapertura falla con el error real del navegador (`NoModificationAllowedError`). El `rename` da ese error, que no es el *tomado*; las siete operaciones siguientes fallan; en el directorio queda solo el destino, con lo que tenía; y el archivo muerto no lo tiene tomado;
    - `a_dropped_rename_that_cannot_reopen_gives_its_error_to_the_next_operation`: el mismo `rename`, soltado (`now_or_never`); la operación siguiente, un `rename` a `../x`, da el error de la reapertura antes de rechazar su ruta, y no corre; las demás, el del archivo muerto;
    - `a_move_that_fails_reopens_the_file_where_it_was`: en `rename.closed`, el test toma el archivo, que el `rename` acaba de cerrar, y lo suelta en `rename.moved`: el `move()` falla con el error *tomado*, el archivo sigue abierto en su nombre y su generación (escribe y lee), y un `rename` después lo mueve a otro nombre y deja la generación vieja del destino del que falló, que un nombre equivocado borraría en el paso 1;
    - `a_move_to_another_directory_that_fails_reopens_the_file_where_it_was` (de la segunda ronda de la revisión de delta): el mismo `move()` fallido, a otro directorio; la capa encuentra el archivo en el listado de su directorio, no en el del destino, y lo reabre;
    - `a_move_and_a_reopening_that_fail_leave_the_file_dead_and_not_taken` (de la revisión de delta): el test toma el archivo en `rename.closed` y no lo suelta; el `move()` y la reapertura fallan, el `rename` da el error de la reapertura, que no es el *tomado*, y el archivo queda muerto;
    - `a_move_that_fails_and_moved_the_file_leaves_it_dead_without_reopening_its_source` (de la revisión de delta): el `move()` falla con el archivo tomado, y en `rename.moved` el test lo mueve con otro handle al destino, como haría un `move()` rechazado con efecto; el `rename` no reabre el origen, da el error de la entrada que falta, y el archivo queda muerto. En Chromium, reabrir el origen también fallaría (no lo crea de nuevo, como Safari), así que el test lo distingue por el error;
    - `create_locked_is_none_for_a_file_taken_once_it_found_the_name_free`: en `open`, el test toma la entrada que `create_locked` acaba de crear, y `create_locked` da `None`, con el archivo creado; con el archivo ahí, `AlreadyExists`. Desde el arnés no se puede provocar: el archivo tiene que aparecer entre el listado y la apertura.
  - **Nativos** (de `opfs_names.rs`, 4 nuevos y 1 ajustado): `an_opening_opens_the_newest_generation_or_the_name_itself`; `a_read_repeats_until_it_has_every_byte_or_the_file_ends` y `a_write_repeats_until_it_wrote_every_byte`, con un archivo que lee y escribe de a pocos bytes, que el OPFS no da y por eso el arnés no prueba, y con lo que se rechaza antes de llamar al navegador: una lectura más allá del largo (también de `usize::MAX` bytes, que no se reservan) y una escritura más allá de 2^53; `a_count_larger_than_what_was_asked_is_an_error` (de la revisión de delta), con el conteo de Chromium 153; y `a_taken_file_is_the_error_that_each_browser_gives_it`, con `TAKEN_BY_OPEN`.
  - **Pasos del arnés** (la suite `handles`, sobre la capa real, con una recarga y la segunda página; cada paso falla si no termina en 20 s):
    - `handles_write`: `open_locked` crea el archivo; escribir en un offset, con ceros en el medio; el largo; leer en un offset; `read_at` más allá del fin o cruzándolo, y después de truncar, es `UnexpectedEof`, y con largo 0 al final da nada; truncar y extender (con ceros); una escritura de 300 KB, leída exacta; `sync_data` y `sync_all`; y cerrado el archivo, el navegador encuentra lo escrito en él y en ninguna otra entrada. `create` vacía un archivo que tenía algo, y crea uno que no existe (de la revisión de delta). Una escritura que termina en 2^53 llega al navegador y falla, dé el conteo que dé, sin escribir nada (el paso anota el error que vio: el del conteo en Chromium 153, `StorageFull` en el headless shell 157, y `WriteZero`, 0 bytes escritos, en Firefox 155), y una más allá se rechaza: corre en los dos navegadores, sin CDP (de la revisión de delta). `read_at` de largo 0 más allá del fin da nada, como en nativo, y uno de `usize::MAX` bytes es `UnexpectedEof` sin reservarlos (de la segunda ronda);
    - `handles_rename`, el `rename` del archivo abierto, como la rotación del WAL y su vuelta atrás: a un nombre libre y de vuelta, sobre un nombre con dos generaciones (a la siguiente, borrando las viejas), a otro directorio y dentro de él, con una generación vieja que quedó ahí (el archivo sigue a su directorio), desde un nombre con generaciones viejas sobre uno que existe, desde ahí con una generación vieja que quedó (que el `rename` siguiente borra), y desde un nombre con una generación más nueva que la abierta, que también se borra (los dos últimos casos de la revisión de delta, como el del directorio); el handle sirve después de cada uno. Un `rename` cuyo paso 1 falla (una generación vieja del origen abierta, con el error *tomado*), a un directorio que falta (`NotFound`) o a `..` (`InvalidInput`) no cambia ninguna entrada, y el archivo sigue abierto donde estaba;
    - `handles_after_reload`, después de recargar: lo que dejaron los dos pasos anteriores, leído con las aperturas (también el archivo que creó `create`), y una escritura más;
    - `handles_generations`: cada apertura abre la generación más nueva (`n~2`, con `n` y `n~2`); `create` la vacía y deja la vieja; `create_locked` da `AlreadyExists` con cualquier generación (también con solo `m~3`) y no trunca nada; en un nombre libre crea el nombre mismo, vacío; `open_read` de un nombre sin generaciones, y las cuatro aperturas en un directorio que falta, dan `NotFound`; y con una ruta que no se toma (`n~2`, `..`, o que termina en `/`), `InvalidInput` sin tocar nada;
    - `handles_hold` (en la primera página), `handles_taken` (en la segunda), `handles_release` (en la primera) y `handles_free` (en la segunda): con `t` abierto por la capa en la primera página, y con un handle del navegador sobre `s`, `open_locked` da `None`, `create_locked` `AlreadyExists`, y `open_read` y `create` el error *tomado*, sin cambiar nada, también desde el Worker de la segunda página, que verifica que no es la que tiene el archivo; cerrado en la primera, la segunda lo abre (con un plazo de 5 s para que el navegador lo suelte) y lee lo que escribió la primera. El arnés ya tenía lo que hacía falta: la segunda página y los pasos que conservan un handle entre pasos (`tabs`);
    - `handles_dropped`: una apertura soltada termina y cierra su archivo, que la siguiente abre (`open_locked`, `create`, que lo vacía, `open_read` y `create_locked`); el `rename` de un archivo abierto, soltado, termina, y la operación siguiente lo espera y escribe en el archivo renombrado; uno soltado que falla (una generación vieja abierta) da su error a la operación siguiente, que no corre, también si se soltó después de mandar su error; y un archivo soltado con su `rename` en vuelo se cierra cuando el `rename` termina.
  - **La cuota llena, en Chromium** (la suite `quota`, `handles_quota`): el runner fija la cuota del origen en 4 MiB por CDP (`Storage.overrideQuotaForOrigin`), en un contexto propio, antes de abrir la página; una escritura de 1 KiB sale bien, y una de 16 MiB falla, y el archivo sigue: conserva lo escrito antes y `sync_all` sale bien. El navegador lo dice de dos formas, y el paso acepta las dos y anota cuál vio (corregido en la revisión de delta, que encontró la segunda en la CI):
    - en Chromium 153, el de Playwright 1.63 y el de la CI, la escritura no lanza nada y da como escritos 4294967288 bytes: la capa lo rechaza con el error del conteo (`Other`, con el conteo en el mensaje);
    - en el headless shell de Chromium 157 que Playwright toma en este entorno, lanza `QuotaExceededError`, que es `StorageFull`.

    En los dos, la escritura deja escrita una parte, hasta casi la cuota (el archivo quedó con 4193650 bytes en las corridas de este lote). Sin la cuota chica, el paso falla (la escritura de 16 MiB sale bien), así que es la cuota la que la hace fallar. **En Firefox no corre:** Playwright no tiene CDP para Firefox, y el arnés no tiene otra forma de fijar la cuota del origen; el runner lo dice en su salida. El `ErrorKind` de `QuotaExceededError` lo prueban además el test nativo de `error_kind` y el unitario en wasm32.
  - **La suite del workspace, sin tocar ninguna expectativa:** en nativo, el lote solo cambia los `cfg` de los módulos de tests y suma tests nativos de `opfs_names.rs`.
- **Mutaciones inversas: 68 corridas, en una copia, cada mutación sola, contra los tests unitarios del storage en wasm32 y el runner entero en Chromium (las 16 de la revisión de delta, en el 153), y las de `opfs_names.rs` también contra sus tests nativos. Mueren todas salvo 2, que no se pueden provocar; otras 3 sobrevivieron, pidieron un test y ahora mueren; y la revisión de delta encontró otras, que ahora mueren (la cuarta tanda).**
  - **La primera tanda, 36: mueren 33.**
    - **Las aperturas:** abrir la generación más vieja (`handles_rename`: el `q` abierto lee `q0`); `create_locked` que no mira las generaciones (`handles_generations` y `create_locked_is_none…`), o que trunca la más nueva antes de dar `AlreadyExists` (`handles_generations`: `n~2` queda vacío); `open_locked` que no crea (`handles_write` y los unitarios); `create` que no vacía (`handles_write`); `open_locked` o `create_locked` con el error *tomado* en lugar de `None` (`handles_hold`, y `create_locked_is_none…`); cualquier error de una apertura como `None` (`handles_generations`); y `open_read` y `create` sin el error *tomado* (`handles_hold`).
    - **La E/S:** `read_at` que da una lectura corta, o un `Ok` sin el resto más allá del fin (`handles_write`); `write_at` que escribe la mitad (`handles_write`); `set_len` que no hace nada, y `size` en 0 (`handles_write`).
    - **El `rename` del archivo abierto:** que cierra antes del paso 1 (`handles_rename`: el paso 1 que falla deja el archivo muerto); que mueve sobre el nombre existente, que Chromium reemplaza (`handles_rename`, las entradas); que no reabre (`handles_rename`), o que no reabre después de un `move()` que falla (`a_move_that_fails…`); que da el paso 1 antes de buscar el directorio del destino (`handles_rename`: el `rename` a un directorio que falta borra `u`); que no borra las viejas del destino (`handles_rename`); y que no sigue el nombre, o la generación, del archivo (`handles_rename`, el `rename` desde `r~1` con una `r` que quedó, que pidió ese caso al escribir los pasos).
    - **El handle muerto:** que, si no puede reabrir, abre otra vez el nombre del origen con `create`, como Safari con una entrada que se movió, y escribe ahí (`a_rename_that_cannot_reopen…`); y que renombra (los dos unitarios del handle muerto).
    - **El orden y los errores que nadie esperó:** el error de un `rename` soltado que se pierde, en su tarea (`handles_dropped` y `a_dropped_rename…`) o en el `Outcome`, ya mandado (`handles_dropped`, su último caso, que se sumó para eso); las llamadas síncronas que ignoran el error que nadie esperó (`handles_dropped`), o que no toman el permiso (`handles_dropped`: la escritura corre antes del `rename` soltado que falla, y su error lo da la lectura siguiente); un `Drop` que no cierra (`handles_write`); un `run` dentro de otro, que se cuelga (`handles_rename`, a los 20 s de `within`, y los unitarios, por el plazo de `wasm-bindgen-test-runner`); y las aperturas sin el turno (`handles_dropped`).
    - **Los errores:** `QuotaExceededError` a `Other` (`handles_quota` y el unitario de `error`), y un `TypeError` sin su nombre (el unitario de `error`).
  - **Las que sobrevivieron a la primera tanda, 3:**
    - `read_at` y `write_at` que no repiten una lectura o una escritura que da menos: el OPFS lee y escribe en una llamada todo lo que puede, así que el arnés no tiene cómo dar una que da menos. Pidieron un test: las repeticiones pasaron a `opfs_names.rs` (`read_exact` y `write_all`), con sus tests nativos con un archivo que lee y escribe de a pocos bytes, y mueren (abajo).
    - `sync_data` y `sync_all` sin `flush()`: lo escrito se lee igual después de recargar, porque cerrar el handle no pierde nada; solo un corte del navegador lo vería, y lo midió S1 (lo escrito con `flush()` sobrevive a un corte). Queda sin test.
  - **La segunda tanda, 10, sobre el lote con la generación de cada apertura y las repeticiones en `opfs_names.rs`: mueren 9.** La generación más vieja, `create_locked` que no mira las generaciones y `open_read` que crea (los nativos y `handles_*`); las repeticiones que no repiten, o que cuentan todo lo pedido (los nativos); una lectura corta y un `Ok` vacío más allá del fin (`handles_write` y los nativos); y una lectura en otro offset (`handles_write`). Sobrevivía `write_at` que da `Ok` cuando una escritura no escribe nada, que el OPFS no hace: el `WriteZero` pasó a `write_all`, con su test nativo.
  - **La tercera tanda, 6, sobre el lote final: mueren 5.** Las repeticiones que no repiten, la lectura corta, el `WriteZero` que se ignora (los nativos, y la lectura corta también `handles_write`), y el error de una escritura a `Other` (`handles_quota`). **Sobrevive** el error de una lectura a `Other`: ninguna lectura de un handle abierto falla en el navegador sin cortarlo, y la función que arma el error es la misma que la de la escritura, que muere.
  - **La cuarta tanda, 13, las de los hallazgos de la primera ronda de la revisión de delta, sobre el lote corregido, con el runner en el Chromium 153 local: mueren las 13.** `create` que no crea (`handles_write`); el conteo recortado, en la escritura (`handles_write`, la escritura en 2^53, y el nativo) y en la lectura (el nativo); con el `move()` y la reapertura fallidos, el error *tomado* del `move()` (`a_move_and_a_reopening_that_fail…`); el paso 1 que borra solo las más viejas (`handles_rename`, `p~4`); el archivo que no sigue a su directorio (`handles_rename`, la `x` que quedó en `deep`); el nombre y la generación que cambian aunque falló el `move()` (`a_move_that_fails…`, la `k` del destino); la ruta validada antes del error que nadie esperó, y ese error mirado antes del permiso (`a_dropped_rename…`, el `rename` a `../x`); reabrir un origen que ya no está (`a_move_that_fails_and_moved…`); y en `read_exact`, la reserva antes de validar, el largo que no se valida, y en `write_all`, el fin de 2^53 que no se valida (los nativos).
  - **La quinta tanda, 3, las de la segunda ronda de la revisión de delta: mueren las 3.** El listado de después de un `move()` fallido sobre el directorio del destino (`a_move_to_another_directory…`); `read_at` sin el largo real del archivo (`handles_write`, la lectura de `usize::MAX` bytes, cuya reserva entra en pánico); y la lectura de largo 0 que se chequea contra el largo (`handles_write` y el nativo). Las 2 primeras son las que sobrevivían a la segunda ronda.
- **Interacciones revisadas.**
  - **Los requisitos de DC17** (§1 del plan de 4.1):
    - `read_at` da exactamente lo pedido o un error: la búsqueda de frames (`find_valid_frame_in`) y la lectura por ventanas del replay (`WindowedReader`, que corta la ventana en `start..start + len`) entraban en pánico con una lectura corta, y en wasm32 un pánico aborta el módulo. Los dos piden lo que el largo del archivo dice que hay, así que un `UnexpectedEof` solo sale si el archivo se achicó en el medio, y es un error de E/S, no un pánico;
    - `create_locked` falla si existe alguna generación, y nunca adopta ni trunca un archivo: verifica y después crea, lo que es seguro bajo el lock de la sala y con una pestaña por directorio (DC41, Lote 10);
    - el handle muerto falla en toda operación siguiente y nunca escribe en otro archivo, así que la escritura siguiente del WAL (un `apply_batch`, una compactación o el truncado de un `apply_snapshot`) falla y marca la sala fallida, y la recuperación pliega `.wal.compacting`. Que el motor lo haga sobre el OPFS lo prueba el Lote 4 (DEF-185), con el motor ahí.
  - **La rotación del WAL y su vuelta atrás** (`rotate_wal_to_compacting` y `roll_back_rotation`): renombran el WAL con su handle, que lo sigue. Un `rename` que falla en el paso 1, o con el directorio que falta, deja el WAL abierto donde estaba, como espera la rotación, que propaga el error sin marcar la sala fallida; uno cuyo `move()` falla, también, porque lo reabre donde estaba. Si `create_locked` del WAL nuevo lo dejó creado (un `None`), la vuelta atrás renombra `.wal.compacting` a su generación siguiente y borra en el paso 3 la vieja, vacía. Una reapertura que falla deja el handle muerto: la rotación da el error, y la escritura siguiente marca la sala fallida.
  - **Lo que el motor da por hecho de una apertura:** el `.lock` y el WAL con `open_locked` (`None` es `RoomLocked`); los temporales de un snapshot con `create_locked`, que nunca tienen generaciones; la copia de una absorción con `create`; y el `.snap` y `.wal.compacting` con `open_read`, sobre su generación más nueva, que es su valor.
  - **El turno es un lock hoja:** dentro de una tarea con el turno solo se esperan promesas del OPFS. El `rename` de un archivo abierto toma primero el permiso del archivo y después el turno, y corre los pasos del reemplazo sin `run`, así que nunca hay un `run` dentro de otro; `create` vacía el archivo con la llamada del handle, sin tomar su permiso (que nadie más tiene) dentro de la tarea. Ninguna tarea que tiene el turno espera un permiso, así que el orden no puede trabarse.
  - **INV-L7:** todo lo que es una promesa de una operación de un archivo (obtener el handle de la entrada, `createSyncAccessHandle`, el `move()` y los listados y borrados del `rename`) corre en su tarea, con el turno, hasta el fin; un archivo, con su handle y su lock, vive hasta que termina su última operación.
  - **La capa nativa y las de prueba no cambian,** y el clippy y la suite del workspace pasan sin tocar nada. `.cargo/config.toml` de la raíz solo da un runner para wasm32: el `cargo test` nativo no lo usa.
- **Desvíos y precisiones.**
  - **Las operaciones síncronas de un archivo no toman el turno,** solo el permiso del archivo: terminan en la misma llamada, sin promesas, así que no se solapan con ninguna otra operación de la capa, y con el turno esperarían, sin necesidad, las promesas de las otras salas. El permiso las ordena detrás de un `rename` soltado del mismo archivo.
  - **El estado del archivo se comparte con su `rename` en vuelo en un `Rc<RefCell<..>>`,** y el permiso es un `Arc<tokio::sync::Mutex<()>>` aparte: un `Arc` de un estado con objetos de JavaScript, que no son `Send`, lo rechaza `clippy::arc_with_non_send_sync`, y `lock_owned` pide un `Arc`. El `RefCell` nunca se toma a través de un `await`: cada operación lo toma durante su llamada síncrona, y el `rename`, entre sus promesas. Lo cierra el `Drop` del estado, no el del archivo, así el `FileSystemSyncAccessHandle` se cierra con la última referencia: la del archivo o la del `rename` en vuelo.
  - **El paso 1 del `rename` de un archivo abierto borra las generaciones de su nombre que no son la abierta,** también una más nueva que la abierta, que el motor no deja (nunca renombra sobre un archivo abierto): así el nombre del origen no queda con ninguna, como un `rename` en nativo.
  - **Una reapertura que falla da su propio error, no el *tomado*,** aunque el navegador diga `NoModificationAllowedError`: el *tomado* es lo que reintenta la apertura de DC41, y un `rename` no se reintenta.
  - **Un `move()` que falla reabre el archivo donde estaba,** si el listado tiene ahí su entrada, y el `rename` da el error del `move()`; el plan solo decía qué pasa si falla la reapertura. Si fallan los dos, el `rename` da el de la reapertura, que no es el *tomado* (el archivo no quedó como estaba: está cerrado), y el archivo queda muerto; si la entrada ya no está, también.
  - **Un conteo del navegador mayor que lo pedido es un error de tipo `Other`,** con el conteo y lo pedido en el mensaje, en la lectura y en la escritura (de la revisión de delta): no se lo toma por `StorageFull`, aunque en Chromium 153 venga de la cuota, porque el valor (4294967288, −8 como entero sin signo) es un detalle interno de Chromium que no está documentado. Una escritura que lo da pudo haber dejado escrita una parte, como cualquier escritura que falla: el motor la trata como un error de E/S.
  - **Una escritura que terminaría más allá de 2^53 se rechaza con `InvalidInput`** antes de llegar al navegador, y una lectura que termina más allá del largo del archivo es `UnexpectedEof` antes de reservar nada (de la revisión de delta): los offsets del navegador son números de JavaScript, exactos solo hasta 2^53, y en wasm32 una reserva que falla aborta la instancia.
  - **El `rename` de un archivo abierto da primero el error de una operación que nadie esperó,** también antes de rechazar su ruta, como `NativeFile` (de la revisión de delta): la ruta se valida después del permiso, y antes de tocar nada.
  - **El runner** acepta `--executable` (otro ejecutable del navegador, con el que corrió acá el Chromium 153), corre `--suites quota` siempre con su cuota (y la rechaza con otras suites o en otro navegador), e imprime como nota de la corrida un mensaje de la consola de un paso que empieza con `harness note: ` (de la revisión de delta). El crate de pasos suma la feature `console` de `web-sys`.
  - **Los puntos de los tests** son tres, no solo el de la reapertura: con el de `open`, `create_locked` da `None` (desde el arnés no se puede provocar), y con `rename.closed`, un `move()` falla.
  - **La cuota llena se probó en Chromium, no en Firefox** (arriba), con los dos resultados del navegador. La escritura que termina en 2^53 corre en los dos navegadores, y no es una cuota llena: en Firefox 155 da `WriteZero`.
  - **El comando de la CI corre en el job `browser`,** que tiene el `wasm-bindgen-test-runner` (de la versión del lock del arnés, que `build.sh` verifica que es la de la raíz) y el chromedriver del runner, con `working-directory: .`, porque el job corre en `tools/browser`. Su caché de Rust es la del arnés, así que el storage en wasm32, con sus dependencias, se compila en frío cada vez: no se sumó la raíz al caché, que lo invalidaría entero una vez. Su clippy va en el job de WebAssembly, que no necesita el navegador.
  - **Los pasos de `handles` corren con un plazo propio** (`within`, 20 s), además del de la página para un paso (60 s): una operación que se cuelga (por ejemplo, un `run` dentro de otro) hace fallar su paso enseguida, con su nombre.
  - **Firefox no corrió acá:** el proxy del entorno no deja bajar el de Playwright (un 403 de `cdn.playwright.dev` y de `playwright.download.prss.microsoft.com`), como en el Lote 2. Corrió en la CI: Firefox 155, el build de Playwright 1.63, pasó todos los pasos, también los de `files` y `handles`, en el run 46021628394, sobre el lote final, con el job `browser` en verde: el Chromium 153 pasó los 24 pasos y `quota`, y los tests unitarios del storage en wasm32, los 8. Antes, en el run 46018624452, sobre el lote corregido en la primera ronda de la revisión de delta, igual, con 7 tests unitarios; y en el run 46010464222, sobre el lote antes de la revisión, el job falló en `quota` en Chromium 153 (el Alto de la primera ronda), y Firefox no llegó a correr.
  - **El Chromium de este entorno no es el de la CI:** Playwright toma acá un headless shell, un build 157 (`chromium_headless_shell-1243`), y la CI usa el Chromium 153.0.8010.12 de Playwright 1.63. La revisión de delta encontró por eso, en la CI, lo que acá no se veía (la cuota llena, arriba). Desde su primera ronda, el arnés corre acá también con el 153 local (`--executable`).
  - **Los tests unitarios en wasm32, acá,** con el chromedriver del entorno (147, con Chrome 153) y `--disable-build-check` (`CHROMEDRIVER_REMOTE`), como en los Lotes 1 y 2.
  - **`INVARIANTES.md`, versión 53:** INV-L7 sobre el OPFS, ✅ en los archivos (en su texto y en el resumen); en §3.2, los archivos de la capa. S1 a S8 siguen 🆕 sobre el OPFS hasta que el motor corra ahí (Lote 4).
- **La revisión de delta** la hace un agente aparte, de contexto fresco y solo lectura, que prueba en su propia copia con sus propias mutaciones.
  1. **Primera ronda: 1 Alto, 2 Medios y 8 Bajos, corregidos.**
     - **El Alto, A1:** en Chromium 153, el de la CI, una escritura sobre la cuota no lanza `QuotaExceededError`: da como escritos 4294967288 bytes, y `write_all` lo recortaba a "todo" y daba `Ok`; lo mismo una escritura en `(1 << 53) - 1`. La CI lo mostró (el run 46010464222, `FAIL quota/handles_quota: Error: write_at of 16 MiB, over the quota succeeded`). Se escapó porque Playwright toma acá un build 157 que lanza el error. Ahora un conteo mayor que lo pedido es un error, en la lectura y en la escritura, con su test nativo; `handles_quota` acepta las dos formas y anota cuál vio; `handles_write` prueba la escritura en 2^53 sin CDP, en los dos navegadores (en Firefox 155 da `WriteZero`, que ya era un error: el conteo de más es de Chromium); y el runner corre acá también con el 153 local (`--executable`).
     - **Los Medios:** M1, ningún paso probaba `create` de un archivo que no existe (la mutación que no lo crea sobrevivía): ahora `handles_write` lo crea y `handles_after_reload` lo lee. M2, los documentos afirmaban de más: Firefox en la CI (todavía no corrió), la cuota llena como `StorageFull` en Chromium, y "Chromium 157 (Playwright 1.63)" en los chequeos de este lote y en las notas de los Lotes 1 y 2, corregidos, con una precisión en las de los Lotes 1 y 2.
     - **Los Bajos:** B1, con el `move()` y la reapertura fallidos, una mutación que daba el *tomado* del `move()` sobrevivía: un test unitario nuevo, con el archivo tomado todo el `rename`. B2, el paso 1 que borra también una generación más nueva que la abierta no tenía test: un caso en `handles_rename`. B3, que el archivo sigue a su directorio después de un `rename` entre directorios: otro `rename` dentro del directorio nuevo, con una generación vieja que quedó ahí. B4, que el nombre y la generación no cambian si falló el `move()`: una generación vieja en el destino del `rename` que falla, en el test unitario. B5, un `rename` después de uno soltado que falló corría si se validaba antes del permiso: el test unitario del `rename` soltado sigue con un `rename` a `../x`, que tiene que dar primero el error que nadie esperó, y la ruta se valida ahora después del permiso. B6, en Safari, reabrir el origen después de un `move()` rechazado con efecto lo crearía vacío: ahora se lista el directorio antes, y si la entrada no está, el archivo queda muerto, con su test unitario. B7, `--suites quota` corría sin su cuota: ahora siempre la tiene, y el runner la rechaza con otras suites o fuera de Chromium. B8, `read_exact` reservaba `len` antes de validar: ahora valida contra el largo del archivo, con un test de `usize::MAX` bytes.
     - **Las mutaciones de los hallazgos** mueren todas (abajo, la cuarta tanda).
  2. **Segunda ronda, acotada a los arreglos: verde, sin Altos ni Medios, con 6 Bajos, arreglados.** El revisor corrió 15 mutaciones: murieron 13 y sobrevivieron 2, que los arreglos cubren y ahora mueren. La CI sobre el lote de la primera ronda, el run 46018624452, ya estaba en verde en el job `browser`, con Firefox.
     - **B1,** `read_at` de largo 0 más allá del fin daba `UnexpectedEof`, y en nativo `read_exact_at` con un buffer vacío da `Ok`: ahora un largo 0 da nada, en cualquier offset, antes del chequeo, con su test nativo y un caso en `handles_write`.
     - **B2,** dos huecos de test sobre código nuevo, las 2 mutaciones que sobrevivían: el listado de después de un `move()` fallido sobre el directorio del destino en lugar del del archivo (un test unitario nuevo, con un `move()` fallido a otro directorio), y `read_at` sin el largo real del archivo (`read_at(0, usize::MAX)` en `handles_write`, que tiene que dar `UnexpectedEof`). Mueren las dos.
     - **B3,** DC39 (*Los errores*) seguía diciendo solo "la cuota llena a `StorageFull`": ahora tiene una precisión del Lote 3, con el conteo de más, la escritura más allá de 2^53 y la que no escribe nada.
     - **B4,** Firefox en los documentos: la escritura en `2^53 - 1` da en Firefox 155 0 bytes escritos (`WriteZero`), no `QuotaExceededError`, y la cuota llena en Firefox sigue sin probarse: en §1.2.1, en esta nota, en el README y en `ARCHITECTURE.md`, con el run 46018624452 citado donde estaba el marcador *pendiente*, también en `INVARIANTES.md`.
     - **B5,** un `getSize` que fallaba dentro de `read_at` salía como `read: …`: ahora sale como `getSize: …`.
     - **B6,** el docstring de `rename_open` dice ahora que un listado fallido después de un `move()` fallido también deja el archivo muerto.
- **Chequeos,** sobre el lote corregido en la primera ronda de la revisión de delta:
  - fmt de la raíz; clippy del workspace (`--all-targets`, con `-D warnings`); el build y el clippy de wasm32 de `zemdb-core`, `zemdb-storage` y `zemdb-client`, con `-D warnings`, y el clippy de los tests unitarios del storage en wasm32 (`--lib --profile test`); y doc con `-D warnings`, también el del storage en wasm32 (antes de la revisión, encontró un enlace a un elemento privado en el docstring del `rename`, corregido);
  - la suite del workspace: 1730 pasan, ninguna falla y 57 ignorados (los 1726 del Lote 2 y los 4 nativos nuevos), corrida con el arnés corriendo a la vez; y los `--ignored` del cliente en release, 7, sin fallas;
  - los tests unitarios del storage en wasm32 (7), en Chrome 153 con el chromedriver del entorno (147, con `--disable-build-check`);
  - **del arnés:** su fmt, el clippy de los pasos en wasm32 (`--all-targets`) y el del servidor de pruebas, con `-D warnings`; los tests del servidor de pruebas (2); `build.sh`; `npm ci`; el runner (24 pasos) y su self-check, en el Chromium 153.0.8010.12 local (`--executable`, el de la CI) y en el headless shell 157 que Playwright toma en este entorno; y su test unitario en wasm32 (1). Antes de la revisión, el runner había corrido solo en el 157 (y la nota decía, de más, "Chromium 157 (Playwright 1.63)");
  - **los pasos nuevos, repetidos:** el runner entero, seis veces seguidas en el 153 y cuatro en el 157, todas en verde (24 pasos cada una), con la suite del workspace corriendo a la vez; antes de la revisión, diez veces en el 157;
  - **Firefox:** en la CI, el run 46018624452 (arriba). Después de la segunda ronda, los arreglos (B1, B2 y B5, que tocan el storage y el arnés) corrieron acá en los dos Chromium: fmt, los clippy nativo y de wasm32 (también el de `--profile test`), doc, `cargo test -p zemdb-storage` (344 pasan, ninguno falla, 22 ignorados), los tests unitarios del storage en wasm32 (8), y el runner (24 pasos) y su self-check en el 153 y en el 157. En la CI, el run 46021628394, sobre el lote final: el job `browser` en verde, con Firefox 155 (todos los pasos, también los nuevos de `handles_write`) y Chromium 153 (los 24 pasos y `quota`), y los 8 tests unitarios del storage en wasm32. En su primer intento, el job de Windows falló en un test del cliente, `a_read_whose_replica_is_discarded_once_it_read_decrypts_nothing` (`crypto_tests`), que esperó 60 s una respuesta (*no reply came*); el lote no toca `zemdb-client`, `zemdb-core` ni `zemdb-server`, el test pasó acá quince veces seguidas, y el job repetido salió en verde, con el run entero.
  - **en `main`, con el lote aplicado:** fmt; los dos clippy de la raíz (el del workspace y el del cliente sin features por defecto); el build y el clippy de wasm32, y el clippy de los tests unitarios del storage en wasm32 (`--lib --profile test`); doc con `-D warnings`; la suite del workspace (1730 pasan, ninguna falla y 57 ignorados); los `--ignored` del cliente en release (7); los tests unitarios del storage en wasm32 (8); y del arnés, su fmt, sus dos clippy, los tests del servidor de pruebas (2), su test unitario en wasm32 (1), `build.sh`, `npm ci`, y el runner (24 pasos) y su self-check, en el Chromium 153 local (`--executable`) y en el headless shell 157.

### Lote 4 · El motor en disco en wasm32 (`zemdb-storage`) · ✅ Hecho (27 de Noviembre de 2026)
**Objetivo:** `DiskStorageEngine<OpfsFiles>` en wasm32, sin cambiar nada en nativo (DC38, DC39).

**Archivos:** `crates/storage/src/sys.rs` (el azar), `disk/mod.rs` (las tareas, y `zstd_level` y su validación), `disk/compactor.rs` (los nombres temporales), `disk/recovery.rs`, `lib.rs` (el motor en disco y los *fail points* en wasm32), `fail_point.rs`, `disk/tests` (fuera de wasm32), `crates/storage/Cargo.toml` (`getrandom`, o la fuente de `uuid`, en wasm32), el `clippy.toml` del storage (el lint de DC38, también en nativo fuera de los tests unitarios, como en el cliente desde el Lote 7), `disk/tests/opfs_like.rs` (DEF-184 y DEF-185), los pasos del arnés y `ARCHITECTURE.md` §8.

**Qué se hace:** las tareas del motor (`open_room`, `apply_snapshot`, `delete_room`, `compact_room` y la compactación automática) con la tarea de `sys`; el motor, la recuperación y la compactación compilados en wasm32; el `.snap` con `ruzstd` `Fastest`; los nombres temporales al azar; el módulo de los *fail points*, también en wasm32 (DC39); el lint del storage. En la capa de prueba nativa, el segundo handle rechazado (DEF-184) y los dos tests del handle muerto de la revisión de cierre de 4.1 (DEF-185).

**Tests exigidos:**
- **En el navegador:** una sala que aplica lotes, se recarga la página y reabre con todo lo aplicado; el Worker terminado en medio de un lote, y la sala que reabre en un prefijo del log; una compactación, también terminada en el medio, y la sala que reabre igual; `apply_snapshot` de un snapshot nativo (Zstd) y de uno propio (`ruzstd`), también después de recargar; `delete_room`, también terminado en el medio; el Worker terminado en medio de la instalación de un snapshot, y la sala que reabre con el viejo o con el nuevo; una segunda página que abre la misma sala recibe `RoomLocked`.
- **En nativo:** la suite del storage entera, sin cambios; DEF-184 y DEF-185 en la capa de prueba.

**Interacciones a revisar:** INV-S1, S3, S4, S5 y S8, cuyos mecanismos (las tareas propias, el lock del WAL) cambian de runtime en wasm32; INV-L7 (las tareas del motor retienen los locks, D86 y D89); el decodificador y el codificador `ruzstd` (INV-W1, W2); la compactación automática, que se lanza sin que nadie la espere; un rechazo con efecto en el OPFS (el riesgo aceptado de DC39); las generaciones bajo la instalación de un snapshot, la rotación y su vuelta atrás, y la limpieza de temporales de la recuperación. Además, de la revisión de delta del Lote 2: medir en Firefox cuánto tarda abrir decenas de salas sobre el OPFS, con los listados del directorio que crecen como N² (en la CI, Firefox ya tarda unas quince veces más que Chromium en el primer paso del directorio), y si hace falta, que un `rename` use un solo listado para sus dos nombres, o un índice del directorio.

**Documentos:** `ARCHITECTURE.md` §8 (el motor en wasm32); `INVARIANTES.md`: el estado de S1 a S8, L7, W1 y W2 sobre el OPFS.

**Entrega:** `feat(storage): the disk engine in WebAssembly, over the Origin Private File System (Fase 4.5, Lote 4)`.

**Hecho** (27 de Noviembre de 2026).
- **Qué entró.**
  - **La tarea de `sys`, en los dos** (`crates/storage/src/sys.rs`): `sys::spawn` da un `Task`, el handle de una tarea que corre hasta su fin aunque nadie la espere (soltarlo la deja seguir, con lo que tiene: las salas del motor, el lock de la compactación, los archivos de la sala), y esperarlo da lo que dio, o un `TaskError` si no dio nada, nunca un pánico de quien la espera. En nativo es `tokio::spawn`, sin nada en el medio (el `TaskError` tiene el texto del `JoinError`, con su *panicked*, que miran los tests de la suite); en wasm32, `wasm_bindgen_futures::spawn_local`, con su salida en un `oneshot` mientras alguien la espere, como en el Lote 2. Sus cotas, `MaybeSend`: `Send` en nativo, nada en wasm32. La capa del OPFS la usa igual que antes.
  - **Las tareas del motor** (`disk/mod.rs`): `open_room`, `apply_snapshot`, `delete_room`, `compact_room` y la compactación automática pasan de `tokio::spawn` a `sys::spawn`, con el mismo código adentro; la automática se suelta sin esperarla (`drop`), como antes su `JoinHandle`.
  - **El motor, la recuperación y la compactación en wasm32** (`lib.rs`): `disk`, `format`, `fail_point` y las exportaciones (`DiskStorageEngine`, `DiskStorageOptions`, `WalReader` y `WalWriter`) compilan en los dos. `DiskStorageEngine<F = PlatformFiles>`: `PlatformFiles` (en `files/mod.rs`, público) es `NativeFiles` en nativo y `OpfsFiles` en wasm32, así que `DiskStorageEngine::new` usa la capa de la plataforma en los dos. El trabajo de CPU, en el lugar (`run_blocking`, que ya lo hacía en wasm32).
  - **El `.snap`** (`snapshot.rs`): `compress_zstd` y `deserialize_zstd` compilan en los dos, con `ruzstd` `Fastest` en wasm32; `zstd_levels()`, los niveles que da la plataforma (un método nuevo del códec, `levels`): en nativo, `zstd::compression_level_range()`; en wasm32, solo 1. `DiskStorageEngine::with_files` valida `zstd_level` contra ellos, y el nivel por defecto es el de la plataforma (`DEFAULT_ZSTD_LEVEL`: 3 en nativo, como antes; 1 en wasm32).
  - **Los nombres temporales** (`compactor.rs`): `staged_snapshot_path`, nueva, da `room_{id}.snap.tmp.` seguido de `sys::random_name()`: en nativo, un UUID de versión 4, como antes; en wasm32, 32 dígitos hexadecimales de 128 bits de `crypto.getRandomValues`, por `getrandom` 0.4 con su feature `wasm_js` (en `crates/storage/Cargo.toml`, solo en wasm32; ya estaba en el `Cargo.lock`, por `uuid`). Un error de `getrandom` es un error, no un pánico.
  - **La recuperación** (`recovery.rs`): el largo del cuerpo del `.snap`, que declara su header, pasa a `usize` con `try_from`, y uno que no cabe en la memoria (en wasm32, más de 4 GiB) es un error (`Other`), no una lectura corta, que daría un `SnapshotCorruption` falso. En nativo no puede fallar.
  - **Los *fail points* en wasm32** (`fail_point.rs`): el módulo compila ahí, donde fuera de los tests no hace nada, como en nativo. Las pausas bloqueantes, que esperan con un plazo en un hilo, y los puntos de hilo, quedan para los tests nativos (`cfg(all(test, not(target_arch = "wasm32")))`); los demás (`arm`, `check`, las pausas asíncronas) los pueden armar los tests en wasm32. Los `cfg` de wasm32 alrededor de los *fail points* de `memory/` salen, porque el módulo ya existe ahí.
  - **El lint** (`crates/storage/clippy.toml`, nuevo): `clippy::disallowed_methods`, con las 17 entradas del cliente, cada una con su razón y, donde lo hay, su reemplazo en `sys`. Vale también en nativo, fuera de los tests unitarios: `lib.rs` lo permite solo con `cfg_attr(test, ..)`, y `sys.rs` siempre. Lo que lo usaba fuera de `sys`: los `tokio::spawn` del motor y el `Uuid::new_v4` de la compactación pasan a `sys`; los dos `spawn_blocking` de `files/native.rs` quedan, con un `allow` en la declaración del módulo (en desvíos). Los 13 tests de integración, crates aparte que leen el mismo archivo, llevan un `#![allow(clippy::disallowed_methods)]` cada uno.
  - **La capa de prueba nativa** (`disk/tests/opfs_like.rs`): el segundo handle rechazado, como el OPFS real (DEF-184): `None` en `open_locked` y en `create_locked` (este, después de crear el archivo, que queda creado, como en el OPFS y en su contrato), y el error *tomado* en `create` (antes de vaciarlo) y en `open_read`; sus negativas de mover y borrar pasan de `Other` al error *tomado*. Y la reapertura que falla después de un `move()` (`fail_reopen`), que deja muerto el handle (DEF-185).
  - **El arnés:** las suites `engine` (nueva, entre las de por defecto) y `rooms` (nueva, que el runner corre en un contexto propio); dos rutas del servidor de pruebas, `POST /harness/snapshot` (el snapshot de una sala de un `DiskStorageEngine` nativo, con `zstd`) y `POST /harness/snapshot/check` (aplica a un `MemoryStorageEngine` nativo un snapshot del navegador y dice cuántas filas tiene como las escribió), con `zemdb-storage` en sus dependencias; el plazo de una página del runner, de 180 a 300 s; y su `README.md`.
  - **128 líneas de producción que entran y 51 que salen, 77 netas**, contadas como en los Lotes 2 y 3 (sin comentarios, líneas en blanco ni la declaración de los módulos de tests unitarios): +68 −7 en `sys.rs`, +17 −13 en `disk/mod.rs`, +13 −12 en `fail_point.rs` (casi todo, `cfg` que cambian), +10 −2 en `snapshot.rs`, +7 −3 en `recovery.rs`, +5 −2 en `compactor.rs`, +5 en `files/mod.rs`, +3 −6 en `lib.rs`, −4 en `memory/state.rs` y −2 en `memory/mod.rs` (los `cfg` de los *fail points*). El plan estimaba 120. Aparte: `clippy.toml` (17 entradas), `Cargo.toml` (una dependencia) y una línea en cada test de integración.
- **Tests.**
  - **Nativos** (unitarios del storage, porque lo que prueban es interno):
    - de `sys` (`src/tests/sys.rs`, 5, el mismo archivo que en wasm32): `a_task_runs_until_its_end_after_it_is_dropped` (y suelta lo que tenía al terminar), `a_task_gives_what_it_returned` (también si terminó antes de que alguien la esperara), `a_task_that_panics_gives_an_error_that_says_so`, `a_task_dropped_by_its_runtime_gives_an_error` (el runtime apagado con la tarea pendiente) y `names_at_random_are_different_and_hexadecimal` (mil distintos, de 32 dígitos hexadecimales en minúscula y, en nativo, un UUID de versión 4);
    - de la capa de prueba (`opfs_like.rs`, 3 nuevos y 2 ajustados): `the_layer_refuses_a_second_handle_on_a_file`, con cada modo abierto (`open_locked` da `None`, `open_read` y `create` el error *tomado*, `create_locked` `AlreadyExists`, que encuentra antes, y el archivo conserva lo que tenía), y un archivo que aparece entre el chequeo de `create_locked` y su apertura, que da `None` y queda creado; `a_rotation_that_leaves_a_dead_handle_loses_nothing` y `a_rollback_that_leaves_a_dead_handle_marks_the_room_failed`, los dos de `storage.patch` con otro nombre y sin códigos, que además fijan que `close_room` da el error del sync del handle muerto (el parche lo imprimía) y terminan con la sala limpia (`assert_clean`); `the_layer_refuses_to_move_or_remove_a_file_with_a_handle_open`, que exige ahora el error *tomado*; y `the_layer_renames_an_open_file_by_closing_moving_and_reopening_it`, que abría un segundo handle sobre el origen a propósito: ahora solo sobre el destino, mira el lock de la reapertura con la capa nativa, y termina con una reapertura que falla y deja el handle muerto, que no escribe nada;
    - del motor (`src/tests/disk.rs`, 1): `the_automatic_compaction_runs_in_a_task_that_nobody_awaits`, el mismo que en wasm32, por dos mutaciones que sobrevivían a los tests nativos (abajo);
    - `no_name_of_the_engine_or_of_the_metadata_has_the_mark_of_a_generation` (de `opfs_names.rs`) toma el nombre temporal de `staged_snapshot_path`, y no de un UUID armado en el test.
  - **Unitarios en wasm32** (8 nuevos, 16 en total, en un Worker dedicado del Chrome 153 de este entorno):
    - de `sys` (`src/tests/sys.rs`, 3): la tarea que corre hasta su fin soltada, la que da lo que dio, y los nombres al azar, que en wasm32 varían en todos sus bits (ninguno fijo, como la versión de un UUID);
    - del motor (`src/disk/tests/opfs.rs`, 4, el módulo de tests de `disk` en wasm32): `zstd_level_takes_only_the_level_of_ruzstd` (el nivel por defecto es 1, y 3, 0, 2, −1, −7, 19 y 22 son `InvalidOptions` sin tocar nada: el negativo de la regla nueva); `a_compaction_writes_a_snapshot_that_ruzstd_compresses` (300 filas compactadas: el `.snap` es un frame de Zstandard de menos de la mitad del payload, que `deserialize_zstd` decodifica a sus 300 filas, y la sala reabre de él); `the_automatic_compaction_runs_in_a_task_that_nobody_awaits` (cuando vuelve el lote que la lanza, la compactación tiene el lock y todavía no corrió; termina sola, con el lock); y `the_recovery_removes_a_staged_snapshot_left_behind` (un nombre de `staged_snapshot_path`, distinto cada vez, que la capa toma y lista por su prefijo, y que la apertura de la sala borra);
    - de la capa (`files/tests/opfs.rs`, 1): `a_rotation_of_the_engine_that_cannot_reopen_the_wal_loses_nothing`, el handle muerto en el motor sobre el OPFS real, el pendiente del Lote 3: en `rename.moved` el test toma `.wal.compacting`, la reapertura falla con el error del navegador, la compactación falla con un error que no es el *tomado*, los lectores siguen viendo las 5 filas, la escritura siguiente falla y marca la sala fallida, `close_room` da el error del sync, el WAL muerto no escribió en ningún archivo (quedan el `.lock`, el `.snap` de una sala nueva y `.wal.compacting`), y la sala reabre con las 5 filas, escribe, compacta y reabre con 7.
  - **Pasos del arnés** (la suite `engine`, sobre el motor real y la capa real, con dos recargas, cuatro `terminate` y la segunda página; cada paso falla si no termina en 20 s, y uno que abre una sala después de que terminó un Worker reintenta 5 s mientras sus archivos siguen tomados, con `RoomLocked` o el error *tomado*):
    - `engine_write` y, después de recargar con la sala abierta, `engine_after_reload`: 1000 filas en lotes de 5, con compactaciones automáticas, que nadie espera (alguna tiene que haber escrito el `.snap`, cuyo header lee el paso); reabre con todo, sigue, compacta y reabre; un `reload_schema` a un esquema que no evoluciona del de la sala da `SchemaNotEvolving`, y uno a uno que evoluciona (con una columna nueva) se toma, y la sala reabre con él (INV-S7 sobre el OPFS);
    - `engine_batches_in_flight`, `engine_compactions_in_flight`, `engine_snapshots_in_flight` y `engine_deletes_in_flight`, cada uno seguido del `terminate` del Worker y de su paso `_cut`: un bucle de lotes de 50 filas; de un lote de una fila y una compactación de una sala de 2000; de la instalación de snapshots de un `MemoryStorageEngine`, de 2000 filas y cada uno una fila adelante; y de ciclos que escriben 80 filas, compactan, cierran y borran la sala (cómo se corta, abajo). El `_cut`, en un Worker nuevo, reabre la sala y la compara con lo que el bucle registró: cada fila confirmada y a lo sumo el lote en vuelo, entero (las filas exactas, y ninguna más); el último snapshot instalado o el que estaba en vuelo; un borrado en un prefijo del log de su ciclo (un múltiplo de 20 hasta 80, con las filas exactas) o vacío, y borrarla de nuevo la deja vacía. La recuperación borra lo que dejó el corte (el directorio queda con el `.lock`, el `.snap` y el WAL, sin temporales ni `.wal.compacting`), la sala sigue (escribe, cierra y reabre), y el paso anota dónde cayó el corte, con las entradas que dejó;
    - `engine_snapshots` y, después de recargar con las salas abiertas, `engine_snapshots_after_reload`: `apply_snapshot` de un snapshot nativo de 500 filas (del servidor de pruebas, con `zstd`, el header lo dice) en una sala nueva; uno que el navegador hace de ella (`create_snapshot`, con `ruzstd`), que el motor nativo del servidor lee con las mismas 500 filas, en otra; y otro nativo de 800, sobre la primera. Después de recargar reabren con sus snapshots (el header de cada `.snap` está en su head) y siguen;
    - `engine_hold` y `engine_release` (en la primera página), y `engine_locked` y `engine_free` (en la segunda): con la sala abierta en la primera, `open_room` y `delete_room` desde el Worker de la segunda dan `RoomLocked` sin cambiar ninguna entrada (el paso compara el listado del navegador antes y después, y verifica que no es la página que la tiene); la primera escribe una fila más y cierra, y la segunda la abre con las cuatro;
    - `engine_dropped`: `open_room`, `compact_room`, `apply_snapshot` (de un snapshot nativo) y `delete_room`, cada uno sondeado una vez y soltado (`now_or_never`), terminan en su tarea: otro `open_room` da `RoomAlreadyOpen`; `close_room` espera la compactación (el `.snap` queda en el head) y la instalación (la sala reabre en la secuencia del snapshot); y un `open_room` después del borrado soltado lo espera y encuentra la sala vacía.
  - **La medición** (la suite `rooms`, `engine_rooms_10`, `engine_rooms_30` y `engine_rooms_60`): cada paso abre esas salas una tras otra en un directorio propio, las cierra y las vuelve a abrir con otro motor, y anota cuánto tardó cada pasada (`harness note:`). Una pasada deja de abrir a los 20 s y anota hasta dónde llegó, así un navegador lento da su número en lugar de fallar el paso. **En Chromium:** en 11 corridas en cada uno (el headless shell 157 de Playwright en este entorno y el 153 de la CI, `--executable`), con 10 salas, de 32 a 48 ms por sala la primera vez y de 31 a 42 al reabrirlas; con 30, de 44 a 60 y de 47 a 59; con 60, de 59 a 70 la primera vez y de 102 a 121 al reabrirlas, con 180 entradas en el directorio (unos 6,5 s para reabrir las 60). Reabrir una sala no renombra nada (son siete operaciones que listan: las dos aperturas con lock, `exists` y `open_read` del `.snap`, la búsqueda de temporales, el borrado de la copia de una absorción y `exists` de `.wal.compacting`), y es lo más lento. Cada apertura hace unas diez operaciones de la capa, y cada una lista el directorio entero: el costo por sala crece con las entradas, del orden de N² para N salas.
  - **Firefox, en la CI** (no baja en este entorno, como en los Lotes 2 y 3): **pendiente** (el run, los pasos de `engine` y la nota de `rooms` en Firefox): lo completa el coordinador con el run de este parche.
  - **La suite del workspace, sin tocar ninguna expectativa:** en nativo, el lote solo cambia la tarea de debajo (`tokio::spawn` por `sys::spawn`, que es lo mismo), los `cfg` de los tests y de los *fail points*, y suma los tests de arriba; los 13 tests de integración, una línea cada uno.
- **Cómo se corta el Worker en el medio.** Un paso *en vuelo* deja un bucle de operaciones corriendo en el Worker (`spawn_local`), que registra hasta dónde llegó en un archivo del arnés, fuera del directorio de la sala: después de cada paso que el motor confirmó, 8 bytes en una escritura y un `flush`, que un corte deja enteros. Vuelve cuando el bucle corrió tres pasos y, después, una parte al azar del tiempo que tardaron (`Math.random()`), así que la página termina el Worker (la acción `terminate`, entre pasos) en un punto al azar de un paso siguiente; nunca antes de que el bucle empiece, ni después de que termine, porque no termina. El paso `_cut` lo comprueba (lo registrado tiene que incluir los tres pasos) y anota dónde cayó.
  - **Chromium termina el Worker al final de su tarea en curso**, no en el medio: con una primera versión del paso, que corría ráfagas de cuatro lotes sin ceder el bucle de eventos entre ellos, el corte cayó en las ocho corridas entre dos ráfagas, nunca adentro de un lote. Un lote es una sola vuelta del bucle de eventos (su escritura, su `flush` y su aplicación en memoria son llamadas síncronas, y sus locks no esperan), así que el corte cae entre dos promesas de una operación, nunca adentro de la parte síncrona de un lote. Por eso el bucle de lotes le da una vuelta al bucle de eventos después de cada lote y antes de registrarlo: el corte puede caer con el lote en el WAL y sin registrar, y el `_cut` lo acepta (la sala en lo confirmado más el lote en vuelo). Con lotes de 500 filas y una espera al azar de una parte del tiempo de un solo paso, el corte cayó en diez corridas siempre en el mismo punto: por eso los lotes son de 50 filas y la espera, de una parte del tiempo de los tres primeros pasos. En Firefox puede ser otro: lo dice la CI.
  - **Dónde cayó**, en las 16 corridas en Chromium después del último cambio de los pasos (10 del runner entero, 5 en cada Chromium, y 6 de la suite `engine` en el 157), según sus notas: lotes: 9 entre dos lotes y 7 con el lote en el WAL y sin registrar; compactaciones: 10 con el WAL rotado y `.wal.compacting` (en 3, sin el WAL nuevo: entre el `move()` de la rotación y `create_locked`; en 1, con dos generaciones del `.snap`: después del `move()` del snapshot nuevo y antes de la limpieza), 2 con el snapshot en un temporal, y 4 donde no quedó nada; instalaciones: 10 donde no quedó nada, 3 con el snapshot en un temporal y 3 con el nuevo instalado y sin registrar (con dos generaciones del `.snap`, la vieja y la nueva); borrados: 3 en el borrado (la sala reabre con todo), y 13 en la escritura del ciclo siguiente (vacía, o en 60 con `.wal.compacting` de la compactación del ciclo, o con el temporal del snapshot vacío de la sala nueva). En corridas anteriores, con el mismo paso de borrados, el corte cayó también en el medio del borrado, con el WAL ya borrado y el `.snap` todavía ahí, y la sala reabrió en 60 de 80.
  - **No se usan los *fail points* para cortar:** en el arnés el storage se compila sin `cfg(test)`, donde no hacen nada.
- **Mutaciones inversas:** **21, en una copia hecha con `tar`, cada una sola, contra los tests nativos del storage, sus tests unitarios en wasm32 y la suite `engine` en el Chromium 157; mueren 20, y sobrevive una que no se puede provocar. Dos pidieron un test nativo, y ahora mueren también ahí.**
  - **Las tareas:** la de `sys` que se corta al soltar su handle, en nativo (9 tests nativos: los de las operaciones soltadas) y en wasm32 (3 unitarios en wasm32, y `engine_write` en el arnés: la compactación automática, que nadie espera, no corre); la de wasm32 que no entrega su salida (todos los unitarios en wasm32 que abren un archivo); `delete_room` que suelta las salas del motor al empezar (un test nativo de `delete`, y `engine_dropped`: la apertura después del borrado soltado da `RoomLocked`); y `open_room` en el future de su llamador, sin tarea (3 tests nativos de aperturas soltadas, y `engine_dropped`: otra apertura no da `RoomAlreadyOpen`).
  - **La compactación automática** que no se lanza, o que se espera dentro de `apply_batch` (soltando antes el WAL, para que no se trabe): **sobrevivían a los tests nativos**, que la disparan sin mirarla, y morían solo en `the_automatic_compaction_runs_in_a_task_that_nobody_awaits` en wasm32 (y la primera, en `engine_write`). Pidieron un test: el mismo, nativo, en `src/tests/disk.rs`, y mueren las dos ahí.
  - **El `.snap`:** a un nivel que `ruzstd` no da (`Default`, que entra en pánico con `unimplemented!`: los unitarios en wasm32 y `engine_write`); sin comprimir (`Uncompressed`: `a_compaction_writes_a_snapshot_that_ruzstd_compresses`, por el tamaño; el arnés no lo ve, porque el `.snap` sigue valiendo); los niveles de `zstd` en wasm32 (`zstd_level_takes_only_the_level_of_ruzstd`); y el 3 por defecto en wasm32 (5 unitarios en wasm32: todo motor por defecto da `InvalidOptions`).
  - **Los nombres temporales:** fijos (el test de los nombres al azar, en nativo y en wasm32); con un prefijo que la recuperación no reconoce (`snap.tmp-`: 8 tests nativos, también de la simulación, y en el arnés `engine_compactions_cut`, que encuentra el temporal que el corte dejó y la recuperación no borró); y con un `~` (5 unitarios en wasm32: la capa rechaza el nombre).
  - **Un *fail point* que en wasm32 hace algo fuera de los tests** (`compaction.phase2` falla siempre): `engine_write` (ninguna compactación automática termina).
  - **La capa de prueba** que vuelve a aceptar un segundo handle (`the_layer_refuses_a_second_handle_on_a_file`), y la que vuelve a dar `Other` en sus negativas (3 tests de la capa).
  - **El handle muerto que no marca la sala fallida** (`apply_batch` completa su `PendingWrite` y devuelve el error de la escritura): los dos tests nativos del handle muerto, y el unitario en wasm32 del motor sobre el OPFS. El arnés no llega a un handle muerto.
  - **El lint:** un `tokio::spawn` en `compact_room`, en nativo (`use of a disallowed method tokio::task::spawn`), y un `std::time::Instant::now` ahí, en nativo y en wasm32 (`use of a disallowed method std::time::Instant::now`), cada uno con su clippy y `-D warnings`. Un `tokio::spawn` en wasm32 no compila (Tokio va solo con `sync`): ahí el lint ve lo que compila.
  - **La que sobrevive:** el largo del cuerpo del `.snap` con `as usize` en lugar de `try_from`: solo se ve con un `.snap` de más de 4 GiB en wasm32, donde no cabría en la memoria; en nativo `usize` es de 64 bits. Queda sin test, leída a mano.
  - **Contra la suite `engine` sola** (9 de las de arriba, para ver qué alcanza el arnés): mueren 6 (la tarea de wasm32 cortada, `delete_room` y `open_room`, la compactación automática que no se lanza, el nivel de `ruzstd` y el temporal que la recuperación no reconoce), y sobreviven 3, que mueren en los tests unitarios: la compactación automática esperada, el `.snap` sin comprimir y el handle muerto que no falla la sala. El arnés no ve si la compactación se esperó, ni el tamaño del `.snap`, ni llega a un handle muerto.
  - Una primera versión de la mutación de la tarea de wasm32 no compilaba, y se rehízo; y el driver de la primera tanda pasaba `-q` al runner de los tests en wasm32, que no lo acepta: esas corridas no cuentan, y se repitieron.
- **Interacciones revisadas.**
  - **INV-S1, S3, S4, S5 y S8, cuyos mecanismos cambian de runtime en wasm32.** Las tareas propias pasan a `sys::spawn`, con el mismo código adentro: en wasm32 corren en el bucle de eventos del Worker hasta su fin, y retienen lo mismo que en nativo. El lock del WAL y los de la sala son `tokio::sync`, que el storage ya usaba en wasm32, sin el runtime de Tokio. Un `apply_batch` no corre en una tarea, como en nativo: en wasm32 su escritura, su `flush` y su aplicación en memoria son llamadas síncronas del handle y locks que no esperan, así que terminan en la misma vuelta del bucle de eventos, y uno soltado después de su primer `poll` ya terminó (si espera un lock tomado, se suelta antes de escribir, y `PendingWrite` cubre lo demás, como en nativo). Lo que se ve en el navegador: con el Worker terminado en un punto al azar de cada operación, la sala reabre en lo confirmado y a lo sumo lo que estaba en vuelo, entero (S1, S3); con una tabla de cada lote y del snapshot exactamente como se escribieron, sin descartar nada (S4); con el snapshot viejo o el nuevo, nunca uno a medias (S5); y un borrado cortado en un prefijo de su log o vacía (S8). La recuperación limpia lo que dejó cada corte.
  - **INV-L7: las tareas del motor retienen los locks** (D86, D89). `open_room` y `delete_room` mueven a su tarea las salas del motor (y el `.lock` de la sala, `delete_room` hasta su último borrado); `compact_room`, `apply_snapshot` y la compactación automática, el lock de la compactación. Una tarea soltada sigue con ellos: los tests nativos de siempre (soltar cada operación en cada `.await`) pasan sin cambios, y `engine_dropped` lo muestra en el navegador. Una tarea que no da nada da un error, nunca un pánico (en wasm32 un pánico aborta la instancia, así que solo pasaría si el bucle de eventos soltara la tarea).
  - **El decodificador y el codificador `ruzstd` (INV-W1, W2).** El `.snap` se escribe con `ruzstd` `Fastest` y se lee con el mismo camino acotado que un snapshot recibido (la ventana y los bloques acotados de DC9), sin el tope de tamaño, porque lo escribió el motor y su CRC coincide, como en nativo; el cuerpo se lee después de verificar su largo contra el del archivo, y ahora solo si cabe en la memoria. Un snapshot nativo se aplica en el navegador, y uno del navegador lo lee el motor nativo del servidor de pruebas con las mismas filas (INV-W2).
  - **La compactación automática, que se lanza sin que nadie la espere.** Toma el lock de la compactación con `try_lock_owned` dentro de `apply_batch`, antes de lanzarse, así que cuando el lote vuelve el lock ya está tomado y `close_room` la espera; corre cuando el Worker vuelve al bucle de eventos, con el WAL suelto (`apply_batch` ya lo soltó). Lo fijan los tests unitarios, el nativo y el de wasm32, y `engine_write`.
  - **Un rechazo con efecto en el OPFS** (el riesgo aceptado de DC39, en §4 de `INVARIANTES.md`). El motor no cambia: `delete_room` y la recuperación siguen tomando un `NotFound` de un borrado como éxito, y un borrado rechazado como error (lo devuelve, y el reintento lo termina). Si un `removeEntry` rechazado después de un error de E/S hubiera borrado, ningún `sync_dir` lo haría durable: sigue sin poder provocarse, y el riesgo queda como estaba.
  - **Las generaciones bajo la instalación de un snapshot, la rotación y su vuelta atrás, y la limpieza de temporales de la recuperación.** El `rename` del temporal sobre el `.snap` va a su generación siguiente, y su generación vieja se borra en el paso 3, o queda si alguien la tiene abierta (el `_cut` lo vio, y la siguiente instalación la borra). La rotación mueve el WAL abierto a `.wal.compacting` y `create_locked` del WAL nuevo encuentra el nombre libre, porque el paso 1 borró las demás generaciones; la vuelta atrás, a la generación siguiente del WAL, si `create_locked` lo dejó creado. Un corte entre el `move()` de la rotación y `create_locked` dejó `.wal.compacting` sin `.wal` en tres corridas, y la recuperación lo plegó con todo. La limpieza busca los temporales por `room_{id}.snap.tmp.` con `list`, que da cada nombre una vez sin sufijo, y `remove` borra todas sus generaciones; el nombre al azar de wasm32 no tiene `~`, y un temporal que dejó el corte (también el del snapshot vacío de una sala nueva, en un ciclo de `engine_deletes`) desaparece al abrir.
  - **La medición de Firefox** (de la revisión de delta del Lote 2): los pasos de `rooms`, arriba. No se optimizó el `rename`: el número de Chromium ya muestra el costo, y la decisión va con el de Firefox. El `rename` sigue listando dos veces. Pero lo que miden los pasos dice que el `rename` no es lo que cuesta: reabrir una sala no renombra nada y es lo más lento, porque cada una de sus operaciones lista el directorio entero y baja desde la raíz del OPFS. Si Firefox confirma el costo (unas quince veces el de Chromium serían unos 1,5 s por sala con 60, más de un minuto para reabrirlas), la optimización que sirve es un índice del directorio en la capa, o conservar los handles de sus directorios, y no un solo listado para los dos nombres del `rename`: una decisión para el dueño del proyecto, con el número de la CI.
- **Desvíos y precisiones.**
  - **La tarea de `sys` tiene la forma de la de `rt` del cliente** (un `Task` que es un future de `Result<T, TaskError>`, `MaybeSend`), sin `abort` ni `is_finished`, que el storage no usa. En nativo el `TaskError` lleva el texto del `JoinError` de Tokio, así que el error de una tarea que entró en pánico sigue diciendo *panicked*, como miran tres tests de la suite.
  - **`zstd_level` en wasm32 toma solo 1,** que es su valor por defecto ahí: `ruzstd` 0.9 implementa solo `Uncompressed` y `Fastest` (sus otros niveles entran en pánico con `unimplemented!`), y `Fastest` es *aproximadamente* el nivel 1 de `zstd`. Un nivel que en nativo vale (también el 3, el de por defecto en nativo) es `InvalidOptions` en wasm32, antes de tocar nada: aceptarlo sería ignorarlo. `encode_snapshot_envelope`, público, sigue ignorando el nivel en wasm32, como dice su contrato; el motor nunca le pasa otro.
  - **El nombre temporal** es `room_{id}.snap.tmp.` seguido de 128 bits al azar: en nativo, el UUID de versión 4 de antes, sin ningún cambio; en wasm32, 32 dígitos hexadecimales, sin los bits fijos de un UUID (sin `uuid` en wasm32, donde no compila sin elegir su fuente). Los dos son dígitos hexadecimales y `-`: ningún `~`, el sufijo de las generaciones, ni nada que la capa rechace. `getrandom` 0.4 elige su backend `wasm_js` con su feature, sin la `--cfg` que pedía la 0.3.
  - **`PlatformFiles`,** un alias público nuevo, es el parámetro por defecto de `DiskStorageEngine` y la capa de `DiskStorageEngine::new` en los dos: el plan no decía cómo se escribía el tipo en wasm32.
  - **El `allow` del lint en la capa nativa:** los dos `spawn_blocking` de `files/native.rs` quedan ahí, con un `#[allow(clippy::disallowed_methods)]` en la declaración del módulo y su razón en un comentario: la capa nativa corre en el pool bloqueante de Tokio y no compila en wasm32, así que el lint no tiene nada que vigilar ahí, y pasarlos a `sys` sumaría una función que solo usaría esa capa.
  - **Las entradas de Tokio y de `uuid` del `clippy.toml` llevan `allow-invalid = true`:** en wasm32 Tokio tiene solo `sync` y `uuid` no es una dependencia, así que sus rutas no resuelven ahí, y clippy avisaría en cada corrida (el aviso que anotó el Lote 7 para el Lote 8). Ahí el lint vigila lo que compila (`std::time`, `std::thread`), y en nativo, todo.
  - **El módulo de tests de `disk` en wasm32 es `src/disk/tests/opfs.rs`,** junto a `opfs_like.rs`, y no `src/tests/disk.rs`, que es el nativo: `GEMINI.md` pide un archivo por módulo, y el nativo no compila en wasm32 (Tokio, `tempfile`, la capa nativa). Son dos declaraciones de `mod tests`, cada una con su `cfg`. El handle muerto del motor sobre el OPFS va en los tests de la capa (`files/tests/opfs.rs`), porque usa los puntos de `opfs.rs`, que son privados del módulo.
  - **El snapshot nativo sale del servidor de pruebas,** y no de un fixture: el servidor es nativo y tiene el motor, así que el snapshot es siempre el del formato y el motor del commit, y no hay un archivo binario que mantener. La misma razón para la ruta que lee con el motor nativo el snapshot del navegador.
  - **Los pasos con un bucle en vuelo usan un archivo del arnés para su registro,** escrito con el handle del navegador y no con la capa: la capa es lo que se prueba, y su turno lo serializaría con el motor.
  - **El plazo de una página del runner pasa de 180 a 300 s,** y la suite `rooms` corre en un contexto propio: en Firefox las suites del OPFS tardan unas quince veces más que en Chromium (la revisión de delta del Lote 2).
  - **`entries` de `files.rs`, y `fetch`, `bytes` y `text` de `server.rs`, del crate de pasos,** pasan a `pub(crate)`, para `engine.rs`.
  - **El tamaño:** 128 líneas que entran y 51 que salen, 77 netas, sobre una estimación de 120: dentro de la regla 1 de `docs/METODOLOGIA.md` §4. De las que entran, 61 son la tarea, sus cotas y el azar de `sys`, y unas 25, `cfg` que cambian (los *fail points*, el lint, los módulos de wasm32).
  - **Firefox no corrió acá:** el proxy del entorno no deja bajar el de Playwright, como en los Lotes 2 y 3. Corre en la CI.
  - **`INVARIANTES.md`, versión 54:** INV-S1 a S8 sobre el OPFS, ✅ (en su texto y en el resumen); en INV-L7, las tareas del motor, ✅; en INV-W1 e INV-W2, la recuperación y el `.snap` del motor en wasm32, ✅; todo en Chromium, y en Firefox pendiente de la CI; en §1.4, DEF-184 y DEF-185 arreglados, que el plan de 4.1 marca ✅.
- **La revisión de delta:** pendiente.
- **Chequeos:**
  - fmt de la raíz y del arnés; clippy del workspace (`--all-targets`, `-D warnings`); el build de wasm32 de `zemdb-core`, `zemdb-storage` y `zemdb-client`, su clippy (`-D warnings`) y el de los tests del storage en wasm32 (`--lib --profile test`); doc con `-D warnings`, también el del storage en wasm32: todos en verde;
  - la suite del workspace: **1738 pasan, ninguno falla y 57 ignorados** (los 1730 del Lote 3 y 8 nuevos), y después del test nativo de la compactación automática, la del storage: **353 pasan, ninguno falla, 22 ignorados**; los `--ignored` en release del storage (**22**) y del cliente (**7**), sin fallas;
  - los tests unitarios del storage en wasm32: **16 pasan** (8 nuevos), en el Chrome 153 del entorno con el chromedriver 147 (`--disable-build-check`, `CHROMEDRIVER_REMOTE`), arrancado con `setsid` y su grupo matado al terminar;
  - **del arnés,** como el job `browser`: fmt, el clippy de los pasos en wasm32 (`--all-targets`) y el del servidor de pruebas, con `-D warnings`; `cargo test -p zemdb-browser-server` (2); `build.sh`; `npm ci`; y en los dos Chromium, el headless shell 157 que Playwright toma en este entorno y el 153 de la CI (`--executable /opt/pw-browsers/chromium-1243/chrome-linux64/chrome`): `node run.mjs` (**44 pasos**: los 24 de antes, los 17 de `engine` y los 3 de `rooms`, y `quota`), `node run.mjs --self-check` y `node run.mjs --suites quota`, en verde; y el test unitario en wasm32 del arnés (1);
  - **los pasos nuevos, repetidos:** sobre el lote final, el runner entero 10 veces seguidas (5 en cada Chromium, 440 pasos) y la suite `engine` sola 6 veces; antes, sobre versiones anteriores de los pasos, el runner entero 15 veces y la suite `engine` sola 14: en verde todas;
  - **Firefox:** pendiente de la CI.

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

### Lote 6 · El despachador del runtime del Worker (`zemdb-client`) · ✅ Hecho (24 de Noviembre de 2026), partido en 6a y 6b
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

#### Lote 6b · Las suscripciones y el token (`zemdb-client`) · ✅ Hecho (24 de Noviembre de 2026)
**Objetivo:** lo que queda de DC42 en el despachador: `watch`, con un valor en vuelo por suscripción, y el token que el Worker le pide a la página.

**Archivos:** `crates/client/src/dispatch/` (las suscripciones y el `TokenProvider` de la página), sus tests (`crates/client/tests/dispatch_tests.rs`) y `ARCHITECTURE.md` §11.

**Qué se hace:** `Request::Watch` (`watch` y `watch_scan`), `Unwatch`, `Confirm` y `Token`; los avisos (`Notice`: un valor, el fin de una suscripción y el pedido de un token) en `Message`; una tarea por suscripción, que no lee el valor siguiente de su stream hasta la confirmación del anterior; el `TokenProvider` que publica el pedido y espera la respuesta, con el plazo de 60 s del cliente, y que `Platform::open` recibe; y, con el aviso de que una pestaña se fue, el fin de sus suscripciones y de los pedidos de token a ella.

**Tests exigidos:** el de `watch`, el del token, el de dos pestañas en lo de los valores de `watch` y las suscripciones que termina el aviso, y la ida y vuelta de los avisos (los del Lote 6); además, el modo del worker que sale de las suscripciones del despachador (INV-K18) y una suscripción que termina con un descarte a pedido (DC37).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K10 y K15 en el despachador.

**Entrega:** `feat(client): the subscriptions and the tokens of the dispatcher of the browser worker (Fase 4.5, Lote 6b)`.

**Hecho** (24 de Noviembre de 2026).
- **Qué entró.**
  - **Los comandos y los avisos** (`command.rs`): `Request::Watch`, sobre una tabla por su sala y su nombre, con lo que lee (`Watch::Row`, una clave, como `watch`, o `Watch::Rows`, un scan, como `watch_scan`); `Unwatch` y `Confirm`, por el id de la suscripción; y `Token`, la respuesta de una pestaña a un pedido, por su id, con un token o ninguno. `Answer::Subscription`, el id de una suscripción. Y `Notice`, en `Message::Notice`: `Value` (un valor de una suscripción, o el error de su lectura), `Ended` (el fin de su stream) y `Token` (el pedido de un token, con su id, el client id y la sala), cada uno con su pestaña.
  - **Las suscripciones** (`subscription.rs`): `Watch` arma el `ChangeStream` de la fachada al despacharse (así se suscribe antes de su primera lectura), entra la suscripción con su tarea, publica la respuesta con su id y recién después deja empezar la tarea, con un `oneshot`. La tarea publica cada elemento como `Notice::Value` y espera su confirmación, otro `oneshot` que `Confirm` toma de la entrada; con el stream terminado, y su valor en vuelo confirmado, publica `Ended`. `Unwatch` corta la tarea al despacharse y responde cuando terminó; el aviso de una pestaña que se fue corta las suyas, y responde cuando terminaron. Una suscripción cortada queda en el despachador, marcada, hasta que su tarea termina: la tarea tiene un guard que, al terminar de cualquier modo (con su stream, cortada, o con un pánico de la función que publica), la saca del despachador, suelta su stream y avisa su fin a todos los que lo esperan (un `oneshot` compartido), así que otro `Unwatch` o el aviso de su pestaña la esperan también. Soltar el `Dispatcher` corta las que quedan.
  - **El token** (`token.rs`): `PageTokens`, el `TokenProvider` del despachador, con una referencia débil a él, que `Platform::open` recibe ahora, y a qué pestaña va cada pedido. Publica `Notice::Token` a una pestaña de la sala y espera su respuesta; un guard saca el pedido del despachador cuando el cliente deja de esperarlo (la respuesta, o su plazo de 60 s). Soltar el despachador termina los pedidos en vuelo, sin token, y después no sale ninguno.
  - **El despachador** (`mod.rs`): su estado suma, por sala, la pestaña cuyo `room` corre en su turno (una marca con un guard, que la deshace también con un pánico, después de anotar la pestaña en la sala); las pestañas cuyo aviso está en vuelo (otra marca con su guard, puesta bajo el mismo lock con que terminan los pedidos a la pestaña, así que ninguno nuevo le llega entre las dos cosas); el último id que dio; las suscripciones y los pedidos de token, cada uno con su pestaña; y si el despachador se soltó. `Shutdown` olvida las pestañas de las salas cuando el cliente terminó de apagarse, y no al despacharse. Las tareas del despachador son de `rt` (`rt::spawn`, y el `rt::Task` de cada suscripción), del Lote 7.
  - **293 líneas de producción**, contadas como en el 6a (sin comentarios, líneas en blanco ni la declaración de los módulos de tests unitarios): 26 más en `command.rs` (107), 111 más en `mod.rs` (324), 105 en `subscription.rs` y 51 en `token.rs`. El plan estimaba unas 200: lo de más es la señal que deja empezar la tarea después de la respuesta, el fin de una suscripción que se espera más de una vez (de la revisión de delta), a qué pestaña va cada pedido de token (la marca del `room` en su turno y las pestañas que se van, con sus guards) y el `Drop` del despachador. La primera versión, antes de la revisión de delta, tenía 255; con los arreglos, 296; sobre el Lote 7, sin la función propia que lanzaba sus tareas, que ahora es `rt::spawn`, 292; con los arreglos de la segunda ronda, 293, y con los de la tercera, también.
- **Tests** (`crates/client/tests/dispatch_tests.rs`). La página de prueba recibe también los avisos, guarda en orden todo lo que llega (para mirar el orden de las respuestas y los avisos), y puede correr un gancho dentro de `publish`, que despacha comandos; la plataforma de prueba puede abrir el cliente con el `PageTokens` del despachador.
  - `a_subscription_has_one_value_in_flight_until_its_tab_confirms_it_and_then_the_last` (el de `watch`): la respuesta llega antes del primer valor; cinco escrituras sin confirmar no publican nada, y la confirmación trae una vez el último estado; confirmado sin cambios, nada; `Unwatch` con un valor en vuelo la termina (`status` no la cuenta, y nada llega después); el cierre de la sala termina enseguida una confirmada, y la que tiene un valor en vuelo, después de su confirmación; terminada, `Confirm` y `Unwatch` no cambian nada.
  - `with_the_token_of_its_tab_a_room_opens_and_without_one_in_time_it_opens_as_without_one` (el del token, con el reloj pausado de Tokio): con la respuesta de la pestaña, la sala abre y su registro lleva ese token; sin respuesta, el primer pedido vence a los 60 s, el registro va sin token, rechazado pide otro, y la sala sin metadatos no abre (`Network`) a los 120 s; una respuesta tardía no cambia nada; todo igual que la fachada con un `TokenProvider` que no responde (el resultado, el tiempo y lo que se manda).
  - `each_value_goes_to_the_tab_of_its_subscription_and_a_tab_that_left_ends_only_its_own` (el de dos pestañas): tres pestañas (una que no abrió la sala) miran la misma sala; un cambio da un valor a cada suscripción, en su pestaña; la confirmación y el `Unwatch` de otra pestaña no cambian nada; el aviso de una termina la suya y no las otras, también con un valor en vuelo y en una pestaña que no abrió la sala.
  - `every_command_reply_and_notice_goes_through_bincode_unchanged` (la ida y vuelta, el test del 6a con otro nombre): suma los comandos nuevos, `Answer::Subscription` y cada aviso, con un valor de cada respuesta y de cada error.
  - `in_periodic_the_worker_is_live_while_the_room_has_a_subscription_of_the_dispatcher` (el modo, INV-K18): con la sala en `periodic`, la primera suscripción pide `live`, terminar una de dos lo deja, y terminar la última con el aviso de su pestaña pide `periodic`; la sala abierta otra vez, después de un cierre que terminó una, vuelve en `periodic`.
  - `a_discard_on_request_ends_the_subscriptions_of_its_room` (el del descarte, DC37): el descarte de otra pestaña termina las suscripciones de la sala, la que tiene un valor en vuelo después de su confirmación; abierta otra vez, como nueva, una suscripción nueva lee la réplica del servidor.
  - Además:
    - `a_watch_gives_what_the_facade_gives_for_a_room_or_a_table_it_cannot_read`: antes de abrir y en una sala que no está abierta, `RoomClosed`; una tabla que el esquema no tiene, `InvalidOperation`; y una clave que el esquema no toma, un valor que es el error de su lectura.
    - `a_shutdown_ends_the_subscriptions_with_their_streams`: `Ended` de cada una, la que tiene un valor en vuelo después de su confirmación, y después un `Watch` es `RoomClosed`.
    - `dropping_the_dispatcher_ends_its_subscriptions`: la fachada deja de contarlas.
    - `a_page_that_answers_from_inside_what_publishes_gets_its_tokens_and_the_last_value`, con varios hilos: una página que confirma cada valor y contesta cada pedido de token desde `publish`, con cuarenta escrituras, recibe valores cada vez más nuevos y, al final, el último estado.
    - `a_slow_page_gets_the_reply_to_a_watch_first_and_nothing_after_its_unwatch_or_its_left`, con varios hilos: una página cuyo `publish` tarda con la respuesta de un `Watch` recibe esa respuesta antes del primer valor; y si la pestaña termina la suscripción mientras su `publish` tarda con el valor (con `Unwatch`, con su aviso, o con dos a la vez: un `Unwatch` y el aviso, dos `Unwatch`, dos avisos), el valor llega antes de cada respuesta, y nada después.
    - `a_confirmation_without_a_value_in_flight_is_not_kept_for_the_next_value`: dos confirmaciones seguidas, un cambio que llega, y otro que espera su propia confirmación.
    - `a_page_that_unwatches_from_inside_the_reply_to_its_watch_ends_the_subscription`: el `Unwatch` despachado desde la respuesta de su `Watch` la termina.
    - `a_request_of_a_token_goes_to_the_tab_that_opens_its_room_or_else_to_one_that_holds_it`: cada `room` le pide a su pestaña, y la respuesta de otra no cambia nada; la renovación de una escritura, fuera de un `room`, va a la de menor id de las que abrieron la sala; dos pedidos a la misma pestaña, contestados al revés, van cada uno a su pedido (cada sala se registra con su token).
    - `a_tab_that_left_ends_the_requests_of_a_token_to_it_and_gets_no_more`: el aviso de la pestaña cuyo `room` espera el token termina el pedido enseguida (con el reloj pausado, sin los 60 s), la renovación que sigue no le llega, y la sala sin metadatos no abre; la renovación de otra sala va después a la pestaña que queda; y, una vez que su aviso respondió, la pestaña abre otra vez una sala que la otra tiene, y el pedido de su `room` le llega.
    - `the_renewal_of_an_open_goes_to_its_tab_while_an_open_of_another_tab_waits_for_its_turn`, con el reloj pausado: la pestaña 1 abre una sala sin contestar su pedido, y la 2 la abre detrás; a los 60 s, la renovación del registro de la 1 le va a la 1, y después el `room` de la 2, en su turno, le pide a la 2.
    - `a_request_of_a_token_during_the_shutdown_goes_to_a_tab_of_its_room`: una escritura rechazada por su token, con el rechazo retenido, y un `shutdown` cuyo cierre de la sala en el motor está retenido también: el rechazo llega durante el apagado, y la renovación le pide el token a la pestaña que tenía la sala.
    - `an_unwatch_or_a_left_ends_a_subscription_without_a_value_in_flight` (de la segunda ronda de la revisión de delta): dos suscripciones con su primer valor confirmado, que esperan un cambio; sin ningún cambio en la sala, el `Unwatch` de una y el aviso de la pestaña de la otra responden, y `status` no cuenta ninguna.
    - `a_tab_that_leaves_during_the_shutdown_does_not_wait_for_its_close` (de la misma ronda): con el cierre de la sala del apagado retenido en el motor, el aviso de la pestaña que tiene la sala termina su suscripción y responde sin esperar ese cierre; confirmado después del aviso, su valor en vuelo no trae el fin de su stream.
    - `a_tab_that_left_during_the_shutdown_gets_no_request_of_a_token_of_it` (de la tercera ronda): las pestañas 1 y 2 tienen la sala, la 2 con un token vencido; la 2 escribe, con el rechazo de su commit retenido; llega el `shutdown`, con el cierre de la sala retenido en el motor, y la 1 se va; al soltar el rechazo, la renovación le va a la 2, y no a la 1.
  - En `crates/client/src/tests/dispatch.rs`: `the_marks_of_an_open_and_of_a_tab_that_left_are_undone_once_dropped` (las dos marcas, la de una pestaña que se va una por aviso, también con un pánico). En `crates/client/src/dispatch/tests/token.rs`: `a_request_of_a_token_goes_to_the_tab_that_opens_the_room_or_else_to_one_that_holds_it` (la marca del `room`, la de menor id, nunca una que se va, y ninguna con el despachador soltado) y `a_request_of_a_token_leaves_the_dispatcher_once_answered_or_once_its_caller_stops_waiting` (también con una respuesta sin token, una de otra pestaña, sin pestaña a la que pedirlo, y con el despachador soltado mientras un comando en vuelo retiene lo que comparte: el pedido en vuelo termina, y uno nuevo no se publica). En `crates/client/src/dispatch/tests/subscription.rs`, sobre la tarea con un stream propio: `a_subscription_publishes_one_value_at_a_time_and_leaves_once_its_stream_ended`, `a_subscription_cut_stays_until_its_task_ended_and_its_end_is_waited_for_more_than_once` (también una que no empieza), `a_subscription_whose_publish_panics_leaves_the_dispatcher` y `the_end_of_a_subscription_comes_once_it_left_the_dispatcher_and_its_stream_was_dropped` (con varios hilos, 200 veces: el fin se espera desde el hilo del test, que no es un worker del runtime, y cuando llega, la suscripción ya salió del despachador y su stream ya se soltó).
- **Mutaciones inversas** (en una copia, cada una con algún test que falla), 27: publicar el valor siguiente sin la confirmación; mandar el valor a otra pestaña; que el aviso no termine las suscripciones de su pestaña, o que termine las de las otras; que `Unwatch` no la termine, o que termine la de otra pestaña; que no llegue el aviso de fin (lo que dejaría colgada a la página después del cierre, del descarte o del apagado); que el pedido de token quede en el despachador cuando el cliente deja de esperarlo; que la respuesta vaya a otro pedido de la pestaña, o que se tome la de otra pestaña; que se tome la confirmación de otra pestaña; que la tarea empiece sin esperar la respuesta de su `Watch`; que `Unwatch` responda, o el aviso, antes de que terminen sus tareas; que una pestaña cuyo aviso está en vuelo reciba pedidos; sin la marca del `room` en su turno; la pestaña de mayor id en lugar de la de menor; que el aviso no termine los pedidos a su pestaña; que soltar el despachador no termine sus suscripciones, o que soltar una no corte su tarea; que una suscripción terminada quede en el despachador; que la marca del `room` no se deshaga; que la suscripción entre después de su respuesta, o el pedido después de su aviso (para una página que despacha desde `publish`); una confirmación que no se manda; la respuesta de la página que se pierde; y que el aviso olvide las tareas de las suscripciones que saca. La de la tarea que empieza sin esperar la respuesta la encontraban al principio solo los tests unitarios: en el de la página lenta, la demora de su `publish` con un valor era más larga que con la respuesta, y lo tapaba; ahora la respuesta tarda más, y ese test también la encuentra. Además, otra vez, las dos del 6a que encontraban los tres tests del 6a que cambiaron (en *Desvíos y precisiones*).

  Después de los arreglos de la primera ronda de la revisión de delta, las 27 otra vez sobre el código nuevo, todas encontradas, y las de los arreglos: que un segundo `Unwatch` o el aviso no esperen una suscripción que ya se está terminando (el Medio 1); que una suscripción terminada quede en el despachador (también sin empezar o con un pánico); que la marca de una pestaña que se va no se deshaga (el Medio 2); marcar el `room` al despacharse, y no en su turno; olvidar las pestañas de las salas al despachar el `shutdown`; que soltar el despachador no termine los pedidos de token, o que después todavía pida; y guardar una confirmación sin un valor en vuelo para el valor siguiente. Sobrevivían dos:
  - **Avisar el fin de una suscripción antes de sacarla del despachador y soltar su stream:** la nota decía que no se veía porque quien espera el fin se despierta en el worker que suelta la tarea, en el slot LIFO de Tokio. La segunda ronda de la revisión de delta mostró que eso no es una garantía (Tokio deja de usar ese slot después de varios polls seguidos, y la tarea que desplaza va a una cola que otro worker puede robar). Ahora la encuentra un test unitario, que espera el fin desde el hilo del test (arriba).
  - **Poner la marca de una pestaña que se va después de terminar sus pedidos:** abría una ventana en la que un pedido nuevo le iba a la pestaña, que ningún test reproduce de forma determinista. Ahora la marca se pone bajo el mismo lock con que terminan los pedidos, así que el orden no se ve.

  De la segunda ronda: que `end()` no corte la tarea (la encuentra el test de `Unwatch` y del aviso sin un valor en vuelo, que el revisor mostró sin test: una suscripción confirmada, que espera en su stream, la termina solo el corte); que el aviso cierre sus salas durante el apagado; y la del fin avisado antes, ya dicha. Quedan dos casi equivalentes, sin test: sin el chequeo de `cut` en `run`, una tarea que corre en el instante del corte publicaría un valor más, igual antes de la respuesta; y un `Shutdown` que nunca vacía `rooms`, que ya figuraba sin test desde el 6a.

  De la tercera ronda, las tres encontradas: que una pestaña que se fue durante el apagado quede en las pestañas de sus salas (la encuentra el test de la tercera ronda); que el aviso no haga nada durante el apagado (lo encuentra la confirmación que suma el test del aviso durante el apagado: con la mutación llega el fin del stream); y que el aviso cierre sus salas durante el apagado.

  En dos de ellas (que soltar el despachador no termine los pedidos de token, o que después todavía pida), el test unitario esperaba el pedido para siempre: la corrida de la mutación vencía, y su binario de tests quedaba corriendo, porque el plazo cortaba `cargo` y no a su hijo. Esos tests esperan ahora a lo sumo 5 s, y el script corta el grupo entero.
- **Interacciones revisadas.**
  - **La fachada con el worker (DC36, INV-K18):** cada suscripción es un stream de la fachada que vive en su tarea, así que el modo sale de ella como en la API de Rust; `Unwatch` y el aviso responden cuando la tarea terminó, y con ella el stream, así que el `status` que sigue ya ve el modo nuevo (el test del modo).
  - **El descarte a pedido con suscripciones (DC37):** termina sus streams, así que cada una avisa su fin, después de la confirmación de su valor en vuelo; la sala abierta otra vez, como nueva, no tiene ninguna.
  - **El cierre del cliente con suscripciones (INV-K11):** `shutdown` termina sus streams, y cada una avisa su fin; `Confirm` y `Unwatch` siguen andando, para las que tienen un valor en vuelo, y `Watch` es `RoomClosed`. Los pedidos de token del apagado tienen pestaña (en *Desvíos y precisiones*).
  - **El turno de una sala:** `Watch`, como las lecturas, no lo toma. El `room` en su turno es el que pide el primer token, y su marca dice a qué pestaña va; un `close`, un `discard` o un aviso de la sala despachados mientras tanto esperan ese turno, que espera el token (hasta 60 s por pedido, y hasta tres esperas, como en la fachada).
  - **El lock de registro mientras se espera un token** (el docstring de `TokenProvider`): la página contesta sin esperar un comando de la sala; lo dicen `Notice::Token` y §11 (*Tokens*).
  - **Una página que despacha desde `publish`:** la suscripción entra al despachador antes de su respuesta, y el pedido de token antes de su aviso, y nada se publica con el lock tomado: un `Unwatch` o un `Token` despachados desde `publish` encuentran lo suyo (los tests con el gancho).
- **Desvíos y precisiones.**
  - **A qué pestaña va un pedido de token.** El `TokenProvider` no sabe quién lo pidió (§1.3, 6), pero el despachador sabe de quién es cada sala: el pedido va a la pestaña cuyo `room` de la sala corre en su turno (la que pide el primer token, también de una sala que nadie tiene todavía), o si no a la de menor id de las que abrieron la sala (una renovación del worker o de una escritura de cualquier pestaña), nunca a una cuyo aviso de que se fue está en vuelo. Sin ninguna, no hay token, y no se publica nada. Es de las pestañas que tienen la sala, y no la que abrió el cliente, para que el token no dependa de una pestaña que puede irse antes que las demás. El paso D lo revisa.
  - **Si esa pestaña se va:** su aviso termina, al despacharse, los pedidos a ella, sin token (el cliente sigue como sin uno nuevo), y hasta que responde no le va ningún pedido; el siguiente pedido de esa sala va a otra pestaña que la tenga. El pedido en vuelo no pasa a otra pestaña: el cliente ya lo da por perdido.
  - **El plazo** es el del cliente: el pedido no tiene uno propio. El cliente suelta el future a los 60 s, y un guard saca el pedido del despachador; una respuesta tardía, o de otra pestaña, no cambia nada.
  - **El aviso de fin** dice solo la pestaña y la suscripción: el stream de la fachada termina sin decir por qué (el cierre, el descarte, el apagado o un error que sacó la sala). Llega después de la confirmación del valor en vuelo, porque la tarea no lee el stream hasta entonces. No llega después de la respuesta a un `Unwatch` de la suscripción ni a la del aviso de su pestaña, que esperan el fin de su tarea, también cuando hay varios; con un `Unwatch` en carrera con el fin del stream, `Ended` puede llegar, antes de esa respuesta.
  - **`Confirm`, `Unwatch` y `Token` de una suscripción o un pedido que no existe, o que es de otra pestaña,** responden `Done` sin cambiar nada: una suscripción que terminó o un pedido que venció son carreras normales con la página, y una pestaña no confirma, ni termina, ni contesta lo de otra. Un `Confirm` sin un valor en vuelo tampoco cambia nada (no se guarda para el valor siguiente).
  - **`Watch` no sigue el turno de la sala,** como las lecturas: arma su stream al despacharse, sobre la sala como el cliente la tiene, y una sala que se cierra después termina su suscripción.
  - **`Watch`, `Confirm` y `Token` responden en el momento,** como `ClientId`; `Unwatch` y el aviso, cuando terminaron las tareas de sus suscripciones.
  - **Los ids** de las suscripciones y de los pedidos de token salen de un mismo contador del despachador, y no se repiten.
  - **`Platform::open` recibe el `PageTokens`:** cambia la firma del 6a, y el `Platform` ya no arma el `TokenProvider`.
  - **Soltar el `Dispatcher` termina sus suscripciones y sus pedidos de token** (un `Drop`): sus tareas retienen el despachador, que si no viviría con ellas, y ninguna pestaña contesta ya un pedido. Un comando en vuelo sigue reteniendo lo que el despachador comparte, así que el `Drop` lo marca como soltado, y después no sale ningún pedido.
  - **Una suscripción cortada queda en el despachador hasta que su tarea termina** (de la revisión de delta): un `Unwatch` la sacaba al despacharse y solo su propia respuesta esperaba el fin de la tarea, así que el aviso de su pestaña, o un segundo `Unwatch`, respondían antes de un valor que la tarea estaba publicando. Ahora queda marcada como cortada (una confirmación no la alcanza), la tarea la saca al terminar, y su fin se espera cuantas veces haga falta.
  - **`Shutdown` olvida las pestañas de las salas cuando el cliente terminó de apagarse,** y no al despacharse (de la revisión de delta): así un pedido de token durante el apagado (el último `Ack` del worker, una escritura en vuelo) tiene pestaña, como la fachada le pide el token a su provider. Lo del 6a sigue igual: desde el `Shutdown`, ningún comando de una sala llega al cliente, y un `room` que termina después no cuenta su pestaña. Si la página no contesta un pedido de token del apagado, el apagado puede esperar hasta 60 s, como la fachada con un provider que no responde.
  - **Un aviso de que una pestaña se fue no cierra nada una vez que el cliente se apaga,** también uno despachado antes del `Shutdown` cuyo turno llega después, porque se mira en el turno (de la segunda ronda de la revisión de delta): como las pestañas de las salas se olvidan al final del apagado, el aviso las encontraba y llamaba a `close`, que esperaba el cierre del apagado; en el 6a no tocaba el cliente. Ahora, con el cliente apagándose, el aviso suelta sus salas pero no cierra ninguna (el apagado las cierra todas), y sigue terminando las suscripciones y los pedidos de token de la pestaña. Que las suelte es de la tercera ronda: en la segunda no las soltaba, y como su marca de pestaña que se va se deshace cuando responde, un pedido de token del apagado podía irle a la pestaña que se fue, si era la de menor id de la sala, y esperar sin respuesta hasta 60 s.
  - **La pestaña cuenta en la sala antes de que se vaya la marca de su `room`,** para que una renovación entre las dos no se quede sin pestaña.
  - **Un `Notice::Token` puede llegar después de la respuesta al aviso de su pestaña:** se publica sin el lock del despachador, y el aviso pudo terminar el pedido en el medio. Su respuesta no cambia nada.
  - **La marca del `room` en su turno, con un pánico:** el test unitario deshace el guard con un pánico, pero ningún test de integración provoca un pánico dentro de `ZemdbClient::room`, porque la fachada corre los cambios de la réplica en tareas propias, y un pánico del motor de prueba no llega a la tarea del despachador.
  - **Un `close` o un `discard` de cualquier pestaña** termina las suscripciones de la sala de todas, como en la fachada (§1.3, 11). **Un `Watch` de una pestaña que se fue** abre una suscripción, como un `room` abre la sala (§1.3, 4): los dos, del paso D.
  - **Con el Lote 7,** que se hizo en paralelo: el 7 entró primero, con el cambio mínimo en `dispatch/` (el `tokio::spawn` de `Shared::spawn`), y el 6b se rebasó sobre él y pasó a `rt` lo suyo, porque el lint de DC38 vale también en la producción nativa: la tarea de cada suscripción es un `rt::spawn`, y `Watcher` la corta con `rt::Task::abort`. El fin de una suscripción no se espera con su `Task` sino con el `oneshot` compartido de su guard (arriba), así que no depende de `TaskError`. El despachador no usa plazos ni relojes. Con un `tokio::spawn` en `dispatch/subscription.rs`, el clippy del workspace falla (se probó una vez y se sacó).
  - **Tres tests del 6a esperaban una ventana fija** (300 ms o 500 ms) para ver que unas llamadas no esperan una sala lenta o un cierre retenido: `a_slow_open_does_not_hold_back_the_calls_of_other_rooms_through_a_tab_that_left`, `a_tab_that_left_releases_each_of_its_rooms_without_waiting_for_a_slow_one` y `the_reads_writes_and_other_calls_of_a_room_do_not_wait_for_its_close`. Bajo carga, el segundo fallaba: en 100 corridas de a cuatro a la vez, 35 con un build en release al lado y 12 sin él, y también con el código del 6a (2 de 100), porque el binario, más grande con los tests del 6b, carga más la máquina. Como lo lento está retenido hasta que el test lo suelta, ahora los tres esperan cada respuesta (con la espera de la página, 30 s) antes de soltarlo: igual de estrictos, sin depender del tiempo. Las mutaciones del 6a que encontraban (que el aviso espere sus salas de a una; que las demás llamadas de una sala esperen su turno) siguen encontrándose.
  - **El test del pedido de token durante el apagado retiene el cierre de la sala en el motor,** y no el último `Ack` del worker: que ese `Ack` salga depende de lo que ya reconoció el worker (su primer heartbeat sale en un momento al azar), y la primera versión del test, que lo retenía, venció una vez esperándolo, durante las mutaciones.
- **La revisión de delta** la hizo un agente aparte, de contexto fresco y solo lectura, que probó en su propia copia con sus propias mutaciones y sondas. Corrió los tests del despachador 200 o 300 veces por ronda, también bajo carga (hasta ~34 de carga media). **Salió en verde en su cuarta ronda.**
  1. **Primera ronda: ningún Alto, 2 Medios y 10 Bajos, corregidos.**
     - **Los Medios:**
       - Un valor de una suscripción llegaba después de la respuesta al `Left` de su pestaña si un `Unwatch` de la misma suscripción estaba en vuelo (y con dos `Unwatch` o dos `Left`): `Unwatch` sacaba la suscripción al despacharse, y solo su respuesta esperaba la tarea. Se reprodujo seis de seis veces con varios hilos. De ahí sale el guard de la tarea, que tiene la suscripción en el estado hasta que termina, y su fin, que se espera más de una vez.
       - La marca de una pestaña que se va no tenía test de que se deshace: la mutación que no la deshacía sobrevivía, y con ella una pestaña que abre una sala después de su `Left` no recibía más pedidos de token. De ahí sale `Shared::leaving`, con sus tests.
     - **Los Bajos:**
       - tests para dos mutaciones que sobrevivían: la marca del `room` al despacharse, y un `Confirm` sin valor en vuelo que se guarda;
       - `Unwatch` corta la tarea al despacharse;
       - después del `Shutdown`, un pedido de token no tenía a qué pestaña ir, porque el apagado vaciaba las pestañas de las salas al despacharse, y la fachada sí lo pide: ahora se vacían cuando el apagado termina;
       - la pestaña cuenta en la sala antes de soltar la marca del `room`;
       - el `Drop` del despachador termina también los pedidos de token;
       - el docstring de `PageTokens`;
       - un pánico de `publish` dejaba la suscripción en el estado (lo resuelve el guard);
       - un `Notice::Token` puede llegar después de la respuesta al `Left` de su pestaña (una precisión);
       - los tests del token, a `dispatch/tests/token.rs`.
  2. **Segunda ronda, con el rebase sobre el Lote 7 y la migración a `rt`: ningún Alto, 1 Medio y 7 Bajos, corregidos.** Los dos Medios de la primera estaban cerrados, y la migración no cambiaba nada.
     - **El Medio,** un hueco de tests: nada probaba que `Unwatch` o `Left` terminen una suscripción sin valor en vuelo, que espera el próximo cambio (la más común), y que solo el corte de su tarea termina. La mutación que no la cortaba sobrevivía.
     - **Los Bajos:** el orden del guard, que la nota daba por inobservable, se puede ver (un test unitario lo fija); un `Left` durante el apagado volvía a llegar a la fachada, porque las pestañas de las salas se vacían al final (ahora no cierra nada); una contradicción de la nota; los 60 s que puede esperar el apagado por un pedido de token; y dos mutaciones casi equivalentes, anotadas.
  3. **Tercera ronda: ningún Alto, 1 Medio y 2 Bajos, corregidos.**
     - **El Medio lo trajo el arreglo del `Left` durante el apagado:** la pestaña que se iba no salía de sus salas hasta el fin del apagado, y un pedido de token del apagado le podía llegar a ella, que no lo contesta (hasta 60 s). Ahora suelta sus salas y no cierra ninguna. Se reprodujo con una sonda, que es el test de regresión.
     - **Los Bajos:** un test de que ese `Left` termina las suscripciones de su pestaña, y la precisión de un aviso despachado antes del `Shutdown` cuyo turno llega después.
  4. **Cuarta ronda: verde,** sin hallazgos en el código. La única mutación que sobrevive (dejar la entrada vacía de la sala durante el apagado) es equivalente.
- **Chequeos:**
  Sobre el Lote 7, con los arreglos de las tres rondas de la revisión de delta, repetidos en `main` sobre `acd89b6` (el Lote 7) con el lote integrado, con los mismos números (salvo las repeticiones del despachador y la prueba del lint, que se hicieron en el worktree del lote):
  - fmt; clippy del workspace con `--all-targets`, y del cliente con `--no-default-features --all-targets`, con `-D warnings`; el build a wasm32 de `zemdb-core`, `zemdb-storage` y `zemdb-client`, y su clippy con `-D warnings`; doc con `-D warnings`;
  - un `tokio::spawn` en `dispatch/subscription.rs` hace fallar el clippy del workspace por el lint de DC38 (se probó una vez y se sacó);
  - la suite del workspace, con 1714 tests y sin fallas (57 ignorados);
  - los `--ignored` del cliente en release, con 7 tests y sin fallas;
  - los tests del despachador (los de integración y los unitarios), 100 veces solos y 100 de a cuatro a la vez, sin fallas, y los de integración 104 veces más, de a ocho a la vez (con una carga media de 39), sin fallas. También pasaron así antes de la tercera ronda, antes de la segunda, antes del rebase, y antes de la primera, después del arreglo de los tres tests del 6a (arriba).

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

**Precisiones del Lote 2 (el turno de la capa).** El turno de `OpfsFiles` es un lock hoja: dentro de `run` solo se esperan promesas del OPFS. Por eso los reintentos de la apertura durante su plazo, y la espera del Web Lock, van fuera de `run`: una operación de la capa por intento, y nunca una espera con el turno tomado.

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
