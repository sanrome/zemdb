# Transporte nativo y `SyncWorker` — Plan de la sub-fase 4.3

**Fecha:** 4 de Noviembre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lote 0 (el contrato) cerrado el 6 de Noviembre de 2026, con la revisión cruzada en verde en su quinta ronda; las precisiones de DC18 y de DC24 a DC26 y las decisiones DC27 a DC31 quedan como *Propuestas*, que se confirman con el dueño del proyecto antes del Lote 1.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.3. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).
**Parte de:** el SSE del servidor, del Lote 5 de la [Fase 3.12](2026-11-fase3_12-rendimiento-servidor.md) (`2c0a908`): el evento `ops` con la encarnación de la sala, el stream que termina cuando vence su token y `EventsDisabled` (`ARCHITECTURE.md` §7.2).

---

## 0. Resumen

- **Qué.** El transporte HTTP nativo del cliente, sobre `reqwest`, con el stream de eventos; el `SyncWorker`, que programa por sala los pasos del núcleo de 4.2 (heartbeat, sync, `Ack`, bajada y subida de snapshots) según el modo de frescura (DC24), con el `Ack` periódico (DC25) y el evento `ops` (DC26); la renovación del token ante `Unauthorized`; y el contrato de lo que puede detener a una sala (INV-K17, nueva).
- **Dónde.** Solo en `zemdb-client` y sus tests, que está en el alcance auditado desde la auditoría que cerró 4.2 (`INVARIANTES.md` §1.4): cada lote, con su revisión de delta. No toca el servidor, `zemdb-core` ni `zemdb-storage`: lo que el SDK necesita del servidor (el SSE con las operaciones y el SSE apagado) está desde el Lote 5 de la Fase 3.12. El cambio de §2 de `INVARIANTES.md` aplica a los eventos SSE el criterio que ya vale para las respuestas (la precisión de DC11 que adoptó el dueño del proyecto al cerrar 4.2): no cambia el modelo de amenaza, así que no pide una auditoría amplia (§1.4).
- **Qué no.** La fachada de Rust, `watch` y el modo `live` atado a los `watch`, el `Offline` inmediato y la verificación del `ClientId` de un token renovado (4.4); el transporte del navegador y `EventSource` (4.5); la encarnación en los commits, los syncs y los acks (backlog para después de la v0.1, `ROADMAP.md` §3).
- **Decisiones:** las precisiones de DC18 y de DC24 a DC26 de la fase, y DC27 a DC31, nuevas (§1). Todas *Propuestas*.
- **Cómo cierra:** con una revisión de cierre (`docs/METODOLOGIA.md` §6), con dos revisores (§3).

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC6 (la fila de `Unauthorized` de la tabla de §A.4), DC11, DC18 (los pasos que programa el worker), DC21, DC22 y DC24 a DC26 (§3 del plan de la fase). Esta sub-fase precisa DC18 y DC24 a DC26, y agrega:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC27** | El transporte nativo: `reqwest`, plazos que no cortan un pedido que avanza, el tope de lo que lee, el stream por su propia conexión y su parser en el núcleo | 1, 2 | Propuesta (4 de Noviembre de 2026) |
| **DC28** | El `SyncWorker`: una tarea por sala, a pedido, cuyo heartbeat no espera las transferencias | 4 a 6 | Propuesta (4 de Noviembre de 2026) |
| **DC29** | Qué detiene a una sala: solo `FailedClosed` y `RoomClosed` | 3, 4, 7 y 8 | Propuesta (4 de Noviembre de 2026) |
| **DC30** | La renovación del token, con un `TokenProvider` de la aplicación | 3, 7 | Propuesta (4 de Noviembre de 2026) |
| **DC31** | Los endurecimientos opcionales de la auditoría de 4.2 (DEF-192 y DEF-196) | 6 | Propuesta (4 de Noviembre de 2026) |

### Precisiones de DC24 · Modos de frescura
- **Dónde se eligen.** `ClientConfig::freshness` (por defecto `periodic`) es el modo con que arranca el worker de cada sala, y `set_freshness` lo cambia por sala (DC28). La fachada de 4.4 enciende `live` mientras haya un `watch` sobre la sala.
- **`periodic` y `live`.** El heartbeat del worker, uno por intervalo (`heartbeat_interval`, 30 s por defecto), compara el head que informa su `HeartbeatAck` con el de la réplica, y si lo supera, el worker sincroniza. También en `live`: el SSE solo acelera, y lo que un stream no trae (un proxy que corta sin cerrar) llega en un intervalo.
- **El intervalo** es menor que los 60 s en que el servidor espera que el designado empiece a subir su snapshot, que el designado conoce por el ack de un heartbeat (DC22, INV-K7): uno de 60 s o más es `Config` al abrir el cliente (DC28). La aplicación lo elige también menor que el `lease_timeout` de la sala (90 s por defecto), que el cliente no conoce.
- **`manual`.** El heartbeat no lleva a un sync por su head, pero el worker sigue haciendo lo que el heartbeat le pide a la réplica: sube el snapshot si es el designado y baja el que espera (INV-K7: el designado sube igual). La réplica se pone al día con sus escrituras y con los `sync` que pide la aplicación.
- **Precisa DC18:** `sync` sigue público, y es también de la aplicación: con el worker, lo llama cuando quiere ponerse al día ya (en `manual`, o al volver a la pestaña), y la fachada de 4.4 lo expone para eso. `heartbeat` sigue siendo un paso de bajo nivel.
- **`EventsDisabled`.** `live` cae a `periodic`, `status` lo dice, y el worker vuelve a intentar el stream después de `events_retry` (10 min por defecto): el SSE apagado es de la configuración del servidor, que puede cambiar al reiniciarse.
- **El primer heartbeat** de un worker sale en un momento al azar dentro del intervalo, para que los clientes que arrancan juntos (después de un reinicio del servidor) no latan juntos.

### Precisiones de DC25 · El `Ack`, periódico
- **El sync del worker no manda `Ack`:** usa una variante interna del paso `sync` de 4.2. El `sync` público lo sigue mandando, porque es el paso de bajo nivel y el que pide la aplicación. Un `snapshot_wanted` en un lote del sync del worker se confirma con un heartbeat en la tarea de las transferencias, como hace el `sync` público (DC22).
- **El `Ack` del worker** lleva el head de la réplica, leído bajo el aplicador (INV-K5), con cada heartbeat si el head pasó del último confirmado, y una vez al detenerse (DC28). Su respuesta se valida como hoy (un `AckConfirmed` con la correlación, la sala y la secuencia del pedido), y un head menor que el confirmado, `InvalidSequence` o `ClientNotRegistered` llevan a registrarse con el cursor (DC21).
- **D78.** Un cliente en `live` presenta su cursor menos seguido (en los `Ack`, en sus escrituras y en sus syncs), como notó la revisión del Lote 5 de la Fase 3.12. Pero el heartbeat de cada intervalo compara su head con el de la réplica en cualquier modo, y uno menor lleva a registrarse con el cursor, donde `InvalidSequence` descarta (INV-K1): con el worker, la detección espera a lo sumo un intervalo, salvo que el head del servidor vuelva a alcanzar el cursor antes (el riesgo aceptado de INV-K1).

### Precisiones de DC26 · El evento `ops`
- **Cada stream es de una época** (DC7): el worker anota la época de la réplica al abrirlo, no usa los eventos de un stream de otra época, y lo cierra y abre otro apenas lo nota (en su próximo evento o heartbeat). Así no se aplica un evento de antes de un descarte por D78, que deja la misma encarnación.
- **Después de abrir un stream, un sync:** los eventos de antes de suscribirse no llegan. Primero se suscribe y después sincroniza; lo que llega mientras tanto se resuelve como cualquier aviso durante un sync.
- **La validación de un `ops`,** antes de usarlo y en este orden:
  1. que decodifique (base64 estándar con padding, y un frame del protocolo) como `ServerMessage::OpsEvent` de la sala del stream, con operaciones contiguas, no vacías, que terminan en su `head_seq`; si no, un sync, como ante un evento que no decodifica;
  2. la época del stream;
  3. la encarnación, que tiene que ser la de los metadatos: si no, la réplica se registra con su cursor, y es el registro el que descarta, con `RoomReset`;
  4. el esquema, con las reglas de DC20: con otro id o una versión posterior, el evento no se aplica, la réplica se registra y marca un sync, que trae sus operaciones con el esquema de `Registered`.

  Después, su head es un head visto (DC21). Si no pasa el head local, se ignora (el evento del commit propio, que ya aplicó su catch-up); si su primera operación llega hasta la siguiente al head local, el aplicador aplica las que siguen (descarta el prefijo y no aplica un hueco, DC21); si no, un sync.
- **Un evento nunca hace fallar cerrada la sala:** una operación de un evento que no valida con el esquema local lleva a registrarse y a sincronizar, como el paso 4, y es el sync el que decide (DC29).
- **Los demás eventos:** `head_advanced` y `lagged`, un sync; `schema_reloaded`, un registro con el cursor (DC20); `snapshot_wanted`, y `snapshot_available` si la réplica espera un snapshot, un heartbeat en la tarea de las transferencias, que confirma la designación o baja el anunciado (DC22); un evento de nombre desconocido y los comentarios (el keep-alive), nada.
- **Juntar avisos:** a lo sumo un sync del worker en vuelo por sala; lo que pide uno mientras tanto (un evento, un heartbeat) marca uno más para después, una sola vez.
- **Un stream que calla** 45 s (tres keep-alive del servidor, que manda uno cada 15 s) se da por muerto, y el worker abre otro.
- **El fin del stream** (venció su token, el servidor se apaga, otro stream del cliente tomó su lugar, la sala se detuvo) y cualquier error al abrirlo o al leerlo llevan a abrir otro, con backoff exponencial y *full jitter*; `Unauthorized`, con la renovación de DC30; `EventsDisabled`, con la caída de DC24.

### DC27 · El transporte nativo
- **`reqwest`,** con `rustls` y HTTP/2 (por ALPN sobre TLS; con `http://`, HTTP/1.1, o HTTP/2 con *prior knowledge* si se pide), en una feature `native` del cliente, por defecto (§A.2 del plan de la fase), solo fuera de wasm32. Un tipo público del transporte HTTP, sobre la URL base del servidor, con sus plazos configurables.
- **Los plazos no cortan un pedido que avanza, por lento que sea.** Conectar, 10 s; y un plazo de inactividad, 60 s: un pedido que no avanza durante ese tiempo, ni al mandar su cuerpo ni, una vez mandado, esperando los headers o recibiendo el cuerpo de la respuesta, se corta. Es lo que hace el servidor con una respuesta (el *stall timeout*: algún progreso cada 60 s), y el cuerpo de un pedido demasiado lento lo corta el servidor con su propio plazo. Sin plazo total: un `SyncBatch` de hasta 16 MiB (`MAX_FRAME_SIZE`; el núcleo pide tantas operaciones como sirva el servidor) o un chunk de 4 MiB que se baja terminan en un enlace lento que avanza, y uno que calla se corta. Una subida tiene además los plazos del servidor (INV-B2), que con sus valores por defecto piden una velocidad mínima: el del cuerpo de cada chunk (60 s más 1 s por cada 32 KiB), 120 s sin un chunk nuevo (con chunks de 4 MiB, unos 34 KiB/s) y el tope de la subida (2 h; para un snapshot de 512 MiB, unos 73 KiB/s). Un designado más lento no sube su snapshot, y el servidor designa a otro (el límite aceptado de INV-K7). Un plazo total, fijo o que crece con los bytes, impondría una velocidad mínima: debajo de ella, el mismo pedido fallaría siempre igual, y la réplica no se pondría al día teniendo red (INV-K7). Un plazo vencido, o ninguna respuesta, es un `TransportError`, y el intento queda incierto, como hoy. Una red que demora sin cortar está en el modelo de amenaza (`INVARIANTES.md` §2), y contra ella no se promete vivacidad: un pedido que avanza de a un byte por minuto no se corta, pero tampoco pasa de `MAX_FRAME_SIZE`.
- **El pool** retiene una conexión ociosa menos que el `header_read_timeout_secs` del servidor (10 s por defecto), después del cual el servidor la cierra, para no mandar un pedido sobre una conexión que el servidor está cerrando; el valor es configurable.
- **`send`:** el `POST` del frame con `Authorization: Bearer`, que devuelve el cuerpo sea cual sea el estado HTTP, sin leer más de `MAX_FRAME_SIZE` (DEF-195): un cuerpo más largo, por su `Content-Length` o a medida que llega, es un `TransportError`, sin leer el resto.
- **`events`,** método nuevo de `TransportClient`: el `GET /rooms/{room_id}/events` con `Authorization: Bearer`. Con 200, devuelve el cuerpo como una secuencia de bloques de bytes; con otro estado, los bytes del frame de error, con el mismo tope, que el núcleo decodifica (`EventsDisabled`, `Unauthorized`, `Forbidden`, `RateLimited`, `Unavailable`…). Su implementación por defecto responde el frame de `EventsDisabled`: un transporte sin stream de eventos se comporta como un servidor con el SSE apagado, y `live` cae a `periodic` (así siguen compilando los transportes que no lo necesitan, como los de los parches de la auditoría de 4.2). Lo implementan el transporte HTTP, el de prueba (sobre el router en proceso, en el Lote 2) y el de la simulación (en el Lote 9).
- **El stream va por su propia conexión,** con HTTP/1.1 (un cliente HTTP aparte del de los pedidos), sin plazo total: el de inactividad lo pone el worker (DC26). Con HTTP/2, los streams y los pedidos compartirían una conexión: el servidor acepta 32 streams a la vez por conexión (los proxies tienen su propio tope), así que muchas salas en `live` dejarían esperando a los heartbeats y a los commits; y un stream que no se lee durante 60 s con datos pendientes hace que el servidor cierre su conexión entera, con los pedidos en vuelo de todas las salas, que quedarían inciertos. El costo es una conexión por sala en `live`.
- **El núcleo parsea el SSE** (las líneas, `event:`, `data:`, los comentarios y la línea en blanco que cierra un evento), con un tope de 64 KiB por evento (un `ops` ocupa hasta unos 5,5 KiB). Un evento o una línea más largos, o bytes que no son UTF-8, cierran el stream: el worker abre otro y sincroniza. Un solo parser para todos los transportes: el del navegador (4.5) entrega los mismos bytes (con `fetch` y el cuerpo en streaming, que además manda el token en el header) o los rearma desde `EventSource`; lo decide su plan.
- **S4** (`docs/METODOLOGIA.md` §3), al arrancar el Lote 1, antes de su código: ¿compila `reqwest` 0.13 (con `rustls` sobre `aws-lc-rs`, su opción por defecto, y `http2`) en Linux con el MSRV del workspace (Rust 1.85: el job de MSRV construye `--workspace --lib --bins`), sin tocar la compilación a wasm32 de `zemdb-core`, `zemdb-storage` y `zemdb-client`, y cuánto suma al tiempo de compilación? ¿Cómo se arma con su API el plazo de inactividad, al recibir (`read_timeout`) y al mandar el cuerpo de un pedido, sin un plazo total? ¿Qué hace su cliente HTTP/2 cuando una conexión llega a su tope de streams: espera o abre otra? macOS y Windows los responde la CI del Lote 1; si `aws-lc-rs` no compila en alguno, la alternativa es `rustls-no-provider` con `ring`. `reqwest` 0.13.5 declara `rust-version = 1.85.0`.
- **Alternativas descartadas:** `hyper` directo (el pool, HTTP/2 y TLS a mano); `reqwest-eventsource` o `eventsource-stream` (sin tope por evento, y el navegador tendría otro parser); un parser del SSE en cada transporte (dos parsers, y dos superficies de fuzzing); los streams por HTTP/2, en la conexión de los pedidos (el tope de streams y el cierre de toda la conexión por un stream trabado, arriba).

### DC28 · El `SyncWorker`
- **Por sala, a pedido.** `Client::start_worker(room_id)` arranca el worker de una sala que el cliente abrió (abierta, aunque no esté registrada, o vacía después de un registro que falló: su primer heartbeat la registra), en el modo de `ClientConfig::freshness`; `set_freshness(room_id, mode)` lo cambia (y lo arranca, si la sala no tiene); `stop_worker(room_id)`, `close_room` y `shutdown` lo detienen. Una sala sin worker se maneja con los pasos de 4.2, como hasta ahora (los tests de 4.2 y la API de bajo nivel de DC18); la fachada de 4.4 arranca el worker de cada sala que abre.
- **Una tarea de Tokio por sala,** que tiene al cliente por un `Weak` (esos métodos toman `self: &Arc<Self>`): si la aplicación suelta el cliente, el worker termina. Los syncs y las transferencias del worker corren en tareas propias: a lo sumo un sync y una transferencia del worker por sala (las escrituras y los pasos que llama la aplicación suman los suyos, y el lock de las transferencias del núcleo deja una a la vez).
- **El heartbeat del worker no espera las transferencias.** Renueva el lease y toma lo que dice su ack (el esquema, el head visto, la designación, el snapshot anunciado), pero no baja ni sube nada: si el ack designa al cliente o anuncia el snapshot que la réplica espera, el worker hace la transferencia en su tarea, y los heartbeats siguen mientras tanto, porque los chunks no renuevan el lease (de los Lotes 8 y 9 de 4.2).
- **El worker decide solo por los errores de sus propios pasos** (el registro, el heartbeat, el sync, el `Ack`, las transferencias, el stream), que leen la réplica bajo el aplicador y no ven un cambio a medias, como un descarte. Nunca por `status`, que, como en 4.2, responde `RoomClosed` mientras la réplica no está abierta (vacía, o a mitad de un cambio): una sala vacía no muestra su worker en `status` hasta que se abre.
- **Detenerlo** corta sus tareas en su próximo `.await` (el núcleo lo admite: cada cambio de la réplica corre en una tarea propia, DC21) y espera que terminen; después manda el último `Ack` si el head se movió (un intento, con un plazo de 5 s; en `shutdown`, los de todas las salas a la vez), y recién entonces `close_room` y `shutdown` cierran la sala del storage (INV-K11).
- **Reintentos:** un paso que falla se reintenta con backoff exponencial y *full jitter* (`retry_base`, 1 s, que se duplica hasta `retry_max`, 30 s), y el primero que sale bien vuelve al intervalo. Ningún ciclo sin espera.
- **La configuración,** en `ClientConfig`: `freshness`, `heartbeat_interval`, `retry_base`, `retry_max` y `events_retry`, con sus valores por defecto. Una que no puede funcionar (un intervalo en cero o de 60 s o más, un backoff que empieza en cero o cuyo máximo es menor que su base, un `events_retry` en cero) es `Config` al abrir el cliente.
- **`status`** suma, como datos serializables (DC18): si la sala tiene worker y en qué modo, el modo efectivo (`live` caído a `periodic`), si su stream está abierto, si el último pedido de la sala tuvo respuesta (el estado de la conexión, que 4.4 usa para `Offline`) y el último error del worker.

### DC29 · Qué detiene a una sala
- **El contrato** (INV-K17, nueva): ninguna respuesta ni evento que no cuenta como del servidor (`INVARIANTES.md` §2) lleva a un error que el SDK o su worker traten como permanente para la sala. Es la propuesta 3 de la auditoría de 4.2, que el dueño del proyecto adoptó para este contrato el 31 de Octubre de 2026.
- **Lo único que detiene al worker,** cuando lo devuelve uno de sus pasos (DC28):
  - `FailedClosed`: la sala falla cerrada, el núcleo se da de baja y no manda más heartbeats (DC20). Sale solo de un `Registered` con un esquema que no evoluciona del local, o de una operación de un `SyncBatch` que no valida después de que el mismo sync se volvió a registrar, con lo que la réplica ya tiene el esquema de `Registered`: respuestas que cuentan como del servidor. El catch-up de un `CommitAck` no la hace fallar cerrada (lleva a registrarse, y decide el sync de la escritura), y un evento tampoco.
  - `RoomClosed`: la aplicación cerró la sala o el cliente, o un error del storage local la sacó del cliente.

  Cerrar la sala y volver a abrirla (`close_room` y `open_room`) vuelve a intentar: `open_room` sobre una sala abierta no la reabre, y la que falla cerrada sigue así hasta que se cierra (DC20). Arrancar otra vez el worker solo retoma una sala que la aplicación reabrió.
- **Todo lo demás se reintenta** con el backoff del worker: `Network`, `Storage` con la sala abierta, y también `Config`. Responde lo que el plan de la fase deja a este Lote 0: el `Config` de una reserva que no entra en memoria, que viene del dispositivo, no es permanente (la memoria puede liberarse); tampoco el de un snapshot mayor que `max_snapshot_bytes`, que sale del primer chunk validado de una bajada y cuenta como del servidor, pero que la aplicación arregla subiendo el tope o descartando la sala. Cada intento cuesta una sonda de 64 KiB (DEF-190), uno por heartbeat, y `status` muestra el error.
- **Un evento nunca detiene nada** (DC26): ni uno que no decodifica, ni una operación que no valida.
- **`EventsDisabled`** es permanente para la configuración del servidor, no para la sala: `live` cae a `periodic` (DC24).

### DC30 · La renovación del token
- **Un `TokenProvider`,** que la aplicación da al abrir el cliente (`Client::open`), junto al transporte: un método asíncrono que devuelve un token nuevo para una sala, o un error. Se confía en él como en la aplicación (`INVARIANTES.md` §2). El token de `open_room` sigue siendo el primero, y el de un `open_room` posterior reemplaza al renovado.
- **Ante `Unauthorized`** a cualquier pedido (el registro, que también lleva el token adentro; un sync, un ack, un heartbeat, un commit, un chunk, la baja y el stream), el núcleo le pide un token al proveedor, una vez por sala a la vez (los pedidos que recibieron `Unauthorized` con el mismo token esperan esa renovación y usan su resultado), lo guarda como el token de la sala y repite el pedido una vez con él. Un segundo `Unauthorized`, o un proveedor que falla, es "sin token nuevo" (§A.4 del plan de la fase): el pedido falla como hoy, y el worker reintenta con su backoff, pidiendo otro.
- **El proveedor tiene un plazo** (60 s). Una renovación que vence, que falla o cuyo pedido se corta no deja esperando a los demás: el siguiente que necesite un token lo vuelve a pedir.
- **En una escritura,** un intento con `Unauthorized` no tuvo efecto (la autenticación va antes que todo), así que no la deja incierta: reintenta con el token nuevo, con el mismo `MutationId` y dentro del plazo de DC6. Sin token nuevo, termina como dice la tabla de §A.4: `OutcomeUnknown` si algún intento quedó incierto y, si no, un error sin efecto.
- **`Forbidden`** sigue siendo permanente para ese token: una escritura termina con `Validation` (§A.4). Además, el núcleo marca el token de la sala como inservible, y el pedido siguiente de la sala (del worker, de una escritura o de un paso) le pide otro al proveedor antes de mandarse.
- **El `ClientId` del token renovado:** verificarlo sigue en 4.4 (DC15). Mientras tanto, el servidor rechaza con `Forbidden` todo pedido cuyo mensaje nombra otro cliente que su token, así que un token de otro `ClientId` nunca secuencia una operación con otra clave de dedup (INV-Y4).
- **Sin renovación proactiva:** el cliente no lee el vencimiento del token. El stream que termina con su token se vuelve a abrir, y el `Unauthorized` de ese intento renueva.

### DC31 · Los endurecimientos de la auditoría de 4.2
Los dos opcionales que el plan de la fase deja a 4.3. Solo los dispara una respuesta dañada que cuenta como del servidor, fuera de alcance por `INVARIANTES.md` §2, pero con el worker durarían hasta que la aplicación cierre la sala:
- **DEF-196:** una réplica que espera un snapshot que nadie anuncia se registra otra vez cada 10 heartbeats del worker (5 min por defecto). `Registered` vuelve a decidir la espera con la cola del log y, si la réplica está dentro, deja de esperar. Unas 10 líneas.
- **DEF-192:** un `SyncBatch`, un `HeartbeatAck` o un `AckConfirmed` con un head menor que el head visto (DC21) al mandar su pedido llevan a registrarse, y el head visto de la época pasa a ser el de `Registered`. El head del servidor no baja (la respuesta que dio el head visto salió del servidor antes de que saliera el pedido), salvo D78, que el registro detecta. Una escritura que ya anotó su `seen` lo conserva. Unas 15 líneas.

### 1.1. Lo que hereda

| # | De dónde | Qué | Cómo se resuelve | Lote |
|---|---|---|---|---|
| 1 | Plan de la fase, §4.3 | El adaptador `reqwest` sobre HTTP/2 | DC27 | 1, 2 |
| 2 | §4.3 | El worker: SSE con la reacción a cada evento, pull paginado, `Ack`, heartbeat y reconexión con backoff y *full jitter* | DC28 y DC26; el pull paginado es el `sync` de 4.2 | 4 a 8 |
| 3 | §4.3, de los Lotes 8 y 9 de 4.2 | El worker no encadena los heartbeats detrás de una bajada o una subida larga | DC28 | 4 |
| 4 | §4.3 | Los modos de frescura, el `Ack` periódico y el evento `ops`, con su caída a `/sync` y a `periodic` | Las precisiones de DC24 a DC26 | 5 a 8 |
| 5 | §4.3 | La renovación del token ante `Unauthorized` | DC30 | 3 |
| 6 | §4.3, propuesta 3 de la auditoría de 4.2 | La invariante de los errores permanentes, y si el worker trata como permanente el `Config` de una reserva que no entra en memoria | INV-K17 y DC29: no lo trata como permanente | 0, 3, 4, 7 y 8 |
| 7 | §4.3, DEF-195 | El adaptador no lee un cuerpo más largo que `MAX_FRAME_SIZE` | DC27 | 1, 2 |
| 8 | §4.3, DEF-192 y DEF-196 (opcionales) | Un head más allá del servidor, y una cola que deja la réplica esperando un snapshot que nadie anuncia | DC31 | 6 |
| 9 | Lote 5 de la Fase 3.12 | La encarnación del evento, el stream que termina con su token y `EventsDisabled` | DC26, DC30 y DC24 | 7, 8 |
| 10 | Lote 5 de la Fase 3.12 (nota de su revisión) | Un cliente que aplica eventos presenta su cursor menos seguido (D78) | El heartbeat compara su head en cualquier modo (DC25) | 0 (INV-K1) |
| 11 | DC18 (plan de 4.2) | El estado de la conexión, para el `Offline` de 4.4 | `status` (DC28) | 4 |

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC18, DC24 a DC31 | Nueva: K17. Ajustes: K1, K2, K5, K6, K7, K11, K12, K15 y §2 | ✅ |
| 1 | El transporte HTTP nativo | client | DC27 | K12, K7 | ⬜ |
| 2 | El stream de eventos: el transporte y el parser | client | DC27 | K12, K15, K7 | ⬜ |
| 3 | La renovación del token | client | DC30, DC29 | K2, K17 | ⬜ |
| 4 | El worker: la tarea, el heartbeat y las transferencias | client | DC28, DC29 | K7, K15, K17 | ⬜ |
| 5 | Los modos `periodic` y `manual` | client | DC24, DC25, DC28 | K7, K15 | ⬜ |
| 6 | El `Ack` periódico, detener el worker y los endurecimientos | client | DC25, DC28, DC31 | K5, K11, K2, K7 | ⬜ |
| 7 | El modo `live`: el stream en el worker | client | DC24, DC26, DC29, DC30 | K7, K6, K12, K15, K17 | ⬜ |
| 8 | El evento `ops` | client | DC26, DC29 | K1, K6, K2, K7, K12, K17 | ⬜ |
| 9 | El arnés de simulación con el worker y el SSE | client (tests) | §5.3 del plan de la fase | K1, K2, K3, K4, K7, K15, K17 | ⬜ |
| 10 | Robustez: fuzzing de los eventos | client (tests) | — | K12, K17 | ⬜ |
| 11 | Revisión de cierre y cierre | Documentos | Todas | Las de §3.11 que toca 4.3 | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirman antes del Lote 1.
- El 1 y el 2 (el transporte) y el 3 (el token) no dependen entre sí. Del 4 al 8, en orden: cada uno usa lo anterior, y el 7 usa además el `events` del 2 y la renovación del 3.
- El 9 y el 10 son pruebas, sin producción (salvo arreglos de lo que encuentren, que van como desvíos en su nota o como lotes nuevos); el 9 antes que el 10, que reusa su transporte.
- El 11 cierra con la revisión de cierre (§3); sus rupturas se arreglan en lotes nuevos de 4.3.

**Tamaño.** Unas 150 líneas de producción el Lote 1, 200 el 2, 150 el 3, 200 el 4, 150 el 5, 150 el 6, 250 el 7 y 150 el 8.

**Pruebas.**
- **El worker (Lotes 4 a 8),** contra el servidor real en proceso, como en 4.2, con el reloj pausado (el worker mide sus plazos con el reloj de Tokio) y el transporte de prueba de `crates/client/tests/common/`, que suma `events` sobre el cuerpo SSE del router. Lo que el servidor no produce (un evento dañado o de otra encarnación, un stream que calla) lo fabrica el transporte de prueba. El worker se observa sin hacer `pub` nada interno: por los pedidos que registra el transporte de prueba, por `status` y por el Control Plane del servidor (`GET /admin/rooms/{id}`, con los clientes por estado).
- **El vencimiento de los tokens con el reloj pausado.** El servidor verifica el token en hora de pared (§5.3 del plan de la fase), que el reloj pausado no controla, pero espera el fin del stream con el reloj de Tokio (lo que le queda al token en hora de pared, como un `sleep`), que sí avanza: con el reloj pausado, un stream termina cuando pasa el tiempo de su token en tiempo virtual y se vuelve a abrir con el mismo token, que en hora de pared sigue vigente. Por eso los tests del worker emiten tokens más largos que su tiempo virtual (o cuentan con esos cortes, y con el sync que sigue a cada reapertura), y uno de ellos prueba esa reapertura sin renovación. La renovación se prueba fabricando el vencimiento: el proveedor de prueba da tokens que ya vencieron al emitirse (con un TTL de 0), o el transporte de prueba termina el stream y responde `Unauthorized` a la reapertura. El fin del stream por el vencimiento real se prueba con el reloj real, en el Lote 2.
- **El transporte HTTP (Lotes 1 y 2),** contra el servidor real en un socket local (`serve_until_shutdown` sobre `127.0.0.1:0`), con el reloj real y plazos cortos; lo que el servidor no produce (un cuerpo más largo que `MAX_FRAME_SIZE`, uno lento, un servidor que no responde), con un servidor HTTP mínimo del test.
- **El parser del SSE (Lote 2),** con tests unitarios en `src/tests/`, que acceden a lo privado (`GEMINI.md`), y con fuzzing: una versión corta en la suite y una larga con `#[ignore]`.

**Checklist propio.** Los Lotes 3 a 8 recorren este checklist, además del del cliente de 4.2 (§2 de su [plan](2026-10-fase4_2-nucleo-replica.md)) para lo que cambie del núcleo; el 8, que cambia lo que acepta una réplica, también el de determinismo de la Fase 3.11 (§5.4 de su [propuesta](2026-10-fase3_11-restricciones-proposal.md)).
- Cada `.await` del worker es un punto de corte: detenerlo no deja nada a medias (los cambios de la réplica corren en sus tareas, DC21), y ningún lock síncrono cruza un `.await`.
- Todo ciclo que reintenta espera, un intervalo o un backoff con *full jitter*: ningún paso que falla enseguida se repite sin espera.
- Ningún evento ni respuesta se usa antes de validarlo y de comparar su época (DC21, DC26).
- El worker tiene al cliente por un `Weak`, y cada tarea que lanza termina cuando se detiene (`stop_worker`, `close_room`, `shutdown` o soltar el cliente).
- El worker decide por los errores de sus pasos, nunca por `status`, y ningún error fuera de los de DC29 lo detiene.
- Nada crece por evento: no hay colas, y los avisos se juntan en una marca (DC26).
- Los argumentos y los resultados de los métodos públicos nuevos son datos propios y serializables (DC18); el transporte y el `TokenProvider` se dan al abrir el cliente.

### Lote 0 · Contrato · ✅ Hecho
**Objetivo:** el contrato contra el que se revisa cada lote de 4.3. Solo documentos.

**Qué se hace:**
- **Este plan,** con las precisiones de DC18 y de DC24 a DC26 y DC27 a DC31 como *Propuestas*, lo que hereda (§1.1) y la revisión de cierre (§3).
- **`INVARIANTES.md`, versión 16:**
  - **INV-K17, nueva** (§3.11), 🆕 (4.3, Lotes 3, 4, 7 y 8): ninguna respuesta ni evento que no cuenta como del servidor lleva a un error que el SDK o su worker traten como permanente para la sala (DC29).
  - **§2:** un evento SSE cuenta como del servidor con el mismo criterio que una respuesta; el `TokenProvider` es de la aplicación (DC30).
  - **INV-K1:** el evento `ops` en "Sostenida por", con la época del stream (DC26), 🆕 (Lote 8); en el riesgo aceptado, la detección de D78 por el heartbeat del worker (DC25).
  - **INV-K2:** la renovación del token (DC30), 🆕 (Lote 3), DEF-192 (DC31), 🆕 (Lote 6), y el head del evento `ops` como head visto (DC26), 🆕 (Lote 8).
  - **INV-K5:** el `Ack` periódico del worker (DC25), 🆕 (Lote 6).
  - **INV-K6:** los eventos `ops` y `schema_reloaded`, y que un evento no hace fallar cerrada la sala (DC26, DC29), 🆕 (Lotes 7 y 8).
  - **INV-K7:** qué es "al día" en cada modo (DC24), el worker y su intervalo menor que 60 s (DC28), los plazos del transporte y el stream por su propia conexión (DC27), y DEF-196 (DC31), 🆕 (Lotes 1, 2 y 4 a 8).
  - **INV-K11:** detener el worker antes de cerrar la sala (DC28), 🆕 (Lote 6).
  - **INV-K12:** el tope del cuerpo del transporte HTTP (DEF-195, Lote 1), el del parser del SSE (Lote 2), los eventos en el worker (Lotes 7 y 8) y su fuzzing (Lote 10), 🆕.
  - **INV-K15:** el worker y su stream (DC28, DC27), 🆕 (Lotes 2, 4, 5 y 7).
  - El resumen, con el lote de 4.3 que implementa cada parte; §1.6, con las decisiones hasta DC31.
- **El plan de la fase:** DC27 a DC31 en la tabla de decisiones, como *Propuestas*; la nota de las precisiones en DC18, DC24, DC25 y DC26; 4.3 en curso, con el enlace a este plan (encabezado, §4, §4.3 y §8).
- **`ROADMAP.md`:** 4.3 en curso, con el enlace a este plan.
- `ARCHITECTURE.md` no cambia: describe comportamiento implementado. Desde el Lote 1, cada lote suma lo suyo a §11.

**INV-K17** (Medio si se rompe): su enunciado, cómo se sostiene y cómo intentar romperla están en `INVARIANTES.md` §3.11, que es la única versión.

**Entrega:** `docs: plan Fase 4.3 with its batches and the native transport and worker decisions, and adjust the client contract for it (Fase 4.3, Lote 0)`.

**Hecho.**
- **Documentos:** este plan, con las precisiones de DC18 y de DC24 a DC26 y DC27 a DC31 como *Propuestas*, lo que hereda (§1.1) y la revisión de cierre con dos revisores (§3); `INVARIANTES.md` en la versión 16 (INV-K17 nueva; ajustes de §2, K1, K2, K5, K6, K7, K11, K12 y K15, el resumen y §1.6); el plan de la fase con DC27 a DC31, las notas en DC18 y DC24 a DC26, la respuesta del Lote 0 en §4.3 y 4.3 en curso; `ROADMAP.md` con 4.3 en curso.
- **Revisión cruzada** (agentes aparte, de contexto fresco y solo lectura), en cinco rondas, cada una acotada a las correcciones de la anterior:
  1. **2 bloqueantes, 6 ajustes y 10 menores.**
     - **Los bloqueantes:**
       - el plazo de 60 s de DC27 era para el pedido entero, así que un `SyncBatch` de 16 MiB o un chunk de 4 MiB en un enlace lento fallarían siempre igual (INV-K7);
       - "reabrir la sala, o arrancar otra vez el worker, vuelve a intentar" (INV-K17, DC29) era falso: una sala que falla cerrada sigue así hasta que se cierra, y `open_room` sobre una sala abierta no la reabre.
     - **Los ajustes:**
       - con HTTP/2, los streams y los pedidos comparten una conexión (el tope de 32 streams del servidor, y un stream sin leer que hace cerrar la conexión entera): el stream pasó a su propia conexión HTTP/1.1;
       - el `Forbidden` del worker, sin lote: ahora el núcleo marca el token como inservible (Lote 3);
       - los tests con el reloj pausado contra el vencimiento de los tokens en hora de pared;
       - `status`, que responde `RoomClosed` para una réplica vacía: el worker decide solo por los errores de sus pasos;
       - un test del `ops` de la sala recreada que podía pasar sin el chequeo de la encarnación;
       - el `snapshot_wanted` de un lote del sync del worker, y el intervalo por debajo de los 60 s de la designación.
     - **Los menores:** la precisión de DC18 (la aplicación sí llama a `sync`); "no toca código auditado"; las funciones privadas en INV-K17; el `ops` con otro esquema, que ahora también sincroniza; `events` y los transportes de prueba; K3 en la simulación; el plazo del `TokenProvider`; la poda con el cursor atrasado; INV-K15; las fechas; partir el lote del worker (los lotes pasaron de 11 a 12); un `ops` sin operaciones.
  2. **1 bloqueante, 2 ajustes y 5 menores.**
     - **El bloqueante:** el plazo que crecía con los bytes, que reemplazaba al total fijo, imponía igual una velocidad mínima. Quedaron solo el de conectar y el de inactividad.
     - **Los ajustes:**
       - con el reloj pausado, el stream sí termina, en tiempo virtual, y se reabre con el mismo token;
       - un cliente que baja un snapshot está `Bootstrapping`, no `Connected`.
     - **Los menores:**
       - la fila de DC18 del plan de la fase;
       - las referencias a lotes;
       - el intervalo menor que 60 s, que ahora exige `Client::open`;
       - una implementación por defecto de `events` (el frame de `EventsDisabled`), para los parches de la auditoría de 4.2;
       - dos redacciones.
  3. **Sin bloqueantes; 1 ajuste y 4 menores.**
     - **El ajuste:** las listas de lotes de INV-K7, K15 y K17 y de DC29 no coincidían entre `INVARIANTES.md`, la tabla de lotes y las líneas **Documentos**. Ahora coinciden, verificado con un script.
     - **Los menores:**
       - "uno de 60 s o más";
       - DC18 en los resúmenes;
       - los chunks que se suben, que el servidor corta por debajo de una velocidad;
       - `events_retry` en cero, que ahora es `Config`.
  4. **1 ajuste y 2 menores.**
     - **El ajuste:** para una subida manda el plazo del relay de 120 s sin un chunk nuevo (unos 34 KiB/s con chunks de 4 MiB) y su tope de 2 h, no el del cuerpo. Quedó como límite aceptado de INV-K7, con sus cifras.
     - **Los menores:** dos redacciones.
  5. **Verde,** sin hallazgos.
- **Desvíos:**
  - la precisión de DC18, que no estaba en el plan de la fase, salió de la revisión;
  - INV-K7 suma un límite aceptado (las velocidades mínimas de una subida, que ya enunciaba INV-B2).
- **Pendiente:** la confirmación de las *Propuestas* por el dueño del proyecto, antes del Lote 1.

### Lote 1 · El transporte HTTP nativo (`zemdb-client`) · ⬜
**Objetivo:** que el núcleo hable con un servidor real por HTTP, sin leer de más y sin cortar un pedido que avanza (DC27, DEF-195).

**Archivos:** `crates/client/Cargo.toml` (la feature `native`, por defecto, con `reqwest`), `src/` (un módulo del transporte HTTP y `lib.rs`), y sus tests (`tests/http_transport_tests.rs`, con el servidor en un socket local). `ARCHITECTURE.md` §11 (*Transport*).

**Qué se hace:** primero S4 (DC27), con su respuesta anotada en DC27. Después, el tipo público del transporte HTTP y su `send`: el `POST` del frame con el token, el cuerpo hasta `MAX_FRAME_SIZE`, los plazos (conectar y la inactividad, configurables), el pool de conexiones y HTTP/2.

**Tests exigidos.**
- Un cliente del núcleo sobre el transporte HTTP, contra el servidor real en un socket local, abre una sala, escribe, sincroniza y manda un heartbeat; los errores del servidor (un token vencido, `Unauthorized`; uno de otra sala, `Forbidden`) llegan como sus frames, con el estado HTTP que sea.
- Los mismos pedidos con HTTP/1.1 y con HTTP/2 (*prior knowledge*).
- Un servidor del test que manda una respuesta grande despacio, sin pausas más largas que el plazo de inactividad, y uno que recibe despacio el cuerpo de un pedido grande: los dos terminan bien, con el plazo acortado para el test y una duración total de varias veces el plazo (no hay plazo total).
- Negativos: un cuerpo más largo que `MAX_FRAME_SIZE`, con su `Content-Length` y sin él, es `TransportError`, sin leerlo entero; un servidor que se calla más que el plazo de inactividad antes de los headers, en medio del cuerpo de la respuesta o mientras recibe el del pedido, también, y un puerto cerrado; en todos, la escritura queda incierta o termina sin efecto, como dice §A.4.

**Interacciones a revisar:** el `header_read_timeout_secs` del servidor, que cierra las conexiones ociosas, y el pool; el *stall timeout* y el plazo del cuerpo del servidor, contra los del cliente; el tope de conexiones del servidor; el presupuesto de respuestas (un `503` llega como un frame); `TransportError` en cada paso del núcleo (incierto en una escritura); la compilación a wasm32 (el crate sigue vacío ahí) y el MSRV.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K12 (DEF-195) y K7.

**Entrega:** `feat(client): the native HTTP transport, over reqwest (Fase 4.3, Lote 1)`.

### Lote 2 · El stream de eventos: el transporte y el parser (`zemdb-client`) · ⬜
**Objetivo:** que el transporte abra el stream SSE por su propia conexión y que el núcleo lo lea sin pánicos ni reservas de más (DC27).

**Archivos:** `src/transport.rs` (`events` y su resultado), el transporte HTTP, un módulo privado del parser y de la decodificación de los eventos, con sus tests unitarios en `src/tests/`; `tests/common/` (el `events` del transporte de prueba, sobre el router en proceso) y los tests del transporte HTTP. `ARCHITECTURE.md` §11.

**Qué se hace:** `TransportClient::events`, con su implementación por defecto (el frame de `EventsDisabled`), en el transporte HTTP (con su propio cliente HTTP/1.1) y en el de prueba; el parser, con su tope; y la decodificación de cada evento a un tipo interno (`ops` con su `OpsEvent`, `head_advanced` con su head, `schema_reloaded`, `snapshot_wanted`, `snapshot_available` con su secuencia, `lagged`, y uno desconocido), que todavía nadie usa: lo usa el worker desde el Lote 7.

**Tests exigidos.**
- El transporte HTTP, contra el servidor en un socket: el stream trae un `event: ops` con el frame del commit de otro cliente; con el SSE apagado, el frame de `EventsDisabled` (HTTP 404); sin token, `Unauthorized`; y, con el reloj real y un token de vencimiento corto, el stream termina cuando vence su token.
- El stream va por su propia conexión: con varios streams abiertos sin leer, los pedidos del mismo transporte siguen saliendo y respondiéndose. Un transporte sin `events` propio responde `EventsDisabled`.
- El parser (unitarios): un evento partido en cualquier punto entre bloques, varias líneas `data:`, `\r\n`, comentarios y campos desconocidos; los seis eventos del servidor y uno de nombre desconocido.
- Negativos: un evento o una línea de más de 64 KiB, bytes que no son UTF-8, base64 inválido, y un frame que no decodifica o que es de otro mensaje son un error tipado, sin pánico; el fuzzing con bytes al azar y eventos dañados, en su versión corta y en la larga.

**Interacciones a revisar:** el keep-alive del servidor, cada 15 s; el tope del evento del servidor (4 KiB de frame); el estado HTTP y el `Content-Type` del stream y de sus errores; el tope de streams por cliente (el tercero cierra el más viejo) y el total del servidor (`max_sse_streams`, la mitad de las conexiones); el *stall timeout*, que cierra la conexión de un stream que no se lee; el corte del stream en el apagado del servidor.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K12, K15, K7.

**Entrega:** `feat(client): open the event stream on its own connection and read its events within a limit (Fase 4.3, Lote 2)`.

### Lote 3 · La renovación del token (`zemdb-client`) · ⬜
**Objetivo:** que un token vencido o inservible no deje a la sala sin red (DC30).

**Archivos:** `src/client.rs` (el envío de cada pedido, el registro y las escrituras), un módulo del `TokenProvider` y `src/lib.rs`; `tests/common/` (un proveedor de prueba) y `tests/token_tests.rs`. `ARCHITECTURE.md` §11 (*Tokens*).

**Qué se hace:** el `TokenProvider` en `Client::open`; ante `Unauthorized`, la renovación, una por sala a la vez y con su plazo, y el pedido repetido una vez; ante `Forbidden`, el token marcado como inservible; en una escritura, el intento con `Unauthorized` que no queda incierto y el reintento dentro del plazo.

**Tests exigidos.**
- Con un token vencido (emitido con un TTL de 0), el registro, un sync, un ack, un heartbeat, un commit, y un chunk de bajada y uno de subida reciben `Unauthorized`, piden un token y se repiten con el nuevo, que usa también el pedido siguiente.
- Varios pedidos a la vez con el mismo token vencido: una sola renovación. Un proveedor que no responde: al vencer su plazo, los pedidos fallan sin token nuevo; y una renovación cuyo pedido se corta no deja esperando a los demás.
- Una escritura con `Unauthorized` y un token nuevo termina con la decisión de la réplica, con el mismo `MutationId`; sin token nuevo (el proveedor falla, o el nuevo también recibe `Unauthorized`), con `Network`, y con `OutcomeUnknown` si un intento anterior quedó incierto.
- Después de un `Forbidden` (un token de otra sala), el pedido siguiente de la sala pide un token al proveedor antes de mandarse, y con uno bueno sale bien.
- Negativos: `Forbidden` no se repite en el mismo pedido, y la escritura termina con `Validation`; un `Unauthorized` de otra correlación o de otra sala no renueva; un segundo `Unauthorized`, con el token renovado, no pide otro en el mismo pedido.

**Interacciones a revisar:** el token de `open_room`, que reemplaza al renovado (gana el último); el lock de los registros; el plazo de una escritura, dentro del que cuenta la renovación; la baja al fallar cerrado; un pedido en vuelo cuando la sala se descarta (el token es de la sala, no de la época).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K17.

**Entrega:** `feat(client): renew the token of a room on Unauthorized (Fase 4.3, Lote 3)`.

### Lote 4 · El worker: la tarea, el heartbeat y las transferencias (`zemdb-client`) · ⬜
**Objetivo:** que una sala mantenga su lease y haga lo que el servidor le pide sin que la aplicación la atienda (DC28, DC29, INV-K7).

**Archivos:** un módulo del worker; `src/client.rs` (el heartbeat sin transferencias y el estado de la conexión); `src/config.rs` y `src/lib.rs`; `tests/worker_tests.rs`. `ARCHITECTURE.md` §11 (*Worker*).

**Qué se hace:** `start_worker`, `set_freshness` y `stop_worker` (el modo se guarda, y hasta el Lote 5 ninguno sincroniza por el head); la tarea por sala, con el cliente por un `Weak` y un heartbeat por intervalo (el primero al azar) que no espera las transferencias; las transferencias en su tarea; los reintentos con backoff y *full jitter*; lo que lo detiene (DC29), por los errores de sus pasos; `close_room` y `shutdown` lo detienen (el orden completo del cierre, con el último `Ack`, es del Lote 6); y `status`, con el worker y el estado de la conexión.

**Tests exigidos.**
- Un heartbeat por intervalo, el primero dentro del primer intervalo.
- Mientras una bajada larga está en curso (chunks demorados por el transporte de prueba), los heartbeats siguen, uno por intervalo, y el cliente no pasa a `Disconnected` en el servidor después de pasar el `lease_timeout` (sigue `Bootstrapping`, porque está detrás del log; `GET /admin/rooms/{id}`). El designado sube su snapshot, y el atrasado lo baja, sin llamar a ningún paso.
- Una sala abierta sin red se registra con el primer heartbeat del worker que tiene respuesta. Los fallos se reintentan con una espera que crece (los instantes de los pedidos), y el primero que sale bien vuelve al intervalo.
- `FailedClosed` detiene el worker, sin más heartbeats; un `Config` (un snapshot mayor que el tope) y `Network` no lo detienen, y los heartbeats siguen. `stop_worker` y `close_room` lo detienen, y el de un cliente que la aplicación suelta termina.
- Una configuración con un intervalo de heartbeat en cero o de 60 s o más, un backoff que empieza en cero o cuyo máximo es menor que su base, o un `events_retry` en cero, es `Config` al abrir el cliente.
- `status` da el modo y el estado de la conexión, que pasa a falso con un pedido sin respuesta y vuelve a verdadero con el siguiente que la tiene; y responde `RoomClosed` para una sala vacía, sin que eso detenga a su worker.

**Interacciones a revisar:** las escrituras, que hacen sus propios heartbeats; el `heartbeat` público que la aplicación llama a la vez; el lock de las transferencias; la designación y su plazo de 60 s en el servidor; el registro que hace el worker (el lock de los registros); un descarte en curso; el reloj pausado y las tareas de `spawn_blocking` del storage.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K7, K15, K17.

**Entrega:** `feat(client): the sync worker of a room, with its heartbeats and transfers (Fase 4.3, Lote 4)`.

### Lote 5 · Los modos `periodic` y `manual` (`zemdb-client`) · ⬜
**Objetivo:** que en `periodic` la réplica se entere sola de lo que escriben los demás, sin un `Ack` por sync (DC24, DC25).

**Archivos:** el worker; `src/client.rs` (el sync sin `Ack`); `tests/worker_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** en `periodic` (y en `live`, que hasta el Lote 7 se comporta igual), un sync cuando el head del ack supera el local; en `manual`, no; la variante del sync sin `Ack`, cuyo `snapshot_wanted` se confirma con un heartbeat en la tarea de las transferencias; a lo sumo un sync del worker en vuelo, y uno más si se pide mientras tanto.

**Tests exigidos.**
- En `periodic`, lo que escribe otro cliente llega a la réplica en a lo sumo un intervalo, sin llamar a `sync`; en `manual`, no llega hasta que la aplicación sincroniza o escribe.
- Los syncs del worker no mandan `Ack`; el `sync` público sí.
- Varios heartbeats con el head adelante mientras el sync del worker está retenido: un solo sync más.
- Un lote del sync del worker con `snapshot_wanted`: el designado sube sin esperar al heartbeat siguiente.
- El `sync` público de la aplicación a la vez que el del worker: los dos terminan, y la réplica queda en el head.

**Interacciones a revisar:** las escrituras y sus syncs; el `sync` público; la bajada que hace un sync cuando la réplica espera un snapshot anunciado; un cambio de esquema durante el sync del worker.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K7, K15.

**Entrega:** `feat(client): the periodic and manual modes of the worker (Fase 4.3, Lote 5)`.

### Lote 6 · El `Ack` periódico, detener el worker y los endurecimientos (`zemdb-client`) · ⬜
**Objetivo:** que el cursor del servidor avance sin un `Ack` por sync, y que cerrar con el worker no pierda nada (DC25, DC28, DC31).

**Archivos:** el worker; `src/client.rs` (`close_room`, `shutdown`, el registro y el head visto); `tests/worker_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** el `Ack` con cada heartbeat si el head se movió, y el último al detenerse, con su plazo; `close_room` y `shutdown` detienen el worker antes de cerrar la sala, en el orden de DC28; DEF-196 y DEF-192, si se adopta DC31.

**Tests exigidos.**
- Un worker en `periodic` que sincroniza muchas veces manda un `Ack` por intervalo, no uno por sync, con el head de la réplica; uno sin cambios no manda ninguno.
- `close_room` y `shutdown` mandan el último `Ack` si el head se movió (en `shutdown`, el de varias salas a la vez) y, sin red, no tardan más que su plazo. Después, ninguna tarea del worker toca la sala, y el motor en disco la reabre con todo lo aplicado (INV-K11).
- Un `AckConfirmed` con un head menor que el confirmado registra la réplica con su cursor.
- DEF-196: una réplica que espera un snapshot que nadie anuncia (un `Registered` fabricado, detrás del log) deja de esperar con el registro de los 10 heartbeats. DEF-192: después de un `HeartbeatAck` fabricado con un head más allá del servidor, el siguiente, con el head real, registra la réplica, y una escritura posterior termina con la decisión de la réplica.

**Interacciones a revisar:** la poda por cursores (el cursor del servidor avanza a lo sumo un intervalo tarde, INV-Y6); la poda por cuota o por TTL de un log que alcanza el cursor guardado de un cliente en `live`, que se atrasa hasta un intervalo: el heartbeat lo deja `Bootstrapping` en el servidor y abre una demanda de snapshot que la réplica no necesita (un costo, no una ruptura); el `last_ack_seq` de los commits y el `from_seq` de los syncs; la baja al fallar cerrado; apagar el runtime después de `shutdown` (D93); lo que retienen las escrituras (DEF-192 no cambia el `seen` ya anotado).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K5, K11, K2, K7.

**Entrega:** `feat(client): acknowledge the head every interval, stop the worker before closing, and two hardenings of the audit (Fase 4.3, Lote 6)`.

### Lote 7 · El modo `live`: el stream en el worker (`zemdb-client`) · ⬜
**Objetivo:** que una sala en `live` se entere enseguida de lo que escriben los demás (DC24, DC26), con cada evento como un aviso; aplicar el `ops` sin sync es del Lote 8.

**Archivos:** el worker; `tests/live_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** en `live`, el stream: abrirlo y sincronizar; cada evento como un aviso (un sync por `ops`, `head_advanced` y `lagged`, juntando los que llegan durante un sync; un registro por `schema_reloaded`; un heartbeat en la tarea de las transferencias por `snapshot_wanted` y `snapshot_available`); la época del stream; el stream que calla 45 s; reabrirlo con backoff y *full jitter*; `Unauthorized`, con DC30; `EventsDisabled` y la vuelta después de `events_retry`; y `status`, con el stream.

**Tests exigidos.**
- En `live`, lo que escribe otro cliente llega antes del primer heartbeat. Con el stream cortado, se abre otro (con espera), y la réplica se pone al día con el sync de después de abrirlo, también con lo escrito mientras estaba cortado.
- Muchos eventos durante un sync retenido llevan a un solo sync más.
- `lagged` y `head_advanced` (una operación más grande que el tope del evento) llevan a un sync.
- `schema_reloaded`, con una columna nueva: la réplica se registra y toma el esquema.
- `snapshot_wanted`: el designado sube sin esperar el heartbeat. `snapshot_available` con la réplica esperando: lo baja.
- Con el SSE apagado, `live` cae a `periodic` (lo dice `status`), y lo escrito llega con el heartbeat; después de `events_retry`, el worker vuelve a intentar el stream.
- Un stream que el transporte de prueba termina, con un `Unauthorized` a la reapertura, se vuelve a abrir con un token renovado (el vencimiento se fabrica, §2, *Pruebas*).
- Negativos: un stream de una época anterior (la réplica descartada por `InvalidSequence` en un registro, D78) no dispara nada, y se reemplaza; un stream que calla 45 s se reemplaza; un evento que no decodifica lleva a un sync y no detiene nada.

**Interacciones a revisar:** el tope de 2 streams por cliente y sala (el tercero cierra el más viejo, así que reabrir no espera al viejo); el `RateLimited` del tope total; la sala que el servidor detiene por inactividad y vuelve a levantar; el keep-alive; el apagado del servidor; las escrituras y su catch-up, cuyos eventos llegan después (en el Lote 8 se ignoran por el head); el tiempo que el worker deja de leer el stream mientras espera al aplicador, contra el *stall timeout* del servidor (que cierra solo la conexión de ese stream, DC27).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K7, K6, K12, K15, K17.

**Entrega:** `feat(client): the live mode, with the event stream of the room (Fase 4.3, Lote 7)`.

### Lote 8 · El evento `ops` (`zemdb-client`) · ⬜
**Objetivo:** que un evento `ops` se aplique sin un `/sync`, y solo si es de la encarnación y de la época de la réplica y sigue a su head (DC26, INV-K1).

**Archivos:** el worker y `src/client.rs` (aplicar el evento por el aplicador); `tests/live_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** la validación del `ops` de DC26 y su aplicación por el aplicador; con un hueco, un sync; si no pasa el head, se ignora; con otra encarnación, un registro; con un cambio de esquema o una operación que no valida, un registro y un sync.

**Tests exigidos.**
- Lo que escriben varios clientes a la vez llega por eventos, sin ningún `/sync` (el transporte de prueba los cuenta), y la réplica queda igual a la de un sync, con las mismas decisiones (INV-Y2, INV-K4); las escrituras que esperaban esas secuencias terminan con su decisión.
- El evento del commit propio, que ya aplicó su catch-up, se ignora.
- Un evento perdido (el transporte de prueba lo saca): el siguiente tiene un hueco, y un sync lo cubre.
- La sala borrada y recreada con el mismo id, con el stream abierto con el token viejo y la respuesta del sync de después de abrirlo retenida, para que sea el evento el que lleva al registro: los eventos de la sala nueva no se aplican, aunque sigan al cursor; la réplica se registra, recibe `RoomReset` y descarta.
- Un evento con una versión posterior del esquema: no se aplica; la réplica se registra y sincroniza, y queda con las operaciones del evento, decididas con el esquema nuevo.
- Negativos (fabricados): un evento de otra sala, sin operaciones, con operaciones que no son contiguas o que no terminan en su head, con una operación que no valida (la réplica se registra y sincroniza, y nunca falla cerrada por el evento), o de un stream de otra época.

**Interacciones a revisar:** el aplicador, lo retenido para las escrituras y el head visto del evento (DC21); un sync a la vez que aplica las mismas secuencias; el descarte y la época; el cambio de esquema (INV-E3: el evento lleva el esquema con que se validaron sus operaciones); el checklist de determinismo de la Fase 3.11.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K1, K6, K2, K7, K12, K17.

**Entrega:** `feat(client): apply the operations of the ops event without a sync (Fase 4.3, Lote 8)`.

### Lote 9 · El arnés de simulación con el worker y el SSE (`zemdb-client`, tests) · ⬜
**Objetivo:** las invariantes del cliente con el worker en marcha, en ejecuciones al azar.

**Qué se hace:** el transporte de la simulación (`Link`, en `tests/simulation_tests.rs`) implementa `events`, y el arnés de 4.2 (§5.3 del plan de la fase) suma clientes con worker, en un modo al azar que cambia en el medio; el SSE por el transporte que inyecta fallas (cortar el stream, perder eventos, demorarlos, mandar `lagged`, callar); tokens que vencen, fabricados como dice §2 (*Pruebas*): el proveedor de prueba da a veces tokens ya vencidos, y el transporte termina streams y responde `Unauthorized`; y el SSE apagado en algunos reinicios del servidor. El reloj pausado hace correr los intervalos.

**Tests exigidos.**
- En cada paso, cada réplica es igual al oráculo en su head (INV-K1), y cada escritura termina con una de las respuestas de INV-K2.
- Al cerrar, con la red curada, los clientes con worker en `periodic` o `live` llegan al head sin ningún `sync` del test, en a lo sumo una cantidad fija de intervalos (INV-K7); los de `manual`, con un `sync`.
- Ningún worker se detiene salvo por `FailedClosed` o `RoomClosed` (INV-K17); los descartes, por la época de `status`, solo en los casos de INV-K3, como en 4.2; y lo retenido no pasa de las cotas de INV-K15.
- Una versión corta en la suite y una larga con `#[ignore]`.

**Interacciones a revisar:** los reinicios del servidor con streams abiertos (el apagado los corta); las tareas del worker al reiniciar el servidor (el arnés las espera, como a los pedidos); el oráculo por encarnación.

**Documentos:** `INVARIANTES.md`: K1, K2, K3, K4, K7, K15, K17 (*Automatizable*).

**Entrega:** `test(client): the simulation harness with the worker and the event stream (Fase 4.3, Lote 9)`.

### Lote 10 · Robustez: fuzzing de los eventos (`zemdb-client`, tests) · ⬜
**Objetivo:** INV-K12 e INV-K17 con eventos que no son del servidor.

**Qué se hace:** en el arnés, la red reemplaza bytes del stream: bytes al azar, eventos dañados, líneas sin fin, y un `ops` con un mensaje al azar de los generadores de `crates/core/tests/common`, de otra sala o de otra encarnación. Además, el fuzzing del parser a través del transporte HTTP, contra un servidor del test que escribe esos bytes. No fabrica un evento que el cliente no distingue de uno del servidor (operaciones que siguen al cursor, con la sala, la encarnación y el esquema de la réplica): queda fuera de alcance (`INVARIANTES.md` §2).

**Tests exigidos.** Ningún pánico, tampoco en una tarea; cada réplica es igual al oráculo en su head; ningún worker se detiene salvo por DC29; al final, con la red curada, todos llegan al head.

**Documentos:** `INVARIANTES.md`: K12, K17.

**Entrega:** `test(client): fuzzing of the event stream (Fase 4.3, Lote 10)`.

### Lote 11 · Revisión de cierre y cierre · ⬜
- La revisión de cierre (§3). Si no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.3, cada uno con su revisión de delta.
- `INVARIANTES.md`: el estado de cada invariante revisada. El plan de la fase: 4.3 cerrada. `ROADMAP.md`: "Lo que sigue".

---

## 3. Revisión de cierre

Al cerrar 4.3, sobre el commit del Lote 10, con dos revisores en paralelo, de contexto fresco y solo lectura (`docs/METODOLOGIA.md` §6), cada uno con sus corridas en una copia del repo:

| Revisor | Invariantes | Foco |
|---|---|---|
| Worker y réplica | K1, K4, K5, K6, K7, K11 y K17 | El worker y sus modos, el `Ack` periódico, el evento `ops`, detener el worker |
| Transporte y robustez | K2, K3, K12 y K15 | El transporte HTTP, el parser del SSE, la renovación del token, la memoria, y que el worker no descarte una réplica por su cuenta |

Cada uno recibe `INVARIANTES.md`, `ARCHITECTURE.md`, este plan, el de la fase (decisiones DC1 a DC31) y los arneses, y responde con veredictos sobre lo que tocó 4.3 y los mecanismos con los que interactúa, con las tres condiciones de `INVARIANTES.md` §1.4. INV-K17 se revisa entera; lo de 4.4 a 4.6 sigue 🆕.

---

## 4. Seguimiento

**Avance:** plan escrito el 4 de Noviembre de 2026. Lote 0 (el contrato) cerrado el 6 de Noviembre, con la revisión cruzada en verde en su quinta ronda: las precisiones de DC18 y de DC24 a DC26 y DC27 a DC31, como *Propuestas*, y `INVARIANTES.md` en la versión 16.
