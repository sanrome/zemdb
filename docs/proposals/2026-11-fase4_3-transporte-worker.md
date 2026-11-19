# Transporte nativo y `SyncWorker` — Plan de la sub-fase 4.3

**Fecha:** 4 de Noviembre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lote 0 (el contrato) cerrado el 6 de Noviembre de 2026, con la revisión cruzada en verde en su quinta ronda. El dueño del proyecto adoptó el mismo día las precisiones de DC18 y de DC24 a DC26 y las decisiones DC27 a DC31 (DC27, con la condición de S4), con dos ajustes: DC26 dice explícitamente que, en el camino normal, un `ops` se aplica sin ningún `/sync`, y la verificación del `ClientId` de un token renovado pasa de 4.4 al Lote 3 (DC30). Lote 1 (el transporte HTTP nativo) cerrado el 7 de Noviembre de 2026, con S4 respondido y la revisión de delta en verde en su cuarta ronda. Lote 2 (el stream de eventos: el transporte y el parser) cerrado el 9 de Noviembre de 2026, con la revisión de delta en verde en su primera ronda. Lote 3 (la renovación del token) cerrado el 10 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda. Lote 4 (el worker: la tarea, el heartbeat y las transferencias) cerrado el 12 de Noviembre de 2026. Lote 5 (los modos `periodic` y `manual`) cerrado el 12 de Noviembre de 2026. Lote 6 (el `Ack` periódico, detener el worker y los endurecimientos) cerrado el 13 de Noviembre de 2026. Lote 7 (el modo `live`: el stream en el worker) cerrado el 16 de Noviembre de 2026, con la revisión de delta en verde en su primera ronda. Lote 8 (el evento `ops`) cerrado el 16 de Noviembre de 2026, con la revisión de delta en verde en su primera ronda. Lote 9 (el arnés de simulación con el worker y el SSE) cerrado el 17 de Noviembre de 2026, con la revisión de delta en verde en su segunda ronda. Lote 10 (robustez: el fuzzing de los eventos) cerrado el 18 de Noviembre de 2026, con la revisión de delta en verde en su primera ronda. Lote 11 (la revisión de cierre) cerrado el 18 de Noviembre de 2026: la revisión no salió en verde, y la siguen los Lotes 11b y 11c. Lo que sigue es el Lote 11b.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.3. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).
**Parte de:** el SSE del servidor, del Lote 5 de la [Fase 3.12](2026-11-fase3_12-rendimiento-servidor.md) (`2c0a908`): el evento `ops` con la encarnación de la sala, el stream que termina cuando vence su token y `EventsDisabled` (`ARCHITECTURE.md` §7.2).

---

## 0. Resumen

- **Qué.** El transporte HTTP nativo del cliente, sobre `reqwest`, con el stream de eventos; el `SyncWorker`, que programa por sala los pasos del núcleo de 4.2 (heartbeat, sync, `Ack`, bajada y subida de snapshots) según el modo de frescura (DC24), con el `Ack` periódico (DC25) y el evento `ops` (DC26); la renovación del token ante `Unauthorized`; y el contrato de lo que puede detener a una sala (INV-K17, nueva).
- **Dónde.** Solo en `zemdb-client` y sus tests, que está en el alcance auditado desde la auditoría que cerró 4.2 (`INVARIANTES.md` §1.4): cada lote, con su revisión de delta. No toca el servidor, `zemdb-core` ni `zemdb-storage`: lo que el SDK necesita del servidor (el SSE con las operaciones y el SSE apagado) está desde el Lote 5 de la Fase 3.12. El cambio de §2 de `INVARIANTES.md` aplica a los eventos SSE el criterio que ya vale para las respuestas (la precisión de DC11 que adoptó el dueño del proyecto al cerrar 4.2): no cambia el modelo de amenaza, así que no pide una auditoría amplia (§1.4).
- **Qué no.** La fachada de Rust, `watch` y el modo `live` atado a los `watch`, y el `Offline` inmediato (4.4); el transporte del navegador y `EventSource` (4.5); la encarnación en los commits, los syncs y los acks (backlog para después de la v0.1, `ROADMAP.md` §3).
- **Decisiones:** las precisiones de DC18 y de DC24 a DC26 de la fase, y DC27 a DC31, nuevas (§1), adoptadas por el dueño del proyecto el 6 de Noviembre de 2026.
- **Cómo cierra:** con una revisión de cierre (`docs/METODOLOGIA.md` §6), con dos revisores (§3).

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC6 (la fila de `Unauthorized` de la tabla de §A.4), DC11, DC18 (los pasos que programa el worker), DC21, DC22 y DC24 a DC26 (§3 del plan de la fase). Esta sub-fase precisa DC18 y DC24 a DC26, y agrega DC27 a DC31. El dueño del proyecto adoptó todo el 6 de Noviembre de 2026, con dos ajustes: la frase de DC26 sobre el camino normal sin `/sync`, y la verificación del `ClientId` de un token renovado, que pasa de 4.4 a DC30 (Lote 3).

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC27** | El transporte nativo: `reqwest`, plazos que no cortan un pedido que avanza, el tope de lo que lee, el stream por su propia conexión y su parser en el núcleo | 1, 2 | Adoptada (6 de Noviembre de 2026); S4, respondido en el Lote 1 (macOS y Windows, con su CI) |
| **DC28** | El `SyncWorker`: una tarea por sala, a pedido, cuyo heartbeat no espera las transferencias | 4 a 6 | Adoptada (6 de Noviembre de 2026) |
| **DC29** | Qué detiene a una sala: solo `FailedClosed` y `RoomClosed` | 3, 4, 7 y 8 | Adoptada (6 de Noviembre de 2026) |
| **DC30** | La renovación del token, con un `TokenProvider` de la aplicación, y la verificación del `ClientId` del token renovado | 3, 7 | Adoptada (6 de Noviembre de 2026) |
| **DC31** | Los endurecimientos opcionales de la auditoría de 4.2 (DEF-192 y DEF-196) | 6 | Adoptada (6 de Noviembre de 2026) |

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
- **En el camino normal, un `ops` se aplica sin ningún `/sync`:** sus operaciones entran a la réplica desde el evento. El worker sincroniza solo por lo que un evento no cubre: una vez al abrir cada stream (lo que pasó mientras no había stream), y ante un evento que no se puede aplicar (un hueco, `lagged`, `head_advanced`, uno que no decodifica, un cambio de esquema o una operación que no valida). El heartbeat también compara heads, pero en el camino normal el evento llega antes. En el Lote 7, antes de que el Lote 8 aplique el `ops`, cada evento todavía se trata como un aviso que lleva a un sync: es un estado intermedio entre dos lotes, para revisarlos por separado.
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
- **S4, respuesta** (6 de Noviembre de 2026, Linux x86_64 con 4 núcleos; el código del spike no se mergea):
  - *Compila,* `reqwest` 0.13.5 con `rustls` sobre `aws-lc-rs` (`aws-lc-sys` 0.47, con `cc`, sin `cmake`) y `http2`, con Rust 1.85, una vez que el lock elige versiones que compilan con 1.85: la resolución del workspace pasa a `resolver = "3"` (elige las versiones que compilan con la `rust-version` declarada), el lock baja `idna_adapter` a 1.2.1 y los `icu_*` a 2.1 (la 1.2.2 y las 2.3 piden Rust 1.86 y 1.88; ya estaban en el lock por el `reqwest` 0.12 de los tests del servidor) y sube `yoke` y `zerovec` (con sus *derive*) a 0.8.4 y 0.11.9 (las 0.8.3 y 0.11.8 no declaran versión y no compilan con 1.85). wasm32 no cambia: `reqwest` es una dependencia solo fuera de wasm32. En frío y en debug, su árbol suma unos 20 s con 4 núcleos (`aws-lc-sys`, 14 s, en paralelo con lo demás). macOS y Windows, en la CI del lote.
  - *El plazo de inactividad.* `read_timeout` se reinicia con cada lectura del cuerpo de la respuesta, pero hasta los headers es un solo reloj desde que sale el pedido: corta una subida lenta que avanza (medido: un cuerpo de 8 MiB que el servidor lee a unos 640 KiB/s, cortado al segundo con `read_timeout` de 1 s). Así que el transporte no usa `read_timeout` ni `timeout`: lleva su propio reloj, que avanza con cada pedazo del cuerpo que toma la capa HTTP (de 16 KiB) y con cada pedazo de la respuesta que llega. Lo que la capa HTTP ya tomó queda en sus búferes y en los del sistema, que el cliente no ve (`reqwest` no expone el socket; `TCP_NOTSENT_LOWAT` o un `SO_SNDBUF` fijo achicarían lo que no se ve, pero piden un conector propio): TCP agranda el de envío con la ventana del camino, chico en un enlace lento y de unos MiB después de uno rápido (en Linux, hasta 4 MiB, que vuelve a aceptar datos cuando salió un tercio; en loopback, donde el MSS es de 64 KiB, el último pedazo de un cuerpo de 8 MiB leído a unos 640 KiB/s se tomó 9 s antes de los headers). La espera de la respuesta cuenta desde el último pedazo tomado, así que lo que quedó en esos búferes tiene que salir dentro del plazo: con HTTP/1.1, el socket y hasta 256 KiB de hyper (después de un camino rápido, con 4 MiB, un cuerpo que sale a menos de unos 72 KiB/s puede cortarse avanzando; en el medio del cuerpo, con el socket que suelta de a un tercio, a menos de unos 22 KiB/s); con HTTP/2, a lo sumo la ventana que da el par (1 MiB el servidor de ZemDB, unos 17 KiB/s; medido: los headers llegaron 1,5 s después del último pedazo de una subida de 4 MiB a 512 KiB/s, contra 6 s con HTTP/1.1), o, detrás de un proxy con ventanas grandes, el socket y hasta 1 MiB por stream, con lo de los demás pedidos de la conexión. Queda como límite aceptado en INV-K7. Además, `reqwest` pone por defecto *keep-alive* de TCP (a los 15 s, con 3 sondas donde el sistema lo deja fijar, no en Windows) y, en Linux, `TCP_USER_TIMEOUT` de 30 s (cierra una conexión cuyos datos enviados no se confirman en ese tiempo).
  - *HTTP/2 con su tope de streams* espera en la misma conexión, sin abrir otra (medido: 6 pedidos de 1 s con un tope de 2 tardan 3 s, en una conexión). Un pedido que espera un stream libre no avanza, y el reloj lo cuenta: con 32 pedidos en vuelo que no terminan en 60 s, el siguiente se corta (y se reintenta). Confirma el stream por su propia conexión HTTP/1.1.
  - *Conclusión:* la condición se cumple con `aws-lc-rs`, y el plazo de inactividad lo arma el transporte. Además de lo de arriba, el transporte no sigue redirecciones (un `POST` redirigido no llegaría como frame), rechaza una URL con usuario (que `reqwest` mandaría como un segundo `Authorization`), aplica el *prior knowledge* solo con `http://`, pasa a un cliente HTTP nuevo, con conexiones nuevas, después de un pedido que corta por no avanzar (con HTTP/2, un pedido cortado no cierra su conexión, y una que deja de responder no se soltaría mientras lleguen pedidos; el cliente nuevo se arma de a uno, en el pool bloqueante, porque en Linux lee los certificados de la plataforma, en una tarea propia que no cancela quien deja de esperar, y no se arma después de un pedido que quien llama suelta antes), y usa los certificados de la plataforma y el proxy de las variables de entorno (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`; sin la feature `system-proxy` de `reqwest`, no el configurado en macOS o Windows). Lo de la URL, el *prior knowledge* y el cliente nuevo salió de la revisión de delta del lote; los pings de HTTP/2 se descartaron, porque un ping sale detrás de lo que hay en vuelo y un `WINDOW_UPDATE` no cuenta para hyper como respuesta, así que cortaban una subida lenta que avanza, con la conexión entera.
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
- **El `ClientId` del token renovado** (DC15; pasó de 4.4 a este lote al adoptarse DC30): el núcleo lee el `ClientId` de cada token que le da el proveedor, del formato con que lo emite `generate_client_token` (`v1.<base64url(client_id)>.<base64url(room_id)>.<vencimiento>.<firma>`), sin verificar la firma, que es del servidor. Si no es el suyo, o el token no tiene ese formato, cuenta como sin token nuevo, y no se manda nada con él. Un test con `generate_client_token` (`zemdb-server` es dev-dependency) detecta un cambio del formato. El servidor, además, rechaza con `Forbidden` todo pedido cuyo mensaje nombra otro cliente que su token, así que un token de otro `ClientId` nunca secuencia una operación con otra clave de dedup (INV-Y4).
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
| 12 | Plan de la fase, §4.4 (DC15) | La verificación del `ClientId` de cada token renovado | DC30: pasó de 4.4 a 4.3 al adoptarse | 3 |

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC18, DC24 a DC31 | Nueva: K17. Ajustes: K1, K2, K5, K6, K7, K11, K12, K15 y §2 | ✅ |
| 1 | El transporte HTTP nativo | client | DC27 | K12, K7 | ✅ |
| 2 | El stream de eventos: el transporte y el parser | client | DC27 | K12, K15, K7 | ✅ |
| 3 | La renovación del token | client | DC30, DC29 | K2, K17 | ✅ |
| 4 | El worker: la tarea, el heartbeat y las transferencias | client | DC28, DC29 | K7, K15, K17 | ✅ |
| 5 | Los modos `periodic` y `manual` | client | DC24, DC25, DC28 | K7, K15 | ✅ |
| 6 | El `Ack` periódico, detener el worker y los endurecimientos | client | DC25, DC28, DC31 | K5, K11, K2, K7 | ✅ |
| 7 | El modo `live`: el stream en el worker | client | DC24, DC26, DC29, DC30 | K7, K6, K12, K15, K17 | ✅ |
| 8 | El evento `ops` | client | DC26, DC29 | K1, K6, K2, K7, K12, K17 | ✅ |
| 9 | El arnés de simulación con el worker y el SSE | client (tests) | §5.3 del plan de la fase | K1, K2, K3, K4, K7, K15, K17 | ✅ |
| 10 | Robustez: fuzzing de los eventos | client (tests) | — | K12, K17 | ✅ |
| 11 | Revisión de cierre y cierre | Documentos | Todas | Las de §3.11 que toca 4.3 | ✅ (su revisión de cierre no salió en verde: la siguen el 11b y el 11c) |
| 11b | El head de un `ops`, visto solo con sus operaciones validadas | client | DC26 | K2 | ⬜ |
| 11c | Ninguna configuración que acepta el cliente lleva a un pánico, y el cierre de 4.3 | client | DC28 | K12, K17, K7 | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirman antes del Lote 1.
- El 1 y el 2 (el transporte) y el 3 (el token) no dependen entre sí. Del 4 al 8, en orden: cada uno usa lo anterior, y el 7 usa además el `events` del 2 y la renovación del 3.
- El 9 y el 10 son pruebas, sin producción (salvo arreglos de lo que encuentren, que van como desvíos en su nota o como lotes nuevos); el 9 antes que el 10, que reusa su transporte.
- El 11 cierra con la revisión de cierre (§3); sus rupturas se arreglan en lotes nuevos de 4.3.
- El 11b y el 11c arreglan lo que rompió la revisión de cierre, en ese orden; el 11c cierra 4.3.

**Tamaño.** Unas 150 líneas de producción el Lote 1, 200 el 2, 170 el 3, 200 el 4, 150 el 5, 150 el 6, 250 el 7, 150 el 8, unas 10 el 11b y unas 20 el 11c.

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
- **Las Propuestas:** el dueño del proyecto las adoptó el 6 de Noviembre de 2026, con dos ajustes: DC26 dice explícitamente que, en el camino normal, un `ops` se aplica sin `/sync`, y la verificación del `ClientId` de un token renovado pasa de 4.4 al Lote 3 (DC30).

### Lote 1 · El transporte HTTP nativo (`zemdb-client`) · ✅ Hecho
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

**Hecho** (7 de Noviembre de 2026).
- **Qué entró.**
  - `HttpTransport` y `HttpTransportConfig`, en `src/http_transport.rs`, detrás de la feature `native` (por defecto): `reqwest` 0.13.5, con `rustls` y `http2`, y `http-body`, solo fuera de wasm32.
  - `send` hace el `POST` del frame con `Authorization: Bearer` y `Content-Type: application/octet-stream`, y devuelve el cuerpo sea cual sea el estado HTTP.
    - No sigue redirecciones.
    - Lee hasta `MAX_FRAME_SIZE`: si el `Content-Length` anuncia más, falla antes de leer; sin `Content-Length`, falla en cuanto lo pasa, sin leer el resto. Lo que anuncia el `Content-Length` lo reserva con `try_reserve_exact`.
  - Los plazos:
    - conectar, 10 s;
    - inactividad, 60 s, con un reloj propio que avanza con cada pedazo de 16 KiB del cuerpo que toma la capa HTTP y con cada pedazo de la respuesta que llega;
    - sin plazo total.
  - El pool retiene una conexión ociosa 5 s, con un máximo de 300 s.
  - Después de un pedido que el transporte corta por no avanzar, pasa a un cliente HTTP nuevo, con conexiones nuevas, para los pedidos que siguen: con HTTP/2, el corte no cierra la conexión, y una que deja de responder no se soltaría mientras lleguen pedidos. No pasa después de un pedido que quien llama suelta antes.
    - El cliente nuevo se arma en el pool bloqueante (en Linux, lee los certificados de la plataforma), fuera del lock y de a uno, con un guard, en una tarea propia que no cancela quien deja de esperar.
    - Una generación evita reemplazar un cliente que ya es nuevo.
    - Si el armado falla, queda el actual hasta el próximo corte.
  - El prior knowledge de HTTP/2 se aplica solo con `http://`.
  - Es `Config`:
    - una URL que no es `http(s)://` o que tiene usuario, query o fragmento;
    - un plazo en cero;
    - un pool de más de 300 s (hyper-util entra en pánico en segundo plano con `Duration::MAX`);
    - un cliente HTTP que no arranca, también con `http://`, con su causa (en Linux, por ejemplo, sin certificados de la plataforma).
  - Unas 260 líneas de producción, sin comentarios ni líneas vacías (se estimaron 150).
- **Tests.**
  - `tests/http_transport_tests.rs`: 21 tests contra el servidor en un socket local, detrás de un frente TCP que reenvía, guarda los primeros bytes de cada conexión y puede callarse, y contra servidores mínimos del test.
    - Un cliente del núcleo abre la sala, escribe, sincroniza la escritura de otro y manda un heartbeat, por HTTP/1.1 y por HTTP/2, con la versión confirmada por el frente.
    - `Unauthorized` y `Forbidden` llegan como frames, por las dos versiones.
    - Una respuesta lenta y una subida lenta terminan aunque duran más de 3 y de 2 veces el plazo.
    - Una respuesta más larga que un frame, por su `Content-Length` o sin él, es un error sin leerla entera; una de exactamente un frame se lee entera.
    - Un servidor que calla antes de los headers, en medio de la respuesta o al recibir el cuerpo corta el pedido en el plazo.
    - Un puerto cerrado es un error.
    - Una escritura con el servidor callado queda `OutcomeUnknown`.
    - Una conexión ociosa más que el pool no se reusa, y dos pedidos seguidos comparten una.
    - Después de un pedido que corta el transporte en una conexión HTTP/2 que deja de responder, el pedido siguiente va por otra conexión, aunque los pedidos sigan llegando. Lo hace dos veces seguidas, con un frente que congela las conexiones abiertas.
    - Después de una respuesta HTTP/2 que se calla a la mitad, el pedido siguiente va por otra conexión: con un servidor axum por h2c.
    - Una subida lenta por HTTP/2 no se corta: contra el servidor, con un frente que lleva los primeros 3 MiB a 1 MiB/s, con un horario fijo para que no lo frene un temporizador grueso, y un plazo de 750 ms. También pasa con un temporizador que imita el de Windows.
    - Con `https://`, el handshake de TLS ofrece h2 y http/1.1, también con prior knowledge.
    - Una configuración que no puede funcionar es `Config`.
    - Una URL con camino lleva los endpoints debajo, con el token y el `Content-Type`.
    - Una redirección no se sigue.
  - `src/tests/http_transport.rs`: 4 unitarios con el reloj pausado.
    - Un pedido que avanza no se corta nunca.
    - Uno que deja de avanzar se corta un plazo después de su último avance.
    - Uno que nunca avanza se corta un plazo después de empezar.
    - El cuerpo se entrega en pedazos, y cada uno cuenta como avance, con el largo exacto.
- **Mutaciones inversas: las 29 las mató algún test.**
  - Los dos topes de la respuesta, y sus bordes `>=`.
  - El reloj del pedido.
  - El avance por pedazo del cuerpo, en el test de integración y en el unitario.
  - El plazo por pedazo de la respuesta.
  - Agregar un plazo total.
  - El pool.
  - El prior knowledge, y aplicarlo también con `https`.
  - No seguir redirecciones.
  - El esquema, el usuario, y la query o el fragmento de la URL.
  - El cero de cada plazo.
  - La barra final de la URL.
  - El `Content-Type` y el bearer.
  - El largo exacto del cuerpo.
  - El reloj medido desde el último avance.
  - Pasar a un cliente nuevo después de un corte, en el pedido o en la respuesta.
  - Soltar la marca de que se está armando un cliente nuevo.
  - Volver a poner los pings de HTTP/2.
  - El tope del pool.
  - La feature `rustls`.

  Quedan sin test, con su argumento:
  - `connect_timeout`: haría falta una dirección que descarte los SYN, y eso no es portable en la CI. Igual cubre la conexión el reloj de inactividad.
  - `try_reserve_exact`: haría falta quedarse sin memoria.
  - El proxy del entorno: las variables de entorno son globales al proceso, y los tests corren en paralelo.
  - La segunda comprobación de la generación, después del armado: con un solo armado a la vez, solo cubre una carrera angosta entre la primera comprobación y la marca, en la que otro corte termina su reemplazo en el medio. Sin ella, se reemplazaría un cliente recién armado, y eso cuesta un armado y nada más.
- **Interacciones revisadas.**
  - El `header_read_timeout_secs` del servidor y el pool: el pool retiene la conexión menos tiempo, 5 s contra 10 s. Hay test; el test cuenta solo las conexiones que llevan bytes, porque hyper-util abre una conexión en paralelo al `checkout` y a veces la cierra sin usarla.
  - El *stall timeout* y el plazo del cuerpo del servidor, contra los del cliente: el plazo de inactividad del cliente es igual al stall del servidor, y un cuerpo más lento que el mínimo del servidor lo corta el servidor con su 408, que llega como frame.
  - La cola de los búferes contra la espera de los headers: queda documentada como límite aceptado de INV-K7.
  - El tope de conexiones del servidor.
    - Con HTTP/1.1, una conexión por pedido concurrente, que el pool suelta a los 5 s.
    - Con HTTP/2, una por host mientras se use dentro del plazo del pool. Un pedido espera un stream libre (S4), y una conexión que deja de responder se deja para el pedido siguiente (hay test).
  - El presupuesto de respuestas: un 503 llega como frame, por el mismo camino que el 401 y el 403 del test de errores.
  - `TransportError` en cada paso del núcleo: en el núcleo solo se prueba el caso del servidor callado (la escritura incierta). Para los demás alcanza con que el núcleo trata igual todo `TransportError`, lo que ya se probó en 4.2 con el transporte de prueba.
  - Con HTTP/2, los pedidos de todas las salas comparten la conexión. Un heartbeat espera en el cable detrás de los bytes de una transferencia, y un corte de la conexión deja inciertos los pedidos de todas. Va a las interacciones del Lote 4, contra el *lease*.
  - El `write_timeout` de 30 s del núcleo es menor que el plazo de 60 s: en un commit corta primero el plazo de la escritura, que suelta el pedido, y eso no hace pasar a un cliente nuevo. Los demás pasos no tienen plazo en el núcleo, así que los acota el del transporte, y el heartbeat del worker es el que deja atrás una conexión HTTP/2 trabada. Va a las interacciones del Lote 4, con un test.
  - Con HTTP/1.1, el corte de un pedido cierra su conexión (hyper); el cliente nuevo cuesta solo las conexiones ociosas del pool.
  - El armado de un cliente lee los certificados de la plataforma (unos 16 ms en Linux): va al pool bloqueante, uno a la vez y a lo sumo uno por plazo.
  - Un corte que llega mientras se arma el cliente nuevo no lo espera, así que el pedido que le sigue puede salir todavía por el viejo. Lo reintentan el núcleo y el worker, con backoff, y en el peor caso espera otro plazo.
  - Con `rustls-platform-verifier`, en Linux el armado del cliente lee los certificados (unos 10 a 16 ms): `new` lo hace una vez, en el hilo que lo llama. En macOS y Windows, en cambio, el sistema verifica el certificado en cada handshake, con su revisión de revocación, que puede ir a la red dentro del `poll` del pedido. Es comportamiento de la dependencia, y queda para la revisión de cierre de 4.3.
  - El cliente HTTP aparte del stream del Lote 2 no pasa a un cliente nuevo con los cortes de los pedidos. Está en las interacciones del Lote 2.
  - El `deregister` se manda con el lock de registro tomado: es preexistente, y ahora un transporte trabado lo demora a lo sumo un plazo (antes, sin límite).
  - `reqwest` pone por defecto keep-alive de TCP (a los 15 s, con 3 sondas salvo en Windows) y, en Linux, `TCP_USER_TIMEOUT` de 30 s. Está nombrado en ARCHITECTURE.
  - Los tests van a `127.0.0.1`. En una máquina con `HTTP_PROXY` y sin `NO_PROXY` para loopback, pasarían por el proxy.
  - La compilación a wasm32: el build y el clippy pasan, y `reqwest` no entra.
  - El MSRV: `cargo +1.85 build --workspace --lib --bins` y `cargo +1.88 test --workspace --no-run` pasan con el lock nuevo.
- **Revisión de delta, primera ronda: no verde.** Dos hallazgos Medios y cinco Bajos, todos arreglados en el lote:
  - Medio: el test del pool fallaba de vez en cuando, por la carrera de hyper-util de arriba.
  - Medio: el límite aceptado nuevo de INV-K7 prometía de más. Daba 22 KiB/s, el tercio del búfer en el medio del cuerpo, cuando lo que manda es la cola, unos 68 KiB/s con 4 MiB. Se corrigió en INVARIANTES, ARCHITECTURE y S4.
  - Bajos: el prior knowledge con `https`; la URL con usuario; tests de los bordes de los topes y de TLS; la conexión HTTP/2 que deja de responder; los márgenes del test de la respuesta lenta.
  - Notas: `try_reserve_exact`; el proxy del sistema en el texto de S4; el `write_timeout` y el `deregister` en las interacciones.
- **Revisión de delta, segunda ronda: no verde.** Confirmó resueltos los hallazgos de la primera y encontró un Medio que traían los pings: en hyper, un ping sale detrás de lo que hay en vuelo, y un `WINDOW_UPDATE` no cuenta como respuesta, así que una subida lenta que avanza perdía la conexión entera, con los pedidos de todas las salas.
  - Arreglo: sin pings, y con un cliente nuevo después de cada corte, que es su opción (a).
  - Bajos: `Duration::MAX` en el pool hace entrar en pánico a hyper-util, y ahora el pool tiene un tope de 300 s. Las cifras de la cola ahora cuentan los búferes de hyper: unos 85 KiB/s.
  - Notas: el proxy con `http://`, el keep-alive de TCP en todos los sistemas, el texto de la cabecera de `INVARIANTES.md` (hoy, la versión 19), la conexión compartida de HTTP/2 (que pasó al Lote 4), el ClientHello leído entero, y macOS y Windows en la fila de DC27.
- **Revisión de delta, tercera ronda: no verde.** Confirmó resuelta la segunda, sin problemas de concurrencia en el cliente nuevo.
  - Medio: el test de la subida lenta por HTTP/2 dependía del temporizador. Con el de Windows, de 15,6 ms por tic, el frente se frenaba y el test fallaba (reproducido imitándolo). Ahora el frente sigue un horario fijo, y el plazo es de 750 ms.
  - Bajos:
    - el cliente nuevo no llega cuando quien llama suelta el pedido: precisado en los documentos, y llevado al Lote 4;
    - el armado del cliente bajo el lock, con E/S bloqueante: ahora va al pool bloqueante;
    - el cliente nuevo después de un corte en la respuesta no tenía test: test nuevo.
  - Notas:
    - con HTTP/1.1, el corte cierra la conexión;
    - con HTTP/2, la cola la acota la ventana del par;
    - la conexión HTTP/2 se comparte solo dentro del plazo del pool;
    - qué pasa si el armado falla;
    - el keep-alive en Windows;
    - el cliente del stream, en el Lote 2.

    Todo se corrigió en ARCHITECTURE, K7, S4 y los Lotes 2 y 4.
- **Revisión de delta, cuarta ronda: verde.** Confirmó resuelta la tercera. Sus Bajos y notas se arreglaron en el lote:
  - Bajo: si quien llama soltaba el pedido mientras se armaba el cliente nuevo, el reemplazo se perdía (reproducido con 3 y 5 ms). Ahora el armado corre en una tarea propia; lo verifiqué a mano con 12 casos, soltando el pedido 2, 3 y 5 ms después del corte. Un corte durante el armado que no lo espera queda con su argumento, en las interacciones.
  - Bajo: sin certificados de la plataforma, `new` fallaba también con `http://`, con un mensaje sin la causa. Ahora el `Config` lleva la cadena de causas, y el caso está documentado.
  - Notas: que el armado lee certificados solo en Linux, y en macOS y Windows lo que se verifica en el handshake (en las interacciones); la redacción de K7; "over both versions" en ARCHITECTURE.
- **Desvíos.**
  - S4 cambió cómo se arma el plazo de inactividad: un reloj propio, en lugar de `read_timeout`. Lo que la capa HTTP ya tomó y queda en los búferes no se ve, y queda como límite aceptado en INV-K7, con la respuesta de S4 en DC27.
  - `resolver = "3"` en el workspace, y versiones del lock que compilan con 1.85: `idna_adapter` 1.2.1, `icu_*` 2.1, `yoke` 0.8.4 y `zerovec` 0.11.9.
  - En el lock quedan dos versiones de `reqwest`: la 0.12, de los tests del servidor, que no se tocan en este lote, y la 0.13, del cliente.
  - El test de la subida lenta lee despacio solo mientras al cliente le queda más cuerpo que lo que guardan los búferes de loopback.
  - No estaban en el plan y salieron de las revisiones: el cliente nuevo después de un corte (los pings de HTTP/2 se probaron y se descartaron), el rechazo de una URL con usuario, el prior knowledge solo con `http://` y el tope del pool.
  - El chequeo de host de la URL se sacó: con `http` y `https`, el parser siempre exige un host.

### Lote 2 · El stream de eventos: el transporte y el parser (`zemdb-client`) · ✅ Hecho
**Objetivo:** que el transporte abra el stream SSE por su propia conexión y que el núcleo lo lea sin pánicos ni reservas de más (DC27).

**Archivos:** `src/transport.rs` (`events` y su resultado), el transporte HTTP, un módulo privado del parser y de la decodificación de los eventos, con sus tests unitarios en `src/tests/`; `tests/common/` (el `events` del transporte de prueba, sobre el router en proceso) y los tests del transporte HTTP. `ARCHITECTURE.md` §11.

**Qué se hace:** `TransportClient::events`, con su implementación por defecto (el frame de `EventsDisabled`), en el transporte HTTP (con su propio cliente HTTP/1.1) y en el de prueba; el parser, con su tope; y la decodificación de cada evento a un tipo interno (`ops` con su `OpsEvent`, `head_advanced` con su head, `schema_reloaded`, `snapshot_wanted`, `snapshot_available` con su secuencia, `lagged`, y uno desconocido), que todavía nadie usa: lo usa el worker desde el Lote 7.

**Tests exigidos.**
- El transporte HTTP, contra el servidor en un socket: el stream trae un `event: ops` con el frame del commit de otro cliente; con el SSE apagado, el frame de `EventsDisabled` (HTTP 404); sin token, `Unauthorized`; y, con el reloj real y un token de vencimiento corto, el stream termina cuando vence su token.
- El stream va por su propia conexión: con varios streams abiertos sin leer, los pedidos del mismo transporte siguen saliendo y respondiéndose. Un transporte sin `events` propio responde `EventsDisabled`.
- El parser (unitarios): un evento partido en cualquier punto entre bloques, varias líneas `data:`, `\r\n`, comentarios y campos desconocidos; los seis eventos del servidor y uno de nombre desconocido.
- Negativos: un evento o una línea de más de 64 KiB, bytes que no son UTF-8, base64 inválido, y un frame que no decodifica o que es de otro mensaje son un error tipado, sin pánico; el fuzzing con bytes al azar y eventos dañados, en su versión corta y en la larga.

**Interacciones a revisar:** el keep-alive del servidor, cada 15 s; el cliente HTTP aparte del stream, que no pasa a un cliente nuevo con los cortes de los pedidos (Lote 1); el tope del evento del servidor (4 KiB de frame); el estado HTTP y el `Content-Type` del stream y de sus errores; el tope de streams por cliente (el tercero cierra el más viejo) y el total del servidor (`max_sse_streams`, la mitad de las conexiones); el *stall timeout*, que cierra la conexión de un stream que no se lee; el corte del stream en el apagado del servidor.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K12, K15, K7.

**Entrega:** `feat(client): open the event stream on its own connection and read its events within a limit (Fase 4.3, Lote 2)`.

**Hecho** (9 de Noviembre de 2026).
- **Qué entró.**
  - `TransportClient::events(room_id, token)`, con su resultado, `EventStream`: `Open` con el cuerpo como bloques de bytes (`EventBlocks`, un `Stream` de `futures`) si el servidor responde 200, y `Refused` con los bytes de la respuesta, hasta `MAX_FRAME_SIZE`, con cualquier otro estado. La implementación por defecto responde el frame de `EventsDisabled`, con la sala y sin correlación, como un servidor con el SSE apagado.
  - En el transporte HTTP, un segundo cliente HTTP, solo HTTP/1.1, para los streams: con `https://`, el handshake ofrece solo `http/1.1`, y el *prior knowledge* no le aplica. No pasa a un cliente nuevo con los cortes de los pedidos.
    - El `GET /rooms/{room_id}/events` lleva `Authorization: Bearer` y `Accept: text/event-stream`.
    - Abrir el stream tiene el plazo de conectar y, hasta los headers, el de inactividad; una negativa se lee como una respuesta (hasta un frame, con el plazo de inactividad entre sus pedazos). Un stream abierto no tiene plazo.
    - La lectura acotada de una respuesta pasó a una función que comparten `send` y `events`; en `send`, un corte en la respuesta sigue llevando a un cliente nuevo.
  - En el transporte de prueba, `events` sobre el router en proceso, y en `tests/common/`, un helper que lee los datos del próximo evento de un nombre.
  - Un módulo privado, `src/events.rs`:
    - el parser del SSE (`EventParser`): líneas con `\n`, `\r` o `\r\n` (también partidos entre bloques), los campos `event` y `data` (un espacio inicial menos; varias líneas `data` unidas con `\n`), los demás campos y los comentarios ignorados, y la línea en blanco que cierra un evento, que se entrega solo si tiene datos, con el nombre `message` si no tiene `event`. Lo que queda después de la última línea en blanco al terminar el stream se descarta.
    - El tope: 64 KiB por evento, contando los bytes de sus líneas sin sus fines, comentarios incluidos, y así por línea. Un evento o una línea que lo pasan, o una línea que no es UTF-8, son un error que cierra el stream: el parser lo repite en cada llamada y no retiene nada, tampoco lo que llega después.
    - `EventReader`, que lee los bloques del transporte de a un evento (un error del transporte también lo termina).
    - La decodificación de cada evento a `RoomEvent`: `Ops` (el `OpsEvent` del frame, en base64 estándar con padding), `HeadAdvanced` y `SnapshotAvailable` con su secuencia, `SchemaReloaded`, `SnapshotWanted`, `Lagged` y `Unknown`. Lo que no decodifica es un `EventError` tipado: `Base64`, `Frame`, `NotOps` o `Sequence`, y el stream sigue.
    - Nadie lo usa todavía: lo usa el worker desde el Lote 7. El módulo lleva `expect(dead_code)` fuera de los tests, que avisa cuando deje de hacer falta.
  - `futures` y `base64` pasan a dependencias del cliente (ya estaban en el lock), solo fuera de wasm32.
  - Unas 300 líneas de producción, sin comentarios ni líneas vacías (se estimaron 200): el parser, el lector y los tipos de los eventos son unas 210.
- **Tests.**
  - `tests/http_transport_tests.rs`: 13 tests nuevos (34 en total), contra el servidor en un socket local y contra servidores mínimos del test.
    - El stream trae un `event: ops` con el frame del commit de otro cliente (la sala, la encarnación y las operaciones), con el transporte por HTTP/1.1 y por HTTP/2.
    - Las negativas llegan como frames: un token vencido, vacío o que no es un token, `Unauthorized`; uno de otra sala, `Forbidden`; con el SSE apagado, `EventsDisabled` (HTTP 404).
    - Los topes del servidor: con `max_sse_streams` en 2, un stream de otro cliente es `RateLimited`, y el tercero del mismo cliente termina el más viejo.
    - Con el reloj real, el stream termina cuando vence su token (de 2 s), y cuando el servidor se apaga.
    - Con 33 streams abiertos sin leer (más de los 32 que el servidor acepta en una conexión HTTP/2), un pedido del mismo transporte por HTTP/2 sale y se responde; el frente ve 33 conexiones que empiezan con el `GET` del stream y una sola de HTTP/2.
    - El stream va debajo de la URL, con el token y `Accept: text/event-stream`, y trae el cuerpo hasta su fin.
    - Con `https://`, el handshake del stream ofrece solo `http/1.1`, también con *prior knowledge*.
    - Un servidor que calla antes de los headers del stream, o en medio del cuerpo de una negativa, lo corta en el plazo.
    - Una negativa se lee entera hasta un frame, y una más larga no se lee.
    - Una redirección no se sigue y un 204 no abre el stream: los dos son negativas.
    - Una conexión del stream ociosa más que el pool no se reusa, y dos negativas seguidas comparten una.
    - Un stream cortado en el medio, sin el último chunk, termina con un error y no como un fin.
  - En los tests del Lote 1, el que corta un pedido en el plazo y el de la respuesta HTTP/2 cortada a la mitad tienen ahora un tope propio, para fallar en lugar de colgarse si el plazo deja de cortar; y el servidor mínimo que lee un pedido acepta uno sin `Content-Length`, como el `GET` del stream.
  - `tests/events_tests.rs` (nuevo): un transporte que solo implementa `send` responde el frame de `EventsDisabled` con la sala; el transporte de prueba abre el stream del servidor en proceso, trae un `ops` y da `Unauthorized` con un token vencido.
  - `src/tests/events.rs` (nuevo): 21 unitarios y la versión larga del fuzzing.
    - Los seis eventos del servidor, como los escribe (con un keep-alive en el medio), partidos en dos bloques en cada punto posible y de a un byte, dan los mismos eventos.
    - Varias líneas `data`, con y sin espacio, y una sin dos puntos; los tres fines de línea, también un `\r` al final de un bloque con el `\n` al principio del siguiente; los comentarios y los campos `id`, `retry` y desconocidos; un evento sin datos, que no se entrega y cuyo nombre no pasa al siguiente; y lo que queda sin línea en blanco.
    - Decodifica los seis eventos del servidor, y uno de nombre desconocido (también `message`) es `Unknown`.
    - Un evento de exactamente 64 KiB se lee; una línea de un byte más, antes de que llegue su fin, y un evento de líneas cortas que pasa el tope, también con comentarios, terminan el stream. El tope es de cada evento: 8 eventos de 16 KiB se leen.
    - Pedir otro evento sin un bloque nuevo no da nada más, y el bloque siguiente completa el evento sin duplicar bytes.
    - Un `\r` seguido de otra línea que sigue en el bloque siguiente: el `\n` que empieza ese bloque termina la segunda línea.
    - 70.000 keep-alive y después un evento: los eventos sin datos no se suman al tope.
    - Un campo sin dos puntos tiene un valor vacío.
    - Los búferes de un evento del tope, que llega en dos pedazos, suman a lo sumo el doble del tope, y nada después de entregarlo.
    - Después de un error, el parser no retiene nada, tampoco lo que llega, y repite el error.
    - Bytes que no son UTF-8, en un dato, en un comentario o en un nombre, terminan el stream; un carácter partido entre dos bloques no.
    - Base64 inválido o sin padding, un frame vacío, truncado, con bytes de más o de otro mensaje (`Error`, `Schema`), y secuencias vacías, negativas, decimales o más grandes que un `u64`, son errores tipados.
    - El lector entrega los eventos de los bloques hasta que el stream termina, y después sigue dando el fin, sin volver a pedir el stream (un `unfold`, como el del transporte HTTP, entraría en pánico); un error del transporte o del parser lo termina, y lo repite.
    - Un lector que se suelta mientras espera un bloque (con el reloj pausado, más de 10 veces) no pierde ni duplica nada.
    - El fuzzing: bytes al azar (sobre todo, piezas de la sintaxis, frames de `ops` en base64 y líneas largas) y los eventos del servidor dañados (un byte cambiado, quitado o agregado, o cortados), partidos en bloques al azar, dan los eventos y el error de un modelo del formato que lee el texto entero de una vez y no comparte código con el parser; ningún evento pasa el tope, y cada uno decodifica o es un error tipado. 300 corridas en la suite y 40.000 en la larga, con `#[ignore]`.
- **Mutaciones inversas: las 50 las mató algún test.**
  - El parser: el `\r\n` como un solo fin, el `\r` como fin, la marca del `\r` al final de un bloque; el tope por evento y su borde `>=`, contarlo por línea en lugar de por evento, y no reiniciarlo en la línea en blanco; UTF-8 (con un decodificador con reemplazos); el evento sin datos, el nombre por defecto y que el nombre no pase al siguiente; el espacio inicial, el `\n` entre las líneas `data`, los comentarios como datos y el campo `event`; consumir el bloque cuando no trae un fin de línea; el error que se repite, no retener nada después, y el error del transporte que se repite.
  - La decodificación: el base64 con padding, el nombre de cada uno de los seis eventos, el desconocido, la secuencia como error tipado y el frame de otro mensaje.
  - El `events` por defecto: `EventsDisabled` y la sala.
  - El transporte HTTP: el cliente del stream solo HTTP/1.1, el stream por su cliente y no por el de los pedidos, el bearer y el `Accept`, el plazo hasta los headers, el estado 200, la negativa acotada y la negativa que calla.
  - Las reglas del Lote 1 que se movieron a la lectura compartida: los dos topes de la respuesta, el plazo por pedazo y el cliente nuevo después de un corte en la respuesta.
  - Dos sobrevivieron a la primera corrida y llevaron a tests nuevos: no reiniciar el tope en la línea en blanco (8 eventos de 16 KiB se leen) y no consumir el bloque sin fin de línea (pedir otro evento sin un bloque nuevo no da nada más, ni duplica bytes). Otra, el plazo por pedazo, colgaba dos tests del Lote 1 sin tope propio, que ahora lo tienen.
  - Siete más, de la revisión de delta, con sus tests: el `Fuse` del lector; reponer la marca del `\r` cuando sigue otro byte; reiniciar el tope también en un evento sin datos; que el cliente del stream no siga redirecciones y su pool; el error de un stream cortado en el medio; y abrir el stream solo con 200, no con cualquier 2xx.

  Quedan sin test, con su argumento:
  - el `connect_timeout` del cliente del stream: como en el Lote 1, haría falta una dirección que descarte los SYN, y eso no es portable en la CI; el plazo hasta los headers cubre también la conexión;
  - revisar el tope antes de copiar la línea, y no después: la diferencia es transitoria (después del error el parser se vacía igual) y de a lo sumo un bloque;
  - que el cliente del stream no cambie con los cortes de los pedidos: no hay código que lo cambie (en las interacciones).
- **Interacciones revisadas.**
  - El keep-alive del servidor, cada 15 s, es un comentario (`:` y una línea en blanco): el parser lo saltea sin entregar nada y no lo suma al tope (hay tests). Que cuente como señal de vida es del Lote 7: el silencio de 45 s del worker se mide en los bloques que llegan, y el lector no entrega los comentarios, así que el worker envuelve los bloques antes del lector o el lector avisa de cada bloque (anotado en el Lote 7).
  - El cliente HTTP del stream no pasa a uno nuevo con los cortes de los pedidos: es otro campo del transporte, que `replace` no toca, y con HTTP/1.1 cada stream tiene su conexión, que se cierra al soltarlo. Una conexión trabada es solo la de su stream, y el worker la deja atrás al darlo por muerto (Lote 7). Sin test: no hay código que lo pueda romper.
  - El tope del evento del servidor (4 KiB de frame, unos 5,5 KiB en el evento) queda muy debajo del de 64 KiB del cliente: ningún evento del servidor lo pasa.
  - El estado HTTP y el `Content-Type`: con 200, el stream; con cualquier otro, la negativa, hasta un frame, que el núcleo decodifica (hay tests con 401, 403, 404 y 429). El `Content-Type` de un 200 no se mira: un proxy que responde 200 con otra cosa da bytes que el parser lee como cualquier otro stream, sin pánico (el fuzzing), y el worker lo trata como un stream sin eventos que termina.
  - El tope de streams por cliente (el tercero cierra el más viejo) y el total del servidor (`max_sse_streams`): hay test; el más viejo termina como cualquier stream, y `RateLimited` llega como frame.
  - El *stall timeout* del servidor cierra la conexión de un stream que no se lee: con su propia conexión, solo la de ese stream (el test de los 33 streams muestra una conexión por stream y los pedidos por otra).
  - El apagado del servidor y el vencimiento del token terminan el stream: hay tests, con el reloj real.
  - El pool del cliente del stream retiene una conexión ociosa 5 s, menos que el `header_read_timeout_secs` del servidor (10 s), así que un stream que se vuelve a abrir después de uno que terminó bien no sale por una conexión que el servidor está cerrando.
  - El tope de conexiones del servidor: cada stream ocupa una, y `max_sse_streams` es por defecto la mitad, así que los streams no las toman todas.
  - `new` arma dos clientes HTTP: en Linux, lee los certificados de la plataforma dos veces (unos 10 a 16 ms cada una).
  - Los transportes que no implementan `events` (el de la simulación, los de los parches de la auditoría de 4.2) compilan igual y responden `EventsDisabled`.
  - El proxy del entorno (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`) vale también para el cliente del stream: con `https://`, un `CONNECT` por stream; con `http://`, un proxy de reenvío que podría retener el cuerpo en su búfer, y entonces el stream parecería callado (lo cubren el heartbeat del worker y el silencio de 45 s del Lote 7).
  - El cliente del stream comparte con el de los pedidos la configuración validada (el pool, con su tope de 300 s, y el plazo de conectar) y la política de redirecciones: un `3xx` es una negativa (hay test), que el núcleo no decodifica como frame.
  - Un stream cuya apertura el cliente corta antes de los headers puede seguir abierto en el servidor hasta que nota la conexión cerrada (a lo sumo, en el keep-alive siguiente): ocupa un lugar de `max_sse_streams` y uno de los 2 por cliente, y el tercero del mismo cliente reemplaza al más viejo, así que reabrir no espera.
  - La negativa reserva lo que anuncia su `Content-Length`, hasta `MAX_FRAME_SIZE`, aunque un frame de error es chico: es el mismo tope que el de una respuesta, como pide DC27.
  - Un BOM al principio del stream no se descarta, a diferencia del formato: el servidor no manda uno, y el primer evento detrás de uno sería de un nombre desconocido (en ARCHITECTURE). `u64::from_str` acepta `+7` como secuencia: inofensivo.
  - Cancelación: el lector se puede soltar mientras espera un bloque sin perder ni duplicar nada (hay test), como lo va a usar el worker.
  - wasm32: el crate sigue vacío ahí, y `futures` y `base64` son dependencias solo fuera de wasm32; el build y el clippy pasan.
  - El MSRV: `cargo +1.85 build --workspace --lib --bins` y `cargo +1.88 test --workspace --no-run` pasan; `expect` con `reason` es de Rust 1.81.
- **Revisión de delta, primera ronda: verde.** Sin hallazgos Altos ni Medios; los Bajos y las notas se arreglaron en el lote:
  - Bajo: el argumento para dejar el `Fuse` sin test no se sostenía (cambiando también el tipo del campo, compila, y con el `unfold` del transporte HTTP el lector entraba en pánico al pedirlo otra vez): test nuevo.
  - Bajo: dos reglas del parser solo las mataba el fuzzing largo (la marca del `\r` y el tope de un evento sin datos, que con los keep-alive cerraría el stream a los 11 días): tests nuevos.
  - Bajo: tres reglas del cliente del stream sin test (las redirecciones, el pool y el error de un corte en el medio): tests nuevos, y uno más para un 204.
  - Bajo: faltaban en las interacciones el proxy del entorno, la configuración y las redirecciones compartidas, y el stream que queda abierto en el servidor después de un corte antes de los headers.
  - Notas: el BOM (precisado en ARCHITECTURE), la capacidad de los búferes (K15 y ARCHITECTURE dicen ahora hasta unos 128 KiB, con un test), lo que el test de K15 decía de más, el silencio de 45 s del Lote 7 (anotado en sus interacciones) y la reserva de la negativa (en las interacciones).
  - Adoptó sus tests, con otros nombres, salvo los dos que fallaban a propósito (el BOM y la capacidad), que se resolvieron en los documentos y con el test de los búferes.
- **Desvíos.**
  - Unas 300 líneas de producción, contra las 200 estimadas: casi todo, el parser y los tipos de los eventos.
  - Precisiones de DC27 que el plan no decía:
    - el tope de 64 KiB cuenta los bytes de las líneas del evento sin sus fines, con los comentarios;
    - un evento sin datos no se entrega, y lo que queda sin línea en blanco al terminar el stream se descarta, como en el formato de SSE;
    - abrir el stream tiene el plazo de inactividad hasta los headers, y la negativa, entre sus pedazos (DC27 dice que el de un stream abierto lo pone el worker);
    - el pedido lleva `Accept: text/event-stream`;
    - el `Content-Type` de un 200 no se mira.
  - Un lector (`EventReader`) que el plan no nombraba: lee los bloques del transporte de a un evento, que es lo que cuenta INV-K15.
  - `futures` y `base64` pasan a dependencias del cliente.
  - Los tests del `events` por defecto y del transporte de prueba van en un archivo nuevo, `tests/events_tests.rs`.
  - La lectura acotada de una respuesta se sacó de `send` a una función que comparte con `events`, sin cambiar lo que hace (lo confirman las mutaciones del Lote 1 que se volvieron a correr).

### Lote 3 · La renovación del token (`zemdb-client`) · ✅ Hecho
**Objetivo:** que un token vencido o inservible no deje a la sala sin red (DC30).

**Archivos:** `src/client.rs` (el envío de cada pedido, el registro y las escrituras), un módulo del `TokenProvider` y `src/lib.rs`; `tests/common/` (un proveedor de prueba) y `tests/token_tests.rs`. `ARCHITECTURE.md` §11 (*Tokens*).

**Qué se hace:** el `TokenProvider` en `Client::open`; ante `Unauthorized`, la renovación, una por sala a la vez y con su plazo, y el pedido repetido una vez; la verificación del `ClientId` del token renovado; ante `Forbidden`, el token marcado como inservible; en una escritura, el intento con `Unauthorized` que no queda incierto y el reintento dentro del plazo.

**Tests exigidos.**
- Con un token vencido (emitido con un TTL de 0), el registro, un sync, un ack, un heartbeat, un commit, y un chunk de bajada y uno de subida reciben `Unauthorized`, piden un token y se repiten con el nuevo, que usa también el pedido siguiente.
- Varios pedidos a la vez con el mismo token vencido: una sola renovación. Un proveedor que no responde: al vencer su plazo, los pedidos fallan sin token nuevo; y una renovación cuyo pedido se corta no deja esperando a los demás.
- Una escritura con `Unauthorized` y un token nuevo termina con la decisión de la réplica, con el mismo `MutationId`; sin token nuevo (el proveedor falla, o el nuevo también recibe `Unauthorized`), con `Network`, y con `OutcomeUnknown` si un intento anterior quedó incierto.
- Después de un `Forbidden` (un token de otra sala), el pedido siguiente de la sala pide un token al proveedor antes de mandarse, y con uno bueno sale bien.
- Un token renovado de otro `ClientId` (emitido con `generate_client_token` para otro dispositivo), o que no tiene el formato de un token, cuenta como sin token nuevo, y ningún pedido sale con él; uno del mismo `ClientId` se usa.
- Negativos: `Forbidden` no se repite en el mismo pedido, y la escritura termina con `Validation`; un `Unauthorized` de otra correlación o de otra sala no renueva; un segundo `Unauthorized`, con el token renovado, no pide otro en el mismo pedido.

**Interacciones a revisar:** el token de `open_room`, que reemplaza al renovado (gana el último); el lock de los registros; el plazo de una escritura, dentro del que cuenta la renovación; la baja al fallar cerrado; un pedido en vuelo cuando la sala se descarta (el token es de la sala, no de la época).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K17.

**Entrega:** `feat(client): renew the token of a room on Unauthorized (Fase 4.3, Lote 3)`.

**Hecho** (10 de Noviembre de 2026).
- **Qué entró.**
  - `TokenProvider` (`src/token.rs`, nuevo): un trait asíncrono de la aplicación, `token(client_id, room_id)`, que devuelve un token o un `TokenError`. `Client::open` lo recibe después del transporte, y `ZemdbClient::open` de 4.4 se lo pasa (lo que su plan ya decía).
  - El token de la sala: el del último `open_room` o el de la última renovación después de él. Cada pedido lo lee al mandarse (`Request` ya no lo lleva: el token es de la sala, no de su réplica), y el registro lo lleva también adentro.
  - Un solo camino de envío para todos los pedidos (el registro, el sync, el ack, el heartbeat, el commit, los chunks de la bajada y de la subida, y la baja):
    - un `Unauthorized` que responde al pedido (con su correlación y su sala, o sin ellas) renueva el token, y el pedido se repite una vez con el nuevo;
    - un `Forbidden` marca el token de la sala si sigue siendo ese, y el pedido no se repite;
    - un pedido de la sala con el token marcado pide otro antes de mandarse y, sin uno, se manda con el viejo;
    - un pedido renueva una vez como mucho, también contando la renovación de antes de mandarse.
  - La renovación (`renew`): una por sala a la vez, con un candado de Tokio y un contador de renovaciones terminadas.
    - El pedido cuyo token ya reemplazó otra renovación o un `open_room` toma el de la sala sin pedir.
    - El que esperó una renovación de su mismo token que no consiguió uno no consigue ninguno; uno con otro token, que esperó una renovación ajena, pide el suyo.
    - Si no, le pide al proveedor, que tiene 60 s.
    - Solo cuenta un token con el formato con que el servidor los emite (`v1.<cliente>.<sala>.<vencimiento>.<firma>`, con los ids en base64url sin padding, el vencimiento en dígitos y la firma en hexadecimal) y que nombra el `ClientId` propio. Ese token pasa a ser el de la sala, salvo que un `open_room` la haya reemplazado mientras se renovaba: entonces gana el del open.
    - Una renovación cortada no cuenta como terminada, y el siguiente que necesita un token lo pide.
  - Lo que no se manda: una escritura no manda nada más una vez que terminó (la sala cerrada, la réplica descartada o fallada cerrada) mientras se renovaba el token, y un chunk de una subida, una vez que la réplica no puede subir. El envío pregunta antes de cada vez, también después de renovar.
  - En la escritura, el intento lleva una marca "en vuelo". El `Unauthorized` la baja antes de renovar, porque no tuvo efecto, así que un plazo que corta la renovación no deja la escritura incierta. Sin token nuevo, la escritura termina como dice la tabla de §A.4.
  - Unas 170 líneas de producción, sin comentarios ni líneas vacías (se estimaron 170).
- **Tests.**
  - `tests/token_tests.rs` (nuevo, 22), contra el servidor en proceso, con un proveedor de prueba en `tests/common/` (`TestTokens`, que da tokens válidos y distintos, vencidos, de otra sala, de otro cliente, texto, con demora, un error o nada) y el token con que sale cada mensaje (`TestTransport::bearers`).
    - Con un token vencido (TTL 0), el registro, un sync, un heartbeat y un commit reciben `Unauthorized`, piden un token para el `ClientId` y la sala, y se repiten con el nuevo, que usa también el pedido siguiente; un ack y un chunk de subida y uno de bajada con un `Unauthorized` fabricado, lo mismo.
    - Un heartbeat, un sync y una escritura a la vez con el mismo token vencido: una sola renovación, y los tres salen con el token nuevo.
    - Un proveedor que no responde: el heartbeat y el sync que esperan la misma renovación fallan a los 60 s, con un solo pedido al proveedor, y el pedido siguiente pide otra vez.
    - Una renovación cortada por el plazo de su escritura: la escritura termina con `Network` a los 30 s, sin otro commit, y el heartbeat que la esperaba pide otra vez y sale bien.
    - Una escritura con `Unauthorized` y un token nuevo termina con la decisión de la réplica, con dos commits del mismo `MutationId`. Sin token nuevo termina con `Network`, y con `OutcomeUnknown` después de un intento con `Unavailable`.
    - Un segundo `Unauthorized`, al token renovado, no pide otro, en un heartbeat, una escritura y un pedido renovado antes de mandarse. Es con el reloj real y un tope propio, porque un pedido que pidiera otra vez no terminaría.
    - `Forbidden` (un token de otra sala): el heartbeat falla y la escritura termina con `Validation`, sin repetirse. El pedido siguiente pide otro token antes de mandarse y sale bien. Sin token nuevo, se manda con el viejo, y con uno bueno sale bien.
    - Un token renovado de otro `ClientId`, `not-a-token` o vacío: el pedido falla, ningún pedido sale con él, y el siguiente pide otra vez.
    - Un `Unauthorized` de otra correlación o de otra sala no renueva (el pedido falla); uno sin correlación o sin sala, sí.
    - El token de un `open_room` reemplaza al renovado, y también a uno marcado, sin pedir otro. Un `Forbidden` a un token que un `open_room` reemplazó mientras tanto no marca el nuevo.
    - De la revisión de delta:
      - una escritura cuya sala se cierra o falla cerrada mientras renueva el token, después de un `Unauthorized` o antes de su primer intento con un token marcado, termina con `Network`, sin mandar nada después, y el servidor no secuencia nada;
      - el commit que sale otra vez con el token renovado y no tiene respuesta antes del plazo deja la escritura incierta, y el servidor lo secuenció;
      - un pedido rechazado con otro token, que esperó la renovación ajena que no consiguió ninguno, pide el suyo;
      - un `open_room` mientras se renueva el token gana: el pedido y el siguiente salen con su token;
      - un token renovado de otra sala recibe `Forbidden`, y el pedido siguiente pide otro antes de mandarse;
      - un chunk de una subida que espera la renovación no sale otra vez si la sala se cierra mientras tanto.
  - `src/tests/token.rs` (nuevo, 3): el formato de los tokens que emite `generate_client_token`, con TTL 0 o no, para otra sala, con ids cuyo base64url tiene `-` o `_` y uno que necesitaría padding, nombra su cliente; uno de otro cliente no; y no lo nombran: vacío, sin puntos, con cuatro o seis campos, otra versión, el cliente en base64 estándar, con padding, sin codificar, vacío o que no es UTF-8, y la sala, el vencimiento o la firma fuera de su formato.
  - Dos tests de 4.2 sacan `Unauthorized` de sus listas de errores que terminan el pedido (`a_sync_or_an_ack_the_server_refuses_fails_and_changes_nothing` y `a_refusal_of_the_server_ends_the_write`): ahora renueva, y `token_tests.rs` lo cubre.
- **Mutaciones inversas: las 38 las mató algún test.**
  - El formato del token, sobre su versión final: el sexto campo, la versión `v1`, comparar el cliente, el base64url sin padding, la sala en base64url, los ids no vacíos, el vencimiento en dígitos, la firma en hexadecimal y los campos no vacíos. Antes de completar el formato, también cinco campos y no cuatro.
  - La renovación:
    - el candado, tomar el token de la sala si ya no es el viejo, no pedir otro si la renovación que esperó (de su mismo token, y solo de ese) no consiguió ninguno, y contar las renovaciones terminadas;
    - el plazo de 60 s, cambiado a 30 s y quitado;
    - el `ClientId` propio, guardar el token renovado, que guardarlo borre la marca, y que gane un `open_room` hecho mientras se renovaba.
  - El envío:
    - renovar ante `Unauthorized`, y una sola vez, también contando la renovación de antes de mandarse;
    - renovar antes de mandar con el token marcado;
    - marcar ante `Forbidden` el token mandado, y solo si sigue siendo el de la sala;
    - que `open_room` borre la marca;
    - la correlación y la sala del `Unauthorized`;
    - el token adentro del registro;
    - preguntar si se puede mandar antes de cada vez; que la escritura pregunte si sigue en vuelo y el chunk, si la réplica puede subir.
  - La escritura: la marca "en vuelo" en cada envío y no una vez, bajarla ante un `Unauthorized`, y el corte durante la renovación sin efecto.
  - Tres reglas no tenían test en la primera versión y llevaron a tests nuevos antes de correrlas:
    - el segundo `Unauthorized`, también después de la renovación previa a un `Forbidden`: con el reloj real y un tope propio, porque con el reloj pausado la mutación colgaba la suite;
    - el `Forbidden` a un token ya reemplazado;
    - el `open_room` que borra la marca.
  - Los tests de las reglas que agregó la revisión de delta (siete mutaciones más, y cinco del formato) los mataron.
- **Interacciones revisadas.**
  - El token de `open_room` y el renovado: un `open_room` reemplaza al renovado y borra la marca de `Forbidden`, y una renovación posterior reemplaza al de `open_room` (hay tests). Un `open_room` durante una renovación gana sobre ella, como dice DC30: los que esperan toman su token sin pedir, y la renovación que termina después devuelve el del open y no guarda el suyo (hay test).
  - El candado de los registros: el registro renueva con ese candado tomado, y la renovación nunca lo toma (solo el suyo), así que no hay un ciclo de candados (hay test del registro). Tampoco toma el de las transferencias ni el aplicador: ningún cambio de la réplica manda pedidos.
  - El plazo de una escritura, dentro del que cuenta la renovación: el plazo la corta, y la escritura no queda incierta (hay test).
  - La baja al fallar cerrada: va por el mismo envío, así que un `Unauthorized` renueva y la repite; su respuesta sigue sin usarse. Sin test propio: es el mismo código que los pedidos con test.
  - Un pedido en vuelo cuando la sala se descarta: el token es de la sala, no de la época. Ni el descarte ni la renovación miran la época, y la renovación guarda el token en la sala.
  - Los reintentos de un chunk (`send_chunk`): cada envío puede renovar una vez, y el que se repite después de un `Unavailable` sale con el token de la sala.
  - Los cortes: cada `.await` nuevo es seguro ante cancelación. El candado se suelta al caer, el contador solo sube cuando el proveedor respondió, y ningún lock síncrono cruza un `.await` (hay test del corte).
  - INV-K15: cada sala suma un token, una marca, un contador con el token de la última renovación terminada, y un candado: nada crece por pedido.
  - La fachada de 4.4 (Lote 2, `e536d3a`): `ZemdbClient::open` recibe el proveedor después del transporte, y sus tests le pasan uno. El primer token del proveedor y la verificación de su `ClientId` en `room(room_id)` son de su Lote 8, que puede usar el mismo formato y el mismo plazo.
  - La simulación: sus tokens valen un día, y sus fallas no tienen `Unauthorized`. Nada renueva, y sigue igual.
  - De la revisión de delta:
    - El ciclo de la escritura y su chequeo entre intentos (cerrar la sala, descartar la réplica o fallarla cerrada terminan la escritura) frente a la renovación, que corre dentro del envío: el envío pregunta si la escritura sigue en vuelo antes de cada vez, también después de renovar (hay tests).
    - La baja de una sala que falla cerrada espera la misma renovación que una escritura (comparten el candado): con lo anterior, el commit de esa escritura ya no sale.
    - `send_chunk`: su condición se pregunta también antes del envío que sigue a una renovación (hay test de la subida); la de la bajada es siempre que sí, como antes.
    - `shutdown` durante una renovación: cierra las salas, y vale lo mismo que con `close_room`.
    - Los demás pedidos (sync, ack, heartbeat, registro, baja) que salen después de una renovación durante la que la sala se cerró: su respuesta se descarta como antes (un cambio de la réplica da `RoomClosed`, y la baja no la usa). La ventana existía, entre leer el pedido y mandarlo, y ahora puede durar lo que tarda el proveedor; ninguno tiene efecto en la réplica.
    - Un pedido con otro token vencido que espera una renovación ajena (hay test), y cuánto espera en total: las renovaciones que tiene delante y la suya (en `INVARIANTES.md`).
- **Revisión de delta, primera ronda: no verde.** Dos Medios y cinco Bajos, todos arreglados en el lote:
  - Medio: una escritura que renueva el token no volvía a mirar si seguía en vuelo. Si la sala se cerraba o fallaba cerrada mientras tanto, el intento salía igual, el servidor lo secuenciaba y la escritura terminaba incierta en lugar de con `Network` (contra INV-K2 e INV-K6, y contra lo que 4.2 promete al cerrar). Lo mismo pasaba con un chunk de una subida. Ahora el envío pregunta antes de cada vez, con tests.
  - Medio: que el intento mandado otra vez y cortado por el plazo deje la escritura incierta no tenía test: test nuevo.
  - Bajo: marcar el token mandado (el renovado) ante `Forbidden` no tenía test: test nuevo, con un token renovado de otra sala.
  - Bajo: un pedido rechazado con otro token tomaba el fracaso de una renovación ajena. Ahora la toma solo si era de su mismo token, con test.
  - Bajo: una renovación que terminaba después de un `open_room` pisaba su token, contra DC30 ("el de un `open_room` posterior reemplaza al renovado"). Ahora gana el del open, con test.
  - Bajo: la renovación previa a un `Forbidden` cuenta como la única del pedido; va en *Desvíos*.
  - Bajo: documentos. "Ningún pedido espera más de 60 s" era inexacto, porque uno detrás de una renovación cortada espera también la suya. "Un open con otro token" borra la marca: lo hace cualquier open. Y había dos líneas de docstring de más de 100 caracteres.
  - El formato verificado solo en parte iba contra la letra de DC30: ahora se verifica entero (la sala, el vencimiento y la firma, sin validarlos), con tests y sus mutaciones.
  - Adoptó sus tests de reproducción, con otros nombres.
- **Revisión de delta, segunda ronda: verde.** Verificó los siete arreglos de la primera ronda y el formato completo, y mató otras 8 mutaciones de lo nuevo (el envío que pregunta una sola vez, la escritura y el chunk sin su condición, la marca "en vuelo" antes de preguntar, y las variantes de H4 y H5). Sin Altos ni Medios. Sus dos Bajos, de redacción, se arreglaron: en estas interacciones, el `open_room` durante una renovación (decía lo de antes del arreglo) y lo que suma cada sala, y el docstring de `send_chunk` (un chunk que su condición frena después de una renovación es una falla transitoria: `None` después del backoff, o `Network` si era la última vez).
- **Desvíos.**
  - `TokenProvider::token` recibe también el `ClientId`: el proveedor se da antes de que exista el cliente, y así el backend no necesita guardar el id.
  - Precisiones de DC30, que DC30 no decía:
    - El pedido con un token marcado que no consigue uno nuevo se manda con el viejo. Mandarlo deja que un `Forbidden` que no vino del servidor no frene la sala (INV-K17), y uno real vuelve a dar `Forbidden`.
    - Esa renovación de antes de mandarse es la única del pedido: un `Unauthorized` al token que trajo no pide otro, y el pedido siguiente lo vuelve a pedir. Así, un pedido le pide al proveedor una vez como mucho.
    - Un `open_room` durante una renovación gana sobre ella.
  - `INVARIANTES.md` toma la versión 24: la 23 la tomó el Lote 2 de 4.4.

### Lote 4 · El worker: la tarea, el heartbeat y las transferencias (`zemdb-client`) · ✅ Hecho
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

**Interacciones a revisar:** las escrituras, que hacen sus propios heartbeats; el `heartbeat` público que la aplicación llama a la vez; el lock de las transferencias; la designación y su plazo de 60 s en el servidor; el registro que hace el worker (el lock de los registros); un descarte en curso; el reloj pausado y las tareas de `spawn_blocking` del storage; con HTTP/2, los pedidos de todas las salas comparten una conexión (de la revisión de delta del Lote 1): un heartbeat no espera a una transferencia en su tarea, pero sí en el cable, detrás de los bytes que ella ya entregó, contra el *lease* del servidor (si empieza dentro del plazo del pool desde el pedido anterior; si no, abre otra conexión). Y los heartbeats del worker, que el núcleo no corta, son los que hacen pasar al transporte a un cliente nuevo cuando la conexión HTTP/2 deja de responder: las escrituras la sueltan al vencer su plazo (30 s), antes que el del transporte (60 s), y eso no cambia el cliente; con un test.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K7, K15, K17.

**Entrega:** `feat(client): the sync worker of a room, with its heartbeats and transfers (Fase 4.3, Lote 4)`.

**Hecho** (12 de Noviembre de 2026).
- **Qué entró.**
  - El worker (`src/worker.rs`, nuevo): una tarea de Tokio por sala, que tiene al cliente por un `Weak`. `start_worker(room_id)` y `set_freshness(room_id, mode)` toman `self: &Arc<Self>`; `stop_worker(room_id)`, `&self`.
    - Si la aplicación suelta el cliente, el worker termina enseguida: su canal de parada se cierra con la sala. Pero mientras corren un paso, las tareas de los workers retienen el cliente (el heartbeat de cada sala y cada transferencia), así que toda referencia que toman pasa por un guard (`Held`) que el cliente cuenta, con un contador de altas y otro de bajas que solo crecen. El worker que encuentra que solo esas referencias retienen el cliente se detiene en su heartbeat siguiente y corta su transferencia.
    - La cuenta nunca da por soltado un cliente que la aplicación retiene: una referencia se cuenta después de tomarla y se descuenta antes de soltarla, así que la cuenta nunca pasa de las referencias, y si cualquiera de los dos contadores cambia mientras se lee la cuenta de referencias, la pregunta queda para el heartbeat siguiente.
    - `start_worker` arranca el de una sala abierta en el cliente, también sin registrar o vacía después de un registro que falló, en el modo de `ClientConfig::freshness` (`Periodic` por defecto). Una sala que el cliente nunca abrió, o cerró, es `RoomClosed`, leído bajo el aplicador (un cambio en curso saca la réplica del cliente mientras tanto). Arrancar una sala cuyo worker corre no hace nada.
    - `set_freshness` cambia el modo del que corre, o arranca uno en ese modo. El modo se guarda y `status` lo muestra; hasta el Lote 5, ninguno sincroniza por el head.
  - El heartbeat del worker: uno por `heartbeat_interval` (30 s), contado desde el comienzo del anterior, el primero en un momento al azar dentro del primer intervalo. Registra la sala si hace falta y toma el ack como el `heartbeat` público, pero no transfiere nada:
    - si la réplica espera el snapshot que el ack anuncia, lo baja; si no, sube el que el ack le pide;
    - la transferencia corre en una tarea propia, una a la vez, con el lock de las transferencias de la sala, y los heartbeats siguen mientras tanto, salvo durante sus fases bajo el aplicador (crear el snapshot o instalarlo), como con todo cambio de la réplica;
    - una transferencia que falla la vuelve a pedir el heartbeat siguiente que la pide.
  - El núcleo separa el heartbeat de sus transferencias (`beat`): el `heartbeat` público hace lo mismo que antes sobre él, y la subida marca su falla (la pausa de un minuto) adentro, para los dos.
  - Los reintentos: un heartbeat que falla se manda otra vez después de un backoff con *full jitter* (`retry_base`, 1 s, que se duplica hasta `retry_max`, 30 s), y el primero que sale bien vuelve al intervalo y empieza el backoff de nuevo.
  - Lo que lo detiene (DC29): solo `FailedClosed` o `RoomClosed` de sus pasos (el heartbeat, con su registro, y la transferencia, en el momento); nunca `status`. Todo lo demás se reintenta.
  - Detenerlo: `stop_worker`, `close_room` (antes de cerrar la sala del storage) y `shutdown` (todas las salas a la vez, antes de cerrarlas) cortan su tarea en su próximo `.await` y su transferencia, y esperan que terminen. Un worker que se detuvo solo queda en `status`, sin correr, con el error que lo detuvo, hasta que se lo arranca otra vez.
  - `ClientConfig` suma `freshness`, `heartbeat_interval`, `retry_base`, `retry_max` y `events_retry` (10 min, para el Lote 7), y `Client::open` rechaza con `Config` un intervalo en cero o de 60 s o más, un backoff que empieza en cero o cuyo máximo es menor que su base, y un `events_retry` en cero. `Freshness` (`Live`, `Periodic`, `Manual`) es serializable.
  - `RoomStatus::worker` (`WorkerStatus`, serializable): el modo, si corre, si el último pedido de la sala tuvo respuesta (del worker o no; el estado de la conexión, que el núcleo anota en cada envío, `None` antes del primero) y el error de su último paso, salvo que haya salido bien.
  - Unas 350 líneas de producción, sin comentarios ni líneas vacías (se estimaron 200): unas 145 del módulo del worker y el resto en `client.rs` (el heartbeat partido, los métodos del worker, la cuenta de las referencias que retienen el cliente, la validación de la configuración y `status`) y `config.rs`.
- **Tests.**
  - `tests/worker_tests.rs` (nuevo, 19), contra el servidor en proceso, con el reloj pausado salvo uno.
    - Un heartbeat por intervalo, contado desde el comienzo del anterior (cada uno tarda 2 s en llegar al servidor), el primero dentro del primer intervalo, y repartido en él: en 16 salas cuyos workers arrancan juntos, los primeros ocupan más de un cuarto del intervalo; sin worker, ninguno.
    - Durante una bajada cuyo primer chunk retiene el transporte de prueba durante tres *leases* del servidor (270 s), los heartbeats siguen, uno por intervalo, el servidor tiene al cliente como atrasado y al designado como conectado, y la bajada es una sola. El designado sube su snapshot y el atrasado lo baja sin llamar a ningún paso.
    - Un sync durante la bajada del worker espera su transferencia y no baja nada.
    - Con el reloj real y plazos cortos: una sala abierta sin red queda vacía y responde `RoomClosed` en `status`, y su worker sigue; reintenta el registro con una espera que crece (unas 25 veces en 2 s, contra unas 180 si no creciera); el primer heartbeat con respuesta la registra y los siguientes van por intervalo; sin red otra vez, el worker sigue, sin conexión y con el `Network`, y con la red de vuelta, sin error.
    - Con el reloj pausado y fallas contadas (doce), con el backoff de las escrituras mucho más largo que el del worker: cada reintento espera a lo sumo el doble que el anterior, hasta el máximo; el primero que sale bien vuelve al intervalo; y después de un heartbeat que salió bien, el reintento vuelve a esperar a lo sumo la base.
    - `FailedClosed` detiene el worker (un ack con otro esquema y un `Registered` que no evoluciona), y `status` lo muestra detenido con ese error.
    - Una transferencia que hace fallar cerrada la sala (un snapshot que no instala con el esquema de la réplica, y un registro que trae uno que no evoluciona) detiene el worker en el momento, antes del heartbeat siguiente.
    - Un snapshot mayor que el tope: un primer chunk de 64 KiB por heartbeat, el worker sigue y `status` da el `Config`; un error del storage con la sala abierta se reintenta.
    - Un `RoomClosed` de un paso (un `RoomReset` al registrarse y un descarte que no puede borrar los datos): el worker se detiene; la sala reabierta no lo retoma, y `start_worker` sí.
    - `stop_worker` y `close_room` lo detienen (la sala cerrada y reabierta no tiene worker), y el de un cliente que la aplicación suelta termina.
    - El de un cliente que la aplicación suelta con una bajada en vuelo deja de mandar heartbeats, y el cliente se suelta; y lo mismo con dos salas cuyos heartbeats tardan 31 s, así que siempre hay uno en vuelo: los workers no se retienen entre ellos.
    - Con el reloj real y varios hilos, 20 salas cuyos heartbeats van cada 10 ms mientras la aplicación toma y suelta referencias al cliente: ningún worker lo da por soltado (de la tercera ronda; mata la mutación de la relectura de los contadores).
    - Un cliente soltado sin `shutdown` con un heartbeat de su worker en vuelo retiene sus metadatos (otro `Client::open` sobre el mismo directorio da `Storage`) y los suelta cuando el heartbeat termina.
    - Cuando `close_room` y `shutdown` cierran la sala del storage (el motor de prueba retiene el cierre), con una bajada en vuelo, ninguna tarea del worker retiene el cliente: ya lo detuvieron.
    - Un worker que se arranca mientras un heartbeat descarta la réplica por `RoomReset` (el motor retiene el borrado de los datos) espera que termine el descarte y arranca en la sala vacía.
    - Detenerlo con un heartbeat o una bajada en vuelo, con `stop_worker`, `close_room` y `shutdown`, termina en su plazo, y después ninguna tarea del worker retiene el cliente (la cuenta de referencias del `Arc`).
    - `status`: el modo de la configuración, que `set_freshness` cambia y `start_worker` deja como está; `start_worker` y `stop_worker` sobre una sala nunca abierta dan `RoomClosed`; `set_freshness` arranca uno en una sala sin worker; un pedido sin respuesta, del worker o no, deja la sala sin conexión, y el siguiente con respuesta la devuelve; una sala nunca abierta o cerrada no arranca worker.
    - La configuración: un intervalo en cero, de 60 s o de 600 s, un `retry_base` en cero, un `retry_max` menor que la base y un `events_retry` en cero son `Config`; 59,999 s, un máximo igual a la base y 1 ms abren.
  - En `tests/http_transport_tests.rs` (1 más): con una conexión HTTP/2 que deja de responder, las escrituras, que sueltan su pedido a su plazo (200 ms, menor que el de inactividad del transporte, 400 ms), no la dejan atrás; los heartbeats del worker sí, y la escritura siguiente sale por otra conexión.
  - `tests/common/`: el instante de cada mensaje que manda el transporte de prueba (`sent_at`); un tope de 20 000 mensajes, después del cual no responde más, para que un cliente que reintenta sin esperar haga fallar un test con el reloj pausado en lugar de colgarlo (no lo alcanza ningún test); y el motor de prueba retiene también `close_room` y `delete_room`.
- **Mutaciones inversas: 58 de las 60 las mató algún test, y una de las otras dos, de forma probabilística** (los logs, en el scratchpad de la sesión: `mut4/run4.log`, la última corrida entera, y `mut4/run5.log`, la de las que tocan los tests de la tercera ronda).
  - El ritmo: el primer heartbeat al azar (a intervalo fijo y en cero), uno por intervalo, contado desde el comienzo del anterior, volver al intervalo después de un heartbeat que sale bien, y que el fin de una transferencia no mande un heartbeat (sin el tope de mensajes del transporte de prueba, esta colgaba un test pausado).
  - Las transferencias: en la tarea del worker y no adentro del heartbeat, a lo sumo una, la bajada si la réplica la espera y la subida si el ack la pide, cortarla y esperarla al detenerse.
  - Los reintentos: esperar, que la espera crezca y que vuelva a empezar después de uno que sale bien.
  - Lo que lo detiene: `FailedClosed` y `RoomClosed`, y nada más; el de una transferencia, en el momento; que el resultado de la transferencia cuente; que un heartbeat que sale bien borre el error.
  - Detenerlo: el `Weak`, cortar el heartbeat en vuelo, esperar la tarea, cortar la transferencia, `close_room` y `shutdown`, y los dos antes de cerrar la sala del storage.
  - El cliente soltado: preguntarlo; contar cada referencia (la del heartbeat y la de la transferencia) y descontarla; y el límite de la comparación, de más y de menos (de más, el worker se detiene con el cliente retenido).
  - `drop` del canal de parada antes de esperar la tarea (con los topes propios de los tests, falla y no cuelga), y `stop_worker` sobre una sala nunca abierta.
  - `start_worker` bajo el aplicador.
  - El backoff del worker, con su base y su máximo y no los de las escrituras, y el reparto del primer heartbeat sobre todo el intervalo (de la tercera ronda).
  - La API: `start_worker` sobre una sala cerrada, dejar el worker que corre, reemplazar el que se detuvo, el modo que pone `set_freshness`, el que arranca y el de la configuración.
  - `status`: el estado de la conexión (siempre verdadero, o sin anotarse), el worker y si corre.
  - La configuración: cada condición, y 60 s exactos.
  - La pausa de la subida, que pasó adentro de `upload` (la mata `snapshot_tests.rs`), y un heartbeat del worker con un plazo propio (lo mata el test de HTTP/2).
  - Dos dependen de una carrera entre tareas:
    - esperar la transferencia después de cortarla: sobrevive, sin un test determinista;
    - que los contadores no cambien mientras se lee la cuenta de referencias: la mata el test de estrés con varios hilos en 4 de 5 corridas (en la de `run5.log`, sobrevivió; al revisor de la tercera ronda, en 3 de 3). El orden entre tomar una referencia y contarla es de la misma clase, sin un test que lo distinga.
  - Sobre la primera: Con el planificador de los tests, una tarea cortada mientras espera se suelta antes de que retome quien espera que el worker termine, así que sin esa espera la cuenta de referencias da lo mismo. La espera es lo que pide DC28 ("espera que terminen") y lo que la garantiza en un runtime de varios hilos, donde la tarea puede estar corriendo en otro hilo al cortarla; no hay un test determinista que la distinga. La segunda, y el orden, son los que dice *Qué entró*.
  - La época de la subida (la del heartbeat que la pidió): pasa sin cambios del heartbeat al `upload` de 4.2, cuyo test con la sala recreada después de la designación (`a_room_created_again_after_the_designation_gets_no_snapshot_of_the_old_one`) cubre el camino público; en el del worker, la mutación que la cambia sobrevive, porque un descarte entre el heartbeat y la tarea de la subida no se puede armar sin retener esa tarea.
  - La primera corrida mostró que el test de los reintentos fallaba a veces: con el primer heartbeat al azar, dos intervalos no siempre alcanzaban para ver el octavo. Esperaba poco, y mataba mutaciones ajenas. Ahora espera tres intervalos, y pasa 30 de 30 veces; las mutaciones que mataba se corrieron otra vez. En esa corrida, una mutación colgaba un test pausado (la espera de que la sala falle cerrada no tenía tope): ahora lo tiene.
- **Interacciones revisadas.**
  - Las escrituras, que hacen sus propios heartbeats: una escritura con la réplica detrás del log llama al `heartbeat` público, que transfiere adentro, con el mismo lock de las transferencias que el worker; la que llega segunda encuentra el snapshot instalado y no baja nada. Lo prueba el test del sync durante la bajada del worker, que toma el mismo camino (`snapshot`).
  - El `heartbeat` público que la aplicación llama a la vez: los dos renuevan el lease, y si los dos suben, suben uno por vez, por el lock, como dos heartbeats públicos a la vez (de 4.2, `a_heartbeat_uploads_once_and_two_upload_one_at_a_time`). El público sigue igual: el refactor deja su orden (subir, una vez por llamada, antes de bajar), su `again` por llamada y la pausa de la subida fallida, que pasó adentro de `upload` (la mutación la mata un test de 4.2).
  - El lock de las transferencias: con test (el sync durante la bajada espera y no baja nada, y la bajada es una sola durante tres *leases*).
  - La designación y su plazo de 60 s en el servidor: el worker sube en el momento en que el ack lo designa, en su tarea, y el intervalo es menor que 60 s por configuración (con tests: el designado sube sin ningún paso, y la configuración).
  - El registro que hace el worker: va por `register`, con el lock de los registros, como el de los pasos y el de `open_room` (con test: la sala sin red se registra con el primer heartbeat con respuesta).
  - Un descarte en curso: el heartbeat del worker lee la réplica bajo el aplicador, como los pasos, y `start_worker` también, porque un descarte la deja fuera del cliente mientras dura. Un `RoomReset` en el registro del worker descarta la réplica como en los pasos (con test, con un descarte que falla). La subida que pide un ack lleva la época de ese heartbeat: después de un descarte no sube nada.
  - El reloj pausado y las tareas de `spawn_blocking` del storage: Tokio no adelanta el reloj pausado mientras corre una tarea de `spawn_blocking` (las del motor en disco y las del pool de E/S del servidor, que usa la misma), así que un intervalo no pasa en medio de una operación del storage. Los tests del ritmo usan motores en memoria, para que el instante de cada heartbeat sea exacto.
  - HTTP/2: un heartbeat no espera a una transferencia en su tarea, pero comparte la conexión con ella. Sus frames salen entre los de la transferencia (HTTP/2 los intercala por stream), y uno que no avanza en 60 s lo corta el transporte, se reintenta con backoff y va por un cliente HTTP nuevo; el *lease* (90 s) aguanta un heartbeat fallido con el intervalo por defecto (30 s). Sin test propio: con la red local, el cable no se llena.
  - El cliente HTTP nuevo después de un corte: los heartbeats del worker, que el núcleo no corta, son los que dejan atrás una conexión HTTP/2 que dejó de responder, y las escrituras, que sueltan su pedido a su plazo, no (con test).
  - La renovación del token (Lote 3): el heartbeat del worker va por el mismo envío, así que un `Unauthorized` renueva; sin token nuevo, el heartbeat falla con `Network`, espera el backoff y el siguiente le pide otro al proveedor.
  - La baja al fallar cerrada: el heartbeat del worker que hace fallar cerrada la sala manda la baja una vez, como los pasos, y el worker se detiene; uno detenido no manda nada más.
  - `close_room` y `shutdown`: detienen el worker antes de cerrar la sala (con test); un `start_worker` a la vez que un cierre puede arrancar uno sobre la sala que se cierra, que se detiene en su primer paso con `RoomClosed`.
  - Soltar el cliente: el worker lo tiene por un `Weak`, y su canal de parada se cierra con la sala cuando el cliente se suelta. Con una transferencia en vuelo, que lo retiene, el worker lo nota en su heartbeat siguiente, corta la transferencia y el cliente se suelta (con tests, con y sin transferencia; lo encontró la revisión de delta).
  - INV-K15: por sala con worker, su tarea, su modo y su último error, y a lo sumo una transferencia; por cliente, dos contadores de las referencias que toman y sueltan las tareas de sus workers; nada crece por heartbeat.
  - De la revisión de delta:
    - El aplicador, que comparten el heartbeat y las fases de una transferencia bajo él (crear el snapshot que sube e instalar el que bajó): el heartbeat las espera, como espera todo cambio de la réplica, porque lee la réplica bajo el aplicador para no ver un cambio a medias (leerla sin él daría `RoomClosed` en medio de un descarte, y detendría el worker). Una réplica tan grande que una de esas fases dure más que el *lease* lo pierde hasta su heartbeat siguiente: va a `INVARIANTES.md` (INV-K7, *Límite aceptado*) y a `ARCHITECTURE.md`. El `heartbeat` público de 4.2 ya hacía lo mismo, y más (bajaba y subía adentro). No es inevitable: con la réplica abierta, el heartbeat podría leer la época y el registro sin el aplicador, y el head del storage como lo lee `status`, a costa de leerlo en medio de una instalación (lo señaló la segunda ronda). Queda como límite, y lo decide el dueño del proyecto.
    - Los workers de las otras salas del mismo cliente: mientras corren un paso, retienen el cliente, y la cuenta las incluye (con test, de la segunda ronda). Un cliente soltado que siguiera vivo retendría además el lock de `FileMetadata`, y otro `Client::open` sobre el mismo directorio fallaría.
    - `start_worker` a la vez que `close_room`: el worker que arranca sobre la sala que se cierra se detiene en su primer paso con `RoomClosed` si sigue cerrada, y sigue si la aplicación la reabrió antes.
    - Abrir otro cliente sobre el mismo directorio después de soltar uno sin `shutdown` (de la tercera ronda, con test): el lock de `FileMetadata` sigue tomado mientras un paso de un worker está en vuelo (hasta el plazo del transporte o del proveedor de tokens, 60 s) o una transferencia espera el heartbeat siguiente de su worker. Lo que se promete es que `shutdown` lo suelta (§11, *Durable metadata*); queda escrito en §11 y en INV-K11.
    - `start_worker` sobre un worker que se está deteniendo (entre el `break` de uno que encontró `RoomClosed` y el fin de su tarea, mientras espera su transferencia cortada): `is_running` sigue dando verdadero y el `start_worker` no hace nada, y la sala queda con un worker detenido, que `status` muestra. Solo pasa si la aplicación reabre la sala justo cuando un paso la encontró fuera del cliente; `start_worker` otra vez lo arranca.
    - Dos llamadores que detienen el worker a la vez (`stop_worker` y `close_room`, o `shutdown`): el segundo no encuentra el worker y no espera al que el primero detiene. Hoy no hace daño, porque todo lo que toca el storage se vuelve a mirar bajo el aplicador, pero importa para el último `Ack` del Lote 6: pasa a las interacciones de ese lote.
    - El estado de la conexión con un pedido que su llamador corta (una escritura que vence su plazo, un heartbeat que corta `stop_worker`): no se anota, porque no tuvo respuesta ni falla del transporte. Los heartbeats del worker, que el núcleo no corta, lo anotan dentro del plazo del transporte. Para el `Offline` de DC36 (Lote 8 de 4.4): sin worker, o con escrituras que siempre vencen antes que el transporte, el estado puede quedar en verdadero.
  - La fachada de 4.4: guarda el `Client` adentro de un `Arc` propio, así que su Lote 8 tiene que guardarlo como `Arc<Client>` para llamar a `start_worker`. El estado de la conexión que usa para `Offline` está en el núcleo (`State::connected`), que lo anota también sin worker; el lote solo lo muestra con el worker, y la fachada lo tiene que leer solo con worker, como pide DC36. Una respuesta que no decodifica (la página de error de un proxy) cuenta como respuesta.
- **Revisión de delta, primera ronda: no verde.** Un Medio y seis Bajos:
  - Medio: si la aplicación soltaba el cliente durante una transferencia, la tarea de la transferencia lo retenía, y el worker seguía mandando heartbeats hasta que terminara (sin plazo total, DC27), contra DC28 y lo que decían INV-K15 y §11. Ahora el cliente cuenta las transferencias que lo retienen y el worker lo nota en su heartbeat siguiente, con el test de la revisión.
  - Bajo: el heartbeat espera las fases de una transferencia bajo el aplicador: es inherente (lee la réplica bajo el aplicador), y va como límite aceptado de INV-K7 y a §11.
  - Bajo: cuatro reglas sin test que las matara: el intervalo desde el comienzo, `close_room` antes de cerrar el storage, `start_worker` bajo el aplicador (ahora con tests, y `shutdown` también) y la época de la subida (argumentada arriba).
  - Bajo: quitar el `continue` del fin de una transferencia colgaba un test con el reloj pausado: el transporte de prueba tiene ahora un tope de mensajes, y la mutación falla.
  - Bajo: el test de los reintentos fallaba en falso una vez en 4096: ahora son doce reintentos.
  - Bajo: dos llamadores que detienen el worker a la vez: va a las interacciones del Lote 6.
  - Bajo: el estado de la conexión con un pedido que su llamador corta: va a las interacciones, para el `Offline` de 4.4.
- **Revisión de delta, segunda ronda: no verde.** Verificó los arreglos de la primera, y encontró un Medio y cuatro Bajos:
  - Medio: el arreglo del Medio de la primera ronda contaba solo las transferencias, y la referencia que cada worker toma durante su heartbeat contaba como de la aplicación. Con dos o más salas cuyos heartbeats se solapan (una red lenta, o que descarta paquetes), los workers de un cliente soltado se retenían entre ellos. Ahora toda referencia de una tarea de un worker pasa por el guard, con el test de dos salas.
  - Bajo: un falso positivo de carrera, si entre leer la cuenta de referencias y releer el contador una transferencia terminaba y otra empezaba (el contador volvía al mismo valor): ahora son dos contadores que solo crecen.
  - Bajo: reglas sin test: `stop_worker` sobre una sala nunca abierta (ahora con test); que una transferencia que sale bien borre el error, que es equivalente (el heartbeat que la lanzó ya lo borró); y el orden entre tomar una referencia y contarla, de carrera.
  - Bajo: soltar el canal de parada después de esperar la tarea colgaba dos tests pausados que esperan `stop_worker` o `close_room` sin tope: ahora tienen uno.
  - Bajo: una línea de un docstring de más de 100 caracteres.
  - Además, que `again` se parte en el worker y que la precisión de B1 no es inevitable (en *Desvíos* y en las interacciones).
- **Revisión de delta, tercera ronda: verde.** Verificó los arreglos (el guard es correcto con cualquier intercalación entre hilos, y no hay otra referencia fuerte al cliente), y con tests de estrés en varios hilos: el cliente soltado se suelta en milisegundos con 20 salas que fallan sin parar, y ningún worker se detiene en falso con la aplicación que clona y suelta su referencia. Sus siete Bajos se arreglaron en el lote:
  - el backoff del worker no estaba atado a `retry_base` y `retry_max` (el de las escrituras pasaba el test): ahora el de las escrituras es más largo;
  - el reparto del primer heartbeat no estaba probado (uno en el 1% del intervalo pasaba): ahora 16 salas, más de un cuarto del intervalo;
  - la relectura de los contadores se podía matar con un test de estrés en varios hilos: entró el test;
  - INV-K11 seguía diciendo que detener el worker antes de cerrar era del Lote 6;
  - una línea de las interacciones hablaba todavía de contar solo las transferencias;
  - faltaban dos interacciones: abrir otro cliente sobre el directorio de uno soltado sin `shutdown` (con test) y `start_worker` sobre un worker que se está deteniendo;
  - y cuatro precisiones que no estaban en *Desvíos*.
- **Desvíos.**
  - Precisiones de DC28, que DC28 no decía:
    - `status` muestra el worker en un `Option<WorkerStatus>`, y el estado de la conexión va adentro: sin worker no hay estado, como dice DC36 para `Offline`.
    - El estado de la conexión es si el transporte devolvió una respuesta, decodifique o no.
    - Una transferencia que falla no tiene backoff propio: la vuelve a pedir el heartbeat siguiente cuyo ack la pide (un intento por heartbeat, como DC29 dice del `Config` de un snapshot mayor que el tope).
    - Un worker que se detuvo solo queda en `status`, sin correr y con su error, hasta que se lo arranca otra vez o se lo detiene.
    - El heartbeat del worker tiene su propio `again` (registrarse otra vez una vez), y su transferencia otro; en el `heartbeat` público es uno por llamada.
    - `stop_worker` toma `&self`, no `self: &Arc<Self>` como los otros dos: no lanza nada.
    - El worker no manda un heartbeat después de terminar una transferencia (el `heartbeat` público sí): lo manda en el intervalo.
    - `stop_worker` sobre una sala que el cliente abrió y cerró da `Ok` (no hay nada que detener), y sobre una que nunca abrió, `RoomClosed`.
    - `start_worker` sobre una sala que falla cerrada da `Ok`, y el worker se detiene en su primer heartbeat con `FailedClosed`.
  - `INVARIANTES.md` actualiza también INV-K11: detener el worker antes de cerrar la sala del storage, que el plan dejaba para el Lote 6, ya entró en este lote; el Lote 6 conserva el último `Ack`.
  - El orden completo del cierre (el último `Ack`) sigue siendo del Lote 6, pero `close_room` y `shutdown` ya detienen el worker antes de cerrar la sala.
  - Los tests observan el lease por las cuentas de `GetMetrics` (atrasados y conectados), no por `GET /admin/rooms/{id}`: dan lo mismo.
  - El límite de las fases bajo el aplicador (INV-K7, *Límite aceptado*), de la revisión de delta: lo tenía ya el `heartbeat` de 4.2, y queda escrito; el dueño del proyecto lo puede revisar.
  - Unas 350 líneas de producción contra 200 estimadas, más que las ~300 de `METODOLOGIA.md` §4: unas 300 en la primera versión, y unas 50 de los arreglos de la revisión de delta (el cliente soltado mientras las tareas de los workers lo retienen). Partirlo dejaba el worker sin lo que lo detiene, que DC28 pide con él.
  - `INVARIANTES.md` toma la versión 28: la 25 y la 26 las tomaron los Lotes 3 y 4 de 4.4, y la 27, el Lote 7 de 3.12.
- **Al backlog** (de las pruebas largas del cierre; Bajo: `INVARIANTES.md` §1.2):
  - **DEF-198 · Bajo · (b) · K1.**
    - **Qué pasó:** con el árbol integrado sobre `main` (con el Lote 4 de 4.4 y el Lote 7 de 3.12), una corrida de `cargo test -p zemdb-client --release -- --ignored` falló una vez, en `no_reply_in_place_of_the_servers_panics_a_client_over_many_long_simulations`, con la semilla 11 (esquema `Random`, con fuzzing). Una réplica, en el head 132, tenía `+0.0` (`F0000000000000000`) en una columna float que no es clave de la tabla 0, donde el oráculo, por el log, tenía `-0.0` (`F8000000000000000`). Para el sistema son el mismo valor (`Value` los compara iguales), pero el oráculo compara los bits, y el storage los guarda. El otro test largo falló solo por el contador de pánicos, que comparten.
    - **Lo que se probó:** la semilla sola pasó 3 de 3; las simulaciones completas, 2 de 2 en el árbol del lote y 3 de 3 sobre `main` sin el lote (antes de integrar, 3 de 3). El diff del lote no toca los valores, el storage, los snapshots ni el aplicador, y la simulación no arranca workers, así que el camino que pierde el signo ya está en `main`. Un `INSERT` y un `UPDATE` sobre un `0.0` con `-0.0`, y un snapshot de las dos filas instalado en otro motor, conservan los bits, en memoria y en disco: no es ninguno de esos caminos solos.
    - **Fix:** encontrar el camino. Candidatos: una secuencia que depende de los tiempos (el reparto de los pedidos entre los clientes, la poda, las respuestas del fuzzing), con un snapshot, un catch-up o una restricción de por medio; o el oráculo de la simulación.
    - **Test:** la simulación con la semilla 11 hasta que vuelva a fallar, con un registro de los pasos de la réplica que diverge; después, un test determinista del camino.
    - El log de la corrida quedó en el scratchpad de la sesión, fuera del repositorio.

### Lote 5 · Los modos `periodic` y `manual` (`zemdb-client`) · ✅ Hecho
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

**Hecho** (12 de Noviembre de 2026).
- **Qué entró.**
  - En `periodic`, y en `live` hasta que el worker abra el stream (Lote 7), un heartbeat del worker cuyo ack informa un head mayor que el de la réplica lleva a un sync. No sincroniza una réplica que espera un snapshot: la bajada que pide el mismo ack instala uno, y el heartbeat siguiente sincroniza desde él (un sync suyo fallaría con `Network` mientras no hay uno anunciado, o esperaría la bajada del worker).
  - El sync del worker es el de los pasos sin su `Ack` ni su heartbeat (una variante interna de `sync`): termina cuando un lote sin `has_more` deja la réplica en su head. El `sync` público y los syncs de las escrituras siguen mandando el `Ack`. El cursor del servidor avanza con el `from_seq` de cada `Sync`, así que con los syncs del worker solos queda un sync detrás de la réplica (el `Ack` periódico es del Lote 6).
  - Corre en una tarea propia, como las transferencias, y retiene al cliente por la referencia contada del Lote 4. A lo sumo uno por sala: un heartbeat que encuentra el head adelante mientras corre marca uno más (una marca, no una cola), que arranca cuando termina, y los heartbeats siguen mientras tanto.
  - Un lote de ese sync con `snapshot_wanted` se confirma con un heartbeat en la tarea de las transferencias, salvo que corra una: el `heartbeat` público, que sube si su ack designa al cliente y baja lo anunciado.
  - En `manual`, ningún heartbeat lleva a un sync, y el worker sigue subiendo y bajando. El modo se lee en cada heartbeat, así que `set_freshness` vale desde el siguiente, y una marca pendiente no arranca su sync si el modo pasó a `manual` mientras tanto.
  - El resultado del sync cuenta como el de cualquier paso: `FailedClosed` o `RoomClosed` lo detienen en el momento; otro error queda en `status`, y el heartbeat siguiente que encuentra el head adelante lo vuelve a pedir; uno que sale bien borra el error.
  - Detenerlo corta el sync y la transferencia, y espera los dos. Un sync que termina con uno más marcado después de que la aplicación soltó el cliente no arranca otro: el worker se detiene.
  - Unas 79 líneas de producción netas (116 agregadas y 37 quitadas, sin comentarios ni líneas vacías), contra 150 estimadas: el worker lanza sus tareas con dos funciones (la transferencia y el sync), que comparten la espera y el corte del final.
- **Tests.** `tests/worker_tests.rs` (15 más, 34 en total), con el reloj pausado:
  - En `periodic` y en `live`, lo que escribe otro cliente llega a la réplica en a lo sumo un intervalo, sin llamar a `sync`: un sync por cada head que un heartbeat encontró adelante, y ninguno con la réplica al día.
  - Los syncs del worker no mandan `Ack`, y el `sync` de la aplicación sí.
  - En `manual`, lo que escribe otro cliente no llega en tres intervalos con heartbeats, sin ningún sync; llega con un `sync` de la aplicación y con el catch-up de una escritura; vuelto a `periodic` con `set_freshness`, el heartbeat siguiente sincroniza.
  - En `manual`, el designado sube su snapshot y el atrasado lo baja.
  - Tres heartbeats con el head adelante mientras el sync del worker está retenido no mandan otro sync; cuando termina, uno más, desde donde dejó la réplica, y ninguno después. Si el modo pasa a `manual` mientras tanto, la marca no arranca su sync (de la revisión de delta).
  - Un lote del sync del worker con `snapshot_wanted`: el designado sube en el momento, sin esperar al heartbeat siguiente. El ack de su primer heartbeat está fabricado como lo daría el servidor un momento antes de la designación (con el head adelante, sin designarlo): con el servidor solo, el escenario armado ya traía la designación en ese ack.
  - El `sync` de la aplicación mientras el del worker está retenido: los dos terminan, la réplica queda en el head y el worker sin error.
  - Un sync del worker sin respuesta deja el worker andando con `Network`, y el heartbeat siguiente lo vuelve a pedir y lo borra; un sync que sale bien borra también el error de un heartbeat que falló mientras corría (de la revisión de delta); uno cuyos lotes traen una operación que no valida, también después de registrarse otra vez, hace fallar cerrada la sala y detiene el worker antes del heartbeat siguiente.
  - Detenerlo con un sync en vuelo lo corta (la cuenta de referencias del `Arc`); el de un cliente soltado con un sync en vuelo termina en su heartbeat siguiente y el cliente se suelta; y si el sync termina con uno más marcado mientras el worker de otra sala retiene al cliente con su heartbeat en vuelo, el worker se detiene sin mandarlo, y el cliente se suelta con el heartbeat siguiente del otro.
  - De las interacciones: una escritura cuya secuencia aplica el sync del worker antes de su `CommitAck` termina con la decisión de la réplica (`RowMissing`); un sync del worker detrás del log (`BehindCompaction`) se registra y baja el snapshot anunciado una sola vez, mientras los heartbeats siguen y la bajada que piden espera la suya; un cambio de esquema durante un sync del worker lo registra con su cursor y aplica el lote con el esquema nuevo.
  - Los 19 de antes siguen pasando sin cambios (el de la bajada larga verifica, además, que una réplica que espera un snapshot no sincroniza por el head). La suite del worker pasó 20 de 20 veces seguidas.
- **Mutaciones inversas: 34 de las 36 las mató algún test** (los logs, en el scratchpad de la sesión: `mut5/run1.log`, la corrida entera; la de la que mató el test reescrito; y `mut5/run3.log`, las de los arreglos de la revisión de delta, con las reglas que cambiaron).
  - El sync sin `Ack`: el del worker, la bandera con que lo llama, y el `Ack` del `sync` público y de los syncs de las escrituras (la de las escrituras la matan dos tests de `write_tests.rs`).
  - Cuándo un heartbeat lleva a un sync: el head estrictamente mayor, la réplica que espera un snapshot (la mata el test de la bajada larga del Lote 4), nunca, siempre, y que el ack del worker pase el resultado.
  - Los modos: `manual` no sincroniza, `live` sí, el modo se lee en cada heartbeat, y una marca no arranca su sync en `manual`.
  - La tarea: el sync en su tarea y no adentro del ciclo, a lo sumo uno, la marca, tomarla una vez y lanzar el que marca.
  - La confirmación: que el lote la pida, lanzarla, el heartbeat de la tarea de las transferencias, y que ese heartbeat suba (uno sin la subida no la confirma).
  - El resultado del sync: que cuente, que `FailedClosed` lo detenga, y que uno que sale bien borre el error.
  - El cliente soltado al terminar un sync con uno más marcado: la primera corrida la dejó viva, porque el sync suelta su referencia antes de que el worker lo vea terminar, así que sin otra tarea que retenga al cliente la referencia débil ya no sube. El test se reescribió con otra sala cuyo heartbeat queda en vuelo, y la mata.
  - Detenerlo: cortar el sync.
  - Las reglas del Lote 4 que este lote movió: a lo sumo una transferencia, cortarla, que cortar aborte, preguntar si el cliente se soltó, la bajada y la subida que pide el ack, y que el fin de una transferencia no mande un heartbeat.
  - Dos sobreviven, de la misma clase que la que sobrevivió en el Lote 4: esperar el sync y la transferencia después de cortarlos. Con el planificador de los tests, una tarea cortada mientras espera se suelta antes de que retome quien espera al worker, así que sin la espera la cuenta de referencias da lo mismo; es lo que pide DC28 ("espera que terminen") y lo que la garantiza en un runtime de varios hilos.
- **Interacciones revisadas.**
  - Las escrituras y sus syncs: siguen igual, con su `Ack`. El sync del worker aplica por el aplicador, que saltea lo que la réplica ya aplicó y guarda para las escrituras en vuelo lo que decidió, como cualquier sync (con test: la escritura cuya secuencia aplica el worker termina con su decisión, y con la mutación que quita el `Ack` de las escrituras fallan dos tests de `write_tests.rs`).
  - El `sync` público: corre a la par del del worker, cada uno con su cursor leído bajo el aplicador, y el lote atrasado de uno no es una violación (con test).
  - La bajada que hace un sync cuando la réplica espera un snapshot anunciado: el sync del worker que se registra detrás del log baja el snapshot dentro de su tarea, con el lock de las transferencias, y la bajada que pide un heartbeat mientras tanto lo espera y lo encuentra instalado (con test). Una réplica que ya espera un snapshot no arranca un sync del worker.
  - Un cambio de esquema durante el sync del worker: no aplica el lote, se registra con su cursor (el lock de los registros, el mismo del heartbeat del worker y de `open_room`) y aplica con el esquema nuevo (con test).
  - El heartbeat de la confirmación: es el `heartbeat` público en la tarea de las transferencias, con el lock de las transferencias; una confirmación que llega mientras corre una transferencia no se guarda: la que corre es una bajada (la réplica espera un snapshot y no puede subir) o una subida, y si no, el heartbeat siguiente vuelve a traer la designación dentro de los 60 s del servidor (el intervalo es menor).
  - El cursor del servidor sin `Ack`: el servidor toma el `from_seq` de cada `Sync` como cursor, así que con el worker solo queda en el `from_seq` del último sync, un sync detrás del head de la réplica, y sin límite mientras nadie escribe, hasta el `Ack` periódico del Lote 6. INV-K5 vale (el cursor sigue siendo un head que la réplica alcanzó), y la poda por cursores lo ve atrasado, un costo. Pero una poda por TTL o por cuota que pasa ese cursor deja al cliente `Bootstrapping` en cada heartbeat, aunque su réplica esté en el head, y no puede ser designado: otro cliente atrasado puede quedar sin snapshot mientras la sala calla, hasta que el cliente sincronice con `Ack` (lo encontró la revisión de delta, con un test; no es una regresión, porque sin este lote el worker no sincronizaba). Queda escrito en INV-K7, y el escenario pasa a los tests exigidos del Lote 6, que lo cierra.
  - El orden del `select!` del worker, que elige al azar entre las tareas que terminaron y el heartbeat: una transferencia que terminó y que el worker todavía no recogió hace que se pierda la confirmación de un sync que termina a la vez (la trae el heartbeat siguiente, dentro de los 60 s de la designación); un sync que terminó sin recoger hace que un heartbeat marque uno más, que no trae nada (de la revisión de delta).
  - D78 (INV-K1): el heartbeat sigue comparando su head con el de la réplica antes que nada, y uno menor lleva a registrarse, en cualquier modo.
  - El estado de la conexión y la renovación del token: el sync del worker va por el mismo envío que los pasos, así que anota la conexión y renueva ante `Unauthorized`.
  - HTTP/2: el sync del worker comparte la conexión con los demás pedidos, como cualquier sync.
  - Detenerlo con `stop_worker`, `close_room` y `shutdown`: cortan el sync en su próximo `.await` y lo esperan; los cambios de la réplica que lanzó corren hasta el final en sus tareas (DC21), así que no queda nada a medias (con test).
  - El cliente soltado: el sync retiene al cliente como la transferencia, y el worker que encuentra que solo sus tareas lo retienen se detiene en su heartbeat siguiente, o al terminar el sync si tiene uno más marcado o una confirmación (con tests).
  - INV-K15: por sala con worker, a lo sumo un sync en vuelo y una marca; nada crece por heartbeat.
  - La fachada de 4.4: no cambia; su Lote 8 arranca el worker, y `sync` sigue público para la aplicación.
- **Revisión de delta, primera ronda: verde.** Ningún Alto ni Medio; siete Bajos, arreglados en el lote:
  - el cursor del servidor sin `Ack` (arriba, en las interacciones): la nota decía "un costo, no una ruptura", y no lo es para este lote solo; va a INV-K7 y a los tests exigidos del Lote 6;
  - que un sync que sale bien borre el error no tenía test: ahora lo tiene;
  - una marca pendiente de antes de pasar a `manual` arrancaba su sync, sin test y un poco contra el modo: ahora no lo arranca, con test;
  - el docstring de la referencia contada y el de `stop_worker`, y *The task* de `ARCHITECTURE.md`, no nombraban el sync;
  - "la transferencia la pone al día" decía de más: la bajada deja la réplica en el snapshot, y el resto llega con el sync del heartbeat siguiente;
  - dos detalles de texto (una coma de la fila de INV-K7 y la cuenta de los tests de antes);
  - y las dos mutaciones de la espera después del corte, conocidas.
  
  Además, el orden del `select!` (arriba, en las interacciones).
- **Desvíos.**
  - Precisiones de DC24 y DC28 que el plan no decía:
    - Una réplica que espera un snapshot no sincroniza por el head: la bajada instala uno, y el heartbeat siguiente sincroniza desde él.
    - Un sync del worker que falla no tiene backoff propio: lo vuelve a pedir el heartbeat siguiente que encuentra el head adelante (como las transferencias del Lote 4).
    - El modo se lee en cada heartbeat, y una marca pendiente no arranca su sync si el modo pasó a `manual` (de la revisión de delta).
    - Una confirmación que llega mientras corre una transferencia no se guarda (arriba, en las interacciones).
    - El heartbeat de la confirmación es el `heartbeat` público: sube una vez y baja lo anunciado, como el del `sync` público.
    - Un sync que sale bien borra el error del worker, como cualquier paso.
  - `INVARIANTES.md` toca también INV-K17 (el sync como paso del worker que lo detiene), que el plan no listaba para este lote, y toma la versión 31: la 30 la tomó la revisión de cierre de la Fase 3.12.
  - El test de `snapshot_wanted` fabrica el ack del primer heartbeat (arriba, en los tests).

### Lote 6 · El `Ack` periódico, detener el worker y los endurecimientos (`zemdb-client`) · ✅ Hecho
**Objetivo:** que el cursor del servidor avance sin un `Ack` por sync, y que cerrar con el worker no pierda nada (DC25, DC28, DC31).

**Archivos:** el worker; `src/client.rs` (`close_room`, `shutdown`, el registro y el head visto); `tests/worker_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** el `Ack` con cada heartbeat si el head se movió, y el último al detenerse, con su plazo; `close_room` y `shutdown` detienen el worker antes de cerrar la sala, en el orden de DC28; DEF-196 y DEF-192 (DC31).

**Tests exigidos.**
- Un worker en `periodic` que sincroniza muchas veces manda un `Ack` por intervalo, no uno por sync, con el head de la réplica; uno sin cambios no manda ninguno.
- `close_room` y `shutdown` mandan el último `Ack` si el head se movió (en `shutdown`, el de varias salas a la vez) y, sin red, no tardan más que su plazo. Después, ninguna tarea del worker toca la sala, y el motor en disco la reabre con todo lo aplicado (INV-K11).
- Un `AckConfirmed` con un head menor que el confirmado registra la réplica con su cursor.
- Un lector que solo sincroniza con su worker, en una sala que no recibe escrituras, cuyo log se poda por TTL más allá del cursor que le dejó su último sync: sigue `Connected` en el servidor y puede ser designado, así que un cliente atrasado recibe un snapshot sin que el lector llame a `sync` (de la revisión de delta del Lote 5: sin el `Ack` periódico, queda `Bootstrapping` y el atrasado sin snapshot).
- DEF-196: una réplica que espera un snapshot que nadie anuncia (un `Registered` fabricado, detrás del log) deja de esperar con el registro de los 10 heartbeats. DEF-192: después de un `HeartbeatAck` fabricado con un head más allá del servidor, el siguiente, con el head real, registra la réplica, y una escritura posterior termina con la decisión de la réplica.

**Interacciones a revisar:** la poda por cursores (el cursor del servidor avanza a lo sumo un intervalo tarde, INV-Y6); la poda por cuota o por TTL de un log que alcanza el cursor guardado de un cliente en `live`, que se atrasa hasta un intervalo: el heartbeat lo deja `Bootstrapping` en el servidor y abre una demanda de snapshot que la réplica no necesita (un costo, no una ruptura); el `last_ack_seq` de los commits y el `from_seq` de los syncs; la baja al fallar cerrado; apagar el runtime después de `shutdown` (D93); lo que retienen las escrituras (DEF-192 no cambia el `seen` ya anotado); dos llamadores que detienen el worker a la vez (`stop_worker` y `close_room`, o `shutdown`), de los que el segundo no espera al worker que detiene el primero, así que podría cerrar la sala antes del último `Ack` (de la revisión de delta del Lote 4).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K5, K11, K2, K7.

**Entrega:** `feat(client): acknowledge the head every interval, stop the worker before closing, and two hardenings of the audit (Fase 4.3, Lote 6)`.

**Hecho** (13 de Noviembre de 2026).
- **Qué entró.**
  - El `Ack` del worker, en cualquier modo, después de cada heartbeat que sale bien y antes de lanzar la transferencia o el sync que pide su ack: si el head de la réplica, leído bajo el aplicador, pasó del último que el servidor tomó como cursor del cliente. Ese head es el cursor del registro que la registró (el servidor guarda el cursor del registro, o 0 sin cursor) o el de un `AckConfirmed`, del worker o de un `sync`. La respuesta se valida como la del `Ack` del `sync`, que ahora es la misma función.
  - Cada 10 heartbeats que salen bien, el `Ack` va aunque el head no se haya movido, salvo que la réplica espere un snapshot: el servidor puede haber tomado un cursor menor desde el último (un registro que le llegó tarde, de una llamada cortada, o un roster que perdió en una caída), y el cliente no lo ve (de la revisión de delta).
  - Un `AckConfirmed` con un head menor que el confirmado o que el head visto, `InvalidSequence`, `ClientNotRegistered` y `BehindCompaction` registran la réplica en el mismo paso, con su cursor. El registro presenta el head, así que no hace falta otro `Ack`.
  - Un `Ack` que falla deja el error en `status`, sin el backoff de los heartbeats, y el heartbeat siguiente lo vuelve a mandar. `FailedClosed` y `RoomClosed` detienen el worker, como en cualquier paso.
  - El último `Ack` lo manda la tarea del worker al terminar, después de cortar y esperar su sync y su transferencia, salvo que la aplicación haya soltado el cliente. Es un pedido, con un plazo de 5 s, que no registra nada aunque la respuesta lo pida. Un worker que se detuvo solo no lo manda: su réplica falla cerrada (y no está registrada) o está fuera del cliente.
  - `stop_worker` también manda el último `Ack`, y `shutdown`, que detiene todos los workers a la vez, manda los de todas las salas a la vez.
  - **Dos llamadores.** Detener el worker deja en la sala el fin de su tarea (`Ended`, un `oneshot` que la tarea suelta al terminar, compartido). Un llamador que detiene el worker mientras otro lo detiene espera ese fin, también si al otro lo cortan, porque espera a la tarea y no a la llamada. Un `start_worker` o un `set_freshness` en ese momento también lo espera antes de arrancar otro, así que la sala tiene un solo worker a la vez (de la revisión de delta). Decide con los dos locks tomados, el del worker y el de la detención, como los toma quien detiene, después de mirar la sala bajo el aplicador: una detención que empieza mientras espera el aplicador también se espera (de su segunda ronda). `status` no cambia: la sala queda sin worker.
  - **DEF-196.** El worker cuenta los heartbeats seguidos que dejan la réplica esperando un snapshot que ninguna respuesta anunció. Después de 10, el heartbeat siguiente la deja sin registrar antes de mandarse, así que se registra con su cursor y `Registered` vuelve a decidir la espera con la cola del log. La cuenta vuelve a cero con ese registro y con un heartbeat que no la deja esperando; uno que falla no la cambia.
  - **DEF-192.**
    - El pedido de un sync y el de un heartbeat leen el head visto bajo el aplicador, junto con el cursor. El `Ack` del `sync` lo lee al mandarse, si la época no cambió, y el del worker, con su pedido.
    - Un `SyncBatch`, un `HeartbeatAck` o un `AckConfirmed` con un head menor que ese llevan a registrarse, como D78 (una vez por llamada), y el head visto baja a ese head. El `Registered` que sigue lo sube con el máximo, como cualquier respuesta. Un `SyncBatch` se valida antes (sus operaciones siguen al cursor): uno que no las sigue es `Network`, sin cambiar nada (de la revisión de delta).
    - Las escrituras en vuelo conservan el `seen` que anotaron.
  - Unas 166 líneas de producción netas (216 agregadas y 50 quitadas, sin comentarios ni líneas vacías), contra 150 estimadas; unas 33 son de los arreglos de la revisión de delta, en buena parte `run_worker`, que cambió de forma.
- **Tests.** `tests/worker_tests.rs` (17 más, 51 en total), con el reloj pausado:
  - **Un `Ack` por intervalo.** Un worker en `periodic` que sincroniza muchas veces (de a una operación por `Sync`, nueve en tres intervalos) confirma con cada heartbeat el head que tenía la réplica: uno por intervalo, no uno por sync. Uno sin cambios no manda ninguno en nueve heartbeats; en una sala que nunca cambia, el décimo confirma el head igual, en el instante de ese heartbeat.
  - **Las respuestas que piden registrarse.** Un `AckConfirmed` con un head menor que el confirmado, `ClientNotRegistered`, `InvalidSequence` y `BehindCompaction` registran la réplica con su cursor dentro del mismo intervalo, y no sale otro `Ack`.
  - **Un `Ack` que falla.** Uno sin respuesta deja el worker andando con `Network`, y el heartbeat siguiente lo vuelve a mandar y borra el error. Uno cuya respuesta lleva a un registro con un esquema que no evoluciona hace fallar cerrada la sala y detiene el worker.
  - **El último `Ack` y el disco.** `close_room` lo manda con el head que se movió, y ninguna tarea del worker manda nada después. Sin red, el `Ack` sale y no vuelve; `close_room` tarda el plazo de 5 s y no más, y `shutdown`, con tres salas, también: los `Ack` de las dos salas que se movieron salen en el mismo instante, y la que no se movió no manda ninguno. El motor en disco reabre las salas con todo lo aplicado.
  - **Un solo intento.** El último `Ack` es un pedido: con `ClientNotRegistered` o con un head menor, `stop_worker` no registra nada.
  - **El orden del último `Ack`.** Sale con el sync del worker ya cortado y terminado: mientras su respuesta está retenida, solo el test, la detención y el `Ack` retienen al cliente.
  - **Dos llamadores.** Un `close_room` durante un `stop_worker` cuyo último `Ack` está retenido espera ese `Ack`, también si el `stop_worker` se corta. Un `start_worker` en ese momento también lo espera, sin worker en `status` mientras tanto; el worker nuevo arranca después, y el `close_room` siguiente no manda ningún `Ack` más ni nada después. Lo espera también un `start_worker` que esperaba el aplicador, retenido por una escritura, cuando empezó la detención (el test del revisor, de su segunda ronda).
  - **La confirmación de una réplica descartada.** Un `AckConfirmed` retenido mientras un heartbeat de la aplicación descarta la réplica por `RoomReset` no se toma para la réplica nueva, que se registró sin cursor: el heartbeat siguiente la sincroniza y el otro confirma su head.
  - **Un registro demorado.** Un registro que la red demora, de un `open_room` cortado, llega al servidor después del `Ack` del worker y le baja el cursor. En una sala sin escrituras cuyo log se poda más allá, el décimo heartbeat confirma el head igual: el lector vuelve a estar `Connected`, es designado y un cliente atrasado baja su snapshot (el test del revisor; sin el `Ack` de cada 10 heartbeats, el lector queda `Bootstrapping`).
  - **El lector podado.** Un lector que solo sincroniza con su worker, en una sala sin escrituras, cuyo log se poda por TTL más allá del `from_seq` de su último sync, sigue `Connected` y es designado. Un cliente atrasado baja su snapshot sin que el lector llame a ningún paso. Sin el `Ack`, queda `Bootstrapping`: es el test que pidió la revisión del Lote 5.
  - **DEF-196.** Una réplica que espera un snapshot que nadie anuncia, por un `Registered` fabricado con la cola más allá del log, se registra otra vez después de exactamente 10 heartbeats, también después de un segundo registro fabricado que la deja esperando. La cuenta vuelve a empezar después de 15 heartbeats que no la dejan esperando, y un heartbeat que falla no cuenta ni la reinicia. Además, el test de la bajada larga del Lote 4 verifica que más de 10 heartbeats de una réplica que espera un snapshot anunciado no la registran.
  - **DEF-192.** Después de un `HeartbeatAck` fabricado con un head más allá del servidor, el heartbeat, el sync o el `Ack` siguiente, cuya respuesta trae el head real, registra la réplica, y una escritura posterior termina con la decisión de la réplica. Una escritura que anotó el head dañado lo conserva y termina incierta. Y el `Ack` del worker compara su confirmación con el head visto al mandarse: con el head dañado en el ack del heartbeat del mismo paso, se registra en ese paso (el test del revisor). En un paso sin `Ack`, porque el head ya estaba confirmado, el sync del worker se registra con el lote que trae el head real. Un lote que no sigue al cursor, con un head menor que el visto, es `Network` y no registra nada: se valida antes de usar su head.
  - **Tests del Lote 5 ajustados**, porque el worker ahora manda un `Ack` por intervalo:
    - el de `periodic` y `live`: cada sync va después del `Ack` del head que dejó el anterior, y queda el del último;
    - el de los heartbeats durante un sync retenido: el heartbeat siguiente confirma el head de los dos syncs;
    - el de la marca antes de pasar a `manual`: en `manual` también se confirma;
    - el de `snapshot_wanted`: admite el `Ack` de un segundo heartbeat que entre en el primer segundo;
    - y el de la escritura cuya secuencia aplica el sync del worker: compara solo los syncs, porque el `Ack` del heartbeat siguiente puede llegar antes de que termine.
    
    El test de que los syncs del worker no mandan `Ack` deja de valer, y lo reemplaza el primero de esta lista. El `Ack` del `sync` público lo prueban los tests de `sync_tests.rs`.
  - **Fuera de `worker_tests.rs`**, tres tests de 4.2 fabricaban un servidor que se contradice: un head más allá del real y, después, respuestas del servidor real con un head menor. Con DEF-192, esas respuestas registran la réplica.
    - En `snapshot_tests.rs`, el `Registered` fabricado de `registered_at` lleva un head explícito (el real, en los dos tests que siguen contra el servidor). `a_heartbeat_takes_the_head_the_schema_and_the_snapshot_its_ack_announces` suma un heartbeat real, que muestra el registro y deja el head visto en el del servidor.
    - En `write_tests.rs`, el caso `sync` de `the_head_seen_comes_from_every_reply_of_the_epoch` responde `Unavailable` al lote que sigue al fabricado, para que el head visto siga siendo el del lote fabricado.
  - **Sin depender de cuándo cae el primer heartbeat.** Los tests que actúan después de un sync del worker esperan que la réplica llegue al head (`synced_to`, que mira cada 1 ms) en lugar de dormir un intervalo. Con el primer heartbeat en los primeros 10 ms, el segundo entraba antes y mandaba el `Ack` que el test esperaba del paso siguiente o de la detención (una vez en 20 corridas de la suite, y la revisión de delta los hizo fallar fijando el primer heartbeat a los 5 ms). También las rondas del test del `Ack` por intervalo esperan el sync de la anterior. Con el primer heartbeat fijo a los 5 ms o a 1 ms del final del intervalo pasan todos los del lote; fallan solo el del primer heartbeat al azar, que lo prueba a propósito, y, a los 5 ms, tres del Lote 5 que ya fallaban así antes de este lote (en los desvíos). El log es `mut6/run6.log`.
  - **MSRV.** Con Rust 1.88 pasan los 19 binarios de tests del cliente, y con 1.85 compila el workspace. La suite del worker, con el árbol final, pasó 30 de 30 veces seguidas. En corridas anteriores falló dos veces: una por un test del Lote 5 de los desvíos, y otra por el del `Ack` por intervalo, antes de que sus rondas esperaran el sync.
- **Mutaciones inversas: 32 de las 33 las mató algún test.** El log, en el scratchpad de la sesión: `mut6/run1.log`.
  - El `Ack` periódico: mandarlo; solo si el head pasó del confirmado; leer el confirmado en el pedido; subirlo con la confirmación y ponerlo con el registro; registrarse en el mismo paso; y que su resultado cuente y `FailedClosed` lo detenga.
  - El último `Ack`: mandarlo; que no registre nada; su plazo y el largo del plazo; y no mandarlo con el cliente soltado.
  - Dos llamadores: guardar el fin de la tarea, y que se resuelva recién cuando la tarea termina.
  - El head visto: leerlo en el pedido; compararlo en el sync, en el heartbeat y en el `Ack`, y bajarlo en los tres; y leerlo al mandar el `Ack` del `sync`.
  - DEF-196: 9 y 11 en lugar de 10; reiniciar la cuenta con el registro y con un heartbeat que no deja la réplica esperando; contar los heartbeats; pasar el registro y hacerlo; y contar solo la espera sin un snapshot anunciado (la mata el test de la bajada larga).
  - Las de los arreglos de la revisión de delta (`mut6/run2.log`, `run3.log` y `run4.log`), todas muertas: el `Ack` de cada 10 heartbeats y que no lo mande una réplica que espera un snapshot; que un `start_worker` espere la detención; el head visto en el `Ack` del worker; que un heartbeat que falla no cuente ni reinicie la cuenta de DEF-196; el último `Ack` antes de cortar el sync; la época de la confirmación; la comparación del lote con el head visto en el sync del worker, que baje el head visto, y que vaya después de validar el lote. La que dejaba de mandar el `Ack` de cada 10 heartbeats después de un registro en el mismo paso sobrevivía, y esa guarda salió: un registro en ese paso deja, a lo sumo, un `Ack` de más.
  - Las de la segunda ronda (`mut6/run5.log`), muertas: mirar la detención antes del aplicador, como la primera versión del arreglo, y 11 heartbeats en lugar de 10 (la mata siempre el instante del décimo).
  - Con el primer heartbeat fijo a los 5 ms (la mutación de la revisión de delta) fallan solo el test del primer heartbeat al azar y los tres del Lote 5 de los desvíos.
  - Sobrevive una guarda: el `Ack` del `sync` no compara con el head visto de otra época. Para hacerla fallar, un descarte tendría que entrar entre el último lote aplicado y el `Ack`, sin un `.await` del sync en el medio, y no hay un test determinista que lo ubique ahí. Sin la guarda, ese `Ack` podía pedir registrarse por un head de la réplica nueva y gastar el registro de la llamada. Son de la misma clase las guardas de época de `saw_below` y de `waits_for_none`, que encontró la revisión de delta: un descarte entre una respuesta y su uso, sin un `.await` en el medio.
- **Interacciones revisadas.**
  - **La poda por cursores (INV-Y6).** El cursor del servidor de un cliente con worker avanza a lo sumo un intervalo tarde: el `from_seq` de cada `Sync`, y el `Ack` del heartbeat siguiente con el head que dejó. La poda por cursores espera eso.
  - **La poda por cuota o por TTL** que alcanza el cursor guardado de un cliente en una sala que recibe escrituras lo deja `Bootstrapping` hasta el `Ack` siguiente, y abre una demanda de snapshot que no necesita. Es un costo, escrito en INV-K7 y en *Acknowledgements* de §11. En una sala que calla, el cursor queda en el head (con test).
  - **El `last_ack_seq` de los commits y el `from_seq` de los syncs** mueven el cursor del servidor sin confirmarlo, y el worker no los cuenta como confirmados. Puede mandar un `Ack` del head que el servidor ya tiene, a lo sumo uno por intervalo: en una sala que cambia, el `Ack` sale antes del `Sync` que presenta el mismo head (con test, el de `periodic` y `live`). Es un pedido más por intervalo.
  - **La baja al fallar cerrado.** Una réplica que falla cerrada no está registrada. El `Ack` periódico la registraría, y el registro devuelve `FailedClosed`, que detiene el worker (con test, desde el `Ack`). El último `Ack` no registra nada.
  - **Apagar el runtime después de `shutdown` (D93).** `shutdown` espera el fin de las tareas de los workers, con sus últimos `Ack` (5 s a lo sumo), y recién entonces cierra las salas. Después no manda nada, y el motor en disco las reabre con todo (con test).
  - **Lo que retienen las escrituras.** DEF-192 no cambia el `seen` que ya anotó una escritura en vuelo (con test). Bajar el head visto solo hace que las escrituras siguientes retengan desde más abajo, y nunca da una decisión que la réplica no tomó: lo que la réplica aplicó antes de que la escritura entrara a la sala está por debajo de su `seen`, que es al menos el head de la réplica.
  - **Dos llamadores que detienen el worker a la vez** (de la revisión de delta del Lote 4). El segundo espera el fin de la tarea que detuvo el primero, con su último `Ack`, también si al primero lo cortan (con test).
  - **`start_worker` durante una detención.** Espera esa detención, con su último `Ack`, antes de arrancar otro worker (con test), también una que empieza mientras espera el aplicador para mirar la sala (con test, de la segunda ronda: la primera versión del arreglo miraba la detención antes del aplicador y arrancaba otro worker). Sin eso, un `close_room` o un `shutdown` posterior esperaba solo al worker nuevo y volvía con el viejo andando, y salían dos últimos `Ack` (lo encontró la revisión de delta, con un test). Con el Lote 8 de 4.4, un `set_freshness` por un `watch` que se suelta durante un cierre pasa por acá.
  - **El servidor que baja el cursor sin que el cliente lo sepa.** Un registro que le llega tarde (INV-Y6 deja que un registro baje el cursor) o una caída dentro de `roster_persist_secs` (INV-R3) dejan el cursor del servidor por debajo del head confirmado. Sin cambios en la sala, el worker no mandaba otro `Ack`, y una poda que pasara ese cursor dejaba al lector `Bootstrapping` hasta la próxima escritura (lo encontró la revisión de delta, con un test). El `Ack` de cada 10 heartbeats lo corrige (con test).
  - **DEF-196 con un snapshot descartado.** Una réplica que descartó el snapshot que se anuncia espera como si no hubiera ninguno, así que se registra cada 10 heartbeats y renueva la demanda del servidor: un registro cada 5 minutos, sin efecto sobre la réplica.
  - **El plazo del último `Ack`** incluye la espera del aplicador: un cambio largo bajo el aplicador (un `create_snapshot` de una subida que se cortó, por ejemplo) puede consumirlo, y el `Ack` no sale. Es un costo: el cursor del servidor queda en el último confirmado.
  - **El cliente soltado.** El worker de un cliente que la aplicación soltó no manda el último `Ack`: los tests de los Lotes 4 y 5, en que no sale nada después de soltarlo, siguen pasando, también el de otra sala cuyo heartbeat retiene al cliente.
  - **D78 (INV-K1).** Las comparaciones con el head de la réplica siguen antes que nada; la del head visto se suma. Bajar el head visto entra en el riesgo aceptado de INV-K2: si el servidor perdió secuencias, y después de la respuesta con el head menor se aplica una respuesta de antes de la pérdida, una escritura que entra en ese intervalo puede tomar la decisión de otra operación, que antes salía incierta.
  - **El token y la conexión.** El `Ack` va por el mismo envío que los pasos: anota la conexión y renueva el token ante `Unauthorized`, el último dentro de sus 5 s.
  - **INV-K15.** Por sala, las dos cuentas de los heartbeats y el fin de la última detención: nada crece por heartbeat.
  - **El orden del paso.** El `Ack` va dentro del paso, como el heartbeat: mientras está en vuelo, el worker no recoge las tareas que terminaron, que recoge después. Un `Ack` colgado demora el heartbeat siguiente, pero no lo puede vencer: va después del heartbeat, que ya renovó el lease.
  - **La fachada de 4.4.** Sus cierres van por `close_room` y `shutdown` del núcleo, así que mandan el último `Ack`. Un `Ack` no cambia la réplica, así que no avisa a ningún `watch`.
- **Revisión de delta, primera ronda: verde.** Ningún Alto ni Medio; seis Bajos, arreglados en el lote:
  - el servidor que baja el cursor sin que el cliente lo sepa (en las interacciones): el `Ack` de cada 10 heartbeats, con el test del revisor;
  - un `start_worker` durante una detención: ahora la espera, con el test del revisor;
  - tests que fallaban de vez en cuando, con el primer heartbeat en los primeros 10 ms (en los tests): ahora esperan el sync. Tres del Lote 5 fallan igual sin este lote, y van al Lote 7 (en los desvíos);
  - seis mutaciones que sobrevivían: cuatro tienen test ahora (el head visto en el `Ack` del worker, con el test del revisor; un heartbeat que falla en la cuenta de DEF-196; el orden del último `Ack`; y la época de la confirmación), y las otras dos son guardas de época, de la clase de la que ya sobrevivía (en las mutaciones);
  - el head de un `SyncBatch` se comparaba con el head visto antes de validar el lote: ahora va después;
  - un marcador sin completar en esta nota.

  Además, faltaban interacciones (el servidor que baja el cursor, el `start_worker` entre dos detenciones, DEF-196 con un snapshot descartado, el plazo del último `Ack` con la espera del aplicador, y D78 con el head visto que baja), y que el cursor del registro cuenta como confirmado no estaba en los desvíos.
- **Revisión de delta, segunda ronda: verde.** Los arreglos de los seis Bajos, bien, salvo el del `start_worker`; tres Bajos, arreglados en el lote:
  - un `start_worker` que esperaba el aplicador cuando empezaba una detención arrancaba otro worker igual: ahora decide con los dos locks tomados, y vuelve a probar después de la detención, con el test del revisor;
  - los docstrings de `start_worker`, `set_freshness` y del ciclo del worker no decían el `Ack` de cada 10 heartbeats, ni que esperan una detención;
  - la mutación de 11 heartbeats en lugar de 10 solo moría si el primer heartbeat caía en el primer segundo: el test compara ahora el instante del `Ack` con el del décimo heartbeat.
- **Desvíos.**
  - **El head visto (DC31).** DC31 dice que el head visto "pasa a ser el de `Registered`". Baja al head de la respuesta que lo contradice, y el `Registered` que sigue lo sube con el máximo, como cualquier respuesta: al final del camino de DEF-192 queda el de `Registered`. Que cada `Registered` lo pise lo bajaba también en registros que no vienen de una contradicción, como el de un cambio de esquema, y el test de 4.2 `the_head_seen_comes_from_every_reply_of_the_epoch` lo mostró en su caso del catch-up.
  - **Precisiones de DC25 y DC28** que el plan no decía:
    - El `Ack` va después del heartbeat y no antes: sin red, un `Ack` colgado no demora la renovación del lease. No usa el backoff de los heartbeats.
    - Una respuesta que pide registrarse registra la réplica en el mismo paso.
    - El último `Ack` lo manda la tarea del worker, y no quien la detiene, para que cortar a quien la detiene no lo corte y un segundo llamador lo espere.
    - Una sala sin worker cierra como antes, sin `Ack`.
    - El cursor del registro cuenta como confirmado, porque el servidor lo guarda como el cursor del cliente: después de registrarse, el worker no manda un `Ack` del mismo head.
    - El `Ack` de cada 10 heartbeats aunque el head no se mueva (de la revisión de delta): DC25 lo manda solo si el head se movió.
    - Un `start_worker` o un `set_freshness` esperan una detención en curso (de la revisión de delta): DC28 no lo decía.
  - **Tres guardas que no se distinguen con ningún test** salieron de la primera versión:
    - Que no manda `Ack` una réplica que espera un snapshot o que falla cerrada: su head es el que confirmó su registro, o no está registrada.
    - Que el último `Ack` no siga después de una respuesta rechazada: lo corta el chequeo de la réplica sin registrar.
    - Que solo mande el último `Ack` un worker que detuvieron: en los otros casos no sale igual.
  - **La cuenta de DEF-196** cuenta solo los heartbeats que dejan la réplica esperando sin un snapshot anunciado: una bajada larga de uno anunciado no la registra. El registro es el del paso siguiente al décimo.
  - **Tres tests de 4.2 ajustados** (arriba, en los tests).
  - **Tres tests del Lote 5 que fallan de vez en cuando** (`a_schema_change_during_a_sync_of_the_worker_registers_it_again_and_applies_with_it`, `a_sync_of_the_application_during_one_of_the_worker_ends_and_both_leave_the_head` y `a_sync_of_the_worker_without_a_reply_goes_on_and_one_that_fails_closed_stops_it`): suponen un solo heartbeat en `past(interval)`, y fallan sin este lote cuando el primero cae en los primeros 10 ms (cerca de una vez en mil). No son de este lote: van al Lote 7, que vuelve a tocar los tests del worker.
  - **`INVARIANTES.md`** toma la versión 34: la 32 la tomó el Lote 6 de 4.4, y la 33, el Lote 8b de 3.12.

### Lote 7 · El modo `live`: el stream en el worker (`zemdb-client`) · ✅ Hecho
**Objetivo:** que una sala en `live` se entere enseguida de lo que escriben los demás (DC24, DC26), con cada evento como un aviso; aplicar el `ops` sin sync es del Lote 8.

**Pendiente del Lote 6** (de su revisión de delta): tres tests del Lote 5 que suponen un solo heartbeat en `past(interval)` y fallan cuando el primero cae en los primeros 10 ms (`a_schema_change_during_a_sync_of_the_worker_registers_it_again_and_applies_with_it`, `a_sync_of_the_application_during_one_of_the_worker_ends_and_both_leave_the_head` y `a_sync_of_the_worker_without_a_reply_goes_on_and_one_that_fails_closed_stops_it`): que esperen el sync, como los del Lote 6 (`synced_to`), y que pasen con el primer heartbeat fijo a los 5 ms.

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

**Interacciones a revisar:** el tope de 2 streams por cliente y sala (el tercero cierra el más viejo, así que reabrir no espera al viejo); el `RateLimited` del tope total; la sala que el servidor detiene por inactividad y vuelve a levantar; el keep-alive; el apagado del servidor; las escrituras y su catch-up, cuyos eventos llegan después (en el Lote 8 se ignoran por el head); el tiempo que el worker deja de leer el stream mientras espera al aplicador, contra el *stall timeout* del servidor (que cierra solo la conexión de ese stream, DC27); el silencio de 45 s se mide en los bloques del transporte, y el lector del Lote 2 no entrega los comentarios (el keep-alive), así que el worker envuelve los bloques antes del lector o el lector avisa de cada bloque (de la revisión de delta del Lote 2).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K7, K6, K12, K15, K17.

**Entrega:** `feat(client): the live mode, with the event stream of the room (Fase 4.3, Lote 7)`.

**Hecho** (16 de Noviembre de 2026).
- **Qué entró.**
  - **La apertura.** En `live`, el worker abre el stream de la sala en una tarea propia, como el sync y la transferencia, con el token de la sala, después de leer la época de la réplica bajo el aplicador. Una réplica fuera del cliente (`RoomClosed`) o que falla cerrada (`FailedClosed`) detiene el worker sin abrir nada. Abrirlo es un paso del worker: su error se ve en `status`, y una apertura que sale bien lo borra. No cambia el estado de la conexión de la sala, que dan los pedidos.
    - `Unauthorized` renueva el token y abre otra vez con el nuevo, y `Forbidden` deja el token para que el pedido siguiente lo renueve primero, como en un pedido (DC30).
    - `EventsDisabled`: el worker sigue como en `periodic` hasta reintentar después de `events_retry`.
    - Cualquier otra negativa, una que no es de la sala (de otra sala, o con una correlación) o una que no decodifica, y la falta de respuesta, son un error del paso.
  - **El sync después de abrirlo**, por lo que pasó antes de suscribirse.
  - **Los avisos.** Cada evento es un aviso, si el stream es de la época de la réplica:
    - `ops`, `head_advanced`, `lagged` o uno que no decodifica: un sync del worker, juntado con los de los heartbeats (uno en vuelo y una marca);
    - `schema_reloaded`: la réplica queda sin registrar, y el sync del worker que lo sigue se registra primero con el cursor;
    - `snapshot_wanted`, y `snapshot_available` si la réplica espera un snapshot: un heartbeat en la tarea de las transferencias, como lo manda `heartbeat`, que sube o baja el snapshot (si hay una transferencia en curso, nada: la pide el heartbeat siguiente);
    - uno de nombre desconocido: nada.

    Ningún evento detiene el worker ni es un error suyo.
  - **La época.** El worker anota la época con que abrió el stream. Uno de otra época se reemplaza en el momento, sin espera, cuando lo nota su próximo evento, que no se usa, o su próximo heartbeat (`Asked` lleva la época de la réplica que lo mandó).
  - **Reabrirlo.** Su fin, un error al leerlo y una apertura que falla abren otro después de un backoff con *full jitter* como el de los heartbeats (`retry_base`, que se duplica hasta `retry_max`). Un evento lo vuelve a empezar, así que un stream que termina enseguida, o un servidor que lo rechaza, se reabre con una espera que se duplica, y no una vez por segundo. Los errores al leerlo son:
    - un evento o una línea demasiado largos;
    - bytes que no son UTF-8;
    - un error del transporte;
    - 45 s sin un bloque.
  - **El silencio de 45 s** se mide dentro del lector, en los bloques del transporte, con los comentarios del keep-alive. `EventReader` anota cuándo llegó el último bloque, y su lectura vence a los 45 s de él, también entre lecturas cortadas (`StreamError::Silent`). Después, como con los demás errores, no lee nada más.
  - **El modo.** `set_freshness` despierta la tarea (un `Notify`), que abre el stream en `live`, y lo cierra o corta su apertura en otro modo. Lo hace cuando termina el paso en curso: en el momento, salvo que haya un heartbeat en vuelo. El modo se lee en cada paso. Detener el worker cierra el stream, corta su apertura y espera que termine.
  - **`status`.** `WorkerStatus` suma `effective` (`periodic` en `live` con el SSE apagado, hasta que se abra un stream) y `stream_open`, que un guard baja al terminar la tarea.
  - **El módulo de los eventos** deja su `expect(dead_code)`: lo usa el worker.
  - **El tamaño.** Unas 260 líneas de producción, sin comentarios ni líneas vacías (se estimaron 250).
- **Tests.** `tests/live_tests.rs` (nuevo, 28 tests), contra el servidor en proceso y con el reloj pausado. El intervalo es de 59 s, y los tests actúan después de un heartbeat (`after_a_heartbeat`): lo que llega a la réplica en menos de un segundo llegó por el stream. Uno más, unitario, en `src/tests/events.rs`.
  - **Lo que escribe otro** llega por el stream en el momento, un sync por evento, sin ningún heartbeat.
  - **El stream cortado.** Se abre otro después de una espera de hasta `retry_base`. Lo escrito mientras estaba cortado (con la reapertura retenida antes de llegar al servidor) no llega por ningún evento, y llega con el sync de después de abrirlo.
  - **Muchos eventos** durante un sync retenido llevan a un solo sync más.
  - **`lagged`** (fabricado) y **`head_advanced`** (real, con una operación más grande que el tope del evento) llevan a un sync.
  - **`schema_reloaded`**, real, con una columna nueva: la réplica se registra con su cursor y su encarnación, guarda el esquema nuevo en sus metadatos, y el sync del worker no encuentra nada más (un solo sync).
  - **`snapshot_wanted`**: el designado sube su snapshot en menos de un segundo, sin esperar su heartbeat, con los dos heartbeats que manda `heartbeat` en la tarea de las transferencias. **`snapshot_available`**, por una subida del worker de snapshots del servidor: la réplica que espera lo baja igual.
  - **Lo que no lleva a nada:** un `snapshot_available` con la réplica sin esperar, un evento de nombre desconocido y un comentario no llevan a ningún sync, heartbeat ni bajada.
  - **El SSE apagado**, con el servidor real (`max_sse_streams` en 0): `live` cae a `periodic` (lo dice `status`), lo escrito llega con el heartbeat, y el worker vuelve a intentar el stream después de `events_retry`, no antes.
  - **El SSE apagado y encendido**, con la negativa fabricada: una apertura sin respuesta deja `Network` en `status`, y la siguiente, rechazada con `EventsDisabled`, lo borra y deja el modo efectivo en `periodic`. Cuando el servidor lo vuelve a encender, el worker abre el stream después de `events_retry`, con su sync, y lo escrito llega por evento.
  - **El token.** Un stream que termina, con un `Unauthorized` a la reapertura, se vuelve a abrir con un token renovado. Una reapertura `Forbidden` hace que la siguiente, después de una espera, pida el token antes de abrir.
  - **El fin con el token**, en tiempo virtual: el stream se reabre con el mismo token, que en hora de pared sigue vigente, sin pedir otro, y con un sync después.
  - **Un stream de una época anterior** (la réplica descartada por `InvalidSequence` en el registro de un sync de la aplicación): su evento no lleva a ningún heartbeat, y el stream se reemplaza con su sync. Sin un evento, el heartbeat siguiente lo nota.
  - **El silencio.** Un stream fabricado que calla se reemplaza a los 45 s y no antes; el del servidor, con su keep-alive cada 15 s, sigue abierto 120 s.
  - **Los eventos que no se pueden usar.** Uno que no decodifica (`ops` sin base64, `head_advanced` sin secuencia) lleva a un sync y no detiene nada. Uno de más de 64 KiB cierra el stream: se abre otro, con su sync, sin error en `status`.
  - **El modo.** `set_freshness(Live)` abre el stream en el momento, y `manual` lo cierra; `stop_worker` lo cierra. Una apertura retenida la corta otro modo, y también una detención, que no la espera: nada retiene al cliente después.
  - **La espera entre aperturas.** Con streams que terminan enseguida sin un evento, unas 25 aperturas en 300 s; con streams que traen un evento, más de 60 en 60 s. Con aperturas rechazadas (`RateLimited`), la espera también se duplica, y `status` muestra el error salvo después de un heartbeat.
  - **La apertura que encuentra la sala fuera del cliente** (un sync de la aplicación cuyo descarte no pudo borrar los datos) o **fallando cerrada** (un `Registered` con un esquema que no evoluciona) detiene el worker sin abrir nada, mucho antes del heartbeat siguiente.
  - **Una negativa que no es de la sala** (`EventsDisabled` de otra sala, o con una correlación) es un error, y no cambia el modo efectivo. La apertura siguiente lo borra.
  - **La sala que el servidor detiene** termina el stream: el worker abre otro, y lo escrito después llega por evento.
  - **De la revisión de delta:**
    - un worker que un paso detiene con el stream abierto lo muestra cerrado en `status`;
    - también lo muestra cerrado mientras está en vuelo el `Ack` que sigue al heartbeat que reemplazó un stream de otra época (con un stream callado, para que no lo note un evento);
    - un servidor que rechaza todas las aperturas por el token: cada apertura renueva una vez, y las aperturas esperan cada vez más;
    - con un stream abierto pero callado (un proxy que lo retiene), el heartbeat trae lo que escribe otro;
    - un cliente soltado mientras un sync del worker lo retiene termina el worker en el evento siguiente, mucho antes del heartbeat;
    - el worker abre un stream a la vez, al arrancar y cuando termina, y nada retiene al cliente después.
  - **El silencio del lector** (unitario): con dos keep-alive a 40 s de distancia y lecturas cortadas cada 7 s, el lector da `Silent` a los 125 s, a 45 s del último bloque, y no lee un evento que llega después.
  - **Cambios en tests de otros lotes:**
    - `worker_tests.rs`: `WorkerStatus` con sus dos campos.
    - El test de `periodic` y `live` del Lote 5 queda solo en `periodic`, ahora sin ningún stream: `live` abre el suyo.
    - El del estado espera que el stream se abra y se cierre.
    - **El pendiente del Lote 6:** los tres tests del Lote 5 esperan el sync, o el error, en lugar de dormir un intervalo.
    - En `tests/common/`:
      - el transporte de prueba cuenta las aperturas del stream con su token y su instante, las retiene, las contesta a pedido, escribe bloques en los streams que abrió, los termina y cuenta los que el cliente sigue leyendo;
      - tiene un tope de aperturas como el de los mensajes, para que una mutación que reabre sin esperar falle y no se cuelgue;
      - `Held::replied` espera hasta 60 s, más que cualquier intervalo.
  - **Sin depender de cuándo cae el primer heartbeat.**
    - La suite del worker, la de `live` y la de los snapshots pasaron 30 de 30 veces seguidas. Con el árbol de la revisión, la del worker y la de `live` pasaron 30 de 30.
    - En esa corrida, el test del SSE encendido después de un error falló una vez: el tercer intento podía caer dentro de los 10 ms que esperaba antes de mirar el estado. Ahora retiene el segundo y el tercer intento, y mira el estado entre los dos. La suite de `live` pasó después 100 de 100.
    - **La CI de `fd09b6e` falló en macOS** por la misma carrera en el test de la negativa de otra sala: la apertura que sigue a la negativa, con una espera al azar de hasta 2 s, ya había borrado el error cuando el test lo miró, 10 ms después (una vez en unas 200 vueltas). Ahora ese test también retiene la apertura rechazada y la siguiente, y mira el estado entre las dos. La suite de `live` pasó 100 de 100, y las mutaciones que solo ese test mataba se volvieron a correr (`mut7/run7.log`).
    - Con el primer heartbeat fijo a los 5 ms o a 1 ns del final del intervalo, pasan los de `live` y los tres del Lote 5 del pendiente. Fallan solo el del primer heartbeat al azar, que lo prueba a propósito, y, a 1 ns del final, tres de los Lotes 4 a 6 (en los desvíos).
    - A 2 ms del final pasan todos.
  - **MSRV y versiones largas.** Con Rust 1.88 pasan los 20 binarios de tests del cliente, y con 1.85 compila el workspace. Pasan las pruebas largas del cliente (`--release --ignored`): el fuzzing de los eventos y del cifrado, la simulación y los `watch`.
- **Mutaciones inversas: 52 de las 53 las mata algún test, y con la otra la suite no termina.** El log, en el scratchpad de la sesión: `mut7/run1.log` a `run7.log`.
  - **El silencio:** sin plazo, sin anotar el bloque, el plazo por lectura y no por bloque, 48 s, y no repetir `Silent`.
  - **La apertura:**
    - sin `FailedClosed`;
    - sin renovar el token marcado, ni ante `Unauthorized`;
    - sin marcar `Forbidden`;
    - `EventsDisabled` como un error;
    - la negativa de cualquier sala o con correlación;
    - la época en 0.
  - **El núcleo:** `unregister` sin efecto, `awaits_snapshot` siempre falso o siempre verdadero, y la época de `Asked` en 0.
  - **El modo y el estado:**
    - sin `Notify`, o sin esperarlo;
    - sin cerrar fuera de `live`, ni cortar la apertura;
    - el modo efectivo siempre el pedido;
    - sin compartir `disabled` ni `stream_open`;
    - el stream también en `periodic`.
  - **El resultado de la apertura:**
    - un error que nunca detiene;
    - reabrir sin espera;
    - una apertura que no borra el error;
    - sin marcar `disabled`, reintentarlo con el backoff, o no borrarlo;
    - sin el sync después de abrir.
  - **La lectura:**
    - reabrir sin espera tras el fin;
    - sin comparar la época en el evento, ni en el heartbeat;
    - sin reiniciar el backoff con un evento;
    - abrir otro mientras uno se abre;
    - no cortar la apertura al detenerse.
  - **Los avisos:**
    - `schema_reloaded` solo con un sync, o sin nada;
    - `snapshot_wanted` sin nada;
    - `snapshot_available` sin nada, o siempre;
    - un evento desconocido que sincroniza;
    - `ops`, `head_advanced`, `lagged` o uno que no decodifica sin nada.
  - **Cuatro casos en la primera corrida:**
    - **Dos sobrevivieron** y llevaron a tests más fuertes:
      - Que `Silent` se repita: sin eso, la lectura siguiente también vencía, porque el último bloque seguía lejos. Ahora un evento que llega después del silencio no se lee.
      - Que una apertura borre el error: el sync que la sigue también lo borraba. Ahora se ve con una apertura rechazada con `EventsDisabled`, que no lleva a un sync.
    - **No cortar la apertura al detenerse** colgaba el test, que ahora tiene un plazo propio.
    - **Abrir otro stream mientras uno se abre** deja al worker lanzando aperturas sin espera. Cada vuelta reemplaza la apertura en vuelo por una nueva, que nunca alcanza a terminar, y las tareas se acumulan sin fin, también sin nada retenido. El reloj pausado no avanza y el proceso se queda sin memoria, así que la suite no termina. Ningún test lo dice mejor con el reloj pausado: el test de la revisión de delta, que lo mataba sin agotar la memoria, se colgaba igual en esta máquina. Queda el test de un stream a la vez, al arrancar y al reabrir.
  - **Cinco más, de la revisión de delta, con sus tests:**
    - `status` que no se limpia al terminar la tarea, ni después del reemplazo en el heartbeat;
    - no marcar la renovación en la apertura (reabría sin espera ni límite);
    - el heartbeat que no sincroniza con el stream abierto;
    - seguir en un evento con el cliente soltado.
  - **Una dependencia corregida.** Un test nuevo dependía del backoff que dejó el anterior: fallaba 13 de 30 veces hasta que inyectó un evento para reiniciar la espera. Las tres mutaciones que solo él mataba se volvieron a correr.
- **Interacciones revisadas.**
  - **El tope de 2 streams por cliente y sala.** El worker suelta el stream viejo antes de abrir otro: su conexión se cierra, y el servidor lo nota en su próxima escritura, a lo sumo el keep-alive siguiente. Mientras tanto, el nuevo es el segundo, y uno más reemplazaría al más viejo, así que reabrir nunca espera al viejo (el test del Lote 2 del tercer stream que termina el más viejo).
  - **El `RateLimited` del tope total** es una negativa más, con su espera que se duplica (con test).
  - **La sala que el servidor detiene.** Con un suscriptor, el servidor no la detiene por inactividad. Si se detiene igual, el stream termina y el worker abre otro, que la vuelve a levantar (con test).
  - **El keep-alive** cuenta para el silencio: un stream del servidor que calla no se reemplaza (con test).
  - **El apagado del servidor** termina el stream como cualquier fin (el test del Lote 2), y las aperturas sin respuesta esperan cada vez más. Sin test con el reloj pausado: el servidor de prueba reiniciado es otro router, que el transporte del cliente no ve.
  - **Las escrituras y su catch-up.** En `live`, el evento del commit propio llega después del catch-up y lleva a un sync que no trae nada: un pedido más por escritura, hasta que el Lote 8 ignore ese evento por el head.
  - **El tiempo que el worker no lee el stream.** Mientras manda un heartbeat y su `Ack`, que esperan el aplicador, no lee. Lo que llegó espera en la conexión, y cuenta para el silencio apenas se lee: el plazo mira el último bloque, pero una lectura que encuentra un bloque listo lo toma antes de vencer. El *stall timeout* del servidor cierra solo la conexión de ese stream (DC27).
  - **El silencio** se mide en los bloques del transporte y no en los eventos: el lector no entrega los comentarios, así que lo mide él (la nota de la revisión de delta del Lote 2).
  - **Los syncs de los eventos y de los heartbeats** comparten la tarea y la marca del worker: a lo sumo uno en vuelo y uno más (con test). Los de las escrituras y de la aplicación van por su lado, como antes.
  - **La renovación del token** es la misma que la de los pedidos, una por sala a la vez: el stream y un pedido que reciben `Unauthorized` con el mismo token esperan la misma renovación.
  - **El descarte por D78** deja la misma encarnación, y solo la época distingue los eventos de antes (con test). `discard_room` detiene el worker antes de descartar, con su stream.
  - **Una réplica que espera un snapshot** no sincroniza: el sync después de abrir el stream es `Network` sin mandar nada, y queda en `status` hasta el heartbeat siguiente.
  - **El cliente que se suelta.** La apertura en vuelo retiene al cliente como un paso más, y el worker la corta al detenerse. Un evento con el cliente soltado termina la tarea, como un heartbeat.
  - **4.4.** El `set_freshness` de un `watch` (Lote 8 de 4.4) tiene efecto cuando termina el paso en curso, y espera una detención en curso como antes.
  - **`set_freshness` con un heartbeat en vuelo** (de la revisión de delta). La tarea no mira el modo mientras manda un heartbeat y su `Ack`: lo toma al terminarlos, que puede ser hasta el plazo del transporte y del proveedor de tokens. Así lo dicen ahora los docstrings y §11.
  - **`schema_reloaded` con un registro en vuelo** (de un heartbeat, una escritura o la aplicación; de la revisión de delta). El registro deja la réplica sin registrar, pero un `Registered` con el esquema viejo que llega después la vuelve a dejar registrada. Lo cubren el esquema que anuncia cada `SyncBatch`, que el sync de ese aviso trae (un lote con una versión posterior registra otra vez), y el de cada `HeartbeatAck`: la réplica toma el esquema nuevo en ese sync o en el heartbeat siguiente.
  - **El estado de `status` al terminar la tarea** (de la revisión de delta). `stream_open` lo pone un guard que lo baja al terminar la tarea, también si se corta, como pide `GEMINI.md` para un estado transitorio (con test).
  - **El MSRV y wasm32:** `Notify` y `timeout_at` son de Tokio, ya en el MSRV. El crate sigue vacío en wasm32.
- **Revisión de delta, primera ronda: verde.** Sin hallazgos Altos ni Medios. Los seis Bajos se arreglaron en el lote:
  - **Bajo:** `status` mostraba el stream abierto con el worker ya detenido, y durante el `Ack` después de un reemplazo en el heartbeat. Ahora lo pone un guard RAII que lo baja al terminar la tarea, y se actualiza después del reemplazo (tests nuevos).
  - **Bajo:** el modo no se toma en el momento mientras hay un heartbeat en vuelo, y volver a `live` respeta la espera de la reapertura o la de `events_retry`. Los docstrings, §11 y estos desvíos lo dicen ahora.
  - **Bajo:** tres reglas sin test, ahora con test:
    - la marca de la renovación en la apertura;
    - el sync del heartbeat en `live` con el stream abierto;
    - el `break` de un evento con el cliente soltado.
  - **Bajo:** dos esperas de los tests sin tope propio (`after_a_heartbeat` y la del error del Lote 5), que ahora lo tienen. Además, un test para la mutación que agota la memoria, que acá no la mata sin colgarse (en las mutaciones).
  - **Bajo:** faltaban interacciones: `set_freshness` con un heartbeat en vuelo, `schema_reloaded` con un registro en vuelo y el estado al terminar la tarea.
  - **Bajo:** los docstrings de `Worker::stop` y de `stop_worker` no nombraban la apertura del stream.

  El revisor volvió a correr las mutaciones y la suite con el primer heartbeat fijo, con los mismos resultados.
- **Desvíos.**
  - **Precisiones de DC26 que el plan no decía:**
    - **la espera de la reapertura** vuelve a empezar con un evento del stream, no con una apertura que sale bien, para que un stream que termina enseguida no se reabra una vez por segundo con un sync cada vez;
    - **el registro de `schema_reloaded`** lo manda el sync del worker, que se registra primero porque la réplica quedó sin registrar: un registro y un sync, en lugar de un registro solo;
    - **una negativa del stream** que no es de la sala, o que trae una correlación, es un error;
    - **la apertura** es un paso del worker, con su error en `status`, que no cambia el estado de la conexión.
  - **Precisión de DC28:** el modo se toma cuando termina el paso en curso (un `Notify`), no en el heartbeat siguiente como en el Lote 5, para que `live` abra el stream enseguida.
    - Mientras hay un heartbeat en vuelo, al terminarlo.
    - La espera de la reapertura y la de `events_retry` se conservan entre modos.
  - **Los errores de lectura del stream** y su fin no van a `status`: los de la apertura sí.
  - **El silencio** se mide en el lector del Lote 2, que anota su último bloque, y no en un envoltorio de los bloques en el worker: así vale también entre lecturas que el worker corta en su `select!`.
  - **`WorkerStatus`** suma dos campos públicos: `effective` y `stream_open`.
  - **Un pendiente para el Lote 9.** La verificación con el primer heartbeat a 1 ns del final del intervalo, que no se había hecho en los lotes anteriores, encontró tres tests de los Lotes 4 a 6 que fallan así: el reloj de Tokio redondea el heartbeat al mismo milisegundo en que termina la espera del test, una vez en unas 10.000 corridas. Quedan anotados en el Lote 9.
  - **Dos tests del pendiente del Lote 5** tenían ese mismo borde con el plazo de 10 s de `Held::replied`, que ahora es de 60 s.

### Lote 8 · El evento `ops` (`zemdb-client`) · ✅ Hecho
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

**Hecho** (16 de Noviembre de 2026).
- **Qué entró.**
  - **El `ops` sin un sync.** Las operaciones de un evento `ops` llegan a la réplica desde el evento (`Client::worker_apply`), por el aplicador, como un lote de un sync: el aplicador descarta las que la réplica ya tiene, no aplica un hueco, entrega la decisión de cada secuencia a la escritura que la espera y la retiene para las que no tienen su `CommitAck`.
  - **La validación, en el orden de DC26.**
    1. Que decodifique (del Lote 2), de la sala del stream, con operaciones contiguas, al menos una, que terminan en su head; si no, un sync, como ante un evento que no decodifica.
    2. La época del stream (del Lote 7): un evento de otra época no se usa, y el stream se reemplaza.
    3. La encarnación, que tiene que ser la de los metadatos: si no, la réplica queda sin registrar, y el sync que sigue se registra con su cursor y recibe `RoomReset`, que la descarta.
    4. El esquema, con la regla de un lote (DC20): otro id o una versión posterior dejan la réplica sin registrar, y el sync que sigue toma el esquema de `Registered` y trae las operaciones; una versión anterior del propio no es un cambio.
  - **Después, su head es un head visto** (DC21), que se toma antes de que sus operaciones lleguen al aplicador. El evento que no pasa el head local (el del commit propio, que su catch-up ya aplicó) no cambia nada; uno con un hueco lleva a un sync, y también una réplica que no está abierta en la época del stream (vacía, o fuera del cliente en el medio de un cambio: el sync lo decide bajo el aplicador).
  - **Ningún evento hace fallar cerrada la sala.** El aplicador lo aplica como el catch-up de un commit, sin fallar cerrado: una operación que no valida con el esquema local deja la réplica sin registrar, y el sync que sigue la registra y decide.
  - **Un evento a la vez.** La aplicación corre en una tarea propia, como el sync, y el worker no lee el stream mientras tanto: ninguna tarea espera por cada evento, y lo que llega espera en la conexión o en el canal del servidor, que lo cambia por `lagged` si se llena. Al terminar, el sync que pide, salvo que el modo haya pasado a `manual`. Aplicarlo es un paso: su error se ve en `status` (y lleva a un sync), y una aplicación que sale bien lo borra. Detener el worker la corta, como al sync; el cambio de la réplica que empezó termina en su tarea.
  - **El tamaño.** Unas 90 líneas de producción, sin comentarios ni líneas vacías (se estimaron 150).
- **Tests.** En `tests/live_tests.rs`, 19 nuevos, contra el servidor en proceso y con el reloj pausado, con el intervalo de 59 s; los que esperan que nada llegue con un heartbeat actúan después de uno (`after_a_heartbeat`). El transporte de prueba suma `take_next_event`, que saca del stream el próximo evento de un nombre, como una red que lo pierde, y le da su texto al test, para escribirlo en el stream después (`inject`) o nunca.
  - **Varios clientes a la vez.** Tres clientes con su worker en `live` insertan las filas 1 a 4 con su nombre y las actualizan, todo a la vez (24 escrituras): cada escritura termina con `Ok` o `RowMissing`, las tres réplicas llegan al head sin ningún sync (salvo uno por cada primer heartbeat que caiga en el medio de las escrituras, cuyo ack puede informar un head que la réplica todavía no alcanzó), y tienen las filas de un cuarto cliente que sincroniza, con las mismas decisiones.
  - **El `CommitAck` después del evento.** Con el `CommitAck` de un update de una fila que no existe retenido, su evento aplica la secuencia antes; la escritura termina con `RowMissing`, la decisión retenida, sin ningún sync, y no queda nada retenido.
  - **El evento del commit propio,** que el test escribe en el stream después de que el catch-up aplicó la operación, no lleva a ningún sync.
  - **Un evento y un sync que traen las mismas secuencias.** El primero de dos eventos se pierde y el segundo tiene un hueco: el sync al que lleva espera su respuesta, que trae las dos, mientras el primero llega y se aplica. Al soltarla, el aplicador descarta lo que ya tiene: las dos filas, una vez, con un solo sync y sin error.
  - **Un evento perdido:** el siguiente tiene un hueco, y un sync lo cubre.
  - **La sala borrada y recreada con el mismo id.** El stream se reabre con el token viejo en la sala nueva, y el sync de después de abrirlo espera su respuesta. Los eventos de la sala nueva, también el que sigue al cursor de la réplica, no se aplican, y la réplica queda sin registrar. Al soltar la respuesta, se registra con su cursor y su encarnación, recibe `RoomReset`, se descarta (la época sube) y se registra sin cursor: queda con las filas de la sala nueva.
  - **Una versión posterior del esquema.** El transporte saca el `schema_reloaded` de una columna nueva, y el `ops` de un commit que vale con las dos versiones no se aplica: la réplica se registra con su cursor, guarda el esquema nuevo en sus metadatos y sincroniza, con un solo sync. Un `ops` fabricado de la versión anterior del propio esquema, después, se aplica.
  - **Los `ops` fabricados que no se usan:** de otra sala, sin operaciones, con operaciones que no son contiguas, con operaciones que no llegan a su head y con operaciones más allá de su head llevan, cada uno, a un sync, sin aplicar nada, sin registrar la réplica y sin error.
  - **Una operación que no valida** (un `title` entero): la réplica se registra con su cursor y sincroniza, y no falla cerrada ni detiene el worker. El head del evento fue un head visto, así que el lote del sync, con uno menor, la registra una vez más (dos registros y dos `Sync`).
  - **Un `ops` de un stream de una época anterior** (la réplica descartada por `InvalidSequence` en el registro de un sync de la aplicación, con la misma encarnación), que sigue al head de la réplica nueva: no se aplica, y el stream se reemplaza.
  - **Un evento a la vez.** Con el `apply_batch` del primero retenido en el motor, ocho más esperan en el stream: solo el test y esa aplicación retienen al cliente (ninguna tarea por evento), y al soltarlo se aplican los nueve, en orden, sin sync.
  - **Detener el worker con una aplicación retenida** no la espera (termina en 5 s, con el último `Ack`, que espera al aplicador), y el lote se aplica entero al soltarla.
  - **El modo a `manual` con una aplicación en vuelo.** Un sync retiene el aplicador, `set_freshness` espera detrás, y la aplicación de un evento con una operación que no valida, detrás de los dos: deja la réplica sin registrar, y no lleva a ningún sync.
  - **Un error del storage al aplicar** (`RoomLocked`): se ve en `status`, con el worker corriendo, mientras el sync al que lleva espera su respuesta, que trae la operación y borra el error.
  - **Una aplicación que sale bien borra el error** de un sync que el servidor rechazó.
  - **Un `ops` para una réplica que falla cerrada** (un sync de la aplicación con un `Registered` que no evoluciona) no se aplica, y el sync al que lleva detiene el worker.
  - **Un `ops` con la réplica vacía:** el registro de la apertura no tuvo respuesta, y el sync de después de abrir el stream la registra, con la respuesta retenida. El evento que llega mientras tanto no se aplica a la réplica vacía, y marca un sync más.
  - **El head visto antes del aplicador.** Un sync retiene el aplicador, y una escritura espera detrás, con la respuesta a su commit retenida. Llega el evento de un update que la réplica rechaza, cuya aplicación espera detrás de la escritura: la escritura vio su head, así que no retiene el rechazo.
  - **Un cliente soltado mientras una aplicación lo retiene,** con un sync del worker retenido también: al terminar la aplicación, que pide un sync, el worker termina y cierra el stream, mucho antes del heartbeat siguiente.
  - **Cambios en tests del Lote 7,** porque un commit ya no lleva a un sync:
    - `in_live_what_another_client_writes_arrives_by_its_events_without_a_sync_or_a_heartbeat` (antes `..._arrives_at_once_not_with_a_heartbeat`) espera ningún sync, en lugar de uno por evento;
    - `many_notices_during_a_held_sync_lead_to_a_single_sync_more_and_events_apply_meanwhile` (antes `many_events_during_a_held_sync_...`) retiene el sync con `lagged`, y los eventos de tres commits se aplican mientras tanto, así que el sync de más pide desde 3;
    - `a_client_dropped_while_a_sync_of_its_worker_holds_it_ends_the_worker_at_the_next_event` retiene el sync con `lagged`.
  - **Contra el código del Lote 7,** 13 de los nuevos fallan por comportamiento, y también los dos del Lote 7 que cambiaron de asserts. Seis pasan, porque prueban reglas que el Lote 7 ya cumplía al llevar cada `ops` a un sync: el evento perdido, los fabricados que no se usan, el de una época anterior, detener el worker con una aplicación retenida (la retención la alcanzaba el sync), el `ops` para una réplica que falla cerrada y el de la réplica vacía. Las mutaciones muestran que prueban las reglas de este lote. Siete se escribieron o reescribieron después de la implementación (el del modo `manual`, el del error del storage, el que borra el error, el de la réplica que falla cerrada, el de la réplica vacía, el del cliente soltado y el del head visto, este de la revisión de delta), y se corrieron después contra el código del Lote 7, con el de producción guardado aparte.
  - **Repeticiones:** `live_tests` y `worker_tests`, 30 de 30. Con el primer heartbeat fijo a 5 ms y a 1 ns del final del intervalo, tres corridas de cada uno: pasan los 46 de `live_tests`; en `worker_tests` falla el que prueba que el primer heartbeat cae al azar, y, a 1 ns, los tres del pendiente del Lote 9 (ninguno nuevo).
  - **MSRV y versiones largas.** Con Rust 1.88 pasan los 21 binarios de tests del cliente, y con 1.85 compila el workspace. Pasan las pruebas largas del cliente (`--release --ignored`): el fuzzing de los eventos y del cifrado, la simulación y los `watch`.
- **Interacciones revisadas.**
  - **El aplicador, lo retenido y el head visto (DC21).** El evento pasa por `Local::apply`, el mismo camino que un lote: descarta el prefijo, no aplica un hueco, entrega las decisiones y retiene los rechazos para las escrituras sin su `CommitAck`. Su head se toma como visto después de las cuatro verificaciones y antes de esperar al aplicador, como el de una respuesta: una escritura que entra después lo ve, y un `CommitAck` en una secuencia que el servidor volvió a asignar (D78) queda incierto. Tests: el `CommitAck` después del evento, varios clientes a la vez.
  - **Un sync, el catch-up de una escritura o un sync de la aplicación que aplican las mismas secuencias.** Todos pasan por el aplicador, de a uno, y el que llega segundo descarta lo que ya está. Tests: el evento y el sync de las mismas secuencias, el evento del commit propio, varios clientes a la vez.
  - **El sync después de abrir el stream** y los eventos que llegan mientras corre: se aplican en paralelo, por el aplicador. Uno con un hueco marca un sync más. Test: el de muchos avisos durante un sync retenido.
  - **El descarte y la época.** La época se compara al leer el evento (del Lote 7), y otra vez bajo el aplicador, en `Local::apply` (`Stale`): un descarte entre las dos lleva a un sync de la época nueva, y el stream viejo se reemplaza en su próximo evento o heartbeat. La encarnación se compara fuera del aplicador, con la época: en una época, una réplica abierta tiene siempre la misma encarnación (la cambia solo un descarte, que sube la época). Tests: la sala recreada, el stream de una época anterior.
  - **El cambio de esquema (INV-E3).** El evento lleva el esquema con que el servidor validó sus operaciones, y se compara como el de un lote. Un registro en el medio que cambia el esquema lo nota la comparación bajo el aplicador. Test: la versión posterior y la anterior.
  - **El checklist de determinismo de la Fase 3.11.** La decisión sobre cada operación es la del motor en `apply_batch`, sea que llegue por un evento o por un lote, y con cualquier partición en eventos. Test: tres clientes cuyas réplicas, por eventos, tienen las filas de una que sincroniza. Una versión posterior del esquema no se aplica con la anterior, así que no depende de cuándo la réplica conoció la versión.
  - **Fallar cerrada.** `Local::apply` con un evento no falla cerrada la sala (como el catch-up de un commit), y una réplica que ya falla cerrada no aplica nada (`Stale`): el sync que sigue detiene el worker. Tests: la operación que no valida, el `ops` para una réplica que falla cerrada.
  - **DEF-192.** El head de un evento de la encarnación de la réplica es uno del servidor. El de uno dañado (con una operación que no valida) puede estar más allá: el lote del sync que sigue, con un head menor, registra la réplica una vez más, y el head visto vuelve al del servidor. Test: la operación que no valida.
  - **DEF-196 y la réplica que espera un snapshot.** Un evento no sigue a su head, así que lleva a un sync, que baja el snapshot anunciado o falla sin mandar nada, como antes del lote (un sync por evento, juntados). Sin test nuevo.
  - **El heartbeat.** Su ack puede informar un head que un evento todavía no aplicó (la aplicación espera al aplicador): un sync de más, que no encuentra nada. Sin test: es un costo, no una ruptura.
  - **El `Ack` periódico (DC25).** En `live`, con los eventos aplicados sin syncs, el cursor del servidor avanza solo con el `Ack` de cada heartbeat (del Lote 6) y con las escrituras: es lo que prevé DC25 para D78.
  - **La lectura en pausa.** Mientras se aplica un evento, el worker no lee el stream: lo que llega espera en la conexión, el silencio de 45 s se mide en los bloques que llegaron (y `timeout_at` lee primero lo que espera), y si el canal del servidor se llena, llega `lagged`, un sync. Test: un evento a la vez.
  - **Detener el worker, soltar el cliente y el cambio de modo** con una aplicación en vuelo: detenerlo la corta y no la espera, y el cambio de la réplica que empezó termina en su tarea; la aplicación retiene al cliente como un sync, y suelta el `Held` al terminar; un cambio a `manual` no la corta, y su sync no sale. Tests: detener el worker con una aplicación retenida, el modo a `manual`.
  - **`watch`.** El aplicador marca las suscripciones con el cambio de cada lote, también el de un evento: es el mismo `Local::apply`. Sin test nuevo (los de `watch_tests.rs` cubren `apply`).
  - **`set_freshness`** toma el aplicador para entrar el worker: espera una aplicación en curso, como espera un sync (del Lote 4).
  - **El estado de la conexión y el `Offline` de DC36** (de la revisión de delta). Lo cambian solo los pedidos, no los eventos, y hasta el Lote 7 cada `ops` llevaba a un sync, que lo refrescaba. En `live`, después de un pedido sin respuesta que no es un heartbeat (cuyo reintento llega dentro de su backoff), `connected` queda en `false` hasta el próximo heartbeat, a lo sumo un intervalo después, aunque el stream siga trayendo operaciones: una escritura de la fachada en ese lapso es `Offline` (el Lote 8 de 4.4, que cerró antes que este). Queda anotado para la revisión de cierre de 4.4 (su Lote 11), que lo decide.
- **Desvíos.**
  - **Un evento a la vez, con la lectura en pausa.** DC26 no dice qué hacer con un `ops` que llega mientras se aplica otro. Juntarlo en la marca del sync volvería al sync por evento cuando hay mucho tráfico, y una cola crecería por evento. La lectura en pausa los aplica en orden, sin tareas por evento.
  - **Aplicar un evento es un paso del worker,** como el sync que reemplaza: su error se ve en `status` y lleva a un sync (las operaciones del evento no llegaron), y una aplicación que sale bien borra el error. Solo `RoomClosed` lo detendría (un error del storage que sacó la sala del cliente al reabrirla).
  - **Una réplica que no está abierta en la época del stream** (vacía, por ejemplo, con un stream abierto antes de su registro) no aplica el evento, y lleva a un sync, que la registra o detiene el worker.
  - **La comparación del esquema antes del head visto** repite la de `Local::apply`, que la hace bajo el aplicador: está por el orden de DC26 (el head es visto después de las cuatro verificaciones). Sin ella, el evento igual no se aplica, y el head, de la encarnación de la réplica, es uno del servidor: la mutación es equivalente.
  - **El head visto de un evento con una operación que no valida** lleva a un segundo registro (DEF-192). DC26 pone el head visto después de las cuatro verificaciones, y la operación se valida al aplicarla.
  - **Una operación de una tabla que el esquema local no tiene** (`TableNotFound`), con el mismo esquema y la misma versión, no la manda un servidor honesto (§2 de `INVARIANTES.md`): lleva, como en un lote de un sync, a un error de `Storage`, aquí con un sync después, y no a un registro.
  - **Un test intermitente del servidor,** fuera del lote: `a_panic_in_a_file_operation_stops_the_room_and_the_next_request_reopens_it` falló una vez en `cargo test --workspace`. Solo pasó 20 de 20 veces, y el binario entero, dos veces más; el lote no toca el servidor. Va al backlog: DEF-199 (en la nota del Lote 9).
- **Mutaciones inversas: 28 de las 30 las mata algún test, y las otras dos son equivalentes.** El script y los logs, en el scratchpad de la sesión: `mut8/mutate.py`, `mut8/run1.log`, `run2.log` y `run3.log`.
  - **La validación:** sin la sala, sin la contigüidad, aceptar un evento sin operaciones, aceptar operaciones que no llegan a su head o que pasan de él (esta, en `run3.log`, de la revisión de delta), sin la encarnación, la encarnación distinta sin dejar la réplica sin registrar o sin sync, y la réplica no abierta sin sync (esta última, en `run2.log`, con el test de la réplica vacía).
  - **La aplicación:** sin el head visto, el head visto después del aplicador (en `run3.log`, de la revisión de delta), fallar cerrada la sala con un evento, un hueco sin sync, un sync después de cada aplicación, `Stale` sin sync, y `Schema` sin dejar la réplica sin registrar o sin sync.
  - **El worker:** el `ops` como aviso (lo del Lote 7), la época 0 en la aplicación, leer el stream mientras se aplica, nunca sincronizar después de una aplicación, sincronizar en `manual`, un error sin sync, un error que no se ve en `status`, una aplicación que no borra el error, el cliente soltado que sigue (en `run2.log`, con el test del cliente soltado) y no cortar la aplicación al detener el worker.
  - **Equivalentes:**
    - Sin la comparación del esquema antes del head visto, el aplicador igual no aplica el evento (la repite, como para un lote), y el head que queda visto es uno del servidor.
    - Un error de la aplicación que nunca detiene el worker: la aplicación devuelve `RoomClosed` solo con la sala ya fuera del cliente (al reabrirla después de un error del storage), y el sync que la sigue detiene el worker igual; en `manual`, donde ese sync no sale, lo detiene el heartbeat siguiente, a lo sumo un intervalo después.
  - **Otras dos, de la revisión de delta, equivalentes:** sacar la época de la comparación de la encarnación (`Room::saw`, `Room::update` y `Local::apply` la repiten), y no esperar la aplicación cortada al detener el worker (el último `Ack` espera al aplicador, y la tarea cortada termina antes).
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia): en verde en su primera ronda, sin hallazgos Altos ni Medios. El lote implementa DC26 y DC29 en lo que le toca y nada más, y los desvíos de la nota son precisiones que no contradicen DC21, DC24 a DC31 ni DC36. No rompe ninguna invariante (K1, K2, K6, K7, K12, K15, K17), y los checklists se cumplen. Sus cinco Bajos, arreglados en el lote:
  - el test de varios clientes dependía de que ningún primer heartbeat cayera en el instante de las escrituras (lo reprodujo con el primer heartbeat fijo en ese instante, una vez en unas 20.000 corridas): ahora admite un sync por cada heartbeat de ese lapso, y pasa con su reproducción;
  - dos reglas sin test: las operaciones más allá del head (un caso más en el test de los fabricados) y el head visto antes del aplicador (un test nuevo, del revisor); con ellos, sus dos mutaciones mueren;
  - la interacción con el `Offline` de DC36, que faltaba en la nota (arriba), anotada para la revisión de cierre de 4.4;
  - la equivalencia de "un error que nunca detiene el worker", que en `manual` espera un heartbeat (arriba);
  - los enunciados de INV-K15 e INV-K17, que no nombraban la aplicación de un evento.

### Lote 9 · El arnés de simulación con el worker y el SSE (`zemdb-client`, tests) · ✅ Hecho
**Objetivo:** las invariantes del cliente con el worker en marcha, en ejecuciones al azar.

**Pendiente del Lote 7** (de su verificación con el primer heartbeat fijo): tres tests de los Lotes 4 a 6 fallan cuando el primer heartbeat cae en el último milisegundo del intervalo, porque el reloj de Tokio lo redondea al mismo instante en que termina la espera del test (`a_reader_that_syncs_only_with_its_worker_stays_connected_past_a_prune_and_is_designated`, `a_snapshot_larger_than_the_limit_and_an_error_of_the_storage_do_not_stop_the_worker` e `in_manual_what_another_client_writes_reaches_the_replica_with_a_sync_or_a_write`): una vez en unas 10.000 corridas cada uno. Que esperen el estado que miden, como `synced_to`, y que pasen con el primer heartbeat fijo a 1 ns del final del intervalo.

**Qué se hace:** el transporte de la simulación (`Link`, en `tests/simulation_tests.rs`) implementa `events`, y el arnés de 4.2 (§5.3 del plan de la fase) suma clientes con worker, en un modo al azar que cambia en el medio; el SSE por el transporte que inyecta fallas (cortar el stream, perder eventos, demorarlos, mandar `lagged`, callar); tokens que vencen, fabricados como dice §2 (*Pruebas*): el proveedor de prueba da a veces tokens ya vencidos, y el transporte termina streams y responde `Unauthorized`; y el SSE apagado en algunos reinicios del servidor. El reloj pausado hace correr los intervalos.

**Tests exigidos.**
- En cada paso, cada réplica es igual al oráculo en su head (INV-K1), y cada escritura termina con una de las respuestas de INV-K2.
- Al cerrar, con la red curada, los clientes con worker en `periodic` o `live` llegan al head sin ningún `sync` del test, en a lo sumo una cantidad fija de intervalos (INV-K7); los de `manual`, con un `sync`.
- Ningún worker se detiene salvo por `FailedClosed` o `RoomClosed` (INV-K17); los descartes, por la época de `status`, solo en los casos de INV-K3, como en 4.2; y lo retenido no pasa de las cotas de INV-K15.
- Una versión corta en la suite y una larga con `#[ignore]`.

**Interacciones a revisar:** los reinicios del servidor con streams abiertos (el apagado los corta); las tareas del worker al reiniciar el servidor (el arnés las espera, como a los pedidos); el oráculo por encarnación.

**Documentos:** `INVARIANTES.md`: K1, K2, K3, K4, K7, K15, K17 (*Automatizable*).

**Entrega:** `test(client): the simulation harness with the worker and the event stream (Fase 4.3, Lote 9)`.

**Hecho** (17 de Noviembre de 2026).
- **Qué entró.** Solo tests: ninguna línea de producción.
  - **El transporte de la simulación, con el stream.** `Link` implementa `events` sobre el router en proceso. Mientras la red tiene fallas, una apertura no tiene respuesta (una de cada 20), se rechaza con `Unauthorized` porque su token vence en ese momento, o con `RateLimited`. En cada bloque del stream abierto: el bloque se pierde, o se demora hasta 3 s, con los que siguen (dos de cada 100 cada una); se reemplaza por `lagged`; la conexión se corta; el stream calla (el worker lo da por muerto a los 45 s); o vence su token, y el stream termina (una de cada 100 cada una). El stream de un cliente sin red se corta, y no abre otro. Cuenta los `ops` que llegan y cada falla que inyecta.
  - **Los tokens que vencen.** La red toma por vencidos los tokens que vencieron: un pedido o una apertura con uno reciben `Unauthorized`, como del servidor (sin correlación), sin llegar a él. Uno de cada 20 pedidos con fallas vence su token en el momento. El cliente lo renueva con su proveedor, que cada tanto (un 5 % por paso, uno por vez) da uno ya vencido, con un TTL de 0, que el servidor rechaza. Una escritura con `Unauthorized` se repite con el token nuevo y el mismo `MutationId`. Cada `open_room` da un token propio (un segundo más de vigencia por apertura), que reemplaza al vencido.
  - **El servidor que se reinicia** no toma pedidos ni aperturas mientras tanto (los workers mandan los suyos en cualquier momento), y se reinicia cuando terminaron los que tomó y las aperturas en camino, con un tope de 10 minutos. Ni el servidor caído ni un cliente sin red responden nada, tampoco una respuesta fabricada. Apagar sus salas termina sus streams. Con workers, uno de cada tres reinicios apaga el SSE (`max_sse_streams` en 0), y el siguiente lo vuelve a encender.
  - **Los clientes con worker.** De los cuatro iniciales, los dos de B: el de disco en `live` y el de memoria en un modo al azar (`live` la mitad de las veces); y uno de cada dos de los que se suman a la sala podada. Heartbeat cada 10 s, `retry_max` de 10 s y `events_retry` de 60 s. Una de cada diez acciones de un cliente con worker le cambia el modo, y las demás son las de 4.2 (escrituras, syncs, heartbeats, reaperturas), también cortadas y con caídas. Después de cada paso, el arnés vuelve a arrancar el worker que una caída o un cierre detuvo, en el modo que tenía, como una aplicación. Con workers, cada paso deja correr un tiempo al azar, de hasta un intervalo.
  - **Después de cada paso,** además de lo de 4.2: ningún worker se detuvo, salvo uno que un cambio de modo del mismo paso arrancó mientras el paso cerraba la sala, sin caídas, que se detiene con `RoomClosed` (DC29: lo cerró la aplicación). La réplica se lee hasta que su head y su época son los mismos antes y después de leerla, porque el worker la cambia en cualquier momento. Los descartes se cuentan como en 4.2 (los metadatos leídos antes de cada apertura), y una reapertura de una réplica de una encarnación anterior puede descartar una vez más en su paso (lo que no usa, no queda). Lo que una escritura pudo saltear (la época que cambió, un snapshot bajado) se mira cuando termina, no después del tiempo libre.
  - **El cierre.** Antes de curar la red, la aplicación pone los workers en `periodic`, `live` y `manual`, por turno, y con su sala abierta, quedan sin red mientras los demás escriben, hasta que el log pasa diez secuencias el head más alto de los workers, y el log se poda. Con la red curada, los de `periodic` y `live`, todos detrás del head al volver, llegan a él sin ninguna llamada del test (salvo abrir la sala), en a lo sumo 12 intervalos (`HEAL_INTERVALS`), y cada uno de los que quedaron detrás del log baja un snapshot que sube otro cliente. Mientras tanto, los demás, también el archivista, mandan un heartbeat por intervalo, como su aplicación, para que el servidor tenga a quién designar. Después, otra vez sin red, mientras los demás escriben, sin poda: vuelven por un sync, con la misma cota. Al final, el cierre de 4.2, en el que los de `manual` sincronizan. Además, algún `ops` llegó a un cliente, y los streams o sus aperturas tuvieron fallas; la versión larga verifica que hubo cada falla del stream, de sus aperturas y de los tokens, y un token ya vencido del proveedor que el servidor rechazó, en el conjunto de sus semillas.
  - **Los tres tests pendientes del Lote 7.** `a_snapshot_larger_than_the_limit_and_an_error_of_the_storage_do_not_stop_the_worker` e `in_manual_what_another_client_writes_reaches_the_replica_with_a_sync_or_a_write` esperan la cantidad de pedidos que miden (`sent_count`, con un tope). `a_reader_that_syncs_only_with_its_worker_stays_connected_past_a_prune_and_is_designated` le da al lector un intervalo de 10 s, para que su primer heartbeat llegue antes del primer mantenimiento del servidor, a los 30 s, que lo dejaba detrás del log cuando el primero caía al final de un intervalo de 30 s; y espera el head del lector (`synced_to`) y el del cliente atrasado. Pasan con el primer heartbeat fijo a 5 ms y a 1 ns del final del intervalo; solo falla, como se espera, `a_worker_sends_a_heartbeat_every_interval_the_first_at_random_within_the_first`.
- **Tests.** En `tests/simulation_tests.rs`, `replicas_converge_through_faults_restarts_pruning_and_schema_changes` y `rankings_converge_through_faults_restarts_pruning_and_schema_changes` corren ahora con workers (96 pasos), y `replicas_converge_over_many_long_simulations` (16 semillas de 400 pasos de cada esquema, `#[ignore]`) también; en release, `cargo test -p zemdb-client --release -- --ignored` pasa, con las dos simulaciones largas en unos 100 s. Una semilla repite los pasos: el generador sortea antes de mirar lo que depende de las fallas (las renovaciones del token). No repite las escrituras de antes de curar la red, que siguen hasta que entran las suficientes. Las de fuzzing y el escenario de la app siguen sin workers: el fuzzing de los eventos es del Lote 10.
- **Repeticiones.** Con el arnés final, 480 simulaciones de 96 pasos (seis tandas de 40 semillas de cada esquema, `SIM_SEED` y `SIM_SEEDS`), sin fallas: en las 960 vueltas de los workers, la llegada al head tardó 8 s de mediana, 20 s en el percentil 99 y 49 s como máximo, contra una cota de 120 s; en cada vuelta con poda había workers detrás del log (1411 en las 480), y cada uno bajó su snapshot. Antes de los arreglos de la revisión, otras 480 en verde, y después de los de su segunda ronda, 240 más. Las corridas de antes, unas 720, encontraron los tres problemas del arnés de *Desvíos*; y una de las tandas finales, que la versión corta no siempre tiene fallas en los bloques del stream (pide alguna del stream o de una apertura). La suite de la simulación tarda unos 5 s en debug, como antes.
- **Mutaciones.** No hay reglas de producción que deshacer: se rompió el cliente en lo que la simulación verifica, una regla por vez, hasta tres corridas de las dos simulaciones cortas cada una (`mut9/mutate.py`). Mueren ocho, de doce:
  - cualquier error de un paso detiene el worker (ningún worker se detiene);
  - `periodic` no sincroniza por el heartbeat (no vuelven al head solos, en la vuelta sin poda);
  - el heartbeat no baja el snapshot anunciado (no vuelven al head solos, detrás del log);
  - el worker nunca abre el stream (ningún `ops` llegó);
  - la renovación nunca da un token (no vuelven al head solos);
  - y las tres de la revisión de delta: `EventsDisabled` detiene el worker como `RoomClosed`; un `Unauthorized` deja incierto el intento (en la segunda corrida: una escritura termina incierta sin un intento incierto); y cerrar la sala borra sus metadatos (una réplica sana descartada).

  Sobreviven cuatro, que la simulación sin fuzzing no distingue:
  - el designado no sube por el ack de su heartbeat: sube igual por el evento `snapshot_wanted` o por el lote de un sync que lo pide, que llevan a un heartbeat que sube (el camino del ack lo prueban `a_reader_that_syncs_only_with_its_worker_stays_connected_past_a_prune_and_is_designated` y `in_manual_the_worker_still_uploads_the_snapshot_and_downloads_it`);
  - un `ops` de otra encarnación se aplica: el sync de después de abrir el stream de la sala recreada registra la réplica, que se descarta, antes de que la sala nueva pase su head;
  - un evento de un stream de otra época se usa, y uno con operaciones que no son contiguas también: el aplicador los vuelve a cubrir (la época, el prefijo y el hueco), y los eventos del servidor siempre son contiguos.

  Los cuatro los prueba `live_tests.rs` (Lote 8), y los eventos fabricados son del Lote 10.
- **Interacciones.**
  - **Los reinicios del servidor con streams abiertos:** el apagado de las salas termina sus streams, y el worker abre otro, con un backoff, cuando el servidor vuelve; las aperturas que fallan mientras tanto no tienen respuesta. El arnés espera las aperturas en camino antes de reiniciar: una apertura sobre el servidor viejo abriría la sala otra vez, con sus archivos, junto al nuevo.
  - **Las tareas del worker al reiniciar el servidor:** sus pedidos corren en las tareas del transporte, que el arnés espera como a los pedidos de los pasos; los que llegan mientras tanto no tienen respuesta, y el worker los reintenta.
  - **El oráculo por encarnación:** sin cambios; los eventos de la sala recreada no se aplican (del Lote 8), y la réplica se compara con la encarnación de sus metadatos.
  - **El descarte en medio de un paso:** el registro del worker, con `RoomReset`, descarta la réplica en el medio de un paso, y una reapertura del mismo paso la abre sin metadatos (y la vuelve a vaciar): dos épocas por una encarnación, que el conteo de 4.2 (los metadatos leídos antes del paso) no admitía. Falló una vez en unas 80 corridas. El conteo admite ahora un descarte más por cada reapertura de una réplica de una encarnación anterior, en su paso, y nada más: un primer arreglo, que contaba cada lectura sin metadatos del cliente, aceptaba también una réplica que perdía sus metadatos al cerrarse (la revisión de delta lo mostró con una mutación).
  - **Un cambio de modo y un cierre a la vez:** `set_freshness` toma el aplicador entre la detención del worker y el cierre de la sala, ve la réplica abierta y arranca un worker, que se detiene con `RoomClosed` en su próximo paso. La sala reabierta queda con ese worker detenido, como dice DC29 ("arrancar otra vez el worker solo retoma una sala que la aplicación reabrió"); el arnés lo vuelve a arrancar. Una vez en unas 240 corridas.
  - **Leer la réplica con el worker en marcha:** `rows` puede fallar por un descarte en curso; la lectura se repite.
  - **El cliente caído con tareas del worker:** las tareas que retienen al cliente terminan antes de que suelte sus metadatos; el arnés espera hasta 60 s para reabrirlo (eran 2 s).
  - **La reasignación de esquema con workers** (`Sim::reassign`, sin detener la red): apagar la sala corta sus streams, y un pedido de un worker en el medio puede levantarla otra vez con el esquema viejo; el arnés repite, hasta 100 veces. Los streams cortados se reabren, y la réplica toma el esquema nuevo por el registro (Lote 7).
  - **La sala recreada y la columna agregada con pedidos y streams de workers en vuelo:** el borrado detiene la sala y termina sus streams, y los pedidos en vuelo reciben `RoomNotFound` o la sala nueva, que el registro descarta (`RoomReset`); una columna agregada llega como `schema_reloaded`, o por el registro de un sync.
  - **Los cambios de modo cortados:** `set_freshness` toma el modo bajo el aplicador, así que uno cortado lo cambió o no; el arnés lee el modo de `status` antes de volver a arrancar el worker.
  - **El token de una reapertura mientras corre una renovación:** el `open_room` reemplaza el token (Lote 3), y la renovación que termina después no lo pisa.
- **Desvíos.**
  - **Tres problemas del arnés, no del cliente,** que encontraron las corridas: las dos interacciones nuevas de arriba (el descarte en medio de un paso y el cambio de modo durante un cierre), en las que el cliente cumple el contrato y el arnés ahora las admite, solo en esos casos; y unos syncs de los clientes sin worker antes del cierre, sin heartbeats, que no terminaban cuando uno de ellos esperaba un snapshot que nadie estaba designado a subir (se sacaron: los heartbeats del cierre designan).
  - **La cota de K7** es de 12 intervalos: el próximo heartbeat (con un backoff y un token renovado), la designación de quien sube el snapshot (otra a los 60 s si la primera no empieza), la subida, el heartbeat que lo anuncia y el sync. Lo medido, 49 s como máximo, deja margen para la designación que no empieza.
  - **Los clientes sin worker mandan heartbeats** mientras los workers se ponen al día: sin un cliente designable al día, ninguno podría subir el snapshot (INV-R4); el archivista también, porque está en el head.
  - **Dos vueltas en el cierre.** La primera, con poda, trae los workers por un snapshot, que los deja en el head; la segunda, sin poda, por un sync. Con una sola, `periodic` sin sync sobrevivía.
  - **Los modos del cierre los pone la aplicación** (`periodic`, `live` y `manual`, por turno), para que la red se cure con cada modo; los pasos de antes los eligen al azar.
  - **El fuzzing sigue sin workers:** el de las respuestas con workers y el de los eventos son del Lote 10.
  - **Una verificación que se sacó:** que algún cliente con worker suba un snapshot. El servidor designa también a los clientes sin worker, y en una de unas 1200 corridas no designó a ningún worker; no mataba ninguna mutación (la de la subida por el ack del heartbeat sobrevive igual).
- **Revisión de delta:** no verde en su primera ronda, con dos Medios, los dos del arnés: el conteo de los descartes, más débil que el de 4.2 (aceptaba una réplica que perdía sus metadatos al cerrarse), y una semilla que ya no repetía los pasos (el generador sorteaba según las renovaciones del token, que dependen de las fallas). Y ocho Bajos: la excepción del worker detenido, más amplia que lo que dice K17; lo que una escritura pudo saltear, mirado después del tiempo libre; `settle` y la reasignación, sin tope; respuestas fabricadas sin red o con el servidor caído; el mismo token de `open_room` en toda la corrida; el cierre, que no verificaba que los workers estuvieran atrasados ni que bajaran el snapshot, ni que hubiera fallas del stream; interacciones que faltaban; y la nota. Todo arreglado en el lote (arriba). Verde en su segunda ronda, con cuatro Bajos, también arreglados: el descarte de más de una reapertura, que quedaba para el resto de la vida del cliente (ahora vale en su paso); los heads de los workers con la sala cerrada, leídos como 0 antes de dejarlos sin red (ahora se abre su sala antes); las escrituras del cierre, que una semilla no repite (dicho en el módulo); y el token vencido del proveedor, contado al pedirlo (ahora, cuando el servidor lo rechaza).
- **Al backlog** (Bajo: `INVARIANTES.md` §1.2; lo abrió el dueño del proyecto al cerrar el lote):
  - **DEF-199 · Bajo · (b) · L1.**
    - **Qué pasó:** `a_panic_in_a_file_operation_stops_the_room_and_the_next_request_reopens_it` (`crates/server/src/actor/tests/manager.rs`, del Lote 3 de la Fase 3.12) falló tres veces en `cargo test --workspace`, siempre con otras compilaciones o tests corriendo en paralelo: en los Lotes 3 y 4 de 4.4 y en el Lote 8 de 4.3. La vez que quedó registrado el error, el commit respondió `Timeout` en lugar de `Internal`. Solo pasa (20 de 20, y el binario entero).
    - **Lo que se sabe:** el test corre con el reloj real, y `RoomManager::ask` le da al actor `ACTOR_TIMEOUT` (5 s) para tomar y responder el comando. Con la máquina cargada, el pánico en el hilo de E/S y su vuelta al actor pueden pasar de ese plazo, y el pedido termina con `Timeout`. También es un código documentado y reintentable, y el pedido tiene su respuesta en tiempo acotado: INV-L1 se sostiene. Lo que falla es la precisión del test, no la invariante.
    - **Fix:** que el test no dependa de la carga. Una opción es el reloj pausado: el pool de E/S corre cada operación con `spawn_blocking`, y mientras corre el plazo del actor no avanza. Otra es aceptar `Timeout` además de `Internal`, verificando igual que el pedido siguiente reabre la sala.
    - **Test:** el mismo, con la operación de archivo demorada más que `ACTOR_TIMEOUT` (un `sleep` en el hook antes del pánico), que hoy falla con `Timeout`.

### Lote 10 · Robustez: fuzzing de los eventos (`zemdb-client`, tests) · ✅
**Objetivo:** INV-K12 e INV-K17 con eventos que no son del servidor.

**Qué se hace:** en el arnés, la red reemplaza bytes del stream: bytes al azar, eventos dañados, líneas sin fin, y un `ops` con un mensaje al azar de los generadores de `crates/core/tests/common`, de otra sala o de otra encarnación. Además, el fuzzing del parser a través del transporte HTTP, contra un servidor del test que escribe esos bytes. No fabrica un evento que el cliente no distingue de uno del servidor (operaciones que siguen al cursor, con la sala, la encarnación y el esquema de la réplica): queda fuera de alcance (`INVARIANTES.md` §2).

**Tests exigidos.** Ningún pánico, tampoco en una tarea; cada réplica es igual al oráculo en su head; ningún worker se detiene salvo por DC29; al final, con la red curada, todos llegan al head.

**Documentos:** `INVARIANTES.md`: K12, K17.

**Entrega:** `test(client): fuzzing of the event stream (Fase 4.3, Lote 10)`.

**Hecho** (18 de Noviembre de 2026).
- **Qué entró.** Solo tests: ninguna línea de producción.
  - **El fuzzing de la simulación, con los workers.** Las dos simulaciones de fuzzing corren ahora con los clientes con worker del Lote 9, en sus modos al azar, con las fallas del stream y de los tokens, y con las respuestas fabricadas del Lote 11 de 4.2. Se renombraron: `no_reply_or_event_in_place_of_the_servers_panics_a_client_or_moves_its_replica_off_the_log` (96 pasos de cada esquema) y `no_reply_or_event_in_place_of_the_servers_panics_a_client_over_many_long_simulations` (16 semillas de 400 pasos de cada uno, `#[ignore]`).
  - **Los bloques del stream que no son del servidor.** Mientras la red hace fuzzing, en lugar de uno de cada cinco bloques del stream pone uno de estos:
    - bytes al azar: mayormente pedazos de la sintaxis del stream, números y frames de los generadores en base64;
    - el evento dañado (`generators::mutate`);
    - su frame dañado y vuelto a base64, que decodifica más seguido;
    - una línea sin fin, de hasta el doble del tope de 64 KiB;
    - un `ops` con un mensaje al azar de los generadores de `crates/core/tests/common`;
    - el `ops` del servidor con operaciones de la sala que el log no tiene en sus secuencias (las de otros eventos), de otra sala o de otra encarnación;
    - el `ops` del servidor con una de sus operaciones en su tabla y con su clave, pero con datos al azar, que no valida;
    - otro evento con datos al azar: `head_advanced`, `snapshot_available`, `schema_reloaded`, `snapshot_wanted`, `lagged` o uno que el cliente no conoce.
  - **Las negativas al azar.** En lugar de una de cada cinco aperturas que llegan al servidor, la red pone una negativa al azar: bytes al azar, la negativa dañada, un mensaje al azar o un error de la sala con cualquier código (`EventsDisabled`, `Unauthorized`, `Forbidden`...).
  - **Lo que queda afuera** (`INVARIANTES.md` §2, DC26): un `ops` que el cliente no distingue de uno del servidor.
    - **Cómo se detecta.** Un modelo de las líneas que lee el cliente (`Lines`) mira cada línea `data:` que termina el bloque fabricado, también la que forma con lo que vino antes. Para el modelo, un fin de línea es `\n`, `\r` o los dos, y lo que pasa de 64 KiB no cuenta. El bloque no se fabrica, y pasa el del servidor, si esa línea es base64 de un `OpsEvent` con todo esto (`event_lies`):
      - de la sala del stream;
      - con operaciones en orden que llegan a su head;
      - de una encarnación que la red no falsificó;
      - con un tramo final, desde alguna de sus secuencias, que valida con un esquema de la sala y trae alguna operación que no es del evento del servidor que reemplaza. Es lo que el cliente aplicaría después del head de una réplica: descarta lo que la réplica tiene y valida el resto.
    - **Los bloques del servidor** empiezan con `event:` o `:`, así que no completan una línea `data:` en base64.
  - **Lo que se verifica,** además de lo del Lote 11 de 4.2 y del Lote 9:
    - ningún worker se detiene, salvo el caso de DC29 del Lote 9;
    - ninguna sala falla cerrada;
    - cada réplica es igual al oráculo en su head, también con los `ops` falsificados, que solo la sala o la encarnación delatan, y con las operaciones que no validan;
    - al final, con la red curada, los workers llegan al head como en el Lote 9, y todos llegan al head;
    - algún bloque fabricado llegó a un cliente; la versión larga verifica que hubo cada clase y alguna negativa al azar.
  - **El cierre con fuzzing.** La red deja de hacer fuzzing antes del cierre del Lote 9 (`Sim::unfuzz`, lo que antes hacía `heal`): cierra la sala de cada cliente, salvo el archivista, y con eso detiene los workers, que el cierre vuelve a arrancar. Así la cota de K7 se mide como en el Lote 9.
    - **Las salas:** el cierre abre la sala de todos, no solo la de los workers, antes de dejarlos sin red, para que los demás escriban.
    - **Las escrituras:** los demás escriben hasta que el log pasa en diez secuencias el head más alto de los workers y también su propio head de antes, para que la poda tenga un segmento sellado que tirar. Con fuzzing, una réplica en memoria vuelve vacía de `unfuzz`, y el head más alto de los workers podía quedar más de diez detrás del log: nadie escribía y la poda no podaba ("The log was not pruned", en dos de cuatro corridas de una semilla).
  - **El fuzzing del parser a través del transporte HTTP** (`crates/client/src/tests/events.rs`, módulo `over_http`).
    - **El servidor del test** escribe los streams al azar y los eventos del servidor dañados del Lote 2 como el cuerpo de una respuesta con HTTP 200, en pedazos de largos al azar. Lo enmarca de una de estas formas:
      - en chunks, con su último chunk, o cortado sin él;
      - con su `Content-Length`, entero, o cortado antes;
      - hasta cerrar la conexión.
    - **Lo que se verifica.** A través de `HttpTransport::events` y el `EventReader` del worker, el lector da los eventos y el error del modelo del formato del Lote 2. Después da el fin del stream o, si se cortó, un error del transporte. Cada evento decodifica o es un error tipado.
    - **Las dos versiones:** corta en la suite (100 streams) y larga con `#[ignore]` (20 semillas de 500).
- **Tests.**
  - **En `tests/simulation_tests.rs`:** los dos de fuzzing de arriba, con workers. Los del Lote 9, sin fuzzing, siguen igual, salvo que el cierre abre la sala de todos.
  - **En `src/tests/events.rs`:** `streams_at_random_are_read_through_the_http_transport_as_the_model_reads_them` (100 streams, 0,3 s en debug) y su versión larga con `#[ignore]` (10.000 streams, unos 25 s en debug).
  - **Tiempos:** la suite de la simulación tarda unos 12 s en debug.
  - **En release,** `cargo test -p zemdb-client --release -- --ignored` pasa, con las dos simulaciones largas en unos 2 minutos.
- **Repeticiones.** Con `SIM_SEED` y `SIM_SEEDS`:
  - **Antes de la variante de la operación que no valida y de los dos arreglos del arnés** (DEF-200 y las escrituras del cierre), dos tandas de 40 semillas de las cuatro simulaciones cortas de cada esquema: 160 de fuzzing con workers y 160 sin fuzzing, en verde, unos 275 s cada tanda.
  - **Con el código final,** dos tandas más, también en verde, y las largas en release.
  - **Cada simulación de fuzzing corta,** con 30 a 250 `ops` del servidor que llegan a un cliente:
    - entre 25 y 90 bloques fabricados y entre 7 y 28 negativas al azar;
    - hasta unos 15 `ops` falsificados de otra sala o de otra encarnación;
    - de 1 a 3 con una operación que no valida.
  - **El filtro** dejó afuera una o dos falsificaciones cada ocho simulaciones.
  - **Por HTTP:** la versión larga, 10.000 streams en unos 25 s en debug, y en release, en verde.
- **Mutaciones.** No hay reglas de producción que deshacer: se rompió el cliente en lo que el fuzzing verifica, una regla por vez, hasta tres corridas cada una (`mut10/mutate.py`). La simulación corrió la de fuzzing corta; el parser y el transporte, la de HTTP corta. Mueren las nueve, en su primera corrida:
  - **En la simulación:**
    - un `ops` de otra sala se aplica (la réplica deja de ser igual al oráculo);
    - un `ops` de otra encarnación se aplica (lo mismo; en el Lote 9 sobrevivía, sin eventos falsificados);
    - una operación de un evento que no valida hace fallar cerrada la sala;
    - una línea demasiado larga o bytes que no son UTF-8 detienen el worker;
    - una negativa que no decodifica detiene el worker.
  - **A través del transporte HTTP:**
    - el lector toma un error del transporte como el fin del stream;
    - el transporte toma un cuerpo cortado como su fin;
    - el parser no tiene tope por evento;
    - un `\r\n` partido entre dos bloques cuenta como dos fines de línea.
  - **Una regla que el fuzzing no detecta:** un `ops` de otra encarnación registra la réplica, y es el registro el que la descarta. Una mutación que descarta en el momento sobrevive a la simulación de fuzzing, que no verifica los descartes. La mata `the_events_of_the_room_created_again_are_not_applied_and_the_replica_is_discarded` (`live_tests.rs`, Lote 8). La probó la revisión de delta.
  - **Dos mutaciones del arnés.** Sin el filtro, la simulación sigue en verde: lo que deja afuera son una o dos falsificaciones cada ocho simulaciones, casi todas frames dañados en su encarnación o en la versión de su esquema, que el cliente también rechaza. El filtro es conservador. Sin el filtro y con el `ops` falsificado en la sala del stream, la simulación falla en su primera corrida: el cliente aplica lo que no distingue, y la réplica deja de ser igual al oráculo. El filtro hace falta, y la comparación con el oráculo ve lo que dejaría pasar.
- **Interacciones.**
  - **El filtro y los chequeos del cliente** (`worker_apply` y `Local::apply`). Lo que el filtro deja afuera es un superconjunto de lo que el cliente aplicaría sin distinguirlo de lo del servidor:
    - la sala del stream;
    - las operaciones en orden que llegan a su head;
    - la encarnación: cualquiera que la red no falsificó, también una dañada;
    - cualquier esquema y versión;
    - un tramo final que valida con algún esquema de la sala: el cliente valida entero lo que sigue al head de su réplica, antes de aplicarlo. Que el filtro mire todo el evento lo encontró la revisión de delta (abajo).
  - **La línea en curso entre bloques.** Un bloque fabricado que deja una línea abierta la junta con el bloque siguiente, y el filtro mira las líneas que el bloque termina con lo que vino antes. Un bloque del servidor que sigue empieza con `event:` o `:`, que no son base64. Una línea más larga que el tope no cuenta: el cliente corta el stream con un error.
  - **Los `ops` falsificados y el oráculo.** Sus operaciones son de la sala, validan y no son las del log en esas secuencias. Si el cliente no mirara la sala o la encarnación, la réplica dejaría de ser igual al oráculo.
  - **Las negativas al azar y el token.** Ninguna detiene el worker:
    - un `Unauthorized` renueva el token con el proveedor, que a veces da uno vencido;
    - un `Forbidden` lo marca;
    - un `EventsDisabled` deja el worker como en `periodic` hasta `events_retry`.

    El cierre de las salas de `unfuzz` deja un worker nuevo, sin esa espera.
  - **Los avisos fabricados.** `head_advanced` y `lagged` llevan a un sync. `snapshot_wanted` lleva a un heartbeat, que sube solo si el servidor designó al cliente. `snapshot_available` lleva a una bajada solo si la réplica espera un snapshot. `schema_reloaded` lleva a un registro. El head de un aviso no es un head visto.
  - **El stream reabierto.** Después de una línea demasiado larga o de bytes que no son UTF-8, el worker abre otro stream con su backoff. El servidor deja dos streams por cliente y sala, y el tercero reemplaza al más viejo.
  - **`Sim::isolate`** abre la sala de todos: con fuzzing, `unfuzz` las cerró. Sin fuzzing cambia solo una sala que una reapertura que falló dejó cerrada.
  - **Los clientes de la fachada del Lote 9 de 4.4,** que entró antes a `main`. Están solo en las simulaciones con workers y sin fuzzing (`workers && !fuzz`), como en ese lote: sus verificaciones (lo que termina cada escritura, el último valor de cada stream) no están hechas para respuestas fabricadas. El fuzzing con workers corre con los clientes del núcleo, y `simulate` verifica los casos de la fachada solo sin fuzzing.
  - **El contador de pánicos.** El fuzzing por HTTP corre en el binario de los tests unitarios, otro proceso que la simulación.
  - **La caída y los tokens** (DEF-200). El cliente que vuelve de una caída conserva el proveedor de tokens: sus tokens nuevos no repiten los que la red tomó como vencidos.
  - **Las verificaciones del cierre con fuzzing.** `Sim::fuzz` sigue en `true` después de `unfuzz`, así que en el cierre de esas corridas no se verifican los resultados de las escrituras ni los descartes. Las corridas sin fuzzing los verifican.
  - **Las llamadas soltadas a los diez minutos,** ahora con los workers en marcha: lo que dejan a medias lo aplica el aplicador, como en el Lote 11 de 4.2, y los workers siguen.
  - **El head de un `ops` que no valida** es un head visto antes de validarse (DC26). En el fuzzing es el head real, así que no tiene efecto.
  - **El servidor del test por HTTP.** Responde con `connection: close`, así que el transporte no reusa la conexión. Un cliente que deja de leer, porque el modelo falla antes, cierra la conexión, y la escritura del servidor falla sin colgarse.
- **Desvíos.**
  - **Una variante más que las del plan:** el `ops` con una operación que no valida. Es la de K17 (*Cómo intentar romperla*) que llega a la validación del lote: de 48 frames dañados, 41 no decodificaban y 2 validaban.
  - **Las negativas al azar** en las aperturas del stream: son respuestas del servidor (K12), y las de los workers son del fuzzing de las respuestas con workers que dejó pendiente el Lote 9.
  - **Los `ops` de otra sala y de otra encarnación llevan operaciones de la sala que el log no tiene** (las de otros eventos), no las del evento. Con las del evento, aplicarlos no cambiaría nada, y las mutaciones que no miran la sala o la encarnación sobrevivirían.
  - **El fuzzing se detiene antes del cierre del Lote 9;** antes paraba en `heal`, cuando los workers ya estaban sin red. Así la cota de K7 se mide sin respuestas fabricadas.
  - **`simulate` siempre corre con workers;** el escenario de la app sigue sin ellos.
  - **El test por HTTP es unitario:** usa el lector del worker (`EventReader`) y el modelo del formato del Lote 2, que no son públicos.
  - **DEF-200, arreglado en el lote:** lo pidió el dueño del proyecto (abajo).
- **DEF-200 · Bajo · (b) · K7, arreglado en el lote.**
  - **Qué pasó:** `replicas_converge_over_many_long_simulations` (del Lote 9, sin fuzzing) falló en release con la cota de K7: un worker no llegó solo al head en 121 s, contra 120.
    - Las tres corridas completas antes del fix fallaron, en distintas semillas.
    - También falló con el arnés del Lote 9, en una de cuatro corridas de las semillas 1 y 2.
  - **La causa:** el arnés, no el cliente.
    - Después de una caída, `Sim::open` le daba al cliente un `TestTokens` nuevo, y un cliente de disco conserva su id.
    - Los tokens de un proveedor vencen a la hora más un segundo por cada pedido anterior a ese proveedor, para que no se repitan. El proveedor nuevo volvía a contar desde cero, y con el reloj de pared daba los mismos tokens que el anterior había dado segundos antes.
    - Esos tokens el anterior los había renovado justamente porque la red los había tomado como vencidos. Así, cada renovación daba otro token vencido, y el worker recibía `Unauthorized` en cada heartbeat por más de 12 intervalos.
    - En la realidad un token renovado vence más tarde: el cliente no tiene la culpa, y ninguna invariante se rompe.
  - **Fix:** la caída conserva el proveedor del miembro, el backend de su aplicación, y lo que le pidió (`Sim::crash`). El Lote 9 de 4.4, que entró antes a `main`, encontró y arregló lo mismo en su arnés, como un hallazgo de su revisión. Al rebasar queda su arreglo, equivalente a este.
  - **Test:** la misma simulación larga. Con el fix, seis corridas completas en verde (192 simulaciones de 400 pasos), con cada vuelta al head en menos de 20 s.
- **Revisión de delta:** verde en su primera ronda, con tres Bajos, arreglados en el lote:
  - **El filtro validaba el evento entero,** y el cliente valida solo lo que sigue al head de su réplica. Un `ops` con una operación que no valida en la secuencia del head, y después otra que valida y no es del log, pasaba el filtro, y el cliente la aplicaría: fallaría el arnés. No apareció en 60 simulaciones; la revisión lo probó llamando al filtro y con un test del cliente en una copia. Ahora el filtro mira cada tramo final del evento.
  - **K12 y K17** no decían que, con fuzzing, las salas se reabren antes de curar la red. Ahora lo dicen, y K17 aclara que no prueba que una sala salga sola de lo que dejaron los eventos fabricados.
  - **La mutación del `ops` de otra encarnación que descarta en el momento,** que el fuzzing no detecta, faltaba en la nota.

  Faltaban también cuatro interacciones, ahora en la lista: el tramo que el cliente descarta, las verificaciones del cierre con fuzzing, las llamadas soltadas con los workers en marcha y el head de un `ops` que no valida. Y K12 no nombraba la excepción de DC29. Con el filtro nuevo, una tanda más de 40 semillas de fuzzing en verde. Su segunda ronda, en verde: confirmó los arreglos (el filtro nuevo, con casos armados a mano y la simulación con contadores; la variante de la operación que no valida sigue llegando, y su mutación muere), con un Bajo de documentación, arreglado: el encabezado de la versión 42 no decía que las salas se reabren, y K12 decía "cada cliente", cuando `unfuzz` deja afuera al archivista.

### Lote 11 · Revisión de cierre y cierre · ✅ Hecho (su revisión de cierre no salió en verde: la siguen el 11b y el 11c)
- La revisión de cierre (§3). Si no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.3, cada uno con su revisión de delta.
- `INVARIANTES.md`: el estado de cada invariante revisada. El plan de la fase: 4.3 cerrada. `ROADMAP.md`: "Lo que sigue".

**Hecho** (18 de Noviembre de 2026).
- **La revisión de cierre** (§3), sobre `ea1cafe`, con dos revisores en paralelo, de contexto fresco y solo lectura, cada uno con sus corridas en un clon del repo:
  - **Los revisores:** Worker y réplica (K1, K4, K5, K6, K7, K11 y K17) y Transporte y robustez (K2, K3, K12 y K15).
  - **Lo que recibió cada uno:**
    - `INVARIANTES.md` (§1, §2, el resumen, §3.11 y §4) y `ARCHITECTURE.md` §11;
    - este plan y el de la fase (DC1 a DC37, §4.3 y §5);
    - la auditoría de 4.2, con su plan, y `GEMINI.md`;
    - el código y los arneses.
  - **El origen (a)** es un commit de 4.3: los de los Lotes 0 a 10, entre los que hay commits de 4.4 y de 3.12.
  - **El resultado: no salió en verde.** Se rompen INV-K2 ★ e INV-K12 ★, cada una con una Media de origen (a).
  - **Los tests de la revisión,** como parches contra `ea1cafe`, están en [`docs/audits/2026-11-fase4_3-cierre/`](../audits/2026-11-fase4_3-cierre/README.md).
- **Las rupturas:**
  - **INV-K2 ★ · Media · (a), del Lote 8 (`19a5ae2`).**
    - **El defecto:** `worker_apply` toma el head del evento como head visto después de las cuatro verificaciones de DC26, pero antes de validar sus operaciones.
    - **El escenario:**
      - llega un `ops` de la sala, de la encarnación y del esquema de la réplica, cuya operación en `h+1` no valida;
      - ese evento no cuenta como del servidor (§2 de `INVARIANTES.md`): el cliente lo verifica, y se registra otra vez;
      - aun así deja su head `F` como head visto;
      - una escritura que entra antes de que el sync que pide lo corrija (DEF-192) anota `seen = F`;
      - su `CommitAck` en `h+1` la termina en `OutcomeUnknown`, con la secuencia aplicada y su reporte, que no es ninguno de los casos (a) a (d) de K2.
    - **El alcance:** falla cerrada (la escritura queda incierta, y nada se pierde ni se inventa), y dura hasta ese sync.
    - **Por qué no lo vio el fuzzing del Lote 10:** pone esos eventos con el head real, y con fuzzing no verifica las escrituras inciertas.
    - **El test:** `review_b_an_ops_event_that_does_not_validate_leaves_no_head_seen_for_the_writes_after_it` da `Err(OutcomeUnknown)` en lugar de `Ok(())`.
    - Lo arregla el **Lote 11b**.
  - **INV-K12 ★, INV-K17 e INV-K7 · Media · (a), del Lote 7 (`fd09b6e`), con la validación de la configuración del Lote 4 (`0bb4167`).**
    - **El escenario:**
      - la aplicación configura `events_retry: Duration::MAX`, la forma habitual de decir "nunca", y `Client::open` lo acepta: solo rechaza el cero, y el rustdoc dice "Above zero";
      - llega un `EventsDisabled`: un servidor con el SSE apagado, o una negativa de la red, que cuenta como del servidor;
      - la tarea del worker entra en pánico al sumar el plazo al instante.
    - **Lo que deja:**
      - el worker queda detenido (`running: false`, sin `last_error`), sin heartbeats, sin syncs del worker y sin último `Ack`;
      - con la fachada, cada sala vigilada pierde su worker, y lo vuelve a perder cada vez que la fachada lo arranca.
    - **La severidad:** los dos revisores lo encontraron por separado.
      - Uno lo calificó Bajo: es una configuración de la aplicación, como el `Duration::MAX` del pool en el Lote 1.
      - El otro, Medio.
      - El dueño del proyecto eligió Medio (18 de Noviembre de 2026): el SDK acepta el valor como válido, y una respuesta del servidor lo hace caer.
    - **Los tests,** que fallan con el pánico:
      - `review_a_an_events_retry_beyond_the_clock_leaves_a_live_worker_running_as_in_periodic`;
      - `review_a_a_refusal_events_disabled_with_events_retry_beyond_the_clock_does_not_stop_the_worker`;
      - `review_b_an_events_retry_of_duration_max_does_not_stop_the_worker_of_a_room`.
    - Lo arregla el **Lote 11c**.
- **Lo que se sostiene,** con el argumento de cada revisor:
  - **K1 ★.**
    - `worker_apply` sigue el orden de DC26. Mira la época al leer el evento y con el lock del estado, sin un `.await` entre la validación y el head visto.
    - Bajo el aplicador, `Local::apply` repite la época, la falla cerrada y el esquema, descarta lo que la réplica ya tiene y no aplica un hueco.
    - La encarnación se compara fuera del aplicador, y alcanza: en una época, una réplica abierta no cambia de encarnación.
    - Sin test propio: un `ops` de otro id de esquema. Lo cubre `changes`, probada en `schema_tests.rs`.
  - **K4 ★.** `Sim::check` compara cada réplica con el oráculo después de cada paso, también con los workers, las fallas del stream, los tokens que vencen, el SSE apagado y el fuzzing. Con la red curada, `heal` exige que todos lleguen al head.
  - **K5.**
    - El `Ack` periódico se lee bajo el aplicador, y sale solo si pasó del último. El último `Ack` tiene su plazo.
    - Todo lo que cambia la respuesta está atado a la época.
  - **K6 ★.**
    - Solo `Registered` cambia el esquema.
    - Una operación de un evento que no valida da `Schema`, nunca `FailedClosed`.
    - `schema_reloaded` deja la réplica sin registrar.
  - **K7, salvo `events_retry`.**
    - Se sostienen los heartbeats, el backoff, los modos, el stream, DEF-196, las transferencias en su tarea y la cota de la simulación.
    - Sin test, una carrera: si el `select!` no recogió todavía una transferencia que terminó, la que pide el heartbeat siguiente no arranca. Es de la clase anotada en el Lote 5, y cuesta a lo sumo un intervalo.
  - **K11.**
    - `close_room`, `shutdown`, `discard_room` y `stop_worker` pasan por la misma detención, que corta y espera las tareas, manda el último `Ack` y recién después cierra la sala.
    - Un segundo llamador espera el mismo fin.
    - Un worker que se arranca entre la detención y el cierre ve `RoomClosed`. Está argumentado, y con `shutdown` no tiene test.
    - Un cliente soltado no manda el último `Ack`.
  - **K17, salvo `events_retry`.**
    - Solo detienen al worker `FailedClosed` y `RoomClosed`.
    - `FailedClosed` sale de un `Registered` que no evoluciona, o de un `SyncBatch` después de que el mismo sync se volvió a registrar, nunca de un evento.
    - Lo demás se reintenta.
  - **K2 ★, salvo el `ops` que no valida.**
    - La renovación del token.
    - El head visto de DEF-192: `saw_below` solo lo baja.
    - El head de un `ops` genuino.
    - Una escritura mientras el worker sincroniza, aplica un evento, transfiere o se detiene.
  - **K3 ★.**
    - El worker descarta solo como en 4.2: por el registro, ante `RoomReset` o un `InvalidSequence` a un cursor, o por un daño local al reabrir.
    - Ninguno de sus caminos descarta una réplica sana: DEF-196, un `ops` de otra encarnación o de otro esquema, `schema_reloaded`, un `Registered` con otra encarnación y un snapshot que se instala.
    - `review_b_a_forged_ops_of_another_incarnation_keeps_a_sound_replica` prueba el `ops` falsificado de otra encarnación en una sala sana, que ningún test determinista probaba, y pasa.
  - **K12 ★, salvo `events_retry`.**
    - Se sostienen la respuesta acotada, el parser, los pánicos de las tareas (que se vuelven `Storage`), los tokens y el túnel del proxy.
    - El fuzzing pasó con semillas nuevas.
  - **K15.**
    - Hay una tarea de cada clase por sala, sin colas.
    - El stream se lee de a un evento.
    - Detener el worker corta y espera todas sus tareas.
- **Indeterminada, para K7:** lo que dejó el Lote 1, el verificador de la plataforma.
  - **Lo que hace cada sistema:**
    - En macOS, `SecTrustEvaluateWithError` es síncrono, y puede ir a la red sin tope.
    - En Windows, la revisión de la revocación acumula hasta 10 s de descargas.
  - **Por qué lo puede trabar:**
    - Las dos llamadas corren dentro del `poll` del handshake, y bloquean un hilo del runtime.
    - `connect_timeout` no las corta.
    - Con TLS 1.2, una verificación de más de 10 s cortaría la conexión cada vez, si el sistema no recuerda la URL caída.
  - **Lo que no toca:** K2. Un plazo que vence tarde corta igual, y el intento cuenta como incierto.
  - **Lo que faltó:** correrlo en esos sistemas contra un certificado cuya URL de OCSP o de CRL no responda.
  - Queda para la revisión que lo pueda correr (D90): la de cierre de la Fase 4, en 4.7.
- **Las corridas** de los revisores en `ea1cafe`:
  - **Las suites:** `worker_tests` (51), `live_tests` (47), `token_tests` (22), `http_transport_tests` (35), `events_tests` (2) y los 32 unitarios del parser, del transporte y del token, también los largos.
  - **La simulación:**
    - sin fuzzing, con las semillas 700 a 715;
    - con el fuzzing de los eventos, con las semillas 600 a 623, de cada esquema.
  - **El fuzzing del parser:** 40 semillas de 2000 corridas, y 20 de 500 a través del transporte HTTP.
- **La verificación del coordinador:** con los dos parches juntos sobre `ea1cafe`, fallan los cuatro tests de las rupturas y el del Bajo de K12, y pasan los demás, también los dos largos del parser.
- **Bajos corregidos en este lote** (documentos):
  - **INV-K12, *Sostenida por*:** sin `Content-Length`, el búfer de una respuesta crece hasta el doble de `MAX_FRAME_SIZE`, con reservas que abortan si no hay memoria. El texto decía que reservaba a lo sumo ese largo.
    - El test `review_b_a_response_without_a_length_reserves_at_most_a_frame` mide 32.768.000 bytes reservados para un frame de 16 MiB más 4.
    - Es proporcional, así que K12 se sostiene. Va con DEF-195.
  - **`ARCHITECTURE.md` §11, *Timeouts that do not cut a request that advances*, y el rustdoc de `HttpTransportConfig::connect_timeout`:** en macOS y Windows, la verificación del certificado corre dentro del handshake, y un temporizador no la corta.
  - **`ARCHITECTURE.md` §11, *The task*:** la aplicación de un evento retiene al cliente, y el worker de un cliente soltado se detiene también cuando esa aplicación termina pidiendo un sync. Ya lo decían los rustdoc, y lo prueba `live_tests.rs`.
- **A un lote nuevo** (un Bajo de tests): el plazo de conexión, sin test desde los Lotes 1 y 2, que lo dejaron porque en la CI no hay una dirección que descarte los SYN.
  - `review_b_a_tls_handshake_the_server_never_answers_is_cut_by_the_connect_timeout` lo prueba en loopback, contra un servidor `https://` que no contesta el `ClientHello`, y pasa.
  - Mata la mutación que quita el plazo, que sobrevive a los 35 tests de `http_transport_tests.rs`.
  - Lo trae el Lote 11c.
- **Una observación, sin hallazgo:** un evento leído en la misma vuelta en que el worker pasa a `manual` puede lanzar un sync en `manual`, porque `notice` no mira el modo. Es un sync de más, sin efecto.
- **Las propuestas,** que el dueño del proyecto adoptó el 18 de Noviembre de 2026:
  - **De los dos revisores:** en INV-K12 y DC28, ninguna configuración que aceptan `Client::open` y `HttpTransport::new` lleva a un pánico ni detiene un worker. Entra con el Lote 11c.
  - **Del revisor de transporte:** una precisión de DC26 y de INV-K2. El head de un `ops` es un head visto recién cuando sus operaciones también validan, así que un evento que no cuenta como del servidor no deja incierta ninguna escritura. Entra con el Lote 11b.
- **Desvíos:**
  - **Respecto de §3:** el revisor de transporte respondió también por el verificador de la plataforma, que el Lote 1 dejó para esta revisión y que toca a K7, del otro revisor.
  - **La compilación:** los revisores compilaron sin información de depuración, para que sus dos targets entraran en el disco.

### Lote 11b · El head de un `ops`, visto solo con sus operaciones validadas (`zemdb-client`) · ⬜
**Objetivo:** que un `ops` que no cuenta como del servidor no deje incierta ninguna escritura.
- La revisión de cierre del Lote 11 rompió ahí INV-K2 ★, con una Media de origen (a), del Lote 8.
- El dueño del proyecto eligió arreglarla en un lote nuevo (18 de Noviembre de 2026), y adoptó la precisión de DC26 que propuso la revisión.

**Archivos:** `crates/client/src/client.rs` (`worker_apply`) y `crates/client/tests/live_tests.rs`.

**Qué se hace:**
- **La validación de las operaciones.** `worker_apply` valida también las operaciones del evento con el esquema de la réplica (`Schema::validate_operation`), junto con las demás verificaciones de DC26 y antes de tomar su head como head visto.
- **Si una no valida:** el evento deja la réplica sin registrar y pide un sync, como uno de otro esquema, sin head visto y sin pasar por el aplicador.
- **La alternativa, descartada:** tomar el head después de aplicar. Se probó en un clon al cerrar el Lote 11, y rompe `a_write_that_waits_for_the_applier_when_an_event_comes_saw_the_head_of_the_event`, del Lote 8: una escritura que espera el aplicador mientras llega un evento genuino tiene que haber visto su head, para no retener sus rechazos.
- **La precisión de DC26** (§1), con lo que dicen INV-K2 y `ARCHITECTURE.md`.

**Tests exigidos:**
- **El de la revisión,** traído al repo con otro nombre y sin códigos.
- **El head de un evento genuino:** sigue siendo un head visto antes del aplicador. Es el test del Lote 8, sin cambios.
- **Los dos tests del Lote 8 que usan un evento cuya operación no valida,** ajustados:
  - `an_event_with_an_operation_that_does_not_validate_registers_and_syncs_and_never_fails_closed`, que esperaba un segundo registro, el que corregía el head visto;
  - `a_client_dropped_while_an_application_holds_it_ends_the_worker_once_it_ends`, que usaba ese evento para retener `apply_batch`.
- **La simulación con el fuzzing de los eventos,** con su clase "invalid operation", sin cambios en sus expectativas.

**Interacciones a revisar:**
- una operación de una versión anterior del esquema, que valida con el local (INV-E3);
- un esquema que cambia entre la verificación y el aplicador (`Local::apply` repite `changes`);
- una operación de una tabla que el esquema no tiene, que hoy llega a `apply_batch` como un error del storage;
- operaciones que la réplica ya tiene;
- el costo de validar cada operación dos veces.

**Documentos:**
- `INVARIANTES.md`: INV-K2 y su estado;
- `ARCHITECTURE.md` §11, *Its operations*;
- el rustdoc de `worker_apply`;
- las precisiones de DC26.

**Entrega:** `fix(client): the head of an ops event is a head seen once its operations validate (Fase 4.3, Lote 11b)`.

### Lote 11c · Ninguna configuración que acepta el cliente lleva a un pánico, y el cierre de 4.3 (`zemdb-client`) · ⬜
**Objetivo:** que ningún valor que aceptan `Client::open` y `HttpTransport::new` lleve después a un pánico, ni detenga un worker.
- La revisión de cierre del Lote 11 rompió ahí INV-K12 ★, INV-K17 e INV-K7, con una Media de origen (a), del Lote 7.
- El dueño del proyecto eligió arreglarla en un lote nuevo (18 de Noviembre de 2026), y adoptó la precisión de INV-K12 y DC28 que propusieron los dos revisores.

**Archivos:** `crates/client/src/worker.rs`, `config.rs`, `client.rs` (`Client::open`) y `http_transport.rs`, y sus tests.

**Qué se hace:**
- **El reintento del stream después de `EventsDisabled`** se suma al instante sin desbordar. Un `events_retry` que no cabe en el reloj no vuelve a intentar el stream mientras el worker no cambie de modo, como si dijera "nunca". O, si el paso 0 lo prefiere, es `Config` al abrir, como el `write_timeout` que no cabe en el reloj.
- **Cada plazo que aceptan `ClientConfig` y `HttpTransportConfig`, con su valor más grande:**
  - `write_timeout`, que ya es `Config` si no cabe en el reloj;
  - `backoff_base`, `backoff_max`, `retry_base` y `retry_max`: la espera con *full jitter* se acota en `u64::MAX` nanosegundos, que caben en el reloj;
  - `heartbeat_interval`, debajo de 60 s;
  - `connect_timeout` e `idle_timeout`;
  - `pool_idle_timeout`, que no pasa de 300 s.
  - Un plazo que hoy puede desbordar se acota como `events_retry`.
- **La precisión de INV-K12 y DC28** que adoptó el dueño del proyecto.

**Tests exigidos:**
- **Los de la revisión,** traídos al repo con otro nombre y sin códigos: el SSE apagado del servidor, y una negativa `EventsDisabled` de la red, con `events_retry: Duration::MAX`. El worker sigue, en `periodic`.
- **Cada plazo de la configuración en `Duration::MAX`:** el cliente abre o da `Config`, y nada entra en pánico. El worker sigue con sus heartbeats, las escrituras terminan y el transporte manda.
- **El plazo de conexión de la revisión:** un handshake de TLS que el servidor no contesta se corta en el plazo de conexión.

**Documentos:**
- `INVARIANTES.md`: INV-K12, K17 y K7, con su estado y el de cada invariante revisada;
- `ARCHITECTURE.md` §11;
- el rustdoc de `ClientConfig` y de `HttpTransportConfig`;
- DC28;
- el cierre de 4.3, que pasó a este lote: el plan de la fase, con 4.3 cerrada, y `ROADMAP.md`, "Lo que sigue".

**Entrega:** `fix(client): no configuration the client takes panics a task or stops a worker, and close Fase 4.3 (Fase 4.3, Lote 11c)`.

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

**Avance:** plan escrito el 4 de Noviembre de 2026. Lote 0 (el contrato) cerrado el 6 de Noviembre, con la revisión cruzada en verde en su quinta ronda: las precisiones de DC18 y de DC24 a DC26 y DC27 a DC31, como *Propuestas*, y `INVARIANTES.md` en la versión 16. El dueño del proyecto las adoptó el mismo día, con dos ajustes (la frase de DC26 sobre el camino normal sin `/sync` y la verificación del `ClientId` en el Lote 3). Lote 1 (el transporte HTTP nativo) cerrado el 7 de Noviembre de 2026: S4 respondido en DC27 (compila con `aws-lc-rs` y el MSRV; el plazo de inactividad lo arma el transporte), la revisión de delta en verde en su cuarta ronda e `INVARIANTES.md` en la versión 19 (la 18 la tomó el Lote 0 de 4.4). Lote 2 (el stream de eventos: el transporte y el parser) cerrado el 9 de Noviembre de 2026: el stream por su propio cliente HTTP/1.1, el parser con su tope de 64 KiB y la decodificación de los eventos, que todavía nadie usa; la revisión de delta en verde en su primera ronda, con sus Bajos arreglados en el lote, e `INVARIANTES.md` en la versión 22 (la 20 la tomó el Lote 6b de 3.12, y la 21, el Lote 1 de 4.4). Lote 3 (la renovación del token) cerrado el 10 de Noviembre de 2026: el `TokenProvider` en `Client::open` y en la fachada de 4.4, la renovación ante `Unauthorized` (una por sala a la vez, con su plazo de 60 s, y el pedido repetido una vez), la verificación del formato y del `ClientId` del token renovado, el token marcado ante `Forbidden`, y la escritura que no queda incierta por un `Unauthorized` ni manda nada después de terminar; la revisión de delta en verde en su segunda ronda, e `INVARIANTES.md` en la versión 24 (la 23 la tomó el Lote 2 de 4.4). Lote 4 (el worker: la tarea, el heartbeat y las transferencias) cerrado el 12 de Noviembre de 2026: `start_worker`, `set_freshness` y `stop_worker`, una tarea por sala que tiene al cliente por un `Weak`, un heartbeat por intervalo (el primero al azar) que no espera las transferencias, que corren en su tarea, los reintentos con backoff y *full jitter*, lo que lo detiene (solo `FailedClosed` y `RoomClosed` de sus pasos), `close_room` y `shutdown` que lo detienen antes de cerrar, y el worker y el estado de la conexión en `status`; e `INVARIANTES.md` en la versión 28 (la 25 y la 26 las tomaron los Lotes 3 y 4 de 4.4, y la 27, el Lote 7 de 3.12). Lote 5 (los modos `periodic` y `manual`) cerrado el 12 de Noviembre de 2026: en `periodic`, y en `live` hasta el stream, un heartbeat cuyo ack informa un head mayor que el de la réplica lleva a un sync del worker, sin `Ack`, en su tarea, uno a la vez y uno más marcado; en `manual`, ninguno, y el worker igual sube y baja snapshots; la revisión de delta en verde en su primera ronda, e `INVARIANTES.md` en la versión 31 (la 29 la tomó el Lote 5 de 4.4, y la 30, la revisión de cierre de 3.12). Lote 6 (el `Ack` periódico, detener el worker y los endurecimientos) cerrado el 13 de Noviembre de 2026: el `Ack` del head de la réplica con cada heartbeat que sale bien, si pasó del último que el servidor tomó como cursor, y el último al detener el worker, con su plazo de 5 s, antes de cerrar la sala; un llamador que detiene el worker mientras otro lo detiene espera esa detención; DEF-196 (una réplica que espera un snapshot que nadie anuncia se registra otra vez después de 10 heartbeats) y DEF-192 (una respuesta con un head menor que el visto baja el head visto y registra la réplica); e `INVARIANTES.md` en la versión 34 (la 32 la tomó el Lote 6 de 4.4, y la 33, el Lote 8b de 3.12). Lote 7 (el modo `live`: el stream en el worker) cerrado el 16 de Noviembre de 2026: en `live`, el worker abre el stream de la sala y sincroniza después de abrirlo, y cada evento es un aviso (un sync, un registro con el cursor o un heartbeat en la tarea de las transferencias), si el stream es de la época de la réplica; lo reabre después de su fin, de un error o de 45 s sin un bloque, con un backoff que un evento vuelve a empezar, y con el token renovado ante `Unauthorized`; con el SSE apagado sigue como en `periodic` hasta `events_retry`; `set_freshness` lo abre o lo cierra en el momento, y `status` dice el modo efectivo y si el stream está abierto; e `INVARIANTES.md` en la versión 36 (la 35 la tomó el Lote 7 de 4.4). Lote 8 (el evento `ops`) cerrado el 16 de Noviembre de 2026: las operaciones de un evento `ops` llegan a la réplica por el aplicador, sin un sync, una vez validado (de la sala del stream, con operaciones contiguas que terminan en su head, de la época, de la encarnación y del esquema de la réplica), con su head como un head visto; un hueco, otra encarnación, otro esquema o una operación que no valida llevan a un sync o a un registro, nunca a fallar cerrada la sala; un evento a la vez, sin leer el stream mientras se aplica; e `INVARIANTES.md` en la versión 38 (la 37 la tomó el Lote 8 de 4.4). Lote 9 (el arnés de simulación con el worker y el SSE) cerrado el 17 de Noviembre de 2026: la simulación de 4.2 suma clientes con worker, en modos al azar que cambian, sobre un stream que pierde, demora y corta eventos, con tokens que vencen y el SSE apagado en algunos reinicios; con la red curada, los workers que sincronizan solos vuelven al head sin una llamada del test en a lo sumo 12 intervalos, detrás del log y detrás del head; ningún worker se detiene; y los tres tests de los Lotes 4 a 6 que dependían de cuándo cae el primer heartbeat esperan el estado que miden; solo tests, e `INVARIANTES.md` en la versión 41 (la 39 la tomó el Lote 10 de 4.4, y la 40, el Lote 8c de 3.12). Lote 10 (robustez: el fuzzing de los eventos) cerrado el 18 de Noviembre de 2026: el fuzzing de la simulación corre con los workers, con bloques del stream que no son del servidor (bytes al azar, eventos dañados, líneas sin fin, `ops` con un mensaje al azar, con una operación que no valida o falsificados de otra sala o de otra encarnación) y negativas al azar, sin entregar un `ops` que el cliente no distingue de uno del servidor; ningún worker se detiene ni ninguna sala falla cerrada; y el parser se prueba a través del transporte HTTP contra un servidor del test; solo tests, e `INVARIANTES.md` en la versión 43 (la 42 la tomó el Lote 9 de 4.4). Lote 11 (la revisión de cierre) cerrado el 18 de Noviembre de 2026, sobre `ea1cafe`, con dos revisores: no salió en verde. Rompió INV-K2 ★ (un `ops` cuya operación no valida deja su head como head visto, y una escritura incierta con su secuencia aplicada) e INV-K12 ★, K17 y K7 (un `events_retry` que no cabe en el reloj hace entrar en pánico al worker ante `EventsDisabled`), con dos Medias de origen (a). Las arreglan los Lotes 11b y 11c. Se corrigieron Bajos de documentos, e `INVARIANTES.md` queda en la versión 44. Lo que sigue es el Lote 11b.
