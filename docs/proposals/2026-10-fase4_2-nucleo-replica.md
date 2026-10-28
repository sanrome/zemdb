# Núcleo de la réplica — Plan de la sub-fase 4.2

**Fecha:** 26 de Octubre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lote 0 cerrado (el contrato). DC18 adoptada; DC19 a DC23, *Propuestas*, que el dueño del proyecto confirma antes del Lote 1.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.2. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).

---

## 0. Resumen

- **Qué.** El núcleo de `zemdb-client`: una réplica por sala contra un transporte abstracto (`TransportClient`), sin red real. Abre la sala desde sus metadatos durables, se registra, aplica los lotes por un único aplicador, resuelve cada escritura con la decisión de la réplica o como incierta, baja y sube snapshots, cambia de esquema solo a uno que evoluciona del suyo, descarta la réplica solo en los casos de INV-K3 y cierra las salas antes de apagar el runtime. Se prueba contra el servidor real en proceso, con un transporte que inyecta fallas.
- **Dónde.** En `zemdb-client`, salvo dos lotes en código auditado: `delete_room` en `zemdb-storage` (DC19), con el checklist de crash-safety de 4.1, y la encarnación en los chunks del relay, en `zemdb-core` y `zemdb-server` (DC23).
- **Qué no.** El `SyncWorker`, SSE, el transporte `reqwest` y la renovación del token (4.3); la fachada de Rust, lecturas, `watch`, el dueño completado por el SDK, el pre-chequeo, `Offline` y el cifrado (4.4); el navegador (4.5). En wasm32 el crate compila vacío hasta 4.5 (DC18).
- **Decisiones nuevas:** DC18 a DC23 (§1). DC18 está adoptada; las demás son *Propuestas* y se confirman con el dueño del proyecto antes del código que depende de ellas. DC19 ajusta DC12, DC20 ajusta DC5, y DC21 precisa DC6, DC7 y DC11.
- **Cómo cierra:** con la auditoría amplia del SDK (DC3), con tres auditores (§3).

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC1, DC5, DC6, DC7, DC11, DC12, DC14 y DC15 (§3 del plan de la fase). Esta sub-fase agrega:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC18** | La forma del núcleo: métodos con datos serializables, el transporte, los pasos sin worker | 3 a 9 | Adoptada (28 de Octubre de 2026) |
| **DC19** | Metadatos durables, el cursor como head del storage, y el descarte con `delete_room` | 1, 3, 4 | Propuesta (ajusta DC12) |
| **DC20** | El esquema se aprende solo de `Registered`, y fallar cerrado da de baja al cliente | 6 a 8 | Propuesta (ajusta DC5) |
| **DC21** | El aplicador, lo retenido y los reintentos | 5, 7 | Propuesta (precisa DC6, DC7 y DC11) |
| **DC22** | Snapshots en el cliente: la bajada con tope, la subida por chunks y la memoria | 8, 9 | Propuesta |
| **DC23** | Los chunks del relay llevan la encarnación de la sala | 2 | Propuesta |

### DC18 · La forma del núcleo
- **API (DC14).** `Client` se abre sobre un `StorageEngine`, un almacén de metadatos (DC19) y un `TransportClient`, con un `ClientConfig` (la persona, el plazo de DC6, el backoff y el tope de DC22), y expone métodos asíncronos, cada uno con su resultado tipado:
  - `open_room(room_id, token)`: abre la réplica local (DC19) y se registra (DC20). Sin red, una sala con metadatos abre igual, para leer, y se registra en la próxima llamada que necesite al servidor; una sala sin metadatos necesita al servidor.
  - `write(room_id, op)`: un commit con su resultado (DC21). En 4.4 se le suman, antes, el dueño y el pre-chequeo.
  - `sync(room_id)`: ponerse al día con el head del servidor (con un snapshot si hace falta, DC22) y mandar `Ack`.
  - `heartbeat(room_id)`: el heartbeat y su reacción (subir el snapshot si es el designado, bajarlo si lo espera, cambiar de esquema).
  - `status(room_id)`: el diagnóstico de la sala: head, época (cuántas veces el SDK borró la réplica desde que abrió la sala, contando el borrado al abrirla sin metadatos), si está registrada, si falla cerrada, cuántas escrituras hay en vuelo y cuánto retiene el aplicador (DC21). Sirve a la aplicación (un panel de depuración) y le permite al arnés verificar INV-K3 e INV-K15 sin hacer `pub` nada interno. Los búferes de snapshot viven dentro de una llamada y no figuran: su tope lo prueban los tests de sus lotes.
  - `close_room(room_id)` y `shutdown()`.

  Las lecturas y `watch` llegan en 4.4; los tests de 4.2 leen el motor que le pasan al cliente, por la API pública de `zemdb-storage`. Las llamadas corren concurrentes (varias escrituras en vuelo); cada sala serializa sus cambios en el storage en el aplicador (DC21).
- **Datos serializables, comandos en 4.5.** Los argumentos y los resultados de estos métodos son datos propios, sin referencias, callbacks ni streams, y con `serde`: es lo que pide DC14 para cruzar el límite del Worker. El enum de comandos y respuestas aparece recién en 4.5, en el runtime del Worker, que traduce cada mensaje en una llamada a uno de estos métodos; la fachada de 4.4 los llama directamente. Así cada método conserva su resultado tipado y no se escribe un dispatcher antes de que haga falta. Que todo sea serializable lo cuida el checklist de cada lote (§2), no el compilador.
- **Pasos sin worker.** `sync` y `heartbeat` son los pasos que el `SyncWorker` de 4.3 programa (SSE, timers, backoff); en 4.2 los llaman los tests. Mantener el heartbeat no es responsabilidad de la aplicación: la fachada de 4.4 arranca el worker y no expone estos pasos. Quedan públicos en el núcleo como API de bajo nivel, para quien programe su propio ciclo (por ejemplo, una app móvil que el sistema despierta unos segundos en segundo plano). Un `snapshot_wanted` se atiende con un heartbeat (DC22).
- **`TransportClient`.** Un método: mandar un `ClientMessage` con un token y devolver los bytes de la respuesta, sea cual sea su estado HTTP (en el Data Plane toda respuesta es un frame), o un `TransportError` si no hubo respuesta. El transporte elige el endpoint por el tipo del mensaje; el registro lleva el token adentro. El núcleo decodifica (`decode_message`, con la regla de la versión de `ARCHITECTURE.md` §7.2) y valida (DC21, INV-K12). Los snapshots se suben siempre por chunks (DC22), así que todo pedido es un `ClientMessage`. El transporte no devuelve los headers: el backoff no usa `Retry-After`.
- **Token.** El de `open_room`. En 4.2, `Unauthorized` es "sin token nuevo" (tabla de §A.4 del plan de la fase); la renovación es de 4.3, y la verificación del `ClientId` de un token renovado, de 4.4.
- **Sin red.** En 4.2 una escritura sin red reintenta hasta el plazo de DC6 y termina con `Network` (o `OutcomeUnknown`, si algún intento quedó incierto). El `Offline` inmediato de §A.4 necesita el estado de la conexión, que lleva el `SyncWorker` (4.3), y llega en 4.4.
- **Identificadores (DC15).** El `ClientId` es `persona#dispositivo`: la persona viene de la configuración (sin `#` y no vacía) y el id de dispositivo lo genera y guarda DC19 (128 bits al azar, en hexadecimal). Cada `MutationId` es aleatorio de 128 bits (`uuid` v4), y cada `CorrelationId`, un contador del cliente.
- **`ClientError` en 4.2:** las variantes que produce el núcleo (§A.1 del plan de la fase): `ConstraintViolation`, `RowMissing`, `OutcomeUnknown`, `Validation` (un rechazo permanente del servidor), `Network` (un error sin efecto), `Storage`, y la sala cerrada o fallida. 4.4 lo completa.
- **wasm32.** El núcleo usa el reloj y las tareas de Tokio y la capa de archivos nativa: en wasm32 el crate compila vacío (la CI lo construye igual), y 4.5 trae su soporte.

### DC19 · Metadatos durables y descarte
- **Dónde.** En el directorio de datos del motor en disco, junto a sus archivos y con nombres que el motor no usa, a través de la capa de archivos de DC17 (para que el OPFS de 4.5 use el mismo código); o en memoria, para `MemoryStorageEngine`. Un `client.lock` con el lock exclusivo mientras el cliente vive: otro cliente sobre el mismo directorio falla, y un motor se usa con un solo cliente (dos clientes sobre el mismo motor se borrarían las salas). Un archivo con el id de dispositivo y, por sala, `room_{id}.meta` con la encarnación, el `schema_id` y el esquema, los dos en JSON con un CRC32 (INV-W2) y reemplazados de forma atómica (temporal, `sync_all`, rename, `sync_dir`; `GEMINI.md`, principio 3). Un archivo que no verifica es un archivo que falta.
- **El cursor no se guarda.** Es el head con que el storage abre la sala (`get_head_seq`) y, después, el del aplicador (INV-K5). Una réplica que reabre por detrás (un `.snap` perdido, D96) o vacía es un prefijo de su log y se pone al día desde su head: nunca presenta un cursor guardado mayor. Así D96 no necesita detección.
- **Los metadatos van antes que los datos que describen:** la encarnación, antes de aplicar cualquier operación suya, con la sala del storage vacía; un esquema nuevo, antes de `reload_schema` (DC5). La encarnación de los metadatos es siempre la de todos los datos de la réplica.
- **Sin metadatos no hay réplica.** Una sala cuyos metadatos faltan tiene datos de una encarnación desconocida: el SDK los borra (`delete_room`) antes de abrirla. Es un caso más de INV-K3, y reemplaza al de la réplica perdida de DC12: una réplica perdida no se descarta, se pone al día.
- **Descartar (DC12, INV-K3),** en este orden: borrar los metadatos (de forma durable), cerrar la sala del storage, `delete_room` y sumar uno a la época (DC7); después, registrarse sin cursor. Un corte en cualquier punto deja la sala sin metadatos, y la próxima apertura vuelve a borrar. Las escrituras que esperaban se resuelven por DC21.
- **Errores.** Un error de E/S al leer o listar los metadatos o el id de dispositivo no es un archivo que falta: la apertura falla con `Storage` y no se borra nada (INV-K3), y un id de dispositivo nunca se regenera por un error (sería otro `ClientId`). Si falla escribir los metadatos o `delete_room` (por ejemplo, con `RoomLocked`), el paso se detiene: la sala sale del cliente con el error, sin registrarse ni aplicar nada más, y la próxima apertura retoma (sin metadatos, vuelve a borrar).
- **`StorageEngine::delete_room`** (nuevo, Lote 1): borra los archivos de una sala que no está abierta en el motor (si lo está, `RoomAlreadyOpen`, en los dos motores). En disco toma los locks de la sala como `open_room`, el del `.lock` y el del WAL, que una operación de archivo de la sala cerrada retiene hasta terminar (si alguno está tomado, `RoomLocked`); suelta el del WAL y, con el `.lock` tomado, borra por ruta y sin handles abiertos (como pide el OPFS, DC17), en este orden y con un `sync_dir` después de cada uno, el WAL activo, `.wal.compacting`, los temporales y el `.snap`, así que un corte la deja en un prefijo de su log (INV-S8). El `.lock` queda. En memoria, cerrar ya suelta la sala, así que no borra nada.
- **Alternativas descartadas:** guardar el cursor en los metadatos (hay que detectar D96 y escribirlos en cada lote); guardar los metadatos en el storage (cambia formatos de código auditado); un id de sala del storage distinto por época (un `RoomId` tiene hasta 64 bytes y los archivos de las épocas viejas quedarían).

### DC20 · El esquema se aprende de `Registered`
- **Una sola fuente.** El SDK se entera del esquema de la sala solo por `Registered`, que está atado a la encarnación, y nunca por `GetSchema`. Ante cualquier señal de cambio se vuelve a registrar con su cursor y decide contra el esquema de `Registered`; un `RoomReset` lo lleva a descartar la réplica. Las señales:
  - un `CommitAck`, `SyncBatch` o `HeartbeatAck` con otro `schema_id` o con una versión mayor del mismo (una versión igual o menor del mismo no es un cambio: sus operaciones se aplican con el esquema local, INV-E3);
  - una operación que no valida contra el esquema local (`SchemaValidation` de `apply_batch`);
  - `SnapshotCorruption` o `SnapshotConstraints` al aplicar el snapshot de otro miembro; no `SnapshotBehind`, `SnapshotHeadMismatch` ni los errores de E/S (requisito 12 de §1.1).

  Un registro por señal: si el esquema no cambió, la señal se trata como sin cambio de esquema (la operación que no valida viola el protocolo, y el snapshot se descarta).
- **La decisión.** El esquema local es el de los metadatos, que es siempre el de la sala del storage (DC21: guardarlo y recargarlo corren en una tarea que no se corta). El mismo `schema_id` con una versión igual o menor que la local no cambia nada. Si no, el esquema de `Registered` tiene que evolucionar del local (`evolves_from`); entonces se guarda en los metadatos, se llama a `reload_schema` y recién después se aplican las operaciones de esa versión (DC5). Si no evoluciona, la sala falla cerrada.
- **Fallar cerrado** (INV-K6, y una operación que no valida bajo el esquema que anuncia su respuesta): la sala deja de aplicar lotes y de escribir; las escrituras se resuelven por DC21; el cliente manda una vez `DeregisterClient`, bajo el mismo lock que los registros, y deja de mandar heartbeats y de registrarse, también ante `ClientNotRegistered` (requisito 2), así que no frena la poda por cursores ni puede ser designado; la réplica sigue legible. Si la baja se pierde, o un registro anterior que quedó en la red llega después, la entrada pasa a `Disconnected` cuando vence su lease, como la de un cliente que se fue. Reabrir la sala (o descartarla a pedido de la aplicación, 4.4) vuelve a intentar. Cambia el "no los heartbeats ni el registro" de DC5.
- **Por qué.** Un camino en lugar de dos (`GetSchema` y, antes de fallar cerrado, el registro), y el esquema es siempre el de la encarnación (requisito 3). Cuesta un registro en lugar de un `GetSchema` por cambio: reemplaza la entrada del roster con el mismo cursor y renueva el lease; el dedup es por `MutationId` y no cambia.

### DC21 · El aplicador y las escrituras
- **El aplicador (DC7).** Por sala, un lock bajo el que pasa todo cambio del storage: `apply_batch`, `apply_snapshot`, `reload_schema`, abrir, cerrar y descartar (y `create_snapshot` para subirlo, DC22). Cada cambio de varios pasos (aplicar un lote y entregar sus decisiones; guardar un esquema y recargarlo; instalar un snapshot; descartar) corre en una tarea propia que retiene el lock, como `open_room` y `apply_snapshot` del motor en disco: soltar la llamada que lo espera no lo corta a la mitad. Un lote cuya época no es la actual se ignora; se descarta el prefijo ya aplicado; uno que no empieza en head + 1 no se aplica (quien lo pidió sincroniza); después, `apply_batch`, la entrega de cada decisión y el head nuevo. Ante un error del storage después del cual la sala pudo cambiar (`Io` o `RoomFailed`, del motor en disco), cierra y reabre la sala del storage con el esquema de los metadatos, y las secuencias entre el head anterior y el reabierto quedan sin reporte; un daño al reabrir lleva a descartar (INV-K3). Si no reabre por otro error, la sala se cierra en el cliente y el error llega a quien la usa.
- **El head visto.** El cliente guarda el mayor head del servidor que informaron las respuestas de la época actual: `Registered`, `SyncBatch`, `HeartbeatAck`, `AckConfirmed` y, de un `CommitAck`, su secuencia asignada o la última de su catch-up (la de un reintento deduplicado puede ser menor). Cada escritura, después de registrarse en la época, anota antes de su primer intento `seen`: el mayor entre el head visto y el head local (requisito 13). El head visto de una respuesta se actualiza antes de que sus operaciones lleguen al aplicador, y `seen` y la entrada de la escritura se toman bajo el mismo lock con que el aplicador entrega las decisiones y mueve el head, así que ninguna secuencia mayor que `seen` se entrega sin que la escritura la retenga.
- **Lo retenido (requisitos 4, 9 y 14).** Mientras haya escrituras sin `CommitAck`, el aplicador retiene, de las secuencias mayores que el menor `seen` de ellas: los rechazos, y los rangos que la réplica pasó sin reporte (los que cubrió un snapshot y los de una reapertura después de un error del storage). Si no hay, no retiene nada, y descartar lo vacía. Al llegar el `CommitAck` con la secuencia `s`:
  - si `s` es mayor que el head local, la escritura espera esa secuencia;
  - si no, y `s > seen`, su decisión es el rechazo retenido, el resultado incierto si `s` está en un rango sin reporte (INV-K2, b o c), o `Ok`: `s` se aplicó una por una después de que la escritura empezó a retener;
  - si `s ≤ seen` (reutilización de D78), `OutcomeUnknown`. Nunca `Ok` por la ausencia de un rechazo que no se retuvo.
- **Esperas y cortes.** Cada escritura en vuelo es una entrada (antes o después del `CommitAck`) que quita un guard RAII si la llamada se suelta (`GEMINI.md`, principio 2), bajo un lock síncrono que nunca cruza un `.await` (principio 8).
- **Resoluciones sin decisión (INV-K2).** Una escritura con `CommitAck` que espera su secuencia termina como incierta si un snapshot instalado la cubre (b), si la réplica se descarta, recibe `RoomReset` o falla cerrada (b), si la sala reabre después de un error del storage en un head que la cubre (c), o si se cierra la sala o el cliente (d). Un `CommitAck` que llega después de fallar cerrado, de cerrar la sala o en otra época también la termina como incierta. Una escritura sin `CommitAck` sigue la tabla de §A.4 del plan de la fase: incierta si algún intento quedó incierto y, si no, un error sin efecto.
- **Reintentos (DC6).** El plazo es de 30 s (configurable), medido con el reloj de Tokio (DC11), y no se manda ningún intento después de vencido. Entre intentos, backoff exponencial con *full jitter*, con el mismo `MutationId` y el mismo `ClientId`. La reacción a cada respuesta es la tabla de §A.4, con `Unauthorized` como "sin token nuevo" hasta 4.3 y `Internal` reintentado (el comentario de `ErrorCode::Internal` cambia, como pide §A.4). Si una respuesta pide registrarse o ponerse al día (`ClientNotRegistered`, `InvalidSequence`, `BehindCompaction`), la escritura lo hace dentro del plazo y vuelve a intentar.
- **Después del `CommitAck`,** la escritura sincroniza hasta que la réplica aplica `s` (paso 4 de §A.4), con backoff y sin plazo, mientras la sala siga abierta (INV-K2).
- **Un head del servidor menor que el cursor (requisitos 8 y 10, INV-K1).** Un `SyncBatch`, `HeartbeatAck` o `AckConfirmed` con un head menor que el head local al mandar el pedido, y un `InvalidSequence` en un sync, ack o commit, llevan a registrarse con el cursor; un `InvalidSequence` en ese registro, a descartar la réplica (D78). Una respuesta demorada, con un head mayor o igual que el del pedido pero menor que el actual, no viola nada y se ignora. Precisa DC11 (2): un lote con hueco no se aplica y se vuelve a sincronizar, un head menor lleva a registrarse, y fallar cerrado queda para el esquema (DC20).
- **Validar antes de usar (INV-K12).** Cada respuesta se valida antes de tocar nada: su tipo para el pedido; la sala y la correlación del pedido cuando vienen (los errores de autenticación y de los extractores salen sin ellas); en un `CommitAck`, su `MutationId`, un catch-up contiguo desde el cursor del pedido y, sin `has_more`, una secuencia asignada que no pasa del catch-up (o del cursor, si viene vacío); en un `SyncBatch`, operaciones contiguas desde `from_seq` que no pasan de su head y, sin `has_more`, que llegan a él. Una respuesta que no valida no se aplica: para una escritura, el intento queda incierto.

### DC22 · Snapshots en el cliente
- **Bajada (§A.6).** Cuando un registro deja al cliente detrás del log (`cursor < tail_seq − 1`), o un sync, un commit o un ack recibe `BehindCompaction`, la réplica espera un snapshot: si `Registered` o un `HeartbeatAck` anunciaron uno usable, lo baja; si no, o si el chunk 0 responde `RoomNotFound` (no hay uno usable), espera a que un heartbeat lo anuncie. Por chunks de 4 MiB (el máximo del servidor), que se juntan en memoria hasta `max_snapshot_bytes` (configurable, por defecto 512 MiB, el de `ZEMDB_MAX_SNAPSHOT_BYTES`; requisito 6): un `total_bytes` mayor es un error, y no se baja. Cada chunk se valida antes de guardarlo: la misma secuencia, `total_chunks`, `total_bytes` y hash que el primero, su índice y su largo. Ante `SnapshotSuperseded`, se descarta lo bajado y se vuelve al chunk 0. Al terminar, el BLAKE3, y `apply_snapshot` con el esquema local y `S`, en una tarea propia del aplicador, con DC20 ante sus errores. Después de instalarlo, `(head anterior, S]` queda sin reporte (DC21) y se sincroniza desde `S`. Cada chunk lleva la encarnación de los metadatos, y el relay responde `RoomReset` al chunk 0 sin ancla si la sala es otra encarnación y tiene un snapshot activo (si no tiene, `RoomNotFound`); a los anclados los cubre la purga del relay al recrear la sala, salvo que la sala nueva active un snapshot con el mismo hash, que son los mismos bytes que el chunk 0 validó en la vieja (DC23). Así el snapshot es de la encarnación de la réplica, y uno bajado en una época anterior no se instala (DC7). Un `RoomReset` en un chunk de la época actual, si la sala no falla cerrada, lleva a registrarse con el cursor (y de ahí a descartar), y un `RoomNotFound` (sin snapshot usable, o la sala borrada), a esperar un anuncio.
- **Subida (§A.8).** Un `snapshot_wanted` (en cualquier respuesta) se confirma con un heartbeat: si su `HeartbeatAck`, de la época actual, también lo trae, el cliente crea el snapshot en el head local (`create_snapshot`, bajo el aplicador, para que su secuencia sea el head que anuncia) y lo sube en `n = ⌈T / 4 MiB⌉` chunks de `⌈T / n⌉` bytes, el último con el resto (la regla del relay, `ARCHITECTURE.md` §6), aunque entre en un request. Ante `SnapshotSuperseded` u otro error, deja de subir (y ante `RoomReset` de la época actual, si la sala no falla cerrada, además se registra con el cursor): si hace falta, el servidor designa a otro (si no empieza a subir dentro de 60 s, o si la subida pasa 2 minutos sin chunks). No sube si falla cerrado ni mientras espera un snapshot. Cada chunk lleva la encarnación de la época en que se creó el snapshot, y el relay lo rechaza con `RoomReset` en otra encarnación (DC23); además, el cliente no manda un chunk si la época cambió, porque después de un descarte por D78 la encarnación es la misma.
- **Memoria (requisito 5, INV-K15).** El búfer de la bajada, hasta el tope; el de la subida (`create_snapshot` devuelve un `Vec<u8>`, un snapshot de la réplica); y el segundo estado completo que arma `apply_snapshot` antes de reemplazar el primero. INV-K15 los cuenta. Una sala cuyo snapshot pasa el tope del cliente no se pone al día (INV-K7 lo exceptúa): la aplicación sube el tope o descarta la sala.

### DC23 · Los chunks del relay llevan la encarnación
- **El problema.** Los pedidos del relay (`RequestSnapshotChunk`, `UploadSnapshotChunk`) se autentican con el token, que vale para una sala recreada con el mismo id (`INVARIANTES.md` §4), y no dicen a qué encarnación pertenecen. Un cliente honesto de la encarnación vieja, designado antes de que la sala se recreara, podría subir su snapshot a la nueva, que lo acepta si su secuencia entra en el log retenido: los miembros que se incorporan desde él divergen sin aviso (INV-K4). Al revés, un cliente que espera un snapshot podría bajar el de la sala nueva e instalarlo con los metadatos de la vieja (INV-K1). Exigir que el miembro esté en el roster no alcanza: el roster es por `ClientId`, que no cambia al descartar, así que un chunk de la encarnación vieja que llega después de que el mismo cliente se volvió a registrar en la nueva pasaría (lo reprodujo la revisión cruzada).
- **La regla.** Los dos mensajes llevan, al final, la `room_incarnation` a la que pertenece el pedido: la de los metadatos del cliente. Cuando el relay le pide al actor de la sala sus límites del log (`GetLogBounds`, que pasa a devolver también la encarnación de la sala), lo que ya hace en el chunk 0 sin ancla de una bajada y en cada chunk de una subida, después de atar el pedido al estado actual de la sala, compara las encarnaciones y responde `RoomReset` si no coinciden, sin cambiar nada. Un chunk 0 sin ancla de una sala sin snapshot activo recibe `RoomNotFound`, sin consultar al actor. Los chunks anclados de una bajada no consultan al actor: si la sala se recreó, la purga del relay los vuelve `SnapshotSuperseded`, y el chunk 0 siguiente sí consulta; la excepción es que la sala nueva active un snapshot con el mismo hash, y entonces el chunk anclado da los mismos bytes que el chunk 0 validó en la vieja. La subida en un request (`POST /snapshot/upload`), que el SDK no usa, no cambia.
- **Costo.** Un campo en dos mensajes de `zemdb-core` (antes del primer release, `PROTOCOL_VERSION` no cambia, `GEMINI.md`) y unas 30 líneas en `zemdb-server` (el actor, el relay y su error), con su revisión de delta; los tests y generadores que arman esos mensajes suman el campo. El secreto de administración (un worker de snapshots) manda la encarnación como cualquiera, y la obtiene al crear la sala o de `GET /admin/rooms/{id}`, que pasa a devolverla (unas 3 líneas más).
- **Alternativas descartadas:** exigir que el miembro esté en el roster (deja pasar el chunk demorado de un cliente que se volvió a registrar, ver arriba); que el cliente confirme con un heartbeat antes de subir y después de bajar (deja una ventana de un RTT en la subida).

### 1.1. Requisitos de la revisión cruzada de 4.0
Los de §4.2 del plan de la fase, en su orden:

| # | Requisito | Cómo se resuelve | Lote |
|---|---|---|---|
| 1 | Una escritura con `CommitAck` en una sala que falla cerrada | Se resuelve como incierta al fallar cerrado (DC21; INV-K2, b) | 6, 7 |
| 2 | Un cliente que falla cerrado sigue con heartbeats y frena la poda | Se da de baja, deja los heartbeats y no se vuelve a registrar (DC20) | 6, 8 |
| 3 | Decidir contra el esquema de `Registered` | El esquema solo sale de `Registered` (DC20) | 6 |
| 4 | Un `CommitAck` demorado con su secuencia ya aplicada | Se resuelve con lo retenido (DC21) | 7 |
| 5 | INV-K15 no cuenta el búfer de la subida ni el segundo estado de `apply_snapshot` | INV-K15 los cuenta (DC22) | 0 |
| 6 | El tope de la bajada de snapshots | `max_snapshot_bytes` (DC22) | 8 |
| 7 | En el arnés, otro pedido puede volver a levantar la sala entre `shutdown_room` y `get_or_spawn` | El arnés repite los dos pasos hasta que `GET /admin/rooms/{id}` da el `schema_id` nuevo | 10 |
| 8 | Un `HeartbeatAck` o un `AckConfirmed` con un head menor que el local | Registro con el cursor si es menor que el head al mandar (DC21) | 5, 8 |
| 9 | Acotar los rechazos retenidos por la secuencia asignada | DC21 | 7 |
| 10 | `InvalidSequence` en un sync o un ack | Registro con el cursor (DC21) | 5 |
| 11 | Cotas calculables para INV-K15, y cómo las mide el arnés | INV-K15 con sus cotas (versión 9); el arnés las mide con `status` (DC18) | 0, 10 |
| 12 | El pedido de esquema ante un error de `apply_snapshot` | Solo ante `SnapshotCorruption` y `SnapshotConstraints` (DC20) | 8 |
| 13 | El head visto antes del primer intento, no de cada reintento | `seen` (DC21) | 7 |
| 14 | La secuencia asignada por debajo del head visto | `OutcomeUnknown`, nunca `Ok` (DC21) | 7 |

Y los dos que §5.3 del plan de la fase deja a este plan: cómo espera el arnés las tareas sueltas al reiniciar el servidor (el transporte en proceso corre cada pedido en una tarea propia, que cortar al cliente no cancela, y el arnés espera esas tareas antes de rearmar el servidor; Lote 10) y cómo observa los descartes (la época de `status`, DC18). Del cierre de 4.1: un `close_room` que falla (por ejemplo, con el handle muerto de DC17) se informa y la sala sale igual del cliente (Lote 4).

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC18 a DC23 | Nueva: S8. Ajustes: R6, K1, K2, K3, K5, K6, K7, K11, K15 | ✅ |
| 1 | `delete_room` | storage | DC19 | S8, S3, L7 | ⬜ |
| 2 | La encarnación en los chunks del relay | core, server | DC23 | R6, R5, W1, W2 | ⬜ |
| 3 | El esqueleto: comandos, transporte y metadatos | client | DC18, DC19, DC15 | W1, W2 (metadatos) | ⬜ |
| 4 | Apertura, registro, descarte y cierre | client | DC19, DC12 | K3, K5, K11 | ⬜ |
| 5 | El aplicador y `sync` | client | DC7, DC21 | K1, K12 | ⬜ |
| 6 | El esquema | client | DC5, DC20 | K6, C1 | ⬜ |
| 7 | Escrituras | client | DC6, DC7, DC21 | K2, K15 | ⬜ |
| 8 | `heartbeat` y la bajada de snapshots | client | DC20, DC22, DC23 | K7, K2, K12, K15 | ⬜ |
| 9 | La subida de snapshots | client | DC22, DC23 | K7, K4 | ⬜ |
| 10 | El arnés de simulación | client (tests) | §5.3 del plan de la fase | K1, K2, K3, K4, K7, K15 | ⬜ |
| 11 | Robustez: fuzzing de respuestas y snapshots | client (tests) | — | K12 | ⬜ |
| 12 | Auditoría amplia y cierre | Documentos | Todas | Las de §3.11 que toca 4.2, S8 y R6 | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirman antes del Lote 1.
- El 1 y el 2 tocan código auditado y van antes de los del cliente que los usan (el 4 descarta con `delete_room`; el 8 y el 9 dependen de DC23). Del 3 al 9, en orden: cada uno usa lo anterior.
- El 10 y el 11 son pruebas, sin producción (salvo arreglos de lo que encuentren, que van como desvíos en su nota o como lotes nuevos); el 10 antes del 11, que reusa su transporte.
- El 12 cierra con la auditoría amplia (DC3, §3); sus rupturas se arreglan en lotes nuevos de 4.2 antes de abrir 4.3 y 4.4.

**Tamaño.** Unas 60 líneas de producción el Lote 1 y unas 30 el 2; de 150 a 300 cada uno del 3 al 9.

**Pruebas.** Desde el Lote 3, contra el servidor real en proceso: `build_router` sobre un `data_dir` temporal, llamado como un servicio de `tower`, con un transporte de prueba que implementa `TransportClient` (en `crates/client/tests/common/`; `zemdb-server` como dev-dependency) y corre cada pedido en una tarea propia, como en una red real, donde el servidor sigue con un pedido aunque el cliente deje de esperarlo. Para los casos que el servidor no produce (una respuesta que viola el protocolo, D78, un esquema que no evoluciona), el transporte de prueba fabrica la respuesta. Para las fallas del storage, un motor de prueba que envuelve a uno real y falla a pedido. Cada lote suma sus tests; el 10 los combina al azar.

**Checklist propio.** Los Lotes 3 a 9 recorren este checklist del cliente; el 1, el de crash-safety de 4.1 (§2 de su [plan](2026-10-fase4_1-storage-servidor.md)); y los que tocan cómo decide o qué acepta una réplica (5, 6 y 8), además, el de determinismo de la Fase 3.11 (§5.4 de su [propuesta](2026-10-fase3_11-restricciones-proposal.md)).
- Cada `.await` es un punto de corte: el estado compartido (esperas, lo retenido) se limpia con guards RAII, ningún lock síncrono cruza un `.await`, y un cambio de varios pasos del aplicador corre en su propia tarea (DC21).
- Ninguna respuesta se usa antes de validarla (DC21) y de comparar su época (DC7).
- Los argumentos y los resultados de los métodos públicos del núcleo son datos propios y serializables, sin referencias, callbacks ni streams (DC18, DC14).
- Todo cambio del storage pasa por el aplicador, bajo su lock.
- Los metadatos se escriben antes que los datos que describen, y un error al escribirlos detiene el paso (DC19).
- Ningún resultado de E/S se descarta, y un error de lectura nunca se toma como un archivo que falta.
- Cada camino de una escritura termina en una de las respuestas de INV-K2, una sola vez.

### Lote 0 · Contrato · ✅ Hecho
**Objetivo:** el contrato contra el que se revisa cada lote de 4.2. Solo documentos.

**Qué se hace:**
- **Este plan,** con DC18 a DC23 como *Propuestas*, la resolución de los requisitos de 4.0 (§1.1) y la auditoría de cierre (§3).
- **`INVARIANTES.md`, versión 9:**
  - **INV-S8, nueva** (§3.2), 🆕 (4.2, Lote 1): `delete_room` nunca deja la sala en un estado que no sea un prefijo de su log.
  - **INV-R6:** el chunk 0 sin ancla de una bajada y los de una subida, de otra encarnación, reciben `RoomReset`, y a los anclados los cubre la purga (DC23), 🆕 (Lote 2).
  - **INV-K1:** en "Sostenida por", la validación de las respuestas, el cursor de DC19 y DC23; en el "Riesgo aceptado", la detección de un head menor (DC21).
  - **INV-K2:** el caso (b) suma la sala que falla cerrada; "Sostenida por", con lo retenido de DC21.
  - **INV-K3:** suma el caso de los metadatos que faltan, en lugar del de la réplica perdida (DC19); "Sostenida por", con `delete_room`.
  - **INV-K5:** el cursor es el head con que abre el storage, y no se guarda (DC19).
  - **INV-K6:** el esquema sale de `Registered` y fallar cerrado da de baja al cliente (DC20).
  - **INV-K7:** salvo que la sala falle cerrada o su snapshot pase el tope del cliente.
  - **INV-K11:** el lote que lo implementa.
  - **INV-K15:** las cotas de cada elemento (DC21, DC22) y lo que antes no contaba (requisitos 5 y 11).
  - El resumen, con el lote de 4.2 que implementa cada una; §1.4 y §1.6 (las decisiones llegan a DC23, y la auditoría de 4.2 tiene tres auditores); §2 y §4 (D96 sin detección).
- **El plan de la fase:** DC18 a DC23 en la tabla de decisiones, como *Propuestas*; las notas de DC19, DC20 y DC21 en DC5, DC8, DC11 y DC12; los textos que contradecían DC19 y DC20 (§4.2, §5.3, §A.5, §A.6, §A.9); 4.2 en curso, con el enlace a este plan.
- **`ROADMAP.md`:** 4.2 en curso, con el enlace a este plan.
- `ARCHITECTURE.md` no cambia: describe comportamiento implementado. Desde el Lote 1, cada lote suma lo suyo (§8 para `delete_room`, §6 para el relay, y una sección nueva del cliente, §11, desde el Lote 3, que 4.7 completa).

**INV-S8** (Medio si se rompe): su enunciado, cómo se sostiene y cómo intentar romperla están en `INVARIANTES.md` §3.2, que es la única versión.

**Entrega:** `docs: plan Fase 4.2 with its batches and the replica core decisions, and adjust the client contract for it (Fase 4.2, Lote 0)`.

**Hecho.**
- **Documentos:** este plan, con DC18 a DC23 *Propuestas*, la resolución de los 14 requisitos de 4.0 (§1.1) y la auditoría de cierre con tres auditores (§3); `INVARIANTES.md` en la versión 9 (INV-S8 nueva; ajustes de R6, K1, K2, K3, K5, K6, K7, K11 y K15, el resumen, §1.4, §1.6, §2 y §4); el plan de la fase con DC18 a DC23, las notas en DC5, DC8, DC11 y DC12, y los textos que contradecían DC19 y DC20 (§4.2, §5.3, §A.5, §A.6, §A.8, §A.9); `ROADMAP.md` con 4.2 en curso.
- **Revisión cruzada** (agentes aparte, de contexto fresco y solo lectura), en cuatro rondas, cada una acotada a las correcciones de la anterior:
  1. **2 bloqueantes, 9 ajustes y 12 menores.** Los snapshots no estaban atados a la encarnación: el relay no mira el roster, y un cliente honesto de una sala borrada y recreada podía subir su snapshot a la nueva (divergencia sin aviso) o bajar el de la nueva; de ahí DC23 y el Lote 2. Y "tener el reporte de `s`" (DC21) contradecía INV-K2 después de un snapshot o una reapertura: ahora se retienen los rangos sin reporte. Los ajustes: el head visto y `seen`; las escrituras sin `CommitAck` al cerrar, descartar o fallar cerrado, y la sala que falla cerrada sin registros; los errores de E/S y de escritura de DC19; los cambios de varios pasos del aplicador en una tarea propia; el largo de los chunks de subida que exige el relay y `RoomNotFound` en el chunk 0; los frames de error sin correlación; cómo espera el arnés las tareas sueltas; textos del plan de la fase que contradecían DC19 y DC20; los tests que faltaban. Los lotes se partieron (de 10 a 13).
  2. **1 bloqueante y 5 menores.** La primera versión de DC23 (el relay exige que el miembro esté registrado) no alcanzaba: el roster es por `ClientId`, que sobrevive a un descarte, y un chunk demorado de la encarnación vieja pasaba después de que el mismo cliente se volvía a registrar en la nueva (el revisor lo reprodujo con un prototipo). DC23 pasó a llevar la encarnación en los dos mensajes del relay, y el cliente no manda chunks de una época anterior. Los menores: el enunciado de INV-K1, `seen` tomado bajo el lock del aplicador, §A.8 y la redacción del chunk 0 sin ancla.
  3. **1 ajuste y 5 menores.** El secreto de administración no tenía de dónde sacar la encarnación de una sala que no creó: `GET /admin/rooms/{id}` pasa a devolverla. Los menores: un chunk anclado cruza una recreación si la sala nueva activa un snapshot con el mismo hash (los mismos bytes); el chunk 0 sin snapshot activo da `RoomNotFound`; el mensaje de `RoomReset`; la reacción a `RoomReset` en un chunk, solo en la época actual y sin fallar cerrado; el largo del campo nuevo; §A.6 y §4 de `INVARIANTES.md`. El revisor midió DC23 con un prototipo: unas 32 líneas de producción, y los tests del servidor pasan salvo el que sube sin registrarse, que el plan ya prevé.
  4. **Verde,** con 3 menores: tres resúmenes que no decían la excepción del mismo hash, el test del Lote 8 sin fijar el contenido del snapshot de la sala nueva, y que `room_incarnation` vaya como campo propio de `RoomDetails` (`#[serde(flatten)]` no admite un `u128`). Corregidos.
- **Desvíos:** DC23, el Lote 2 y el ajuste de INV-R6 no estaban en el plan de la fase; salieron de la revisión cruzada. Los lotes son 13 en lugar de los 10 del primer borrador.

### Lote 1 · `delete_room` (`zemdb-storage`)
**Objetivo:** que el SDK pueda descartar una réplica en disco (DC19).

**Archivos:** `crates/storage/src/engine.rs`, `memory/mod.rs` y `disk/mod.rs` (con lo que haga falta de `disk/`), y sus tests. `ARCHITECTURE.md` §8.

**Qué se hace:** `StorageEngine::delete_room(&self, room_id) -> Result<(), StorageError>`, con la regla de DC19: los dos locks de la sala como `open_room` (`lock_room_files`), el del WAL suelto antes de borrar, y los archivos que existen borrados en el orden de DC19, cada uno seguido de `sync_dir`. El `.lock` queda: borrarlo con su lock tomado dejaría a otro proceso bloquear un archivo nuevo mientras tanto.

**Tests exigidos.**
- Después de `delete_room`, abrir la sala da head 0 y tablas vacías, en los dos motores; también después de una compactación interrumpida (con `.wal.compacting` y temporales).
- Negativos: con la sala abierta, `RoomAlreadyOpen`, en los dos motores; con un lock tomado por otro motor (el `.lock`, o el del WAL retenido por una operación que no terminó), `RoomLocked`; en los dos casos no se borra nada.
- Imágenes de crash (`fail_point::arm_crash_plan`): un corte después de cada borrado deja una sala que abre en un prefijo de su log (el `.snap` más `.wal.compacting`, o el `.snap` solo) o vacía.
- Un error de E/S en un borrado se devuelve, y la sala abre en un prefijo.

**Interacciones a revisar:** las tareas de compactación y de `apply_snapshot` de una sala cerrada (`close_room` las espera; D93, INV-L7); los locks de la sala y `RoomLocked`; la recuperación de `.snap.tmp.*` y `.wal.compacting`; los fail points por ruta; la capa de archivos del OPFS (borrar por ruta sin handles abiertos: la sala está cerrada, el del WAL se suelta y el `.lock` no se borra).

**Documentos:** `ARCHITECTURE.md` §8; `INVARIANTES.md`: INV-S8 con su test.

### Lote 2 · La encarnación en los chunks del relay (`zemdb-core`, `zemdb-server`)
**Objetivo:** que un cliente de otra encarnación de la sala nunca baje otro snapshot que el de su encarnación ni suba nada (DC23).

**Archivos:** `crates/core/src/protocol/messages.rs` (los dos mensajes y el comentario de `ErrorCode::RoomReset`) y los generadores de mensajes de sus tests; `crates/server/src/actor/room.rs` (`log_bounds`) y `command.rs`, `src/relay/` (`LogBounds`, `pick_chunk`, `stage_chunk`), `src/api/relay.rs` y `control_plane.rs` (la encarnación en `GET /admin/rooms/{id}`), y el mensaje del error `RoomReset`; los tests del relay, del Control Plane y de codificación. `ARCHITECTURE.md` §6, §7.1 y §7.2.

**Qué se hace:** `room_incarnation: u128` al final de `RequestSnapshotChunk` y de `UploadSnapshotChunk`; `LogBounds` con la encarnación de la sala; el relay compara en las consultas que ya hace y responde `RoomReset` si no coincide; `GET /admin/rooms/{id}` devuelve `room_incarnation` (un número JSON de hasta 128 bits, como en `POST /admin/rooms`); el mensaje de `RoomReset` y su comentario dejan de hablar solo del cursor de un registro.

**Tests exigidos.**
- Ida y vuelta de los dos mensajes con la encarnación, y su lugar en los fuzzers de mensajes (INV-W1, W2).
- Con la encarnación de la sala, se baja y se sube como hoy, también con el secreto de administración, que la toma de `GET /admin/rooms/{id}`.
- Negativos: con otra encarnación, el chunk 0 sin ancla de una bajada y un chunk de una subida reciben `RoomReset`, y nada cambia en el relay (ni la subida en curso ni el snapshot activo).
- Una sala borrada y recreada, con un snapshot activo de otro contenido: un cliente de la vieja recibe `RoomReset` al subir y al bajar en la nueva, también después de volver a registrarse en ella con el mismo `ClientId`, y la nueva no activa su snapshot. Un chunk anclado de la vieja recibe `SnapshotSuperseded`. Sin snapshot activo en la nueva, el chunk 0 recibe `RoomNotFound`.

**Interacciones a revisar:** la purga del relay al crear y al borrar una sala; el ancla de retención de una subida en curso y la designación; los chunks anclados de una bajada; el presupuesto de respuestas; el tope del cuerpo de un pedido (los dos mensajes crecen hasta 17 bytes, lejos del tope con un chunk de 4 MiB); `RoomDetails` incrusta `RoomMetrics` con `#[serde(flatten)]`, que no admite un `u128` al deserializar, así que `room_incarnation` va como campo propio de `RoomDetails`, y los tests la leen con un tipo, no como `serde_json::Value` (que la pasaría a `f64`).

**Documentos:** `ARCHITECTURE.md` §6 (*Multipart Chunked Snapshot Transfer*, *Snapshot Relay*), §7.1 (`GET /admin/rooms/{id}`) y §7.2 (`RoomReset` en el relay); `INVARIANTES.md`: INV-R6.

### Lote 3 · El esqueleto: comandos, transporte y metadatos (`zemdb-client`)
**Objetivo:** los tipos de DC18, el transporte, los metadatos de DC19 y la prueba contra el servidor en proceso, sin la lógica de las salas.

**Archivos:** `crates/client/Cargo.toml`, `src/` (los módulos que hagan falta: comandos y errores, configuración, transporte, metadatos, el cliente); `tests/common/` (el servidor en proceso y el transporte de prueba); se borra el test del stub. `ARCHITECTURE.md` §11, nueva.

**Qué se hace:** `ClientError` y `ClientConfig`; `TransportClient` y `TransportError`; el almacén de metadatos en archivos y en memoria, con `client.lock`, el id de dispositivo y `room_{id}.meta`; `Client::open` (el lock, el id de dispositivo, el `ClientId`), con `status` y `shutdown` sobre un cliente sin salas. En wasm32, el crate vacío (DC18).

**Tests exigidos.**
- Ida y vuelta de los metadatos y del id de dispositivo; un bit invertido, un archivo truncado o un temporal que quedó cuentan como que falta, sin pánico. Negativo: un error de E/S al leerlos (con una capa de archivos de prueba) es un error, no un archivo que falta, y el id de dispositivo no cambia.
- Un segundo cliente sobre el mismo directorio falla; el id de dispositivo es el mismo al reabrir y otro en otro directorio; el `ClientId` es `persona#dispositivo` y su dueño es la persona; una persona con `#` o vacía es un error de configuración.
- El transporte de prueba: un `RegisterClient` llega a `build_router` y vuelve `Registered`.

**Interacciones a revisar:** la capa de archivos (`NativeFiles`) y sus locks; los nombres de los archivos del motor en disco en el mismo directorio; `ClientId::owner` (DC15); el `ServerConfig` y los secretos del servidor en proceso.

**Documentos:** `ARCHITECTURE.md` §11 (la forma del núcleo y los metadatos).

### Lote 4 · Apertura, registro, descarte y cierre (`zemdb-client`)
**Objetivo:** abrir una sala, registrarse con la encarnación correcta, descartar solo en los casos de INV-K3 y cerrar ordenado.

**Qué se hace:**
- `open_room`: con metadatos, abre el storage con su esquema (un daño al abrir, `SnapshotCorruption` o `WalCorruption`, lleva a descartar; `RoomLocked`, `Io`, `SchemaMismatch` y `SnapshotConstraints` son un error y la réplica queda); sin metadatos, `delete_room`, y el storage se abre recién con el esquema de `Registered`.
- El registro: con el head del storage como cursor y la encarnación de los metadatos, o sin ninguno de los dos. `Registered` sin metadatos guarda la encarnación y el esquema y abre el storage; con metadatos y el mismo esquema, sigue (el cambio de esquema es del Lote 6). `RoomReset` y un `InvalidSequence` en el registro llevan a descartar y registrarse sin cursor; sin red, la sala queda abierta sin registrar (si tiene metadatos). Varios pedidos que necesitan registrarse a la vez hacen un solo registro.
- Los errores de DC19: un paso que no puede escribir los metadatos o borrar se detiene y la sala sale del cliente.
- `close_room` y `shutdown` (D93, INV-K11): bajo el aplicador, que espera lo que esté en curso, cierran la sala del storage; un error al cerrar se informa y la sala sale igual del cliente.
- `status`, con la época.

**Tests exigidos.**
- Un cliente nuevo abre una sala, recibe el esquema y guarda sus metadatos; al reabrir en disco presenta el cursor con la encarnación y no descarta nada.
- La sala borrada y recreada en el servidor: `RoomReset`, la época sube, la réplica queda vacía y el registro es sin cursor. Lo mismo con `InvalidSequence` en el registro (con el transporte que fabrica la respuesta, o con el `data_dir` del servidor restaurado desde una copia anterior).
- Negativos de INV-K3: `RoomLocked`, `Io` (con el motor de prueba), `SchemaMismatch` y `SnapshotConstraints` al abrir no descartan; un error de E/S al leer los metadatos tampoco. Los metadatos que faltan con datos en el storage, sí; un `.snap` con un byte cambiado, sí.
- `delete_room` que falla (`RoomLocked`) en un descarte y en una apertura sin metadatos: la sala sale del cliente sin registrarse, y la próxima apertura vuelve a borrar.
- Sin red: abre con metadatos y queda sin registrar; sin metadatos es un error.
- `shutdown` con una compactación en curso cierra la sala, y al reabrir en el mismo proceso no falta nada (D93).

**Interacciones a revisar:** INV-Y7 en el servidor (el cursor con la encarnación); la recuperación del storage al reabrir; el roster del servidor ante un registro repetido; la regla de DC12 por el origen de cada error.

**Documentos:** `ARCHITECTURE.md` §11 (la apertura, el registro y el descarte); `INVARIANTES.md`: el estado de K3, K5 y K11.

### Lote 5 · El aplicador y `sync` (`zemdb-client`)
**Objetivo:** el único camino de los lotes al storage (DC7, DC21) y ponerse al día.

**Qué se hace:** el aplicador con su época, el prefijo, el hueco, la tarea propia de cada cambio, la reapertura ante un error del storage y el head visto; `sync` (pull paginado hasta `has_more: false` y `Ack`); la validación de `SyncBatch` y `AckConfirmed`; el registro ante un head menor o `InvalidSequence` (DC21); `BehindCompaction` deja la sala esperando un snapshot (que baja el Lote 8).

**Tests exigidos.** Las operaciones las manda al servidor un cliente crudo, por el transporte de prueba.
- `sync` con lotes chicos (`has_more`) llega al head y manda `Ack`.
- Negativos: un lote con hueco, uno no contiguo y uno de una época anterior (pedido antes de un descarte) no se aplican; uno demorado aplica solo lo que falta.
- Un head menor que el del pedido, y un `InvalidSequence` en un sync o un ack: se vuelve a registrar.
- Un error del storage en `apply_batch` (con el motor de prueba): reabre la sala y sigue.
- Soltar `sync` en cada `.await`: la réplica queda en un head que el aplicador alcanzó entero, con sus decisiones entregadas.

**Interacciones a revisar:** `apply_batch` (`SequenceMismatch`, INV-S1); el registro del Lote 4; el presupuesto de respuestas del servidor (`Unavailable` en un sync).

**Documentos:** `ARCHITECTURE.md` §11 (el aplicador y el sync); `INVARIANTES.md`: K1 y K12.

### Lote 6 · El esquema (`zemdb-client`)
**Objetivo:** cambiar solo a un esquema que evoluciona del local, en el orden de DC5, y fallar cerrado si no (DC20).

**Qué se hace:** la decisión sobre el esquema de `Registered`, con los metadatos antes de `reload_schema` en una tarea propia; las señales de `SyncBatch` y de `apply_batch` (`SchemaValidation`), con un registro por señal (las de `CommitAck` y `HeartbeatAck` las suman los Lotes 7 y 8 con la misma función); fallar cerrado, con la baja, sin registros y con las esperas resueltas. El comentario de `schema_id` en `CommitAck` de `zemdb-core` deja de mencionar `GetSchema`.

**Tests exigidos.**
- Un `add_column` con `UNIQUE` en el servidor: al registrarse, los metadatos tienen el esquema nuevo antes de `reload_schema`; en medio del log, se registra, recarga y aplica, con las mismas decisiones que el oráculo del storage. Un corte (de proceso) entre guardar y recargar: al reabrir, abre con el nuevo.
- Una respuesta con una versión anterior del mismo esquema se aplica con el local, sin registrarse.
- Negativos: un esquema que no evoluciona (fabricado) falla cerrado: se da de baja, no se vuelve a registrar ante `ClientNotRegistered`, no aplica y sigue legible. Una operación que no valida ni con el esquema de `Registered` (fabricada): falla cerrado.

**Interacciones a revisar:** `reload_schema` bajo el mismo lock que los lotes (INV-S7) y `evolves_from`; INV-E3 (la versión que anuncia una respuesta); `DeregisterClient` y el roster.

**Documentos:** `ARCHITECTURE.md` §11 (el esquema); `INVARIANTES.md`: K6.

### Lote 7 · Escrituras (`zemdb-client`)
**Objetivo:** `write` con su resultado: la decisión de la réplica o el error que corresponde (DC6, DC7, DC21).

**Qué se hace:** el `MutationId`, el plazo y el backoff; la reacción a cada respuesta (tabla de §A.4); `seen`, los rechazos y los rangos sin reporte retenidos; las esperas con guards RAII; el sync después del `CommitAck`; las resoluciones de DC21; la señal de esquema de `CommitAck`. El comentario de `ErrorCode::Internal` en `zemdb-core` dice que el SDK lo reintenta.

**Tests exigidos.** Con el transporte de prueba, que pierde o fabrica respuestas, y el reloj pausado:
- Una escritura aceptada, una rechazada por `Unique` (con el nombre de la restricción) y un `Update` de una fila que no existe (`RowMissing`).
- Una respuesta perdida y el reintento deduplicado, con la misma secuencia: la decisión de la réplica.
- Un intento incierto y después un rechazo: `OutcomeUnknown`. El plazo vencido con un intento incierto: `OutcomeUnknown`; sin ninguno: `Network`.
- `ClientNotRegistered` e `InvalidSequence`: se registra y reintenta con el mismo `MutationId`.
- Un `CommitAck` con `has_more`: sincroniza hasta la secuencia. Un sync que aplica la secuencia antes de que llegue el `CommitAck`, con un rechazo: la decisión retenida. Un `CommitAck` demorado: lo mismo.
- Negativos: un `CommitAck` con una secuencia no mayor que `seen` (fabricado): `OutcomeUnknown`, nunca `Ok`; un `CommitAck` sin el sync que aplica la secuencia: la escritura no termina antes.
- Con `CommitAck`: `RoomReset`, cerrar la sala y fallar cerrado con la escritura esperando, y un `CommitAck` que llega después de fallar cerrado: `OutcomeUnknown`. Sin `CommitAck`, en los mismos casos: `Network` si ningún intento quedó incierto.
- Soltar la llamada en cada `.await`: `status` da cero escrituras en vuelo y nada retenido al terminar. Con una réplica atrasada y una escritura sin `CommitAck`, solo se retiene lo de las secuencias mayores que `seen`.
- Dos escrituras concurrentes.

**Interacciones a revisar:** el dedup del servidor (INV-Y4) y el orden del commit (el dedup antes de la retención); el aplicador del Lote 5 y el esquema del Lote 6; el registro y el descarte del Lote 4; `check_operation_size` (lo rechaza el servidor con `BadRequest`).

**Documentos:** `ARCHITECTURE.md` §11 (las escrituras); `INVARIANTES.md`: K2 y K15.

### Lote 8 · `heartbeat` y la bajada de snapshots (`zemdb-client`)
**Objetivo:** que un cliente atrasado termine al día (DC22, INV-K7).

**Qué se hace:** `heartbeat` y su reacción (la señal de esquema, el head menor, la bajada si la espera, y el `snapshot_wanted` que confirma, que sube el Lote 9); la bajada por chunks, con su validación, el tope, `SnapshotSuperseded`, `RoomNotFound` en el chunk 0, el BLAKE3 y `apply_snapshot` en una tarea propia del aplicador, con DC20 ante sus errores; el commit que recibe `BehindCompaction` se pone al día y reintenta dentro del plazo; una sala que falla cerrada no manda heartbeats.

**Tests exigidos.**
- Un cliente que entra a una sala podada baja el snapshot activo, lo aplica y sincroniza; sin snapshot usable (`RoomNotFound`), espera y lo baja cuando un heartbeat lo anuncia.
- `SnapshotSuperseded` en medio de la bajada: vuelve al chunk 0. Negativos: un chunk de otro snapshot o con otro largo (fabricado), un `total_bytes` mayor que el tope y un BLAKE3 que no coincide no se aplican, y la réplica no cambia.
- Un snapshot hecho con una versión más nueva del esquema (una columna con `UNIQUE`): se registra, recarga y lo aplica. Un snapshot con filas que no cumplen las restricciones con el mismo esquema: se descarta el snapshot, no la réplica (INV-K3).
- La sala borrada y recreada en medio de una bajada, con un snapshot activo de otro contenido o sin snapshot: el cliente recibe `RoomReset`, `SnapshotSuperseded` o `RoomNotFound`, se vuelve a registrar, recibe `RoomReset` y descarta; nunca instala el snapshot de la sala nueva. Una bajada que termina después de un descarte no se instala.
- Una escritura que espera una secuencia que cubre el snapshot instalado: `OutcomeUnknown`; una ya aplicada una por una antes del snapshot conserva su decisión. Un commit con `BehindCompaction`: se pone al día y termina con la decisión de la réplica.

**Interacciones a revisar:** el relay (aceptación, superseded, la purga, el ancla de retención y DC23); `apply_snapshot` (INV-S5, `SnapshotBehind`, `SnapshotHeadMismatch`); el aplicador y lo retenido de DC21.

**Documentos:** `ARCHITECTURE.md` §11 (el heartbeat y la bajada); `INVARIANTES.md`: K7, K12 y K15.

### Lote 9 · La subida de snapshots (`zemdb-client`)
**Objetivo:** que el designado suba su snapshot (DC22, INV-K7).

**Qué se hace:** la confirmación con un heartbeat, `create_snapshot` bajo el aplicador y la subida en chunks del largo que exige el relay; `SnapshotSuperseded` y los demás errores la detienen.

**Tests exigidos.**
- Sin snapshot, el designado lo sube y el atrasado lo baja; con un snapshot de un largo que no es múltiplo de 4 MiB (más de un chunk) y con uno de un chunk.
- La subida recibe `SnapshotSuperseded` y deja de subir; una sala que falla cerrada o que espera un snapshot no sube.
- La sala borrada y recreada entre la designación y la subida: el cliente no sube a la sala nueva (DC23), se vuelve a registrar y descarta; también si, mientras sube, otro comando descarta la réplica y se vuelve a registrar en la sala nueva. Después de un descarte en la misma encarnación (D78, fabricado), no manda más chunks.

**Interacciones a revisar:** la designación y su plazo, el ancla de retención de una subida en curso, la aceptación del relay (`tail_seq − 1 ≤ S ≤ head_seq`, `S` mayor que el activo), `create_snapshot` en los dos motores.

**Documentos:** `ARCHITECTURE.md` §11 (la subida); `INVARIANTES.md`: K7.

### Lote 10 · El arnés de simulación (`zemdb-client`, tests)
**Objetivo:** el arnés de §5.3 del plan de la fase, las capas 2 y 3 de su §5.2.

**Qué se hace:** lo que dice §5.3, salvo SSE, que llega con el `SyncWorker` (4.3), y con estas decisiones:
- El transporte que inyecta fallas envuelve al de prueba; el reloj pausado, los clientes en memoria y en disco (algunos dispositivos del mismo dueño), los cortes (soltar la llamada en un `.await` al azar) y los crashes (reabrir el cliente desde su directorio); el archivista, que llama a `sync` después de cada paso.
- El oráculo es el del storage (`crates/storage/tests/common/oracle.rs`), incluido con `#[path]` para no duplicarlo.
- Reiniciar el servidor: como el transporte corre cada pedido en una tarea propia, el arnés espera esas tareas y las del Control Plane, apaga las salas (`shutdown_all`) y vuelve a armar el servidor sobre el mismo directorio. Un `RoomLocked` que quede es reintentable para el cliente. Una reasignación repite `shutdown_room` y `get_or_spawn` hasta que `GET /admin/rooms/{id}` da el `schema_id` nuevo (requisito 7).
- Los descartes se observan por la época de `status`, y la memoria, por sus contadores (DC18).
- La app de referencia (§6 del plan de la fase) es uno de los esquemas, con sus escenarios que no dependen de 4.4: la tabla de posiciones que converge, los rechazos (`Duplicate`, `ForeignKey`, `Unique`, `Restrict`), dos dispositivos del mismo dueño y un miembro que entra a un ranking podado.

**Verificaciones:** las de §5.3: en cada paso, cada réplica igual al oráculo en su head (INV-K1); al final, todas al día e iguales, con las mismas decisiones (INV-K4, K7); cada escritura terminó una vez, con una respuesta válida por INV-K2; los descartes, solo por las causas de INV-K3; lo retenido, dentro de las cotas de INV-K15.

**Tamaños:** una versión corta en la suite (unos segundos en debug) y una larga con `#[ignore]`, en `.github/workflows/randomized.yml`. La semilla se imprime al fallar.

**Documentos:** `INVARIANTES.md`: *Automatizable* de K1, K2, K4 y K15 apunta al arnés.

### Lote 11 · Robustez: fuzzing de respuestas y snapshots (`zemdb-client`, tests)
**Objetivo:** INV-K12 (capa 4 de §5.2 del plan de la fase).

**Qué se hace:** el transporte del arnés devuelve bytes al azar, respuestas válidas con bytes cambiados, y mensajes bien formados que violan el protocolo (con los generadores de `crates/core/tests/common`); los chunks de snapshot, con datos al azar o con un snapshot válido dañado (también con Zstd). Ninguno hace entrar en pánico al cliente, la réplica sigue siendo un prefijo del log del oráculo, y ninguna reserva pasa del tope. Versión corta y larga.

**Documentos:** `INVARIANTES.md`: *Automatizable* de K12.

### Lote 12 · Auditoría amplia y cierre
- La auditoría amplia del SDK (DC3, §3), con el método de `INVARIANTES.md` §1. Si no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.2, cada uno con su revisión de delta, antes de abrir 4.3 y 4.4.
- `INVARIANTES.md`: el estado de cada invariante auditada. El plan de la fase: 4.2 cerrada. `ROADMAP.md`: "Lo que sigue", 4.3 y 4.4.

---

## 3. Auditoría amplia (DC3)

Al cerrar 4.2, sobre el commit del Lote 11, con tres auditores en paralelo, de contexto fresco y solo lectura, cada uno con sus corridas en una copia del repo (`INVARIANTES.md` §1.6):

| Auditor | Invariantes | Foco |
|---|---|---|
| Réplica y log | K1, K4, K5, K6 (y C1 en el cliente) | El aplicador, la época, el registro, los metadatos y el cambio de esquema |
| Escrituras | K2, K11, K15 | El pipeline de escritura, los reintentos, lo retenido, el cierre |
| Recuperación y robustez | K3, K7, K12, S8 y R6 | El descarte y `delete_room`, los snapshots y el relay, la validación de las respuestas |

Cada uno recibe `INVARIANTES.md`, `ARCHITECTURE.md`, este plan, el de la fase (decisiones DC1 a DC23) y el arnés, y responde solo con veredictos. Las invariantes del SDK que 4.2 no implementa (K8, K9, K10, K13, K14 y las partes de 4.3 a 4.6 de las demás) quedan 🆕 y las audita la revisión de cierre de la sub-fase que las implementa. El informe va a `docs/audits/`, con sus tests como parches si no entran a la suite.

---

## 4. Seguimiento

**Avance:** Lote 0 cerrado el 27 de Octubre de 2026, con la revisión cruzada en verde en su cuarta ronda. DC18 adoptada el 28 de Octubre, con métodos en lugar de un `execute(Command)` y los pasos `sync` y `heartbeat` como API de bajo nivel que la fachada de 4.4 esconde. DC19 a DC23 quedan *Propuestas* hasta que el dueño del proyecto las confirme; el Lote 1 empieza después.
