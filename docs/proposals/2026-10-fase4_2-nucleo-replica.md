# Núcleo de la réplica — Plan de la sub-fase 4.2

**Fecha:** 26 de Octubre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lotes 0 (el contrato), 1 (`delete_room`), 2 (la encarnación en los chunks del relay), 3 (el esqueleto del cliente), 4 (apertura, registro, descarte y cierre), 5 (el aplicador y `sync`), 6 (el esquema) y 7 (las escrituras) cerrados. DC18 a DC23 adoptadas el 28 de Octubre de 2026. El próximo es el Lote 8.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.2. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).

---

## 0. Resumen

- **Qué.** El núcleo de `zemdb-client`: una réplica por sala contra un transporte abstracto (`TransportClient`), sin red real. Abre la sala desde sus metadatos durables, se registra, aplica los lotes por un único aplicador, resuelve cada escritura con la decisión de la réplica o como incierta, baja y sube snapshots, cambia de esquema solo a uno que evoluciona del suyo, descarta la réplica solo en los casos de INV-K3 y cierra las salas antes de apagar el runtime. Se prueba contra el servidor real en proceso, con un transporte que inyecta fallas.
- **Dónde.** En `zemdb-client`, salvo dos lotes en código auditado: `delete_room` en `zemdb-storage` (DC19), con el checklist de crash-safety de 4.1, y la encarnación en los chunks del relay, en `zemdb-core` y `zemdb-server` (DC23).
- **Qué no.** El `SyncWorker`, SSE, el transporte `reqwest` y la renovación del token (4.3); la fachada de Rust, lecturas, `watch`, el dueño completado por el SDK, el pre-chequeo, `Offline` y el cifrado (4.4); el navegador (4.5). En wasm32 el crate compila vacío hasta 4.5 (DC18).
- **Decisiones nuevas:** DC18 a DC23 (§1). Todas adoptadas por el dueño del proyecto el 28 de Octubre de 2026. DC19 ajusta DC12, DC20 ajusta DC5, y DC21 precisa DC6, DC7 y DC11.
- **Cómo cierra:** con la auditoría amplia del SDK (DC3), con tres auditores (§3).

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC1, DC5, DC6, DC7, DC11, DC12, DC14 y DC15 (§3 del plan de la fase). Esta sub-fase agrega:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC18** | La forma del núcleo: métodos con datos serializables, el transporte, los pasos sin worker | 3 a 9 | Adoptada (28 de Octubre de 2026) |
| **DC19** | Metadatos durables, el cursor como head del storage, y el descarte con `delete_room` | 1, 3, 4 | Adoptada (28 de Octubre de 2026; ajusta DC12) |
| **DC20** | El esquema se aprende solo de `Registered`, y fallar cerrado da de baja al cliente | 6 a 8 | Adoptada (28 de Octubre de 2026; ajusta DC5) |
| **DC21** | El aplicador, lo retenido y los reintentos | 5, 7 | Adoptada (28 de Octubre de 2026; precisa DC6, DC7 y DC11) |
| **DC22** | Snapshots en el cliente: la bajada con tope, la subida por chunks y la memoria | 8, 9 | Adoptada (28 de Octubre de 2026) |
| **DC23** | Los chunks del relay llevan la encarnación de la sala | 2 | Adoptada (28 de Octubre de 2026) |

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
- **Bajada (§A.6).** Cuando un registro deja al cliente detrás del log (`cursor < tail_seq − 1`), o un sync, un commit o un ack recibe `BehindCompaction`, la réplica espera un snapshot. En el caso de `BehindCompaction`, solo si la réplica sigue en el cursor de ese pedido: si pasó de él, la respuesta es demorada, como la del sync que un ack adelantó (precisión del Lote 5). Entonces: si `Registered` o un `HeartbeatAck` anunciaron uno usable, lo baja; si no, o si el chunk 0 responde `RoomNotFound` (no hay uno usable), espera a que un heartbeat lo anuncie. Por chunks de 4 MiB (el máximo del servidor), que se juntan en memoria hasta `max_snapshot_bytes` (configurable, por defecto 512 MiB, el de `ZEMDB_MAX_SNAPSHOT_BYTES`; requisito 6): un `total_bytes` mayor es un error, y no se baja. Cada chunk se valida antes de guardarlo: la misma secuencia, `total_chunks`, `total_bytes` y hash que el primero, su índice y su largo. Ante `SnapshotSuperseded`, se descarta lo bajado y se vuelve al chunk 0. Al terminar, el BLAKE3, y `apply_snapshot` con el esquema local y `S`, en una tarea propia del aplicador, con DC20 ante sus errores. Después de instalarlo, `(head anterior, S]` queda sin reporte (DC21) y se sincroniza desde `S`. Cada chunk lleva la encarnación de los metadatos, y el relay responde `RoomReset` al chunk 0 sin ancla si la sala es otra encarnación y tiene un snapshot activo (si no tiene, `RoomNotFound`); a los anclados los cubre la purga del relay al recrear la sala, salvo que la sala nueva active un snapshot con el mismo hash, que son los mismos bytes que el chunk 0 validó en la vieja (DC23). Así el snapshot es de la encarnación de la réplica, y uno bajado en una época anterior no se instala (DC7). Un `RoomReset` en un chunk de la época actual, si la sala no falla cerrada, lleva a registrarse con el cursor (y de ahí a descartar), y un `RoomNotFound` (sin snapshot usable, o la sala borrada), a esperar un anuncio.
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
| 1 | `delete_room` | storage | DC19 | S8, S3, L7 | ✅ |
| 2 | La encarnación en los chunks del relay | core, server | DC23 | R6, R5, W1, W2 | ✅ |
| 3 | El esqueleto: comandos, transporte y metadatos | client | DC18, DC19, DC15 | W1, W2 (metadatos) | ✅ |
| 4 | Apertura, registro, descarte y cierre | client | DC19, DC12 | K3, K5, K11 | ✅ |
| 5 | El aplicador y `sync` | client | DC7, DC21 | K1, K12 | ✅ |
| 6 | El esquema | client | DC5, DC20 | K6, C1 | ✅ |
| 7 | Escrituras | client | DC6, DC7, DC21 | K2, K15 | ✅ |
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

### Lote 1 · `delete_room` (`zemdb-storage`) · ✅ Hecho
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

**Hecho.**
- **API.** `StorageEngine::delete_room(&self, room_id) -> Result<(), StorageError>`, en los dos motores.
  - En memoria: `RoomAlreadyOpen` si la sala está abierta; si no, no hace nada.
  - En disco, con las salas del motor tomadas (`write_owned`, como `open_room`): `RoomAlreadyOpen` si está abierta. Si no, corre en una tarea propia que retiene las salas y el `.lock` hasta el último borrado, y hace, en orden:
    1. `create_dir_all` del directorio;
    2. `lock_room_files`, con los dos locks (`RoomLocked` si alguno está tomado);
    3. suelta el WAL y lista los `.snap.tmp.*`;
    4. borra con `remove_if_exists` el WAL activo, `.wal.compacting`, `.wal.compacting.tmp`, los `.snap.tmp.*` y el `.snap`.

    Cada borrado lleva antes el fail point `delete_room.remove`, por ruta, y después un `sync_dir` del directorio, con el fail point `sync_dir` de `open_room`. Un error de E/S, también al listar, se devuelve y corta los borrados.
- **Tests agregados:**
  - `crates/storage/src/disk/tests/delete.rs`, nuevo:
    - `delete_room_leaves_only_the_lock_file_and_the_room_opens_empty`: con una compactación interrumpida, temporales y otra sala abierta en el mismo motor, que queda igual.
    - `delete_room_of_a_room_never_opened_leaves_only_its_lock_file`: en un directorio que todavía no existe, como en la primera apertura de una instalación nueva.
    - `delete_room_refuses_an_open_room`: `RoomAlreadyOpen` y, desde otro motor, `RoomLocked` por el `.lock`.
    - `delete_room_refuses_a_room_whose_wal_a_dropped_write_holds`: `RoomLocked` por el WAL con el `.lock` libre, y la sala borrada cuando la escritura aterriza.
    - `crash_at_any_point_of_delete_room_leaves_a_prefix_of_the_log`: en Unix, imágenes en cada punto, que abren en 15, 10, 5 y 0.
    - `failed_removal_of_delete_room_leaves_a_prefix_of_the_log`: la secuencia exacta de puntos (borrado y `sync_dir` por archivo) y un error de E/S en cada uno, con la sala abierta en el prefijo que corresponde y borrada al repetir.
    - `delete_room_dropped_during_a_removal_completes_before_the_room_reopens`: otro motor da `RoomLocked` mientras tanto, y en el mismo motor otro `delete_room` y la reapertura esperan.
  - `disk/tests/opfs_like.rs`:
    - `delete_room_runs_on_a_layer_that_fails_like_the_opfs`: sin rechazos ni handles abiertos, y solo el `.lock` después de borrar.
    - `delete_room_returns_a_removal_the_layer_refuses`: un borrado que la capa rechaza por un handle abierto se devuelve, y la sala abre en un prefijo.
  - `crates/storage/tests/memory_engine_tests.rs`: `delete_room_refuses_an_open_room_and_a_closed_one_opens_empty`.
  - Contra el esqueleto (`delete_room` devolvía `Ok` sin hacer nada) fallan por comportamiento los diez. La revisión de delta agregó `delete_room_of_a_room_never_opened_leaves_only_its_lock_file` y `delete_room_returns_a_removal_the_layer_refuses`, y reforzó el del `delete_room` soltado.
- **Mutaciones verificadas** (17, todas detectadas):
  - sacar `RoomAlreadyOpen` en cada motor;
  - soltar antes de los borrados las salas del motor o el `.lock`, o tomar las salas con un lock de lectura;
  - no soltar el WAL antes de borrar (lo rechaza la capa que falla como el OPFS);
  - no tomar ningún lock, o solo el `.lock`;
  - sin `create_dir_all`;
  - `.wal.compacting` antes que el WAL activo, o el `.snap` antes que los temporales;
  - no borrar el `.snap` o los `.snap.tmp.*`;
  - sin `sync_dir`, o uno solo al final;
  - ignorar el error del fail point o el del borrado.
- **Interacciones revisadas:**
  - **Las tareas de compactación y de `apply_snapshot` de una sala cerrada.** `close_room` las espera, y mientras corren retienen el `Arc<DiskRoom>` y con él el `.lock`, así que `delete_room` da `RoomLocked`. Una operación de archivo que sigue en el pool bloqueante después de soltar el motor retiene el WAL y su lock: también `RoomLocked` (test). El riesgo aceptado D93, un runtime apagado con una tarea del storage en curso, no cambia (INV-L7).
  - **Los locks.** Son los de `open_room`, en el mismo orden, y el `.lock` no se borra. Una apertura en otro motor da `RoomLocked` durante los borrados, y en el mismo motor espera a la tarea (test).
  - **La recuperación.** Cada estado intermedio es uno que la recuperación ya conoce, sin cambiar su código:
    - el `.snap` con `.wal.compacting` y sin WAL activo, que `open_room` crea vacío;
    - el `.snap` con temporales, que borra sin leer;
    - ninguno.

    La revisión de delta lo verificó en cada ventana de crash existente: una compactación cortada después del rename, con `.wal.compacting` por debajo del snapshot; una absorción con duplicados; un `apply_snapshot` cortado después del rename; una vuelta atrás de la rotación que falló.
  - **Los fail points por ruta.** `delete_room.remove` es nuevo y lleva la ruta del archivo; el sync usa el nombre `sync_dir` de `open_room`, con la ruta del directorio. Los demás puntos no cambian, y las imágenes de crash y la simulación del disco pasan sin cambiar sus expectativas (`cargo test -p zemdb-storage --release -- --ignored` en verde).
  - **El OPFS.** Borra por ruta sin handles abiertos, con el WAL soltado y el `.lock` sin borrar (tests en la capa que falla como el OPFS).
- **Desvíos:**
  - El `sync_dir` sigue a cada intento de borrado, también si el archivo no existía. DC19 dice "los archivos que existen": así es más corto, y un sync de más no cambia nada.
  - Un error al listar los `.snap.tmp.*` se devuelve antes de borrar nada, mientras que la recuperación lo deja pasar y los borra en la próxima apertura. Ningún test lo ejercita, porque la capa de archivos no tiene un fail point para `list`; si se tragara, la consecuencia sería benigna, porque la próxima apertura los borra.
  - `ARCHITECTURE.md` §8 suma también `reload_schema` a la lista del contrato, donde faltaba.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura), en dos rondas:
  1. **Sin bloqueantes; 4 ajustes y 3 menores.** El código de producción estaba bien. Faltaban tres tests, porque sobrevivían tres mutaciones: soltar el `.lock` antes de borrar, ignorar el error real de un borrado y quitar `create_dir_all`. También faltaba esta nota. Los menores: el bucle de reintento, ahora con la forma acotada de `open_when_unlocked`; el prefijo de los temporales, que se deriva igual que en `snap_file_path` y que el test de "solo queda el `.lock`" cubre; y el error de `list`, anotado arriba.
  2. **En verde, sin bloqueantes ni ajustes; 4 menores, atendidos.** Sobrevivía tomar las salas del motor con un lock de lectura (un segundo `delete_room` concurrente del mismo motor daba `RoomLocked` en lugar de esperar; las dos cosas son seguras para INV-S8): ahora el test del `delete_room` soltado exige que espere. Los otros: esta nota decía que contra el esqueleto fallaban "los ocho primeros"; el bucle de reintento termina con el error que lo frenó; y en INV-L2, el `delete_room` del servidor y el del storage, que la misma oración nombraba sin distinguirlos. Corrió además los tests de `delete_room` 120 veces, en seis bucles en paralelo, sin fallas.

### Lote 2 · La encarnación en los chunks del relay (`zemdb-core`, `zemdb-server`) · ✅ Hecho
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

**Hecho.**
- **API.**
  - `zemdb-core`: `room_incarnation: u128` al final de `ClientMessage::RequestSnapshotChunk` y de `UploadSnapshotChunk`, y el comentario de `ErrorCode::RoomReset`, que cubre también los chunks.
  - `zemdb-server`:
    - `LogBounds` suma `room_incarnation`, que el actor llena con la suya en `GetLogBounds`.
    - La API del relay (`api/relay.rs`) arma el futuro de los límites con la encarnación del pedido. Al resolverse, compara la del actor y responde `RoomReset` si no coincide. El relay espera ese futuro donde ya lo hacía:
      - en el chunk 0 sin ancla, después de elegir el snapshot y antes de mirar el rango (`pick_chunk`);
      - en cada chunk de una subida, después de tomar el slot de la sala y antes de su lock (`stage_chunk`).
    - La subida en un request pasa sin encarnación.
    - `RoomDetails` suma `room_incarnation` como campo propio, que `GET /admin/rooms/{id}` toma de los metadatos de la sala. `RoomManager::lifecycle_overrides` pasa a ser `room_metadata`, que devuelve todos los metadatos.
    - El mensaje de `ServerError::RoomReset` deja de hablar del cursor.
- **Tests agregados:**
  - `crates/server/tests/snapshot_relay_tests.rs`:
    - `relay_chunks_of_another_incarnation_are_room_reset_and_change_nothing`: la encarnación de `GET /admin/rooms/{id}` es la de `Registered`. Con otra, como miembro y con el secreto de administración, reciben `RoomReset` el chunk 0 sin ancla, un chunk de la subida en curso y el primero de una más nueva que la reemplazaría. Después se sirve el mismo snapshot, y la subida en curso se completa.
    - `a_member_of_a_room_created_again_gets_nothing_from_the_new_one`: con un snapshot y una subida en curso en la vieja, sin snapshot activo en la nueva el chunk 0 da `RoomNotFound`. Con uno de otro contenido, antes y después de volver a registrarse con el mismo `ClientId`:
      - el chunk 0 da `RoomReset`;
      - el anclado al snapshot viejo da `SnapshotSuperseded`;
      - los tres chunks de la subida vieja dan `RoomReset`, y la nueva conserva su snapshot.
  - `crates/server/src/api/tests/data_plane.rs`: `first_chunk_of_a_room_created_again_after_its_snapshot_was_picked_is_room_reset`. La sala se borra y se recrea entre que el relay elige el snapshot y pide el rango.
  - `crates/server/src/relay/tests/lifecycle.rs`: `chunk_that_was_under_way_when_the_room_was_deleted_is_dropped`, el gemelo del que ya existía para la subida en un request. Un chunk atado a la sala antes de que se purgue, mientras espera el rango, da `RoomNotFound` y no deja snapshot, subida ni archivos: una subida de un chunk y el primero de tres.
  - `crates/server/tests/server_integration_tests.rs` (`control_plane_room_lifecycle`): `GET /admin/rooms/{id}`, leído con `RoomDetails`, da la encarnación de `POST /admin/rooms`.
  - Los dos mensajes, con una encarnación de más de 64 bits, en el ida y vuelta de `protocol_codec_tests.rs` y en los generadores de los fuzzers (`crates/core/tests/common`).
  - Los tests que ya armaban esos mensajes suman el campo: con la encarnación de `Registered`, la de `create_room` o la de `GET /admin/rooms/{id}` (el helper `TestServer::incarnation`, también en el del binario) cuando el pedido llega al relay, y 0 donde la respuesta del relay no importa: un pedido que se rechaza antes, o el de `credential_matrix_tests.rs` con el secreto de administración, que llega al relay y solo exige que no sea 401 ni 403.
  - Contra el esqueleto (los campos, sin comparar, y `GET` con 0) fallan por comportamiento los cuatro: los tres de `RoomReset` y el de `control_plane_room_lifecycle`. El del relay lo agregó la revisión de delta: cubre un orden que el lote no cambia pero del que ahora depende, y pasa también contra el esqueleto.
- **Mutaciones verificadas** (10, todas detectadas):
  - no comparar, o comparar al revés;
  - el chunk 0 o los de una subida sin la encarnación;
  - otro error que `RoomReset`;
  - leer los límites antes de elegir el snapshot del chunk 0, o los de un chunk de subida antes de atarlo a la sala;
  - descartar el snapshot del chunk 0 ante un error de los límites;
  - `GET /admin/rooms/{id}` o los límites del actor con la encarnación 0.
- **Interacciones revisadas:**
  - **La purga del relay al crear y al borrar una sala.** Cubre los chunks anclados de la vieja (`SnapshotSuperseded`, test) y la subida en curso de la vieja, que ya no existe en la nueva. Un pedido atado a la vieja antes de la purga recibe `RoomNotFound` al tomar el lock, como antes (test). Uno que pide el rango a la nueva recibe `RoomReset`, también si eligió el snapshot antes de la recreación (test).
  - **El ancla de retención de una subida en curso y la designación.** Un chunk de otra encarnación no llega al lock de la sala, así que no crea, reemplaza ni completa una subida, y no mueve su plazo (test con la subida en curso). Una subida de un chunk toma su lugar entre las que se completan en un pedido (`PendingUpload`, 2 por sala) antes de pedir el rango, así que mientras lo espera ancla el log en 0 y ocupa ese lugar. Es transitorio, acotado por el timeout del actor, y pasa igual con cualquier chunk que se rechaza.
  - **Los reinicios.** La encarnación es durable en `meta_room.json`, la recuperación del relay descarta las subidas parciales, y `create_room` purga lo que la recuperación vuelva a activar. La excepción, previa al lote, es DEF-188 (abajo).
  - **Los chunks anclados de una bajada.** No consultan al actor, como dice DC23. Si la sala nueva activa un snapshot con el mismo hash, son los mismos bytes que el chunk 0 validó en la vieja.
  - **El presupuesto de respuestas.** El chunk 0 compara antes de reservar su frame, así que un `RoomReset` no reserva nada. Un chunk anclado sigue sin esperar a la sala (el test del presupuesto pasa igual).
  - **El tope del cuerpo de un pedido.** Los dos mensajes crecen hasta 17 bytes (varint de un `u128`), lejos del tope con un chunk de 4 MiB.
  - **`RoomDetails`.** `room_incarnation` es campo propio porque `RoomMetrics` va con `#[serde(flatten)]`, que pasa por `Content`, sin variante para un `u128`. Los tests la leen con el tipo, y `serde_json` la da entera. Las métricas salen del actor y la encarnación de los metadatos, en dos lecturas, como ya pasaba con `lifecycle_overrides`: un borrado y recreación en el medio mezcla las métricas de una sala con la encarnación de la otra. Para el relay es inofensivo, porque la encarnación devuelta es la vigente y sale del mismo `meta_room.json` que la del actor.
- **Desvíos:**
  - DC23 dice que el relay compara. Lo hace la API del relay, en el futuro de los límites que el relay espera donde ya lo hacía, así que el código de `pick_chunk`, `stage_chunk` y `SnapshotChunkUpload` no cambia. El momento de la comparación es el de DC23, y es más corto: la comparación entra en una sola función. La garantía pasa a depender de que el relay espere ese futuro después de atar el pedido a la sala y antes de cambiar nada: lo dicen los docstrings de `pick_chunk` y `stage_chunk`, y lo cubren el test del relay con la purga (subida) y los de `RoomReset` que exigen el mismo snapshot activo después (bajada).
  - `RoomManager::lifecycle_overrides` (en `actor/manager.rs`, que no estaba en *Archivos*) pasa a ser `room_metadata`, que devuelve todos los metadatos, para que `GET /admin/rooms/{id}` tome la encarnación sin otra lectura del archivo.
  - El mensaje de `RoomReset` dice "The request does not belong to this incarnation of room …", para un registro y para un chunk.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura):
  1. **Sin bloqueantes; 1 ajuste y 9 menores.** El código de producción estaba bien, y el desvío es correcto: los únicos llamadores de producción de `pick_chunk` y `stage_chunk` pasan la encarnación, y la comparación queda antes de cualquier cambio. El ajuste: sobrevivía esperar los límites de un chunk de subida antes de tomar el slot de la sala, con lo que una subida de un chunk de la encarnación vieja se instalaba en la nueva si la sala se recreaba en el medio. Ahora lo detecta el test del relay con la purga, y el docstring de `stage_chunk` dice el orden. Los menores, atendidos:
     - el plan no marcaba el cierre (la copia era anterior);
     - tres inexactitudes de esta nota;
     - dos interacciones sin su argumento, los reinicios y `PendingUpload`;
     - docstrings que no nombraban la encarnación (`RoomManager::log_bounds`, `get_room`, el campo de `RequestSnapshotChunk`);
     - *Acceptance* en `ARCHITECTURE.md` §6;
     - INV-K1, que daba el lado del servidor como pendiente;
     - las dos lecturas de `GET /admin/rooms/{id}`, anotadas arriba;
     - una línea de más en un helper de los tests;
     - un hallazgo previo al lote, al backlog.
  2. **En verde, sin bloqueantes ni ajustes; 5 menores, atendidos.** El test nuevo mata la mutación del ajuste, y los dos HTTP de `RoomReset`, la de descartar el snapshot ante un error de los límites. Los menores:
     - DEF-188 nombraba menos disparadores que los reales;
     - la fila de INV-R6 en el resumen no reflejaba la ruptura abierta;
     - una línea larga en el docstring de `pick_chunk`;
     - "los cuatro primeros" en esta nota;
     - faltaba esta ronda.
- **Al backlog** (Bajo, previo al lote; `INVARIANTES.md` §1.4):
  - **DEF-188 · Bajo · (b) · R6, L6.** `RoomManager::get_or_spawn` con un esquema, para una sala sin `meta_room.json`, le da una encarnación nueva sin purgar el relay, a diferencia de `create_room`. Si el relay todavía tiene un snapshot de una sala anterior con ese id (su directorio se perdió, o su `delete_room` se cortó o no pudo borrar los archivos antes de un reinicio), la sala nueva puede ofrecerlo cuando su head lo alcance, ya con su propia encarnación. INV-R6 pasa a ❌ Bajo (backlog). Solo se llega por la API de biblioteca, no por HTTP. Fix: purgar el relay cuando `get_or_spawn` escribe los metadatos de una sala nueva, como `create_room`.

### Lote 3 · El esqueleto: comandos, transporte y metadatos (`zemdb-client`) · ✅ Hecho
**Objetivo:** los tipos de DC18, el transporte, los metadatos de DC19 y la prueba contra el servidor en proceso, sin la lógica de las salas.

**Archivos:** `crates/client/Cargo.toml`, `src/` (los módulos que hagan falta: comandos y errores, configuración, transporte, metadatos, el cliente); `tests/common/` (el servidor en proceso y el transporte de prueba); se borra el test del stub. `ARCHITECTURE.md` §11, nueva.

**Qué se hace:** `ClientError` y `ClientConfig`; `TransportClient` y `TransportError`; el almacén de metadatos en archivos y en memoria, con `client.lock`, el id de dispositivo y `room_{id}.meta`; `Client::open` (el lock, el id de dispositivo, el `ClientId`), con `status` y `shutdown` sobre un cliente sin salas. En wasm32, el crate vacío (DC18).

**Tests exigidos.**
- Ida y vuelta de los metadatos y del id de dispositivo; un bit invertido, un archivo truncado o un temporal que quedó cuentan como que falta, sin pánico. Negativo: un error de E/S al leerlos (con una capa de archivos de prueba) es un error, no un archivo que falta, y el id de dispositivo no cambia.
- Un segundo cliente sobre el mismo directorio falla; el id de dispositivo es el mismo al reabrir y otro en otro directorio; el `ClientId` es `persona#dispositivo` y su dueño es la persona; una persona con `#` o vacía es un error de configuración.
- El transporte de prueba: un `RegisterClient` llega a `build_router` y vuelve `Registered`.

**Interacciones a revisar:** la capa de archivos (`NativeFiles`) y sus locks; los nombres de los archivos del motor en disco en el mismo directorio; `ClientId::owner` (DC15); el `ServerConfig` y los secretos del servidor en proceso.

**Documentos:** `ARCHITECTURE.md` §11 (la forma del núcleo y los metadatos).

**Hecho.**
- **API** (`zemdb-client`; en wasm32 el crate es vacío, con `#![cfg(not(target_arch = "wasm32"))]` en la raíz y las dependencias solo fuera de wasm32):
  - `Client::open(config, metadata)`:
    1. valida la persona antes de tocar los metadatos: vacía, con `#` o que no da un `ClientId` válido es `ClientError::Config`;
    2. abre el almacén, que toma el lock y da el id de dispositivo (un error es `Storage`);
    3. arma el `ClientId` `persona#dispositivo`, con el id en 32 dígitos hexadecimales en minúsculas.

    `client_id()` lo devuelve, `status(room_id)` da `RoomClosed` (todavía no hay salas) y `shutdown()` suelta los metadatos.
  - `ClientConfig { person }`; `ClientError` con `Config`, `Storage` y `RoomClosed`; `RoomStatus` con los campos de DC18. Los resultados son serializables (`serde`).
  - `TransportClient::send(&ClientMessage, token) -> Result<Vec<u8>, TransportError>`, y `endpoint(&ClientMessage)`, la ruta del Data Plane de cada mensaje, para los transportes de 4.3 y 4.5.
  - `MetadataStore` (`open`, `close`, `room`, `save_room`) y `RoomMetadata` (la encarnación, el `schema_id` y el esquema), con dos almacenes:
    - `FileMetadata<F: RoomFiles = NativeFiles>`, sobre la capa de archivos del motor: `client.lock`, `client.meta` (el id de dispositivo) y `room_{id}.meta`. Cada `.meta` es JSON con la última línea `crc32:` de los JSON del servidor, reemplazado con un `.meta.tmp` escrito, sincronizado, cerrado y renombrado, con un `sync_dir` después. Cada operación corre en una tarea propia, que sigue hasta el final aunque el llamador la suelte, después de las anteriores y con el lock del almacén, que guarda el handle de `client.lock`: `close` lo suelta recién cuando terminó la última.
    - `MemoryMetadata`, con un id de dispositivo nuevo en cada apertura.
- **Tests agregados** (20; se borró el del stub):
  - `crates/client/tests/metadata_tests.rs`:
    - `room_metadata_survive_reopening_and_each_save_replaces_them`: con una encarnación cerca de `u128::MAX`.
    - `a_file_is_replaced_by_a_synced_temporary_file_renamed_with_no_handle_open`: la secuencia exacta de operaciones de archivo de una apertura y un guardado.
    - `memory_metadata_keep_the_rooms_and_have_a_device_id_of_their_own`.
    - `damaged_room_metadata_count_as_missing` y `a_damaged_device_id_counts_as_missing_and_is_replaced`: un bit invertido en cada byte y cada truncado, sin pánico; el id nuevo queda guardado.
    - `a_temporary_file_left_by_a_cut_is_not_read`: un `.meta.tmp` con contenido válido no se lee, ni solo ni junto a metadatos anteriores, y uno de `client.meta` tampoco da su id.
    - `metadata_of_another_kind_count_as_missing`: cada archivo con el contenido del otro, y un `schema_id` que no valida bajo un CRC32 que sí.
    - `an_error_in_any_step_of_reading_or_saving_room_metadata_is_returned` y `an_error_in_any_step_of_an_open_is_returned_and_keeps_the_device_id`: con la capa de prueba, un error en cada operación de una lectura, un guardado y una apertura. Hasta el rename quedan los metadatos anteriores, y el id de dispositivo no cambia.
    - `an_open_cut_by_its_caller_holds_the_directory_until_its_device_id_is_saved`, `a_save_cut_by_its_caller_ends_before_the_next_operation` y `a_close_waits_for_a_cut_save_and_then_releases_the_directory`: con una escritura lenta que sigue después del corte, que se hace cuando el log de la capa muestra la escritura en curso.
    - `a_second_store_over_the_same_directory_fails_until_the_first_closes`.
    - `metadata_share_the_directory_of_a_disk_engine_without_touching_its_files`: el motor abre, aplica, cierra y borra la sala en el mismo directorio; la sala reabre vacía y los metadatos quedan.
  - `crates/client/tests/client_tests.rs`:
    - `the_client_id_is_the_person_and_the_device_id_of_the_directory`: el dueño es la persona; el mismo id al reabrir, otro en otro directorio.
    - `a_second_client_over_the_same_directory_fails_until_the_first_shuts_down`: con `Storage`.
    - `a_person_that_is_empty_or_holds_a_hash_is_a_configuration_error`: también con un espacio adelante o un control, y sin crear el directorio.
    - `an_error_reading_the_device_id_is_a_storage_error`, sin cambiar el id.
    - `a_client_without_rooms_has_every_room_closed`, antes y después de `shutdown`.
    - `every_message_reaches_the_endpoint_of_its_kind_through_the_test_transport`: el `ClientId` del cliente se registra (`Registered`) y cada uno de los otros ocho mensajes recibe la respuesta de su endpoint.
  - `crates/client/tests/common/`:
    - `TestServer`: las piezas del servidor sobre un directorio temporal, armadas como en los tests del servidor (sin el lock del directorio ni el barrido de vencimientos de `main`), con los secretos de prueba.
    - `TestTransport`: llama a `build_router` como servicio de `tower`, con cada pedido y la lectura de su respuesta en una tarea propia.
    - `TestFiles`: registra lo que se hace con cada archivo, hace fallar a pedido una operación sobre un archivo y puede demorar la primera escritura, que sigue aunque el llamador la suelte; `logged` espera una entrada del log.
  - Contra el esqueleto (cada función sin hacer nada, con un `ClientId` y una ruta fijos) fallan por comportamiento los 20.
- **Mutaciones verificadas** (41, todas detectadas):
  - un error de `open_read`, `size` o `read_at` tomado como un archivo que falta; un JSON válido de otro tipo como un error; no verificar el CRC32;
  - escribir sin el temporal; no sincronizarlo; descartar el error de `write_at` o de `sync_all`; tenerlo abierto al renombrar; no sincronizar el directorio, o descartar su error; no crearlo, o descartar su error;
  - un lock por apertura; `close` que no suelta el lock, o que no espera a las operaciones en curso;
  - las operaciones en el lugar, sin una tarea propia, o soltando el lock del almacén antes de terminar;
  - un id de dispositivo nuevo en cada apertura; sin guardarlo, o descartando el error al guardarlo;
  - los metadatos de la sala con el nombre del `.snap` del motor;
  - el almacén en memoria sin guardar, o con un id fijo;
  - validar la persona después de abrir los metadatos; aceptar una persona vacía o con `#`; el id de dispositivo en mayúsculas;
  - un error de los metadatos como `Config`; `shutdown` sin soltar los metadatos; `status` sin error para una sala que no está abierta;
  - cada uno de los nueve endpoints con otra ruta.
- **Interacciones revisadas:**
  - **La capa de archivos (`NativeFiles`) y sus locks.** `client.lock` se toma con `open_locked` (el lock exclusivo de `fs2`), que falla también entre dos handles del mismo proceso (test). El handle vive en el almacén hasta `close` (test).
  - **Las operaciones que siguen después de un corte.** Una operación de la capa de archivos cuyo llamador la suelta sigue hasta el final (`ARCHITECTURE.md` §8). Por eso cada operación del almacén corre en una tarea propia, con el lock del almacén: una apertura cortada retiene `client.lock` hasta que su id de dispositivo es durable, y un guardado cortado termina antes que la operación siguiente, así que no aterriza sobre un guardado posterior, y `close` espera a las operaciones en curso antes de soltar `client.lock` (tests con una escritura lenta). Apagar el runtime de Tokio corta la tarea en su próximo `.await`, y una escritura que ya pasó al pool bloqueante aterriza sin el lock: es el riesgo aceptado del motor (`ARCHITECTURE.md` §8, D93), y lo evita cerrar el cliente antes de apagar el runtime, porque `shutdown` espera.
  - **El OPFS.** Los `.meta` se renombran por ruta con el temporal ya cerrado (test de la secuencia), y como las operaciones del almacén no se solapan, ninguna lectura tiene abierto el destino de un rename.
  - **Los nombres de los archivos del motor en disco en el mismo directorio.** El motor usa `room_{id}.snap`, `.wal`, `.lock`, `.wal.compacting`, `.wal.compacting.tmp` y `.snap.tmp.*`, y solo lista con el prefijo `room_{id}.snap.tmp.`. Un id de sala no tiene `.`, así que `room_{id}.meta` y `.meta.tmp` no coinciden con un archivo de ninguna sala, y `client.*` no empieza con `room_` (test con el motor).
  - **`ClientId::owner` (DC15).** La persona no es vacía ni tiene `#`, y el dispositivo no es vacío: el dueño es la persona (test), y el servidor registra ese `ClientId` (test del transporte).
  - **El `ServerConfig` y los secretos del servidor en proceso.** `TestServer` arma las piezas como los tests del servidor, con los mismos secretos, y los tokens se firman con `generate_client_token` y el de clientes. El transporte manda el token como bearer en todos los pedidos; `/register` no lee el header y verifica el del mensaje.
  - **El presupuesto de respuestas.** Llamado como servicio, sin `serve_until_shutdown`, la reserva de una respuesta queda en sus extensiones y se suelta al tomar el cuerpo, que ya está en memoria. Alcanza para estos tests; uno que mida el presupuesto tiene que servir por HTTP.
  - **Un motor, un cliente (DC19).** `client.lock` protege el directorio de los metadatos, no el motor: nada ata ese directorio al `data_dir` del motor, y `MemoryMetadata` no tiene lock. Va a las interacciones del Lote 4, cuando `Client::open` recibe el motor.
- **Desvíos:**
  - `ClientError` y `ClientConfig` llevan lo que usa este lote. Las variantes de DC18 (`Network`, `Validation`, `ConstraintViolation`, `RowMissing`, `OutcomeUnknown`, la sala que falla cerrada) y los campos del plazo, el backoff y el tope llegan con el lote que los produce o los usa. `RoomStatus` lleva ya los campos de DC18, porque `status` necesita su tipo.
  - `Client::open` toma la configuración y los metadatos. El `StorageEngine` y el `TransportClient` se suman en el Lote 4, con `open_room`, el primero que los usa. Como el almacén no es serializable, `open` queda fuera de la regla de los datos serializables, que vale para los métodos de las salas; `client_id()` devuelve el `ClientId` por valor.
  - `endpoint`, público, no figura en DC18: es la elección del endpoint por el tipo del mensaje que DC18 deja al transporte, en una función que comparten los transportes.
  - `MetadataStore::open` toma el lock y da el id de dispositivo en una sola llamada. Borrar los metadatos de una sala es del descarte (Lote 4).
  - El id de dispositivo es un uuid v4 (122 de sus 128 bits al azar), como el `MutationId` de DC18, y `client.meta` lo guarda como número; el `ClientId` lo lleva en hexadecimal.
  - Un temporal que quedó no se borra, aunque el principio 3 de `GEMINI.md` pide limpiarlos al arrancar: nunca se lee, y lo reemplaza el próximo guardado del mismo archivo. El de `client.meta` siempre, porque solo queda si el primero no terminó; el de una sala, lo borra el descarte del Lote 4.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura):
  1. **No en verde: 3 ajustes y 5 menores.** El ajuste principal, reproducido: una apertura o un guardado cortados en un `.await` dejaban una escritura en curso en el pool bloqueante, sin el lock. Otro cliente podía abrir el directorio y usar un id que no quedaba guardado, y un guardado cortado aterrizaba sobre uno posterior, con metadatos viejos bajo un CRC32 válido. Ahora cada operación del almacén corre en una tarea propia, con dos tests. Los otros dos ajustes: sobrevivían ocho mutaciones (los errores de `size`, `read_at`, `write_at`, `sync_all`, `sync_dir`, `create_dir_all` y del guardado del id, y un JSON válido de otro tipo), ahora con tests en tabla y uno de contenido de otro tipo. Los menores, atendidos:
     - `client_id()` devolvía una referencia;
     - faltaban tres interacciones;
     - el temporal no citaba `GEMINI.md`;
     - inexactitudes en §11, INV-W1 y esta nota (los 128 bits, "como en `main`", la lista de mutaciones, `endpoint`).

     Las líneas de estado de los dos planes dan el lote por cerrado: van en el mismo commit que la nota, como en los Lotes 1 y 2, y el cierre lo confirma la CI de `main`.
  2. **No en verde: 1 ajuste y 2 menores.** Las correcciones de la ronda 1 estaban bien: sus ocho mutaciones y las de las tareas fallan. El ajuste: sobrevivía un `close` que no espera a las operaciones en curso (con `try_lock`, que con una en curso nunca soltaba `client.lock`); ahora lo detecta `a_close_waits_for_a_cut_save_and_then_releases_the_directory`. Los menores, atendidos: el riesgo aceptado del apagado del runtime, en §11 y en las interacciones, y `run` más corto. Por una observación de la ronda, los tests de corte cortan cuando el log muestra la escritura en curso, no a los 100 ms.
  3. **En verde, sin bloqueantes, ajustes ni menores.** El test de `close` mata la mutación con `try_lock`; las de las tareas (en el lugar, o soltando el guard antes de terminar) y la del handle sin guardar fallan, y los tests de corte ya no dependen del tiempo. Corrió además los tests de corte 20 veces, sin fallas.

### Lote 4 · Apertura, registro, descarte y cierre (`zemdb-client`) · ✅ Hecho
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

**Interacciones a revisar:** INV-Y7 en el servidor (el cursor con la encarnación); la recuperación del storage al reabrir; el roster del servidor ante un registro repetido; la regla de DC12 por el origen de cada error; un motor con un solo cliente (DC19: `client.lock` protege el directorio de los metadatos, que nada ata al del motor, y `MemoryMetadata` no tiene lock); el `.meta.tmp` que dejó un corte, que el descarte borra junto con los metadatos (Lote 3).

**Documentos:** `ARCHITECTURE.md` §11 (la apertura, el registro y el descarte); `INVARIANTES.md`: el estado de K3, K5 y K11.

**Hecho.**
- **API** (`zemdb-client`):
  - `Client::open(config, storage, metadata, transport)` suma el `StorageEngine` y el `TransportClient` (el desvío del Lote 3). Los metadatos son los del motor, que no usa otro cliente: un motor en disco con `FileMetadata` en su directorio de datos, o uno en memoria con `MemoryMetadata`.
  - `open_room(room_id, token)`:
    1. Una sala cerrada abre su sala del storage con el esquema de sus metadatos. Sin metadatos (nueva, o con metadatos que faltan o no verifican), o con `SnapshotCorruption` o `WalCorruption` al abrir, descarta la réplica. Cualquier otro error al abrir (`RoomLocked`, `Io`, `SchemaMismatch`, `SnapshotConstraints`) o al leer los metadatos es `Storage`, no borra nada y la sala queda cerrada.
    2. Se registra: una réplica abierta, con el head del storage como cursor y la encarnación de los metadatos; una vacía, sin ninguno de los dos. La respuesta se valida antes de usarla: `Registered` o un error, con la correlación y la sala del pedido (un error puede venir sin ellas), y `Registered` con la encarnación del cursor. `Registered` en una réplica vacía guarda sus metadatos y después abre la sala del storage con su esquema; en una abierta, la marca registrada. `RoomReset` o `InvalidSequence` al cursor descartan la réplica, que se vuelve a registrar sin cursor. Cualquier otra respuesta, o ninguna, es `Network`: una réplica abierta queda abierta sin registrar, para leer; una vacía falla, y su próxima apertura la registra sin volver a borrar.
    3. Abrir una sala abierta la registra si no lo está, con el token nuevo, que cambia ya con el lock de los registros. Un registro a la vez por sala, así que varias aperturas a la vez hacen uno solo.
  - El descarte, en el orden de DC19: borra los metadatos con `remove_room` (también el `.meta.tmp`), cierra la sala del storage, la borra con `delete_room` y suma uno a la época; la réplica queda vacía hasta `Registered`. Un error en cualquier paso lo detiene, y la sala queda cerrada con `Storage`, sin registrarse.
  - Cada cambio de la réplica (abrirla, descartarla, tomar `Registered`, cerrarla) corre bajo el aplicador de la sala, en una tarea propia, después de los anteriores: soltar la llamada no lo corta a la mitad.
  - `close_room(room_id)` cierra la sala del storage después de los cambios en curso (cerrar una réplica vacía también la cierra); un error al cerrar es `Storage`, y la sala queda cerrada igual. `shutdown()` cierra todas las salas, informa el primer error una vez cerradas todas, suelta los metadatos y después no abre ninguna (`RoomClosed`).
  - `status(room_id)`: el head del storage, la época y si está registrada, de una sala con la réplica abierta; cualquier otra es `RoomClosed`.
  - `ClientError::Network`: sin respuesta válida, o el servidor rechazó el pedido. `From<io::Error>` y `From<StorageError>` dan `Storage`.
  - El head de la sala del storage se lee fuera del aplicador (el cursor del registro, `status`): si la sala del storage se cerró en el medio, es `RoomClosed`. Otro error al leerlo para el cursor es `Storage`, y la réplica queda abierta sin registrar.
  - `MetadataStore::remove_room`: en `FileMetadata`, borra `room_{id}.meta` y `room_{id}.meta.tmp` (uno que falta no es un error) y sincroniza el directorio; en `MemoryMetadata`, los saca del mapa.
- **Tests agregados** (21, y uno ampliado):
  - `crates/client/tests/room_tests.rs` (20), contra el servidor en proceso y el motor en disco, salvo el del motor en memoria:
    - `a_new_client_keeps_the_metadata_of_registered_and_reopens_with_its_cursor`: los metadatos guardados son los de `Registered` (la encarnación del servidor, `tasks` y su esquema), la época es 1 y, al reabrir en disco, el registro lleva el cursor 3 con la encarnación, la época es 0 y las filas están.
    - `a_replica_that_opens_behind_presents_its_head_and_is_kept`: sin el `.snap` que llenó una compactación, la sala abre en 0, se registra con el cursor 0 y no se descarta.
    - `a_room_created_again_resets_the_replica_which_registers_without_a_cursor` y `an_invalid_sequence_to_the_cursor_discards_the_replica` (con el transporte que fabrica la respuesta): la época sube, la réplica queda vacía y el segundo registro va sin cursor.
    - `missing_metadata_and_damaged_data_discard_the_replica`: los metadatos borrados o con un byte cambiado, un `.snap` y un WAL con un byte cambiado (en el primero de tres lotes).
    - `errors_other_than_damage_keep_the_replica`: `RoomLocked` (otro motor tiene la sala), `Io` (con el motor de prueba), `SchemaMismatch` (un esquema sin la tabla), `SnapshotConstraints` (un `UNIQUE` que las filas del `.snap` repiten) y un error al leer los metadatos, cada uno con su mensaje: no se registra y, sin la causa, la réplica reabre como estaba.
    - `a_discard_removes_the_metadata_before_it_touches_the_storage_room`: con la capa de archivos de prueba compartida por el motor y los metadatos, el orden exacto del borrado de los metadatos y del `.meta.tmp`, con su `sync_dir`, antes que la sala del storage, y los metadatos de `Registered` guardados antes de abrirla.
    - `a_failed_step_of_a_discard_leaves_the_room_out_and_its_next_open_deletes_again`: un `delete_room` que falla en una apertura sin metadatos y en un descarte por `RoomReset`, y un cierre que falla en ese descarte; la próxima apertura borra y se registra sin cursor.
    - `a_failed_save_of_the_metadata_of_registered_leaves_the_room_out`: la sala del storage no abre, y la próxima apertura, con una capa sana, borra y se registra sin cursor.
    - `a_discard_whose_metadata_are_not_removed_keeps_the_data`.
    - `without_network_a_room_with_metadata_opens_unregistered_and_one_without_fails`: con la red, volver a abrir registra, y la réplica que quedó vacía no se vuelve a borrar (la época sigue en 1); un token que el servidor rechaza deja la sala abierta sin registrar, y uno válido la registra.
    - `a_memory_replica_reopens_empty_with_its_metadata_and_its_cursor`: con `MemoryStorageEngine` y `MemoryMetadata`, una sala cerrada reabre vacía y se registra con el cursor 0 y su encarnación.
    - `concurrent_opens_of_a_room_register_it_once`.
    - `a_room_closed_while_it_registers_stays_closed_and_keeps_its_replica`: un `close_room` mientras espera la respuesta (con el transporte lento), sin metadatos y con un `RoomReset`: `Registered` no la abre, el `RoomReset` no la descarta y la apertura es `RoomClosed`.
    - `a_reply_that_does_not_answer_the_registration_changes_nothing`: un `Registered` correcto fabricado y siete que no responden (otra correlación, otra sala, otra encarnación, un `RoomReset` de otra correlación o de otra sala, otro tipo, bytes que no decodifican).
    - `without_metadata_a_registration_without_registered_fails_the_open`: también un `RoomReset` o un `InvalidSequence` sin cursor, que no descartan otra vez.
    - `closing_a_room_closes_its_storage_room_even_if_the_close_fails`: cerrar dos veces es `RoomClosed`; un error al leer el head para el cursor falla la apertura sin registrar, y la réplica queda abierta; al reabrir la sala conserva su época y se registra con el cursor; un head que encuentra la sala del storage cerrada es `RoomClosed`.
    - `shutting_down_closes_every_room_and_reports_a_failed_close`: con dos salas, y los metadatos quedan libres.
    - `shutting_down_with_a_compaction_in_progress_closes_the_room_and_loses_nothing`: la compactación (con una escritura lenta, cortada por su llamador) termina antes de que `shutdown` vuelva, y al reabrir en el mismo proceso están las filas.
    - `closing_waits_for_a_change_of_the_replica_cut_by_its_caller`: una apertura cortada mientras guarda los metadatos de `Registered` (con una escritura lenta); `close_room` y `shutdown` esperan el cambio, que abre la sala del storage antes de que se cierre. Con la capa de archivos compartida, `shutdown` suelta `client.lock` después de cerrar la sala del storage, y después no abre ninguna.
  - `crates/client/tests/metadata_tests.rs`: `removing_room_metadata_removes_them_and_a_temporary_file_durably` (la secuencia exacta, borrar lo que falta, un error en cada paso); el de `MemoryMetadata` suma el borrado.
  - `crates/client/tests/common/`:
    - `TestTransport` registra lo que manda (`sent`, `registrations`) y puede no responder sin llegar al servidor (`offline`), responder el próximo registro con lo que fabrique el test (`answer_next_registration`) y demorar los pedidos (`slow`); `error_reply` arma un error.
    - `TestEngine` envuelve un motor y hace fallar la próxima llamada a un método; un cierre que falla cierra igual, como el del motor en disco cuando falla su último sync.
    - `TestServer` recrea una sala, da su encarnación y confirma inserciones desde un cliente propio.
    - `TestFiles::slow` arma la próxima escritura, y `logged` busca por prefijo.
  - Los tests del Lote 3 abren el cliente con un motor en memoria y un transporte que no usan.
  - Contra el esqueleto (cada función nueva vuelve sin hacer nada) fallan los 22.
- **Mutaciones verificadas** (66, todas detectadas):
  - en `open_room`: sin el lock de los registros; el token de la primera apertura; una sala con metadatos que falla sin red, una sin metadatos que abre, y una con metadatos que abre ante cualquier error del registro;
  - en el registro: una sala registrada que se vuelve a registrar; otro cursor que el head del storage; el cursor sin la encarnación, o uno sin metadatos; tomar un `Registered` de otra correlación, otra sala u otra encarnación, o un error de otra correlación o de otra sala; no descartar ante `RoomReset` o `InvalidSequence` al cursor, o descartar sin cursor; un rechazo, una respuesta inválida, ninguna o una que no decodifica que no son `Network`;
  - en el aplicador: los cambios en el lugar, sin una tarea propia, o soltando el lock antes de terminar;
  - en `status`: el head en 0, toda sala registrada, la época en 0; un head de una sala del storage cerrada en el medio que no es `RoomClosed`;
  - en `shutdown`: sin marcar el cliente, sin cerrar las salas, ocultando un cierre que falla, deteniéndose en él, soltando los metadatos antes de cerrar las salas, o sin soltarlos;
  - al abrir: volver a abrir una sala abierta, o volver a borrar una vacía; abrir después de `shutdown`; no descartar sin metadatos, con un `.snap` dañado o con un WAL dañado; descartar ante cualquier error al abrir, o ante un error al leer los metadatos;
  - en `Registered`: abrir una sala cerrada mientras tanto; no marcar registrada una abierta; no guardar los metadatos, ignorar el error al guardarlos, guardarlos después de abrir la sala del storage, o no abrirla;
  - en el descarte: descartar una sala cerrada mientras tanto; no borrar los metadatos, o ignorar el error al borrarlos; no cerrar la sala del storage, o ignorar el error al cerrarla; no borrarla, o ignorar el error de `delete_room`; dejar la sala cerrada en lugar de vacía; no sumar la época;
  - al cerrar: cerrar dos veces sin error; no cerrar la sala del storage;
  - en `remove_room`: no borrar el `.meta` o el `.meta.tmp`; un archivo que falta como error; ignorar un error al borrar; no sincronizar el directorio; `MemoryMetadata` sin borrar;
  - un error de E/S o del storage que no es `Storage`.
- **Interacciones revisadas:**
  - **INV-Y7 en el servidor.** El cursor va siempre con la encarnación de los metadatos, y sin metadatos no hay cursor: sin ella el servidor respondería `RoomReset` y la réplica se descartaría (mutación). Una sala recreada responde `RoomReset` al cursor viejo (test).
  - **La recuperación del storage al reabrir.** El cursor es el head con que abre la sala: sin el `.snap`, abre por detrás y se pone al día desde ahí, sin descartar (test). Un WAL con un lote dañado al que siguen otros es `WalCorruption` y se descarta (test); un corte al final lo recupera el motor y no es daño.
  - **El roster del servidor ante un registro repetido.** Reabrir una sala cerrada se vuelve a registrar con el cursor, y el servidor reemplaza la entrada (test). Una sala registrada no se vuelve a registrar al abrirla, y las aperturas a la vez se registran una vez (test).
  - **La regla de DC12 por el origen de cada error.** Descartan solo los casos de INV-K3 (un test por cada uno, y por cada error que no descarta), y solo con una respuesta validada: un `RoomReset` de otra correlación o de otra sala no descarta (test).
  - **Un motor, un cliente (DC19).** Sigue sin atarse en código: `Client::open` y §11 lo documentan, y la fachada de 4.4 arma el motor y los metadatos sobre el mismo directorio. Si no se cumple, dos clientes no pueden tener abierta la misma sala del motor en disco (`RoomLocked`, y `delete_room` falla igual), y una sala que uno borra mientras el otro guarda sus metadatos reabre vacía en el otro, que presenta el cursor 0 y vuelve a bajar todo (INV-K5).
  - **El `.meta.tmp` que dejó un corte.** El descarte lo borra con los metadatos y sincroniza el directorio antes de tocar la sala del storage (test).
  - **Las respuestas que llegan para una sala cerrada.** Un `close_room` no espera al registro en curso: la respuesta que llega después no abre la sala ni la descarta (test). `close_room` sí espera los cambios de la réplica en curso, y `shutdown` también; después de `shutdown`, ninguna sala abre (test).
  - **D93.** `shutdown` con una compactación en curso vuelve cuando terminó (`close_room` del motor la espera) y con la sala cerrada, así que el runtime se puede apagar después y reabrir en el mismo proceso no pierde nada (test). Suelta `client.lock` recién después de cerrar todas las salas, así que otro cliente no toma el directorio con una sala abierta (test).
  - **El motor en memoria con `MemoryMetadata`.** Cerrar una sala suelta sus datos, y los metadatos quedan: al reabrir se registra con el cursor 0 y su encarnación y se pone al día, como un origen borrado (INV-K5); `delete_room` no borra nada (test).
  - **Las lecturas del head fuera del aplicador.** El cursor del registro y `status` leen el head sin el aplicador, para no esperar detrás de un cambio: un cierre que gana esa lectura da `RoomClosed`, no `Storage` (test con el motor de prueba). No daña nada: reabrir necesita el lock de los registros.
- **Desvíos:**
  - La época no se reinicia al cerrar y volver a abrir una sala: cuenta los descartes desde que el cliente la abrió por primera vez, no desde la última apertura como dice DC18. Reiniciarla dejaría pasar una respuesta de una época anterior (DC7). Por eso cada sala que el cliente abrió queda en él mientras vive, con una entrada chica.
  - `status` de una sala sin réplica abierta (vacía hasta `Registered`) es `RoomClosed`.
  - Un registro que el servidor rechaza es `Network`, como uno sin respuesta; los rechazos permanentes (`Validation`) y el error sin efecto de las escrituras llegan con el Lote 7. Con metadatos la sala abre igual, sin registrar, como sin red: el error aparece en la próxima llamada que necesita al servidor, y `status` dice si está registrada.
  - Abrir una sala abierta la registra si no lo está, con el token nuevo: en este lote es la forma de registrar una sala que abrió sin red.
  - `Registered` con metadatos y otro esquema sigue con el local: el cambio es del Lote 6. Su head, su cola y su snapshot todavía no se usan (Lotes 5 y 8).
  - La época de `Registered` no se compara: en este lote ningún descarte corre fuera del lock de los registros, así que no cambia mientras uno espera su respuesta, y un cierre lo detecta el estado de la réplica. La comparación llega con el Lote 5, cuando el aplicador descarta por su cuenta.
  - `shutdown` marca el cliente: después, `open_room` es `RoomClosed`.
  - Un error al cerrar la sala del storage dentro de un descarte también lo detiene (DC19 nombra escribir los metadatos y `delete_room`): sin metadatos, la próxima apertura vuelve a borrar.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura):
  1. **No en verde: 3 ajustes y 8 menores.** Los ajustes: sobrevivían tres mutaciones (ignorar el error al guardar los metadatos de `Registered`, soltar los metadatos antes de cerrar las salas en `shutdown`, y volver a borrar una réplica vacía al reabrirla), ahora con tests. Los menores, atendidos:
     - un cierre que ganaba la lectura del head daba `Storage` en lugar de `RoomClosed` (reproducido con un motor demorado): ahora la lectura pasa por `head`, con test;
     - sobrevivía abrir una sala con metadatos ante cualquier error del registro: ahora con test;
     - el docstring de `Network` decía "sin efecto", que no vale para un registro;
     - faltaban las interacciones del motor en memoria y de las lecturas del head fuera del aplicador: ahora están, con test;
     - INV-K3 no decía que un error puede venir sin la correlación y la sala;
     - el token cambiaba antes de tomar el lock de los registros;
     - el cierre de una réplica vacía no estaba documentado;
     - las líneas de estado de los planes, que van en el mismo commit que esta nota.

     Corrió además los tests de salas 15 veces, sin fallas.
  2. **En verde, sin bloqueantes ni ajustes.** Las mutaciones de la ronda 1 fallan con los tests nuevos, la carrera del head da `RoomClosed`, y corrió los tests de salas 10 veces sin fallas. Dos menores opcionales:
     - que la réplica queda abierta sin registrar cuando falla la lectura del head para el cursor: ahora lo dicen §11 y esta nota, y lo fija el test;
     - que `head` convierte en `RoomClosed` también un `RoomNotFound` con la réplica abierta: solo pasa si alguien cierra la sala en el motor por fuera del cliente, contra "un motor, un cliente", y es inocuo (el próximo `close_room` la deja cerrada). Queda así.

### Lote 5 · El aplicador y `sync` (`zemdb-client`) · ✅ Hecho
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
**Hecho.**
- **API** (`zemdb-client`):
  - `sync(room_id)`:
    1. Registra la sala si no lo está, bajo el lock de los registros del Lote 4: una réplica abierta, con su cursor; una vacía, sin cursor.
    2. Pide las operaciones después del head de la réplica (`Sync`, con tantas como el servidor sirva en una respuesta) hasta que un lote sin `has_more` deja la réplica en su head.
    3. Manda `Ack` con el head alcanzado.
  - **Lo que presenta un pedido.** Un registro o un sync lee bajo el aplicador lo que presenta: la época, el token y, de una réplica abierta, el head de la sala del storage y la encarnación. Así espera el cambio en curso y nunca ve uno a medio camino (ni el cierre y la apertura de una reapertura, ni un descarte). `status` no espera: mientras un cambio está a medio camino, la sala es `RoomClosed`.
  - **La validación.** Cada respuesta se valida antes de usarla:
    - un `SyncBatch` con la correlación y la sala del pedido, cuyas operaciones siguen al cursor del pedido en orden, no pasan de su head y, sin `has_more`, llegan a él; con `has_more`, trae alguna;
    - un `AckConfirmed` con la correlación, la sala y la secuencia del ack;
    - un error puede venir sin la correlación y la sala.

    Cualquier otra respuesta, o ninguna, es `Network` y no cambia nada. Las comparaciones de secuencias no desbordan.
  - **Volver a registrarse.** Un head menor que el cursor del pedido (en un `SyncBatch` o en un `AckConfirmed`), `InvalidSequence` y `ClientNotRegistered` dejan la réplica sin registrar: el sync se vuelve a registrar con el cursor y sigue. Ese registro descarta la réplica ante `RoomReset` o `InvalidSequence` (Lote 4). Una segunda señal en el mismo sync es `Network`: el servidor se contradice. Una respuesta a un pedido de otra época no cambia la réplica.
  - **La época en el registro** (pasó del Lote 4). Un `Registered` para una réplica que el aplicador descartó mientras tanto no abre ni registra la réplica de la época nueva. Un `RoomReset` o un `InvalidSequence` para ella no la descarta otra vez. En los dos casos la réplica se vuelve a registrar.
  - **Un head que volvió atrás.** `Registered` tampoco registra una réplica abierta cuyo head, bajo el aplicador, quedó por debajo del cursor que presentó el registro (un motor en memoria que se reabrió en 0): la réplica se vuelve a registrar con su head.
  - **`open_room` sin red** decide bajo el aplicador si la réplica quedó abierta (y entonces abre sin registrar) o vacía (y entonces es `Network`).
  - **Esperar un snapshot.** `BehindCompaction`, en un sync o en un ack, deja la réplica esperando un snapshot si sigue en el cursor del pedido. Si pasó de ahí, la respuesta es demorada: el servidor la da a un sync que un ack adelantó, y el cliente sigue `Connected`. El sync de una réplica que espera es `Network` y no manda nada, salvo un registro que le haga falta, hasta que la sala se cierra y se vuelve a abrir, se reabre después de un error o se descarta.
  - **El aplicador** (`apply`), un cambio de la réplica como los del Lote 4, en una tarea propia bajo el lock de la sala:
    - un lote de otra época, o para una réplica cerrada, no se aplica;
    - se descarta el prefijo que la réplica ya tiene;
    - un lote que no empieza en head + 1 no se aplica, y el sync vuelve a pedir desde el head;
    - si no, `apply_batch`, y el head nuevo es el de su `ApplyReport`.
  - **La reapertura.** Ante `Io` o `RoomFailed` de `apply_batch`, el aplicador cierra la sala del storage y la vuelve a abrir como una apertura: un daño descarta, y otro error la deja cerrada con ese error. Si no, el error de `apply_batch` llega al sync, y la réplica reabierta queda sin registrar hasta el próximo pedido. Cualquier otro error de `apply_batch` llega al sync sin reabrir nada.
  - Por dentro:
    - `Replica::Open` lleva un `OpenReplica` (los metadatos, si está registrada y si espera un snapshot);
    - `Request` es lo que presenta un pedido;
    - `change` devuelve el resultado del cambio.
- **Tests agregados** (21), en `crates/client/tests/sync_tests.rs`, contra el servidor en proceso. Las operaciones las confirma un cliente propio del servidor (`TestServer::commit`):
  - `a_sync_in_small_batches_reaches_the_head_and_acknowledges_it`: con lotes de 2, pide desde 0, 2 y 4 y manda `Ack` 5. Al día, pide una vez y confirma el mismo head. Una sala que el cliente nunca abrió es `RoomClosed`.
  - `a_sync_registers_a_room_that_is_not_registered`: sin red, el sync de una réplica vacía es `Network`. Con red, dos syncs a la vez la registran una sola vez, sin cursor.
  - `a_reply_that_does_not_answer_the_sync_or_the_ack_is_not_used`: 21 respuestas fabricadas.
    - Al sync: un lote válido; uno con `has_more` que llega a su head (el sync pide más igual); un hueco; operaciones fuera de orden; más allá de su head; sin llegar a él y sin `has_more`; `has_more` sin operaciones; una operación en la secuencia 0; de otra correlación; de otra sala; otro tipo; un rechazo de otra correlación o de otra sala; bytes que no decodifican.
    - Al ack: una confirmación válida; de otra secuencia, de otra correlación o de otra sala; otro tipo; un rechazo de otra correlación; bytes que no decodifican.

    Las inválidas son `Network`, no aplican nada ni registran, y el sync siguiente sigue. Las válidas mandan los pedidos que corresponden.
  - `a_batch_asked_for_before_a_discard_is_not_applied`: un sync con la respuesta retenida mientras la sala se recrea. Otro sync recibe `ClientNotRegistered`, se registra con el cursor, recibe `RoomReset` y descarta, y el lote de la sala vieja no se aplica. Tampoco cambian la réplica nueva un `BehindCompaction` ni un `InvalidSequence` de la época anterior: ni la dejan esperando ni la dejan sin registrar.
  - `a_batch_for_a_room_closed_meanwhile_is_not_applied`: es `RoomClosed`, y al reabrir el cursor es 0.
  - `a_delayed_batch_applies_only_what_the_replica_lacks`:
    - un lote de 1 a 4 que llega con la réplica en 2 aplica el 3 y el 4 sin volver a pedir, y se fija la secuencia exacta de pedidos;
    - uno con head 2 que llega con la réplica en 4 no lleva a registrarse.
  - `a_head_below_the_request_or_an_invalid_sequence_registers_again`: en un sync (con la réplica en 3) y en un ack (después de un sync desde 0), un head menor, `InvalidSequence` (también sin la correlación ni la sala) y `ClientNotRegistered`. En cada caso hay un registro más, con el cursor 3 y la encarnación, y el sync termina con `Ack` 3.
  - `a_server_that_keeps_asking_to_register_again_ends_the_sync`: con todo sync respondido con `ClientNotRegistered` o con un head menor que el pedido, y con todo ack respondido igual, el sync se registra una vez más y termina con `Network`.
  - `a_sync_or_an_ack_the_server_refuses_fails_and_changes_nothing`: `Unavailable` (el presupuesto de respuestas del servidor), `Internal` y `Unauthorized`, en un sync y en un ack.
  - `behind_the_log_of_the_server_the_replica_waits_for_a_snapshot`: en un sync y en un ack. El sync siguiente no manda nada, y cerrar y abrir la sala vuelve a pedir. Un `BehindCompaction` demorado, que llega con la réplica ya por delante del pedido, no la deja esperando.
  - `a_failed_write_of_a_batch_reopens_the_storage_room_and_the_next_sync_goes_on`: con la capa de archivos de prueba, falla el `sync_data` del WAL. El sync es `Storage`. La sala reabierta tiene el lote (head 3) y queda sin registrar. El sync siguiente se registra con el cursor 3 y confirma.
  - `only_a_storage_error_after_which_the_room_may_have_changed_reopens_it`: con el motor de prueba, `Io` y `RoomFailed` reabren (un `open_room` que falla deja la sala cerrada y su error es el que llega); `SequenceMismatch` y `Other`, no.
  - `a_reopen_that_finds_damage_discards_and_one_that_fails_to_close_leaves_the_room_out`:
    - `SnapshotCorruption` al reabrir descarta: los metadatos se borran, el sync siguiente se registra sin cursor y la época es 2;
    - un cierre que falla deja la sala cerrada, y abrirla de nuevo la registra con el cursor 0.
  - `a_batch_that_does_not_follow_the_head_is_not_applied_and_the_sync_goes_on`: con el motor en memoria, reabierto en 0 después de un `Io`, un lote pedido antes (desde 3) no se aplica, y el sync vuelve a pedir desde 0.
  - `a_sync_waits_for_a_change_of_the_replica_in_progress`: con una apertura lenta del motor de prueba, un sync que empieza mientras la sala del storage se reabre (con `status` en `RoomClosed`) espera la reapertura, se registra y termina bien.
  - `a_registration_waits_for_a_change_of_the_replica_in_progress`: un sync espera el lock de los registros, que otro suelta (con su registro rechazado) mientras la sala del storage se reabre despacio. El registro del primero espera la reapertura en lugar de ver la sala cerrada.
  - `a_registration_answered_after_the_replica_went_back_registers_again_with_its_head`: con el motor en memoria, un registro con el cursor 3 queda retenido mientras la sala se reabre en 0 después de un `Io`. Su `Registered` no registra la réplica, que se vuelve a registrar con el cursor 0 y se pone al día.
  - `an_open_without_network_during_a_change_of_the_replica_opens_the_replica`: un `open_room` sin red cuyo registro falla mientras la sala del storage se reabre despacio espera la reapertura y abre la réplica sin registrar.
  - `a_reply_to_a_registration_of_a_replica_the_applier_discarded_changes_nothing`: un registro retenido mientras el aplicador descarta la réplica (un `Io` y un daño al reabrir). Su `Registered`, o el `RoomReset` de la sala recreada, no cambian la réplica vacía, que se vuelve a registrar sin cursor: la época queda en 2.
  - `a_sync_cut_while_it_applies_a_batch_leaves_the_batch_applied`: cortado durante la escritura del WAL (lenta), el lote se aplica entero, y el sync siguiente confirma 3.
  - `a_sync_cut_at_any_point_leaves_a_head_the_applier_reached_whole`: con lotes de 2, cortado después de 1, 2, … polls (cada uno con 1 ms de pausa, unos 8 puntos). La sala del storage queda en un head par con sus filas, y el sync siguiente llega a 6.
- **Fixtures** (`crates/client/tests/common/`):
  - `TestTransport`:
    - retiene la respuesta al próximo mensaje de un tipo (`hold_next_reply`, varias a la vez, también una fabricada);
    - contesta el próximo mensaje de un tipo (`answer_next`), todos (`answer_every`) o los que elija un closure (`answer`, ahora público);
    - achica los lotes de sync (`sync_batch_size`).
  - `TestFiles::fail_next` hace fallar una operación una sola vez.
  - `TestEngine::slow_open` demora la próxima apertura.
  - `options`, `engine`, `open_client`, `open_room` y `status` pasan de `room_tests.rs` a `common`.
- Contra el esqueleto (`sync` sin hacer nada) fallaban los 14 tests del primer borrador. Los demás llegaron con la verificación de mutaciones y con la revisión de delta.
- **Mutaciones verificadas** (69, todas detectadas; seis dejan `sync` en un ciclo sin fin y las corta el timeout):
  - lo que presenta un pedido:
    - no registrarse antes de pedir (ciclo), o hacerlo sin el lock de los registros;
    - mandar con la réplica esperando un snapshot;
    - otra época (ciclo);
    - otro cursor que el head, o ninguno;
    - una réplica que figura registrada o que figura sin esperar;
    - leer lo que presenta un sync o un registro fuera del aplicador;
  - el lote:
    - tomar uno de otra correlación o de otra sala;
    - no registrarse ante un head menor que el pedido, o seguir sin registrarse;
    - registrarse con el head igual al del pedido;
    - comparar con el head actual en lugar del del pedido;
    - aplicar un lote que no sigue al pedido, o que no sea `Network`;
  - la continuidad (`follows`):
    - fuera de orden; que arranca en otro lado;
    - más allá del head; sin llegar al head y sin `has_more`;
    - `has_more` sin operaciones;
    - una resta que desborda;
  - el fin del sync: terminar con un lote con `has_more`, o con uno que no dejó la réplica en su head;
  - el ack:
    - otro head que el alcanzado;
    - tomar una confirmación de otra correlación, de otra sala o de otra secuencia;
    - terminar con un head menor que el ack, o compararlo con el cursor del sync;
  - los rechazos:
    - tomar un error de otra correlación o de otra sala; no tomar uno sin la correlación o sin la sala;
    - que `ClientNotRegistered` o `InvalidSequence` no registren, o que cualquier error registre;
    - volver a pedir sin dejar la réplica sin registrar;
    - registrarse sin tope (ciclo), o sin tope ante un rechazo, un head menor en un lote o el ack;
    - no esperar el snapshot ante `BehindCompaction`, o esperarlo con la réplica ya por delante del pedido;
    - un rechazo o una respuesta inválida que no son `Network`;
    - una respuesta de otra época que cambia la réplica;
  - la época del registro: tomar un `Registered` de otra época; descartar ante un `RoomReset` de otra época; registrar una réplica que volvió por debajo del cursor;
  - `open_room` sin red: decidir fuera del aplicador; abrir una réplica vacía; fallar con una abierta;
  - el aplicador:
    - aplicar un lote de otra época o para una réplica cerrada;
    - no descartar el prefijo;
    - aplicar un lote que no sigue al head;
    - el head de antes del lote;
    - aplicar en el lugar, sin el lock ni una tarea propia;
  - la reapertura:
    - que `Io` o `RoomFailed` no reabran, o que cualquier error reabra;
    - ignorar un cierre que falla;
    - no abrir; ignorar el error de la apertura;
    - ocultar el error de `apply_batch`.
- **Interacciones revisadas:**
  - **`apply_batch` (`SequenceMismatch`, INV-S1).** El aplicador nunca le pasa un lote que no sigue al head: descarta el prefijo bajo su lock, y un lote que no sigue no se aplica (test con el motor en memoria reabierto en 0). Si igual llega `SequenceMismatch`, es un error que no reabre la sala (test).
  - **El registro del Lote 4.** El sync lo usa tal cual, bajo el lock de los registros: dos syncs a la vez registran una vez (test). Un registro con el cursor después de una señal descarta ante `RoomReset` (test, con la sala recreada) y ante `InvalidSequence` (Lote 4). Lo que presenta el registro (su cursor, su encarnación) se lee ahora bajo el aplicador, como el de un sync. El error al leer el head para el cursor sigue siendo `Storage` con la réplica abierta (test del Lote 4).
  - **El presupuesto de respuestas del servidor.** `Unavailable` en un sync o en un ack es `Network`, no registra ni cambia nada, y el sync siguiente sigue (test).
  - **El cierre y `shutdown` del Lote 4.** Un lote que llega para una sala cerrada no se aplica y el sync es `RoomClosed` (test). `close_room` espera el lote en curso (test de cortes).
  - **El motor en disco ante un corte o una falla.** Un `apply_batch` cortado a la mitad marca la sala fallida, y por eso el aplicador lo corre en una tarea propia (test del corte en la escritura del WAL). Una escritura del WAL que falla deja la sala rechazando escrituras (`RoomFailed`) hasta reabrirla (test con la capa de archivos).
  - **El motor en memoria.** Reabrir después de un error suelta sus datos: la réplica vuelve a 0, se registra con el cursor 0 y su encarnación (INV-K5) y se pone al día, y un lote pedido antes no se aplica (test).
  - **Las lecturas fuera del aplicador.** Un sync o un registro leen lo que presentan bajo el aplicador: uno que empieza durante una reapertura la espera, en lugar de ver la sala cerrada (test). `status` sigue leyendo sin esperar. Un sync que empieza mientras otro aplica un lote pide desde el head anterior, y el aplicador descarta el prefijo (test de corte).
  - **La época.** Una respuesta a un pedido anterior a un descarte no aplica nada ni cambia la réplica nueva: ni la deja sin registrar ni esperando un snapshot (tests). Tampoco la cambian un `Registered` ni un `RoomReset` para el registro de la réplica que el aplicador descartó (test).
  - **El roster y el cursor del servidor.**
    - Un sync confirma su cursor en el servidor, y ese cursor nunca baja. Por eso el servidor responde `BehindCompaction` a un sync que un ack adelantó, y deja al cliente `Connected`: esa respuesta no deja esperando a la réplica (test).
    - Un sync o un ack de antes de un descarte, o de antes de reabrir en 0 el motor en memoria, que llega después del registro nuevo, deja el cursor del servidor por encima del head de la réplica. Es solo vivacidad, y se anota en INV-K5: el servidor puede podar lo que falta, y la réplica se pone al día con un snapshot (Lote 8). No se cierra sin la encarnación en esos mensajes.
- **Desvíos:**
  - El head visto (DC21) pasa al Lote 7, que es el que lo usa (`seen`): en este lote nada lo lee. Tampoco hay decisiones que entregar: llegan con las escrituras.
  - `ClientNotRegistered` en un sync o en un ack también lleva a registrarse (DC21 lo dice para las escrituras). Sin eso, el sync de un cliente que el servidor sacó del roster, o de una sala recreada, quedaría sin salida.
  - Un error del storage que reabre la sala termina el sync con el error, en lugar de seguir: con una falla de E/S que se repite, seguir reabriría sin fin. El sync siguiente sigue desde el head reabierto, y la réplica reabierta se vuelve a registrar.
  - En este lote, esperar un snapshot termina solo al cerrar y volver a abrir la sala, al reabrirla después de un error o al descartar la réplica; la salida normal, la bajada, llega con el Lote 8. El otro caso de DC22, un registro que deja al cliente detrás del log, también pasa al Lote 8: mientras tanto lo detecta el sync siguiente, con un pedido de más.
  - Una señal para volver a registrarse vale una vez por sync, y un lote con `has_more` tiene que traer operaciones. Las dos reglas acotan el sync ante un servidor que se contradice, que no lo pide DC21.
  - El sync pide `u32::MAX` operaciones por lote, y el servidor las acota.
  - DC22 queda precisada: un `BehindCompaction` deja esperando un snapshot solo si la réplica sigue en el cursor del pedido. El Lote 8 aplica la misma regla al de un commit.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura):
  1. **No en verde: 3 ajustes y 6 menores.** Corrió los tests de sync 30 veces seguidas y 15 rondas de a dos en paralelo, sin fallas. Todo quedó atendido:
     - **Un `BehindCompaction` demorado** (al sync que un ack adelantó; el servidor deja al cliente `Connected`) dejaba esperando un snapshot a una réplica al día. Ahora la espera es solo si la réplica sigue en el cursor del pedido, comparado bajo el aplicador. Hay test.
     - **Un sync concurrente con una reapertura o un descarte veía `RoomClosed`.** Lo reprodujo cientos de veces por corrida. Ahora lo que presenta un registro o un sync se lee bajo el aplicador. Hay tests, con una apertura lenta.
     - **Sobrevivía comparar el head de `AckConfirmed` con el cursor del sync.** Ahora los casos del ack corren después de un sync desde 0.
     - **Un servidor que se contradice dejaba el sync en un ciclo.** Siempre `ClientNotRegistered`, o un lote vacío con `has_more`. Ahora vale una señal por sync, y un lote con `has_more` trae operaciones. Hay tests.
     - **Sobrevivía desmarcar la réplica con la época actual.** Ahora hay test con un `InvalidSequence` de la época anterior.
     - **Sobrevivía la resta sin chequeo en `follows`.** Ahora hay test con una operación en la secuencia 0. El chequeo del hueco ya no usa `next()`.
     - **Faltaba el roster del servidor entre las interacciones.** Su consecuencia de vivacidad queda anotada en INV-K5.
     - **Los desvíos:**
       - el head visto queda nombrado en el Lote 7;
       - el registro que deja al cliente detrás del log, en el Lote 8.
     - **§11:**
       - la espera termina también al reabrir;
       - una réplica sin registrar que espera manda su registro.

     Además, al leer bajo el aplicador apareció la comparación de la época de `Registered` que el Lote 4 había dejado para este lote. Un registro en vuelo mientras el aplicador descarta la réplica ahora no la cambia. Hay test.
  2. **No en verde: 2 ajustes y 3 menores.** Los arreglos de la ronda 1 estaban bien, y no encontró deadlocks: el orden de los locks es siempre primero el de los registros y después el aplicador. Corrió los tests de sync 30 veces seguidas y 15 rondas de a dos en paralelo, sin fallas. Todo quedó atendido:
     - **El tope de una señal por sync solo estaba probado al responder `ClientNotRegistered` a un sync.** Ahora el test cubre también el head menor en un lote y el ack.
     - **Un `Registered` de antes de una reapertura, en la misma época, registraba la réplica reabierta en 0.** Lo reprodujo con el motor en memoria. Ahora `registered` compara el head con el cursor que presentó el registro. Hay test.
     - **DC22 no estaba precisada.** Ahora sí, con su desvío.
     - **`open_room` sin red decidía fuera del aplicador si la réplica quedaba abierta.** Ahora decide bajo el aplicador. Hay test.
     - **Las líneas de estado del plan,** que dicen "en verde", quedan para la ronda que cierre así.
  3. **En verde, sin bloqueantes ni ajustes.** Los cuatro arreglos de la ronda 2 están bien y tienen test:
     - el guard del estado se suelta antes de leer el head en `registered`, y un head igual o mayor que el cursor registra sin ciclar;
     - `open_room` deja pasar tal cual cualquier error que no es de red.

     Probó dos mutaciones más, y las dos fallan. Corrió los tests de sync 30 veces seguidas y 15 rondas de a dos en paralelo, sin fallas. Un menor opcional, atendido: INV-K5 nombra la réplica que volvió atrás también en *Sostenida por*.

### Lote 6 · El esquema (`zemdb-client`) · ✅ Hecho
**Objetivo:** cambiar solo a un esquema que evoluciona del local, en el orden de DC5, y fallar cerrado si no (DC20).

**Qué se hace:** la decisión sobre el esquema de `Registered`, con los metadatos antes de `reload_schema` en una tarea propia; las señales de `SyncBatch` y de `apply_batch` (`SchemaValidation`), con un registro por señal (las de `CommitAck` y `HeartbeatAck` las suman los Lotes 7 y 8 con la misma función); fallar cerrado, con la baja, sin registros y con las esperas resueltas. El comentario de `schema_id` en `CommitAck` de `zemdb-core` deja de mencionar `GetSchema`.

**Tests exigidos.**
- Un `add_column` con `UNIQUE` en el servidor: al registrarse, los metadatos tienen el esquema nuevo antes de `reload_schema`; en medio del log, se registra, recarga y aplica, con las mismas decisiones que el oráculo del storage. Un corte (de proceso) entre guardar y recargar: al reabrir, abre con el nuevo.
- Una respuesta con una versión anterior del mismo esquema se aplica con el local, sin registrarse.
- Negativos: un esquema que no evoluciona (fabricado) falla cerrado: se da de baja, no se vuelve a registrar ante `ClientNotRegistered`, no aplica y sigue legible. Una operación que no valida ni con el esquema de `Registered` (fabricada): falla cerrado.

**Interacciones a revisar:** `reload_schema` bajo el mismo lock que los lotes (INV-S7) y `evolves_from`; INV-E3 (la versión que anuncia una respuesta); `DeregisterClient` y el roster.

**Documentos:** `ARCHITECTURE.md` §11 (el esquema); `INVARIANTES.md`: K6.

**Hecho.**
- **API** (`zemdb-client`):
  - **`ClientError::FailedClosed(room_id)`**, nueva: la sala falla cerrada. DC18 la nombraba como "la sala fallida".
  - **El esquema de `Registered`.** El esquema sale solo de `Registered`, nunca de `GetSchema`. Con el esquema de la réplica, o una versión anterior del mismo, `Registered` no cambia nada. Con otro esquema, o con una versión posterior, la réplica lo toma solo si evoluciona del suyo (`evolves_from`), en el mismo cambio del aplicador que toma `Registered`, en su tarea:
    1. guarda los metadatos con el esquema nuevo;
    2. la sala del storage lo recarga (`reload_schema`);
    3. la réplica pasa a los metadatos nuevos y queda registrada.

    Un error al guardar o al recargar saca la sala del cliente con `Storage`, y la próxima apertura abre con los metadatos que hayan quedado (DC19). Un `Registered` para un cursor mayor que el head (Lote 5) no decide nada: la réplica se vuelve a registrar con su head.
  - **Las señales de un sync.** El aplicador no aplica:
    - un `SyncBatch` que anuncia otro `schema_id` o una versión posterior del mismo (una anterior es una respuesta demorada, y se aplica con el esquema local);
    - un lote con una operación que no valida contra el esquema de la réplica (`SchemaValidation` de `apply_batch`).

    En los dos casos la réplica se vuelve a registrar con su cursor, con el tope de una señal por sync del Lote 5, y el sync vuelve a pedir desde el head. Una operación que no valida después de volver a registrarse por una señal en el mismo sync, que le dio a la réplica el esquema de la sala, hace fallar cerrada la sala: el servidor mandó una operación que no vale con el esquema que sirve.
  - **Fallar cerrado.** Un esquema que no evoluciona, o la operación que no valida, dejan la réplica abierta, para leer, con su esquema y sin registrar (`status`: `failed_closed`). No aplica más lotes, tampoco los de un pedido anterior, y no se registra, tampoco ante `ClientNotRegistered`: `open_room` y `sync` dan `FailedClosed` sin mandar nada. Quien la hace fallar manda `DeregisterClient` a lo sumo una vez, bajo el lock de los registros, así que va después de un registro en vuelo, que ya no cambia nada; y no se manda si mientras tanto la sala se volvió a abrir y no falla cerrada. La respuesta no se usa: una baja perdida, o una que la llamada cortada antes no llegó a mandar, deja la entrada hasta que vence su lease (DC20). Cerrar la sala y volver a abrirla vuelve a intentar.
  - **`zemdb-core`:** el comentario de `schema_id` en `CommitAck` ya no menciona `GetSchema`.
- **Tests agregados** (13), en `crates/client/tests/schema_tests.rs`, contra el servidor en proceso:
  - `a_column_added_in_the_middle_of_the_log_is_saved_and_reloaded_before_its_operations`: un `add_column` con `UNIQUE` después de 3 operaciones. El sync se registra con el cursor 3, y los metadatos tienen el esquema nuevo cuando la sala del storage lo recarga (se leen en ese momento). Las cinco operaciones de la versión nueva dan las decisiones del oráculo del storage (incluido con `#[path]`), tres rechazos por `UNIQUE` incluidos.
  - `a_cut_between_saving_a_new_schema_and_reloading_it_opens_with_the_new_one` (en Unix): la imagen de los archivos en el momento de la recarga, como la deja un corte del proceso, abre con el esquema nuevo, sin recargarlo, desde el cursor 3, y aplica operaciones de la versión nueva.
  - `a_reply_with_an_earlier_version_of_the_schema_applies_with_the_local_one`: un lote que anuncia la versión 1 a una réplica en la 2 se aplica con la local, sin registrarse. Un `Registered` con la versión 1 tampoco cambia nada: no recarga, y los metadatos siguen en la 2.
  - `a_reply_that_announces_another_schema_registers_again_and_decides_with_registered`: una versión posterior o otro id llevan a un registro con el cursor. `Registered` trae el esquema de la réplica, así que nada cambia, y el lote pedido otra vez se aplica.
  - `a_server_that_keeps_announcing_another_schema_ends_the_sync`: `Network` después de un registro.
  - `a_room_reassigned_to_a_schema_that_extends_its_own_moves_the_replica_to_it`: una reasignación real del servidor (`shutdown_room` y `get_or_spawn` con otro esquema). Los metadatos pasan al id y al esquema nuevos, y la operación con la columna nueva se aplica.
  - `a_schema_that_does_not_evolve_from_the_replicas_fails_the_room_closed`: un `Registered` fabricado que cambia el tipo de una columna, con otro esquema o con una versión posterior del mismo. `open_room` es `FailedClosed`, el cliente se da de baja una vez y el servidor ya no lo tiene (un sync suyo recibe `ClientNotRegistered`). La réplica sigue legible con su esquema; `sync` y `open_room` no mandan nada. Cerrada y abierta otra vez, vuelve a intentar y se pone al día.
  - `a_reply_that_comes_after_the_room_failed_closed_neither_applies_nor_registers`: la respuesta a un sync que llega después de que otro sync hizo fallar cerrada la sala. Un lote con operaciones nuevas no se aplica, y `ClientNotRegistered` no registra.
  - `a_registration_answered_after_the_room_failed_closed_changes_nothing`: un registro en vuelo (llega tarde al servidor) cuando un sync hace fallar cerrada la sala. El sync manda la baja después de él, así que el servidor ya no tiene al cliente, y el `Registered` no registra la réplica.
  - `a_deregistration_is_not_sent_once_the_room_opened_again`: la sala se cierra después de fallar cerrada, mientras el sync que la hizo fallar espera el lock de los registros. Si una apertura que esperaba antes la vuelve a abrir, la registra, y el sync ya no manda la baja; si solo se cerró, la baja sale.
  - `an_operation_that_does_not_validate_with_the_schema_of_registered_fails_closed`: con cada lote fabricado con una operación de la versión siguiente, se registra una vez y falla cerrado.
  - `an_operation_that_does_not_validate_registers_again_and_applies_with_the_new_schema`: un lote que anuncia la versión de la réplica con una operación de la siguiente. Se registra, toma el esquema nuevo de `Registered` y la aplica.
  - `a_failed_save_or_reload_of_a_new_schema_leaves_the_room_out`: la sala queda fuera del cliente, con los metadatos viejos (falla el `rename`) o los nuevos (falla la recarga). La próxima apertura se pone al día con el esquema nuevo, sin darse de baja.
- **Fixtures** (`crates/client/tests/common/`):
  - `TestTransport::slow_next` demora el próximo mensaje de un tipo;
  - `TestEngine::before_next_reload` corre un closure al empezar la próxima recarga;
  - `TestServer::commit_ops` confirma operaciones cualquiera;
  - `deregistrations`;
  - `setup`, `tasks`, `request`, `answer_next` y `requests` pasan de `sync_tests.rs` a `common`.
  - El test de los errores del storage del Lote 5 exige además que no haya bajas.
- Contra el esqueleto (`ClientError::FailedClosed` sin usar) fallan por comportamiento 9 de los 10 del primer borrador. El de la versión anterior pasa: es el negativo de una regla que el esqueleto no tiene, y lo fija la mutación de tomar una versión anterior como un cambio. Los dos de las respuestas tardías y el del servidor que insiste llegaron con la verificación de mutaciones; el de la baja después de reabrir, y los casos nuevos de dos tests, con la revisión de delta.
- **Mutaciones verificadas** (34, todas detectadas; tres dejan un ciclo sin fin y las corta el timeout):
  - la comparación del esquema anunciado:
    - una versión igual o una anterior como un cambio;
    - otro id, o una versión posterior, sin cambio;
  - el aplicador:
    - aplicar un lote que anuncia un cambio;
    - aplicarlo en una réplica que falla cerrada;
    - una operación que no valida falla cerrado sin registrarse antes, o es un error del storage;
  - `Registered`:
    - tomar un esquema que no evoluciona, comparar `evolves_from` al revés, o exigirlo solo ante otro id;
    - recargar también sin cambio, o tomar como cambio una versión anterior;
    - recargar antes de guardar, no guardar, no recargar o no pasar a los metadatos nuevos;
    - un error al guardar o al recargar deja la sala en el cliente;
    - cambiar una réplica que falla cerrada;
  - fallar cerrado:
    - sin dejar la réplica sin registrar, o sin marcarla (desde `Registered` y desde el aplicador);
    - registrarse igual (ciclo);
    - `status` sin el estado, o lo que presenta un pedido (ciclo);
  - la baja:
    - no darse de baja (desde el registro o desde el sync);
    - darse de baja ante cualquier error (desde el registro o desde el sync);
    - darse de baja sin el lock de los registros, o aunque la sala ya se haya vuelto a abrir, o no darse de baja con la sala cerrada;
  - la señal en el sync: no registrarse, o registrarse sin tope (ciclo).
- **Interacciones revisadas:**
  - **`reload_schema` (INV-S7) y el aplicador.** La recarga corre en el cambio que toma `Registered`, bajo el aplicador: ningún lote se aplica entre guardar y recargar, y el motor además recarga bajo el lock de sus lotes, así que ninguno se aplica con dos esquemas. `reload_schema` no escribe en disco, así que un corte entre guardar y recargar deja lo mismo que después de recargar, y la apertura usa el esquema de los metadatos (test con la imagen). Un error al recargar cierra la sala, porque los metadatos ya tienen el esquema nuevo y la sala del storage no (test).
  - **`evolves_from`.** Se compara contra el esquema de los metadatos, que es siempre el de la sala del storage: los dos cambian juntos, en el mismo cambio, y si algo falla en el medio la sala sale del cliente. Un esquema que no evoluciona falla cerrado (test); una reasignación real a uno que lo extiende pasa (test). El motor vuelve a verificarlo en `reload_schema` (`SchemaNotEvolving`).
  - **INV-E3, la versión que anuncia una respuesta.** Una versión anterior del mismo esquema es una respuesta demorada, y sus operaciones valen con el local (test). Otro id o una versión posterior llevan a registrarse, y se decide contra `Registered` (test). Las operaciones de un lote que anuncia un cambio no se aplican nunca con el esquema viejo, ni aunque `Registered` no traiga cambio: el sync las vuelve a pedir.
  - **Las señales y el registro del Lote 5.** La señal de esquema usa el mismo `register_again` y el mismo tope de una por sync (test del servidor que insiste). Con un servidor que cumple INV-E3, una operación que no valida no puede llegar, porque el lote que la trae anuncia un esquema que no es posterior al local, y el local evoluciona del anunciado. Por eso fallar cerrado por una operación después de volver a registrarse por una señal en el mismo sync no confunde un esquema que cambió dos veces.
  - **`DeregisterClient` y el roster.** El servidor saca al cliente (`lease_tracker.deregister_client`), y un sync suyo recibe `ClientNotRegistered` (test). La baja va bajo el lock de los registros: espera un registro en vuelo (test con el registro demorado al llegar al servidor). Si se pierde, o si un registro anterior llega después, la entrada vence con su lease (DC20).
  - **Una llamada cortada antes de la baja.** El sync que hace fallar cerrada la sala espera el lock de los registros para mandar la baja, y en `register` la baja va después del cambio que toma `Registered`. Si el llamador suelta la llamada antes, la baja no sale, y después nadie la manda: la sala ya no se registra ni sincroniza. La entrada vence con su lease, como una baja perdida (DC20). La revisión lo reprodujo con un test temporal.
  - **Una baja después de reabrir.** Si la sala se cierra y se vuelve a abrir antes de que salga la baja, la apertura la registra, y la baja ya no se manda (test). Antes, la baja sacaba del roster a la réplica nueva.
  - **El motor en memoria.** Una recarga que falla cierra la sala, que suelta sus datos: la próxima apertura se pone al día desde 0 con el esquema nuevo (la revisión lo comprobó con un test temporal: cursor 0, head 5).
  - **Las respuestas que llegan después de fallar cerrado.** Un lote no se aplica, `ClientNotRegistered` no registra, y un `Registered` no registra ni decide (tests).
  - **El descarte y la época (Lotes 4 y 5).** Fallar cerrado no descarta (INV-K3): la réplica sigue legible (test). El chequeo de la época va antes que el del esquema en `Registered` y en el aplicador. Cerrar y volver a abrir crea la réplica sin fallar cerrado (test).
  - **Los metadatos (DC19).** El esquema nuevo se guarda antes de recargarlo (test que lee el archivo en ese momento). Una réplica vacía sigue guardando los metadatos de `Registered` sin decidir nada, porque no tiene datos.
- **Desvíos:**
  - La señal de un `SyncBatch` no aplica el lote cuando `Registered` no trae cambio: el sync lo vuelve a pedir. Si el lote pedido otra vez sigue anunciando un cambio, es la segunda señal del sync, y el sync es `Network`. Así nunca se aplican las operaciones de una respuesta que anuncia un esquema más nuevo que el local (INV-K6). DC20 decía que una señal sin cambio "se trata como sin cambio de esquema".
  - Una operación que no valida hace fallar cerrado después de volver a registrarse por cualquier señal en el mismo sync, no solo por esa. Por INV-E3, ese registro ya le dio a la réplica el esquema de la sala. El registro inicial de una réplica sin registrar no cuenta: cuesta un registro de más.
  - `open_room` de una sala que falla cerrada es `FailedClosed`, con la réplica abierta para leer.
  - Un error al recargar el esquema saca la sala del cliente, como uno al guardarlo (DC19): los metadatos ya tienen el esquema nuevo, y la próxima apertura lo usa.
  - El test del corte es solo de Unix, como las imágenes de crash del Lote 1: copia archivos que la sala tiene abiertos.
  - Dejar los heartbeats al fallar cerrado llega con el Lote 8, que trae los heartbeats, y resolver las escrituras que esperan (requisito 1 de §1.1), con el Lote 7, que trae las escrituras.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura):
  1. **No en verde: 2 ajustes y 8 menores.** Corrió los tests de esquema y de sync 25 veces seguidas, 15 rondas de 3 procesos en paralelo y 12 de 6, sin fallas. Todo quedó atendido:
     - **La decisión de `Registered` tenía dos reglas sin test.** Sobrevivían exigir `evolves_from` solo ante otro id (un `Registered` de la misma id, con una versión posterior que no evoluciona, se guardaba) y tomar como cambio una versión anterior en `Registered`. Ahora hay tests.
     - **`cargo fmt --check` fallaba** en un test.
     - **Una baja después de reabrir sacaba del roster a la réplica nueva.** Ahora la baja solo sale si la sala sigue fallando cerrada. Hay test.
     - **Faltaban interacciones:** la llamada cortada antes de la baja (documentada como "a lo sumo una vez"), la baja después de reabrir y el motor en memoria.
     - **Redacción:** fallar cerrado es después de volver a registrarse por una señal, no después de cualquier registro; §3.1 decía que se aplican las operaciones de la respuesta; faltaba el desvío de las escrituras; la fila de INV-C1; el comentario de `deregister`.
     - **Más corto:** guardar y recargar, en un bloque con `?`.
     - **Opcional:** el test del registro tardío dependía de una ventana de 300 ms (ver la ronda 2).
  2. **En verde, sin bloqueantes ni ajustes; 4 menores, atendidos.** Los arreglos de la ronda 1 están bien, y la salida temprana de `deregister` se lee bajo el lock de los registros, así que entre la lectura y la baja solo puede entrar un cierre. Corrió los tests de esquema y de sync 25 veces seguidas y 12 rondas de 6 procesos en paralelo, sin fallas. Los menores:
     - **Los dos tests del registro tardío necesitaban la ventana de 300 ms también para pasar,** no solo para detectar sus mutaciones: sin demora, uno fallaba 4 de 20 veces y el otro siempre. Ahora retienen la respuesta del registro hasta que la sala falla cerrada, y con 0 ms pasan 20 de 20. La demora del pedido queda solo para detectar la baja sin el lock.
     - **La salida temprana de `deregister` también se salteaba la baja de una sala que solo se cerró.** Ahora se la saltea solo si la réplica volvió a abrirse sin fallar cerrada. El test cubre los dos casos.
     - Una redacción vieja en las interacciones, y dos de estilo (una línea de 102 columnas y un mensaje de `assert_eq!` que faltaba).
  3. **En verde, sin bloqueantes ni ajustes.** Los arreglos de la ronda 2 están bien: las mutaciones de la salida temprana de `deregister` fallan en los dos casos del test, y con 0 ms los dos tests del registro tardío pasan 30 de 30 veces. Corrió los tests de esquema y de sync 20 veces seguidas y 10 rondas de 6 procesos en paralelo, sin fallas. Un menor opcional, atendido: la baja tampoco se saltea si la réplica reabierta vuelve a fallar cerrada (salen dos bajas, inocuas), así que los documentos dicen "se volvió a abrir y no falla cerrada".

### Lote 7 · Escrituras (`zemdb-client`) · ✅ Hecho
**Objetivo:** `write` con su resultado: la decisión de la réplica o el error que corresponde (DC6, DC7, DC21).

**Qué se hace:** el `MutationId`, el plazo y el backoff; la reacción a cada respuesta (tabla de §A.4); el head visto (DC21; pasó del Lote 5, que no tenía quien lo leyera), `seen`, los rechazos y los rangos sin reporte retenidos; las esperas con guards RAII; el sync después del `CommitAck`; las resoluciones de DC21; la señal de esquema de `CommitAck`. El comentario de `ErrorCode::Internal` en `zemdb-core` dice que el SDK lo reintenta.

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

**Hecho.**
- **API** (`zemdb-client`):
  - **`write(room_id, op)`**: un commit de una operación, con un `MutationId` nuevo (aleatorio, de 128 bits). Termina una sola vez, con:
    - la decisión de la réplica sobre la secuencia asignada, una vez que la aplicó: `Ok`, `ConstraintViolation` o `RowMissing`;
    - un rechazo del servidor (`Validation`);
    - un error sin efecto (`Network`);
    - o `OutcomeUnknown`.
  - **`ClientError`**, variantes nuevas:
    - `ConstraintViolation(Violation)`, con la restricción (`Unique`, `ForeignKey` y `Restrict`, con su nombre; `Duplicate` y `NotOwner`);
    - `RowMissing`;
    - `OutcomeUnknown`;
    - `Validation(mensaje)`.

    `From<RejectReason>` da la de cada rechazo de `apply_batch`.
  - **`ClientConfig`**, campos nuevos: el plazo (`write_timeout`, 30 s) y el backoff (`backoff_base`, 100 ms, y `backoff_max`, 5 s). `Client::open` rechaza con `Config` un backoff que arranca en cero o cuyo tope es menor, y `write`, un plazo que no entra en el reloj.
  - **Los intentos** (la tabla de §A.4):
    - Una sala sin registrar se registra antes, bajo el lock de los registros. Un registro sin respuesta válida (sin red, o rechazado por el servidor) se reintenta hasta el plazo.
    - Cada intento presenta como cursor el head del storage, leído bajo el aplicador, siempre con el mismo `MutationId` y el mismo `ClientId`.
    - Entre intentos, backoff exponencial con *full jitter*: al azar hasta `backoff_base`, que se duplica en cada reintento hasta `backoff_max`. Con el reloj de Tokio, no se manda ningún intento después del plazo, y en el plazo se cortan un intento, un registro o la espera del aplicador.
    - Un intento sin respuesta válida, o con `Unavailable`, `Timeout` o `Internal`, queda incierto. Desde ahí, todo final sin `CommitAck` es `OutcomeUnknown`.
    - `ClientNotRegistered` e `InvalidSequence` dejan la réplica sin registrar, y el intento siguiente se registra con el cursor (que descarta ante `RoomReset` o `InvalidSequence`). `RequestTimeout`, `RoomLocked` y `BehindCompaction` no tuvieron efecto: se reintenta.
    - `SchemaViolation`, `BadRequest` y `Forbidden` son `Validation`. Cualquier otro error, y el plazo, son `Network`.
  - **El `CommitAck`** se valida antes de usarlo: su correlación, su sala, su `MutationId` y un catch-up contiguo desde el cursor que, sin `has_more`, llega a la secuencia asignada (vacío, una secuencia no mayor que el cursor: la de un reintento deduplicado). Si no valida, el intento queda incierto. El catch-up pasa por el aplicador como el lote de un sync; si el `CommitAck` anuncia otro esquema o una versión posterior, no se aplica y la réplica se vuelve a registrar (la señal de DC20).
  - **Después del `CommitAck`,** la escritura sincroniza con `sync` hasta que la réplica aplica su secuencia, con backoff y sin plazo, mientras la sala siga abierta.
  - **El head visto y `seen`** (DC21; pasaron del Lote 5):
    - El head visto es el mayor head del servidor que informaron las respuestas de la época, una vez validadas: `Registered` (antes del cambio que lo toma), `SyncBatch`, `AckConfirmed` y, de un `CommitAck`, la mayor entre la secuencia asignada y la última del catch-up.
    - Antes de su primer intento, con la réplica registrada, la escritura entra en su sala bajo el aplicador con `seen`: el mayor entre el head del storage y el head visto. No se vuelve a tomar en los reintentos (requisito 13).
  - **Lo retenido.** Mientras haya escrituras sin `CommitAck`, el aplicador retiene, de las secuencias mayores que el menor `seen` de ellas, los rechazos y los rangos que la réplica pasó sin reporte (los de una reapertura). Si no hay, no retiene nada. Al llegar el `CommitAck` con la secuencia `s`, bajo el aplicador:
    - si `s ≤ seen`, `OutcomeUnknown`: el servidor la había asignado antes del primer intento, así que perdió secuencias y las volvió a asignar (requisito 14);
    - si `s` es mayor que el head, la escritura espera que el aplicador le entregue la decisión de `s`;
    - si no, la decisión es la retenida (un rechazo, o `OutcomeUnknown` en un rango sin reporte) u `Ok` (requisito 4).
  - **El aplicador** entrega, después de cada `apply_batch`, la decisión de cada secuencia a la escritura que la espera y la retiene para las que no tienen `CommitAck`. Una reapertura después de un error del storage deja sin reporte las secuencias entre el head anterior y el reabierto (todas, si el head reabierto no se puede leer).
  - **Las entradas.** Cada escritura en vuelo es una entrada de su sala, bajo el lock síncrono del estado (ningún `.await` lo cruza). Un guard (`InFlight`) la quita al terminar o al soltar la llamada, y poda lo retenido que ya nadie necesita.
  - **Las resoluciones** (requisito 1). Cerrar la sala, descartar la réplica o fallar cerrada terminan todas las escrituras en vuelo y vacían lo retenido: lo hace el cambio del aplicador que deja la réplica así. Una escritura con `CommitAck`, o cuyo `CommitAck` llega después, termina con `OutcomeUnknown`; una sin `CommitAck`, con `Network`, o `OutcomeUnknown` si algún intento quedó incierto, también si fue su propio registro el que hizo fallar cerrada la sala. Una reapertura conserva las escrituras: las que esperan una secuencia que la sala reabierta ya tiene sin reporte terminan con `OutcomeUnknown`, y las demás siguen.
  - **`status`** cuenta las escrituras en vuelo y lo retenido.
  - **`zemdb-core`:** el comentario de `ErrorCode::Internal` dice que el SDK reintenta un commit con el mismo `MutationId`.
- **Tests agregados** (33), en `crates/client/tests/write_tests.rs`, contra el servidor en proceso y con el reloj pausado:
  - `a_write_ends_with_the_decision_of_the_replica_on_its_sequence`: una escritura aceptada (la réplica aplica el catch-up hasta su secuencia), un `Update` de una fila que no existe (`RowMissing`) y una fila con el tag de otra, en una sala con un `UNIQUE` (`ConstraintViolation` con el nombre `tasks_tag`).
  - `a_lost_reply_is_retried_with_the_same_mutation_id_and_gets_its_sequence`: la respuesta perdida de una escritura aceptada y de una rechazada. El reintento lleva el mismo `MutationId` y recibe la misma secuencia: la sala tiene una operación por escritura.
  - `a_refusal_after_an_uncertain_attempt_is_an_unknown_outcome`: un primer intento con `Unavailable`, `Timeout`, `Internal` o sin respuesta, y el segundo con `SchemaViolation`: `OutcomeUnknown`. Con `RoomLocked`, `RequestTimeout`, `BehindCompaction` o `ClientNotRegistered` antes, `Validation`. Y `RoomNotFound` después de `Unavailable`, `OutcomeUnknown`.
  - `an_error_of_the_storage_after_an_uncertain_attempt_is_an_unknown_outcome`: con el motor de prueba, falla leer el head después de un intento incierto.
  - `a_write_registers_in_the_epoch_of_the_room_before_its_first_attempt`: la sala se recrea mientras la réplica está abierta sin registrar. El registro de la escritura descarta la réplica, y la escritura sigue en la época nueva.
  - `each_rejection_of_the_replica_is_its_error`: cada `RejectReason`.
  - `a_refusal_of_the_server_ends_the_write`: una operación con una columna que la sala no tiene (`SchemaViolation` del servidor), y `BadRequest` y `Forbidden` fabricados, son `Validation`; `Unauthorized`, `RoomNotFound`, `SchemaNotFound`, `ProtocolVersionMismatch` y `RoomReset`, `Network`. Un solo intento cada una.
  - `a_write_without_a_commit_ack_ends_at_its_deadline`: con todo commit respondido con `Unavailable` o sin respuesta, `OutcomeUnknown`; con `RequestTimeout` o `RoomLocked`, `Network`. Termina a los 30 s, con varios intentos y ninguno después del plazo. Sin red, una sala abierta sin registrar reintenta el registro y no manda ningún commit. Un registro y un intento cuya respuesta nunca llega se cortan a los 30 s: `Network` sin ningún commit, y `OutcomeUnknown`.
  - `a_server_that_asks_to_register_again_gets_the_same_mutation_id_after_it`: `ClientNotRegistered` e `InvalidSequence` fabricados llevan a un registro con el cursor 1 y su encarnación, y a un reintento con el mismo `MutationId` y el mismo cursor. También con el cliente sacado de verdad del roster (`DeregisterClient`).
  - `a_commit_ack_with_more_syncs_up_to_its_sequence`: con 150 operaciones antes, el catch-up llega a la 100 (`has_more`), y la escritura sincroniza desde 100 hasta 151.
  - `a_write_does_not_end_before_the_sync_that_applies_its_sequence`: un `CommitAck` fabricado sin catch-up y con `has_more`. Con la respuesta al sync retenida, la escritura sigue en vuelo; al soltarla, termina con la decisión de la secuencia, que un lote siguiente no cambia.
  - `a_sequence_applied_before_its_commit_ack_comes_takes_the_retained_decision`: con dos rechazos antes del head que la réplica vio al abrir, un sync aplica la secuencia de la escritura mientras su `CommitAck` está retenido. Solo se retiene lo posterior a `seen`: el rechazo de la escritura, o nada si fue aceptada. Con la respuesta del primer intento perdida y la del reintento retenida, lo mismo.
  - `the_head_seen_comes_from_every_reply_of_the_epoch`: un `SyncBatch`, un `AckConfirmed`, el `CommitAck` de una escritura que se suelta, o la última operación de un catch-up que no se aplica (anuncia otra versión), informan el head 9, y una escritura con la secuencia 5 es `OutcomeUnknown`. No cuentan un `SyncBatch` con un hueco, después del cual una escritura con la secuencia 5 la espera y se aplica, ni uno con el head 9 que llega a un sync de antes de un descarte: una escritura de la época nueva con la secuencia 1 se aplica.
  - `what_a_write_saw_is_never_below_the_head_of_its_replica`: un `Registered` fabricado con un head menor que el cursor, al reabrir la sala, y un `CommitAck` con la secuencia del head de la réplica: `OutcomeUnknown`.
  - `a_write_waits_for_a_change_of_the_replica_in_progress`: una escritura que empieza mientras la sala del storage se reabre despacio la espera, se registra y se aplica.
  - `a_write_with_its_commit_ack_retains_nothing`: un rechazo retenido mientras la escritura espera su `CommitAck` deja de retenerse cuando llega, aunque la escritura siga esperando su secuencia.
  - `a_sequence_in_a_range_without_a_report_is_an_unknown_outcome`: un sync aplica las secuencias 1 y 2 en un lote que pierde su reporte mientras el `CommitAck` de la 2 está retenido.
  - `a_write_keeps_what_it_retains_while_a_later_one_waits`: dos escrituras sin `CommitAck` que vieron heads distintos. Lo que retiene la primera, rechazada, sigue mientras la segunda espera.
  - `a_write_sends_nothing_after_its_deadline_while_the_applier_is_busy`: un sync retiene el aplicador 40 s escribiendo su lote. Una escritura que empieza después termina a los 30 s con `Network`, sin mandar ningún commit.
  - `a_registration_that_fails_the_room_closed_ends_the_write_without_effect`: el commit recibe `ClientNotRegistered` y el registro de la escritura, un esquema que no evoluciona: `Network`, u `OutcomeUnknown` con un `Unavailable` antes.
  - `a_reopened_head_that_cannot_be_read_leaves_every_sequence_without_a_report`: con el motor de prueba sobre la capa de archivos, el lote con el rechazo de la escritura pierde su reporte, y el head de la sala reabierta no se puede leer: `OutcomeUnknown`, nunca `Ok`.
  - `a_write_waiting_for_its_sequence_syncs_with_a_backoff`: con todo sync respondido con `Unavailable`, la escritura sigue esperando su secuencia, con entre 5 y 100 syncs en 60 s.
  - `the_backoff_is_random_up_to_its_longest_delay`: con el backoff de 100 ms a 100 ms, más de 300 intentos en 30 s, ninguno a más de 100 ms del anterior, y no todos al tope. Un backoff que arranca en cero o con el tope menor es `Config` al abrir, y un plazo de `Duration::MAX` es `Config` al escribir.
  - `a_write_and_a_sync_of_a_room_that_is_not_registered_register_it_once`: los dos esperan el mismo registro.
  - `a_commit_ack_at_or_below_the_head_seen_is_an_unknown_outcome`: un `CommitAck` fabricado con la secuencia del head de la réplica, y otro con una secuencia mayor que el head de la réplica pero no que el head del servidor que vio al abrir: `OutcomeUnknown`, sin esperar.
  - `a_reply_that_does_not_answer_the_commit_leaves_the_attempt_uncertain`: un `CommitAck` de otra mutación, de otra correlación o de otra sala (que, tomado, aplicaría su operación), un rechazo de otra correlación o de otra sala, un catch-up con un hueco, fuera de orden o que no llega a la secuencia sin `has_more`, y bytes que no decodifican. El reintento recibe la respuesta del servidor, y la operación fabricada no se aplica.
  - `a_commit_ack_that_announces_another_schema_registers_again_before_applying`: un `CommitAck` con la versión 2, con la operación en el catch-up. No se aplica: la réplica se registra, `Registered` no trae cambio, y el sync la aplica.
  - `with_a_commit_ack_a_reset_a_close_or_failing_closed_end_the_write_unknown`: con el `CommitAck` y la respuesta al sync retenida, la sala recreada (el sync recibe `ClientNotRegistered`, se registra y recibe `RoomReset`) o cerrada; un `CommitAck` con otro esquema, y un `Registered` con uno que no evoluciona (se da de baja una vez); y un `CommitAck` real que llega después de fallar cerrada, que no se aplica. Todas `OutcomeUnknown`.
  - `without_a_commit_ack_a_reset_a_close_or_failing_closed_end_the_write_without_effect`: con un `RoomLocked` retenido, los mismos casos: `Network`, u `OutcomeUnknown` con un intento perdido antes. Ningún intento después.
  - `a_write_whose_batch_lost_its_report_is_an_unknown_outcome`: con la capa de archivos de prueba, falla el `sync_data` del WAL al aplicar el catch-up: la sala reabre con la secuencia y la escritura es `OutcomeUnknown`. Con lotes de 1, una escritura que espera la secuencia 3 sigue después de una reapertura en 2, y termina con la decisión de la 3.
  - `two_writes_at_once_end_with_the_decisions_of_their_sequences`: dos a la vez, y dos con el mismo tag: la primera del log lo tiene y la otra es `ConstraintViolation`.
  - `a_write_to_a_room_that_is_closed_or_fails_closed_sends_nothing`: `RoomClosed` y `FailedClosed`.
  - `a_write_cut_at_any_point_leaves_nothing_in_flight_or_retained`: una escritura y un sync a la vez, cortados después de 1, 2, … polls (unos 15 puntos, 9 con un rechazo retenido): no queda nada en vuelo ni retenido, y la escritura siguiente se aplica.
- **Fixtures** (`crates/client/tests/common/`):
  - `TestTransport::lose_next_reply` hace llegar el próximo mensaje de un tipo al servidor y pierde su respuesta;
  - `TestServer::create_room_with` crea una sala con otro esquema;
  - `commits` (los commits mandados, con su `MutationId` y su cursor) y `update_task`;
  - `request` y `answer_next` toman también un commit.
  - `tokio` con `test-util` en las dev-dependencies, para el reloj pausado.
- Contra el esqueleto (`write` devolvía `Ok` sin mandar nada) fallaban por comportamiento los 18 tests del primer borrador; el de los cortes, después de exigir que la escritura siguiente se aplique. Los demás llegaron con la verificación de mutaciones y con la revisión de delta.
- **Mutaciones verificadas** (77, todas detectadas; ocho dejan una escritura en un ciclo sin fin y las corta el timeout):
  - los intentos:
    - otro `MutationId` en cada intento;
    - sin plazo (ciclo); sin cortar en el plazo un intento, un registro o la espera del aplicador; o mandar después del plazo, una vez libre el aplicador;
    - un backoff que no se acota al plazo, que no se duplica, sin tope, sin *jitter*, o ninguno (ciclo); ninguno después del `CommitAck`;
    - aceptar cualquier backoff, o un plazo que no entra en el reloj (pánico);
    - registrarse sin el lock de los registros;
  - la reacción a cada respuesta:
    - `Unavailable`, `Timeout` e `Internal` sin dejar el intento incierto, o terminando la escritura; una respuesta inválida, o ninguna, que no lo deja incierto;
    - después de un intento incierto: un rechazo, otro error final, el plazo, una escritura terminada por su sala, un error del storage o del registro que no son `OutcomeUnknown`;
    - el error del registro de una escritura que su sala terminó, tal cual;
    - `SchemaViolation`, `BadRequest` y `Forbidden` sin `Validation`;
    - `ClientNotRegistered` e `InvalidSequence` sin registrarse, o terminando la escritura; `RequestTimeout`, `RoomLocked` y `BehindCompaction` terminando la escritura;
    - tomar un rechazo de otra correlación o de otra sala;
    - terminar una escritura sin red en lugar de reintentar el registro;
  - el `CommitAck`:
    - tomar uno de otra correlación, de otra sala o de otra mutación (ciclo);
    - no validar su catch-up (ciclo), tomar una secuencia que el catch-up no alcanza, o exigirla también con `has_more`;
    - no aplicar el catch-up, o ignorar el esquema que anuncia;
  - el head visto y `seen`:
    - no tomarlo de `Registered`, `SyncBatch`, `AckConfirmed` o `CommitAck` (o de un `CommitAck` sin la última del catch-up), tomarlo de otra época, o de un lote antes de validarlo;
    - `seen` solo con el head de la réplica, o solo con el head visto; tomarlo fuera del aplicador, en cada intento o antes de registrarse;
    - una escritura terminada por su sala que sigue;
  - la decisión al llegar el `CommitAck`:
    - `Ok` para una secuencia mayor que el head, o sin compararla antes con `seen`;
    - `Ok` sin lo retenido para una no mayor que el head, o lo retenido para una no mayor que `seen`;
    - no podar al pasar a esperar;
    - la decisión retenida de un rango, solo en su primera secuencia;
  - el aplicador:
    - no entregar las decisiones de un lote (ciclo), o no retenerlas;
    - entregarlas a una escritura que espera una secuencia posterior, o pisar una decisión ya entregada;
    - una reapertura sin rango sin reporte (ciclo), o sin él cuando el head reabierto no se puede leer;
  - lo retenido: no podar nunca, podar por el `seen` más alto, o retener también la secuencia vista;
  - las entradas:
    - un guard que no quita la entrada, o que no poda;
    - cerrar, descartar o fallar cerrado sin terminar las escrituras, o fallar cerrado sin terminarlas;
    - una escritura sin entrada que termina con `Ok`;
    - no sincronizar después del `CommitAck`, o no mirar la decisión antes;
  - `status` sin las escrituras o sin lo retenido;
  - un rechazo por una clave foránea como un `Restrict`.
- **Interacciones revisadas:**
  - **El dedup del servidor (INV-Y4) y el orden del commit.** El servidor busca el `MutationId` en el dedup antes de la retención y de validar la operación, y después de verificar el registro y el cursor (`ensure_registered`, `check_within_head`). Así un reintento después de `ClientNotRegistered` o de `InvalidSequence`, que no secuenciaron nada, recibe la secuencia del primer intento si ese llegó (test con el cliente sacado del roster), y un reintento después de una respuesta perdida, la misma secuencia (test). Un reintento de una mutación ya secuenciada que quedó detrás del log recibe su secuencia con un catch-up vacío y `has_more` (el test del servidor lo fija): la escritura espera su secuencia con syncs, y hasta el Lote 8 el sync espera un snapshot.
  - **El aplicador (Lote 5).** El catch-up de un `CommitAck` pasa por `apply`, con su época, el prefijo, el hueco y la reapertura. La entrada de una escritura pasa a esperar su secuencia bajo el aplicador antes de aplicar el catch-up, así que la entrega la alcanza; y `seen` se toma bajo el aplicador, así que toda secuencia mayor que `seen` que se entrega después queda retenida. Un sync que aplica la secuencia antes del `CommitAck` la retiene (test).
  - **El esquema (Lote 6).** La señal de `CommitAck` usa `changes` en el mismo `apply`. El catch-up nunca hace fallar cerrado por sí mismo: una operación que no valida deja la réplica sin registrar, y es el sync de la escritura el que aplica la regla de DC20. Fallar cerrado termina las escrituras (tests).
  - **El registro y el descarte (Lote 4).** La escritura se registra bajo el lock de los registros, como un sync. Un descarte termina las escrituras en vuelo, y un registro que descarta antes del primer intento deja a la escritura seguir en la época nueva (test).
  - **`check_operation_size`.** El servidor rechaza con `BadRequest` una operación que no entra en el log o en una respuesta, antes de secuenciar: es `Validation`, como un `BadRequest` fabricado (test). Una operación real de ese tamaño queda en una ventana de pocos bytes bajo el tope del frame, y no se armó.
  - **El presupuesto de respuestas del servidor.** Sin lugar, el servidor confirma el commit sin catch-up y con `has_more`: es un `CommitAck` válido, y la escritura sincroniza (test con un `CommitAck` fabricado así).
  - **Escrituras y syncs a la vez.** Las entregas y las entradas van bajo el aplicador y el lock del estado, y `status` lee sin esperar el aplicador. Dos escrituras a la vez terminan cada una con la decisión de su secuencia (test).
  - **El motor en memoria.** Una reapertura en 0 vuelve a aplicar las secuencias con su reporte, y las decisiones son las mismas (INV-Y8): una escritura que espera sigue, y lo retenido se reescribe igual. No hay un rango sin reporte, porque la sala reabre por detrás.
  - **El aplicador retenido más allá del plazo.** Un cambio largo del aplicador (un lote que escribe despacio; en el Lote 8, la instalación de un snapshot) no deja que una escritura mande después del plazo: la espera del aplicador se corta en el plazo, y el plazo se mira otra vez antes de registrarse o de mandar (test).
  - **Un registro y un sync a la vez.** La escritura se registra bajo el mismo lock que el sync y la apertura, así que una sala sin registrar se registra una vez (test), y la baja de DC20 va después de un registro en vuelo de una escritura, como de uno de un sync.
  - **Un commit demorado de antes de un descarte.** El commit no lleva la encarnación: si llega al servidor después de que el cliente se registró en la sala recreada, se secuencia ahí. La escritura termina como incierta (el descarte la termina), pero su operación queda en la sala nueva (la revisión lo reprodujo con un commit demorado 5 s). Queda como límite aceptado en INV-K2; cerrarlo pediría la encarnación en el commit, como DC23 en los chunks.
  - **El reloj pausado.** Los tests corren con `start_paused`; el motor en disco hace su E/S en `spawn_blocking`, que frena el avance automático del reloj, así que un plazo no vence mientras el storage trabaja.
- **Desvíos:**
  - Una secuencia asignada no mayor que `seen` es `OutcomeUnknown` aunque sea mayor que el head de la réplica. DC21 primero comparaba con el head, y una así esperaba la decisión de esa secuencia, de otra operación. Con un servidor que no pierde secuencias, nunca pasa.
  - `BehindCompaction` en un commit es un intento sin efecto, que se reintenta. Dejar la réplica esperando un snapshot y ponerse al día dentro del plazo llegan con el Lote 8. Hasta entonces, una escritura detrás del log termina con `Network` en el plazo.
  - Hasta el Lote 8, una escritura con `CommitAck` cuya réplica espera un snapshot sigue esperando hasta que la sala se cierre.
  - `ConstraintViolation` lleva un `Violation` con la restricción de cada clase, con su nombre cuando lo tiene, en lugar de `{ constraint, kind }` (§7 de la Fase 3.11).
  - La reacción a cada código es la de §A.4, con el error de 4.2 que corresponde: `Forbidden` es `Validation` (§A.1: permanente para ese token), y `Unauthorized` sin token nuevo, `RoomNotFound`, `SchemaNotFound`, `ProtocolVersionMismatch`, `RoomReset` y los demás, `Network`.
  - Un registro de la escritura que el servidor rechaza se reintenta hasta el plazo, como uno sin red: el registro del Lote 4 da `Network` en los dos casos.
  - La validación de la configuración (el backoff al abrir, el plazo al escribir) no estaba en el plan: sin ella, un backoff en cero deja girando sin pausa a una escritura que espera su secuencia, y un plazo enorme hace entrar en pánico a `write`.
  - El head visto de `HeartbeatAck` llega con el Lote 8, que trae los heartbeats.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura):
  1. **No en verde: 5 ajustes y varios menores.** Corrió los tests de escritura, de sync y de esquema 125 veces cada uno (20 seguidas, 15 rondas de 3 procesos en paralelo y 10 de 6), sin fallas. Todo quedó atendido:
     - **Se mandaba un intento después del plazo.** Lo reprodujo con un sync que retiene el aplicador 40 s: la escritura mandaba su commit a los 39,95 s. Ahora la espera del aplicador se corta en el plazo, y el plazo se mira otra vez antes de registrarse o de mandar. Hay test.
     - **El error del registro de la escritura pasaba tal cual.** Un registro que hacía fallar cerrada la sala terminaba la escritura con `FailedClosed`, y sin `OutcomeUnknown` después de un intento incierto. Ahora, si la sala terminó la escritura, es `Network`, u `OutcomeUnknown` después de un intento incierto. Hay tests, también del otro error del registro (el del storage).
     - **Un head reabierto que no se puede leer se descartaba.** Una escritura rechazada terminaba con `Ok` (con fallas inyectadas). Ahora todo queda sin reporte. Hay test.
     - **Siete reglas sin test:**
       - el head visto de la última del catch-up, y no tomarlo de un lote antes de validarlo;
       - el backoff después del `CommitAck`, su tope y su *jitter*;
       - el lock de los registros;
       - el error del registro de la escritura.

       Ahora hay tests.
     - **Faltaba una interacción:** el commit demorado de antes de un descarte, que se secuencia en la sala nueva. Queda como límite aceptado en INV-K2.
     - **Menores:**
       - el head visto de `Registered` va antes del cambio (argumentado, sin test: la carrera es estrecha y solo debilita la detección de D78);
       - la configuración se valida;
       - el registro rechazado que se reintenta queda como desvío;
       - el head visto de `HeartbeatAck`, nombrado en el Lote 8;
       - el anidado de §11;
       - `status` más corto.

     Queda sin test, por conservador, empezar el rango de una reapertura en el head anterior: una decisión con su reporte pasaría a incierta.
  2. **En verde, sin bloqueantes ni ajustes.** Los arreglos de la ronda 1 están bien y tienen test, y las mutaciones de cada uno fallan (dos por timeout). Corrió los tests de escritura, de sync y de esquema 98 veces cada uno (20 seguidas, 10 rondas de 3 procesos en paralelo y 8 de 6), sin fallas. Cuatro menores, atendidos:
     - el head visto de `Registered` antes del cambio queda argumentado, sin test (su mutación sobrevive);
     - §11 dice que un head reabierto que no se puede leer deja sin reporte todas las secuencias posteriores, y nombra los `Config` del backoff y del plazo;
     - lo que garantiza el plazo es que no sale ningún intento después (un registro en curso se corta), no que no salga nada;
     - con un head reabierto que no se lee, mientras una escritura vieja sin `CommitAck` retiene el rango hasta `u64::MAX`, una posterior que se aplicó con su reporte termina `OutcomeUnknown` en lugar de `Ok`. Nunca da un `Ok` falso, solo pasa con fallas inyectadas y dura a lo sumo el plazo de la vieja: queda así.

### Lote 8 · `heartbeat` y la bajada de snapshots (`zemdb-client`)
**Objetivo:** que un cliente atrasado termine al día (DC22, INV-K7).

**Qué se hace:** `heartbeat` y su reacción (la señal de esquema, el head menor, el head visto de DC21, la bajada si la espera, y el `snapshot_wanted` que confirma, que sube el Lote 9); un registro que deja al cliente detrás del log (`cursor < tail_seq − 1`) también lo deja esperando un snapshot (hasta este lote, lo detecta el sync siguiente, con `BehindCompaction`); la bajada por chunks, con su validación, el tope, `SnapshotSuperseded`, `RoomNotFound` en el chunk 0, el BLAKE3 y `apply_snapshot` en una tarea propia del aplicador, con DC20 ante sus errores; el commit que recibe `BehindCompaction` se pone al día y reintenta dentro del plazo; una sala que falla cerrada no manda heartbeats.

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

**Avance:** Lote 0 cerrado el 27 de Octubre de 2026, con la revisión cruzada en verde en su cuarta ronda. DC18 adoptada el 28 de Octubre, con métodos en lugar de un `execute(Command)` y los pasos `sync` y `heartbeat` como API de bajo nivel que la fachada de 4.4 esconde. DC19 a DC23 adoptadas el mismo día. Lote 1 cerrado el 28 de Octubre (`delete_room`), con la revisión de delta en verde. Lote 2 cerrado el mismo día (la encarnación en los chunks del relay), con la revisión de delta en verde. Lote 3 cerrado el mismo día (el esqueleto del cliente: transporte, metadatos y `Client::open`), con la revisión de delta en verde. Lote 4 cerrado el mismo día (apertura, registro, descarte y cierre de las salas), con la revisión de delta en verde. Lote 5 cerrado el 29 de Octubre (el aplicador y `sync`), con la revisión de delta en verde. Lote 6 cerrado el mismo día (el esquema), con la revisión de delta en verde. Lote 7 cerrado el mismo día (las escrituras), con la revisión de delta en verde en su segunda ronda. El próximo es el Lote 8.
