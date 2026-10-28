# SDK del cliente (`zemdb-client`) — Plan de la Fase 4

**Fecha:** 21 de Octubre de 2026
**Tipo:** Feature. Documento vivo: plan de la fase (plan maestro) y seguimiento. Cada sub-fase tiene su propio plan por lotes, que se escribe justo antes de arrancarla.
**Estado:** En curso: sub-fases 4.0 y 4.1 cerradas (4.1 con su [plan por lotes](2026-10-fase4_1-storage-servidor.md)); 4.2 en curso, con su [plan por lotes](2026-10-fase4_2-nucleo-replica.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md).
**Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Catálogo de lo que hay que construir:** el Apéndice A. **Índice de fases y backlog:** [`ROADMAP.md`](../../ROADMAP.md).
**Lugar en el roadmap:** después del cierre de la [Fase 3.11](2026-10-fase3_11-restricciones-proposal.md) y antes de la Fase 5 (pruebas E2E).

---

## 0. Resumen

- **Qué.** El SDK del cliente: una réplica local completa de cada sala, con escrituras *write-through* (el servidor secuencia y la réplica decide, §A.4), lectura local y sin red, reactividad (`watch`), onboarding desde snapshots y subida de snapshots cuando el servidor lo designa. Corre nativo, con una API de Rust, y en el navegador, con una API de JavaScript con tipos de TypeScript sobre el mismo SDK compilado a wasm (DC14). Persiste en los dos.
- **Qué no.** Escrituras offline (outbox o borradores), transacciones de varias filas, el SDK de administración, el dueño seudónimo, el esquema anclado en el log y los bindings para apps móviles nativas: todo eso queda para después de la v0.1 (§2.2).
- **Cómo se organiza.** Ocho sub-fases (§4): el contrato primero (4.0), lo que el storage y el servidor necesitan (4.1), el núcleo de la réplica sin red real (4.2), el transporte nativo (4.3), la API de Rust (4.4), el navegador del lado de Rust (4.5), la API de JavaScript (4.6) y el cierre (4.7).
- **Cómo se audita.** Revisión de delta en cada lote, revisión de cierre en cada sub-fase y una auditoría amplia por invariantes del SDK al cerrar el núcleo (4.2), no al final de la fase (DC3, §5).
- **Cómo se acepta.** Una app de referencia, el ranking por votos (§6), que se define en 4.0, se usa como escenario de la simulación desde 4.2, es el criterio de aceptación de la API de Rust en 4.4 y de la de JavaScript en 4.6, y es la base de las pruebas E2E de la Fase 5.

---

## 1. Punto de partida

- **Lo que existe.** `zemdb-core` (tipos, esquema con restricciones, protocolo), `zemdb-storage` (motores en memoria y en disco, `ApplyReport`, verificación de restricciones) y `zemdb-server` están cerrados y dentro del alcance auditado. `zemdb-client` es un stub.
- **Los requisitos heredados:**
  - **D93 (Fase 3.10):** cerrar las salas del storage antes de apagar el runtime de Tokio.
  - **§7 de la Fase 3.11:** el resultado de cada escritura sale del `ApplyReport` de su secuencia; el SDK completa el dueño de fila; pre-chequeo local; `watch` no emite por operaciones rechazadas; y cuatro requisitos de diseño abiertos (el reporte perdido ante un crash, las restricciones entre dueños distintos, un snapshot local que no cumple las restricciones y el esquema con que el SDK abre y aplica).
- **Los pendientes que se deciden o se cierran acá:** DEF-34 (storage no recarga el esquema de una sala abierta), DEF-54 (Zstd en wasm32), DEF-85 (resultado incierto de un commit fuera de la ventana de dedup) y D96 (detectar la pérdida del `.snap`), todos en `INVARIANTES.md` §4.

---

## 2. Alcance

### 2.1. Entra
Todo lo que lista el Apéndice A: fachada pública, CRUD y escaneos, pipeline de escritura con resultado, `SyncWorker`, onboarding y subida de snapshots, metadatos durables, `watch`, E2EE, transporte nativo y del navegador, persistencia en el navegador, la API de JavaScript, y las invariantes y la auditoría del SDK.

### 2.2. No entra
- **Escrituras offline** (outbox o borradores, `ROADMAP.md` §3, punto 8). Sin red, el cliente solo lee.
- **Transacciones de varias filas:** el protocolo commitea una operación por mensaje.
- **El SDK de administración, el dueño seudónimo y el esquema anclado en el log** (`ROADMAP.md` §3).
- **La detección de divergencia entre réplicas** (digest de estado, backlog de la Fase 3.11).
- **Bindings para apps móviles nativas** (React Native, Swift, Kotlin): no corren el wasm con OPFS y necesitarían otra capa.
- **Tipos de TypeScript generados a partir del esquema.** En la v0.1 la aplicación los declara a mano.

---

## 3. Decisiones

Prefijo **DC**. Las *Propuestas* se confirman con el dueño del proyecto antes del código que depende de ellas, la mayoría en 4.0.

| ID | Decisión | Sub-fase | Estado |
|---|---|---|---|
| **DC1** | **Write-through sin outbox en la v0.1.** El storage local guarda solo operaciones secuenciadas por el servidor; sin red el cliente solo lee. | — | Adoptada (Apéndice A.4) |
| **DC2** | **Dos niveles de plan.** Este plan maestro y un plan por sub-fase, escrito justo antes de arrancarla, con la metodología común de `docs/METODOLOGIA.md`. | — | Adoptada (21 de Octubre de 2026) |
| **DC3** | **Auditoría amplia al cerrar el núcleo.** El SDK entra al alcance auditado con una auditoría amplia por invariantes al cerrar 4.2 (`INVARIANTES.md` §1.4), porque el transporte, las APIs y el navegador se construyen encima del núcleo. Las invariantes que 4.2 todavía no implementa quedan 🆕 y las audita la revisión de cierre que las toque. 4.7 es una revisión de cierre del delta completo de la fase, no otra auditoría amplia. | — | Adoptada (21 de Octubre de 2026) |
| **DC4** | **App de referencia: el ranking por votos** (§6), el formato de la app de rankings entre amigos en la que se originó el proyecto. El esquema y los escenarios se fijan en 4.0. Es un esquema de este repo, no el de la app real, que vive aparte y puede apartarse de él. | 4.0 | Adoptada (21 de Octubre de 2026); el esquema y los escenarios, 22 de Octubre de 2026 |
| **DC5** | **Cambio de esquema en una sala abierta (DEF-34).** `reload_schema` en el storage, en los dos motores, sin cerrar la sala: verifica que el esquema nuevo evolucione del actual con una función nueva de `zemdb-core` (`Schema::evolves_from`: `Schema::extends`; cada restricción del actual sigue en el nuevo, igual y en la misma posición; los dueños y las tablas inmutables no cambian; y toda restricción nueva incluye alguna columna nueva de su tabla, la regla DR3. Las FK que entran a una tabla pueden crecer: la columna nueva vale `NULL` en las filas existentes, así que no cambia ninguna decisión ya tomada) y, si no, falla sin cambiar nada. Construye los índices de las restricciones nuevas desde las filas y verifica las filas contra ellas, como al abrir la sala: como cada restricción nueva incluye una columna nueva, que en las filas existentes vale `NULL`, nacen vacíos, pero así INV-C3 vale por construcción. (Precisión del Lote 5 del [plan de 4.1](2026-10-fase4_1-storage-servidor.md), por su revisión cruzada: en memoria, donde las filas nunca son más anchas que el esquema, nacen vacíos y no se construyen; en disco, una sala abierta con un esquema bajo el que no vale toda operación de su log (uno más angosto que el que las escribió) puede conservar valores en esa columna, así que se construyen y se verifican, y la equivalencia con abrir con el esquema nuevo vale solo si la sala nunca se abrió así; INV-S7.) Corre bajo el mismo lock que `apply_batch`, así que ningún lote se aplica con dos esquemas. El SDK guarda el esquema vigente y su `schema_id` entre sus metadatos durables (§A.9), para abrir la sala sin red, y ante un cambio lo guarda primero, después llama a `reload_schema` y recién después aplica las operaciones de esa versión. El SDK aplica la misma regla a una versión nueva del mismo esquema y a una reasignación a otro: el esquema nuevo tiene que evolucionar del suyo (`evolves_from`). La regla es transitiva, así que la cumple cualquier cadena de evoluciones y reasignaciones válidas aunque el cliente no haya visto los pasos intermedios (comparar contra el esquema local con `has_same_constraints`, como hace el servidor contra el suyo, dejaría trabado a un cliente que se perdió una evolución). Para enterarse del cambio, `CommitAck`, `SyncBatch` y `HeartbeatAck` llevan, además de `schema_version`, el `schema_id` (en 4.1): las versiones son por esquema y dos esquemas pueden tener la misma. Además, el SDK pide el esquema ante una operación que no valida contra el local al aplicar un lote, y ante cualquier error de `apply_snapshot` sobre el snapshot de otro miembro (el storage no distingue una tabla o columna desconocida de un daño): pide el esquema una vez y, si cambió, se pasa a él y vuelve a intentar con el mismo snapshot antes de descartarlo. Una respuesta con el mismo `schema_id` y una versión igual o menor que la local no es un cambio: sus operaciones se aplican con el esquema local (INV-C1). Ante un `schema_id` o una versión que no conoce, el SDK decide contra el esquema que le devuelve `GetSchema`, no contra la respuesta. `GetSchema` no está atado a la encarnación: antes de fallar cerrado por un esquema que no evoluciona del suyo, el SDK se vuelve a registrar con su cursor, y si recibe `RoomReset` descarta la réplica. Fallar cerrado frena las escrituras y la aplicación de lotes, no los heartbeats ni el registro. Alternativa descartada: cerrar y reabrir la sala, que corta lecturas y `watch` y reconstruye todo (segundos para una sala de un millón de filas en el navegador, S2). **Ajuste de DC20** (plan de 4.2, adoptada el 28 de Octubre de 2026): el SDK aprende el esquema solo de `Registered`, nunca de `GetSchema` (ante cualquier señal de cambio se vuelve a registrar con su cursor); el pedido de esquema ante un error de `apply_snapshot` se limita a `SnapshotCorruption` y `SnapshotConstraints`; y fallar cerrado también frena los heartbeats: el cliente se da de baja. | 4.1 / 4.2 | Adoptada (22 de Octubre de 2026); la ajusta DC20 |
| **DC6** | **Resultado incierto de un commit (DEF-85).** En la v0.1, el SDK reintenta con el mismo `MutationId` durante un plazo acotado y, si no confirma, devuelve un error de resultado incierto; nunca reenvía con un id nuevo, y el estado final llega por sync. Que las respuestas de sync lleven la clave de dedup de cada operación, para que el cliente reconozca su propio commit, queda para la outbox. Precisiones: (1) un intento que queda incierto (sin respuesta, falla de red, `Unavailable`, `Timeout`, `Internal`) vuelve incierta a la escritura: si un reintento recibe después un rechazo, eso no prueba que el primero no se secuenció, así que la escritura termina con el error de resultado incierto. Un rechazo es la respuesta solo si ningún intento anterior quedó incierto (no deja rastro, INV-Y5). `ClientNotRegistered`, `InvalidSequence` y `BehindCompaction` no son rechazos: el SDK se vuelve a registrar o se pone al día y sigue reintentando con el mismo `MutationId` dentro del plazo, lo que es seguro porque el servidor busca el `MutationId` en el dedup antes de mirar la retención. Si el plazo vence, la escritura termina como incierta si algún intento quedó incierto, y si no, con un error que dice que no tuvo efecto. La reacción a cada respuesta está en §A.4 (*Reacción del commit a cada respuesta*). (2) El plazo se mide con el reloj monotónico (DC11), con backoff, del orden de 30 s: la ventana de dedup se mide en mutaciones de la sala (10.000 por defecto), y fuera de ella un reintento se aplica de nuevo y puede pisar cambios posteriores. Ese riesgo residual se anota en `INVARIANTES.md` §4, con DEF-85. Un miembro hostil puede provocarlo llenando la ventana mientras se pierde una respuesta, y entonces la decisión que informa el SDK es la de la secuencia nueva, que puede ser un rechazo de una escritura que ya se aplicó en la primera. | 4.2 | Adoptada (22 de Octubre de 2026) |
| **DC7** | **Un único aplicador por sala.** Todo lote (catch-up de un commit, sync, snapshot) entra al storage por un solo camino, que descarta el prefijo ya aplicado, no aplica un lote con hueco (pide un sync), entrega la decisión de cada secuencia a la escritura que la espera (venga el lote por donde venga) y avisa a `watch`. Las respuestas que traen operaciones o snapshots (`CommitAck`, `SyncBatch`, `SnapshotChunk`) no traen la encarnación, así que la réplica tiene una época, que cambia cada vez que se descarta: cada pedido lleva la época con que se mandó, y el aplicador ignora las respuestas de una época anterior (un `SyncBatch` demorado de la sala vieja sería contiguo con la réplica vacía). Como un sync puede aplicar una secuencia antes de que llegue el `CommitAck` que la asigna, conserva los rechazos de toda secuencia mayor que el head local que había cuando empezó la escritura en vuelo más vieja: la secuencia asignada siempre lo supera. Lo retenido es lo que la réplica aplica mientras hay una escritura en vuelo, que para una réplica atrasada puede ser todo su atraso: acotarlo es un requisito de diseño de §4.2. Los rechazos no se guardan en el storage: sin outbox, ninguna escritura espera su resultado después de un crash. Si el reporte de un lote se pierde sin crash (la sala queda fallida después de que el lote es durable, y al reabrirla el head ya lo pasó), las escrituras que esperaban esas secuencias terminan con el error de resultado incierto (DC6), nunca con `Ok`. Guardar los rechazos va con la outbox (`ROADMAP.md` §3). | 4.2 | Adoptada (22 de Octubre de 2026) |
| **DC8** | **Persistencia en el navegador.** Un motor sobre el *Origin Private File System* con `FileSystemSyncAccessHandle`, dentro de un Web Worker dedicado, reusando el formato de archivos del motor en disco detrás de una capa de archivos. Alternativa: IndexedDB. Sujeta al spike S1. **S1 (22 de Octubre de 2026),** en Chrome 155 (headless y con ventana) y Safari 18.6, sobre macOS 15.8.1, con Vite 8 y `wasm-bindgen` 0.2.129 (las preguntas, en §4.0): (1) sí, en los dos: desde Rust (`web-sys`), en un Worker que Vite empaqueta sin plugins, `write` y `read` en un offset, `flush`, `truncate`, `getSize` y `close` funcionan, y lo escrito se ve al reabrir y después de recargar la página; un snapshot de `MemoryStorageEngine` de 100.000 filas (6,1 MB) se escribe en unos 100 ms, se lee en menos de 10 ms y se vuelve a aplicar. (2) Lo escrito con `flush` y sin `close` se lee después de terminar el Worker, en los dos; en Chrome también después de matar el navegador (`SIGKILL`) y reabrirlo con el mismo perfil. En Safari no se probó el corte del navegador: matarlo cierra las ventanas del usuario. (3) Un segundo handle sobre el mismo archivo falla enseguida (en menos de 20 ms), con `NoModificationAllowedError` en Chrome e `InvalidStateError` en Safari, y se puede abrir apenas la pestaña que lo tenía se cierra o su Worker termina. En Safari, si la página navega a otra sin cerrar el handle, el archivo sigue tomado mientras la página vive en el historial (más de 20 s; se liberó después de dos navegaciones más, probablemente por el *back/forward cache*); recargar lo libera enseguida, y terminar el Worker en `pagehide` también. Chrome tiene el modo `readwrite-unsafe` (dos handles a la vez); Safari no. `navigator.locks` existe en el Worker en los dos. (4) `persist()` existe solo en la página, no en el Worker, y sin interacción del usuario devuelve `false` en los dos (Chrome con un perfil nuevo). `estimate()` da una cuota de 10 GiB en Chrome y 76,8 GiB en Safari, en este disco. (5) El wasm con `zemdb-core`, `zemdb-storage` (motor en memoria) y la capa de archivos pesa 493,5 KB: 151 KB con gzip y 120 KB con brotli, más unos 115 KB de `ruzstd` (DC9). **Conclusión:** el criterio (1) se cumple en Chrome y Safari. Para 4.5: un handle por archivo y por pestaña, así que la coordinación entre pestañas va con `navigator.locks` y no reintentando el handle; el Worker cierra sus handles o termina en `pagehide`; la API de JavaScript llama a `persist()` desde la página; y como `persist()` suele dar `false`, el navegador puede borrar la réplica, así que la detección de la pérdida total (DC11) hace falta (con DC19, no hace falta detectarla: el cliente presenta el head con que abre la sala y se pone al día desde ahí). Que `flush()` sobreviva a un corte de energía sigue siendo un supuesto (DC11). **Pendiente:** S1 en Firefox, que no estaba instalado; se corre antes de cerrar 4.5, y si ahí no se cumple el criterio (1), DC8 se revisa. | 4.0 / 4.1 / 4.5 | Adoptada (22 de Octubre de 2026), con la condición de S1 en Firefox antes de cerrar 4.5 |
| **DC9** | **Zstd en wasm32 (DEF-54).** `ruzstd`, en Rust puro, en wasm32: para decodificar los snapshots hechos en nativo, y para codificar con el nivel `Fastest` el `.snap` local del motor del navegador (DC8) y los snapshots que sube el navegador. En nativo sigue `zstd`. Sujeta al spike S2. **S2 (22 de Octubre de 2026):** `ruzstd` 0.8 compila para `wasm32-unknown-unknown` y decodifica por streaming (bloques de 64 KiB, sin retener el resultado) un snapshot `ZMSN` hecho por `DiskStorageEngine`, con el mismo BLAKE3 que `zstd` nativo. Con 1.000.000 de filas (16,6 MB comprimido, 104 MB sin comprimir), decodificar tarda 0,7 s en Chrome 155 y en Node 24, y 2,5 s en Safari 18.6; restaurarlo entero (decodificar y `apply_snapshot` en `MemoryStorageEngine`), unos 2,7 s en Chrome y 4,4 s en Safari. En nativo, `ruzstd` es unas 3 veces más lento que `zstd` (1,1 s contra 0,34 s), así que se usa solo en wasm32. Suma al wasm unos 115 KB (34 KB con gzip). Medido en un Intel i5-5675R con macOS 15.8.1. **Codificador (23 de Octubre de 2026):** `ruzstd` 0.8.3 implementa los niveles `Uncompressed` y `Fastest` (parecido a `zstd` nivel 1). En wasm32, en Node 24, comprime el payload de 104 MB en 2,5 s, a 33,8 MB (el `zstd` nativo de `DiskStorageEngine` da 16,6 MB), y el `zstd` nativo lo decodifica a los mismos bytes. Cada medición es de una sola corrida. **`ruzstd` 0.9 (24 de Octubre de 2026, Lote 6 de 4.1):** la 0.8 no acota lo que producen las secuencias de un bloque (RFC 8878 limita el bloque al menor entre la ventana y 128 KiB) ni, en el primer frame, la ventana, así que unos pocos KB de un snapshot hostil le harían retener gigabytes. La 0.9.1 acota la ventana y las secuencias (los literales después de la última secuencia, a 1 MiB por bloque, que libzstd rechaza), y es la que se usa (la misma API de streaming y los mismos niveles; Rust 1.87). **Codificador en Chromium:** `encode_snapshot_envelope` con compresión, en wasm32 (`ruzstd` 0.9.1, `Fastest`), en Chromium 153 headless (Playwright) sobre Linux, en un Xeon de 2,1 GHz con 4 núcleos, sin `wasm-bindgen` (un `cdylib` con `opt-level = 3`, LTO y `strip`). El payload es el snapshot de `DiskStorageEngine` de una tabla de 756.000 filas (un `Int` de clave, dos `String`, un `Float` y un `Int`): 103,9 MB, que el `zstd` nativo de nivel 3 comprime a 8,8 MB. En seis corridas (dos cargas de la página, tres por carga), comprimirlo tarda de 0,99 a 1,47 s, a 23,4 MB, y el `zstd` nativo lo decodifica a los mismos bytes. En el mismo navegador, `ruzstd` decodifica el envelope nativo en 0,35 s y el suyo en 0,34 a 0,49 s. El codificador suma al wasm 73,0 KB (25,5 KB con gzip y 20,2 KB con brotli de nivel 11): con `zemdb-core`, `zemdb-storage` y el decodificador, de 142,4 KB a 215,4 KB. El programa (el `cdylib`, la página que mide y el generador nativo del payload) no se mergea, como el código de un spike. **Pendiente para 4.5:** la misma medición en Safari, que corre el dueño del proyecto con el mismo programa, antes de cerrar 4.5 (pasó de 4.1 a 4.5 en el cierre de 4.1). | 4.0 / 4.1 | Adoptada (22 de Octubre de 2026; el codificador, 23 de Octubre) |
| **DC10** | **Invariantes del SDK** en un grupo nuevo de `INVARIANTES.md` (§3.11, *Cliente*), con prefijo `INV-K` (`C` ya es de las restricciones). Un solo grupo; cuántos auditores tiene la auditoría de 4.2 lo decide su plan. | 4.0 | Adoptada (22 de Octubre de 2026) |
| **DC11** | **Modelo de amenaza del cliente.** Se confía en la aplicación que usa el SDK, en el disco local (los supuestos de `INVARIANTES.md` §2) y en el servidor como secuenciador. No se confía en los demás miembros (sus operaciones y sus snapshots), en la red (respuestas perdidas, cortadas o demoradas, eventos SSE perdidos) ni en el reloj. Un snapshot falso pero bien formado sigue fuera de alcance (DEF-81). Precisiones: (1) el servidor es de confianza como secuenciador, no para la confidencialidad: las columnas cifradas se protegen del servidor y del proxy, y las claves las maneja la aplicación, fuera de alcance; un servidor malicioso (que reordena, fabrica operaciones o muestra historias distintas a distintos clientes) también queda fuera. (2) Una respuesta puede traer cualquier byte y nunca hace entrar en pánico al SDK; una bien formada que viola el protocolo (un lote con hueco, un head menor que el cursor que mandó el pedido) falla cerrada y no se aplica. Una respuesta demorada, con un head menor que el local o de una época anterior de la réplica (DC7), no viola nada: se ignora. (3) En el navegador, la aplicación es todo el origen, otras pestañas y scripts incluidos; la concurrencia entre pestañas está en el alcance. El supuesto del disco se extiende al OPFS (`flush()` vale como `fsync`). Que el navegador borre el almacenamiento del origen es una pérdida total, no un daño: el cliente la detecta y vuelve a bajar todo, como en D96. (4) Los plazos del SDK (backoff, heartbeat, reintentos de DC6) usan un reloj monotónico; el vencimiento del token lo decide el servidor. **Precisiones de DC19 y DC21** (plan de 4.2): en (2), un lote con hueco no se aplica y se vuelve a sincronizar, y un head menor que el del pedido lleva a registrarse con el cursor (D78); fallar cerrado queda para el esquema (DC20). En (3), no hace falta detectar la pérdida: el cursor es el head con que abre el storage (DC19). | 4.0 | Adoptada (22 de Octubre de 2026) |
| **DC12** | **Un snapshot que no cumple las restricciones** (§7 de la Fase 3.11). Una variante propia de `StorageError` (por ejemplo `SnapshotConstraints { table_id, constraint }`), distinta de `SnapshotCorruption`, en `open_room` y en `apply_snapshot`, que verifican con la misma función. El SDK reacciona según dónde aparece: al abrir, falla cerrado y conserva la réplica (con las reglas de DC5 no debería pasar); al aplicar el snapshot de un miembro, lo descarta, y la réplica no cambió. La regla general depende del origen del error. Al aplicar el snapshot de otro miembro, ningún error descarta la réplica: se descarta ese snapshot, también ante `SnapshotCorruption`, que sale además por una tabla o una fila inválidas. Fuera de eso, el SDK descarta la réplica solo ante `RoomReset`, ante `InvalidSequence` en el registro (el servidor perdió secuencias, D78; su log manda), cuando la réplica se perdió (INV-K5) o ante un daño en sus archivos locales al abrir la sala (`SnapshotCorruption`, `WalCorruption`); nunca ante `RoomLocked`, `Io`, `SchemaMismatch` ni esta variante, salvo que la aplicación lo pida explícitamente (§4.4), para salir de un caso que falla cerrado. Hoy un error de lectura de E/S del `.snap` al abrir sale como `SnapshotCorruption` o `WalCorruption` (los del WAL ya salen como `Io`): en 4.1 pasa a salir como `Io`. **Ajuste de DC19** (plan de 4.2, adoptada el 28 de Octubre de 2026): el cursor es el head del storage y no se guarda, así que una réplica perdida no se descarta sino que se pone al día; en su lugar, el SDK descarta los datos de una sala cuyos metadatos faltan. | 4.1 / 4.2 / 4.4 | Adoptada (22 de Octubre de 2026); la ajusta DC19 |
| **DC13** | **Restricciones entre dueños distintos** (§7 de la Fase 3.11). Documentarlo para quien escribe el esquema, sin regla nueva en la v0.1, en `ARCHITECTURE.md` §3.1: (1) una FK hacia una tabla con dueño incluye la columna dueño cuando las dos filas son del mismo dueño (`rounds` y `votes` hacia `members`, §6); (2) una FK entre dueños distintos le impide al dueño de la fila referenciada borrarla, y sirve cuando es lo buscado (`members` hacia `rankings`, el `candidate` de `votes`); (3) un `UNIQUE` es del primero que llega en el orden del log, y la aplicación recibe `ConstraintViolation`. | 4.0 | Adoptada (22 de Octubre de 2026) |
| **DC14** | **API de JavaScript sobre un Worker.** En el navegador, el SDK compilado a wasm corre en un Web Worker dedicado (donde el OPFS da archivos síncronos) y la página lo usa a través de un paquete con tipos de TypeScript que le habla por mensajes. El paquete vive en este repo y se versiona con el resto. En 4.0 se fijan sus principios: el núcleo (4.2) y la API de Rust (4.4) se expresan como comandos y respuestas serializables, y las suscripciones (`watch`) por id, sin referencias que crucen el límite; las filas son objetos con nombres de columna; `Bytes` es `Uint8Array` y `Uuid` un string; los errores llevan un código que mapea `ClientError`; `watch` se puede usar desde React (`useSyncExternalStore`). Representación de cada tipo: `Int(i64)` es `number` o `bigint`, según una opción del cliente (`int64`, por defecto `number`); en modo `number`, escribir exige un entero seguro (`Number.isSafeInteger`, o un `bigint` dentro de i64) y leer un valor fuera de ±(2⁵³−1) falla con un código de error propio, que nombra la tabla y la columna, nunca con un valor redondeado. `Timestamp(i64)` son milisegundos desde la época Unix en UTC (también en Rust, `ARCHITECTURE.md` §3.1) y en JavaScript es `Date`; fuera del rango de `Date` falla igual que `Int`. `Float` es `number`: un `-0` escrito desde JavaScript se guarda como `0`, porque `zemdb-core` lo canonicaliza al convertir desde un `f64`, y un `-0` que llega por la red se lee como `-0`; `NaN` llega como `NaN`, sin garantizar su payload, así que al escribir un `NaN` en una columna de la clave se rechaza: el orden de `Value` distingue los `NaN` por su payload, y la fila podría cambiar de clave. `String` es `string`, y un string con surrogates sueltos se rechaza al escribir en lugar de reemplazarlos. `Bytes` es `Uint8Array`; `Uuid`, un string en minúsculas y con guiones (al escribir se acepta en cualquier caso); `Bool`, `boolean`; `Null`, `null`, nunca `undefined`. Una fila trae todas las columnas del esquema (las que una fila corta no tiene se leen como `null`); al escribir, una columna nullable que falta vale `null`, una obligatoria que falta o una clave que no es una columna es un error. La clave primaria es un objeto con sus columnas: al escribir va sin la columna dueño, que completa el SDK, y al leer la lleva, para poder leer las filas de otros. Las columnas cifradas llegan descifradas, con su tipo real. Los errores son un `ZemdbError` con un `code` por variante de `ClientError` y sus detalles. Un `watch` expone `subscribe(callback)` y `getSnapshot()`, cuyo valor no se modifica y cambia de referencia solo cuando cambia el dato. | 4.0 / 4.2 / 4.4 / 4.5 / 4.6 | Adoptada (22 de Octubre de 2026) |
| **DC15** | **Un `ClientId` por dispositivo, con el dueño como prefijo.** El backend emite los tokens por dispositivo (y, como siempre, por sala), con un `ClientId` de la forma `persona#dispositivo`. Al renovar un token, el SDK verifica que traiga el mismo `ClientId`: el token lo lleva, y un reintento con otro `ClientId` sería otra mutación para el dedup (INV-Y4). El dueño es la parte anterior al primer `#`, o el `ClientId` entero si no tiene `#` o si alguna de las dos partes queda vacía. El id de persona lo controla el backend, que no le pone `#`; el de dispositivo viene del cliente, que no es de confianza, y puede traer cualquier carácter sin cambiar el dueño (con el último `#`, un dispositivo `y#z` de `bob` daría el dueño `bob#y`). El servidor compara el dueño declarado con esa parte (`Schema::check_owner`, en un lote de 4.1), y todo lo demás (roster, cursor, lease, dedup, designación, subidas y tope de SSE) sigue por `ClientId`: es por dispositivo y nada se comparte. Las réplicas no cambian: comparan el dueño de la fila con el de la operación. El SDK genera un id de dispositivo, lo guarda entre sus metadatos (§A.9) y lo expone para que la aplicación lo mande al pedir el token; llena la columna dueño con la parte de dueño, y genera cada `MutationId` al azar (128 bits). Reemplaza a la primera versión de esta decisión, un `ClientId` compartido por los dispositivos de una persona: su análisis (Apéndice B) mostró que compartir cursor, lease y designación deja dispositivos atrasados sin salida y subidas que se pisan. También se descartó un identificador de dispositivo propio en el protocolo: unas 400 líneas en siete mensajes, el roster y el relay, y sin firma, así que un miembro podría inventar dispositivos. Un navegador que borra su almacenamiento es un dispositivo nuevo: la entrada vieja queda `Disconnected` y frena la poda proactiva hasta pasar a `Dormant` (`INVARIANTES.md` §4). El backend limita los dispositivos por persona: los topes del servidor por cliente (dos streams SSE por sala, la parte del presupuesto de respuestas) son por dispositivo, y una persona con muchos dispositivos los suma (INV-B5). | 4.0 / 4.1 / 4.2 / 4.4 | Adoptada (22 de Octubre de 2026; reemplaza a la primera versión, del mismo día) |
| **DC16** | **Los plazos del servidor con el reloj de Tokio.** Los plazos del servidor (lease, demanda y designación de snapshots, retención de la HotBuffer) usan `tokio::time::Instant`, como el actor y el relay, en lugar de `std::time::Instant`, para que el arnés de simulación los controle con el reloj pausado (§5.3). En 4.1, en un lote con su revisión de delta (el servidor está en el alcance auditado). Los dos arreglos de vivacidad que proponía la primera versión de esta decisión (hallazgos 1 y 2 del Apéndice B) dejan de hacer falta con DC15. | 4.1 | Adoptada (22 de Octubre de 2026) |
| **DC17** | **La capa de archivos del motor en disco** (DC8). Un trait asíncrono de `zemdb-storage` que es lo único del motor en disco que toca el sistema de archivos: operaciones de directorio (abrir con lock exclusivo creándolo si falta, crear con lock un archivo que no existe, crear truncando, abrir para leer, saber si existe, listar por prefijo, renombrar reemplazando, borrar, crear el directorio y sincronizarlo) y E/S posicional sobre un archivo abierto (`read_at`, `write_at`, largo, `set_len`, `sync_data`, `sync_all`). Asíncrono porque en el OPFS abrir, renombrar, borrar y listar son promesas. Renombrar por ruta y borrar se hacen sin handles abiertos en el origen ni en el destino, como exige el OPFS (spike S3); el único archivo que el motor renombra abierto, el WAL en la rotación y en su vuelta atrás, se renombra con una operación propia del handle, que en nativo es el `rename` de hoy y en el OPFS cierra, mueve y reabre. Una operación soltada retiene el archivo y su lock hasta terminar, como hoy el handle del WAL, así que nunca aterriza después de reabrir la sala (INV-S1, INV-L7). `DiskStorageEngine` pasa a ser genérico sobre el trait, con la implementación nativa por defecto; no cambia ningún comportamiento del motor: el formato, la recuperación, la compactación, la instalación de snapshots y los locks siguen igual, y el mecanismo del lock del handle del WAL (y el error diferido de una escritura que nadie esperó) se reimplementa con el mismo efecto. El OPFS, las tareas del motor en wasm32 y la compresión del `.snap` con `ruzstd` quedan para 4.5. El detalle, S3, los requisitos de la implementación del OPFS y las alternativas, en §1 del [plan de 4.1](2026-10-fase4_1-storage-servidor.md). **Pendiente para 4.5:** S3 en Safari y Firefox, antes de cerrar 4.5 (pasó de 4.1 a 4.5 en el cierre de 4.1). | 4.1 / 4.5 | Adoptada (23 de Octubre de 2026) |
| **DC18** | **La forma del núcleo.** `Client` sobre un `StorageEngine`, un almacén de metadatos y un `TransportClient`, con métodos asíncronos (`open_room`, `write`, `sync`, `heartbeat`, `status`, `close_room`, `shutdown`) cuyos argumentos y resultados son datos serializables (DC14); el enum de comandos aparece recién en el runtime del Worker (4.5). `sync` y `heartbeat` son los pasos que programa el worker de 4.3: la aplicación no los llama, y la fachada de 4.4 no los expone. El transporte manda un `ClientMessage` y devuelve los bytes de la respuesta; el núcleo decodifica y valida. El detalle, en §1 del [plan de 4.2](2026-10-fase4_2-nucleo-replica.md). | 4.2 | Adoptada (28 de Octubre de 2026) |
| **DC19** | **Metadatos durables y descarte.** Por sala, la encarnación y el esquema, en archivos con CRC en el directorio del motor, a través de la capa de archivos (o en memoria), escritos antes que los datos que describen; el cursor no se guarda: es el head del storage. Sin metadatos, el SDK borra los datos de la sala; descartar es borrar los metadatos y después los datos, con `StorageEngine::delete_room`, nuevo. Ajusta DC12. Detalle en el plan de 4.2. | 4.2 | Adoptada (28 de Octubre de 2026) |
| **DC20** | **El esquema se aprende de `Registered`.** Ajusta DC5: ante cualquier señal de cambio, el SDK se vuelve a registrar con su cursor y decide contra el esquema de `Registered`, nunca contra `GetSchema`; fallar cerrado da de baja al cliente y frena sus heartbeats. Detalle en el plan de 4.2. | 4.2 | Adoptada (28 de Octubre de 2026) |
| **DC21** | **El aplicador y las escrituras.** Precisa DC6, DC7 y DC11: cada cambio de varios pasos del aplicador en una tarea propia; los rechazos y los rangos sin reporte retenidos por el head que cada escritura vio antes de su primer intento, nunca `Ok` por la ausencia de un rechazo, las resoluciones sin decisión, los reintentos y la validación de cada respuesta; un head menor que el del pedido lleva a registrarse. Detalle en el plan de 4.2. | 4.2 | Adoptada (28 de Octubre de 2026) |
| **DC22** | **Snapshots en el cliente.** La bajada por chunks de 4 MiB hasta `max_snapshot_bytes` (512 MiB por defecto), con cada chunk validado; la subida, confirmada por un heartbeat, siempre por chunks; y la memoria que cuenta INV-K15. Detalle en el plan de 4.2. | 4.2 | Adoptada (28 de Octubre de 2026) |
| **DC23** | **Los chunks del relay llevan la encarnación.** `RequestSnapshotChunk` y `UploadSnapshotChunk` llevan la `room_incarnation` a la que pertenecen, y el relay responde `RoomReset` al chunk 0 sin ancla y a los de una subida si no es la de la sala; a los anclados los cubre la purga. Así un cliente de otra encarnación no baja otro snapshot que el de su encarnación ni sube nada (INV-R6). En `zemdb-core` y `zemdb-server`. Detalle en el plan de 4.2. | 4.2 | Adoptada (28 de Octubre de 2026) |

---

## 4. Sub-fases

| Orden | Sub-fase | Qué entrega | Depende de | Cierra con | Estado |
|---|---|---|---|---|---|
| 4.0 | Contrato y decisiones | Invariantes del SDK, modelo de amenaza, decisiones, spikes, diseño del arnés de simulación, app de referencia | — | Revisión cruzada del contrato | ✅ |
| 4.1 | Storage y servidor para el cliente | Lo que el SDK necesita del storage y del servidor | 4.0 | Revisión de cierre | ✅ |
| 4.2 | Núcleo de la réplica | La réplica por sala contra un transporte abstracto | 4.1 | **Auditoría amplia** (DC3) | ⏳ |
| 4.3 | Transporte nativo y `SyncWorker` | HTTP, SSE, reconexión, heartbeat | 4.2 | Revisión de cierre | ⬜ |
| 4.4 | API de Rust | Fachada, `watch`, dueño, pre-chequeo, E2EE, errores | 4.2 | Revisión de cierre y app de referencia nativa | ⬜ |
| 4.5 | Navegador (Rust) | Motor del navegador, transporte del navegador, runtime del Worker, pestañas | 4.3, 4.4 | Revisión de cierre | ⬜ |
| 4.6 | API de JavaScript | Paquete con tipos de TypeScript sobre el Worker | 4.5 | Revisión de cierre y app de referencia en el navegador | ⬜ |
| 4.7 | Cierre | Revisión de cierre de la fase y documentos | 4.6 | Revisión de cierre del delta completo | ⬜ |

4.3 y 4.4 dependen solo de 4.2 y no tocan los mismos archivos: pueden ir en paralelo. El motor del navegador (4.5) depende de 4.1 y del spike S1, no del resto, así que se puede adelantar si 4.2 lo permite.

### 4.0 · Contrato y decisiones
Solo documentos y spikes (`docs/METODOLOGIA.md` §2 y §3).
- **Invariantes del SDK (DC10).** Escritas como INV-K1 a K15 en `INVARIANTES.md` §3.11, a partir de estas candidatas y de las decisiones (DC5, DC6, DC7, DC8, DC12, DC14):
  - ★ la réplica local es siempre un prefijo del log de la sala: nunca aplica fuera de orden ni algo que el servidor no secuenció;
  - ★ cada escritura se resuelve una sola vez, con la decisión de la réplica en su secuencia, o con un error que dice que el resultado es incierto (DC6);
  - ★ el SDK nunca descarta una réplica local sana: solo ante `RoomReset` o un daño verificado (DC12);
  - ★ las réplicas de clientes que siguen el protocolo convergen (INV-Y8 de punta a punta);
  - un cursor nunca se presenta con otra encarnación;
  - un cliente que se queda atrás termina al día, también cuando el designado para subir el snapshot es él;
  - el SDK nunca manda una operación con un dueño ajeno ni una columna dueño escrita por la aplicación;
  - una columna cifrada nunca sale en claro;
  - cerrar o apagar el cliente no pierde nada confirmado (D93);
  - ninguna respuesta del servidor ni snapshot de un par hace entrar en pánico al SDK;
  - la API de JavaScript no cambia ningún valor al cruzar el límite del Worker (DC14).
- **Modelo de amenaza (DC11)** en `INVARIANTES.md` §2, que deja de decir que el SDK está fuera de alcance.
- **Decisiones DC4 a DC16** confirmadas.
- **Varios dispositivos (DC15):** el análisis, sin código, de varios dispositivos con un mismo `ClientId` está en el Apéndice B, y llevó a un `ClientId` por dispositivo, con el dueño como prefijo.
- **Spikes** (`docs/METODOLOGIA.md` §3). El código va fuera del repo y no se mergea; la respuesta queda en la decisión, con lo medido y en qué navegador o plataforma.
  - **S1 (DC8), una sesión.** Desde una página armada con Vite, un Worker dedicado que carga Rust compilado a wasm32 con `wasm-bindgen`. En Chrome, Firefox y Safari:
    1. ¿Puede el código en Rust abrir un archivo del OPFS con `FileSystemSyncAccessHandle` y hacer `write` y `read` en un offset, `flush`, `truncate`, `getSize` y `close`, y al reabrir (también después de recargar la página) ver lo escrito?
    2. Después de `flush`, si el Worker se termina sin `close` (`terminate()`, o cerrar la pestaña), ¿se lee lo escrito al reabrir?
    3. Si una segunda pestaña pide un handle sobre el mismo archivo mientras la primera lo tiene, ¿falla enseguida, con qué error, o espera? ¿Se libera al cerrar la primera pestaña? ¿Hay modos de acceso compartido y `navigator.locks` dentro del Worker?
    4. ¿Qué devuelven `navigator.storage.persist()` y `persisted()` sin interacción del usuario, y qué cuota da `estimate()`?
    5. ¿Cuánto pesa el wasm en release (sin comprimir, gzip y brotli) con `zemdb-core`, `zemdb-storage` y la capa de archivos del spike?

    **Criterio:** DC8 elige OPFS si (1) se cumple en los tres navegadores (Firefox quedó pendiente para 4.5, ver DC8); (2) y (3) definen el supuesto de durabilidad y la coordinación entre pestañas de 4.5; si (1) falla en alguno, se evalúa IndexedDB.
  - **S2 (DC9), media sesión.**
    1. ¿Compila `ruzstd` para `wasm32-unknown-unknown` y decodifica, por streaming y sin materializar todo, un snapshot `ZMSN` con Zstd hecho por `DiskStorageEngine`, con el mismo resultado que el `zstd` nativo?
    2. ¿Cuánto tarda en wasm32 (Node y Chrome) con un snapshot grande, contra el `zstd` nativo?
    3. ¿Cuánto suma al wasm?

    **Criterio:** DC9 adopta `ruzstd` si (1) se cumple y el tiempo de (2) es aceptable para el onboarding de una sala grande.
- **Arnés de simulación:** el diseño (no el código) de las pruebas del §5.2: varios clientes contra el servidor real en proceso, con un transporte que inyecta fallas y el reloj pausado. Está en §5.3.
- **App de referencia (DC4):** esquema y escenarios (§6).

### 4.1 · Storage y servidor para el cliente
En `zemdb-core`, `zemdb-storage` y `zemdb-server`, dentro del alcance auditado. Plan por lotes: [`2026-10-fase4_1-storage-servidor.md`](2026-10-fase4_1-storage-servidor.md).
- El dueño como prefijo del `ClientId` (DC15), en `zemdb-core`, con `ARCHITECTURE.md` §3.1 e INV-A5.
- `Schema::evolves_from` en `zemdb-core` (DC5): `extends`; cada restricción del actual sigue en el nuevo, igual y en la misma posición; los dueños y las tablas inmutables no cambian; y toda restricción nueva incluye alguna columna nueva de su tabla, la regla DR3. Las FK que entran a una tabla pueden crecer: la columna nueva vale `NULL` en las filas existentes, así que no cambia ninguna decisión ya tomada.
- El `schema_id` en `CommitAck`, `SyncBatch` y `HeartbeatAck` (DC5), en `zemdb-core` y el servidor, con sus tests de codificación (INV-W2).
- Los plazos del servidor con el reloj de Tokio (DC16).
- `reload_schema` (DC5), con los índices de las restricciones nuevas.
- La variante de error de DC12, en `open_room` y `apply_snapshot`, y los enunciados de INV-S4, INV-S5, INV-C2 e INV-W1 que nombran `SnapshotCorruption` para las restricciones.
- Que un error de lectura de E/S al abrir una sala salga como `Io` y no como `SnapshotCorruption` o `WalCorruption` (DC12).
- `ruzstd` para wasm32 (DC9): el decodificador, con el mismo tope de memoria que el de `zstd`, y el codificador `Fastest`, con su entrada en el fuzzing de `apply_snapshot` (INV-W1, INV-K12). Medir el codificador en Chrome y Safari, y cuánto suma al wasm (pendiente de DC9).
- La capa de archivos que separa el formato del motor en disco de `std::fs` (DC8, DC17).

### 4.2 · Núcleo de la réplica
En `zemdb-client`, contra el trait `TransportClient`, sin red real, con la forma de comandos y respuestas de DC14. Plan por lotes: [`2026-10-fase4_2-nucleo-replica.md`](2026-10-fase4_2-nucleo-replica.md), con DC18 a DC23.
- Registro, encarnación y metadatos durables (§A.9), con el id de dispositivo (DC15) y el esquema vigente y su `schema_id` (DC5). Con DC19, un `.snap` perdido (D96) o la pérdida total de la réplica (DC11) no necesitan detección: el cursor es el head del storage.
- `MutationId` aleatorio de 128 bits (DC15).
- El aplicador único (DC7), con la época de la réplica, y el pipeline de escritura con su resultado (§A.4), con los reintentos y el resultado incierto de DC6, y el de las escrituras en vuelo al cerrar el cliente o la sala (INV-K2).
- Cuándo descarta la réplica local (DC12).
- Onboarding y bajada de snapshots (§A.6) y subida como designado (§A.8).
- Cambio de esquema con la regla de DC5 (`evolves_from`, para una versión nueva y para una reasignación), detectado por el `schema_id` y la versión de cada respuesta, por una operación que no valida y por un `SnapshotCorruption` o un `SnapshotConstraints` al aplicar el snapshot de otro miembro (DC20); si no la cumple, falla cerrado. Una respuesta con una versión anterior del mismo esquema no es un cambio.
- Cierre ordenado de las salas antes de apagar el runtime (D93).
- Se prueba contra el servidor real en proceso (`zemdb-server` como dev-dependency) y con un transporte que inyecta fallas.
- **Requisitos de diseño que dejó la revisión cruzada de 4.0** (los resuelve el plan de 4.2, en su §1.1, con su revisión de delta):
  - Una escritura con `CommitAck` en una sala que falla cerrada (INV-K6) espera hasta que se cierra la sala: conviene resolverla como incierta.
  - Un cliente que falla cerrado sigue mandando heartbeats, queda `Connected` con el cursor congelado, frena la poda por cursores hasta el TTL y puede ser designado para subir un snapshot.
  - Al volver a registrarse antes de fallar cerrado, decidir contra el esquema de `Registered`, que está atado a la encarnación, y no contra el de `GetSchema`.
  - Un `CommitAck` demorado, cuya secuencia ya es menor o igual que el head local, resuelve igual su escritura con los rechazos retenidos: no se ignora como las demás respuestas demoradas.
  - INV-K15 no cuenta dos cosas proporcionales a la réplica: el búfer de la subida como designado (`create_snapshot` devuelve un `Vec<u8>`) y el segundo estado completo durante `apply_snapshot`.
  - El tope configurable de la descarga de snapshots (INV-K15), que no figura en §A.6.
  - En el arnés, un pedido de otro cliente puede volver a levantar la sala entre `shutdown_room` y `get_or_spawn`: hay que reintentar hasta que cambie el `schema_id` y definir cómo lo observa el arnés.
  - Un `HeartbeatAck` o un `AckConfirmed` con un head menor que el local podría disparar un sync o un registro, para acortar la ventana de D78.
  - Acotar los rechazos que retiene el aplicador (DC7, INV-K15) por la secuencia asignada y no por el head de la escritura en vuelo más vieja: al llegar el `CommitAck`, conservar solo el de su secuencia; antes, solo los de las secuencias mayores que el último head del servidor que el cliente vio antes de mandar.
  - La reacción a un `InvalidSequence` en un sync o un ack: volver a registrarse con el cursor, como en un commit (INV-K1).
  - Cotas que se puedan calcular para cada elemento de INV-K15, y cómo las mide el arnés sin hacer `pub` nada interno.
  - Limitar el pedido de esquema ante un error de `apply_snapshot` (DC5) a los errores de decodificación y validación, no a `SnapshotBehind` ni a los de E/S.
  - En la cota de antes del `CommitAck`, tomar el head del servidor que el cliente vio antes del primer intento, no antes de cada reintento: un reintento deduplicado recibe la secuencia original, que puede ser menor que un head visto después.
  - Qué responde el aplicador si la secuencia asignada queda por debajo del head visto (reutilización de D78) y no retuvo nada: nunca `Ok` por la ausencia de un rechazo.

### 4.3 · Transporte nativo y `SyncWorker`
- El adaptador `reqwest` sobre HTTP/2.
- El `SyncWorker`: SSE con la reacción a cada evento, pull paginado, `Ack` del último lote, heartbeat y reconexión con *backoff* exponencial y *full jitter* (§A.5).
- La renovación del token ante `Unauthorized`.

### 4.4 · API de Rust
- `ZemdbClient`, `RoomHandle` y `TableHandle`: CRUD, escaneos y `watch` / `ChangeStream` (§A.3 y §A.7).
- Dueño de fila completado por el SDK y pre-chequeo local (§A.4).
- Cifrado de columnas con `CryptoEngine` (§A.10).
- `ClientError` (§A.1).
- Descartar a pedido de la aplicación la réplica local de una sala, para los casos que fallan cerrado (DC12, INV-K3).
- Exponer el id de dispositivo a la aplicación, para que lo mande a su backend al pedir el token, y verificar el `ClientId` de cada token renovado (DC15).
- **Criterio de aceptación:** la app de referencia nativa, en `crates/client/examples/`, corre sus escenarios contra un servidor local.

### 4.5 · Navegador (Rust)
- La feature `wasm` del cliente y su chequeo en la CI (`cargo check --target wasm32-unknown-unknown -p zemdb-client --no-default-features --features wasm`).
- El transporte del navegador (fetch y `EventSource` con `?token=`).
- El motor de DC8.
- El runtime del Worker: recibe los comandos de DC14, los ejecuta sobre la API de 4.4 y publica las respuestas y los eventos de `watch`.
- El cifrado de columnas en el Worker: cómo le llegan las claves o el `CryptoEngine` de la aplicación (INV-K9).
- La coordinación entre pestañas: una sola abre cada sala (con `navigator.locks`, S1), y el Worker suelta sus archivos en `pagehide`.
- S1 en Firefox, pendiente de 4.0 (DC8), y S3 en Safari y Firefox, pendiente de 4.1 (DC17).
- La medición del codificador `ruzstd` en Safari, pendiente de 4.1 (DC9).
- Los requisitos de la implementación del OPFS que dejó el cierre de 4.1 (DC17, en §1 de su plan).
- **Criterio de aceptación:** tests en un navegador headless (por ejemplo, con `wasm-bindgen-test`) en los que la réplica sobrevive a recargar la página.

### 4.6 · API de JavaScript
- El paquete con tipos de TypeScript: abrir el cliente, salas y tablas, CRUD, escaneos y `watch`, con las representaciones de DC14 y errores con código. Dónde vive dentro del repo y cómo se arma (`wasm-bindgen`, `wasm-pack`) lo decide el plan de la sub-fase.
- Pedir `persist()` desde la página al abrir el cliente (S1, en DC8).
- Pasarle al Worker las claves de cifrado de la aplicación (INV-K9).
- Que funcione con un bundler común (Vite), incluida la carga del Worker y del wasm.
- Tests del paquete en navegadores headless (por ejemplo, con Playwright) y su paso en la CI.
- **Criterio de aceptación:** la app de referencia, en TypeScript, corre sus escenarios en el navegador, y su réplica sobrevive a recargar la página.

### 4.7 · Cierre
- La revisión de cierre del delta completo de la fase contra todas las invariantes que tocó.
- Los documentos: una sección del cliente en `ARCHITECTURE.md`, `INVARIANTES.md` con el estado de cada invariante del SDK, `ROADMAP.md` y `README.md`.
- El traspaso a la Fase 5: lo que la app de referencia deja listo para las pruebas E2E.

---

## 5. Cómo se audita y cómo se prueba

### 5.1. Revisiones
Las de `docs/METODOLOGIA.md` §2 (la revisión cruzada) y §6:

| Qué | Cuándo |
|---|---|
| Revisión de delta | Cada lote de cada sub-fase |
| Revisión cruzada del contrato | Cierre de 4.0 |
| Revisión de cierre | Cierre de 4.1, 4.3, 4.4, 4.5, 4.6 y de la fase (4.7) |
| Auditoría amplia por invariantes del SDK | Cierre de 4.2 (DC3) |

Si la auditoría de 4.2 no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.2 antes de abrir 4.3 y 4.4.

### 5.2. Redes de prueba del SDK
Las cuatro capas de `docs/METODOLOGIA.md` §7, aplicadas al cliente:
1. **Reglas.** Cada transición de la réplica (registro, `RoomReset`, `BehindCompaction`, snapshot, commit, reintento, cambio de esquema), positiva y negativa.
2. **Modelo de referencia.** El servidor real en proceso hace de referencia del log, y el oráculo del storage (`crates/storage/tests/common/oracle.rs`) de referencia del estado y de las decisiones de cada réplica.
3. **Simulación.** Varios clientes contra el servidor real, con cortes en cada `.await` del cliente, respuestas perdidas o cortadas, eventos SSE perdidos, poda y reinicios del servidor. Al final: todas las réplicas al mismo estado en la misma secuencia, cada escritura resuelta una sola vez con la decisión de la réplica, y ninguna réplica sana descartada. La app de referencia es uno de sus esquemas.
4. **Robustez.** Fuzzing de las respuestas del servidor y de los snapshots recibidos: nunca un pánico, nunca un lote fuera de orden. En el límite del Worker (4.5 y 4.6), ida y vuelta de cada tipo de valor sin cambios.

Las versiones largas van con `#[ignore]` (`docs/METODOLOGIA.md` §4, regla 5).

### 5.3. Diseño del arnés de simulación
El de las capas 2 y 3 de §5.2. Se diseña en 4.0 y se construye en 4.2, en `crates/client/tests/`, como prueba de integración: usa la API pública del SDK (en 4.2, la de comandos y respuestas de DC14; la de Rust llega en 4.4) y el trait `TransportClient`, sin elementos `pub` solo para probar (`GEMINI.md`).

**Piezas:**
- **El servidor real, en proceso.** `zemdb-server` como dev-dependency: `build_router` sobre un `data_dir` temporal, llamado como un servicio de `tower`, sin sockets. Recorre los extractores, la autenticación, los códigos y la codificación reales; SSE llega como el cuerpo de la respuesta. Lo que agrega `serve_until_shutdown` (el presupuesto de respuestas atado al cuerpo, el *stall timeout*, `max_connections`) queda fuera, y si el arnés toma `server.lock`, lo suelta antes de reiniciar. En 4.2 el arnés no tiene SSE, que llega con el `SyncWorker` (4.3). Reiniciar el servidor es esperar a que terminen las tareas que quedaron sueltas (borrados de sala, recargas de esquema, escrituras del relay) y las del pool bloqueante, que retienen los locks de sus archivos, apagar sus salas (`RoomManager::shutdown_all`) y volver a armarlo sobre el mismo directorio. El servidor no da una forma de esperarlas; el plan de 4.2 lo resuelve así: el transporte en proceso corre cada pedido en una tarea propia, que cortar al cliente no cancela, y el arnés espera esas tareas y las del Control Plane antes de rearmar el servidor (Lote 10 de su plan). Los esquemas, las salas y los cambios de esquema van por el Control Plane del mismo router; los tokens se emiten con `generate_client_token`, una función de la biblioteca, que mide su vencimiento con la hora de pared, así que se emiten con un vencimiento largo.
- **El transporte que inyecta fallas.** Implementa `TransportClient` sobre ese servicio. Por cada pedido, una semilla decide: entregarlo; perderlo antes del servidor; entregarlo y perder la respuesta (el caso de DC6); demorarlo, lo que reordena los pedidos concurrentes; o entregarlo dos veces. En SSE: cortar el stream, perder eventos o mandar `lagged`. Además registra cada respuesta para los oráculos: cada `SequencedOperation` recibida y, por `MutationId`, la secuencia que asignó cada `CommitAck`.
- **El reloj pausado.** Un runtime `current_thread` de Tokio con el reloj pausado (`start_paused`): los plazos del SDK (backoff, heartbeat, reintentos) y los del servidor avanzan sin esperar. Requiere que el servidor use `tokio::time::Instant` en todos sus plazos (DC16). Los que miden la hora de pared (el TTL del log, el vencimiento de los tokens, lo que el roster guarda y una sala restaura al reabrirse, y la recuperación de los snapshots del relay al arrancar, por la fecha de sus archivos) no se controlan: se prueban aparte, con valores chicos y en las versiones largas. Del roster, solo cuenta en hora de pared el tiempo entre que se guarda y la sala se reabre: la inactividad de un cliente hasta el guardado se mide con el reloj de Tokio. La demanda de snapshot, en cambio, se guarda como la hora de su última renovación, así que al reabrir vuelve con el TTL que le queda en hora de pared (con el reloj pausado, casi entero). Mientras corre una tarea de `spawn_blocking`, el reloj pausado no avanza solo: Tokio inhibe el avance automático hasta que termina (visto en Tokio 1.53, en el Lote 3 de 4.1).
- **Los clientes.** N clientes del SDK, en parte con `MemoryStorageEngine` y en parte con `DiskStorageEngine` sobre directorios temporales, algunos como dispositivos del mismo dueño (DC15). Un corte de un cliente es soltar su future en un `.await` elegido al azar; un crash, además, reabrirlo desde su directorio. Las tareas del storage que el cliente soltó siguen hasta terminar y retienen el lock de la sala, así que reabrir puede dar `RoomLocked` por un rato: el arnés reintenta.
- **El archivista.** Un cliente que no se corta ni pierde red y confirma todo con `Ack`: no frena la poda, y la secuencia de operaciones que recibe es el log completo de la sala, del que salen los oráculos. Se pone al día después de cada paso de la carga, antes de cualquier poda, porque la poda por cuota ignora el ancla y podría dejarlo atrás. Una sala borrada y recreada es otra encarnación, con su propio log: el oráculo es por encarnación.

**Carga:** escrituras al azar sobre un dominio chico de claves, para provocar `Unique`, `ForeignKey`, `Restrict`, `Duplicate`, `NotOwner` y `MissingRow`; columnas agregadas en puntos al azar; reasignaciones de la sala a otro esquema que la extiende con las mismas restricciones (no tienen ruta HTTP: se hacen con `RoomManager::get_or_spawn`, que el arnés alcanza por `AppState`; como sobre una sala viva no hace nada, primero la apaga con `RoomManager::shutdown_room`, y después verifica que el `schema_id` cambió); clientes que se suman a una sala podada; poda por cursores y por cuota; reinicios del servidor; una sala borrada y recreada (`RoomReset`). Corre sobre esquemas al azar y sobre el de la app de referencia (§6), con sus escenarios.

**Verificaciones:**
- **En cada paso:** cada réplica es igual al oráculo en su propio head (INV-K1). El oráculo es el modelo de referencia del storage (`crates/storage/tests/common/oracle.rs`), que no comparte código con los motores, aplicado al log del archivista. Para no recalcularlo, se guarda su estado en algunos heads.
- **Al cerrar:** se cura la red, se espera hasta que todos los clientes estén en el head (INV-K7, con un tope de pasos), y todas las réplicas tienen que ser iguales al oráculo, con la misma decisión sobre cada operación (INV-K4).
- **Sobre cada escritura:** termina una sola vez. Con `Ok`, `ConstraintViolation` o `RowMissing`, coincide con la decisión del oráculo en la secuencia que registró el transporte. El resultado incierto solo es válido en los cuatro casos de INV-K2: algún intento quedó incierto y nunca llegó un `CommitAck`; la réplica no aplicó la secuencia asignada (un snapshot, `RoomReset`, la réplica descartada o la sala que falla cerrada); se perdió el reporte del lote (DC7); o el cliente o la sala se cerraron con la secuencia asignada sin aplicar. Con `CommitAck` y la secuencia aplicada, el resultado es la decisión de la réplica. El error sin efecto solo es válido si ningún intento quedó incierto ni llegó un `CommitAck`.
- **Sobre la memoria:** lo que retiene cada cliente fuera de la réplica, al final de cada paso, no pasa de las cotas de INV-K15.
- **Sobre los descartes:** el SDK descartó una réplica solo por `RoomReset`, por `InvalidSequence` en el registro, por un daño en sus archivos al abrir o porque faltaban sus metadatos (INV-K3). La prueba lo observa por la época de `status` (DC18, plan de 4.2).

**Reproducción:** la semilla se imprime al fallar. La ejecución no es del todo determinista, porque el storage y el servidor usan el pool bloqueante de Tokio; una semilla reproduce el escenario, no siempre el orden exacto.

**Tamaños:** una versión corta en la suite normal (unos segundos en debug) y una larga con `#[ignore]`, en `.github/workflows/randomized.yml`.

**Fuera del arnés:** el navegador (4.5 y 4.6 tienen sus propios tests en navegadores headless) y el fuzzing de la capa 4, que reusa el transporte para fabricar respuestas.

---

## 6. App de referencia: el ranking por votos (DC4)

Es el formato de la app de rankings entre amigos en el que se originó el proyecto: en cada ronda, cada miembro vota y los votos suman puntos al ranking. Extiende el caso de los votos de la Fase 3.11 (§3.4 de su propuesta) para que ejercite todo lo que el SDK tiene que hacer, incluida una agregación sobre muchas filas (los puntos) que la réplica calcula localmente y `watch` actualiza mientras llegan votos.

Es un esquema de este repo, no el de la app real (ver *La app real, aparte*, abajo). Esquema (adoptado el 22 de Octubre de 2026):
- **`rankings(ranking_id, creator, name)`:** clave `ranking_id`, dueño en `creator`.
- **`members(ranking_id, member_id, display_name)`:** clave `(ranking_id, member_id)`, dueño en `member_id` (cada uno se suma a sí mismo) y FK `ranking_id → rankings`.
- **`rounds(ranking_id, round_id, opened_by, title, notes)`:** clave `(ranking_id, round_id)`, dueño en `opened_by`, FK `ranking_id → rankings` y `(ranking_id, opened_by) → members` (solo un miembro abre una ronda), `UNIQUE(ranking_id, title)` con `title` no nullable (un `UNIQUE` con `NULL` no se verifica) y `notes` cifrada.
- **`votes(voter, ranking_id, round_id, candidate)`:** clave `(voter, ranking_id, round_id)`, dueño en `voter`, FK `(ranking_id, round_id) → rounds`, `(ranking_id, voter) → members` (solo un miembro vota) y `(ranking_id, candidate) → members`, tabla inmutable. Las FK comparten `ranking_id`, así que un voto no puede mezclar la ronda de un ranking con un miembro de otro.

**Tipos:** `ranking_id` y `round_id` son `Uuid`; `creator`, `member_id`, `opened_by`, `voter` y `candidate`, `String` con la parte de dueño de un `ClientId`, la persona (DC15); `name`, `display_name` y `title`, `String`; `notes`, `String` cifrada.

**Propiedades del esquema**, que la app de referencia acepta:
- Las FK hacia `members` desde `rounds` y desde el `voter` de `votes` incluyen la columna dueño de las dos tablas: es el ejemplo de la guía de DC13.
- Como los votos son inmutables y tienen FK, un miembro que votó o recibió votos no puede irse, y una ronda con votos no se puede borrar.
- Un ranking con otros miembros no se puede borrar: sus filas de `members` son de otros dueños (`Restrict` entre dueños distintos, el caso de DC13).
- Que nadie se vote a sí mismo es una regla de la aplicación, no del esquema.

Escenarios:
- varios miembros votan en una ronda y todas las réplicas llegan a la misma tabla de posiciones;
- un segundo voto en la misma ronda se rechaza (`Duplicate`), un voto a alguien que no es miembro también (`ForeignKey`), uno de alguien que no es miembro también (`ForeignKey`), y dos rondas con el mismo título (`Unique`);
- la misma persona vota desde dos dispositivos (dos `ClientId` con el mismo dueño, DC15): sus dos réplicas convergen y el segundo voto se rechaza (`Duplicate`);
- los miembros que se sumaron le impiden al creador borrar el ranking (`Restrict` entre dueños distintos, el caso de DC13);
- un miembro nuevo entra a un ranking con la historia podada y se pone al día desde un snapshot que sube otro miembro;
- sin red, se sigue viendo la tabla de posiciones y no se puede votar;
- `watch` de la tabla de posiciones mientras llegan votos;
- `notes` nunca viaja en claro.

| Sub-fase | Uso |
|---|---|
| 4.0 | Se fijan el esquema y los escenarios |
| 4.2 | Es uno de los esquemas de la simulación |
| 4.4 | Criterio de aceptación de la API de Rust, como ejemplo nativo |
| 4.6 | Criterio de aceptación de la API de JavaScript, en TypeScript en el navegador |
| Fase 5 | Base de las pruebas E2E |

**La app real, aparte.** La app de rankings se rehace en TypeScript sobre la API de JavaScript, en su propio repo, y es el primer usuario real del SDK. Necesita su propio backend, que emita los tokens de cliente (por dispositivo y por sala, DC15) y cree los esquemas y salas por el Control Plane. Se valida contra ella en dos hitos (después de 4.4, con ese backend en Rust, y después de 4.6), sin que bloquee el cierre de ninguna sub-fase: lo que aparezca entra a este plan o al backlog con la regla de alcance (`docs/METODOLOGIA.md` §5, paso 6). Depende de este repo por git, fijada a un commit (o por ruta mientras se desarrollan juntos), y sigue de cerca a `main`, porque el protocolo no se versiona antes del primer release (`GEMINI.md`).

---

## 7. Riesgos

- **La auditoría de 4.2 puede pedir rediseño del núcleo.** Es la razón de DC3: encontrarlo antes de construir encima.
- **El navegador.** Las garantías de `flush()` en el OPFS no están especificadas más allá del estándar; el navegador puede borrar el almacenamiento si le falta espacio (salvo `persist()`), y entonces el cliente vuelve a bajar todo; varias pestañas compiten por los mismos archivos. S1 tiene que responder lo que se pueda antes de DC8.
- **Herramientas de JavaScript.** El paquete suma `wasm-bindgen`, npm, un bundler y tests en navegadores a un repo que hasta ahora es solo Rust; el tamaño del wasm y la carga del Worker dependen del bundler, y los tests en navegador pueden ser inestables en la CI.
- **Memoria.** Los índices de las restricciones viven en memoria también en el navegador: una sala de 100 MB de filas con tres restricciones por tabla ocupa unos 330 MB (§8 de la propuesta de la Fase 3.11).
- **Alcance.** El catálogo del Apéndice A es grande. Lo que no esté en un lote no se hace en el lote (`docs/METODOLOGIA.md` §5, paso 6).

---

## 8. Seguimiento

**Avance:** 4.0 cerrada el 23 de Octubre de 2026, con la revisión cruzada en verde. 4.1 cerrada el 25 de Octubre, con la revisión de cierre en verde sobre `977837f`; su [plan por lotes](2026-10-fase4_1-storage-servidor.md) lleva su seguimiento. 4.2 en curso desde el 26 de Octubre, con su [plan por lotes](2026-10-fase4_2-nucleo-replica.md): Lote 0 cerrado el 27 de Octubre, con la revisión cruzada en verde; DC18 a DC23 adoptadas el 28 de Octubre. El plan de 4.0 fue este documento más el Lote 0 de `docs/METODOLOGIA.md` §2, en estos pasos:

| Paso | Qué | Estado |
|---|---|---|
| 1 | Decisiones de marco: DC10, DC11, DC4 y DC15 | ✅ Adoptadas |
| 2 | Spikes S1 y S2, con sus preguntas | ✅ S1 respondido en Chrome y Safari (en DC8; Firefox pendiente para 4.5); S2 respondido y DC9 adoptada |
| 3 | Escritura y réplica: DC5, DC6, DC7, DC12 y DC13 | ✅ Adoptadas |
| 4 | DC14 (con `Int(i64)`), y DC8 y DC9 con los spikes | ✅ Adoptadas |
| 5 | Contrato en `INVARIANTES.md`: modelo de amenaza (§2), §3.11 *Cliente*, resumen y reparto (§1.6); en `ARCHITECTURE.md` §3.1, la guía de DC13 y la unidad de `Timestamp` (DC14) | ✅ INV-K1 a K15, todas 🆕 |
| 6 | Varios dispositivos (DC15) y diseño del arnés de simulación (§5.3) | ✅ Análisis en el Apéndice B (DC15 reemplazada), arnés en §5.3, DC16 |
| 7 | Revisión cruzada del contrato | Cuatro rondas completas sin verde (2+13, 1+12, 1+8 y 1+8 bloqueantes y ajustes), todas corregidas. La revisión acotada al delta de la cuarta encontró un enunciado falso que esa corrección había introducido (cómo se detecta la reutilización de D78), un ajuste de conteo y otro de argumento; corregidos. Lo de diseño del SDK pasó a 4.2. La revisión de esas correcciones encontró un ajuste más (INV-K15 prometía una cota que la regla de DC7 no da) y un menor; corregidos, con el diseño en 4.2. La revisión de esas dos frases salió en verde, con dos menores, corregidos. ✅ |

---

## Apéndice A. Catálogo del SDK

Qué tiene que construir la Fase 4. Hasta el 22 de Octubre de 2026 vivía en `ROADMAP.md` §4.4; las sub-fases del §4 dicen en qué orden se construye.

### A.1. Modularización Física del Crate `zemdb-client`
* **Organización Modular:**
  - `src/lib.rs`: Punto de entrada público del SDK, re-exportando facades ergonómicas y tipos esenciales.
  - `src/config.rs`: `ClientConfig` con builders tipados para endpoints de servidor, intervals de heartbeat, estrategias de retry y credenciales.
  - `src/error.rs`: Enum tipado `ClientError` (ValidationError, NetworkError, StorageError, ConstraintViolation, RowMissing, Offline, OutcomeUnknown, OutOfRange, PrecheckFailed). `ConstraintViolation` y `RowMissing` son el resultado de una escritura que la réplica rechaza (§A.4); `OutcomeUnknown`, el de una escritura cuyo resultado es incierto (DC6, DC7); `OutOfRange`, el de un valor que no cabe en su representación de JavaScript (DC14); `PrecheckFailed`, el del pre-chequeo local (§A.4). El error de DC12 llega como `StorageError`. Los `ErrorCode` del servidor (tabla de `ARCHITECTURE.md` §7.2) se agrupan por la reacción que les corresponde: rechazo permanente de la operación (`SchemaViolation`, `BadRequest`, en `ValidationError`), credenciales (`Unauthorized`: pedir un token nuevo; `Forbidden`: permanente para ese token, en `ValidationError`) y reintentables (`Unavailable`, `Timeout`, `RequestTimeout`, `RateLimited` y fallas de red, en `NetworkError`). Los códigos que el SDK resuelve solo (`ClientNotRegistered`, `BehindCompaction`, `RoomReset`, `SnapshotSuperseded`, `InvalidSequence`: en el registro lleva a descartar la réplica, y en un sync, commit o ack, a volver a registrarse) no llegan a la aplicación salvo que fallen sus recuperaciones.
  - `src/api/`: Facade pública de alto nivel:
    - `client.rs`: `ZemdbClient` (gestor global de conexión y catálogo de salas abiertas).
    - `room.rs`: `RoomHandle` (ámbito de sala activa, schemas y despacho de mutaciones).
    - `table.rs`: `TableHandle` (operaciones declarativas CRUD, escaneos con pushdowns y suscripciones reactivas).
  - `src/sync/`: Núcleo de sincronización canónica y consistencia:
    - `syncer.rs`: Cliente de sincronización unificado (Commit & Sync en 1 RTT).
    - `worker.rs`: `SyncWorker` (tarea Tokio en background con consumo de eventos SSE, pulls `/sync` y heartbeats).
    - `reactive.rs`: `ChangeStream` y bus reactivo a tuplas en memoria.
    - `snapshot.rs`: descarga por chunks del snapshot de onboarding (§A.6) y subida del snapshot cuando el servidor designa a este cliente (§A.8).
    - `state.rs`: metadatos durables del cliente por sala (§A.9).
  - `src/transport/`: Abstracción de red desacoplada:
    - `client.rs`: `trait TransportClient`.
    - `native.rs`: Adaptador HTTP/2 persistente basado en `reqwest`.
    - `wasm.rs`: Adaptador WebFetch basado en `web-sys` y `gloo-net` para navegadores.
  - **Paquete de JavaScript** (§A.12): la API del navegador, con tipos de TypeScript, sobre el SDK compilado a wasm. Su ubicación en el repo la decide el plan de la sub-fase 4.6.

### A.2. Feature Flags y Universalidad Multiplataforma
* **Configuración en `Cargo.toml` del cliente:**
  - `default = ["native"]`: Incluye `tokio` (full) y `reqwest`.
  - `wasm = ["web-sys", "wasm-bindgen-futures", "gloo-net"]`: Permite compilar el cliente hacia `wasm32-unknown-unknown` para su ejecución en navegadores sin arrastrar llamadas bloqueantes de sistema operativo.
  - `zemdb-storage` no tiene features: elige su código con `cfg(target_arch = "wasm32")`. En wasm32 compila solo `MemoryStorageEngine`.
* **Persistencia en el navegador (pendiente):** hoy un cliente wasm32 no persiste nada: `MemoryStorageEngine` vive en memoria y `DiskStorageEngine` usa `std::fs`, `flock` y el pool bloqueante de Tokio. La Fase 4 necesita un motor persistente para el navegador. El candidato es el *Origin Private File System* (OPFS) con `FileSystemSyncAccessHandle`, disponible en Chrome, Firefox y Safari: da archivos con escritura, `truncate` y `flush` síncronos, un lock exclusivo por archivo, y funciona solo dentro de un Web Worker dedicado. La alternativa era IndexedDB. Lo eligió DC8: el OPFS. Los índices de las restricciones de la Fase 3.11 viven en memoria también en el navegador: una sala de 100 MB de filas con tres restricciones por tabla ocupa unos 330 MB de RAM (§8 de la [propuesta](2026-10-fase3_11-restricciones-proposal.md)).
* **Zstd en wasm32 (DEF-54, cerrado en el Lote 6 de 4.1):** wasm32 escribía snapshots sin comprimir y rechazaba los Zstd, así que un navegador no podía restaurar un snapshot subido por un cliente nativo. Ahora decodifica y codifica con `ruzstd`, en Rust puro, que en 4.5 también codifica el `.snap` del navegador (DC9; `ARCHITECTURE.md` §8 *Zstandard in WebAssembly*).

### A.3. Fachada Pública Ergonómica del SDK
API idiomática, tipada y declarativa para aplicaciones en Rust:
```rust
let client = ZemdbClient::open(config, storage_engine).await?;
let room = client.room(RoomId::new("engineering_team")).await?;
let tasks = room.table("tasks")?;

// 1. Inserción con validación previa en servidor y sync en 1 RTT (Write-Through)
let pk = tasks.insert(row).await?;

// 2. Consulta de ultra-baja latencia sobre almacenamiento canónico local (<1ms)
let task = tasks.get(&pk).await?;

// 3. Escaneo filtrado con pushdowns
let stream = tasks.scan(ScanOptions::default().limit(50)).await?;

// 4. Suscripción reactiva en vivo
let mut live_stream = tasks.watch(&pk).await?;
tokio::spawn(async move {
    while let Some(updated_row) = live_stream.next().await {
        println!("Registro actualizado: {:?}", updated_row);
    }
});
```

En el navegador, la misma API se expone en JavaScript con tipos de TypeScript (§A.12).

Cada `insert`, `update` o `delete` es un commit de una sola operación (`ClientMessage::Commit` lleva una `Operation`): la v0.1 no tiene transacciones de varias filas.

### A.4. Modelo Server-Authoritative con Sincronización en 1 RTT (Write-Through)
* **Persistencia Canónica Estricta en `StorageEngine`:**
  - El motor de persistencia local (`zemdb-storage`) almacena **única y exclusivamente operaciones secuenciadas (`SequencedOperation`)** validadas y ordenadas por el servidor.
  - Esto erradica de raíz el riesgo de inconsistencias locales, tuplas zombi o estados corruptos ante rechazos de validación (aridad, esquemas o tipos). Una operación que viola una restricción de integridad también queda en el log canónico, y cada réplica la rechaza igual al aplicarlo (Fase 3.11), así que tampoco hace divergir a las réplicas.
* **Pipeline de Escritura y Sincronización en 1 RTT:**
  1. **Envío con Cursor Actual:** Al invocar `insert`, `update` o `delete`, el cliente envía la mutación al endpoint `/commit` del servidor junto con su identificador `MutationId` y su cursor actual `last_ack_seq`. En una tabla con dueño, el SDK ya completó el dueño (ver abajo).
  2. **Validación Server-Side (liviana):** El servidor valida lo que no depende del estado: esquema, tipos, nulabilidad, que el dueño que declara la operación sea el dueño del cliente autenticado (desde 4.1, el prefijo de su `ClientId`, DC15) y que una tabla inmutable no reciba `Update` ni `Delete`. Si alguna validación falla, retorna `ErrorCode::SchemaViolation` (permanente para esa mutación; no `Forbidden`, porque el token es válido), no se secuencia nada y el almacenamiento local **permanece intacto y consistente**. El servidor no guarda filas, así que no valida `UNIQUE`, las claves foráneas (`RESTRICT` incluido), el segundo `Insert` de una clave de una tabla inmutable (`Duplicate`) ni el dueño de la fila existente (`NotOwner`): los decide la réplica (paso 4).
  3. **Secuenciación y Catch-Up Atómico:** Si la mutación es válida, el servidor asigna el `SequenceNumber` canónico y devuelve `CommitAck` conteniendo dicho número junto con cualquier delta remoto pendiente acumulado desde `last_ack_seq`.
  4. **Aplicación en Storage y Resultado de la Escritura:** El cliente aplica los deltas canónicos con `StorageEngine::apply_batch`, que devuelve un `ApplyReport`: el `head_seq` de la sala y las operaciones que no aplicó, con su motivo. La escritura se resuelve cuando la réplica aplicó hasta la secuencia asignada: si el catch-up no llegó hasta ahí (`has_more`, o un acuse sin catch-up por falta de presupuesto), el cliente sigue con `/sync`. El resultado sale del `ApplyReport` del lote que contiene esa secuencia: `Ok`, `ConstraintViolation { constraint, kind }` (por `Unique`, `ForeignKey`, `Restrict`, `Duplicate` o `NotOwner`) o `RowMissing` (un `Update` de una fila que no existe).
  5. **Notificación Reactiva:** Se notifican a los streams activos (`watch`) los cambios aplicados; el `ChangeStream` no emite nada por una operación rechazada.
* **Reacción del commit a cada respuesta (DC6):**

  | Respuesta | Qué hace el SDK |
  |---|---|
  | `CommitAck` | El aplicador resuelve la escritura con la decisión de la réplica en la secuencia asignada (DC7). |
  | `SchemaViolation`, `BadRequest` | Rechazo de la escritura, salvo que algún intento anterior haya quedado incierto. |
  | `Forbidden` | Error permanente para ese token. |
  | `Unauthorized` | Pide un token nuevo a la aplicación y reintenta. |
  | `ClientNotRegistered`, `InvalidSequence` | Se vuelve a registrar y reintenta. |
  | `BehindCompaction` | Se pone al día (§A.6) y reintenta con el mismo `MutationId`. |
  | `RequestTimeout` | Sin efecto en el servidor: reintenta. |
  | `RoomNotFound`, `SchemaNotFound`, `ProtocolVersionMismatch` | Error permanente, salvo que algún intento anterior haya quedado incierto. |
  | `RoomLocked` | La sala no pudo abrirse en el servidor: reintenta. |
  | `Unavailable`, `Timeout`, `Internal`, falla de red, sin respuesta | El intento queda incierto: reintenta con el mismo `MutationId`. |
  | `RoomReset` | Incierto si algún intento quedó incierto o llegó un `CommitAck`; si no, un error sin efecto. |
  | Vence el plazo | Incierto si algún intento quedó incierto; si no, un error que dice que no tuvo efecto. |
  | Cualquier otra, y `Forbidden` o `Unauthorized` sin token nuevo | Incierto si algún intento anterior quedó incierto; si no, un error sin efecto. |

  `ErrorCode::Internal` se documenta hoy como "no se reintenta automáticamente" (`zemdb-core`); el SDK lo reintenta porque el dedup lo hace seguro, y ese comentario cambia con el código de 4.2.
* **El Dueño de Fila lo Completa el SDK:** La aplicación no escribe el dueño. El SDK pone la parte de dueño de su `ClientId` (DC15) en la columna dueño: en la fila del `Insert` o, si la columna es parte de la clave primaria, en la clave, de la que la aplicación da el resto. Con el dueño fuera de la clave, agrega el delta del dueño a cada `Update` y el `owner` a cada `Delete`. Si la aplicación escribe la columna dueño a mano, el SDK rechaza la llamada localmente con un error claro, sin mandar nada: no la pisa en silencio.
* **Pre-Chequeo Local:** Activado por defecto y desactivable: antes de mandar, verifica la escritura contra el estado local y, si la réplica local ya la rechazaría, no la manda y devuelve `PrecheckFailed`, distinto de `ConstraintViolation` porque no es la decisión de la réplica en una secuencia. Como la réplica local puede estar atrasada, puede frenar una escritura que el log habría aceptado; la aplicación que no lo quiera lo desactiva.
* **Requisitos de Diseño de la Fase 3.11 (§7 de su [propuesta](2026-10-fase3_11-restricciones-proposal.md)),** resueltos por DC7 (el reporte perdido), DC13 (entre dueños distintos), DC12 (el snapshot local) y DC5 (el esquema):
  - **El reporte perdido ante un crash:** sin outbox, ninguna escritura espera su resultado después de un crash; si el reporte de un lote se pierde sin crash, las escrituras de esas secuencias terminan como inciertas (DC7).
  - **Restricciones entre dueños distintos:** las reglas valen igual para todos los miembros. Uno puede ocupar primero una clave `UNIQUE`, y una fila propia que referencia la fila de otro (que ese otro no puede borrar, `NotOwner`) le impide borrar la suya (`RESTRICT`). Se documenta para quien escribe el esquema, sin regla nueva (DC13, `ARCHITECTURE.md` §3.1).
  - **Un snapshot local que no cumple las restricciones:** `open_room` lo rechaza con `SnapshotCorruption`, la misma variante que un archivo dañado, pero no es daño: solo pasa si la sala se abre con restricciones con las que sus datos no se escribieron. El SDK no descarta la réplica local por eso: tendrá una variante propia de error (DC12, en 4.1).
  - **El esquema con que el SDK abre y aplica:** `open_room` y `apply_snapshot` aceptan cualquier esquema que tenga las tablas de la sala. El SDK abre solo con el esquema que le sirve el servidor y, ante un cambio, aplica la regla de DC5: el esquema nuevo tiene que evolucionar del suyo (`Schema::evolves_from`), sea una versión nueva o una reasignación; si no, falla cerrado.
  - **Borradores offline (post-v0.1):** con restricciones, mandar lo consolidado deja de ser equivalente a mandar en orden (ver `ROADMAP.md` §3, punto 8).
* **Cero Complejidad de Rebase / Rollback:**
  - Al no aplicar escrituras optimistas sin confirmar sobre las tablas canónicas, se elimina la necesidad de motores de rollback/rebase en el cliente.
* **Comportamiento Offline (Modo Solo Lectura):**
  - En ausencia de conexión a red, el cliente opera en modo **Solo Lectura** de alta velocidad sobre su réplica local en `StorageEngine`. Cualquier intento de escritura sin red retorna inmediatamente `Err(ClientError::Offline)`.
  - *(Nota: la v0.1 no tiene outbox. La capacidad de almacenar escrituras offline no confirmadas de forma segregada queda formalmente diferida para post-v0.1, ver `ROADMAP.md` §3).*

### A.5. Worker de Sincronización en Segundo Plano con Jitter (`SyncWorker`)
* **Escucha Reactiva de SSE (opcional):**
  - Mantiene una conexión Server-Sent Events hacia `GET /rooms/{room_id}/events`, con el token en `Authorization: Bearer` o, en el navegador, en `?token=` (`EventSource` no puede mandar headers). Cada evento tiene su reacción:
    - `head_advanced` (dato: el nuevo head): pull paginado a `/sync`.
    - `schema_reloaded` (dato: el id del esquema): un registro con el cursor y el cambio del esquema local, con las reglas de §A.4 (*El esquema con que el SDK abre y aplica*) y DC20.
    - `snapshot_wanted` (sin dato): un heartbeat, cuyo `snapshot_wanted` dice si este cliente es el designado (§A.8).
    - `snapshot_available` (dato: su secuencia): si el cliente espera un snapshot, empieza la descarga (§A.6).
    - `lagged`: el stream perdió eventos; el worker hace un `/sync`.
  - SSE solo acelera: las respuestas a heartbeats, syncs y commits son la fuente de verdad, y el worker funciona igual sin stream. Un cliente tiene como mucho 2 streams por sala: un tercero cierra el más viejo.
* **Avance del cursor:**
  - El cursor del cliente en el servidor avanza solo con lo que el cliente confirma: el `from_seq` de cada `/sync` servido, el `last_ack_seq` de cada commit aceptado y `ClientMessage::Ack`. Después de aplicar el último lote (`has_more: false`), el worker manda `Ack` con su head local, para que la poda del servidor no espere a su próximo sync.
* **Reconexión Resiliente:**
  - Si la conexión se interrumpe, el worker utiliza un algoritmo de reconexión con *Exponential Backoff* y *Full Jitter*, mitigando el riesgo de tormentas de reintentos (*thundering herd*) contra el servidor.
* **Heartbeat Continuo:**
  - Emite periódicamente `ClientMessage::Heartbeat`, con un intervalo menor que el `lease_timeout` de la sala (90 s por defecto), para renovar su lease e impedir que el servidor lo pase a `Disconnected`. El heartbeat no lleva cursor ni lo mueve. Su respuesta, `HeartbeatAck`, informa `current_head_seq`, `snapshot_wanted`, `active_snapshot_seq`, `schema_version` y, desde 4.1, `schema_id` (DC5).

### A.6. Onboarding de Salas y Descarga Multipart de Snapshots
El camino lo decide el registro (`ARCHITECTURE.md` §6):
1. **Registro.** El cliente manda `RegisterClient` con su cursor (`current_seq`) y la encarnación a la que pertenece (`known_incarnation`) si tiene estado local de la sala, o sin ninguno de los dos si no tiene.
2. **`RoomReset` (409).** La sala se borró y se volvió a crear con el mismo id. El cliente descarta su estado local de la sala y se registra sin cursor, como uno nuevo.
3. **`Registered`.** Trae `head_seq`, `tail_seq`, `active_snapshot_seq`, el esquema y `room_incarnation`, que el cliente guarda entre sus metadatos (§A.9); abre la sala con ese esquema (§A.4). Si su cursor (0 sin estado local) es mayor o igual que `tail_seq - 1`, sincroniza deltas con `/sync` desde el cursor. Si no, está `Bootstrapping` y necesita un snapshot.
4. **Snapshot.** Si `active_snapshot_seq` es `Some(S)`, lo descarga, siempre por chunks (`ClientMessage::RequestSnapshotChunk`, de 64 KiB a 4 MiB; el primero sin `snapshot_hash` y los demás con el hash de la primera respuesta). Ante `SnapshotSuperseded` (409) descarta lo recibido y vuelve al chunk 0. Al completar la descarga verifica el BLAKE3, aplica el snapshot con `StorageEngine::apply_snapshot` pasando `S` como secuencia esperada (que también verifica las restricciones) y sincroniza desde `S`. Si no hay snapshot usable, sigue con heartbeats hasta que `HeartbeatAck` (o el evento `snapshot_available`) anuncie uno. Con DC22 y DC23, cada chunk lleva la encarnación de los metadatos, y un `RoomReset` lleva al paso 1 con su cursor.
5. **`BehindCompaction` (410) más adelante.** Un sync, commit o ack con un cursor que quedó detrás del log (por ejemplo, por una poda por TTL entre el registro y el primer sync) lleva al paso 4.
6. **`ClientNotRegistered` (409).** El servidor no lo tiene en su roster: vuelve al paso 1 con su cursor.
7. **`InvalidSequence` (400) en el registro.** El cursor del cliente supera el head del servidor, que perdió secuencias (D78): el cliente descarta su estado local de la sala y se registra sin cursor (DC12).

### A.7. Bus de Suscripciones Reactivas en Vivo (`ChangeStream`)
* Abstracción que permite a componentes de interfaz de usuario observar cambios sobre registros específicos o rangos (`table.watch(&pk)`).
* El stream asíncrono emite notificaciones inmediatas una vez que los deltas canónicos son confirmados y aplicados en el almacenamiento local.
* Una operación que la réplica rechaza (ver el `ApplyReport` en §A.4) no emite nada.

### A.8. Subida del Snapshot por el Cliente Designado
* Cuando la sala necesita un snapshot (un cliente quedó `Bootstrapping` y no hay uno usable), el servidor designa a un cliente `Connected` y se lo dice con `snapshot_wanted: true` en `HeartbeatAck`, `SyncBatch` o `CommitAck`.
* El designado genera el snapshot con `StorageEngine::create_snapshot` en su head local `S` y lo sube (con DC22 y DC23: siempre por chunks, y cada chunk con la encarnación de la sala), en un request (`POST /rooms/:id/snapshot/upload`, hasta 16 MiB) o por chunks (`ClientMessage::UploadSnapshotChunk`). El relay lo acepta si `tail_seq - 1 <= S <= head_seq` y `S` es mayor que la del snapshot activo. Si no empieza a subir dentro de los 60 s, el servidor designa a otro.
* El repo no tiene un worker de snapshots: sin esta subida, un cliente nuevo en una sala podada no puede incorporarse (INV-R4 supone un cliente `Connected` que sigue el protocolo).

### A.9. Metadatos Durables del Cliente
* Por sala, el cliente guarda de forma durable la `room_incarnation` y el esquema vigente con su `schema_id` (DC5), con el que abre la sala también sin red; su `client_id` sale del id de dispositivo, que también guarda. Su cursor es el head de su storage, y va siempre con la encarnación de los metadatos: después de registrarse sin cursor, el cliente arranca de la secuencia 0 o de un snapshot, nunca de un cursor guardado (`ARCHITECTURE.md` §6, *Room incarnation*; DC19).
* El cursor no se guarda: es el head con que el storage abre la sala (DC19). Si storage abre la sala por detrás de lo que alcanzó (por ejemplo, vacía en la secuencia 0 después de perder el `.snap`, riesgo D96 de `docs/INVARIANTES.md` §4), el cliente presenta ese head y vuelve a bajar del servidor lo que falta. Sin metadatos, los datos de la sala se borran antes de abrirla.
* El token lo emite el backend de la aplicación. Ante `Unauthorized` el SDK pide uno nuevo a la aplicación.
* Cada dispositivo tiene su propio `ClientId`, de la forma `persona#dispositivo` (DC15): el SDK genera el id de dispositivo, lo guarda con los metadatos y la aplicación lo manda a su backend al pedir el token. El SDK genera cada `MutationId` al azar (128 bits).

### A.10. Columnas Cifradas (E2EE)
* Las columnas con `encrypted: true` viajan como `Value::Bytes` opacos (`ARCHITECTURE.md` §3.1). El SDK las cifra al escribir y las descifra al leer con el `CryptoEngine` de `zemdb-core`, usando como datos adicionales `(room_id, table_id, pk, column_idx)`, para que un texto cifrado de otra celda, también de otra sala, no se descifre.

### A.11. Invariantes y Auditoría del SDK
* Las invariantes del SDK están en `docs/INVARIANTES.md` §3.11 (INV-K1 a K15), escritas en 4.0 antes del código, y su modelo de amenaza en §2 (DC10, DC11). El SDK se audita una vez contra ellas, con el método de §1, al cerrar 4.2 (DC3).

### A.12. API de JavaScript para el Navegador
* En el navegador, el SDK compilado a wasm corre en un Web Worker dedicado, donde el OPFS da archivos síncronos (§A.2), y la página lo usa a través de un paquete con tipos de TypeScript que le habla por mensajes (DC14).
* Las filas son objetos con nombres de columna; los errores llevan un código que mapea `ClientError`, y `watch` se puede usar desde React (`useSyncExternalStore`). La representación de cada tipo, `Int(i64)` y `Timestamp` incluidos, la fija DC14.
* Los tipos de TypeScript generados a partir del esquema y los bindings para apps móviles nativas quedan fuera de la v0.1.

---

## Apéndice B. Varios dispositivos con un mismo `ClientId` (primera versión de DC15)

Análisis de la sub-fase 4.0, sin código, sobre el servidor en `a920c04`, de la primera versión de DC15: todos los dispositivos de una persona con el mismo `ClientId`. Llevó a reemplazarla por un `ClientId` por dispositivo, con el dueño como prefijo, con el que cada dispositivo tiene su propia entrada y ninguna de estas filas aplica, salvo la 7 y la 10. El servidor guarda por `ClientId` una entrada del roster (cursor, estado y lease), la designación para subir un snapshot, el uploader de una subida y el tope de streams SSE. La clave de dedup es `(ClientId, MutationId)`.

| # | Mecanismo | Con varios dispositivos del mismo `ClientId` | Conclusión con el `ClientId` compartido |
|---|---|---|---|
| 1 | Registro | Reemplaza la entrada: el cursor pasa a ser el del dispositivo que se registra. Uno nuevo, sin cursor, la deja en 0 y `Bootstrapping` si la sala está podada: hasta el próximo sync, commit o ack de otro dispositivo (un heartbeat no mueve el cursor), la entrada no frena la poda ni se la puede designar. | Aceptado: un cursor más bajo solo retiene más (INV-Y6). |
| 2 | Cursor y poda | El cursor avanza al máximo que confirmó cualquier dispositivo desde el último registro. La poda por cursores protege al dispositivo más adelantado, no al más atrasado. | Aceptado: el atrasado recibe `BehindCompaction` (INV-Y3) y se pone al día desde un snapshot. Nada se pierde. |
| 3 | Lease | Lo renueva cualquier dispositivo; uno que se va no deja la entrada `Disconnected`. | Aceptado: el mismo efecto que la fila 2. |
| 4 | `BehindCompaction` del atrasado | El servidor recalcula el estado con el cursor guardado (el del adelantado): la entrada sigue `Connected` y la sala no pide un snapshot. Si no hay uno usable, el atrasado no tiene salida mientras no se vuelva a registrar: sus heartbeats no prenden la demanda. | **Hallazgo 1.** Rompe INV-R4 e INV-K7. |
| 5 | Designación | `snapshot_wanted` les llega a todos los dispositivos del designado, también al atrasado. | Requisito del SDK (4.2): sube solo si su réplica está al día con el log retenido y no está bajando un snapshot; ante `SnapshotSuperseded`, deja de subir. |
| 6 | Subida por chunks | Dos dispositivos en la misma secuencia con bytes distintos (por ejemplo, Zstd en nativo y otro nivel o sin comprimir en el navegador) se reemplazan entre sí, porque el relay deja que un uploader reinicie su propia subida con otros parámetros. Mientras, la designación no cambia. Ninguna termina hasta el tope de la subida (`snapshot_upload_max_secs`, 2 horas por defecto). En secuencias distintas no pasa: la mayor reemplaza a la menor, que recibe `SnapshotSuperseded`. Una subida en un solo request tampoco: la segunda recibe `SnapshotSuperseded`. | **Hallazgo 2.** Demora acotada, sin pérdida. |
| 7 | Dedup | Con `MutationId` aleatorio de 128 bits, las escrituras de dos dispositivos nunca chocan. | Requisito que se mantiene con DC15. |
| 8 | SSE | Dos streams por `ClientId` y sala: un tercero cierra el más viejo. Con tres o más dispositivos con SSE, se desplazan entre sí. | Aceptado: SSE solo acelera. Requisito del SDK (4.3): reconectar con backoff. |
| 9 | Deregistro | El de un dispositivo saca la entrada de todos; los demás reciben `ClientNotRegistered` y se vuelven a registrar con su cursor (§A.6, paso 6). | Aceptado: cuesta un registro. |
| 10 | Dueño | Todos los dispositivos escriben como el mismo dueño, así que cada uno puede cambiar las filas de los otros. | Se mantiene con DC15: es lo buscado. |

Con un `ClientId` por dispositivo, nada de esto pasa a `INVARIANTES.md` §4, y los hallazgos 1 y 2 no necesitan arreglo.
