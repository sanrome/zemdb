# Rendimiento del servidor — Plan de la Fase 3.12

**Fecha:** 1 de Noviembre de 2026
**Tipo:** Optimización del servidor y del protocolo. Documento vivo: plan, lotes y seguimiento. La fase es chica y no tiene sub-fases: este plan baja directamente a lotes (`docs/METODOLOGIA.md` §1).
**Estado:** En curso. Las decisiones DS1 a DS8 las adoptó el dueño del proyecto en el Lote 0 (1 de Noviembre de 2026); DS7, condicionada a la medición del Lote 7. DS9 salió del Lote 4 y la adoptó el dueño el 3 de Noviembre de 2026, con el mecanismo que eligió al arrancar el Lote 4b (la distancia en bytes al inicio del grupo). DS10 salió de la medición del Lote 6; el dueño del proyecto la dejó para un Lote 6b el 6 de Noviembre de 2026 y la adoptó al arrancarlo, el 7, con tramos de 256 KiB y sin tramos en una lectura después de uno que no entra. DS7 la adoptó el dueño del proyecto con la medición del Lote 7, el 10 de Noviembre de 2026, con N = 10 s y otro criterio, el 11 le sumó una fase por sala, y el 12, con la revisión de delta, que cada sala escriba en los puntos de su propia grilla. El Lote 8 midió la fase entera y corrió su revisión de cierre, que no salió en verde: el dueño del proyecto eligió, el 12 de Noviembre de 2026, arreglar sus dos rupturas en dos lotes nuevos, el 8b y el 8c, con los que cierra la fase. El Lote 8b arregló la de INV-B1 y W1 con la opción que eligió el dueño del proyecto al arrancarlo, el 13 de Noviembre de 2026: la búsqueda después de un grupo dañado lee de cada frame solo dónde empieza su grupo, sin decodificar sus operaciones. El contrato está en la versión 33 de `INVARIANTES.md`.
**Origen:** las Evaluaciones 1 y 2 del [plan de evaluaciones de performance](2026-10-evaluaciones-performance.md) (§3.7, §3.8, §4.7 y §4.8).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).
**Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md), con lo propio de este plan en §5: la medición antes y después de cada lote y los checklists de cada tipo de lote.
**Lo que toca del SDK:** las decisiones DC24 a DC26 del [plan de la Fase 4](2026-10-fase4-plan.md), en sus sub-fases 4.3 y 4.4.
**Lugar en el roadmap:** en paralelo a la Fase 4. Los Lotes 0 a 5 van antes de que arranque 4.3, que se construye sobre el SSE nuevo (DS4, DS5); 4.4 puede ir a la vez.

---

## 0. Resumen

- **Qué.** Sacar los cuellos de botella del servidor que midieron las evaluaciones, y cambiar cómo se entera un cliente de lo que escriben los demás:
  - el presupuesto de respuestas, que rechaza respuestas chicas porque reserva un frame entero por respuesta (DS1);
  - `block_in_place`, que agota el pool de hilos de Tokio con cientos de salas (DS2);
  - un `fsync` por commit, que limita una sala a unos 5.400 commits por segundo en disco (DS3);
  - el SSE, que pasa a poder apagarse (DS4) y a traer las operaciones en lugar de una señal (DS5);
  - la copia de las operaciones en cada catch-up (DS6), y lo que cuesta, una vez por cada operación de cada catch-up, reservar su lugar en el presupuesto de respuestas y volver a medirla (DS10, que salió del Lote 6);
  - el roster en disco cada segundo por sala activa, si una medición en un disco de nube muestra que pesa (DS7).
- **Por qué.** Con la configuración de fábrica, un servidor de 2 núcleos sostiene unas 500 salas de 10 usuarios. Los dos primeros problemas le quitan la mitad de la capacidad real, y el SSE de señales gasta 2,5 veces más CPU por commit que uno que trae las operaciones (§1).
- **Qué no.** Varias operaciones por commit (el outbox), una conexión WebSocket, un WAL compartido entre salas, la replicación y la durabilidad relajada (`ROADMAP.md` §3), otro asignador de memoria, la cantidad de archivos de segmento y la pausa de 5 s que no se repitió (§2.2).
- **Cómo se mide.** Con el arnés de las evaluaciones, que el Lote 1 trae a `main` en `tools/perf`. Cada lote mide su escenario antes y después, en la misma máquina, y cierra solo si cumple su criterio sin empeorar los escenarios de control (DS8, §5).
- **Cómo se audita.** Revisión de delta en cada lote y revisión de cierre de la fase en el Lote 8 (`docs/METODOLOGIA.md` §6). Los lotes que tocan la durabilidad recorren además el checklist de crash-safety y corren las imágenes de crash.
- **La meta,** en la máquina de las evaluaciones y con 10 usuarios por sala que escriben cada 10 s: 1.000 salas con la propagación por debajo de 10 ms en p99 con la configuración de fábrica, y, con el evento `ops`, menos de 0,5 ms de CPU del servidor por commit.

---

## 1. Punto de partida

Lo que midieron las evaluaciones, en una VM de 4 vCPU con el servidor en 2 núcleos:

| Hallazgo | Medido | Dónde |
|---|---|---|
| Capacidad de fábrica | 500 salas de 10 usuarios (500 commits/s) con la propagación por debajo de 35 ms en p99; con 750 salas, de 0,7 a 4,6 s; con 1.000, de 9 a 14 s | §4.7.1 |
| El presupuesto de respuestas | Hasta 900.000 `503` por corrida con 1.000 salas, en respuestas de pocos KB; aparecen desde 100 salas; con 4 GiB de presupuesto vuelven a aparecer con 1.500 salas | §4.7.3, §4.8.2 |
| `block_in_place` | De 411 a 515 hilos con cientos de salas (el máximo de Tokio es 512), y las latencias colapsan con el CPU sin llenarse; sin él, 3 hilos, de un 15% a un 60% más de rendimiento sin disco y la mitad de la memoria | §3.7.4, §4.7.3 |
| Los dos juntos | Sin ellos, 1.000 salas pasan de 9 s a 5,8 ms de propagación en p99, y el techo pasa a ser el CPU, entre 1.000 y 1.500 salas | §4.7.1 |
| Hacer el `fsync` en el worker | En disco real, 1.000 salas colapsan igual (783 ms en p50): bloquea a los dos workers | §4.7.2 |
| Un `fsync` por commit | Una sala no pasa de unos 5.400 commits/s en disco, con cualquier concurrencia | §3.7.2 |
| La copia del catch-up | El 31% del CPU del actor con 64 pedidos en vuelo | §3.7.3 |
| El roster | Dos `fsync` por sala activa por segundo, el doble que los commits con un commit por segundo por sala; entre 7 y 9 escrituras al disco por commit | §4.7.3 |
| El SSE | Con 1.000 salas, 1,16 ms de CPU por commit con señales, 0,46 ms con las operaciones en el evento, 0,35 ms sin SSE | §4.8.1 |

**Lo que no se sabe todavía:** cómo se comporta todo en un disco de nube con un límite de operaciones por segundo (DS7 depende de eso), y cuánto ganaría seguir procesando comandos mientras corre un `fsync` (fuera de este plan, §2.2).

---

## 2. Alcance

### 2.1. Entra
- **El arnés de medición en `main`** (DS8, Lote 1).
- **El presupuesto de respuestas** (DS1, Lote 2).
- **La E/S de los actores fuera de los workers de Tokio** (DS2, Lote 3).
- **El *group commit*** (DS3, Lote 4).
- **El SSE:** apagarlo (DS4) y el evento `ops` (DS5), los dos del lado del servidor y del protocolo (Lote 5). El cliente que los consume es de la Fase 4 (DC24 a DC26).
- **El catch-up sin copias** (DS6, Lote 6), y **la reserva por tramos** (DS10, Lote 6b).
- **El roster**, si la medición en un disco de nube lo justifica (DS7, Lote 7).

### 2.2. No entra
- **Varias operaciones por commit** (el outbox, `ROADMAP.md` §3) y **una conexión WebSocket:** cambian el protocolo de escritura, y con el evento `ops` y el `periodic` del SDK la difusión deja de ser lo caro.
- **Seguir procesando comandos mientras corre un `fsync`** (*pipelining*): el *group commit* de DS3 espera el `fsync` del grupo antes de atender el próximo comando. Lo que ganaría hacerlo se mide después de este plan.
- **Un WAL compartido entre salas:** hace falta cerca del techo del modelo y en discos de nube (§4.8.3 de las evaluaciones), no ahora.
- **La replicación y la durabilidad relajada** (`ROADMAP.md` §3, punto 18).
- **Otro asignador de memoria, la cantidad de archivos de segmento** (uno cada 1.000 operaciones) **y la pausa de 5 s** de la Evaluación 1: quedan como próximos pasos del plan de evaluaciones (§6).

---

## 3. Decisiones

Prefijo **DS**. Se escribieron como *Propuestas*, y el dueño del proyecto las adoptó en el Lote 0 (1 de Noviembre de 2026), con los números de DS5 que fijó ese lote.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **DS1** | El presupuesto de respuestas reserva lo que se lee, no un frame entero | 2 | Adoptada (1 de Noviembre de 2026) |
| **DS2** | La E/S de los actores, en un pool fijo de hilos dedicados | 3 | Adoptada (1 de Noviembre de 2026) |
| **DS3** | *Group commit* sin espera, por sala | 4 | Adoptada (1 de Noviembre de 2026) |
| **DS4** | El SSE se puede apagar | 5 | Adoptada (1 de Noviembre de 2026) |
| **DS5** | El evento `ops`: el SSE trae las operaciones | 5 | Adoptada (1 de Noviembre de 2026) |
| **DS6** | El catch-up comparte las operaciones del búfer en RAM | 6 | Adoptada (1 de Noviembre de 2026) |
| **DS7** | El roster, menos seguido, si la medición lo justifica | 7 | Adoptada (1 de Noviembre de 2026), condicionada a la medición del Lote 7; con ella, el dueño del proyecto la adoptó el 10 de Noviembre de 2026, con N = 10 s y otro criterio |
| **DS8** | El arnés en `main` y la medición de cada lote | 1 | Adoptada (1 de Noviembre de 2026) |
| **DS9** | El último grupo de commits dañado es una escritura rota, como el último registro | 4b | Adoptada (3 de Noviembre de 2026; el mecanismo, al arrancar el Lote 4b) |
| **DS10** | La reserva de una lectura, por tramos, y sin una segunda medición | 6b | Adoptada (7 de Noviembre de 2026, al arrancar el Lote 6b) |

### DS1 · El presupuesto de respuestas reserva lo que se lee
- **Hoy.** El actor reserva un frame entero (16 MiB, `reserve_frame`) antes de leer el log para un `/sync` o el catch-up de un commit, y el handler HTTP achica la reserva al tamaño real recién al codificar la respuesta (`budgeted_response`). Con el presupuesto de fábrica (256 MiB) caben 16 respuestas en ese intervalo en todo el servidor; las demás reciben `503`, y un commit se confirma sin su catch-up.
- **Propuesta.** El actor reserva a medida que lee: por cada operación, lo que ocupa en memoria (`max(wire, 24 × valores)`, la misma medida que el presupuesto de lectura) más lo que ocupará codificada. Si la reserva no puede crecer, corta el lote ahí y lo devuelve con `has_more`, en lugar de un `503`. El `503` queda para cuando no entra ni la primera operación. El catch-up de un commit hace lo mismo: lleva las operaciones que entren, en lugar de ninguna. El handler sigue achicando la reserva al tamaño codificado, como hoy.
- **Lo que no cambia:** la cota total (`max_inflight_response_bytes`) y la parte de cada cliente (`max_inflight_response_bytes_per_client`, INV-B5), que pasan a contarse por lo que se lee y no por frames.
- **Alternativas descartadas.** Un presupuesto más grande: con 4 GiB volvieron los `503` con 1.500 salas, y deja de acotar la memoria. Codificar en el actor: pasa trabajo de CPU al único hilo de la sala.
- **Invariantes:** INV-B1 (cómo se sostiene: la reserva por operación en lugar del frame), INV-B5, INV-L1.

### DS2 · La E/S de los actores, en un pool fijo de hilos dedicados
- **Hoy.** `blocking_io` corre la E/S de cada actor con `tokio::task::block_in_place`, que traspasa su worker a otro hilo del pool de bloqueo mientras dura la E/S. Con cientos de salas, el pool crece hasta su máximo y los traspasos esperan. Correr la E/S en el worker tampoco sirve en un disco real: bloquea a los demás actores del worker.
- **Propuesta.** Un pool de hilos dedicados, de tamaño fijo (`ZEMDB_IO_THREADS`, con un valor de fábrica que fija la medición del Lote 3), al que el actor le manda cada operación de archivo y cuyo resultado espera de forma asíncrona. Ningún worker de Tokio hace E/S de archivos, y la cantidad de hilos del proceso no depende de la cantidad de salas. Las operaciones de una sala siguen en el mismo orden, porque el actor espera cada una antes de mandar la siguiente.
- **Lo que exige.** Lo que usa la E/S (el handle de `active.wal`, sus buffers) viaja al pool y vuelve, o se comparte con un `Arc`. Una operación que ya se mandó termina, y su resultado se espera, antes de que la sala suelte su lock, se detenga o se reabra (INV-L7), también en el apagado (INV-L5). La compresión a cold, que hoy usa `spawn_blocking`, pasa al mismo pool o a uno propio con su tope.
- **Alternativas descartadas.** `spawn_blocking`: crece igual que el pool de hoy. `io_uring`: solo Linux, y no hace falta para lo medido.
- **Invariantes:** INV-D1, INV-D7, INV-L3, INV-L5, INV-L7, INV-B3 (los hilos, acotados por la configuración), INV-B4.

### DS3 · *Group commit* sin espera, por sala
- **Propuesta.** Cuando el actor toma un commit de su buzón, toma también los commits que esperan detrás de él, hasta el primer comando que no es un commit y hasta un tope (de operaciones y de bytes, que fija el Lote 4). Los valida y les asigna secuencia en orden, uno por uno, como hoy; escribe sus registros en `active.wal` como hoy (un registro por commit, sin cambiar el formato); hace **un solo `fsync`** por el grupo, en el pool de DS2; y recién después responde a todos, con su catch-up, y emite los eventos SSE. No hay espera a propósito: con poca carga el grupo es de uno, como hoy.
- **Las reglas que se mantienen:** nada se confirma, se deduplica, se sirve en un catch-up o un `/sync`, ni se avisa por SSE antes del `fsync` que lo cubre (INV-D1, INV-D7). Un commit rechazado dentro del grupo no deja rastro y no frena a los demás (INV-Y5). Dos commits del grupo con el mismo `MutationId`: el segundo recibe la secuencia del primero (INV-Y4). Si el `fsync` falla, todos los commits del grupo reciben `Unavailable` y la sala se detiene, como hoy (INV-L3).
- **Lo que cambia:** el sellado de `active.wal` pasa al final del grupo que alcanza `ram_max_ops` o `segment_max_bytes`, así que un segmento puede pasarse de esos topes por un grupo (`ARCHITECTURE.md` §5.2).
- **Invariantes:** INV-D1, INV-D2, INV-D7, INV-Y1, INV-Y4, INV-Y5, INV-L3, INV-B4.

### DS4 · El SSE se puede apagar
- **Propuesta.** `ZEMDB_MAX_SSE_STREAMS=0` apaga el SSE (hoy el mínimo es 1). `GET /rooms/:room_id/events` responde entonces con un código de error propio, nuevo, al final de `ErrorCode` (por ejemplo, `EventsDisabled`, con HTTP 404), para que el SDK pase a `periodic` (DC24) en lugar de reintentar. El resto del protocolo no cambia: el SSE ya es opcional, y las respuestas a commits, `/sync` y heartbeats son la fuente de verdad.
- **Invariantes:** INV-B3, INV-B5, INV-L1, INV-W1 y INV-W2 (el código nuevo).

### DS5 · El evento `ops`: el SSE trae las operaciones
- **Propuesta.** Cuando un commit (o un grupo, con DS3) se vuelve durable, el actor emite un evento `ops` con sus operaciones, en lugar de `head_advanced`, si ocupan hasta un tope: **4 KiB**, medidos sobre el frame codificado antes del base64 (unos 5,5 KiB en el evento), que fijó el Lote 0. El evento lleva un frame del protocolo en base64 (SSE es texto), con un mensaje nuevo de `ServerMessage`, al final, que trae la sala, las operaciones, el head y el esquema de esas operaciones (`schema_id` y `schema_version`, INV-E3). Se codifica una sola vez y lo comparten todos los streams de la sala. Si las operaciones pasan el tope, el evento es `head_advanced`, como hoy, y el cliente hace `/sync`. Un stream que se atrasa recibe `lagged`, como hoy.
- **La memoria.** El canal de eventos de cada sala retiene hasta `EVENTS_CAPACITY` (256) eventos para el stream más atrasado; con el tope de 4 KiB, más de 1 MiB por sala con un stream que no lee (1,4 MiB ya en base64), y con el tope de streams de fábrica (la mitad de 10.000 conexiones) más de 5 GiB si cada stream es de otra sala. Por eso, como decidió el Lote 0, los eventos `ops` tienen un **tope total en bytes**, propio, en todo el servidor: `ZEMDB_MAX_SSE_OPS_BYTES` (`max_sse_ops_bytes`, 64 MiB de fábrica). Cada evento cuenta los bytes que realmente se retienen (ya en base64) mientras algún canal lo retiene (los devuelve al soltarse, cuando lo leyeron todos los streams o el canal lo pisó; el Lote 5 lo prueba por los dos caminos), y el actor, antes de emitir uno, toma sus bytes del tope; si no entran, emite `head_advanced`. INV-B3 nombra ese tope. Lo que copia cada stream al escribir el evento en su conexión es transitorio, como la escritura de cualquier respuesta.
- **Alternativas descartadas para la memoria.** Que el evento lleve referencias a las operaciones del búfer en RAM (los `Arc` de DS6) y que cada stream las codifique: multiplica el CPU por los streams de la sala, y un stream que no lee retiene igual las operaciones que el búfer ya soltó. Contar los eventos en el presupuesto de respuestas: una sola cota de memoria, pero los eventos compiten con los `/sync` y los catch-ups, que son la fuente de verdad. Dejar INV-B3 como estaba: unos 5 GiB de fábrica en el peor caso.
- **Lo que no cambia:** la autenticación del stream y su tope; las columnas cifradas siguen cifradas (las cifra el cliente); los eventos siguen siendo un atajo, y un cliente que pierde uno lo nota por el hueco en su cursor y hace `/sync`.
- **Lo que sumó la revisión de delta del Lote 5** (su nota **Hecho**): como el stream ahora trae datos, termina cuando vence su token (la fila de D72 de `INVARIANTES.md` §4 lo aceptaba porque solo notificaba), y el mensaje lleva la encarnación de la sala (`room_incarnation`), porque un token de una sala borrada abre, sin registro, el stream de la sala creada después con el mismo id.
- **Antes de la primera versión no hay compatibilidad que conservar** (`GEMINI.md`): `head_advanced` queda para las operaciones grandes, no como modo alternativo.
- **Invariantes:** INV-Y2 (lo que trae un evento es lo que está en el log, en orden, y solo lo durable), INV-D7, INV-E3, INV-B3 (el tope total de los eventos retenidos), INV-B5, INV-A1, INV-W1, INV-W2.

### DS6 · El catch-up comparte las operaciones del búfer en RAM
- **Propuesta.** El búfer en RAM (`HotBuffer`) guarda cada operación en un `Arc`, y el catch-up y el `/sync` arman sus respuestas con esos `Arc` en lugar de copias. Las respuestas se codifican con un tipo del servidor que serializa exactamente los mismos bytes que el `ServerMessage` de hoy (serde serializa un `Arc<T>` como `T`).
- **Invariantes:** INV-B1 (la reserva de DS1 sigue contando cada operación de cada respuesta, aunque se comparta), INV-W2 (los mismos bytes), INV-Y2.

### DS7 · El roster, menos seguido, si la medición lo justifica
- **Hoy.** El tick rápido (1 s) reescribe el roster de cada sala si cambió algún cursor, con una escritura atómica de dos `fsync` (el temporal y el directorio). INV-R3 acepta perder hasta ~1 s de cambios del roster en un corte.
- **Propuesta, condicionada.** El Lote 7 mide primero la Evaluación 2 en un disco de nube con límite de operaciones por segundo. Si el roster pesa (si las escrituras por segundo del disco limitan antes que el CPU), el roster se escribe cada N segundos (`ZEMDB_ROSTER_PERSIST_SECS`, con un valor de fábrica que fija la medición) en lugar de cada tick, y INV-R3 pasa de "~1 s" a "~N s". Se mantiene que el roster se persiste antes de cualquier poda que borre (D56). Si no pesa, DS7 se descarta y el lote cierra solo con la medición.
- **La medición del Lote 7** (9 de Noviembre de 2026, sobre `efb3ec4`): E2-disco en un disco de nube emulado, con 500 y 1.000 salas, sin límite y con 3.000 y 1.000 operaciones de escritura por segundo, contra un diagnóstico que escribe el roster cada 10 s (la nota **Hecho** del Lote 7). Desde los Lotes 3 a 6, un commit cuesta unas 4 escrituras al disco, no las 7 a 9 de la Evaluación 2, y el roster es el 30% de ellas (con un commit por segundo por sala); el resto es del commit (`active.wal`, el journal de ext4 y sus flush). Pero con 3.000 operaciones por segundo, 1.000 salas colapsan escribiendo el roster en cada tick (815 de 1.001 commits por segundo, 19 s en p99) y lo sostienen escribiéndolo cada 10 s (0,2 a 0,5 s en p99).
- **Adoptada** por el dueño del proyecto el 10 de Noviembre de 2026, con esa medición: el tick rápido escribe un roster que cambió como mucho una vez cada `roster_persist_secs` (`ZEMDB_ROSTER_PERSIST_SECS`, 10 s de fábrica, de 1 a 300), y la poda que borra y la parada lo escriben siempre. INV-R3 pasa de "~1 s" a "~`roster_persist_secs`". El criterio del Lote 7 cambia, porque el de antes (las escrituras por commit, a menos de la mitad) suponía que el roster era la mayor parte de ellas (§4, Lote 7). El 11 de Noviembre de 2026, con la primera medición del lote, el dueño del proyecto sumó que la primera escritura del roster de cada sala caiga en un punto de su primer período que sale de su encarnación: las salas que abren juntas (en la medición, y después de reiniciar el servidor) escribían el roster en el mismo tick, período tras período, y con 3.000 escrituras por segundo y 500 salas esa ráfaga llevaba el commit de 4 a 30–46 ms en p99. El 12 de Noviembre de 2026, con la revisión de delta, el dueño del proyecto sumó que cada sala escriba en los puntos de su propia grilla (la fase y después cada período), y que una escritura fuera de ella (la de una poda que borra, o la de un cambio después de un período sin cambios) no la mueva: si no, las salas que pasan un período quietas a la vez, o que podan juntas en el tick lento, volvían a escribir en el mismo tick.
- **Invariantes:** INV-R3, INV-R1, INV-D4.

### DS8 · El arnés en `main` y la medición de cada lote
- **El arnés:** el generador de las evaluaciones y sus scripts, en `tools/perf`, fuera del workspace (DP1 del plan de evaluaciones), con un `README` que dice cómo correr cada escenario. Los cambios de diagnóstico del servidor (sin `block_in_place`, el presupuesto agrandado, el prototipo del evento `ops`) van como parches en `tools/perf/diagnostics/`, para repetir las comparaciones, y no entran al código del servidor.
- **La medición de cada lote:** un escenario propio, con su criterio de aceptación, y los escenarios de control (§5.2). La línea de base se mide en la misma sesión que el resultado, sobre el commit anterior al lote, porque la máquina de las evaluaciones es efímera y otra máquina da otros números (DP3).
- **Invariantes:** ninguna; no toca el servidor.

### DS9 · El último grupo de commits dañado es una escritura rota, como el último registro
Salió del Lote 4, y la adoptó el dueño del proyecto el 3 de Noviembre de 2026, con el Lote 4b para implementarla.
- **Desde el Lote 4.** Un corte de luz (o una caída del kernel) durante el `fsync` de un grupo puede persistir sus sectores fuera de orden: uno posterior llega al disco y uno anterior no. La apertura encuentra un daño seguido de frames válidos del mismo grupo, lo toma por corrupción (INV-D3) y la sala no abre hasta que alguien trunque `active.wal` a mano. Ningún commit del grupo se había confirmado, así que no se pierde nada confirmado, pero la sala queda caída. Antes del grupo no podía pasar: cada `fsync` cubría un registro, solo el último podía quedar roto, y no había ningún frame válido después (D78). El riesgo queda aceptado en `INVARIANTES.md` §4 hasta el Lote 4b.
- **Cuánto pesa** (medido en el Lote 4, en su nota **Hecho**): una sala que recibe commits a la vez tiene un grupo de varios registros escrito y sin sincronizar entre el 20 y el 56% del tiempo (E1-disco con 64 y 8 pedidos en vuelo); una sala a ritmo humano, casi nunca (E2: grupos de uno). Si el disco pierde sectores del grupo fuera de orden, la apertura falla en el 25% de los casos con un grupo de 2 registros y en casi todos con uno de 30. Que el disco los pierda fuera de orden depende del sistema de archivos y del disco, y no se pudo medir sin un dispositivo que simule cortes.
- **Propuesta.** Cada registro de `active.wal` dice dónde empieza su grupo: a cuántos bytes de él (0 en el primer registro del grupo). Al abrir, un daño seguido solo de frames válidos de su mismo grupo (cuyo grupo empieza en el daño o antes), que es el último de `active.wal`, es una escritura rota: se trunca desde el daño, como hoy el último frame dañado (D78). Si después del daño aparece un frame cuyo grupo empieza después del daño, los bytes dañados eran de un grupo anterior, que se había sincronizado (un grupo se escribe recién después del `fsync` del anterior), y es corrupción: la apertura falla, como hoy.
- **El mecanismo, elegido por el dueño del proyecto al arrancar el Lote 4b** (3 de Noviembre de 2026). La versión adoptada con el Lote 4 decía la secuencia del primer registro del grupo, y después se propuso un bit por registro, "continúa el grupo"; se eligió la distancia en bytes porque es la única de las tres que decide todos los casos con los bytes de `active.wal` solos:
  - **Un bit** no ve dónde empieza el grupo: un daño que toma el último registro de un grupo sincronizado y el primero del último grupo, con el resto de este intacto, deja después del daño solo registros que "continúan", y la apertura truncaría un commit confirmado de un grupo que no es el último.
  - **La secuencia** resuelve ese caso comparándola con la del registro dañado, que es la del último registro válido más uno; pero si el daño empieza al principio de `active.wal` no hay registro válido antes, y la secuencia del dañado no se sabe sin otro archivo.
  - **La distancia en bytes** da la posición del inicio del grupo, que se compara con la del daño, que la apertura siempre conoce.
- **El costo aceptado:** si el disco daña el último grupo después de su `fsync` (una falla del medio, no un corte), se truncan hasta un grupo de commits confirmados (64 commits, o 1 MiB de operaciones más la que lo alcanza) en lugar de un registro: la excepción de D78 pasa del último registro al último grupo, con la misma justificación (no se distingue de una escritura rota).
- **Lo que exige:** un cambio del formato del registro del WAL del servidor (antes de la primera versión no hay compatibilidad que conservar, `GEMINI.md`): 8 bytes más por registro. El WAL del storage escribe un frame por `fsync`, así que no lo necesita; como comparte el formato del frame, sus frames llevan el campo en 0 (un grupo de uno), y sus reglas no cambian.
- **Alternativas descartadas.** Aceptar el riesgo para siempre: el grupo sirve justamente a las salas con commits a la vez, que son las expuestas. Tratar como escritura rota cualquier daño seguido de frames válidos: truncaría commits confirmados ante una corrupción real (INV-D1). Un grupo en un solo frame: el payload lleva un solo `mutation_id`, así que también cambia el formato, y más.
- **Invariantes:** INV-D2 (la excepción de D78), INV-D3, INV-D1, INV-D8, INV-D10.

### DS10 · La reserva de una lectura, por tramos, y sin una segunda medición
Salió de la medición del Lote 6 (su nota **Hecho**). El dueño del proyecto eligió, el 6 de Noviembre de 2026, cerrar el Lote 6 con su criterio sin cumplir y dejar esto para un Lote 6b, y adoptó la decisión al arrancarlo, el 7 de Noviembre de 2026: la reserva por tramos, con tramos de 256 KiB y sin tramos en una lectura después de uno que no entra, y sin la segunda medición.
- **Desde el Lote 6.** Con 64 pedidos en vuelo, cada commit de un grupo trae un catch-up de hasta unas 64 operaciones (el protocolo de 1 RTT), así que lo que se hace por operación de cada catch-up crece con el cuadrado del grupo. Sin la copia (DS6), quedan dos costos así:
  - **la reserva de la respuesta** (DS1) crece una vez por operación (`ReadBudget::admit`, que llama a `InflightBudget::take`: un mutex, la parte del cliente en un `HashMap` y el semáforo del total), el 21% del CPU de E1-actor con 64;
  - **`fit_in_response` vuelve a medir** cada operación con `encoded_len` (~9%), aunque la lectura ya las acotó: el presupuesto de lectura cobra `max(wire, 24 × valores)`, que nunca es menos que el tamaño codificado.
- **Propuesta.**
  - **La reserva por tramos:** una lectura hace crecer la reserva de su respuesta de a tramos (256 KiB, o lo que le falte a la operación si es más), descuenta de lo reservado lo que toma cada operación que admite y, al terminar, devuelve lo que no usó, también si termina con un error. Si un tramo no entra, la reserva crece exactamente lo que le falta a la operación, como hoy, y así el resto de esa lectura, que no vuelve a intentar un tramo (con el presupuesto casi lleno, una lectura hace como mucho un intento de más): la lectura se sigue cortando en la primera operación que no tiene lugar, y cada respuesta sigue contando todas sus operaciones. Pasado el tope de un frame, la reserva crece menos que lo pedido, y solo lo que creció cuenta como reservado de más.
  - **Lo que cambia:** mientras dura una lectura (microsegundos en la RAM, milisegundos en disco), su respuesta retiene hasta un tramo más de lo que usa, en la parte de su cliente y en el total: unos 5 MiB con la configuración de fábrica y 4 núcleos (las lecturas que pueden ir a la vez son una por worker de Tokio, las de la RAM, y una por hilo del pool de E/S, las de disco, 16 de fábrica), sobre 256 MiB. Con el presupuesto casi lleno, otra respuesta puede salir un poco más corta, o con `Unavailable`, un poco antes.
  - **Sin la segunda medición:** `fit_in_response` deja de medir las operaciones de una lectura del log, que ya cumplen el tope de una respuesta, con un test que lo fija. Como sus dos llamadas (el catch-up y el `/sync`) reciben siempre una lectura del log, la función sale.
- **Lo medido con un prototipo** (fuera del repo; la nota **Hecho** del Lote 6): E1-actor con 64 en vuelo pasa de unos 32.000 commits por segundo a unos 60.000 con la reserva por tramos sola, 39.000 sin la segunda medición sola y 98.000 con las dos; con 8, de unos 60.900 a 62.900 con las dos.
- **Alternativas, para el Lote 6b.** Reservar una sola vez al final de la lectura, con lo que suman las operaciones admitidas, y volver a la reserva por operación si no entra: no retiene de más, pero con el presupuesto casi lleno la lectura hace el trabajo dos veces. Un tramo más chico: retiene menos de más, y visita más el presupuesto.
- **Invariantes:** INV-B1 (cómo se cuenta la reserva), INV-B5 (la parte de cada cliente, que una lectura en curso ocupa hasta un tramo de más).

---

## 4. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DS1 a DS8 | Ajustes: B1, B3, B4, B5, D1, D7, E3, L7, Y2 (R3, solo con DS7) | ✅ |
| 1 | El arnés en `main` y la línea de base | `tools/perf` (fuera del workspace) | DS8 | — | ✅ (la línea de base, medida con el Lote 2) |
| 2 | El presupuesto de respuestas | server | DS1 | B1, B5, L1 | ✅ |
| 3 | La E/S fuera de los workers | server | DS2 | D1, D7, L3, L5, L7, B3, B4 | ✅ |
| 4 | *Group commit* | server | DS3 | D1, D2, D7, Y1, Y4, Y5, L3, B4 | ✅ |
| 4b | El último grupo dañado | core, server | DS9 | D1, D2, D3, D8, D10 | ✅ |
| 5 | El SSE: apagarlo y el evento `ops` | core, server | DS4, DS5 | Y2, D7, E3, B3, B5, A1, W1, W2 | ✅ |
| 6 | El catch-up sin copias | server | DS6 | B1, W2, Y2 | ✅ (sin cumplir su criterio: lo sigue el 6b) |
| 6b | La reserva por tramos | server | DS10 | B1, B5 | ✅ |
| 7 | El roster | server | DS7 | R3, R1, D4 | ✅ |
| 8 | Medición final y revisión de cierre | Documentos | Todas | Las que tocó la fase | ✅ (su revisión de cierre no salió en verde: la siguen el 8b y el 8c) |
| 8b | La búsqueda después de un grupo, sin decodificar las operaciones | core | — | B1, W1 | ✅ |
| 8c | Una sala que abre después de que venció su pedido | server | — | L1 | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirmaron en el Lote 0, después del Lote 1, que se adelantó (su nota **Hecho**).
- El 1 va antes de cualquier optimización: sin el arnés en `main` no hay medición.
- El 2 no depende de los demás. El 3 va antes del 4, que hace su `fsync` en el pool del 3. El 4b va después del 4 y antes del 5: cierra el riesgo que abrió el 4. El 5 va antes de que arranque la sub-fase 4.3 de la Fase 4, y después del 4, porque con el *group commit* un evento puede llevar las operaciones de un grupo. El 6, después del 2, porque cambia lo que reserva una respuesta. El 6b, después del 6, de cuya medición salió. El 7 depende de la medición en un disco de nube y puede quedar para el final. El 8 mide la fase y la revisa; el 8b y el 8c arreglan lo que encontró su revisión de cierre, y el 8c cierra la fase.

**Tamaño estimado de producción:** el Lote 2, de 100 a 150 líneas; el 3, de 200 a 300; el 4, de 150 a 250; el 4b, de 100 a 200; el 5, unas 200 entre core y servidor (con el tope total de los eventos `ops`); el 6, unas 80; el 6b, unas 40; el 7, unas 50; el 8b, unas 15; el 8c, unas 40. El 1 no tiene código de producción.

---

## 5. Metodología de cada lote

Cada lote sigue el procedimiento de `docs/METODOLOGIA.md` §5, pasos 0 a 7, con la estructura de los planes de la Fase 4 (objetivo, archivos, qué se hace, tests exigidos, interacciones a revisar, documentos, entrega y nota **Hecho**). Este plan agrega tres cosas: la medición antes y después (§5.2), los checklists de cada tipo de lote (§5.3) y una pregunta más para la revisión de delta (§5.4).

### 5.1. El procedimiento, paso por paso
0. **Arranque** (`docs/METODOLOGIA.md` §5, paso 0), y además la **línea de base**: compilar en release el commit anterior al lote y correr el escenario del lote y los de control (§5.2), con 3 repeticiones, en la misma máquina en que después se mide el resultado. Si el lote anterior cerró en la misma sesión, su resultado es la línea de base.
1. **Primero los tests exigidos**, que fallan por el comportamiento.
2. **La implementación mínima.**
3. **Verificación:** los chequeos de `docs/METODOLOGIA.md` §4, regla 4; la mutación inversa de cada regla nueva; las versiones largas de los crates tocados (`cargo test -p zemdb-server --release -- --ignored`, y las de `zemdb-core` en el Lote 5); y el checklist del tipo de lote (§5.3).
4. **Medición:** el escenario del lote y los de control, con 3 repeticiones, contra la línea de base. El lote cierra solo si cumple su criterio y ningún escenario de control empeora más que su dispersión (la diferencia entre repeticiones) más un 10%. Si no lo cumple, se busca por qué con un perfil de CPU (`perf`, como en las evaluaciones) antes de seguir; un criterio que no se cumple y se acepta igual queda como desvío en la nota **Hecho**, con el visto bueno del dueño del proyecto.
5. **Revisión de delta** (`docs/METODOLOGIA.md` §5, paso 4), con la pregunta de §5.4.
6. **Documentos, en el mismo cambio:** `ARCHITECTURE.md`, `INVARIANTES.md` y este plan. La nota **Hecho** suma una tabla de la medición (antes y después, con el entorno de DP3 y los dos commits).
7. **Alcance y entrega** (`docs/METODOLOGIA.md` §5, pasos 6 y 7), con commits del estilo `perf(server): … (Fase 3.12, Lote N)`.

### 5.2. Los escenarios
Todos con el arnés de `tools/perf` y las condiciones de DP3; los nombres son los de las evaluaciones.

| Escenario | Qué mide | Dónde se definió |
|---|---|---|
| **E1-tmpfs** | Una sala por HTTP en tmpfs, con 1, 8 y 64 pedidos en vuelo | 1a/1b, §3.4 de las evaluaciones |
| **E1-disco** | Lo mismo en disco real, con 1, 8 y 64 | 1c |
| **E1-actor** | Directo al actor, sin HTTP, con 1, 8 y 64 | 1d |
| **E2-tmpfs** | Salas de 10 usuarios a ritmo humano, en tmpfs, con 500 y 1.000 salas | 2c, §4.4 |
| **E2-disco** | Lo mismo en disco real, con 500 y 1.000 salas | 2d |
| **E2-modos** | Los modos de frescura a, c y d con 1.000 salas | 2f, §4.8 |

**Escenarios de control,** que corre todo lote además del suyo: E1-tmpfs con 1 y 8 pedidos en vuelo, y E2-tmpfs con 500 salas.

**tmpfs es un diagnóstico, el objetivo es el disco.** Los escenarios en tmpfs sacan el costo del disco para medir el resto de la cadena (el actor, HTTP, el SSE); el caso de uso es el servidor sobre un disco real, y un criterio en tmpfs dice qué queda por resolver fuera del disco, no si el servidor rinde en producción.

### 5.3. Los checklists de cada tipo de lote
- **De crash-safety** (§2 del [plan de 4.1](2026-10-fase4_1-storage-servidor.md)), en los Lotes 3, 4 y 7, que tocan cómo se escribe al disco: ninguna operación de archivo cambia de orden ni de lugar respecto de los locks; una operación soltada nunca aterriza después de que la sala se cierra o se reabre; cada `sync` sigue sobre el mismo archivo o directorio, y ningún resultado de E/S se descarta; cada fail point sigue en el mismo paso, con el mismo nombre; las imágenes de crash pasan sin cambios en sus expectativas.
- **De concurrencia del servidor,** nuevo, en los Lotes 3 y 4:
  - ningún worker de Tokio hace E/S de archivos (ni `block_in_place`, ni E/S en el lugar);
  - cada `.await` del actor es un punto de corte: una operación que ya se mandó al pool termina, y su resultado se espera, antes de que la sala suelte su lock, se detenga o se reabra (INV-L7), y también en el apagado (INV-L5);
  - la cantidad de hilos del proceso no depende de la cantidad de salas (un test con cientos de salas que la cuenta);
  - toda request sigue respondiendo en tiempo acotado (INV-L1), también con el pool lleno.
- **Del protocolo,** en los Lotes 5 y 6: las variantes nuevas van al final de su enum; la ida y vuelta de cada mensaje nuevo y su lugar en los fuzzers de mensajes (INV-W1, W2); y, donde el plan promete los mismos bytes (DS6), un test que los compara.
- **De la medición,** en todos los lotes desde el 1: línea de base y resultado en la misma máquina, con el mismo binario del generador; corridas válidas según DP3 (sin errores, el head igual a lo confirmado, el generador por debajo del 90% de sus núcleos); y el entorno anotado.

### 5.4. La pregunta de más para la revisión de delta
Además de las cinco de `docs/METODOLOGIA.md` §5, paso 4: **6. ¿La medición está hecha según DP3 y §5.2, y respalda el criterio del lote?** El revisor recibe también la tabla de la medición y los datos crudos.

---

### Lote 0 · Contrato · ✅ Hecho
**Objetivo:** el contrato contra el que se revisa cada lote. Solo documentos.

**Qué se hace:**
- **Este plan,** con DS1 a DS8 como *Propuestas*, para confirmar con el dueño del proyecto. Se fijan los números que las decisiones dejan abiertos: el tope del evento `ops` (DS5) y si su memoria entra en INV-B3 tal como está.
- **`INVARIANTES.md`, versión 12:**
  - **INV-B1:** "Sostenida por", con la reserva por lo que se lee en lugar del frame (DS1).
  - **INV-B5:** la parte de cada cliente, contada por lo que se lee.
  - **INV-B3:** los hilos del pool de E/S (DS2) y la memoria del canal de eventos de cada sala con el evento `ops` (DS5).
  - **INV-B4:** sin el traspaso del worker; el `fsync` de un grupo (DS3).
  - **INV-D1 e INV-D7:** "Sostenida por", con el `fsync` en el pool y el del grupo; "Cómo intentar romperla", con un crash entre la escritura de los registros de un grupo y su `fsync`.
  - **INV-L7:** las operaciones en el pool de E/S.
  - **INV-Y2:** lo que trae un evento `ops`, y solo lo durable.
  - **INV-E3:** el evento `ops` anuncia el esquema de sus operaciones.
  - **INV-R3:** solo si DS7 se adopta en el Lote 7; si no, no cambia.
  - Las invariantes que implementa cada lote quedan marcadas con su lote (🆕, `INVARIANTES.md` §1.5).
- **El plan de la Fase 4:** DC24 a DC26 como *Propuestas* (ya escritas al crear este plan), y la dependencia de 4.3 respecto del Lote 5 de este plan.
- **`ROADMAP.md`:** la Fase 3.12 en curso.
- `ARCHITECTURE.md` no cambia: describe comportamiento implementado.

**Metodología:** el Lote 0 de `docs/METODOLOGIA.md` §2, con su revisión cruzada por un agente aparte, de contexto fresco y solo lectura, que contrasta cada enunciado nuevo con el código actual.

**Entrega:** `docs: plan Fase 3.12, the performance of the server, and adjust the contract for it (Fase 3.12, Lote 0)`.

**Hecho.**
- **Las decisiones:** el dueño del proyecto adoptó DS1 a DS8 el 1 de Noviembre de 2026, DS7 condicionada a la medición del Lote 7. Los números que el lote tenía que fijar: el tope del evento `ops` es de 4 KiB del frame codificado, antes del base64; y su memoria no entra en INV-B3 tal como estaba (más de 1 MiB por sala con un stream que no lee, más de 5 GiB de fábrica), así que DS5 suma un tope total propio de los eventos retenidos, `ZEMDB_MAX_SSE_OPS_BYTES` (64 MiB de fábrica), con la caída a `head_advanced`. El Lote 5 suma esa variable, su test y unas 50 líneas.
- **`INVARIANTES.md`, versión 12:** INV-B1 y B5 (la reserva por lo que se lee, Lote 2); INV-B3 (los hilos del pool de E/S, Lote 3, y el tope total de los eventos `ops`, Lote 5); INV-B4 (la E/S en el pool y el `fsync` de un grupo, Lotes 3 y 4); INV-D1 e INV-D7 (el `fsync` en el pool y el del grupo, con el crash entre los registros y el `fsync` de un grupo en "Cómo intentar romperla"; el enunciado de D7 nombra también lo que escribe el propio actor, que hoy ya se sostiene por el `fsync` antes de avanzar el head, y su "Sostenida por" suma el evento `ops`); INV-L7 (las operaciones en el pool de E/S, que se espera antes de soltar `server.lock`); INV-Y2 (lo que trae un evento `ops`); INV-E3 (el esquema del evento `ops`); el riesgo aceptado de un `fsync` por commit, que pasa a uno por grupo (§4); y las decisiones DS en el reparto de §1.6. Cada ajuste queda 🆕 con su lote en la columna de estado del resumen. INV-R3 no cambia hasta el Lote 7.
- **El plan de la Fase 4:** DC24 a DC26 siguen *Propuestas* para 4.3 y 4.4, y la dependencia de 4.3 respecto del Lote 5 ya estaba escrita (su estado, su tabla de sub-fases y la sección de 4.3); no cambió.
- **`ROADMAP.md`:** la Fase 3.12, en curso.
- **La revisión cruzada:** un agente aparte, de contexto fresco y solo lectura, contrastó cada enunciado nuevo con el código de `2f42e21`. Sin hallazgos Altos: lo que el diff da por cierto hoy se sostiene (la frase nueva de INV-D7, por el `sync_data` de `active.wal` antes de avanzar el head, registrar el dedup y emitir el evento; las cifras de 256 eventos y 10.000 conexiones). Cinco Medios de redacción, corregidos: en INV-D7, un `MutationId` repetido dentro de un grupo se reconoce contra el propio grupo; en INV-B3, los hilos del pool bloqueante de Tokio están acotados por su máximo de fábrica, no por la configuración, y siguen ahí el relay y, si no tiene pool propio, la compresión; en INV-L7, el pool de E/S se espera antes de soltar `server.lock` (con su test y `main.rs` en el Lote 3); y el resumen y la cabecera de la versión 12 nombran la precisión del enunciado de INV-D7. Los Bajos, también corregidos: las cifras de memoria cuentan el evento ya en base64; el test del tope del Lote 5 cubre los dos caminos de liberación (que dependen de que `broadcast` suelte el valor cuando lo leyó el último receptor, que la revisión no pudo comprobar sin el código de Tokio); `schema_registry.rs` en los archivos del Lote 3, que también usa `blocking_io`; INV-L3, Y4 e Y5 en los documentos del Lote 4; D7 en los del Lote 5; y citas y estilo.
- **Desvíos:** el Lote 1 se había adelantado a este lote (su nota **Hecho**); no toca el contrato.

### Lote 1 · El arnés en `main` y la línea de base · ✅ Hecho
**Objetivo:** que cada lote pueda medir antes y después sin ramas (DS8, DP1).

**Archivos:** `tools/perf/` (nuevo): `Cargo.toml` con su propio `[workspace]`, `src/main.rs` y `src/rooms.rs` (el generador), `run_e1.sh`, `run_e2.sh`, `e2_all.sh`, `e2f_all.sh`, `profile_e1.sh`, `summarize.sh`, `summarize_e2.sh`, `README.md`, `results/` (los datos crudos de las evaluaciones) y `diagnostics/` (los parches de diagnóstico). El `Cargo.toml` de la raíz suma `exclude = ["tools/perf"]`.

**Qué se hace:** traer el generador y los scripts de las ramas `perf/e1-commit` y `perf/e2-salas`, sin los cambios de diagnóstico del servidor, que van como parches; ajustar los caminos de los scripts; `cargo fmt` y `cargo clippy` sobre el arnés; el `README`, con cada escenario de §5.2, cómo correrlo, cómo preparar la máquina (el rango de puertos locales, el límite de archivos abiertos, `perf`) y cómo leer los resultados. Después, borrar las dos ramas y `claude/evaluaciones-performance`.

**Tests exigidos:** el arnés compila desde `tools/perf` y cada escenario de §5.2 corre en su versión corta (una repetición de pocos segundos) y da una corrida válida; el workspace no cambia (`cargo test --workspace` no compila el arnés).

**Interacciones a revisar:** la CI (el arnés no entra); el `Cargo.lock` de la raíz no cambia; los parches de diagnóstico aplican sobre el commit del lote.

**Medición:** la línea de base de la fase: todos los escenarios de §5.2 sobre el commit del lote, con 3 repeticiones, en la nota **Hecho**.

**Metodología:** el procedimiento de §5.1 sin los pasos de producción (sin tests de comportamiento ni mutación inversa); el checklist de medición de §5.3. La revisión de delta mira que el arnés no toque el workspace ni el servidor.

**Documentos:** el plan de evaluaciones (DP1: el arnés ya está en `main`).

**Entrega:** `chore(perf): the measurement harness of the performance evaluations in tools/perf (Fase 3.12, Lote 1)`.

**Hecho.**
- **El arnés,** en `tools/perf`, con su propio `Cargo.toml` (`[workspace]`, las dependencias por camino a `crates/core` y `crates/server`, y el `Cargo.lock` de la raíz recortado, así que usa las mismas versiones) y `exclude = ["tools/perf"]` en el de la raíz; `/tools/perf/target/` en el `.gitignore`. El generador y los scripts vienen de `perf/e2-salas`, que contenía a `perf/e1-commit`, como un commit nuevo sobre `main`, sin merge. Los scripts toman el servidor de `target/release/` de la raíz y el generador de `tools/perf/target/release/`. `cargo fmt` y `cargo clippy -- -D warnings` sobre el arnés, sin advertencias (un `map_or(true, …)` pasó a `is_none_or`).
- **Los diagnósticos,** como parches en `tools/perf/diagnostics/`: `inline-io.patch` (sin `block_in_place`) y `sse-ops.patch` (el prototipo del evento `ops`), sacados del diff de las ramas contra su base; los dos aplican sobre `main`.
- **Los datos crudos** de las evaluaciones, en `tools/perf/results/`. En los tres archivos de la Evaluación 1 que la tenían, la lista de commits lentos de cada corrida quedó resumida (cuántos hubo y los 20 más largos): ocupaba 8 MB, y ninguna cifra de las evaluaciones sale de ella.
- **El `README`,** con cada escenario de §5.2, cómo preparar la máquina, cómo compilar y cómo usar los diagnósticos.
- **Verificación:** el workspace no cambió (`cargo metadata` lista los mismos cuatro crates, `cargo fmt --all -- --check` pasa y el `Cargo.lock` de la raíz no cambia); el arnés compila; cada escenario corrió en su versión corta (una repetición de 2 a 5 s: E1 en tmpfs con 1 y 8 pedidos en vuelo, en disco y directo al actor; E2 con SSE en tmpfs y sin SSE en disco) contra el servidor de `main`, con corridas válidas.
- **Desvíos:** el lote se adelantó al Lote 0, a pedido del dueño del proyecto, para no dejar el código en ramas; no toca el servidor ni el contrato. La línea de base de la fase no se midió: se mide al arrancar el Lote 2, en la misma sesión que su resultado (§5.1, paso 0), porque la máquina de esta sesión no es la de los lotes siguientes. Las ramas `perf/e1-commit`, `perf/e2-salas` y `claude/evaluaciones-performance` quedan para que las borre el dueño del proyecto: su contenido ya está en `main`.

### Lote 2 · El presupuesto de respuestas · ✅ Hecho
**Objetivo:** que el presupuesto acote la memoria sin rechazar respuestas chicas (DS1).

**Archivos:** `crates/server/src/response_budget.rs`, `actor/room.rs` (`reserve_frame` y quienes lo llaman: el `/sync`, el catch-up, el registro y el esquema), `log/tiered_log.rs` (la lectura con presupuesto), `api/data_plane.rs` (`budgeted_response`), y sus tests. `ARCHITECTURE.md` §5.2 (*Bounded responses*) y §7.3.

**Qué se hace:** la reserva crece con cada operación leída (lo que ocupa en memoria más lo que ocupará codificada); si no puede crecer, el lote se corta con `has_more`; el `503` queda para cuando no entra ni la primera operación; el catch-up de un commit lleva lo que entre. El registro y el esquema, que no leen el log, reservan su tamaño conocido.

**Tests exigidos.**
- Con el presupuesto casi lleno, un `/sync` devuelve un lote más corto con `has_more` en lugar de `503`, y el siguiente sigue desde ahí; un commit lleva el catch-up que entra.
- El `503` sale solo cuando no entra ni una operación, y nada cambió en el actor.
- La parte de cada cliente se respeta contando por lo que se lee (INV-B5): un cliente que no lee sus respuestas no le quita el presupuesto a otra sala.
- Muchas respuestas chicas en vuelo a la vez (más de 16) no reciben `503` con el presupuesto de fábrica.
- La memoria reservada vuelve a cero cuando terminan todas las respuestas (sin fugas de reserva), también con requests cancelados en cada `.await`.

**Interacciones a revisar:** el presupuesto de lectura del log; el tope por respuesta (`MAX_FRAME_SIZE`) y `MAX_MESSAGE_VALUES`; los streams HTTP/2 sin leer y `max_send_buf_size`; el relay, que también reserva frames para sus chunks; un commit sin catch-up (`has_more`) y lo que hace el cliente.

**Medición:** E2-tmpfs con 500 y 1.000 salas, de fábrica. **Criterio:** ningún `503` del presupuesto con 1.000 salas, y la propagación en p99 al menos tan buena como la del diagnóstico con 4 GiB de presupuesto (170 ms con 1.000 salas, §4.7.1 de las evaluaciones).

**Metodología:** §5.1, con los checklists de medición; la mutación inversa incluye volver a reservar el frame entero (los tests de muchas respuestas chicas tienen que fallar).

**Documentos:** `ARCHITECTURE.md` §5.2 y §7.3; `INVARIANTES.md`: INV-B1, INV-B5.

**Entrega:** `perf(server): reserve the response budget by what is read, not a whole frame (Fase 3.12, Lote 2)`.

**Hecho.**
- **La reserva por lo que se lee** (`response_budget.rs`, `log/budget.rs`, `log/tiered_log.rs`, `actor/room.rs`). El actor reserva primero lo que ocupa una respuesta además de sus operaciones (`RESPONSE_OVERHEAD`: el header del frame y `RESPONSE_ENVELOPE_ALLOWANCE`). Después, `ReadBudget` hace crecer esa reserva (`Reservation::grow`) por cada operación que admite, en cualquier tier, en `OpSize::in_response`: lo que la operación cobra en memoria (`op_bytes`) más su tamaño codificado. La primera operación que no entra corta la lectura, aunque sea la primera de todas, y entonces `fetch_deltas` devuelve un lote vacío con `has_more`. El `/sync` lo contesta con `Unavailable`, sin tocar el cursor; el commit, con el catch-up vacío. Un cliente al día recibe su propia operación si la reserva crece por ella. La `HotBuffer` guarda los dos tamaños de cada operación, para no recalcularlos en cada lectura. El camino de RAM ya no copia el presupuesto de lectura: si no admitió nada, no lo tocó.
- **Una reserva no pasa de un frame.** Más allá, crece gratis. Sin eso, una sola operación grande (hasta 16 MiB codificada y 24 MiB en memoria) pediría más que la parte más chica que se acepta (un frame) y no se serviría nunca. Una respuesta así cuenta como un frame, como contaba antes toda respuesta, aunque mientras se codifica puede ocupar hasta unas 2,5 veces eso. Va en INV-B1 y en `ARCHITECTURE.md` §7.3.
- **`Registered` y `Schema`** reservan, antes de copiar el esquema, su tamaño codificado más `RESPONSE_OVERHEAD`, que cubre el resto de los dos mensajes (un test lo comprueba con cada campo en su máximo), y tampoco más de un frame. El chunk de snapshot sigue reservando un frame: su tamaño lo elige el cliente y se lee del archivo después de una espera.
- **La parte de cada cliente** se busca una sola vez por reserva, y el par sala y cliente se copia solo si el cliente no tenía nada reservado (lo pidió la medición, abajo).
- **Tests agregados** (los cuatro primeros de `data_plane.rs` fallan con la reserva del frame entero):
  - `api/tests/data_plane.rs`: `a_sync_with_little_room_returns_a_shorter_batch_and_the_next_goes_on` (un `/sync` con poco lugar, el siguiente, el catch-up de un commit atrasado y el de uno al día, con y sin lugar para su operación); `a_sync_without_room_for_one_operation_is_unavailable_and_changes_nothing` (reemplaza a `sync_without_budget_does_not_move_the_cursor`); `a_client_that_does_not_read_its_responses_only_fills_its_own_part` (respuestas sin leer de 1 MiB por operación: lotes más cortos, después `Unavailable`, y otra sala servida entera mientras tanto); `many_small_responses_in_flight_fit_in_the_factory_budget` (80 syncs y 40 catch-ups retenidos a la vez, de 40 clientes y de uno); `the_budget_returns_to_zero_once_every_response_ends_or_is_cancelled` (sync, commit, registro y esquema cortados después de cada cantidad de polls); `registration_and_schema_reserve_their_known_size`; `schema_responses_reserve_at_least_what_they_take_encoded` (y nunca más de un frame); `an_operation_larger_than_a_frame_in_a_response_is_served_with_the_smallest_budget` (una operación de 10 MiB con la parte y el presupuesto de un frame); y `responses_without_room_in_the_budget_are_unavailable_before_they_are_built`, que reemplaza a `responses_take_a_frame_of_the_clients_budget_before_they_are_built`.
  - `log/tests/tiered_log.rs`: `a_read_ends_where_the_reservation_of_its_response_runs_out` (en disco y en RAM, y sin lugar para la primera).
  - `log/tests/budget.rs`: cuatro, de cómo crece la reserva y cuándo corta la lectura.
  - `tests/response_budget.rs` (nuevo): cuatro, de `grow`, el tope de un frame y la parte de cada cliente.
- **Mutaciones verificadas,** cada una con algún test que falla: reservar el frame entero antes de leer (los cuatro tests de comportamiento); no hacer crecer la reserva al leer (siete); dejarla crecer más allá de un frame; servir un `/sync` vacío sin lugar para su primera operación; tomar la falta de lugar en disco por un hueco del log; darle su operación a un cliente al día sin hacer crecer la reserva; reservar un frame en el registro o en el esquema; el tamaño del esquema sin el sobre; no controlar la parte del cliente al crecer o en la primera reserva; y admitir la primera operación sin lugar; también el tope de un frame en el registro y el esquema. Las mutaciones se corrieron contra los tests unitarios del servidor; las del tope de un frame y de la parte del cliente, también contra los de integración. La salida temprana del camino de RAM sin lugar no tenía test que fallara: el camino de disco, con el presupuesto agotado, no lee nada y devuelve lo mismo. Se sacó.
- **Interacciones revisadas:**
  - **El presupuesto de lectura del log:** la reserva se cobra en el mismo `admit`, después del tope de operaciones y de bytes, que no cambian; la primera operación, que el presupuesto de lectura siempre admite, la reserva puede rechazarla (los tests de `budget.rs` y de `tiered_log.rs`).
  - **`MAX_FRAME_SIZE` y `MAX_MESSAGE_VALUES`:** `fit_in_response` y el tope de lectura siguen igual, y `large_operations_are_split_across_catch_up_and_sync_batches` pasa sin cambios; el tope de un frame por reserva, con su test.
  - **Los streams HTTP/2 sin leer y `max_send_buf_size`:** la respuesta se sigue quedando con su tamaño codificado y le entrega copias a hyper. Los tests de `http_edge_tests.rs` (`responses_over_the_budget_in_flight_wait_for_held_bodies`, `client_over_its_part_of_the_budget_leaves_the_rest_to_other_rooms`) pasan sin cambios: una fila de 5 MiB no entra en lo que deja una respuesta retenida.
  - **El relay:** sigue reservando un frame por chunk, de la misma parte del cliente (`a_request_waiting_for_its_room_holds_none_of_the_budget`; el chunk sin lugar, antes de leer el archivo, en `responses_without_room_in_the_budget_are_unavailable_before_they_are_built`).
  - **Un commit con el catch-up vacío o parcial, y el cliente:** el SDK acepta un catch-up vacío o parcial con `has_more` (la validación del `CommitAck` en `client.rs`) y sigue con `/sync`; un `SyncBatch` vacío con `has_more`, que el SDK rechaza, el servidor no lo manda: responde `Unavailable`.
  - **El reintento deduplicado de un commit:** reserva igual que uno nuevo (los dos commits de `responses_without_room_in_the_budget_are_unavailable_before_they_are_built`).
  - **Los requests cancelados:** la reserva vuelve con la respuesta del actor que nadie espera, con la que no se llegó a codificar o con el body (`the_budget_returns_to_zero_once_every_response_ends_or_is_cancelled`).
  - **INV-L1:** el `Unavailable` sigue saliendo sin esperar, con `Retry-After`.
  - **El relay comparte la parte del cliente:** un chunk necesita un frame libre, y con el presupuesto casi lleno los syncs chicos siguen entrando mientras el chunk recibe `Unavailable`. No es peor que antes (antes, nada entraba), pero la prioridad relativa favorece a los syncs sobre un cliente que baja un snapshot.
  - **El costo de reservar por operación:** cada operación admitida toma el lock de las partes y el semáforo, antes una vez por respuesta. En el perfil de E1-tmpfs con 8 pedidos en vuelo, el código del presupuesto pasa de un 0,9% a un 1,5% del CPU (abajo).
  - **Una lectura en RAM sin lugar para la primera operación** pasa por `blocking_io` (un traspaso del worker sin E/S) antes de devolver el lote vacío: solo bajo presión del presupuesto, y el Lote 3 reemplaza ese camino.
  - **La `HotBuffer`:** cada entrada guarda 8 bytes más (el tamaño codificado), fuera de `ram_max_bytes`: unos 8 MB con 1.000 salas de 1.000 operaciones.
  - **Los parches de diagnóstico:** `sse-ops.patch` dejó de aplicar por dos líneas de contexto del commit del actor; se ajustaron, y los dos parches aplican sobre este lote.
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` y `cargo doc` sin advertencias; las 13 pruebas largas del servidor (`cargo test -p zemdb-server --release -- --ignored`); el checklist de medición de §5.3.
- **La revisión de delta,** por un agente aparte, de contexto fresco y solo lectura, con las seis preguntas. No hubo hallazgos Altos ni Medios. Los ocho Bajos quedaron así:
  - **Corregidos con tests:** el test del cliente que no lee sus respuestas no fallaba sin la parte por cliente (ahora afirma que queda libre el resto del presupuesto); faltaba un test de punta a punta del tope de un frame (el de la operación de 10 MiB); y el registro y el esquema no tenían ese tope.
  - **Corregidos en los documentos:** "hasta el doble" era "hasta unas 2,5 veces"; y el `Unavailable` de un sync sin lugar ni para su respuesta.
  - **Corregido en el código:** un campo `pub(crate)` sin necesidad.
  - **Agregados a las interacciones:** el costo de reservar por operación, la lectura en RAM sin lugar, el relay y la `HotBuffer`.
  - **La pregunta 6, la medición:** las corridas son válidas según DP3. El criterio de la propagación no queda demostrado, y es un desvío que necesita el visto bueno del dueño del proyecto (abajo).
- **Medición.**
  - **El entorno (DP3):** una VM de 4 vCPU (Intel Xeon a 2,80 GHz) con 15 GB, Ubuntu 24.04, kernel 6.18 y rustc 1.97.0. El servidor corre en los núcleos 0 y 1, y el generador, en el 2 y el 3, con el mismo binario del generador en todas las corridas. Los datos van en tmpfs (`/dev/shm`).
  - **Lo que se compara:** la base es `0b03a77`; el resultado, este lote.
  - **El diseño:** tres rondas intercaladas, para que la máquina sea la misma en las dos mitades. La tabla da la mediana y, entre paréntesis, el rango.
  - **Los datos crudos,** en `tools/perf/results/fase3_12/lote2/`.

  | Escenario | Base `0b03a77` | Resultado |
  |---|---|---|
  | E2-tmpfs, 1.000 salas: `503` | 926.253 | 0 |
  | E2-tmpfs, 1.000 salas: propagación p99 | 40,2 s | 2,34 s (1,37–2,61) |
  | E2-tmpfs, 1.000 salas, con 4 GiB de presupuesto (el diagnóstico): propagación p99 | 2,07 s (2,06–2,75), con un `503` en dos de las tres corridas | — |
  | E2-tmpfs, 500 salas: propagación p99, commit p99 y `503` | 37,5 ms (32,7–41,9), 9,3 ms, 91.950 | 13,9 ms (12,0–21,2), 8,5 ms, 0 |
  | E1-tmpfs, 1 pedido en vuelo: commits/s (dispersión) | 4.660 (6%) | 4.582 (−1,7%) |
  | E1-tmpfs, 8 pedidos en vuelo: commits/s (dispersión), p99 | 14.948 (4%), 1.471 µs | 13.253 (−11,3%), 1.685 µs |

  - **El criterio.**
    - **Sin `503` del presupuesto con 1.000 salas:** se cumple.
    - **La propagación en p99 no queda demostrada.** Los 2,34 s quedan dentro de la dispersión del diagnóstico de 4 GiB medido en esta máquina (2,07 s, de 2,06 a 2,75 s), pero no lo superan: con las seis corridas intercaladas de cada uno, dan 2,16 s contra 2,07 s.
    - **Los 170 ms de §4.7.1 no se reproducen acá:** son de la máquina de las evaluaciones. Acá los dos están acotados por la saturación de `block_in_place` (de 169 a 177% de CPU sobre 200%, unos 300 hilos), no por el presupuesto. En el perfil de E2 con 1.000 salas, el del resultado y el del diagnóstico son casi iguales, dominados por los cambios de hilo y los despertares (`futex`, `sched_yield`), y el código del presupuesto no aparece. Eso es lo que resuelve el Lote 3.
  - **La deriva de la máquina.** La primera medición, sin intercalar, daba el diagnóstico en 1,23 s y el resultado, una hora después, en 2,23 s. Intercalados, dieron 2,00 y 1,98 s. Por eso la medición del lote es la intercalada, y la primera se descarta (sus datos quedan en `first-*`).
  - **El control E1-tmpfs con 8 pedidos en vuelo** baja un 11,3%, de forma sistemática, por debajo del umbral (su dispersión más un 10%: 14,1%). Entre mediciones intercaladas, la baja fue de 4,8% a 11,3%.
    - **La causa.** Con el frame entero, la parte del cliente (2 frames) no alcanzaba para 8 commits en vuelo, y el 47% de los commits salía sin catch-up (vacío, con `has_more`), con un promedio de 3,4 operaciones. Con este lote, ninguno sale vacío, y el promedio es de 7,0. Lo midió un generador instrumentado (`probe-catchup-*`).
    - **Lo que cuesta:** el resultado entrega casi el doble de operaciones en catch-ups por segundo (unas 93.000, contra 51.000). En el perfil, leer de la RAM y codificar el `CommitAck` cuestan el doble de CPU, y el código del presupuesto pasa de un 0,9% a un 1,5%.
    - **Por qué la base sale mejor:** el generador de E1 no usa el catch-up ni hace `/sync`, así que la base se ahorraba un trabajo que un cliente real pagaría con un `/sync`.
  - **El binario medido** es el de este lote, salvo dos cambios de la revisión que no tocan estos escenarios: el tope de un frame en la reserva del registro y del esquema, y un campo privado.
- **La línea de base de la fase,** sobre `0b03a77`, con el mismo entorno, 3 repeticiones y la mediana. Los escenarios de este lote y los de control se midieron al arrancar; los demás, al cerrar. Los datos crudos van en `tools/perf/results/fase3_12/base-0b03a77/`.

  | E1 | 1 pedido en vuelo | 8 | 64 |
  |---|---|---|---|
  | E1-tmpfs: commits/s (dispersión), p99 | 4.438 (8%), 486 µs | 14.566 (0%), 1.505 µs | 24.714 (0%), 6,2 ms |
  | E1-disco | 1.188 (12%), 2,3 ms | 2.323 (6%), 7,5 ms | 2.075 (11%), 49 ms |
  | E1-actor | 25.964 (3%), 126 µs | 42.545 (7%), 529 µs | 27.087 (9%), 7,2 ms |

  | E2 (10 usuarios por sala, un commit por segundo por sala) | Propagación p50, p99 | `503` | CPU del servidor |
  |---|---|---|---|
  | E2-tmpfs, 500 salas | 2,0 ms, 36,9 ms | 89.485 | 113% |
  | E2-tmpfs, 1.000 salas | 4,1 s, 29,8 s | 1.025.954 | 172% |
  | E2-tmpfs, 1.000 salas, con 4 GiB de presupuesto | 730 ms, 1,23 s | 1 | 171% |
  | E2-disco, 500 salas | 342 ms, 8,6 s | 511.918 | 165% |
  | E2-disco, 1.000 salas (no válida por DP3: al final de cada corrida, de 2 a 32 de los 10.000 clientes seguían atrasados) | 10 s, 52 s | 665.311 | 172% |
  | E2-modos, 1.000 salas, a (SSE con señales, `Ack` después de cada sync) | 7,8 s, 44 s | 867.680 | 176% |
  | E2-modos, 1.000 salas, c (el prototipo del evento `ops`, `sse-ops.patch`) | 0,8 ms, 5,1 ms | 0 | 77% |
  | E2-modos, 1.000 salas, d (sin SSE, heartbeat cada 30 s) | 5,0 s, 25 s | 0 | 61% |

  - **Lo que deja ver:** esta máquina es más lenta que la de las evaluaciones, sobre todo en disco (E1-disco con 1 pedido en vuelo da 1.188 commits/s), y con 1.000 salas el servidor de fábrica colapsa siempre que los clientes hacen `/sync` después de cada evento (E2-tmpfs, E2-disco y el modo a). En el modo d, la propagación es la del heartbeat, por diseño. El modo c, sin `/sync` después de cada evento, queda en 5 ms de p99 con el 77% del CPU: es lo que buscan DS5 y el Lote 5.
- **Desvíos:**
  - **El tope de un frame por reserva** no estaba en DS1: lo pide que toda respuesta pueda servirse con la configuración más chica que se acepta. Lo aprobó el dueño del proyecto (2 de Noviembre de 2026).
  - **El criterio de la propagación,** que no queda demostrado (arriba): se acepta con el visto bueno del dueño del proyecto (2 de Noviembre de 2026).
  - **El tamaño:** unas 245 líneas de producción, contando los comentarios, contra 100 a 150 estimadas.
  - **La línea de base de la fase** (la que el Lote 1 dejó para este lote) se midió en dos partes: los escenarios de este lote y los de control, al arrancar; E1-disco, E1-actor, E2-disco y E2-modos, al cerrar, en la misma sesión (arriba).
  - **El aviso de la medición:** la regla de §5.1, paso 4, se aplicó contra una medición intercalada, porque la máquina cambió de velocidad con las horas.

### Lote 3 · La E/S fuera de los workers · ✅ Hecho
**Objetivo:** que ningún worker de Tokio haga E/S de archivos, y que los hilos no crezcan con las salas (DS2).

**Archivos:** `crates/server/src/blocking.rs` (reemplazado por el pool), `log/warm_disk.rs`, `log/tiered_log.rs`, `log/cold_disk.rs` (la compresión), `durable.rs`, `schema_registry.rs`, `actor/room.rs`, `actor/lease.rs`, `actor/manager.rs` (la apertura y el apagado), `main.rs` (el pool se espera antes de soltar `server.lock`), `config.rs` (`ZEMDB_IO_THREADS`), y sus tests. `ARCHITECTURE.md` §5.2 (*Blocking I/O in the Actor*) y la tabla de configuración.

**Qué se hace:** el pool fijo; cada operación de archivo de un actor se manda al pool y se espera; la compresión a cold en el pool o en uno propio acotado; el apagado espera las operaciones en curso. El valor de fábrica de `ZEMDB_IO_THREADS` sale de la medición.

**Tests exigidos.**
- Con cientos de salas haciendo commits a la vez, la cantidad de hilos del proceso no pasa de los workers más el pool más una cantidad fija (hoy pasa de 400).
- Un `fsync` lento (un fail point que lo demora) en una sala no demora a otra sala que no comparte su hilo del pool.
- Una sala que se detiene, se apaga o se reabre con una operación en el pool la espera antes de soltar su lock (INV-L7), el apagado no pierde nada confirmado (INV-L5), y el pool termina antes de soltar `server.lock`.
- Un `fsync` que falla en el pool detiene la sala como hoy (INV-L3), y un request cancelado en cada `.await` no deja estado a medias (INV-L2).
- Los tests de durabilidad y las imágenes de crash pasan sin cambios en sus expectativas.

**Interacciones a revisar:** la rotación y el sellado de segmentos; la escritura del roster en el tick; la apertura de una sala (los syncs de D77); la compresión y la poda; el relay, que tiene su propia E/S (`relay/files.rs`); los tests que corren en el runtime de un solo hilo (donde hoy `blocking_io` corre en el lugar); el apagado ordenado y su gracia de 10 s.

**Medición:** E2-tmpfs y E2-disco con 500 y 1.000 salas, con el Lote 2 ya aplicado. **Criterio:** los hilos del servidor fijos en todas las corridas; en tmpfs con 1.000 salas, la propagación en p99 por debajo de 10 ms (el diagnóstico de los dos arreglos dio 5,8 ms); en disco con 1.000 salas, sin colapso (el diagnóstico con la E/S en el worker dio 3,9 s en p99, §4.7.1 de las evaluaciones). El valor de fábrica de `ZEMDB_IO_THREADS` es el más chico que cumple el criterio en disco.

**Metodología:** §5.1, con los checklists de crash-safety, de concurrencia y de medición; las versiones largas del servidor (imágenes de crash) dos veces, antes y después del lote. Por el riesgo, la revisión de delta la hacen dos revisores independientes: uno con el foco en la durabilidad y otro en la concurrencia.

**Documentos:** `ARCHITECTURE.md` §5.2 y la configuración (§5.3 o §7.3); `INVARIANTES.md`: INV-D1, D7, L7, B3, B4; `README.md` (la variable nueva).

**Entrega:** `perf(server): run the file I/O of the rooms on bounded pools, off the Tokio workers (Fase 3.12, Lote 3)`.

**Hecho.**
- **Los pools de E/S** (`io_pool.rs`, que reemplaza a `blocking.rs`). Cada operación de archivo de una sala, del manager y del registro de esquemas espera un permiso de su pool y corre en un hilo bloqueante de Tokio (`spawn_blocking`); el resultado se espera sin bloquear el worker, y un pánico vuelve a quien espera. El pool de las salas tiene `io_threads` permisos (`ZEMDB_IO_THREADS`, de 1 a 512, 8 de fábrica, abajo); la compresión de los segmentos warm, un pool propio de 2, para que una compresión (de 0,1 a 0,3 s por segmento) nunca haga esperar a la E/S de las demás salas, y que una sala no espera: sin un permiso libre, su pasada deja los segmentos para la siguiente. El relay acota su propia E/S a 16 operaciones a la vez, y el runtime del servidor (`IoThreads::runtime_builder`) tiene exactamente los hilos bloqueantes que suman los tres (`io_threads` + 2 + 16): los hilos del proceso quedan fijos, y una operación con su permiso espera un hilo, a lo sumo, lo que tarda en liberarse el de la anterior. Ningún worker hace E/S de archivos, y `block_in_place` desaparece del servidor.
- **El actor** (`actor/room.rs`). El log viaja por valor a cada operación sobre los archivos de la sala y vuelve (`log_io`): nadie más lo lee mientras tanto, porque el actor espera cada operación antes de seguir. Van al pool el append con su `fsync` (y el sellado de segmentos), las lecturas de un `/sync` o de un catch-up que llegan a los tiers de disco (con la reserva de la respuesta, que vuelve con ellas), la poda que borra segmentos, el roster (se serializa en el actor y se escribe en el pool; la marca de sucio se limpia recién después) y el mantenimiento de disco; una lectura desde la ventana de RAM (`fetch_from_memory`) y una poda que no borra nada no tocan archivos y siguen en el actor. Si el actor se descarta mientras espera (solo pasa cuando se apaga su runtime), el log, con el lock de los archivos de la sala, se suelta cuando la operación termina. El piso de snapshot de la sala se publica en el relay cuando el actor empieza a correr, y no al abrir.
- **El manager** (`actor/manager.rs`). La apertura de una sala (`RoomActor::open` entero, con los syncs de D77), la lectura y escritura de `meta_room.json`, y el rename y el borrado de una sala eliminada corren en el pool llevándose el spawn lock de la sala (`io_locked`): si quien las lanzó se cancela, el lock se suelta recién cuando terminan (INV-L7). La lista para las recargas de esquema (`room_meta`) la arma ahora `spawn_actor`, justo antes de leer el esquema y sin ningún `.await` en el medio, y la deshace si la apertura falla o se cancela (`Listed`), para que no quede la entrada de una sala que no abrió (INV-B3).
- **El registro de esquemas y `add_column`** (`schema_registry.rs`, `api/control_plane.rs`): sus escrituras corren en el pool; el handler de `add_column` corre la escritura de la columna y la recarga de las salas en su propia tarea, porque la espera del pool es un punto de corte nuevo (INV-L2).
- **`TieredLog`** queda síncrono, sin `blocking_io`: lo llama el actor en el pool. El mantenimiento comprime en el mismo hilo (`compress_warm_segment_sync`); la versión async de la compresión desaparece.
- **`main`** no cambia de forma: al soltar el runtime, Tokio espera las operaciones que empezaron, y `server.lock` se suelta después (D100).
- **Tests agregados** (los siete marcados con ⊗ fallan con el comportamiento anterior, la E/S en el lugar con `block_in_place`; los demás cuidan reglas nuevas o que ya se cumplían y respaldan las mutaciones):
  - `tests/io_pool.rs` (nuevo): ⊗ `runs_the_work_on_another_thread_and_returns_its_result`, ⊗ `never_runs_more_operations_at_once_than_its_threads`, ⊗ `work_whose_caller_is_dropped_runs_to_its_end_and_drops_what_it_owns_afterwards`, `a_panic_in_the_work_reaches_the_caller_and_the_pool_keeps_working`, `a_long_compression_never_keeps_the_other_file_operations_waiting`, `the_server_runtime_has_a_fixed_number_of_blocking_threads`.
  - `relay/tests/files.rs`: `the_relay_runs_at_most_its_blocking_operations_at_once`.
  - `actor/tests/manager.rs`: ⊗ `the_threads_do_not_grow_with_the_rooms_committing_at_once` (200 salas con el `fsync` demorado: con el código anterior, 133 hilos); ⊗ `a_slow_fsync_in_one_room_does_not_hold_back_another_room` (en el runtime de un solo hilo); ⊗ `a_spawn_dropped_while_its_room_opens_leaves_nothing_and_keeps_the_lock_until_the_end` (ni la lista de recargas, ni el piso de snapshot, y el spawn lock hasta que la apertura termina); `a_room_shut_down_during_its_fsync_releases_its_files_only_after_it`; `a_room_whose_runtime_ends_during_its_fsync_keeps_its_files_until_the_fsync_ends`; `a_room_whose_runtime_ends_during_its_roster_write_keeps_its_files_until_it_ends`; `a_dropped_file_operation_of_the_manager_drops_its_result_before_the_spawn_lock`; `a_request_waiting_for_a_full_pool_is_answered_within_the_actor_timeout` (INV-L1 con el pool lleno); `a_room_compressing_its_segments_does_not_hold_back_another_room`; `a_room_does_not_wait_for_the_compression_threads_of_other_rooms`; `no_file_operation_of_the_manager_runs_on_the_runtime_thread`; `a_reload_while_a_room_opens_reaches_its_new_actor`; `a_panic_in_a_file_operation_stops_the_room_and_the_next_request_reopens_it`.
  - `actor/tests/room.rs`: ⊗ `compression_runs_off_the_runtime_thread` (movido desde `log/tests/tiered_log.rs`: el log ya no decide dónde comprime); `no_file_operation_of_a_room_runs_on_the_runtime_thread` (commits que sellan, un `/sync` desde disco, acks que podan, el roster y el mantenimiento); `a_failed_roster_write_is_retried_by_the_next_tick`; `an_ack_whose_roster_write_fails_prunes_nothing`.
  - `api/tests/control_plane.rs`: `a_column_addition_dropped_while_it_is_written_still_reloads_the_rooms`; `the_schema_registry_writes_off_the_runtime_thread`.
  - `tests/config.rs`: `ZEMDB_IO_THREADS`, con su valor de fábrica, su clave TOML y su rango.
  - **Infraestructura de tests:** `fail_point::watch_threads` registra los hilos que llegan a los fail points de E/S bajo un directorio (los hooks no cuentan, porque varios marcan pasos que no son E/S); los hooks nuevos `room_open`, `warm_append_sync` y `write_atomic_before_rename`, que en producción no hacen nada.
  - **Tests adaptados:** `a_respawn_racing_a_schema_reload_serves_the_new_schema` (antes `schema_reload_racing_a_respawn_reaches_the_new_actor`: la lista para recargas ya no la arma `resolve_metadata`, y una recarga durante la apertura la cubre el test nuevo); `http2_client_that_stops_reading_is_disconnected` (2 syncs retenidos en lugar de 4, abajo); la compresión async de `cold_disk.rs` desaparece con su test, que duplicaba uno síncrono.
- **Mutaciones verificadas,** cada una con algún test que falla: correr la operación en el lugar, como antes (siete tests); quitar el permiso del pool; el `log_io` en el lugar; la lectura de disco de un `/sync` en el lugar; la poda que borra en el lugar; el roster en el lugar, o sin llevarse el log; marcar el roster guardado antes de escribirlo; podar aunque falle la escritura del roster (D56); esperar un permiso de compresión ocupado, o mandar la pasada sin compresión al pool de compresión; que `io_locked` no se lleve el guard, o que lo suelte antes que el resultado; que `Listed` no deshaga la lista; listar la sala después de abrirla; publicar el piso de snapshot al abrir; `room_exists` y las escrituras del registro en el lugar; `add_column` sin su propia tarea; el runtime sin el tope de hilos bloqueantes; el relay sin sus permisos.
- **Interacciones revisadas:**
  - **La rotación y el sellado:** el sellado al llegar a `ram_max_ops` o `segment_max_bytes` corre dentro de `append`, en la misma operación que el `fsync`; el de la apertura, en la apertura. Los tests de `tiered_log` siguen sin cambios en sus expectativas.
  - **El roster en el tick:** se serializa en el actor y se escribe en el pool llevándose el log; la marca de sucio se limpia recién después (`a_failed_roster_write_is_retried_by_the_next_tick`). Espera su turno en el pool, así que con el pool lleno lo que se pierde en un corte puede pasar de ~1 s (INV-R3 lo anota); la poda sigue esperando a que el roster esté escrito (D56, `an_ack_whose_roster_write_fails_prunes_nothing`).
  - **La apertura (los syncs de D77):** `RoomActor::open` entero corre en el pool bajo el spawn lock que la operación se lleva. El piso de snapshot del relay se publica cuando el actor empieza a correr, no al abrir, para que una apertura cortada no deje uno sin actor: toda subida pide antes el rango del log, que ya lo encuentra corriendo.
  - **La compresión y la poda:** una pasada que comprime toma un permiso de compresión sin esperar; si no hay, la pasada corre en el pool de E/S sin comprimir, y la cuota sigue acotando el disco.
  - **El relay:** sigue con su propia E/S en `spawn_blocking`, ahora a lo sumo 16 operaciones a la vez, con un semáforo propio (`the_relay_runs_at_most_its_blocking_operations_at_once`); el runtime del servidor tiene exactamente los hilos bloqueantes de los pools y del relay (`the_server_runtime_has_a_fixed_number_of_blocking_threads`), así que una operación de las salas con su permiso no espera detrás del relay en la cola de Tokio. Por eso `io_threads` no pasa de 512, el máximo de hilos bloqueantes de Tokio.
  - **Los tests en el runtime de un solo hilo:** la E/S ya no corre en el hilo del runtime, así que los timers corren durante la E/S. `http2_client_that_stops_reading_is_disconnected` retenía 4 syncs de 12,6 MB, que en debug tardan de 1 a 1,7 s cada uno en leerse de disco: los dos últimos vencían `ACTOR_TIMEOUT` (504), y pasaba solo porque la lectura en el lugar frenaba también los timers. Ahora retiene 2. Con el reloj de Tokio pausado, `spawn_blocking` frena su avance automático mientras corre una operación.
  - **El apagado y su gracia de 10 s:** `shutdown_all` apaga hasta 64 salas a la vez, y cada una escribe su roster en el pool; al soltar el runtime, Tokio espera las operaciones que empezaron (las que esperaban un permiso o un hilo se descartan sin correr), y `main` suelta `server.lock` después (D100). Si la gracia vence, `main` sale con `exit(1)`, como un crash: lo confirmado ya es durable, y solo se pierde roster (INV-L5).
  - **El presupuesto de respuestas del Lote 2:** la reserva viaja con la lectura al pool y vuelve; una lectura desde la ventana de RAM no va al pool. Los tests del Lote 2 pasan sin cambios.
  - **El registro de esquemas:** sus escrituras corren en el pool y esperan ahí el lock de escritura del registro; varias a la vez (solo del administrador) ocupan permisos esperándose. Se acepta: son pedidos administrativos.
  - **Los nuevos puntos de corte (INV-L2):** un pedido cortado mientras espera una operación la deja terminar entera: una apertura cortada deja la sala sin abrir, sin listar y sin piso; una creación cortada después de escribir `meta_room.json` deja la sala creada, que abre el próximo pedido; `add_column` corre la escritura y la recarga en su propia tarea.
  - **INV-D7:** el dedup, el head y el evento SSE de un commit se actualizan después de que vuelve su `fsync`, y el actor no toma otro comando mientras espera.
  - **Los pánicos:** un pánico en una operación vuelve al actor, que termina como `Panicked`; el log que viajaba con ella se suelta en su hilo. Al apagarse el runtime, una operación que Tokio ya no acepta deja al llamador esperando hasta que el runtime lo suelte, en lugar de un pánico espurio.
  - **Los ticks:** un tick que espera una operación larga deja los siguientes en ráfaga (`MissedTickBehavior::Burst`), como antes; solo la ventana es más larga.
  - **El reloj de Tokio:** `spawn_blocking` corre la operación en el contexto del runtime, así que los `Instant` de la HotBuffer y del roster usan su reloj.
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` y `cargo doc` sin advertencias; las 13 pruebas largas del servidor (`cargo test -p zemdb-server --release -- --ignored`) antes del lote, sobre `414cab3`, y después, también tras los arreglos de la revisión; los checklists de crash-safety y de concurrencia, que recorrieron los dos revisores (abajo); y el de medición.
- **La revisión de delta,** por dos agentes aparte, de contexto fresco y solo lectura, uno con el foco en la durabilidad y otro en la concurrencia, con las seis preguntas. Ninguno encontró hallazgos Altos ni Medios. Los Bajos y las notas quedaron así:
  - **Corregidos con tests:**
    - la escritura del roster no se llevaba el log: un actor soltado durante esa escritura (solo al apagarse el runtime) soltaba el lock de la sala antes de que terminara (`a_room_whose_runtime_ends_during_its_roster_write_keeps_its_files_until_it_ends`);
    - un spawn cortado durante la apertura dejaba el piso de snapshot de la sala en el relay, sin actor (ahora se publica cuando el actor corre);
    - una sala con una compresión pendiente esperaba un permiso de compresión que tenían otras, sin atender comandos: un commit recibía `Timeout` (ahora toma el permiso sin esperar);
    - faltaban tests: el orden en que `io_locked` suelta su resultado y el lock, la rama de D56 en el camino nuevo del roster, el manager y el registro fuera del hilo del runtime, una recarga durante la apertura, la pasada sin compresión en el pool de E/S y el tiempo acotado con el pool lleno.
  - **Corregidos en el código:** el pánico de `IoPool::run` cuando el runtime se apaga (un `Panicked` espurio); el rango de `ZEMDB_IO_THREADS`, que aceptaba hasta 1.024, por encima de los 512 hilos bloqueantes de Tokio; los permisos, que no reservaban hilos del pool bloqueante (ahora el runtime tiene exactamente los hilos de los pools y del relay); la hora del mantenimiento, que se tomaba antes de esperar el permiso; `save` y `persist_if_dirty*` del roster, que repetían las condiciones de `unsaved`; y el comentario de `RoomSlot::finished`.
  - **Agregados a las interacciones y a los documentos:** la espera del pool en la ventana de INV-R3; la salida por la gracia vencida; el nuevo punto de corte de `create_room`; INV-D7 con su argumento; los ticks en ráfaga; y la saturación del pool bloqueante.
  - **Aceptado:** las escrituras del registro de esquemas esperan su lock dentro del pool, con un permiso tomado; son pedidos administrativos.
  - **El test de los hilos** usa el runtime del servidor (`IoThreads::runtime_builder`) y exige a lo sumo sus workers más los hilos bloqueantes que la E/S puede usar, como pidió la revisión: sin el tope, el pool solo no acota los hilos bajo carga sostenida (148 con 8 permisos en E2, abajo). Con el tope, también el traspaso viejo queda acotado; antes del lote, el runtime no tenía tope, y el mismo escenario con `block_in_place` llegó a 133 hilos. El tope lo cubre `the_server_runtime_has_a_fixed_number_of_blocking_threads`, que falla sin él.
  - **La pregunta 6, la medición:** las corridas son válidas según DP3 (sin errores, el head igual a lo confirmado, sin huecos ni clientes atrasados, el generador por debajo del 67% de sus núcleos), con los escenarios y controles de §5.2. Pidió anotar el entorno, guardar el perfil (`profile-*.txt`), corregir el rango de hilos de la base, dar al menos 3 corridas a cada valor del barrido de `ZEMDB_IO_THREADS` que decide el de fábrica, medir 750 salas, donde la máquina todavía no satura, y anotar la regresión de E1 con 1 pedido en vuelo; está todo abajo. El criterio del lote no queda respaldado, salvo el de los hilos.
- **Medición.**
  - **El entorno (DP3):** el de la línea de base de la fase: una VM de 4 vCPU (Intel Xeon a 2,80 GHz) con 15 GB, kernel 6.18 y rustc 1.97.0; el servidor en los núcleos 0 y 1 y el generador en el 2 y el 3, el mismo binario del generador en todas las corridas (el mismo archivo en los tres árboles). El disco es un ext4 sobre un disco virtio (`/dev/vda`, con `mq-deadline`), y tmpfs, `/dev/shm`. El límite de archivos abiertos es 20.000, y `run_e2.sh` arranca el servidor con `ZEMDB_MAX_CONNECTIONS=19500` y `ZEMDB_MAX_SSE_STREAMS=19000`. El rango de puertos efímeros pasó a 20000–65000: con 1024–65000, una conexión de cliente tomó como puerto local el 18080 y el servidor base no pudo arrancar; esa corrida se descartó entera.
  - **Lo que se compara:** la base es `414cab3`; el resultado, este lote con `ZEMDB_IO_THREADS` = 8, el valor de fábrica que se eligió primero (el que quedó, 16, tiene su fila en el barrido de abajo, y E1-disco se midió con él); el diagnóstico, `414cab3` con `inline-io.patch` (la E/S en el worker, sin `block_in_place`), que en las evaluaciones dio 5,8 ms con 1.000 salas.
  - **El diseño:** tres rondas intercaladas. La tabla da la mediana y, entre paréntesis, el rango. Las filas de 750 salas se midieron después, también en tres rondas intercaladas, como pidió la revisión. Los datos crudos, en `tools/perf/results/fase3_12/lote3/`.

  | Escenario | Base `414cab3` | Resultado |
  |---|---|---|
  | E2-tmpfs, 500 salas: propagación p99 | 33,7 ms (19–51) | 8,6 ms (7–17) |
  | E2-tmpfs, 1.000 salas: propagación p99 | 3,20 s (2,67–3,62) | 625 ms (379–1.125) |
  | E2-tmpfs, 1.000 salas, el diagnóstico (la E/S en el worker): propagación p99 | 208 ms (122–390) | — |
  | E2-disco, 500 salas: propagación p99 | 81 ms (64–98) | 43 ms (30–50) |
  | E2-disco, 1.000 salas: propagación p99 | 3,68 s (3,53–4,29) | 4,46 s (4,00–8,25) |
  | E2-tmpfs, 750 salas: propagación p99, CPU | 843 ms (744–1.066), 167% | 50 ms (21–64), 148% |
  | E2-disco, 750 salas: propagación p99, CPU | 2,29 s (1,37–2,59), 173% | 1,63 s (1,09–1,70), 178% |
  | Hilos del servidor | de 239 a 515 | 29 en todas las corridas |
  | E1-tmpfs, 1 pedido en vuelo: commits/s (dispersión) | 4.002 (20%) | 3.657 (−8,6%) |
  | E1-tmpfs, 8 pedidos en vuelo: commits/s (dispersión), CPU | 13.042 (26%), 146% | 11.746 (−9,9%), 173% |
  | E1-disco, 1 pedido en vuelo, con 16 permisos: commits/s (dispersión), p50 | 1.302 (3%), 706 µs | 1.219 (−6,4%), 745 µs |
  | E1-disco, 8 pedidos en vuelo, con 16 permisos: commits/s (dispersión), p50 | 2.496 (12%), 3.006 µs | 2.034 (−18,5%), 3.714 µs |

  - **El criterio.**
    - **Los hilos, fijos:** se cumple. Son los workers, el hilo principal y los bloqueantes del runtime (`io_threads` + 2 + 16).
    - **La propagación en tmpfs con 1.000 salas, por debajo de 10 ms:** no se cumple. Tampoco el diagnóstico: en esta máquina, con 1.000 salas, el servidor se satura de CPU (de 171 a 190%) con cualquiera de los tres. El resultado queda 5 veces por debajo de la base y unas 3 veces por encima del diagnóstico, con rangos que se tocan. Como tmpfs es un diagnóstico (§5.2), dice qué queda fuera del disco con 1.000 salas: el CPU de los `/sync` que dispara el SSE de señales, del Lote 5.
    - **El disco con 1.000 salas, sin colapso:** no se cumple. El resultado colapsa como la base (4,46 s contra 3,68 s), con el CPU saturado (177%); el diagnóstico, con la E/S en el worker, dio 13,1 s en la línea de base del paso 0.
    - **Con 750 salas,** donde la máquina todavía no satura sin disco: en tmpfs la base colapsa (843 ms) y el resultado no (50 ms, con menos CPU: 148 contra 167%); en disco los dos colapsan con el CPU saturado, el resultado menos (1,63 s contra 2,29 s).
    - **Los controles:** ninguno empeora más que su dispersión más un 10% (E1-tmpfs con 1 y 8 pedidos en vuelo, E2-tmpfs con 500 salas, que mejora). Pero comparando ronda por ronda, E1-tmpfs con 1 pedido en vuelo queda por debajo de la base en las tres (−17%, −9% y −18%): son unos 23 µs más por commit (de 220 a 246 µs en p50), lo que cuesta pasarle la operación a otro hilo y volver. Con 8 pedidos en vuelo, cada commit gasta un 32% más de CPU (de 146 a 173% por menos commits). Es el riesgo que el plan anota para un commit aislado, y un costo conocido para el criterio del Lote 4, que mide la latencia con un pedido en vuelo.
    - **Una sala sola en disco (E1-disco, medido después, con el valor de fábrica de 16):** con 1 pedido en vuelo, un 6% menos de commits por segundo (unos 40 µs más por commit); con 8, un 18% menos en las tres rondas (de −14 a −22%), con el CPU al 75%. Con `block_in_place`, el `fsync` corría en el mismo hilo del actor, que seguía apenas terminaba; ahora corre en otro hilo, que tiene que despertarse y conseguir un núcleo ocupado por HTTP, y el actor vuelve a la cola de su worker. Que los workers miren su cola global más seguido (`global_queue_interval(4)`) no lo cambia (de 1.966 a 2.012 con 8 en vuelo, dentro del ruido; `variant-gqi-*`). Es el costo de no bloquear el worker, por cada `fsync`: el Lote 4 lo reparte entre los commits de un grupo, y su criterio pide más del doble de commits por segundo con 8 pedidos en vuelo en disco.
    - **Lo que sí muestra la carga de 1.000 salas,** con la misma carga ofrecida: el resultado atiende 5.025 `/sync` por segundo en disco, contra 3.780 de la base, con el mismo CPU (177 contra 175%), y 8.295 en tmpfs, contra 4.179.
  - **El perfil,** en tmpfs con 1.000 salas y 40 s por corrida: el resultado usa 1,72 CPU con 3.194 cambios de contexto por segundo, y da 163 ms de propagación en p99; el diagnóstico, 1,83 CPU con 781, y 122 ms; la base, 1,75 CPU con 13.055 y 2.240 migraciones por segundo, y 2,4 s. En el perfil plano, el CPU de los dos primeros va a hyper, `malloc` y la red del kernel: los unos 10.000 `/sync` por segundo que dispara el SSE de señales con 1.000 salas, lo que resuelve el evento `ops` del Lote 5. El pool suma unos 2 puntos en los cambios de contexto (`finish_task_switch`, 3,4% contra 1,35%). Es lo que cuesta cada operación en tmpfs, donde el `fsync` casi no cuesta: con 8 pedidos en vuelo en E1-tmpfs, el CPU sube de 146 a 173%.
  - **El valor de fábrica de `ZEMDB_IO_THREADS`.** Un primer barrido en disco con 1.000 salas, una corrida de cada uno y sin el tope de hilos del runtime: con 1 y 2, 44,8 y 25,1 s, lejos del resto. Después, con el tope, al menos tres corridas de 4, 8 y 16 (las de 8 son las de la medición final más una del barrido):

    | En disco | 500 salas: propagación p99 | 1.000 salas: propagación p99 |
    |---|---|---|
    | 4 | 42,8 ms (26–70) | 7,40 s (4,03–9,80) |
    | 8 | 37,4 ms (30–50) | 4,23 s (3,11–8,25) |
    | 16 | 56,9 ms (26–60) | 3,66 s (2,75–4,18) |

    Ninguno evita el colapso con 1.000 salas, 4 empeora, y entre 8 y 16 los rangos se tocan. El valor de fábrica es 16, como decidió el dueño del proyecto (2 de Noviembre de 2026): el disco es el caso de uso, y los permisos acotan los `fsync` en vuelo, de los que depende lo que rinde el disco (las operaciones en paralelo sobre la latencia de cada una: con 8 permisos y un `fsync` de 5 ms, el de un disco de nube, unos 1.600 por segundo, por debajo de lo que piden 1.000 salas con su roster); con 1.000 salas, 16 queda como la base (3,66 s contra 3,68 s), y cuesta 8 hilos fijos más.
  - **Las variantes,** dos corridas intercaladas en tmpfs con 1.000 salas: `spawn_blocking` con los semáforos, sin el tope de hilos, 564 ms con 148 hilos; con el tope, 549 ms con 37; un pool de hilos propios, 689 ms con 39. Las tres, entre 185 y 188% de CPU. El tope deja los hilos fijos sin costo medible, y los hilos propios no son mejores.
  - **La deriva de la máquina:** el diagnóstico pasó de 71 ms, en la línea de base del paso 0, a 208 ms de mediana en la medición final, dos horas después. Por eso la medición es la intercalada.
- **Desvíos,** aprobados por el dueño del proyecto (2 de Noviembre de 2026):
  - **`spawn_blocking` acotado, en lugar de hilos propios.** DS2 pedía un pool de hilos dedicados y descartaba `spawn_blocking` porque crecía con el pool de hoy. Acotado por los semáforos y con el runtime con exactamente sus hilos, no crece. Los hilos propios rompían 9 tests: con el reloj de Tokio pausado, el runtime adelanta su reloj mientras espera una operación que corre fuera de él, y `spawn_blocking` lo impide. Tampoco medían mejor (arriba). De paso, al soltar el runtime, Tokio espera las operaciones que empezaron, así que D100 sigue igual, sin esperar el pool a mano en `main`.
  - **El relay, acotado a 16 operaciones bloqueantes a la vez** (antes, hasta los 512 hilos de Tokio): lo pide que los hilos del runtime sean exactos y que una operación de las salas con su permiso no espere detrás del relay en la cola de Tokio.
  - **Los criterios de 1.000 salas,** que no se cumplen en esta máquina (arriba).
  - **Una sala sola en disco con 8 pedidos en vuelo, un 18% más lenta** (arriba), hasta el *group commit* del Lote 4.
  - **El tamaño:** unas 584 líneas de producción agregadas sin comentarios ni blancos (260 quitadas), unas 47 de ellas de infraestructura de tests en `fail_point.rs`: casi el doble del tope de ~300 de `docs/METODOLOGIA.md` §4. Gran parte es mecánica (los métodos que pasan a `async`, el `.await` que rustfmt parte en otra línea, los accesos al log). Como pidió el dueño del proyecto, el lote va en dos commits, que compilan y pasan los tests cada uno: la E/S de los actores de sala; y la del manager y el registro de esquemas, sin `blocking_io`, con los hilos del runtime fijos y el relay acotado, con los documentos y la medición.
  - **`GEMINI.md`, regla 8:** sin `block_in_place`; la E/S de archivos del servidor, a sus pools.
  - **`inline-io.patch`** se rehízo sobre el pool, para poder repetir el diagnóstico.

### Lote 4 · *Group commit* · ✅ Hecho
**Objetivo:** un `fsync` por grupo de commits, sin perder la garantía de cada `CommitAck` (DS3).

**Archivos:** `crates/server/src/actor/room.rs` (el bucle del actor y `commit`), `log/tiered_log.rs` y `log/warm_disk.rs` (escribir sin sincronizar, sincronizar el grupo, el sellado al final del grupo), `dedup.rs`, y sus tests. `ARCHITECTURE.md` §5.2.

**Qué se hace:** el actor toma la tanda de commits consecutivos de su buzón, hasta su tope; los valida, les asigna secuencia y escribe sus registros; un `fsync` en el pool; después registra la deduplicación, avanza el head visible, responde a cada uno con su catch-up y emite los eventos.

**Tests exigidos.**
- Con muchos commits en espera, se hacen menos `fsync` que commits (un contador de prueba), y cada `CommitAck` sale después del `fsync` que lo cubre.
- Un crash entre la escritura de los registros del grupo y su `fsync`, en cada posición: ningún commit del grupo se confirmó, y los que llegaron al disco se recuperan como hoy (INV-D1, D2, D3).
- Un `fsync` que falla: todos los commits del grupo reciben `Unavailable`, la sala se detiene, y los reintentos con el mismo `MutationId` se deduplican si el registro llegó al disco (INV-L3, Y4).
- Un commit rechazado en el medio del grupo no deja rastro y no frena a los demás (INV-Y5); dos con el mismo `MutationId` en el mismo grupo reciben la misma secuencia (INV-Y4).
- Un `/sync` o un `Ack` en el buzón corta el grupo: se atiende después del `fsync`, y nunca ve una operación del grupo antes de que sea durable (INV-D7).
- El sellado de `active.wal` al final del grupo que alcanza el tope.

**Interacciones a revisar:** el catch-up de 1 RTT (el atajo cuando el cliente está al día); la retención y la poda; el sellado y el índice de segmentos; el roster (los cursores que reportan los commits); el presupuesto de respuestas del Lote 2; el pool del Lote 3.

**Medición:** E1-disco con 1, 8 y 64 pedidos en vuelo, y E2-disco con 1.000 salas. **Criterio:** con 8 y 64 pedidos en vuelo en disco, más de 2 veces los commits por segundo de la línea de base (hoy, unos 5.400); con 1 pedido en vuelo, la latencia en p50 sin empeorar más de un 10%.

**Metodología:** §5.1, con los checklists de crash-safety, de concurrencia y de medición; versiones largas e imágenes de crash; dos revisores de delta, como en el Lote 3.

**Documentos:** `ARCHITECTURE.md` §5.2 (el append, el sellado al final del grupo); `INVARIANTES.md`: INV-D1, D7, B4, y cómo se sostienen con el grupo INV-L3, Y4 e Y5.

**Entrega:** `perf(server): group commit, one fsync for the commits waiting in a room (Fase 3.12, Lote 4)`.

**Hecho.**
- **El grupo** (`actor/room.rs`). Cuando el actor toma un commit de su buzón, toma también los que ya esperan detrás (`Mailbox::next_commit`, sin esperar a ninguno), hasta el primer comando que no es un commit, hasta 64 commits (`GROUP_MAX_COMMITS`, por debajo de los 100 del catch-up de un commit, para que el catch-up de un cliente al día antes del grupo llegue al final del grupo) o hasta 1 MiB de operaciones nuevas (`GROUP_MAX_BYTES`, como las cuenta la ventana de RAM; la que lo alcanza entra). El comando que cortó el grupo queda retenido en el buzón (`Mailbox::held`) y es el próximo que se atiende.
  - **Antes del `fsync`** (`sequence`, `decide`): cada commit se valida y se secuencia en orden, como si viniera solo, contra el head visible y la ventana de dedup, sin cambiar nada fuera del grupo; un reintento de una mutación anterior del grupo recibe su secuencia, y un cursor que nombra una secuencia del grupo, que todavía no es visible, es `InvalidSequence`. Un rechazo no entra en los registros ni frena a los demás.
  - **El `fsync`:** los registros de las mutaciones nuevas se escriben en `active.wal` (un registro por commit, el mismo formato) y se sincronizan con un solo `sync_data`, en una operación del pool del Lote 3 (`TieredLog::append_group`, `WarmDiskLog::append_records`). El sellado, si `active.wal` alcanzó `ram_max_ops` o `segment_max_bytes`, va después del último registro del grupo.
  - **Después del `fsync`** (`answer`): el dedup y el head toman el grupo, un solo `head_advanced` con el head del grupo, y cada commit recibe su respuesta, en orden, con su catch-up hasta el head del grupo. El atajo del catch-up de 1 RTT (la operación propia, sin leer el log) queda para el commit al día cuya secuencia es el head; los demás leen de la RAM. La contabilidad de un rechazo por `BehindCompaction` (`reject_behind_log`) y el cursor que reporta cada commit aceptado (`observe`) también van en orden, al responder.
  - **Si falla** la escritura, el `fsync` o la inserción en la RAM, todos los commits del grupo que no se rechazaron, reintentos incluidos, reciben `Unavailable`, y la sala se detiene; si falla el sellado, el grupo ya es durable y se confirma entero antes de detenerse.
  - Sin espera a propósito: con un commit a la vez, el grupo es ese commit.
- **El log** (`log/tiered_log.rs`, `log/warm_disk.rs`). `TieredLog::append_group` y `WarmDiskLog::append_records` reemplazan al append de un registro, que queda solo para los tests (un grupo de uno). Los registros del grupo se rechazan antes de escribir nada si no continúan `active.wal` o no se continúan entre sí (INV-D10); se codifican en un búfer, se escriben con una sola escritura y se sincronizan con un `sync_data`. Los fail points `warm_append_write` y `warm_append_sync` siguen en los mismos pasos, ahora uno por grupo. El contador de prueba `IoEvent::ActiveWalSync` cuenta los `fsync` de `active.wal`.
- **Tests agregados** (los marcados con ⊗ fallan con el comportamiento anterior, con el esqueleto que conserva un `fsync` por commit; los demás cuidan reglas que ya se cumplían y respaldan las mutaciones):
  - `actor/tests/room.rs`: ⊗ `commits_waiting_behind_an_fsync_share_the_next_one` (diez commits detrás de un `fsync` demorado: dos `fsync` en lugar de once, ninguna respuesta del grupo antes de su `fsync`, y un solo `head_advanced`, con el head del grupo); ⊗ `a_failed_fsync_fails_every_commit_of_its_group_and_retries_get_their_sequences` (los reintentos, en orden inverso, reciben la secuencia que tenían en el grupo); ⊗ `a_rejected_commit_in_a_group_leaves_no_trace_and_a_repeated_one_gets_its_sequence` (un `SchemaViolation`, un cursor más allá del head, un cursor que nombra una secuencia del grupo, un `MutationId` repetido y el mismo id de otro cliente); `a_command_that_is_not_a_commit_ends_the_group_and_sees_it_durable` (un `Ack` y un `/sync` en el medio del buzón); ⊗ `a_group_that_reaches_the_segment_limit_seals_active_wal_at_its_end`; `a_seal_that_fails_after_a_group_acknowledges_every_commit_of_it_and_stops_the_room`; ⊗ `a_group_takes_at_most_its_cap_of_commits_and_of_bytes`; ⊗ `the_catchup_of_every_commit_of_a_group_reaches_the_head_of_the_group`.
  - `log/tests/tiered_log.rs`: ⊗ `a_group_of_records_is_synced_once_and_is_visible_only_after_its_fsync`; ⊗ `every_cut_of_a_group_written_before_its_fsync_recovers_the_whole_records_before_it` (la imagen de crash del grupo: cada corte de los registros escritos sin su `fsync` reabre con los registros enteros anteriores al corte y sus `MutationId`, trunca el resto y sigue).
  - `log/tests/warm_disk.rs`: ⊗ `records_that_do_not_continue_each_other_are_all_rejected_before_writing`.
  - **De la revisión** (abajo), cada uno con su mutación: `a_group_whose_seal_fails_closes_the_mailbox_before_its_first_ack` (falla con el código revisado), `a_retry_in_a_group_whose_fsync_fails_is_not_acknowledged`, `a_group_never_assigns_the_last_sequence_number`, `a_panic_during_a_group_answers_the_command_held_behind_it_unavailable`, el rechazo dentro de un grupo fallido (en `a_failed_fsync_fails_every_commit_of_its_group_and_retries_get_their_sequences`) y, en `log/tests/tiered_log.rs`, `a_group_whose_first_record_was_lost_before_its_fsync_fails_the_open`.
  - **Infraestructura de tests:** el contador `IoEvent::ActiveWalSync`; `RoomActor::commit` pasa a ser un ayudante de los tests (un grupo de uno).
- **Mutaciones verificadas,** cada una con algún test que falla: el grupo de uno (el buzón no junta commits); un `fsync` por registro; el grupo que sigue después de un comando que no es un commit; el comando retenido que no se atiende primero; el head y el evento antes del `fsync`; el atajo del catch-up sin el head del grupo; sin el tope de commits; sin el tope de bytes; un grupo fallido que no responde; un rechazo que corta el grupo; sin el reintento dentro del grupo; el cursor contra el head del grupo en lugar del visible; los registros del grupo sin chequear entre sí; la sala que sigue después de un sellado fallido; un rechazo por `BehindCompaction` sin su contabilidad; y, de la revisión, el buzón que se cierra después de responder un grupo cuyo sellado falla, un reintento confirmado en un grupo fallido, el tope de `u64::MAX` contra el head visible, el pánico que drena el canal sin el comando retenido, y un grupo fallido que responde sus rechazos `Unavailable`. Registrar el dedup antes del `fsync` no tiene efecto observable (el actor no atiende nada mientras espera el `fsync`, y si falla se detiene con su ventana); se registra después por INV-D7.
- **Interacciones revisadas:**
  - **El catch-up de 1 RTT:** el atajo (la operación propia, sin leer el log) solo para el commit al día cuya secuencia es el head del grupo; los demás leen de la RAM hasta el head del grupo, que puede pasar de su propia secuencia (`the_catchup_of_every_commit_of_a_group_reaches_the_head_of_the_group`). El SDK acepta un catch-up en orden desde su cursor que llega a la secuencia asignada (o trae `has_more`), y avanza su cursor hasta la última operación (`last_after` y `saw`, en `crates/client/src/client.rs`): un catch-up más largo es válido sin cambios en el cliente.
  - **La retención y la poda:** la poda corre en un `Ack` o en el tick lento, nunca en el medio de un grupo, así que el `tail` contra el que se chequea cada commit del grupo es el mismo al responder. Un `Ack` corta el grupo (`a_command_that_is_not_a_commit_ends_the_group_and_sees_it_durable`).
  - **El sellado y el índice de segmentos:** un sellado al final del grupo, con `sealed_through_seq` en `log_meta.json` antes del rename, como antes (INV-D8); el segmento entra al índice con el rango del grupo entero. Un segmento puede pasar `ram_max_ops` o `segment_max_bytes` por un grupo, como prevé DS3; la RAM conserva su ventana.
  - **El roster:** el cursor que reporta cada commit aceptado (`observe`) y la contabilidad de un rechazo por `BehindCompaction` van en el orden de los commits, al responder, después del `fsync`; el roster se persiste en el tick, como antes.
  - **El presupuesto de respuestas del Lote 2:** cada respuesta reserva al responderse, en orden; un grupo retiene a lo sumo 64 reservas hasta que sus handlers las codifican, como 64 commits en vuelo antes del lote.
  - **El pool del Lote 3:** una operación del pool por grupo, que se lleva el log (`log_io`); el permiso del pool se toma una vez por grupo.
  - **El SSE:** un `head_advanced` por grupo, con el head del grupo (`commits_waiting_behind_an_fsync_share_the_next_one`); el SDK hace `/sync` hasta él. Menos eventos con carga.
  - **El buzón:** el comando que corta un grupo queda retenido y es el próximo que se atiende (`Mailbox::recv` lo devuelve primero, en la misma llamada que lo saca, así que sigue siendo seguro ante la cancelación en el `select!` del bucle). Si la sala se detiene, se descarta con el buzón, como los comandos que esperaban detrás de un commit fallido; el apagado ordenado (un `Shutdown` retenido), el apagado por inactividad (que drena el buzón cerrado, también en grupos) y el pánico (que rechaza lo que queda, el retenido incluido) usan el mismo buzón.
  - **Un pánico durante un grupo:** todos los commits del grupo pierden su respuesta y `RoomManager` los responde `Internal`, como el comando que se atendía antes del lote; la documentación de `RoomActor::run` lo dice.
  - **INV-L1:** lo que espera el primer commit de un grupo está acotado por los topes (64 commits, 1 MiB) más un `fsync`; antes del lote, el último de 64 commits en el buzón esperaba 64 `fsync`.
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` y `cargo doc` sin advertencias; las 13 pruebas largas del servidor (`cargo test -p zemdb-server --release -- --ignored`) antes del lote, sobre `9214df3`, y después, también tras los arreglos de la revisión; los checklists de crash-safety y de concurrencia, que recorrieron los dos revisores (abajo); y el de medición.
  - **Crash-safety:** las operaciones de archivo de un grupo son las de un commit, en el mismo orden y en una sola operación del pool que se lleva el log (la escritura, el `sync_data` de `active.wal` y, si toca, el sellado con `log_meta.json`, el `sync_all`, el rename, el sync del directorio y el sidecar); ningún resultado de E/S se descarta; `warm_append_write` y `warm_append_sync` siguen en los mismos pasos, uno por grupo; las imágenes de crash pasan sin cambios en sus expectativas, y se suma la del grupo cortado en cada byte.
  - **Concurrencia:** ningún worker hace E/S de archivos (el grupo es una operación del pool); el actor espera el grupo antes de atender otro comando, y un actor descartado en esa espera suelta el log cuando la operación termina (Lote 3); los hilos no cambian (de 35 a 37 en las corridas de E2, con `ZEMDB_IO_THREADS` = 16, igual en la base y en el resultado); y el tiempo de una request sigue acotado (arriba).
- **Medición.**
  - **El entorno (DP3):** el de las mediciones de la fase: una VM de 4 vCPU (Intel Xeon a 2,80 GHz) con 15 GB, kernel 6.18 y rustc 1.97.0; el servidor en los núcleos 0 y 1 y el generador en el 2 y el 3, el mismo binario del generador en todas las corridas. El disco es un ext4 sobre un disco virtio (`/dev/vda`), y tmpfs, `/dev/shm`. El rango de puertos efímeros, 32768–60999, no incluye los de los servidores de medición.
  - **Lo que se compara:** la base es `9214df3`; el resultado, este lote, los dos con `ZEMDB_IO_THREADS` de fábrica (16).
  - **El diseño:** tres rondas intercaladas. La tabla da la mediana y, entre paréntesis, el rango. Los datos crudos, en `tools/perf/results/fase3_12/lote4/`.

  | Escenario | Base `9214df3` | Resultado |
  |---|---|---|
  | E1-disco, 1 pedido en vuelo: commits/s, p50 | 1.387 (1.318–1.435), 655 µs | 1.379 (1.336–1.421), 661 µs |
  | E1-disco, 8 pedidos en vuelo: commits/s, p50, CPU | 2.275 (2.237–2.350), 3.285 µs, 78% | 5.597 (5.408–5.935), 1.298 µs, 99% |
  | E1-disco, 64 pedidos en vuelo: commits/s, p50, CPU | 2.095 (1.972–2.124), 29,6 ms, 81% | 8.160 (8.046–8.229), 7,3 ms, 158% |
  | E1-tmpfs, 1 pedido en vuelo: commits/s | 4.216 (4.084–4.259) | 4.075 (4.007–4.120) |
  | E1-tmpfs, 8 pedidos en vuelo: commits/s | 13.258 (12.901–13.286) | 15.332 (15.226–15.565) |
  | E2-tmpfs, 500 salas: propagación p99, las tres rondas | 5,9 ms (5,8–6,6) | 7,5 ms (6,7–9,8) |
  | E2-tmpfs, 500 salas: propagación p99, once rondas | 6,0 ms (5,1–9,6) | 6,7 ms (5,6–9,8) |
  | E2-disco, 750 salas: propagación p99 | 124 ms (104–161) | 158 ms (134–168) |
  | E2-disco, 1.000 salas: propagación p99, CPU | 2,34 s (1,93–2,70), 182% | 2,06 s (1,19–2,32), 185% |

  - **El criterio:** se cumple. Con 8 pedidos en vuelo en disco, 2,46 veces los commits por segundo de la base, y con 64, 3,9 veces (el criterio nombra 5.400 commits/s, de la máquina de las evaluaciones; en esta, la base da unos 2.200, así que la comparación es contra ella, medida en la misma sesión). Con 1 pedido en vuelo, la latencia en p50 sube un 0,9% (de 655 a 661 µs), por debajo del 10%: el grupo de uno cuesta lo mismo que el commit de antes. Lo que el Lote 3 había perdido con 8 en vuelo en disco (un 18%, por pasar cada `fsync` a otro hilo) queda más que recuperado.
  - **Los controles:** E1-tmpfs con 1 pedido en vuelo, un 3% menos, dentro de su dispersión más un 10%; con 8, un 16% más, porque los commits que esperan también comparten la ida y vuelta al pool. **E2-tmpfs con 500 salas no cumple la regla con las tres rondas de §5.1:** 7,5 contra 5,9 ms en la propagación en p99 (+27%, contra una dispersión de la base del 13% más un 10%), con el resultado por encima en las tres; la diferencia está en la cola (la latencia del commit en p999, 24,5 contra 8,4 ms), con la p50, el CPU, los `/sync` por segundo y los cambios de contexto iguales. Se midió más:
    - cuatro rondas más, intercaladas, empezando por el resultado: de nuevo por encima en las cuatro;
    - cuatro rondas con el orden rotado de la base, el resultado y una variante de diagnóstico (el lote con grupos de un commit, que separa el camino de código nuevo del grupo): base 6,35 ms (5,1–9,6, con un p999 de 191 ms en una corrida), resultado 5,95 ms (5,6–6,3), variante 6,2 ms (5,8–6,9);
    - en las once rondas, 6,0 contra 6,7 ms (+12%), con el resultado por encima en 9 de las 11; la regla se cumple con las once solo porque el rango de la base incluye la corrida de 9,6 ms;
    - el perfil de CPU que pide §5.1 (dos rondas de la base, el resultado y la variante, `profile-*`): el mismo CPU (cerca de un núcleo), los mismos cambios de contexto (unos 3.800 por segundo) y migraciones, y el mismo reparto del perfil plano (el binario del servidor 43,7 contra 43,4%, el kernel 35,9 contra 35,5%, libc 19,2 contra 20,6%, la diferencia en `malloc` y `free`, dentro del ruido de 20 s de muestras); en esas corridas la propagación en p99 dio 6,5 y 5,2 ms la base, 5,8 y 6,0 el resultado, y 12,2 y 5,5 la variante.
    - No aparece una causa: el efecto está en la cola, no en todas las rondas, y la variante con grupos de uno no lo separa del ruido. Con una sala que recibe un commit por segundo los grupos son de uno, así que lo único que cambia en E2 es el camino de código (unas pocas reservas de memoria más por commit, para los vectores del grupo).
  - **E2-disco:** con una sala que recibe un commit por segundo, los grupos son de uno, y el lote no cambia los `fsync` por segundo (1.390 contra 1.362 con 750 salas, 1.701 contra 1.692 con 1.000): lo que pesa en disco con muchas salas es el `fsync` de cada sala y el del roster (Lote 7), y con 1.000 salas, el CPU, saturado por el SSE de señales (Lote 5). Con 750 salas el resultado quedó peor en las tres rondas (158 contra 124 ms), con los rangos que se tocan (134–168 contra 104–161); con 1.000, mejor (2,06 contra 2,34 s), los dos colapsados.
  - **Hilos:** de 35 a 37 en las corridas de E2, igual en la base y en el resultado (a lo sumo los workers, el hilo principal y los 34 bloqueantes del runtime con `ZEMDB_IO_THREADS` = 16).
  - **La línea de base del paso 0,** una ronda sobre `9214df3` al arrancar el lote: E1-disco 1.088, 1.922 y 1.578 commits/s con 1, 8 y 64 en vuelo; la máquina estaba más lenta que en la medición final, dos horas después. Por eso la comparación es la intercalada.
- **La revisión de delta,** por dos agentes aparte, de contexto fresco y solo lectura, uno con el foco en la durabilidad y otro en la concurrencia, con las seis preguntas. Ninguno encontró hallazgos Altos ni Medios. Los Bajos y las notas quedaron así:
  - **Corregido con test (los dos revisores):** un sellado fallido al final de un grupo cerraba el buzón después de responder el grupo, y antes del lote se cerraba antes de responder: un pedido que el cliente mandaba apenas recibía el `CommitAck` caía en el buzón del actor que se detenía y recibía `Unavailable`, en lugar de ir a la sala reabierta. Ahora el buzón se cierra antes de la primera respuesta, como cuando falla la escritura (`a_group_whose_seal_fails_closes_the_mailbox_before_its_first_ack`, determinista: el test retiene el único permiso del pool mientras se lee de disco el catch-up de un commit del grupo).
  - **Faltaban tests, agregados:** un reintento dentro de un grupo cuyo `fsync` falla no se confirma (`a_retry_in_a_group_whose_fsync_fails_is_not_acknowledged`; confirmarlo rompería INV-D1 y L3); el tope de `u64::MAX` contra el head del grupo y no el visible (`a_group_never_assigns_the_last_sequence_number`, INV-D2); el comando retenido ante un pánico, que recibe `Unavailable` (`a_panic_during_a_group_answers_the_command_held_behind_it_unavailable`); un rechazo dentro de un grupo fallido, que conserva su respuesta (en `a_failed_fsync_fails_every_commit_of_its_group_and_retries_get_their_sequences`); y la apertura de un grupo cuyo primer registro se perdió antes de su `fsync`, que falla sin truncar nada (`a_group_whose_first_record_was_lost_before_its_fsync_fails_the_open`, que fija el riesgo de abajo). Cada uno falla con su mutación.
  - **Corregido en el código:** el comentario de `log/cold_disk.rs` sobre lo que mide un segmento (ahora, `segment_max_bytes` más un grupo).
  - **Agregados a las interacciones:**
    - después de un sellado fallido, el segmento renombrado no entra al índice, y los catch-ups del grupo que empiezan antes de la ventana de RAM vuelven vacíos con `has_more` (con un error en el log por commit): inocuo, la sala se reabre;
    - un grupo más grande que la ventana de dedup reconoce un reintento dentro del grupo que, de a uno, ya habría salido de la ventana: más permisivo, y la ventana al reabrir sigue siendo la del log (INV-Y9);
    - los ticks pueden correr entre el grupo y el comando retenido (el `select!` no es `biased`), como entre dos comandos antes del lote;
    - `last_command_at` se actualiza con el primer commit del grupo y no con los que se toman detrás, que llegaron antes; solo cuenta para la inactividad;
    - la ventana de RAM se aplica una vez por grupo: un grupo más grande que `ram_max_ops` o `ram_max_bytes` manda a disco el catch-up de cada commit, una operación del pool por commit;
    - el `head_advanced` del grupo sale antes que un `snapshot_wanted` que provoca un commit del grupo al responderse; son señales, y el orden entre ellas no lleva datos;
    - el grupo se arma antes de esperar el permiso del pool, así que lo que llega durante esa espera va al grupo siguiente (eficiencia, no corrección);
    - los catch-ups llegan al head del grupo: en un grupo de n clientes al día se clonan unas n² operaciones, contra n(n+1)/2 antes, acotadas por DS1; es el costo que ataca el Lote 6;
    - un reintento deduplicado espera el `fsync` de su grupo, y en un grupo fallido recibe `Unavailable` aunque su mutación fuera durable de antes: conservador, como pide DS3, e INV-Y4 se cumple en el próximo reintento (el SDK reintenta, y cuenta `Internal` como incierto).
  - **Sin test, aceptado:** omitir la contabilidad de un rechazo por `BehindCompaction` dentro de un grupo fallido, y registrar los cursores antes del `fsync`: los dos solo cambian el roster de una sala que se detiene, que INV-Y5 excluye, y al reabrir el estado de cada cliente sale de su cursor.
  - **La pregunta 6:** las corridas son válidas según DP3 y el criterio queda respaldado. La mediana del control E2-tmpfs con 500 salas en las once rondas es 6,7 ms, no 6,5 como decía el borrador (+12%), con el resultado por encima en 9 de las 11 rondas; con las tres rondas de §5.1 no cumple la regla de los controles (abajo). Pidieron anotar también E2-disco con 750 salas (peor en las tres rondas, aunque no es control) y que los hilos van de 35 a 37.
  - **La cuestión de las páginas fuera de orden:** los dos coinciden en que es un riesgo nuevo del lote (antes, cada `fsync` cubría un solo frame, así que solo el último podía quedar roto, y una página posterior de ese mismo frame no es un frame válido) y en que no se puede evitar sin cambiar el formato: la apertura no distingue "frames del grupo sin sincronizar después de un hueco" de "un frame confirmado dañado seguido de frames confirmados", y tratarlo como escritura rota truncaría commits confirmados (INV-D1). Escribir el grupo en dos pasos deja la misma imagen si el corte cae en el primero.
- **Desvíos:**
  - **El control E2-tmpfs con 500 salas,** que no cumple la regla de §5.1 con sus tres rondas (+27% en la propagación en p99) y queda un 12% por encima en once, sin una causa en el perfil (arriba). Aprobado por el dueño del proyecto (3 de Noviembre de 2026).
  - **Un riesgo nuevo, aceptado hasta el Lote 4b** por el dueño del proyecto (3 de Noviembre de 2026): un corte de luz durante el `fsync` de un grupo puede persistir sus sectores fuera de orden y dejar un daño seguido de frames válidos del mismo grupo, que la apertura toma por corrupción (INV-D3): la sala no abre hasta que alguien trunque `active.wal` desde el daño. Ningún commit de ese grupo se había confirmado, y no se pierde ni se inventa nada. Queda en `INVARIANTES.md` §4, y lo cierra DS9 (Lote 4b): cada registro dirá dónde empieza su grupo, y la apertura truncará el último grupo dañado como hoy el último registro (D78). Para decidirlo se midió cuánto pesa, con un binario de diagnóstico fuera del repositorio:
    - **La exposición:** el tiempo en que una sala tiene un grupo de dos o más registros escrito y sin sincronizar. E1-disco con 1 pedido en vuelo, 0% (grupos de uno); con 8, el 56% (el 92% de los grupos con más de un registro, de 4 en promedio, unos 1.000 bytes); con 64, el 21% (grupos de 30, unos 7.700 bytes; la sala está limitada por el CPU y pasa menos tiempo en el `fsync`). E2-disco con 500 y 1.000 salas, entre 0,00 y 0,01% (grupos de uno).
    - **Si el disco pierde sectores del grupo fuera de orden** (el peor caso: cualquier subconjunto de los sectores sin sincronizar, en imágenes armadas con registros reales y abiertas con el log), la apertura falla en el 25% de esas imágenes con un grupo de 2 registros, en el 55% con 4 y en casi todas con 30, en sectores de 512 bytes; con páginas de 4 KiB atómicas, nunca con 4 registros (caben en una página) y siempre con 30. Toda imagen que es un prefijo abre.
    - **Lo que no se pudo medir:** con qué frecuencia un disco real pierde sectores fuera de orden en un corte. ext4 con un disco que respeta los flushes lo hace poco común, pero no imposible (la primera página del grupo se reescribe en el lugar, y los `fsync` de otras salas cierran el journal en cualquier momento), y un disco que solo garantiza escrituras atómicas de 512 bytes puede romper una página. Medirlo pide un dispositivo que simule cortes (`dm-log-writes`, o una VM con un disco que inyecte fallas), que la máquina de medición no tiene.
- **Las decisiones que dejaba el plan,** con el visto bueno del dueño del proyecto (3 de Noviembre de 2026): los topes del grupo, 64 commits y 1 MiB de operaciones nuevas (arriba); un `head_advanced` por grupo, con el head del grupo, en lugar de uno por commit (el evento es una señal, y el SDK hace `/sync` hasta él); y `Unavailable` también para el reintento de una mutación ya durable que cae en un grupo fallido.
- **El tamaño:** unas 282 líneas de producción agregadas, sin comentarios ni blancos (85 quitadas), dentro del tope de ~300 de `docs/METODOLOGIA.md` §4.

### Lote 4b · El último grupo dañado · ✅ Hecho
**Objetivo:** que una sala cuyo grupo de commits cortó un corte de luz vuelva a abrir sola, sin aceptar como escritura rota la corrupción de un grupo sincronizado (DS9).

**Archivos:** `crates/core/src/protocol/wal_frame.rs` (el registro del WAL y su clasificación, si el campo va ahí) o el payload del servidor; `crates/server/src/log/warm_disk.rs` (escribir el inicio del grupo, y la recuperación de `active.wal`), `log/segment_reader.rs` (la clasificación de un daño seguido de frames válidos), `log/tiered_log.rs`, y sus tests y los fuzzers del frame. `ARCHITECTURE.md` §5.2 y §8 (*Physical WAL Framing*).

**Qué se hace:** cada registro lleva dónde empieza su grupo (a cuántos bytes de él, el mecanismo que eligió el dueño del proyecto al arrancar el lote; el plan decía la secuencia del primer registro del grupo, ver DS9); al abrir `active.wal`, un daño seguido solo de frames válidos del mismo grupo que el daño, sin un grupo posterior, se trunca desde el daño; si aparece un frame de un grupo posterior, es corrupción, como hoy. Los segmentos sellados, sus copias cold y la lectura no cambian de reglas: solo `active.wal` puede tener un grupo sin sincronizar.

**Tests exigidos.**
- Las imágenes de crash del Lote 4 con sectores perdidos fuera de orden (cualquier subconjunto de los sectores sin sincronizar del último grupo, de 2, 4 y 30 registros): todas abren, con los registros enteros anteriores al daño y sus `MutationId`, y la sala sigue (hoy, `a_group_whose_first_record_was_lost_before_its_fsync_fails_the_open` fija que fallan).
- Un daño en un grupo seguido de un frame de un grupo posterior es corrupción: la apertura falla sin truncar nada (INV-D3, D1).
- Un daño en el último grupo, sincronizado, se trunca desde el daño: la excepción de D78 extendida, con la secuencia que puede reutilizarse si no consta en otro archivo del log (INV-D2, D8).
- La ida y vuelta del registro con el campo nuevo, y su lugar en los fuzzers del frame (INV-W1).
- Las imágenes de crash y las simulaciones de fallas existentes, sin cambios en sus expectativas.

**Interacciones a revisar:** el sellado (un segmento sellado nunca tiene un grupo sin sincronizar); la compresión, que verifica cada frame; la recuperación con un `log_meta.json` por encima del final de `active.wal` (D98); el dedup que se hidrata desde `active.wal`; el storage, que comparte el framing de `zemdb-core`.

**Medición:** E1-disco con 1 y 8 pedidos en vuelo y E1-tmpfs con 1 y 8, contra el Lote 4. **Criterio:** ninguno empeora más que su dispersión más un 10% (el registro crece unos bytes).

**Metodología:** §5.1, con los checklists de crash-safety y de medición; las versiones largas del servidor y de `zemdb-core` (los fuzzers); dos revisores de delta, como en los Lotes 3 y 4.

**Documentos:** `ARCHITECTURE.md` §5.2 y §8; `INVARIANTES.md`: INV-D2 (la excepción de D78), INV-D3, y la fila de §4 que este lote cierra.

**Entrega:** `fix(server): a torn last group of commits is truncated like a torn last record (Fase 3.12, Lote 4b)`.

**Hecho.**
- **El mecanismo** (DS9, con la distancia en bytes que eligió el dueño del proyecto al arrancar el lote, 3 de Noviembre de 2026). El payload de un frame (`WalBatchPayload`) lleva primero `group_offset`: a cuántos bytes del frame empieza su grupo de frames, 0 en el primero del grupo y en un frame que se sincroniza solo (`encode_wal_batch` y los frames del storage). `encode_wal_batch_in_group` lo escribe y `decode_batch_payload` lo devuelve (`BatchPayloadDecode::Batch`); `WarmDiskLog::append_records` escribe cada registro del grupo con la distancia al primero.
- **La apertura de `active.wal`** (`WarmDiskLog::inspect_active_segment`, `classify_last_group_in`): ante un daño en `at`, `find_frame_after_group` busca el primer frame válido cuyo grupo no empieza en `at` o antes. Si no hay ninguno, el daño es una escritura rota del último grupo y se trunca desde `at`, como hoy el último frame (D78); si lo hay, los bytes dañados eran de un grupo anterior, sincronizado, y la apertura falla sin truncar nada. La búsqueda es la de `find_valid_frame`, retomada después de cada frame del grupo desde la ventana que ya leyó (`FrameSearch::resume`), 17 bytes (un header menos uno) antes del final del frame, para que un frame guardado en un valor no tape el principio del frame siguiente (la revisión, abajo). Los checks fallidos se cobran primero, por el largo de su payload, de una franquicia de tantos bytes como recorre la búsqueda, para que los frames rotos del grupo no gasten el tope; los que la pasan cuentan para un solo tope de 16 en todo el recorrido. Cada frame encontrado se lee y se decodifica, y uno que no pasa su checksum o no decodifica cuenta como de un grupo posterior (solo puede convertir una escritura rota en corrupción). El trabajo es lineal en los bytes después del daño, más el tope.
- **Lo que no cambia:** los segmentos sellados y sus copias cold (se sellan después del `fsync` de su último grupo), la lectura de `active.wal` con la sala abierta y el storage siguen con la regla estricta (`classify_in`, `FrameAnomaly::classify`). El registro crece 8 bytes; los frames del storage también, con el campo en 0.
- **Tests agregados** (los marcados con ⊗ fallan con el comportamiento anterior; los demás cuidan reglas que ya se cumplían y respaldan las mutaciones):
  - `log/tests/tiered_log.rs`: ⊗ `losing_any_unsynced_sectors_of_a_group_reopens_with_the_whole_records_before_them` (las imágenes de crash con sectores perdidos fuera de orden: grupos de 2, 4, 30 y 64 registros de unos 330 bytes, de 30 y 64 de 1 KiB y de 64 de 8 KiB, en sectores de 512 bytes y páginas de 4 KiB, después de 3 registros sincronizados y al principio de `active.wal`; cada imagen abre con los registros enteros anteriores al primer daño y sus `MutationId`, trunca el resto y la sala sigue; todas las imágenes hasta 6 sectores y 16 al azar de los grupos más grandes) y su versión larga, `..._long` (todas hasta 12 sectores y 1.024 al azar de los más grandes); ⊗ `a_group_whose_records_each_lost_part_of_their_payload_reopens` (de la revisión); `damage_in_a_group_followed_by_a_later_group_fails_the_open_and_truncates_nothing` (cuatro casos: en el medio de un grupo, el último registro antes de un grupo de uno, un daño que cruza el borde entre dos grupos con el resto del segundo intacto, y uno que empieza al principio de `active.wal` y alcanza el primer registro del grupo siguiente: los dos últimos son los que no resolvían un bit o la secuencia); ⊗ `damage_in_the_last_group_after_its_fsync_is_truncated_from_the_damage` (la excepción de D78 extendida: la secuencia se reutiliza, y con el fin del grupo en `log_meta.json` es un hueco). ⊗ `a_frame_stored_in_a_value_does_not_hide_a_later_acknowledged_record` (de la revisión). Reemplazan a `a_group_whose_first_record_was_lost_before_its_fsync_fails_the_open`, que fijaba el riesgo.
  - `log/tests/cold_disk.rs`: `cold_segment_damaged_before_frames_of_its_group_is_corruption` (de la revisión).
  - `log/tests/warm_disk.rs`: ⊗ `every_record_of_a_group_says_where_its_group_starts` (también después de un truncado); `damage_followed_by_records_of_its_group_is_torn_only_in_active_wal` (un segmento sellado con el mismo daño falla al leerse).
  - `zemdb-core`, `tests/wal_frame_tests.rs`: ⊗ `a_frame_of_a_group_says_where_its_group_starts` (la ida y vuelta, y que el campo no cambia el tamaño del registro); ⊗ `valid_frames_of_the_damaged_group_after_the_damage_are_not_a_later_write`; `a_valid_frame_of_a_later_group_after_the_damage_is_found` (incluido un frame que dice que su grupo empieza antes del principio del log); `frames_nested_in_a_frame_of_the_damaged_group_are_not_looked_at`; `the_search_after_a_group_reads_its_frames_about_once` (un lector que cuenta los bytes: a lo sumo 4 veces el log con mil frames del grupo); y, de la revisión, `the_frames_of_a_torn_group_do_not_spend_the_limit_of_failed_checks`, `the_search_after_a_group_spends_one_limit_of_failed_checks_past_its_allowance`, `a_frame_stored_as_a_value_does_not_hide_the_start_of_the_frame_after_it`, `a_frame_stored_as_a_value_that_predicts_the_whole_header_after_it_hides_that_frame` (fija el riesgo que queda), `a_later_frame_right_after_a_frame_of_the_group_that_crosses_the_window_is_found`, `after_a_damaged_frame_with_a_valid_header_the_search_starts_where_it_ends`, `a_valid_frame_after_the_damage_that_does_not_decode_is_not_of_the_group` y `a_frame_stored_in_a_torn_frame_of_the_group_is_found` (fija el riesgo de D79). Los de core fallan todos con el esqueleto: el codificador que escribe 0, o la búsqueda estricta.
  - **Los fuzzers** (INV-W1): el log de muestra de `decoder_fuzz_tests.rs` suma un grupo de dos frames, y cada caso corre también la búsqueda del grupo (`check_search_after_group`: nunca un pánico; el frame que encuentra empieza después del daño, con un header válido, entra en el log y no es del grupo; y, salvo que haya tomado un header sin chequearlo, es el que encuentra la misma búsqueda hecha a mano con `find_valid_frame`); `random_wal_logs_round_trip` codifica con un `group_offset` al azar y lo verifica. `a_wal_payload_declaring_u64_max_operations_is_corruption` cambia el largo del payload que arma, con la misma expectativa.
- **Mutaciones verificadas,** cada una con algún test que falla: los registros con el campo en 0; la apertura con la regla estricta; los segmentos sellados con la regla del grupo; todo frame después del daño tomado como del grupo; un grupo que empieza en el daño tomado como posterior (`<` en lugar de `<=`); un campo que pasa el principio del log tomado como el principio (`saturating_sub`); la búsqueda que sigue dentro de un frame del grupo; un tope de checks por cada búsqueda reanudada; una ventana nueva por cada búsqueda reanudada; el codificador que descarta el campo; y, de la revisión, la búsqueda que retoma al final de un frame del grupo; sin franquicia; una franquicia que no se gasta; la búsqueda que empieza en el byte siguiente a un frame dañado con header válido; un frame que no decodifica tomado como del grupo; y las copias cold con la regla del grupo. Una ventana nueva que empieza un byte después del punto de reanudación no tiene efecto observable: solo saltearía un frame que empiece justo 17 bytes antes del final de un frame del grupo, que solo puede estar guardado en un valor, y el frame siguiente aparece igual.
- **Interacciones revisadas:**
  - **El sellado:** un segmento se sella al final de un grupo, después de su `fsync` (`TieredLog::append_group`), así que nunca tiene un grupo sin sincronizar; los segmentos sellados y sus copias cold siguen con la regla estricta (`finish_file_read` → `classify_in`; `damage_followed_by_records_of_its_group_is_torn_only_in_active_wal`).
  - **La compresión** verifica cada frame (`cold_disk.rs`) e ignora el campo; un segmento con grupos se comprime igual.
  - **La recuperación con un `log_meta.json` por encima del final de `active.wal`** (D98): si truncar un grupo deja `active.wal` debajo del piso, se sella con su rango real y las secuencias perdidas son un hueco (`damage_in_the_last_group_after_its_fsync_is_truncated_from_the_damage`, la segunda parte).
  - **El dedup que se hidrata desde `active.wal`:** `read_frames` se detiene en el daño, así que solo entran los `MutationId` de los registros anteriores (lo verifican las imágenes de crash); un commit del grupo truncado no se había confirmado, y su reintento recibe una secuencia nueva.
  - **El storage** comparte el framing: sus frames llevan el campo en 0 (8 bytes más) y sus reglas no cambian: no usa `find_frame_after_group`. Sus tests pasan sin cambios.
  - **La lectura de `active.wal` con la sala abierta** (`read_active_range`) sigue estricta: los grupos se escriben enteros y sincronizados, y la apertura ya truncó un grupo roto, así que un daño ahí es de después de abrir.
  - **El tope de tamaño de una operación** (`check_operation_size`, con `wal_record_payload_len`) cuenta los 8 bytes del campo: una operación al borde del tope de 16 MiB entra 8 bytes antes.
  - **Los frames guardados en valores:** en el payload de un frame válido del grupo no se miran, salvo en sus últimos 17 bytes; en el de un frame dañado cuyo header no vale, o en el de uno del grupo que perdió parte de su payload, se encuentran, y uno que dice empezar su grupo después del daño (un frame del storage, o uno escrito solo, con el campo en 0) hace corrupción, como antes (la fila de D79 de §4, ahora también para los frames rotos del grupo). Uno fabricado para parecer del grupo hace que la búsqueda siga 17 bytes antes de su final, así que para tapar el frame siguiente tiene que cubrir su header entero: solo puede tapar registros cuyo header su autor predice, en la práctica los suyos (la fila nueva de §4; la versión de esta interacción que vio la revisión decía que no podía tapar ninguno, y era falso: alcanzaba con cubrir la magia).
  - **El formato anterior:** un `active.wal` o una réplica escritos antes del lote no abren (`GEMINI.md` lo permite antes de la primera versión; el SDK descarta la réplica y vuelve a bajar todo, INV-K3).
  - **El trabajo de la apertura:** lineal en los bytes después del daño, más los 16 checks fallidos de todo el recorrido; cada frame encontrado se lee y se decodifica una vez más.
  - **La excepción de D78, extendida:** un daño del medio (no de un corte) en el último grupo, ya sincronizado, trunca hasta un grupo de commits confirmados (64 commits o 1 MiB de operaciones nuevas), como antes un frame (`INVARIANTES.md` §4).
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (1.243 tests) y `cargo doc` sin advertencias; la compilación y el clippy a `wasm32-unknown-unknown` de core, storage y client; las pruebas largas del servidor y de `zemdb-core` (`cargo test -p zemdb-server -p zemdb-core --release -- --ignored`), 27 antes del lote, sobre `d386522`, y 28 después, también tras los arreglos de la revisión (la nueva es la versión larga de las imágenes de crash del grupo); y los checklists de crash-safety y de medición (abajo y en la revisión).
  - **Crash-safety:** el lote no cambia ninguna operación de archivo, ni su orden, ni sus `sync`, ni sus fail points: el registro lleva 8 bytes más y la apertura decide distinto qué truncar; las imágenes de crash existentes pasan sin cambios en sus expectativas (`every_cut_of_a_group_written_before_its_fsync_recovers_the_whole_records_before_it`, `check_byte_flips_of_active_wal` y los de cola rota), y se suman las del grupo con sectores perdidos fuera de orden.
  - **La CI de `d2a3848`:** en la primera corrida fallaron los jobs de tests de ubuntu y windows, los dos en `http2_client_that_stops_reading_is_disconnected` (`http_edge_tests.rs`), y pasaron los otros seis, con el mismo test en MSRV 1.88 y macOS. El test mandaba dos `/sync` a la vez, y la sala los atiende uno detrás del otro: en una máquina cargada, el segundo podía pasar los 5 s que un pedido espera a su sala (`ACTOR_TIMEOUT`) y volver con `504` en lugar de `200`. Dependía de la carga desde el Lote 3, que lo había bajado de cuatro pedidos a dos, y el lote no toca ese camino. Al volver a correr los dos jobs, la CI quedó en verde en los 8. En un commit aparte, el test manda el segundo pedido recién cuando llegan los headers del primero, con la sala ya libre, y afirma el status de cada uno.
  - **Un tropiezo en la verificación, sin efecto en el resultado:** el script de mutaciones restauraba cada archivo con la fecha de su copia, anterior a la compilación mutada, y `cargo` siguió usando la última mutación (el codificador sin el campo) en las corridas siguientes de debug. Lo mostraron dos tests que fallaban sobre el código correcto; el script ahora toca cada archivo al restaurarlo, y las mutaciones, `cargo test --workspace` y las pruebas largas se corrieron de nuevo desde fuentes frescas. La versión larga de las imágenes de crash, con todas las imágenes de hasta 16 sectores, tardaba 26 minutos en el pool de las pruebas largas; quedó en todas las de hasta 12 y 1.024 al azar de los grupos más grandes.
- **Medición.**
  - **El entorno (DP3):** una VM de 4 vCPU (Intel Xeon a 2,10 GHz) con 15 GB, kernel 6.18.44 y rustc 1.97.0; el servidor en los núcleos 0 y 1 y el generador en el 2 y el 3, el mismo binario del generador en todas las corridas; el disco, ext4 sobre un disco virtio (`/dev/vda`), y tmpfs, `/dev/shm`. La VM cambió desde el Lote 4 (el disco, unas dos veces más rápido), así que la base se midió en la misma sesión.
  - **Lo que se compara:** la base es `d386522`; el resultado, este lote.
  - **El diseño:** tres rondas intercaladas, y tres más de E1-disco con 1 pedido en vuelo, empezando por el resultado. La tabla da la mediana y, entre paréntesis, el rango. Los datos crudos, en `tools/perf/results/fase3_12/lote4b/`.

  | Escenario | Base `d386522` | Resultado |
  |---|---|---|
  | E1-disco, 1 pedido en vuelo: commits/s, p50 | 2.308 (2.134–2.400), 384 µs | 2.088 (2.072–2.339), 428 µs |
  | E1-disco, 1 pedido en vuelo, las seis rondas: commits/s, p50 | 2.196 (1.807–2.400), 409 µs | 2.080 (1.961–2.339), 430 µs |
  | E1-disco, 8 pedidos en vuelo: commits/s, p50 | 8.850 (8.823–8.908), 802 µs | 8.705 (8.376–8.835), 819 µs |
  | E1-tmpfs, 1 pedido en vuelo: commits/s | 7.551 (6.697–7.668) | 7.418 (6.600–7.815) |
  | E1-tmpfs, 8 pedidos en vuelo: commits/s | 24.755 (24.459–26.677) | 24.542 (23.488–25.813) |
  | E2-tmpfs, 500 salas (control): propagación p99, hilos | 3,6 ms (2,9–3,9), 36–37 | 3,5 ms (3,4–3,6), 37 |

  - **El criterio:** se cumple; ningún escenario empeora más que su dispersión más un 10%. E1-tmpfs y E1-disco con 8 pedidos en vuelo quedan entre un 1 y un 2% abajo, dentro del ruido. E1-disco con 1 pedido en vuelo da una mediana un 9,5% más baja en las tres rondas (con una dispersión de la base del 11%), y un 5% en las seis; por pares, +1,3%, −13% y −3%, y en las tres rondas que empiezan por el resultado, +0,4%, +17,6% y −13%: el disco varía unos ±15% entre corridas (E1-tmpfs, en las dos versiones, baja de unos 7.700 a 6.600 commits/s de la primera ronda a la tercera). El lote no cambia la ruta del commit más que en 8 bytes por registro, que caben en los mismos sectores.
  - **El p50 de E1-disco con 1 pedido en vuelo** sube un 11,5% (de 384 a 428 µs), contra una dispersión de la base del 13,5% más un 10%. Las repeticiones que difieren en más de un 10% (DP3): E1-disco con 1 pedido en vuelo (un 12 a 13%) y E1-tmpfs con 1 (un 14 a 18%), en la base y en el resultado.
  - **El control E2-tmpfs con 500 salas** queda igual o mejor (3,5 contra 3,6 ms), con el mismo CPU y los mismos hilos.
  - **Los arreglos de la revisión** tocan solo la búsqueda de la apertura después de un daño; el camino del commit que se midió es el mismo, así que la medición sigue valiendo.
- **La revisión de delta,** por dos agentes aparte, de contexto fresco y solo lectura, uno con el foco en la durabilidad y la recuperación y otro en el framing y la búsqueda de `zemdb-core`, con las seis preguntas. Cada uno encontró un hallazgo Medio, con un test que lo reproduce, y los dos se corrigieron:
  - **Un frame guardado en un valor tapaba un registro confirmado (framing, Medio).**
    - **El caso:** un miembro podía guardar en un valor un frame que dijera ser del grupo de su registro, cuyo payload se estirara sobre el primer byte del frame siguiente (siempre el de la magia, predecible).
    - **Lo que pasaba:** si un daño accidental caía en el header de ese registro, la búsqueda tomaba el frame guardado por uno del grupo y lo salteaba entero, junto con el principio del registro siguiente. Ese registro, de otro miembro y confirmado, la apertura lo truncaba (INV-D1, D2, D3).
    - **El arreglo:** la búsqueda retoma 17 bytes antes del final de cada frame del grupo, así que para tapar un frame hay que predecir su header entero. Lo prueban `a_frame_stored_as_a_value_does_not_hide_the_start_of_the_frame_after_it` (en core) y `a_frame_stored_in_a_value_does_not_hide_a_later_acknowledged_record` (en el servidor).
    - **Lo que queda:** un miembro que predice los headers de sus propios registros siguientes los puede tapar. Lo fija `a_frame_stored_as_a_value_that_predicts_the_whole_header_after_it_hides_that_frame`, y es una fila nueva de `INVARIANTES.md` §4, que aceptó el dueño del proyecto (abajo, en los desvíos).
  - **El tope de checks lo gastaban los frames rotos del propio grupo (durabilidad, Medio).**
    - **El caso:** un registro del grupo que conservó su header y perdió parte de su payload falla su check.
    - **Lo que pasaba:** con más de 16 así, la búsqueda tomaba el header siguiente sin chequearlo y la apertura daba corrupción: la sala no abría, sin perder nada, que es justo lo que el lote cierra. Pasa con registros de más de un sector: un grupo de 64 registros de 1 KiB fallaba en el 99,5% de las imágenes al azar.
    - **El arreglo:** cada check fallido se cobra primero, por el largo de su payload, de una franquicia de tantos bytes como recorre la búsqueda; solo los que la pasan cuentan para el tope. Lo prueban `a_group_whose_records_each_lost_part_of_their_payload_reopens`, `the_frames_of_a_torn_group_do_not_spend_the_limit_of_failed_checks` y `the_search_after_a_group_spends_one_limit_of_failed_checks_past_its_allowance`.
    - **Las imágenes de crash** suman registros de 1 KiB y de 8 KiB, y grupos de 64.
  - **Bajos, corregidos:**
    - **Tres reglas sin test, ahora con uno:**
      - dónde empieza la búsqueda después de un frame dañado con header válido;
      - un frame válido que no decodifica cuenta como de un grupo posterior;
      - la rama de `resume` que lee una ventana nueva, con un frame del grupo que cruza la ventana y uno posterior pegado a él (la mutación con que la revisión la probó, una ventana un byte más adelante, dejó de tener efecto con el arreglo de arriba: ver las mutaciones).
    - **Las copias cold siguen estrictas,** ahora con un test: `cold_segment_damaged_before_frames_of_its_group_is_corruption`.
    - **El oráculo del fuzzer:** ahora compara la búsqueda con una hecha a mano con `find_valid_frame` y verifica que el frame que devuelve no sea del grupo.
    - **El `expect` al releer un header:** ahora devuelve el frame como de un grupo posterior.
    - **El mensaje de la corrupción:** ya no llama válido a un frame que puede no haber pasado su checksum.
    - **Los textos que nombraban el mecanismo anterior o el último frame:** el "Qué se hace" de este lote, la interacción de los frames embebidos, y el comentario de `tiered_log.rs` y la sección de `ARCHITECTURE.md` sobre el piso de secuencia.
  - **Bajo, aceptado y anotado:** un frame guardado en un valor dentro de un frame del grupo que perdió parte de su payload se encuentra y hace fallar la apertura (`a_frame_stored_in_a_torn_frame_of_the_group_is_found`). Es la fila de D79 de §4, que antes valía solo para el último frame.
  - **Notas:**
    - la decodificación de cada frame encontrado es entera, aunque solo hace falta su campo; el costo es lineal;
    - las réplicas y los `data_dir` con el formato anterior no abren: `GEMINI.md` lo permite antes de la primera versión, y el SDK descarta la réplica y vuelve a bajar todo (INV-K3);
    - D77 no cambia: un `fsync` fallido y una reapertura en el mismo proceso leen el grupo entero de la caché;
    - la tabla de la medición trunca en lugar de redondear.
  - **La pregunta 6:** las corridas son válidas según DP3 (el mismo binario del generador, sin errores, el head igual a lo confirmado, el generador por debajo del 72%), y el criterio queda respaldado.
- **Desvíos:**
  - **El mecanismo de DS9:** la distancia en bytes al inicio del grupo, en lugar de la secuencia de su primer registro que decía el plan; lo eligió el dueño del proyecto al arrancar el lote (DS9).
  - **Un riesgo que queda, aceptado por el dueño del proyecto (3 de Noviembre de 2026):** la fila nueva de `INVARIANTES.md` §4. Un miembro que predice los headers enteros de sus propios registros siguientes puede, si un daño accidental cae justo en el header de un registro suyo, hacer que la apertura trunque esos registros, confirmados, hasta el final de `active.wal`. Taparlo del todo pediría no confiar en el largo de ningún frame encontrado después del daño, y frames superpuestos guardados en valores harían cuadrático el trabajo de la apertura.
- **El tamaño:** unas 128 líneas de producción agregadas, sin comentarios ni blancos (21 quitadas), de las que unas 10 son el reformateo de los re-exports de `zemdb-core`; dentro del tope de ~300 de `docs/METODOLOGIA.md` §4 y de la estimación del plan (100 a 200).

### Lote 5 · El SSE: apagarlo y el evento `ops` · ✅
**Objetivo:** que el operador pueda apagar el SSE (DS4) y que el SSE traiga las operaciones (DS5), para que la sub-fase 4.3 se construya sobre él.

**Archivos:** `crates/core/src/protocol/messages.rs` (el mensaje nuevo del evento y el código de error nuevo, al final de sus enums) y los fuzzers y tests de codificación; `crates/server/src/actor/command.rs` (`RoomEvent`), `actor/room.rs` (el evento después del `fsync`), `api/sse.rs`, `config.rs` (`ZEMDB_MAX_SSE_STREAMS=0` y `ZEMDB_MAX_SSE_OPS_BYTES`), y sus tests. `ARCHITECTURE.md` §7 (SSE) y la configuración.

**Qué se hace:** con el SSE apagado, `GET /events` responde el código nuevo; cuando un commit o un grupo se vuelve durable, el actor codifica una vez el mensaje con sus operaciones, si entran en el tope, y lo emite como evento `ops` (base64), si además sus bytes entran en el tope total de los eventos retenidos, y si no, `head_advanced`.

**Tests exigidos.**
- Con `ZEMDB_MAX_SSE_STREAMS=0`, `GET /events` da el código nuevo y el resto del protocolo funciona igual.
- Un cliente que lee el stream recibe, en orden y sin huecos, las mismas operaciones que le daría un `/sync` (INV-Y2), con su `schema_id` y `schema_version` (INV-E3), y solo después de que son durables (INV-D7).
- Una operación más grande que el tope llega como `head_advanced`; un stream atrasado recibe `lagged`.
- Un miembro de otra sala, o sin token, no recibe nada (INV-A1).
- La ida y vuelta del mensaje nuevo y del código nuevo, y su lugar en los fuzzers de mensajes (INV-W1, W2).
- Los eventos `ops` retenidos no pasan de `ZEMDB_MAX_SSE_OPS_BYTES`, contados por los bytes que realmente se retienen: con streams que no leen en muchas salas, los eventos que no entran salen como `head_advanced`, y el tope se recupera cuando esos streams leen o se cierran y cuando el canal pisa un evento (un contador de prueba, por los dos caminos).

**Interacciones a revisar:** el tope de streams por cliente y en total; el keep-alive; el cambio de esquema en una sala abierta (`schema_reloaded` y el esquema que anuncia el evento); el apagado ordenado, que corta los streams; el *group commit* del Lote 4.

**Medición:** E2-modos, con el modo c ahora sin parche de diagnóstico, contra el modo a. **Criterio:** con 1.000 salas, menos de 0,5 ms de CPU del servidor por commit y la propagación por debajo de 2 ms en p99 (el prototipo dio 0,46 ms y 1,8 ms, §4.8.1 de las evaluaciones). El generador del arnés pasa a leer el evento nuevo en el mismo lote.

**Metodología:** §5.1, con los checklists del protocolo y de medición; las versiones largas de `zemdb-core` (los fuzzers) y del servidor.

**Documentos:** `ARCHITECTURE.md` §7 (los eventos SSE y su formato, el código nuevo, el tope total) y la configuración; `INVARIANTES.md`: INV-Y2, D7, E3, B3; `README.md` (las dos variables). Y el aviso a la Fase 4: el Lote 0 de 4.3 parte de este formato.

**Entrega:** `feat(server): the SSE can be turned off, and its events carry the operations (Fase 3.12, Lote 5)`.

**Hecho.**
- **El SSE apagado** (DS4): `ZEMDB_MAX_SSE_STREAMS=0` (`max_sse_streams`) pasa a valer, y `SseStreams::open` responde entonces `ServerError::EventsDisabled`, que es `ErrorCode::EventsDisabled` (nuevo, al final de `ErrorCode`) con HTTP 404, en un frame binario, antes de pedirle nada a la sala: no la abre, y una sala que no existe también es `EventsDisabled`. La autenticación va antes: sin token, sigue siendo `Unauthorized`. El resto del protocolo no cambia.
- **El evento `ops`** (DS5): cuando un grupo de commits se vuelve durable, después de su `fsync` y de que el head y el dedup lo toman, el actor (`RoomActor::announcement`) arma, si la sala tiene algún stream, un `ServerMessage::OpsEvent { room_id, room_incarnation, head_seq, ops, schema_version, schema_id }` (nuevo, al final de `ServerMessage`) con las operaciones nuevas del grupo (las de sus commits que no se rechazaron ni se deduplicaron: secuencias consecutivas desde el head anterior), lo codifica una vez y lo emite en base64 como `event: ops` (`RoomEvent::Ops`, que comparten todos los streams de la sala por un `Arc`). El esquema es el de la sala, con el que se validaron: una recarga en el buzón corta el grupo. La encarnación (de la revisión, abajo) es la de la sala, la que da `Registered`.
- **Los dos topes** (`actor/ops_event.rs`): el del evento, 4 KiB del frame antes del base64 (`MAX_OPS_EVENT_FRAME`; las operaciones se miden de a una con `encoded_len` y no se copian más allá del tope); y el total, `ZEMDB_MAX_SSE_OPS_BYTES` (`max_sse_ops_bytes`, 64 MiB de fábrica, de 0 a 64 GiB), un semáforo del que cada evento toma su base64 más 128 bytes (`EVENT_OVERHEAD`: el bloque que comparten sus copias y los encabezados de sus dos asignaciones) y que el permiso devuelve cuando se suelta la última copia del evento: cuando lo leyó el último stream de la sala, cuando un stream que no lo leyó se cierra, o cuando el canal lo pisa (Tokio 1.53.1, `broadcast`: `RecvGuard::drop` suelta el valor con el último receptor, `Receiver::drop` recorre lo que no leyó, `send` reemplaza el valor de la ranura, y sin receptores devuelve el valor). Si el total no tiene lugar ni para las operaciones solas, el frame no se codifica. Un grupo que no entra en alguno de los dos topes sale como `head_advanced`, como antes; con el total en 0, todos.
- **El stream termina cuando vence su token** (de la revisión, abajo): `EventStreamAuth` guarda el vencimiento del token, y el stream termina entonces, como cuando el servidor se apaga o un stream más nuevo del cliente toma su lugar. El cliente lo vuelve a abrir con un token nuevo.
- **El arnés:** el modo `rooms` del generador lee el evento nuevo (lo ignora si su head no pasa el cursor, aplica lo que sigue al cursor si su primera operación llega hasta la siguiente, y si no, hace `/sync`; un evento que no decodifica cuenta como error del SSE e invalida la corrida) y sigue entendiendo `head_advanced`, así que el mismo binario mide la base y el resultado; el modo c de `e2f_all.sh` ya no usa el parche del prototipo, que sale de `tools/perf/diagnostics/`.
- **Tests agregados** (los marcados con ⊗ fallan con el comportamiento anterior; los demás cuidan reglas que ya se cumplían y respaldan las mutaciones):
  - `actor/tests/ops_event.rs`: ⊗ `an_event_carries_its_operations_with_their_head_room_incarnation_and_schema`; ⊗ `operations_whose_frame_passes_four_kib_are_announced_without_them` (justo en el tope, un byte más, y un grupo que lo pasa con operaciones que entran solas); ⊗ `an_event_holds_its_bytes_of_the_total_until_its_last_copy_is_dropped` (su base64 y su costo fijo); `without_operations_there_is_no_event`; `with_a_total_of_zero_no_event_is_built`.
  - `actor/tests/room.rs`: ⊗ `the_ops_event_of_a_group_carries_exactly_its_new_operations` (con commits rechazados, uno repetido y uno con un cursor adelante en el medio del grupo; lo que traen los eventos es lo que da un `/sync`); ⊗ `a_schema_reload_between_waiting_commits_is_announced_between_their_events` (INV-E3); ⊗ `an_operation_too_large_for_an_event_is_announced_with_head_advanced`; ⊗ `a_stream_that_falls_behind_is_told_it_lagged`; ⊗ `retained_ops_events_stay_within_their_total_and_come_back_when_dropped` (tres salas con streams que no leen: la tercera recibe `head_advanced`, el total nunca se pasa, y vuelve por los tres caminos, con un contador de prueba, `OpsEventBudget::held`); ⊗ `an_ops_event_carries_the_incarnation_of_its_room`; `commits_waiting_behind_an_fsync_share_the_next_one`, adaptado: el grupo se anuncia una vez, después de su `fsync`, con sus operaciones (INV-D7); y, de la revisión, `a_group_whose_fsync_fails_is_never_announced` y `a_commit_whose_write_fails_is_never_announced`.
  - `api/tests/sse.rs`: ⊗ `with_no_streams_allowed_every_stream_is_refused_as_turned_off`. `tests/config.rs`: ⊗ `zero_sse_streams_turns_the_event_stream_off_and_zero_ops_bytes_turns_off_ops_events`, y la variable nueva en los tests de valores, de rangos y de TOML. `tests/error.rs`: la fila de `EventsDisabled` (de la revisión).
  - `tests/server_integration_tests.rs`, por HTTP: ⊗ `sse_ops_events_deliver_what_a_sync_returns` (tres escritores a la vez, con reintentos y rechazos: los eventos, en orden y sin huecos, son el `/sync`, con su esquema y su encarnación; INV-Y2, E3); ⊗ `a_stream_receives_only_the_events_of_its_room` (INV-A1: el token de otra sala es 403, sin token 401, y los commits de otra sala no llegan); ⊗ `with_the_event_stream_off_events_are_refused_and_the_protocol_works`; `sse_announces_an_operation_too_large_for_an_event_with_head_advanced` (reemplaza a `sse_signals_head_advanced`); y, de la revisión, ⊗ `the_ops_events_of_a_recreated_room_say_their_incarnation` y ⊗ `with_the_event_stream_off_events_do_not_open_the_room`.
  - `tests/http_edge_tests.rs`: ⊗ `a_stream_ends_when_its_token_expires` (de la revisión).
  - `zemdb-core`: `server_messages_keep_their_wire_indexes` (cada mensaje en su índice y ninguno sin listar) y `EventsDisabled` en `error_codes_keep_their_wire_indexes`; el mensaje nuevo en el generador al azar de `ServerMessage` y en los mensajes de muestra, así que entra en la ida y vuelta (INV-W2) y en los fuzzers de decodificación (INV-W1).
  - **Adaptados,** porque un commit ahora se anuncia con `ops`: `open_sse_stream_outlives_every_timeout` y `a_third_sse_stream_of_a_client_ends_its_oldest` (`http_edge_tests.rs`) esperan `event: ops`, y `concurrent_commits_are_all_sequenced_and_signalled` (`room_actor_tests.rs`) toma el head de los dos eventos.
- **Mutaciones verificadas,** cada una con algún test que falla: sin el tope del frame; sin ninguno de los dos chequeos del tope; un evento que no toma bytes del total; uno que cuenta el frame en lugar del base64; uno que nunca devuelve sus bytes; uno que los devuelve enseguida; el head del evento, el de su primera operación; todos los grupos como `head_advanced`; un evento con solo la primera operación nueva del grupo; uno que trae también las de los reintentos; la versión siguiente del esquema; el anuncio antes del `fsync` del grupo; el SSE que nunca se apaga; `ops` escrito como `head_advanced` en el stream; 0 streams no aceptado; la variable sin leer; el total sin máximo; `EventsDisabled` como 429 y como el código `RateLimited`; el código y el mensaje nuevos fuera del final de sus enums; y, de la revisión, el anuncio de un grupo cuya escritura o `fsync` falla (que sobrevivía a la suite: lo encontró la revisión), la suscripción antes de rechazar el stream apagado (también sobrevivía), el stream que no termina con su token, el evento sin su encarnación y el total sin el costo fijo. **Sin efecto observable,** como se esperaba: el corte temprano de la medición de las operaciones (el frame entero se chequea igual), no armar el evento sin streams (el envío falla y el evento se suelta enseguida) y no codificar el frame si el total no tiene lugar (el permiso tampoco se toma después); los tres ahorran trabajo, no cambian lo que se emite.
- **Interacciones revisadas:**
  - **El tope de streams, por cliente y en total:** no cambia; con 0, se responde `EventsDisabled` antes de tomar un lugar.
  - **El keep-alive:** no cambia; un evento `ops` es un evento más.
  - **El cambio de esquema con la sala abierta:** una recarga en el buzón corta el grupo, así que cada evento lleva la versión con que se validaron sus operaciones, y `schema_reloaded` sale entre los dos (`a_schema_reload_between_waiting_commits_is_announced_between_their_events`).
  - **El apagado ordenado:** corta los streams que hyper consulta, como antes, y sus receptores devuelven los bytes. Uno cuyo cliente dejó de leer no se corta hasta la gracia de 10 s, y el lote lo agrava (de la revisión; al backlog, abajo).
  - **El *group commit*:** un evento por grupo, con sus operaciones nuevas, después de su `fsync` (`commits_waiting_behind_an_fsync_share_the_next_one`); un grupo cuya escritura o `fsync` falla no se anuncia (de la revisión, con sus dos tests).
  - **La sala recreada con el mismo id** (de la revisión): un token de la sala borrada sigue abriendo el stream, sin registro, y las operaciones de la sala nueva pueden seguir a su cursor viejo; el evento lleva la encarnación, para que el SDK no lo aplique (INV-K1, 🆕 4.3).
  - **El vencimiento del token** (de la revisión, la fila de D72 de `INVARIANTES.md` §4): el stream termina entonces.
  - **El respawn del actor:** el canal viejo se cierra con el actor; sus streams leen lo que quedaba y terminan, y los eventos vuelven al soltarse sus receptores. Un stream no mezcla eventos de dos actores.
  - **El cierre por inactividad:** sigue contando los receptores; armar un evento no los cambia.
  - **El tiempo que un stream que no lee retiene eventos:** cuando se llena el buffer de su conexión, el stream deja de pedir eventos y el canal los retiene (a lo sumo 256 por sala), hasta que el tope de 60 s de una respuesta trabada (`response_stall_timeout_secs`) corta la conexión, que suelta el receptor; un cliente que lee un frame por plazo los retiene sin límite de tiempo, dentro de esos 256. Lo que cada stream copia al escribir un evento espera en la cola de escritura de su conexión, como cualquier respuesta (`ARCHITECTURE.md` §7.2).
  - **Un stream reemplazado** por el tercero de su cliente termina cuando hyper consulta su cuerpo (de la revisión; preexistente, agravado: al backlog).
  - **La falla del sellado después de un grupo durable:** el grupo se anuncia como antes, y después la sala se detiene.
  - **El presupuesto de respuestas** (DS1): independiente; el evento no reserva de él.
  - **El CPU:** armar un evento cuesta, en el actor, codificar hasta 4 KiB y su base64, una vez por grupo y solo con algún stream; escribirlo, en los workers, una copia por stream.
  - **La parte de cada cliente del total** (INV-B5): no hay; la acota el canal de cada sala, y agotarlo solo cambia `ops` por `head_advanced` en las demás salas, que pierden lo que el evento ahorra de CPU (un límite aceptado nuevo en `INVARIANTES.md`, INV-B5).
  - **El SDK** (`zemdb-client`): no lee el SSE todavía (4.3); el mensaje y el código nuevos van al final, y sus `match` tienen un caso por defecto. El aviso a 4.3 está en el plan de la Fase 4 (DC26 y su Apéndice A): la encarnación, la regla del cursor y el stream que termina con su token.
  - **D78** (§4, riesgo aceptado): un cliente que aplica eventos presenta su cursor menos seguido, así que una secuencia reutilizada tiene menos ocasiones de detectarse (de la revisión, nota).
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (1.265 tests) y `cargo doc` sin advertencias; la compilación y el clippy a `wasm32-unknown-unknown` de core, storage y client; las pruebas largas de `zemdb-core` y del servidor (`cargo test -p zemdb-core -p zemdb-server --release -- --ignored`), 28 antes, sobre `e4300ef`, y 28 después, también tras los arreglos de la revisión; y los checklists del protocolo y de medición:
  - **Protocolo:** las variantes nuevas van al final de su enum (con un test que lo fija para `ServerMessage`, que no lo tenía); la ida y vuelta del mensaje nuevo, al azar y de muestra, y su lugar en los fuzzers de decodificación (INV-W1, W2). El lote no promete los mismos bytes de nada que ya existía.
  - **Medición:** abajo.
- **Medición.**
  - **El entorno (DP3):** la misma VM que el Lote 4b (4 vCPU Intel Xeon a 2,10 GHz, 15 GB, kernel 6.18.44, rustc 1.97.0); el servidor en los núcleos 0 y 1 y el generador en el 2 y el 3, con el mismo binario del generador para la base y el resultado (el del lote, que lee el evento `ops` y entiende `head_advanced`); tmpfs, `/dev/shm`. Los dos servidores, compilados en release limpio (sin las features de los tests).
  - **Lo que se compara:** la base es `e4300ef`; el resultado, este lote con los arreglos de la revisión. En la base no hay evento `ops`, así que su modo c es el modo b (señales y `Ack` periódico).
  - **El diseño:** tres rondas intercaladas, base y resultado, de E2-modos con 1.000 salas (modos a y c) y de los controles. La tabla da la mediana y, entre paréntesis, el rango. Los datos crudos, en `tools/perf/results/fase3_12/lote5/` (`final-*`; y `first-*`, la medición de antes de la revisión).

  | Escenario | Base `e4300ef` | Resultado |
  |---|---|---|
  | E2-modos, 1.000 salas, modo c: CPU del servidor por commit, propagación p99 | 0,850 ms (0,848–0,862), 3,7 ms (3,6–4,2) | 0,474 ms (0,472–0,486), 1,46 ms (1,37–1,58) |
  | E2-modos, 1.000 salas, modo a: CPU por commit, propagación p99 | 1,161 ms (1,155–1,204), 5,9 ms (5,1–6,1) | 0,469 ms (0,452–0,476), 1,76 ms (1,65–2,10) |
  | E2-tmpfs, 500 salas (control): CPU del servidor, propagación p99 | 66%, 2,8 ms (2,6–2,9) | 24%, 1,2 ms (1,1–1,4) |
  | E1-tmpfs, 1 pedido en vuelo (control): commits/s | 7.587 (7.453–7.617) | 7.368 (7.134–7.853) |
  | E1-tmpfs, 8 pedidos en vuelo (control): commits/s | 26.510 (25.976–27.150) | 25.893 (25.513–26.548) |

  - **El criterio:** se cumple, con una salvedad sobre la máquina. Con 1.000 salas, el modo c gasta 0,474 ms de CPU del servidor por commit (las tres rondas por debajo de 0,5) y propaga en 1,46 ms en p99 (por debajo de 2). Contra el modo a de la base, el CPU baja un 59% y la propagación en p99, un 75%.
  - **La salvedad:** la primera medición, con el mismo diseño, antes de los arreglos de la revisión, dio 0,501 ms (0,494–0,515) y 1,82 ms, en una sesión en que la máquina estaba más lenta: la misma base gastaba 0,931 ms en el modo b, contra 0,850 en la final. El cociente contra el modo b de la misma sesión se mantiene (0,538 en la primera, 0,558 en la final; el del prototipo, 0,535: 0,46 contra 0,86 ms), así que el lote reproduce la ganancia del prototipo, y que el número absoluto quede por debajo de 0,5 depende de la máquina: en la primera sesión no lo estaba. Los arreglos agregan 17 bytes por evento (la encarnación) y no explican la diferencia.
  - **El perfil** (`perf`, 15 s del resultado de la primera medición en modo c): armar y emitir el evento es un ~2% del CPU del servidor (`RoomActor::announcement`, 1,1%; `broadcast::Sender::send`, 1,0%), unos 10 µs por commit. El resto: HTTP y las escrituras del SSE a los diez streams de cada sala (`tcp_sendmsg`, ~20%; el cuerpo SSE de axum, ~9%), el actor (~13%) y el traspaso de cada `fsync` al pool de E/S del Lote 3 (~4%, más sus despertares), que el prototipo no tenía: se midió con la E/S en línea.
  - **Los modos a y c del resultado** quedan iguales, y el modo a deja de ser "`Ack` después de cada `/sync`": el generador aplica el evento sin `/sync`, así que tampoco manda `Ack` (0,1 a 0,3 por segundo de cada uno, contra 9.230 en la base). El control E2-tmpfs con 500 salas cambia de carga de la misma forma (sin `/sync` ni `Ack`): mejora, pero no compara lo mismo.
  - **Los controles E1-tmpfs:** un 3% abajo con 1 pedido en vuelo y un 2% con 8, dentro de su dispersión más un 10% (las repeticiones difieren en un 2 a 9%); el lote no toca el camino de un commit sin streams.
  - **Las corridas son válidas:** sin errores, el head igual a lo confirmado, sin huecos ni errores del SSE, sin clientes atrasados al final, y el generador por debajo del 82% de un núcleo.
- **La revisión de delta,** por dos agentes aparte, de contexto fresco y solo lectura, uno con el foco en la entrega y el protocolo y otro en la memoria, los recursos, la concurrencia y la medición, con las seis preguntas. Cada uno encontró un hallazgo Medio, con un test que lo reproduce, y los dos se corrigieron:
  - **Un stream seguía trayendo los datos después de que vencía su token (recursos, Medio).**
    - **El caso:** la fila de D72 de `INVARIANTES.md` §4 aceptaba que un stream siguiera vivo después de que vencía su token, porque "SSE solo notifica; cualquier dato se pide con un token vigente".
    - **Lo que pasaba:** con el evento `ops`, el stream trae los datos. Los tokens no se revocan: su vencimiento es lo único que le quita el acceso a un miembro, y uno que abrió un stream antes seguía recibiendo todo lo que se escribe, sin plazo (INV-A1).
    - **El arreglo:** el stream termina cuando vence su token. Lo prueba `a_stream_ends_when_its_token_expires`, y la fila de D72 queda en lo que un stream mantiene abierta su sala.
  - **Un stream daba operaciones de otra encarnación de la sala (entrega, Medio).**
    - **El caso:** el stream se abre solo con el token, que firma el cliente, la sala y el vencimiento, sin pedir registro; un token de una sala borrada abre el stream de la sala creada después con el mismo id.
    - **Lo que pasaba:** las operaciones de la sala nueva que siguen al cursor viejo de ese cliente llegaban en eventos que no decían de qué encarnación eran, y la regla publicada (aplicar si sigue al cursor) las mezclaba con su estado (INV-K1 ★, de origen (a); hoy no las aplica ningún cliente del repo salvo el generador). Por `/sync` el mismo cliente recibe `ClientNotRegistered`, y al registrarse, `RoomReset` (INV-Y7).
    - **El arreglo:** el evento lleva `room_incarnation`, la de `Registered`, como los chunks del relay desde DC23, y el cliente no aplica uno de otra encarnación y se registra con su cursor. Lo prueban `the_ops_events_of_a_recreated_room_say_their_incarnation` y `an_ops_event_carries_the_incarnation_of_its_room`. La otra opción que dio la revisión, exigir el registro para abrir el stream, se descartó: cambiaba qué clientes pueden escuchar una sala (los tests de snapshots se suscriben antes de registrar a nadie) y hacía depender un stream del roster.
  - **Bajos, corregidos:**
    - **Dos mutaciones sobrevivían:** anunciar un grupo cuya escritura o `fsync` falla, y suscribir antes de rechazar el stream apagado; ahora con tests (arriba).
    - **El total no contaba el costo fijo de cada evento** (unos 70 a 80 bytes, 1,35 veces lo contado en la Evaluación 2): ahora cuenta 128 bytes más por evento.
    - **Con el total agotado, cada grupo se codificaba igual:** ahora no se codifica si el total no tiene lugar ni para las operaciones solas.
    - **La fila de `EventsDisabled`** en `each_error_maps_to_its_code_and_status`.
    - **La regla del cliente en `ARCHITECTURE.md` §7.2 y en el plan de la Fase 4:** un evento cuyo head no pasa el cursor se ignora (decía que llevaba a un `/sync`, uno inútil por cada commit propio), y `head_advanced` llega también cuando el total no tiene lugar.
    - **El generador,** ante un evento que no decodifica, no hacía `/sync`, al revés de lo que decía su comentario; ahora lo cuenta como error del SSE.
    - **Los textos:** la cota de lo que copia cada stream al escribir un evento, la pérdida de CPU de las salas cuando el total se agota (INV-B5), y lo que el generador no hace en el modo a (abajo, en la medición).
  - **Bajos, al backlog** (`ROADMAP.md`, punto 19; preexistentes, agravados por el lote): el apagado no corta un stream cuyo cliente dejó de leer, y espera la gracia de 10 s sin persistir los rosters (INV-L5 lo permite); y un stream reemplazado sigue vivo, con lo que retiene la cola de escritura de su conexión, hasta que hyper consulta su cuerpo. Lo cerraría que el apagado y el reemplazo cierren la conexión sin esperar a hyper.
  - **Notas:** `lagged` se prueba en el canal del actor, no en el stream (preexistente); el generador ignora `lagged` (preexistente); el test del total en 0 no ve si el evento se arma (con el chequeo previo, no se arma).
  - **La pregunta 6:** las corridas son válidas según DP3 (el mismo binario del generador, verificado con su sha256; sin errores, el head igual a lo confirmado, sin huecos); el criterio de propagación queda respaldado y el de CPU no se cumplía por poco en la primera medición (la revisión se hizo sobre ella; la final, abajo, lo cumple).
- **Desvíos:**
  - **El criterio de CPU** se cumple en la medición final (0,474 ms) y no se cumplía, por 0,001 ms, en la primera (0,501 ms), con la máquina más lenta (arriba). Queda anotado como salvedad, no como desvío.
  - **El stream termina cuando vence su token,** aunque DS5 decía que su autenticación no cambiaba: lo pidió la revisión, porque el stream ahora trae datos.
  - **El evento lleva la encarnación de la sala,** que DS5 no nombraba: lo pidió la revisión.
- **El tamaño:** unas 206 líneas de producción agregadas, sin comentarios ni blancos (12 quitadas); dentro del tope de ~300 de `docs/METODOLOGIA.md` §4 y cerca de la estimación del plan (unas 200).

### Lote 6 · El catch-up sin copias · ✅ (sin cumplir su criterio: lo sigue el 6b)
**Objetivo:** que el catch-up y el `/sync` no copien las operaciones del búfer en RAM (DS6).

**Archivos:** `crates/server/src/log/hot_buffer.rs`, `log/tiered_log.rs` (`fetch_deltas`), `actor/room.rs`, `api/data_plane.rs` (el tipo que serializa las respuestas), y sus tests.

**Qué se hace:** el búfer guarda `Arc<SequencedOperation>`; las lecturas devuelven `Arc`; las respuestas se codifican con un tipo del servidor que da los mismos bytes que el `ServerMessage` de hoy.

**Tests exigidos.**
- Para respuestas con operaciones del búfer, de disco tibio y de disco frío, los bytes son idénticos a los de codificar el `ServerMessage` equivalente (INV-W2).
- La reserva de DS1 cuenta cada operación de cada respuesta, aunque se comparta (INV-B1).
- La retención del búfer (por operaciones, bytes y TTL) no cambia.

**Interacciones a revisar:** la contabilidad del búfer en RAM (por lo que ocupa en memoria, D73); el evento `ops`, que también lleva operaciones; la poda.

**Medición:** E1-actor con 8 y 64 pedidos en vuelo. **Criterio:** con 64, al menos el 80% de los commits por segundo de 8 (hoy, la mitad: 34.000 contra 68.800).

**Metodología:** §5.1, con los checklists del protocolo y de medición.

**Documentos:** `ARCHITECTURE.md` §5.2 (el búfer en RAM); `INVARIANTES.md`: INV-B1.

**Entrega:** `perf(server): share the operations of the RAM buffer in catch-ups and syncs instead of copying them (Fase 3.12, Lote 6)`.

**Hecho.**
- **El búfer en RAM comparte sus operaciones** (DS6): `HotBuffer` guarda cada operación en un `Arc<SequencedOperation>` y `get_range` devuelve esos mismos `Arc`, sin copiarlas; `TieredLog::fetch_deltas` y `fetch_from_memory` devuelven `Vec<Arc<SequencedOperation>>` (el tipo `Deltas`), y lo que se lee de los segmentos y de `active.wal` se envuelve en un `Arc` al decodificarse, sin copia. `CommitResponse::catchup_ops` y `SyncBatchResponse::ops` pasan a ser `Vec<Arc<SequencedOperation>>`.
- **El grupo de commits:** cada operación nueva se crea una vez, en un `Arc` (`Decision::New`), que comparten el registro que va al log, el búfer y la respuesta del commit que la escribió: antes, el registro llevaba una copia. `WarmDiskLog::append_records` y `TieredLog::append_group` toman los `Arc`; el registro en disco es el mismo.
- **Las respuestas:** `CommitAck` y `SyncBatch` se codifican con `OpsReply` (en `api/data_plane.rs`), un tipo del servidor con los campos de esas dos variantes y las operaciones compartidas, que se serializa como la variante de `ServerMessage` (su índice, 0 y 2, y sus campos en su orden; un `Arc` se serializa como lo que apunta). `binary_response` y `budgeted_response` toman un `ServerMessage` o un `OpsReply` (el rasgo `Frame`). El `OpsReply` se suelta al codificarse, así que la respuesta retiene sus operaciones solo mientras se arma y se codifica, como antes sus copias.
- **Lo que no cambia:** la reserva de DS1 (cada respuesta suma, por cada operación que lee, lo que ocupa en memoria más lo que ocupará codificada, la comparta o no, hasta el frame que una reserva nunca pasa), la ventana del búfer (por cantidad, bytes y edad, con la misma medida), el evento `ops` (que sigue copiando las operaciones nuevas de un grupo una vez, al codificarlo, y solo con algún stream) y los bytes de cada respuesta.
- **Tests agregados** (los marcados con ⊗ fallan con el comportamiento anterior, con el esqueleto que ya tenía la API nueva y copiaba; los demás cuidan reglas que ya se cumplían y respaldan las mutaciones):
  - `log/tests/hot_buffer.rs`: ⊗ `reads_share_the_operations_of_the_buffer` (dos lecturas comparten cada operación entre ellas y con la que se agregó); `the_window_drops_the_operations_a_read_holds_like_any_other` (por cantidad, por bytes y por edad, con una lectura que las retiene; después, la lectura es su único dueño).
  - `log/tests/tiered_log.rs`: ⊗ `each_response_reserves_the_operations_it_shares` (dos respuestas que leen las mismas operaciones del búfer las comparten, y cada una las reserva enteras: el doble que una; INV-B1); y, de la revisión, `a_read_that_goes_on_from_disk_into_ram_shares_the_operations_of_the_buffer` (una lectura que empieza en un segmento y sigue en el búfer, como después de un sellado que falla una vez renombrado `active.wal`, comparte lo que toma del búfer).
  - `actor/tests/room.rs`: ⊗ `catch_ups_and_syncs_share_the_operations_of_the_ram_buffer` (la operación de un cliente al día, el catch-up de uno atrasado y un `/sync` comparten cada operación).
  - `api/tests/data_plane.rs`: `replies_with_operations_from_every_tier_are_the_bytes_of_their_message` (por HTTP, en una sala con segmentos de dos operaciones comprimidos a frío: un `/sync` y un catch-up desde los niveles de disco, frío, tibio y `active.wal`, y otro par desde la RAM, cada uno con los bytes exactos del `ServerMessage` armado con las operaciones que se escribieron; uno con `has_more`; INV-W2); `an_ops_reply_is_the_bytes_of_its_message_with_every_flag` (las dos variantes con cada valor de sus dos flags).
  - **Adaptados** al tipo nuevo, sin cambiar lo que prueban: los que comparan o mueven las operaciones de una lectura (`seqs` y `assert_contiguous` aceptan las dos formas; `owned` las copia para comparar) y los que arman grupos de registros.
- **Mutaciones verificadas,** cada una con algún test que falla: una lectura que copia las operaciones del búfer; el registro del grupo con una copia de la operación nueva, en lugar de la que da la respuesta del commit; una lectura que no reserva lo que ya comparte otra respuesta (la reserva contada una sola vez); la ventana que no suelta las operaciones que retiene una lectura, por cantidad y bytes, o por edad; `CommitAck` y `SyncBatch`, cada uno con otro índice de variante y con sus dos flags cambiados de lugar; `CommitAck` sin su `schema_id`; y `SyncBatch` con el head después de las operaciones. De la revisión, dos más: la parte en RAM de una lectura que empezó en disco, copiada (sobrevivía a la suite: ningún test que llegaba a esa rama miraba si compartía; ahora la atrapa `a_read_that_goes_on_from_disk_into_ram_shares_the_operations_of_the_buffer`), y el camino rápido del cliente al día respondiendo con una copia (la atrapa `catch_ups_and_syncs_share_the_operations_of_the_ram_buffer`).
- **Interacciones revisadas:**
  - **La contabilidad del búfer en RAM** (D73): cada operación sigue cobrando `max(wire, 24 × valores)`, y la ventana la suelta igual aunque una respuesta la retenga (`the_window_drops_the_operations_a_read_holds_like_any_other`). El `Arc` agrega 16 bytes por operación (sus dos contadores) que no se cobran, como no se cobraba el tamaño del `SequencedOperation` mismo (96 bytes). Una operación que la ventana soltó y una respuesta todavía retiene está en la reserva de esa respuesta, así que la memoria de las operaciones sigue acotada por la ventana más el presupuesto de respuestas; dos respuestas que comparten una operación la cuentan las dos, lo que sobreestima la memoria, como antes con las copias.
  - **El evento `ops`:** se arma con las operaciones nuevas del grupo, que ahora son los `Arc` de las decisiones (`announcement` los desreferencia); sigue codificándose una vez, y sus tests del Lote 5 pasan sin cambios.
  - **La poda** (por cursor, por TTL y por cuota): borra archivos y saca del búfer lo que ya no se retiene; una respuesta que tenía una operación la sigue teniendo hasta codificarse, como tenía su copia.
  - **La escritura y el `fsync` de un grupo que fallan:** los registros se sueltan con el log o con las decisiones, que responden `Unavailable`; nada queda retenido.
  - **La apertura:** el búfer se rehidrata con las operaciones decodificadas de `active.wal`, cada una en un `Arc`, sin copia.
  - **El pool de E/S** (DS2): el log viaja con su búfer al pool y vuelve; un `Arc<SequencedOperation>` es `Send` y `Sync`, y sus contadores son atómicos, así que una respuesta puede soltar una operación en el worker de HTTP mientras el log está en el pool.
  - **El tamaño de una respuesta** (`fit_in_response`, `encoded_len`): mide lo mismo, porque un `Arc` se serializa como lo que apunta.
  - **El SDK y el arnés:** los bytes no cambian; el modo `inproc` de `tools/perf`, que lee `CommitResponse`, compila sin cambios.
  - **El tope de un frame de una reserva** (de la revisión): pasado un frame, una reserva crece sin cobrar (`Reservation::grow`), así que "cada respuesta cuenta sus operaciones" vale hasta ese tope, como antes con las copias; `ARCHITECTURE.md` e INV-B1 lo dicen.
  - **Un sellado que falla, o un segmento perdido al final de los niveles de disco** (de la revisión): una lectura que empieza en disco puede seguir en el búfer; comparte lo que toma de él (el test de arriba).
  - **El cierre, el respawn y el apagado con respuestas en vuelo** (de la revisión): el búfer se suelta con el actor, y las respuestas que todavía no se codificaron se quedan con sus operaciones, dentro de su reserva.
  - **Dónde se cuentan y se sueltan las operaciones** (de la revisión): sumar los contadores pasa en el actor; restarlos y liberar lo que ya nadie tiene, en el worker de HTTP que codifica la respuesta (en E1-actor, en el generador). Es parte del costo medido.
  - **El tipo de lo que codifica `binary_response`** (de la revisión): acepta un `ServerMessage` o un `OpsReply` (el rasgo `Frame`), no cualquier `Serialize`.
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (1.272 tests) y `cargo doc` sin advertencias; la compilación y el clippy a `wasm32-unknown-unknown` de core, storage y client; las pruebas largas del servidor (`cargo test -p zemdb-server --release -- --ignored`), las 13 de antes, sobre `2c0a908`, y las 13 después, también tras los arreglos de la revisión; y los checklists del protocolo y de la medición:
  - **Protocolo:** no hay variantes nuevas; el lote promete los mismos bytes, y dos tests los comparan (`an_ops_reply_is_the_bytes_of_its_message_with_every_flag` y, por HTTP y con operaciones de cada nivel, `replies_with_operations_from_every_tier_are_the_bytes_of_their_message`). Las variantes de `ServerMessage` siguen en sus índices (`server_messages_keep_their_wire_indexes`, en core).
  - **Medición:** abajo.
  - **La prueba larga de tiempos** `fetch_cost_does_not_grow_with_the_number_of_segments` falló con la VM cargada, sobre la base y sobre el lote; sola pasa siempre, tres veces en cada uno (unos 2 ms con 100 segmentos y con 50.000). Es preexistente: al backlog de `ROADMAP.md` (punto 19).
- **Medición.**
  - **El entorno (DP3):** una VM como la de los Lotes 4b y 5 (4 vCPU Intel Xeon a 2,10 GHz, 15 GB, kernel `6.18.44-fc-v233`, rustc 1.97.0), tmpfs en `/dev/shm`, el 4 de Noviembre de 2026; los números se comparan solo dentro de esta medición. E1-actor corre el actor y el generador en el mismo proceso, en los núcleos 0 y 1: el generador lleva el servidor adentro, así que hay un binario del generador por árbol, compilado de la misma fuente de `tools/perf`. Las corridas por HTTP, con el servidor en 0 y 1, el generador en 2 y 3, y el mismo binario del generador para la base y el resultado. Los servidores, en release limpio.
  - **Lo que se compara:** la base es `2c0a908` (el Lote 5); el resultado, este lote.
  - **El diseño:** tres rondas intercaladas, base y resultado, del escenario del lote (E1-actor con 8 y 64 pedidos en vuelo) y de los controles (E1-tmpfs con 1 y 8, E2-tmpfs con 500 salas). Como la primera medición dejó la propagación en p99 del control E2 cerca del límite del criterio, se corrieron cuatro rondas más de ese control, empezando por el resultado: 7 de cada lado. La tabla da la mediana y, entre paréntesis, el rango. Los datos crudos, en `tools/perf/results/fase3_12/lote6/`.

  | Escenario | Base `2c0a908` | Resultado |
  |---|---|---|
  | E1-actor, 8 pedidos en vuelo: commits/s, p99 | 42.438 (40.972–42.960), 376 µs | 60.106 (59.490–60.662), 279 µs |
  | E1-actor, 64 pedidos en vuelo: commits/s, p99 | 10.872 (10.241–11.882), 9,0 ms | 31.280 (31.046–31.641), 3,5 ms |
  | E1-actor, con 64 sobre con 8 | 26% | 52% |
  | E1-tmpfs, 1 pedido en vuelo (control): commits/s | 6.696 (5.779–6.945) | 6.797 (6.467–7.149) |
  | E1-tmpfs, 8 pedidos en vuelo (control): commits/s | 24.059 (23.728–24.415) | 24.206 (22.987–24.255) |
  | E2-tmpfs, 500 salas (control, 7 rondas): propagación p99, CPU del servidor | 2,5 ms (2,1–3,1), 25% | 2,8 ms (2,1–3,4), 25% |

  - **El criterio no se cumple:** con 64 pedidos en vuelo, el resultado hace el 52% de los commits por segundo de 8, contra el 80% que pedía el plan (la base, el 26%). Con 64, casi el triple que la base; con 8, un 42% más. El dueño del proyecto aceptó el desvío el 6 de Noviembre de 2026 y dejó lo que falta para el Lote 6b (DS10).
  - **Por qué, según el perfil** (`perf`, 8 s de E1-actor, base y resultado con 64, y resultado con 8):
    - En la base con 64, `HotBuffer::get_range` es el 55% del CPU, casi todo copiar las operaciones (`Vec<Value>::clone`, `malloc`, `free`). En el resultado ya no hay copia.
    - En el resultado con 64, `get_range` sigue siendo el 34%: el 21% es `ReadBudget::admit`, que hace crecer la reserva de la respuesta una vez por operación (un mutex, la parte del cliente en un `HashMap` y el semáforo del total), y el resto, sumar los contadores de los `Arc`; soltar la respuesta (restarlos) es otro 7,5%. Y `fit_in_response` vuelve a medir cada operación con `encoded_len` (~9%).
    - Con 64 en vuelo, cada commit de un grupo trae un catch-up de hasta unas 64 operaciones, así que todo lo que se hace por operación de cada catch-up crece con el cuadrado del grupo. El plan suponía que la copia era lo único así (§3.7.3 de las evaluaciones): lo era antes del *group commit* y del presupuesto por operación.
  - **Un prototipo de diagnóstico,** fuera del repo, con las dos cosas de DS10 (la reserva de a tramos de 256 KiB, que devuelve lo que no usó al terminar, y `fit_in_response` sin la segunda medición), en rondas intercaladas con el resultado: con 64, unos 98.000 commits por segundo (95.630–100.754), el 156% de los de 8 (unos 62.900); con la reserva por tramos sola, unos 59.700 con 64 (57.545–61.883); sin la segunda medición sola, unos 39.000. El resultado, en las mismas rondas, unos 32.000 con 64 y 60.900 con 8. Es lo que mide el Lote 6b, sobre su implementación.
  - **Los controles:** E1-tmpfs con 1 y 8 pedidos en vuelo, un 1,5% y un 0,6% arriba, dentro de la dispersión. E2-tmpfs con 500 salas: con 3 rondas, la propagación en p99 del resultado quedaba un 29% arriba (2,4 contra 3,1 ms), justo dentro de la dispersión de la base (2,1 a 2,9 ms) más un 10%; con 7 rondas de cada lado, un 9% arriba, con los rangos superpuestos y el mismo CPU y la misma mediana: el p99 de esa carga es ruido de planificación, y el lote no cambia sus catch-ups, de una o dos operaciones.
  - **Las corridas son válidas:** sin errores, el head igual a lo confirmado, sin huecos ni errores del SSE, sin clientes atrasados al final; en E1-actor, el proceso (actor y generador) usa el 135% de sus dos núcleos; en las corridas por HTTP, el generador, por debajo del 75% de un núcleo.
- **La revisión de delta,** por un agente aparte, de contexto fresco y solo lectura, con las seis preguntas (el lote es chico y no toca la durabilidad, así que fue uno). Sin hallazgos Altos ni Medios:
  - **Un Bajo, arreglado:** si la parte en RAM de una lectura que empezó en disco copiaba las operaciones, ningún test lo notaba: los que llegan a esa rama (un sellado que falla, un segmento perdido) no miraban si compartían. Lo fija `a_read_that_goes_on_from_disk_into_ram_shares_the_operations_of_the_buffer`.
  - **Las notas, aplicadas:** el texto de INV-B1 y de `ARCHITECTURE.md` decía que cada respuesta cuenta sus operaciones "enteras", sin el tope de un frame de una reserva; INV-Y2, que DS6 nombra, no decía cómo se sostiene con operaciones compartidas (no cambian después de entrar al búfer), ni INV-B1 tenía un caso de operaciones compartidas para intentar romperla; `binary_response` aceptaba cualquier `Serialize` (ahora, el rasgo `Frame`); y un test adaptado (`damage_in_the_last_group_after_its_fsync_is_truncated_from_the_damage`) comparaba menos que antes. Las cuatro interacciones que faltaban están arriba.
  - **Para el Lote 6b,** en su sección del plan: el error del prototipo en la reserva por tramos (acreditaba el tramo entero aunque, pasado el tope de un frame, la reserva creciera menos), el largo de los catch-ups con 8 y 64, la proporción de la línea de base de la fase (con 64, el 64% de 8 sobre `0b03a77`, en otra VM; el 26% sobre `2c0a908`) y el diseño de su medición.
  - **Sobre la medición** (pregunta 6): es válida según DP3 y §5.2; las medianas de la tabla coinciden con los datos crudos; los controles pasan también con solo las tres primeras rondas de E2, así que agregar cuatro después de verlas no cambia el veredicto; y el perfil explica el desvío, con dos costos que el análisis no repartía: los contadores de los `Arc` (~15%) y los cambios de contexto (~18%), que siguen con el prototipo.
- **El tamaño:** unas 134 líneas de producción agregadas, sin comentarios ni blancos (38 quitadas), dentro del tope de ~300 de `docs/METODOLOGIA.md` §4; la estimación del plan era de unas 80: unas 95 son `OpsReply` y su serialización, que repite los campos de las dos variantes. Toca además `actor/command.rs` y `log/warm_disk.rs`, que el plan no nombraba.
- **El desvío,** con el visto bueno del dueño del proyecto (6 de Noviembre de 2026): el lote cierra con su criterio sin cumplir (con 64 pedidos en vuelo, el 52% de los commits por segundo de 8, contra el 80%). Lo que falta va al Lote 6b, con DS10 (§3), que se confirma al arrancarlo.

### Lote 6b · La reserva por tramos · ✅ Hecho
**Objetivo:** que un catch-up no visite el presupuesto de respuestas ni vuelva a medir sus operaciones una vez por operación (DS10), con el criterio que el Lote 6 no cumplió.

**Archivos:** `crates/server/src/log/budget.rs` (`ReadBudget`), `response_budget.rs` (`Reservation`), `actor/room.rs` (`fit_in_response`), y sus tests.

**Qué se hace:** lo de DS10, con el mecanismo que el dueño del proyecto adoptó al arrancar el lote: la reserva por tramos de 256 KiB, sin tramos en una lectura después de uno que no entra, y sin `fit_in_response`.

**Lo que dejó el Lote 6** (su nota **Hecho**, y su revisión de delta):
- **El prototipo tiene un error en la reserva por tramos:** `Reservation::grow` devuelve que creció aunque, pasado el tope de un frame, crezca menos que lo pedido, y el prototipo acredita el tramo entero; al terminar, devuelve de más, y una respuesta cerca de un frame queda reservando hasta un tramo menos de lo que debe (INV-B1). Hay que acreditar solo lo que creció.
- **El largo de los catch-ups:** medirlo con 8 y 64 pedidos en vuelo (como el generador instrumentado del Lote 2). Con el *group commit* y el cursor que manda el generador (la secuencia más alta que vio confirmada), un catch-up puede llegar a `COMMIT_CATCHUP_BATCH` (100 operaciones), no a unas 64.
- **La proporción de la línea de base de la fase:** sobre `0b03a77`, en otra VM, E1-actor hacía con 64 el 64% de los commits por segundo de 8 (27.087 contra 42.545); sobre `2c0a908`, el 26%. Con 8 no cambió; con 64 se perdió entre los Lotes 2 y 5, y ningún control lo vio (ninguno corre con 64). Una hipótesis, sin medir: antes del Lote 2, con la reserva de un frame por respuesta y un solo cliente para los 64 trabajadores del generador, muchos commits salían sin catch-up.
- **El diseño de la medición:** al menos tres rondas intercaladas de cada variante (una mitad del prototipo, con 8, varió un 13% en dos), y la base medida en la misma sesión: la misma `2c0a908` dio en E2-tmpfs con 500 salas 2,5 ms de propagación en p99 en el Lote 6 y 1,2 en el Lote 5.
- **Lo que queda por operación con los tramos:** los contadores de los `Arc` (un ~15% del CPU con 64, al sumarlos en el actor y restarlos al soltar la respuesta) y los cambios de contexto (~18%) siguen ahí.

**Tests exigidos.**
- Una lectura cuyas operaciones entran en un tramo visita el presupuesto una vez, y al terminar la reserva de su respuesta es la misma que con la reserva por operación (INV-B1).
- Con el presupuesto o la parte del cliente casi llenos, la lectura se corta en la misma operación que hoy, con `has_more`, y `Unavailable` sigue siendo para cuando no entra ni la primera: los tests del Lote 2, sin cambios en sus expectativas.
- Lo reservado de más vuelve cuando termina la lectura, también si termina con un error (una lectura de disco que falla) (INV-B5).
- Lo que ocupan codificadas las operaciones de una lectura del log no pasa de `MAX_RESPONSE_OPS_BYTES`, salvo la primera, que entra sola: la segunda medición no hacía falta.

**Interacciones a revisar:** las lecturas que van al pool de E/S con la reserva, que retienen el tramo mientras leen del disco; el tope de un frame por reserva (`Reservation::grow`), que un tramo puede alcanzar antes; el catch-up de un cliente al día, que no lee el log; las reservas del registro, del esquema y de los chunks, que no leen el log.

**Medición:** E1-actor con 8 y 64 pedidos en vuelo, contra el Lote 6, y los controles. **Criterio:** con 64, al menos el 80% de los commits por segundo de 8 (el Lote 6 hizo el 52%; el prototipo, el 156%).

**Metodología:** §5.1, con el checklist de medición.

**Documentos:** `ARCHITECTURE.md` §5.2 (*Bounded responses*) y §7.3; `INVARIANTES.md`: INV-B1, INV-B5.

**Entrega:** `perf(server): grow the reservation of a read by chunks, and measure its operations once (Fase 3.12, Lote 6b)`.

**Hecho.**
- **La reserva por tramos** (DS10): la lectura del log que arma una respuesta (`ReadBudget::reserving`) toma de la reserva de su respuesta lo que cada operación que admite ocupa en ella a través de `ReadReservation` (en `response_budget.rs`), en lugar de llamar a `Reservation::grow` una vez por operación. Cuando lo reservado de más no alcanza, la reserva crece un tramo de 256 KiB (`READ_RESERVATION_CHUNK`), o exactamente lo que le falta a la operación si es más; cada operación descuenta de lo reservado. Si un tramo no entra en la parte del cliente o en el total, la reserva crece exactamente lo que le falta a la operación, y así el resto de esa lectura, que no vuelve a intentar un tramo: la lectura se corta en la misma operación que creciendo una vez por operación, y con el presupuesto casi lleno hace como mucho un intento de más. `Reservation::grow_by` dice cuánto creció la reserva (pasado el tope de un frame, menos que lo pedido, y gratis), y solo eso cuenta como reservado de más: el error del prototipo, que acreditaba el tramo entero. Lo que ninguna operación tomó vuelve al presupuesto cuando `ReadReservation` se suelta (`Drop`), con la lectura, termine como termine (también con un `?` o un pánico), así que al terminar la reserva es la misma que creciendo por operación.
- **Sin la segunda medición:** `fit_in_response` sale. Sus dos llamadas, el catch-up de un commit y el `/sync`, reciben siempre una lectura del log, cuyo presupuesto ya acota lo que cobran sus operaciones (`max(wire, 24 × valores)`, nunca menos que su tamaño codificado) a `MAX_RESPONSE_OPS_BYTES`, salvo una operación sola: la función nunca cortaba nada.
- **Tests agregados** (los marcados con ⊗ fallan con el comportamiento anterior, con el esqueleto que ya tenía la API nueva y crecía una vez por operación; los demás cuidan reglas que ya se cumplían o que el esqueleto ya tenía, y respaldan las mutaciones):
  - `tests/response_budget.rs`: ⊗ `a_read_grows_its_reservation_by_chunks_and_gives_back_what_it_did_not_use` (diez operaciones en un tramo toman del presupuesto una vez; otro tramo cuando lo reservado se acaba; una operación más grande que un tramo, lo que le falta; al soltar la lectura, la reserva es lo que tomaron sus operaciones); `a_chunk_without_room_grows_by_what_the_operation_lacks_for_the_rest_of_the_read` (sin lugar en el total o en la parte del cliente, la reserva crece lo que falta, y lo mismo después, aunque el tramo vuelva a entrar); `a_read_near_a_frame_reserves_ahead_only_what_the_reservation_grew` (a 100 KiB del frame, la lectura no devuelve lo que tomaron sus operaciones, y termina en el frame o en lo que tomaron); y, de la revisión, `a_chunk_without_room_after_one_with_room_grows_by_what_the_operation_lacks` (un tramo que no entra después de uno que entró hace crecer la reserva lo que le falta a la operación más allá de lo reservado de más, no la operación entera, y la lectura se corta donde se cortaba).
  - `log/tests/tiered_log.rs`: ⊗ `a_read_whose_operations_fit_in_a_chunk_takes_from_the_budget_once` (de los segmentos a la RAM y de la RAM, una toma, y la reserva, la de sus operaciones; INV-B1); `what_a_read_reserved_ahead_goes_back_when_it_fails_on_disk` (una lectura cuyo segundo segmento no abre deja la reserva en lo que toman las operaciones que leyó antes del error; INV-B5); `what_a_read_returns_fits_in_a_response_once_encoded` (operaciones de unos bytes a 8 MiB en los niveles frío, tibio y RAM: desde cualquier cursor, lo que devuelve la lectura ocupa codificado a lo sumo `MAX_RESPONSE_OPS_BYTES`, o es una sola operación).
  - `actor/tests/room.rs`: `syncs_and_catch_ups_carry_what_fits_in_a_response` (un `/sync` y el catch-up de un cliente atrasado llevan dos de tres operaciones de 6 MiB, con `has_more`, y lo que ocupan codificadas no pasa del tope, sin `fit_in_response`).
  - **Adaptado,** sin cambiar lo que prueba: `a_reserving_read_grows_the_reservation_by_what_each_operation_takes_in_the_response` suelta la lectura antes que la reserva, porque la lectura la toma prestada hasta soltarse.
  - Los tests del Lote 2 con el presupuesto o la parte del cliente casi llenos pasan sin cambios en sus expectativas (`a_sync_with_little_room_returns_a_shorter_batch_and_the_next_goes_on`, `a_sync_without_room_for_one_operation_is_unavailable_and_changes_nothing`, `a_read_ends_where_the_reservation_of_its_response_runs_out`, `each_response_reserves_the_operations_it_shares` y los de `log/tests/budget.rs`), y con ellos los de `crates/server/tests/http_edge_tests.rs` sobre el presupuesto.
- **Mutaciones verificadas,** cada una con algún test que falla: la reserva sin tramos (una vez por operación); un tramo sin lugar que rechaza la operación en lugar de crecer lo que le falta; la lectura que sigue intentando tramos después de uno sin lugar; una operación más grande que un tramo que hace crecer la reserva solo un tramo; pasado el frame, el tramo entero contado como reservado de más (el error del prototipo); lo reservado de más que no vuelve al terminar la lectura; una operación que no descuenta de lo reservado; y, para lo que ahora sostiene solo la lectura, la lectura que cobra solo lo que ocupan los valores en memoria (la atrapan, entre otros, `what_a_read_returns_fits_in_a_response_once_encoded` y `syncs_and_catch_ups_carry_what_fits_in_a_response`; antes `fit_in_response` la tapaba en el actor). De la revisión, una más, que sobrevivía porque los tests hacían fallar el tramo sin nada reservado de más: después de un tramo sin lugar, la reserva que crece la operación entera en lugar de lo que le falta (la atrapa el test de la revisión, de arriba). La revisión probó además el tope de una respuesta quitado de la lectura de la RAM y de la de disco: los atrapan `fetch_returns_what_fits_in_a_response_from_ram`, y cuatro tests, entre ellos los dos nuevos del log.
- **Interacciones revisadas:**
  - **Las lecturas que van al pool de E/S con la reserva:** la reserva viaja con el log al hilo de E/S y vuelve (`read_log`); la lectura de disco retiene el tramo mientras lee (milisegundos) y lo que no usó vuelve en ese hilo, al terminar `fetch_deltas`, antes de que el log vuelva al actor. La de la RAM, en el actor, sin esperar.
  - **Dos lecturas en un `fetch_deltas`:** el intento en la RAM (`fetch_from_memory`) y, si devuelve `None`, el de disco, cada una con su `ReadReservation`; la primera se suelta antes de la segunda, y `None` sale solo si no admitió ninguna operación, así que no reservó nada de más.
  - **El tope de un frame por reserva** (`Reservation::grow`): un tramo puede alcanzarlo antes que las operaciones; solo lo que creció cuenta, y pasado el frame las operaciones entran gratis, como antes (el test de arriba).
  - **El catch-up de un cliente al día,** que no lee el log: sigue haciendo crecer la reserva una vez, por su operación (`grow`), sin tramo; su caso con poco lugar está en `a_sync_with_little_room_returns_a_shorter_batch_and_the_next_goes_on`.
  - **Las reservas del registro, del esquema y de los chunks,** que no leen el log: reservan su tamaño conocido o un frame (`reserve`, `reserve_frame`), sin cambios; pero se reservan en el handler de HTTP mientras el actor puede estar leyendo para el mismo cliente, y compiten con el tramo de esa lectura en la parte del cliente y en el total: con la parte o el total casi llenos, pueden salir `Unavailable` un poco antes, lo que DS10 acepta para cualquier otra respuesta (de la revisión).
  - **La ventana de la apertura** (`ReadBudget::window`): no reserva.
  - **El achique al codificar** (`budgeted_response`): la respuesta llega con la reserva de sus operaciones, como antes, y se achica a su tamaño codificado.
  - **Un pánico en una lectura:** `ReadReservation` devuelve lo que no usó al desarmarse la pila, antes de que la reserva se suelte con la respuesta.
  - **La cancelación** (de la revisión): si el actor se descarta mientras espera una lectura de disco, la lectura termina en el hilo de E/S y devuelve ahí lo que no usó; la reserva se suelta con el log.
  - **Varias lecturas a la vez:** cada una retiene hasta un tramo de más; pueden ir a la vez una por worker de Tokio (las de la RAM) y una por hilo del pool de E/S (las de disco, 16 de fábrica): con 4 núcleos, unos 5 MiB de 256 MiB, y de la parte de un cliente (32 MiB), un tramo por lectura suya en curso.
  - **`has_more`:** `fit_in_response` lo ponía cuando cortaba, y nunca cortaba; la lectura lo pone como antes.
  - **La devolución también visita el presupuesto** (de la revisión): `give_back` toma el mutex de las partes y suelta permisos del semáforo, así que una lectura que entra en un tramo hace una toma y una devolución, en lugar de una toma por operación; el test cuenta las tomas (`InflightBudget::takes`, que existe solo en los tests).
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (1.305 tests, sobre `main` con el Lote 1 de la sub-fase 4.3, que entró mientras tanto) y `cargo doc` sin advertencias; el lote no toca core, storage ni client, así que no hace falta la compilación a wasm. Las pruebas largas del servidor (`cargo test -p zemdb-server --release -- --ignored`), las 14, también la de tiempos que en el Lote 6 falló con la VM cargada. El checklist de la medición, abajo.
- **Medición.**
  - **El entorno (DP3):** otra VM (4 vCPU Intel Xeon a 2,30 GHz, 15 GB, kernel `6.18.44-fc-v310`, rustc 1.97.0), tmpfs en `/dev/shm`, el 7 y 8 de Noviembre de 2026; los números se comparan solo dentro de esta medición. Como en el Lote 6, E1-actor corre el actor y el generador en el mismo proceso, en los núcleos 0 y 1, con un binario del generador por árbol, de la misma fuente de `tools/perf`; las corridas por HTTP, con el servidor en 0 y 1, el generador en 2 y 3 y el mismo binario del generador para la base y el resultado. Los servidores, en release limpio.
  - **Lo que se compara:** la base es `cf3e374` (el Lote 6); el resultado, este lote. La línea de base del paso 0 (solo del escenario del lote: los controles, en la medición final, intercalados; tres repeticiones de E1-actor, el 6 de Noviembre, antes de que la VM se reiniciara con el mismo procesador): con 8, 63.777 commits por segundo; con 64, 29.405, el 46%.
  - **El diseño:** tres rondas intercaladas, base y resultado, del escenario del lote (E1-actor con 8 y 64 pedidos en vuelo) y de los controles (E1-tmpfs con 1 y 8, E2-tmpfs con 500 salas). Como E1-tmpfs con 1 pedido en vuelo dejó al resultado un 4,8% abajo con los rangos separados (dentro del límite: la dispersión de la base, un 8,5%, más un 10%), se corrieron cuatro rondas más de ese control, empezando por el resultado: 7 de cada lado. La tabla da la mediana y, entre paréntesis, el rango. Los datos crudos, en `tools/perf/results/fase3_12/lote6b/`.

  | Escenario | Base `cf3e374` | Resultado |
  |---|---|---|
  | E1-actor, 8 pedidos en vuelo: commits/s, p99 | 66.778 (63.848–69.742), 223 µs | 73.160 (71.727–75.514), 192 µs |
  | E1-actor, 64 pedidos en vuelo: commits/s, p99 | 30.874 (30.249–36.633), 3,2 ms | 93.205 (88.481–101.068), 1,3 ms |
  | E1-actor, con 64 sobre con 8 | 46% | 127% |
  | E1-tmpfs, 1 pedido en vuelo (control, 7 rondas): commits/s | 8.834 (8.151–9.612) | 8.581 (7.529–9.188) |
  | E1-tmpfs, 8 pedidos en vuelo (control): commits/s | 27.943 (26.662–28.507) | 29.842 (25.772–30.494) |
  | E2-tmpfs, 500 salas (control): propagación p99, CPU del servidor | 824 µs (701–953), 16,6% | 806 µs (627–837), 17,8% |

  - **El criterio se cumple:** con 64 pedidos en vuelo, el resultado hace el 127% de los commits por segundo de 8, contra el 80% que pide el plan (la base, el 46%; el prototipo del Lote 6, en otra VM, el 156%). Con 64, el triple que la base; con 8, un 10% más. La latencia en p99 con 64 baja de 3,2 a 1,3 ms.
  - **El largo de los catch-ups** (lo que pidió la revisión del Lote 6), con un generador instrumentado fuera del repo, dos repeticiones de cada árbol: con 8 en vuelo, unas 9 operaciones de media (p99 de 13 o 14) en los dos; con 64, unas 89 en la base y 87 en el resultado (p50 de 95 y 91, p90 de 100): entre el 33% y el 43% llega a `COMMIT_CATCHUP_BATCH` (100) y sale con `has_more`. El cursor que manda cada trabajador del generador (la secuencia más alta que vio confirmada) queda más de un grupo atrás, así que lo que se hace por operación de cada catch-up pesaba más que las ~64 que suponía el Lote 6. Unas 100 operaciones de E1 ocupan unos 30 KiB en la reserva: un tramo.
  - **Los controles:** E1-tmpfs con 1 pedido en vuelo, con 7 rondas, un 2,9% abajo en la mediana (8.581 contra 8.834) y un 6,4% en la media; las cuatro rondas agregadas, solas, dan un 8,2% abajo (8.058 contra 8.776), más que las tres primeras (un 4,8%), así que no despejaron la caída. Queda dentro del límite (la dispersión de la base, un 8,5% con tres rondas y un 16,5% con siete, más un 10%), con los rangos superpuestos, y el lote no cambia ese camino: con un pedido en vuelo, cada commit va por el camino rápido del cliente al día, que no lee el log ni usa la reserva por tramos. Se toma como ruido de la VM (dos rondas del resultado quedaron en unos 7.530, y las demás, entre 8.144 y 9.188), y la medición final del Lote 8 lo vuelve a mirar. E1-tmpfs con 8, un 6,8% arriba. E2-tmpfs con 500 salas: la propagación en p99, un 2% abajo; el CPU del servidor, de 16,6% a 17,8%, dentro de la dispersión de la base (16,5% a 19,1%). Ninguno empeora más que su dispersión más un 10%.
  - **Lo que no es criterio,** con 64 pedidos en vuelo (de la revisión): el pico de memoria del proceso de E1-actor (`VmHWM`, el actor y el generador) pasa de unos 14 a 27 MiB, porque el generador guarda la latencia de cada commit medido (4 bytes, en vectores que duplican su capacidad) y el resultado mide el triple de commits; con 8, con casi los mismos commits, 19 y 21 MiB. La latencia máxima sube de 24–37 ms a 63–72 ms: es una sola pausa por corrida, a los 30 s de abrir la sala, en la que esperan juntos los 64 commits en vuelo, el tick lento del mantenimiento de disco (`SLOW_TICK_PERIOD`); ya estaba en la base, y con el lote, que escribe el triple de operaciones entre dos ticks, dura más. El p999 baja de 4,6 a 1,9 ms. El lote no cambia el mantenimiento: va al Lote 8.
  - **Las corridas son válidas:** sin errores, el head igual a lo confirmado, sin huecos ni errores del SSE, sin clientes atrasados al final; en E1-actor, el proceso (actor y generador) usa entre el 131% y el 149% de sus dos núcleos; en las corridas por HTTP, el generador, por debajo del 72% de un núcleo.
- **La revisión de delta,** por un agente aparte, de contexto fresco y solo lectura, con las seis preguntas. Sin hallazgos Altos ni Medios:
  - **Un Bajo, arreglado:** después de un tramo sin lugar, si la reserva crecía la operación entera en lugar de lo que le falta, ningún test lo notaba (todos hacían fallar el tramo sin nada reservado de más), y la lectura se cortaba una operación antes que creciendo por operación. Lo fija `a_chunk_without_room_after_one_with_room_grows_by_what_the_operation_lacks`, que escribió la revisión.
  - **Las notas, aplicadas:** tres interacciones que faltaban (las reservas del registro, del esquema y de los chunks, que compiten con el tramo de una lectura del mismo cliente; la cancelación; y la devolución, que también visita el presupuesto); `ARCHITECTURE.md`, INV-B1 e INV-B5 no decían que, con el presupuesto casi lleno, otra respuesta puede salir más corta o con `Unavailable` un poco antes; un docstring nuevo de un test citaba una invariante por su código; el control E1-tmpfs con 1 pedido en vuelo se presentaba como si las rondas agregadas hubieran despejado la caída, y el largo de los catch-ups llegaba a 100 en el 33%, no el 36%; y el pico de memoria y la latencia máxima con 64, que la nota no comentaba.
  - **Lo que encontró fuera del lote:** 12 líneas con códigos de decisiones y de invariantes en comentarios de lotes anteriores, contra `GEMINI.md`; al backlog de `ROADMAP.md` (punto 19).
  - **Sobre la medición** (pregunta 6): es válida según DP3 y §5.2, las medianas y los rangos coinciden con los datos crudos, el criterio se cumple en cada ronda (con 64, el 123% de 8 o más) y ningún control pasa su dispersión más un 10%. Los chequeos se volvieron a correr sobre `main` con lo que entró de la sub-fase 4.3.
- **El tamaño:** unas 72 líneas de producción agregadas, sin comentarios ni blancos, y 35 quitadas (`fit_in_response` y sus usos), dentro del tope de ~300 de `docs/METODOLOGIA.md` §4; la estimación del plan era de unas 40 netas, y son 37 (unas 10 de las agregadas son el conteo de visitas al presupuesto, solo de los tests).

### Lote 7 · El roster · ✅ Hecho
**Objetivo:** decidir DS7 con una medición en un disco de nube y, si se adopta, implementarla.

**Archivos, si se adopta:** `crates/server/src/actor/room.rs` (el tick), `actor/lease.rs`, `config.rs` (`ZEMDB_ROSTER_PERSIST_SECS`), y sus tests. `ARCHITECTURE.md` §5.2 (*Client Roster Persistence*).

**Qué se hace:** primero, E2-disco en una máquina con un disco de nube con límite de operaciones por segundo (las escrituras por segundo y el tiempo ocupado del disco). El dueño del proyecto decide con esos números si se adopta DS7. Si se adopta: el roster se escribe cada N segundos en lugar de cada tick, salvo antes de una poda que borre (D56).

**Tests exigidos, si se adopta.**
- Un crash pierde como mucho N segundos de cambios del roster, y eso solo causa más retención, un re-registro o un snapshot de más (INV-R3).
- Una poda que borra persiste el roster antes, como hoy (INV-R1, D56).
- Un apagado ordenado persiste el roster (INV-L5).

**Interacciones a revisar:** los cursores que reportan los commits y los `Ack`; los estados del lease (`Connected`, `Disconnected`, `Dormant`); la designación de snapshots; la apertura con un roster viejo.

**Medición:** E2-disco en el disco de nube, con 500 y 1.000 salas. **Criterio, si se adopta:** las escrituras al disco por commit bajan a menos de la mitad (hoy, entre 7 y 9). **El criterio con que se adoptó** (el dueño del proyecto, el 10 de Noviembre de 2026, con la medición de la decisión, que dio unas 4 escrituras por commit y el roster en el 30% de ellas): en E2-disco sin límite, las escrituras al disco por commit bajan al menos un 30% con 500 y con 1.000 salas; y con 3.000 operaciones de escritura por segundo, 1.000 salas sostienen sus commits (los confirmados por segundo, al menos el 99% de los programados, en corridas válidas).

**Metodología:** §5.1, con los checklists de crash-safety y de medición, si se adopta. Si se descarta, el lote cierra con la medición en su nota **Hecho** y DS7 pasa a *Descartada*.

**Documentos:** `ARCHITECTURE.md` §5.2; `INVARIANTES.md`: INV-R3.

**Entrega:** `perf(server): persist the client roster every few seconds instead of every tick (Fase 3.12, Lote 7)`, si se adopta.

**Hecho** (12 de Noviembre de 2026).

**La medición de la decisión** (9 y 10 de Noviembre de 2026, sobre `efb3ec4`, en una VM de 4 vCPU Intel Xeon a 2,10 GHz, 15 GiB, kernel `6.18.44-fc-v310`, Rust 1.97.0; servidor en los núcleos 0 y 1, generador en 2 y 3).
- **Dónde:** la VM no tiene un disco de nube, así que el dueño del proyecto eligió emularlo, y dejar la medición en uno real para más adelante.
  - El disco emulado: un archivo de 3 GB con ext4, como loop device con E/S directa, y otro loop device encima, montado; el límite de escrituras por segundo de blkio (cgroup v1: la VM no tiene v2) va sobre el de abajo, donde cada escritura y cada flush del sistema de archivos llega como un bio, después de juntarse, como en un disco. Sobre el dispositivo del sistema de archivos, el límite cuenta los bios antes de juntarse (con esta carga, unos 1,9 por pedido), y con "3.000" el disco veía unas 1.100 operaciones por segundo.
  - Lo que no emula: la latencia de un disco de red (de 0,5 a 2 ms por escritura en uno de nube, 0,15 ms en esta VM; el kernel no tiene device-mapper para sumarla). Y cuenta los flush contra el límite, que un disco de nube quizás no cuente: es la versión pesimista.
  - El script: `tools/perf/cloud_disk.sh`.
- **Qué:** E2-disco (10 clientes por sala, un commit por segundo por sala) con 500 y 1.000 salas, sin límite y con 3.000 y 1.000 escrituras por segundo (con 1.000, también 250 salas), intercalando `main` con un diagnóstico que escribe el roster en el tick cada 10 s (`roster-every.patch`, en los resultados del lote).
- **Las rondas:** 2, no 3: la VM se reinició durante la tercera (y antes, una vez, mientras se esperaba la respuesta del dueño). La ronda 2 no tiene la última corrida (el diagnóstico con 1.000 escrituras por segundo y 1.000 salas, que colapsa igual). Las dos rondas coinciden en cada punto.

Mediana de las 2 rondas, o los dos valores; "operaciones" son las escrituras más los flush, lo que cuenta el límite:

| E2-disco | `main` | El roster cada 10 s (diagnóstico) |
|---|---|---|
| Sin límite, 500 salas: escrituras (operaciones) por commit | 4,21 (6,56) | 3,02 (4,76) |
| Sin límite, 1.000 salas: escrituras (operaciones) por commit; commit p99 | 4,12 (6,15); 6–10 ms | 2,74 (4,18); 4 ms |
| 3.000 por segundo, 500 salas: commit p99 | 50–68 ms | 4 ms |
| 3.000 por segundo, 1.000 salas: commits por segundo (de 1.001 programados); p99 | 815–816, colapsa; 19 s | 1.007–1.014; 0,2–0,5 s |
| 1.000 por segundo, 250 salas: commit p99 | 143–167 ms | 68–72 ms |
| 1.000 por segundo, 500 salas: commits por segundo (de 503); p99 | 418–420, colapsa; 18 s | 515–519; 2,2–2,8 s |
| 1.000 por segundo, 1.000 salas | colapsa, con errores (299–302 commits por segundo) | colapsa, con errores (374) |

- **Lo que dijo:** un commit cuesta unas 4 escrituras al disco, no las 7 a 9 de la Evaluación 2 (los Lotes 3 a 6 cambiaron la carga: sin `block_in_place`, con *group commit*, con el evento `ops` y sin los `/sync` de cada aviso), y el roster es el 30% de ellas; el resto es el commit (`active.wal`, el journal de ext4 y sus flush). Con un disco limitado, el roster decide si 1.000 salas aguantan: con 3.000 operaciones por segundo, `main` colapsa y el diagnóstico sostiene sus commits. Con eso el dueño del proyecto adoptó DS7 con N = 10 s y el criterio de la sección del lote, en lugar del de "menos de la mitad", que el roster no puede dar solo.

**Lo que se hizo.**
- `config.rs`: `roster_persist_secs` (`ZEMDB_ROSTER_PERSIST_SECS`, 10 de fábrica, de 1 a 300).
- `actor/room.rs`: el tick rápido escribe un roster que cambió en los puntos de la grilla de la sala: el primero, cuando abrió el actor más una fase del primer período que sale de su encarnación (aleatoria); los demás, cada período. Después de cada escritura (la del tick, la de una poda que borra o la de la parada), el próximo vencimiento (`roster_due_at`) es el primer punto de la grilla más de medio tick después de que se pidió esa escritura, antes de su espera en el pool de E/S. Una escritura fuera de la grilla no la mueve. Con medio tick de margen: un tick llega un poco antes o después que un punto, y sin el margen el roster de una sala con carga se escribiría a veces un tick tarde, y con 1 s, salteando ticks. Una escritura que falla no mueve el vencimiento, y la reintenta el tick siguiente; un cambio en una sala cuyo roster no cambió en un período entero va con el tick siguiente, fuera de la grilla.
- La poda que borra y la parada lo escriben como antes, sin esperar el período.
- **La fase, agregada con la primera medición del lote** (el dueño del proyecto, el 11 de Noviembre de 2026): sin ella, la primera escritura de cada sala caía en el primer tick con un cambio, y las salas que abren juntas (en la medición, las 500 en 2 s; en producción, después de reiniciar el servidor) escribían el roster en el mismo tick, período tras período. Con 3.000 escrituras por segundo y 500 salas, los dos segundos de cada período con la ráfaga (unas 700 operaciones por segundo más, de 2.100 a entre 2.700 y 3.080) llevaban el commit a 30–46 ms en p99, contra 4 ms en el diagnóstico de la decisión, cuyas escrituras se desfasaban solas (caían a veces a los 10 s y a veces a los 11). Lo que cuesta: el primer cambio de una sala recién abierta puede esperar hasta un período en lugar de un tick, dentro del tope de INV-R3.
- **La grilla, agregada con la revisión de delta** (el dueño del proyecto, el 12 de Noviembre de 2026): con la fase sola, cada escritura empezaba el período de nuevo, así que la fase se perdía en dos casos, y las salas volvían a escribir en el mismo tick. El primero, si varias pasaban un período sin cambios a la vez (un corte de red, un reinicio del proxy) y volvían juntas. El segundo, con una poda que borra en el tick lento, que escribe el roster y cae alineado en salas que abrieron juntas. Con la grilla, una escritura fuera de ella no la mueve. Un cambio en una sala quieta sigue yendo con el tick siguiente, así que la grilla no suma espera.
- `actor/tests/support.rs`: las salas de `test_manager` escriben el roster con el período más corto, 1 s, como los demás tiempos acortados de los tests.

**Tests agregados** (⊗ = fallan con el comportamiento anterior).
- `actor/tests/room.rs`, con el actor detenido y el reloj de Tokio pausado (el test corre los ticks), con un período de 4 s (distinto del de fábrica) y la encarnación 1 (la primera escritura, a 1 ms del inicio: en el primer tick), salvo donde se dice:
  - ⊗ `the_fast_tick_writes_a_changed_roster_at_most_once_every_roster_persist_secs`: el primer tick escribe; un cambio espera al tick de un período después de esa escritura; el período empieza de nuevo con cada escritura.
  - ⊗ `a_room_first_writes_its_roster_at_the_point_of_the_period_its_incarnation_gives`: con una encarnación que da 2,5 s, los ticks hasta los 2 s no escriben, el de los 2,5 s sí, y después, un período después de cada escritura.
  - ⊗ `the_tick_of_the_period_writes_the_roster_even_if_it_runs_a_little_early`: el tick anterior al del período no escribe, ni uno que llega 0,6 de tick antes; uno que llega 0,4 de tick antes, sí (el margen, entre 0,4 y 0,6 de tick).
  - ⊗ `a_write_off_the_grid_does_not_move_the_grid_of_a_room` (de la revisión de delta): con una grilla en 2,5, 6,5, 10,5 y 14,5 s, un cambio después de un período sin cambios va con el tick siguiente, a los 12 s, y el siguiente espera al punto de los 14,5 s, no a los 16.
  - `the_next_point_counts_from_when_a_write_was_asked_for_not_from_when_the_io_threads_ran_it` (de la revisión de delta): una escritura que espera más de un período al pool de E/S (el test toma sus permisos) deja vencido el punto siguiente, así que esa espera no se suma a lo que puede perder un crash.
  - `a_change_to_a_roster_not_written_for_roster_persist_secs_goes_with_the_next_tick`: los ticks sin nada que escribir no mueven el vencimiento, y un cambio después de un período sin cambios va con el tick siguiente.
  - ⊗ `a_crash_loses_only_the_roster_changes_since_its_last_write` (INV-R3): después de un crash dentro del período, el cursor vuelve al de la última escritura (la sala retiene más) y un registro perdido da `ClientNotRegistered`, y el cliente se registra de nuevo.
  - `a_prune_that_deletes_writes_the_roster_before_its_period_ends` (INV-R1, D56): una poda que borra dentro del período escribe el roster antes.
  - ⊗ `shutdown_writes_the_roster_changes_that_wait_for_their_period` (INV-L5), con el manager y una encarnación aleatoria: los ticks dentro del período no escriben el cambio, y el apagado sí.
- `tests/config.rs`: ⊗ `roster_persist_secs_has_a_default_a_toml_key_a_variable_and_a_range`; y la variable, en las listas de valores que no se leen y de variables vacías.

**Mutaciones verificadas** (15, sobre la versión final; todas atrapadas; el script, `mutate.py`, toca cada archivo al restaurarlo):
- **Lo que escribe el tick:** el roster en cada tick (el comportamiento de antes); la grilla sin la fase, empezando al abrir; sin el margen; con 3/4 y con 1/4 de tick de margen; una escritura que no mueve el vencimiento; y cada tick, también uno sin nada que escribir, que lo mueve.
- **La grilla:** una escritura que empieza el período en lugar de seguir la grilla (la versión de antes de la revisión, que solo atrapa `a_write_off_the_grid_does_not_move_the_grid_of_a_room`); el punto siguiente contado desde el final de la escritura, después de su espera en el pool (solo lo atrapa `the_next_point_counts_from_when_a_write_was_asked_for_not_from_when_the_io_threads_ran_it`); y el punto siguiente sin el medio tick, que hace escribir al tick siguiente.
- **El período y la configuración:** el período de 10 s fijo, sin leer `roster_persist_secs`; la variable que no se lee; y la variable sin validar.
- **Lo que no espera el período:**
  - una poda que borra que espera el período para escribir el roster;
  - la parada que espera el período. Esta la atrapan también tests del manager, de la parada por inactividad y por fallo, y `snapshot_demand_survives_a_room_restart`.
- **Antes:** las mismas mutaciones, menos las de la grilla, se verificaron también sobre la versión sin fase y sobre la versión con la fase sola.

**Interacciones revisadas.**
- **Los cursores** que reportan los commits (también los de un grupo), los `Ack` y los `/sync`: marcan el roster cambiado, como antes, y se escriben dentro del período (los tests del período, el del crash y el del apagado).
- **Los estados del lease** y sus transiciones en el tick (`Disconnected`, `Dormant`, el olvido de un `Dormant`): también marcan el roster cambiado y se escriben dentro del período. Después de un crash, la sala vuelve con el estado de la última escritura y el tick lo recalcula con el cursor y la última actividad, como con el segundo de antes, solo que el período es más largo.
- **La última actividad** (`last_seen`): la actividad sola no marca el roster cambiado; su valor en disco se refresca con la escritura siguiente, que ahora puede llegar un período después. Después de un crash, la inactividad restaurada puede ser hasta un período más larga: un cliente puede pasar a `Disconnected` antes (vuelve con su pedido siguiente), con el lease de fábrica de 90 s; está en `ARCHITECTURE.md` desde antes, con el tick.
- **La demanda de snapshot persistida:** una renovación marca el roster cambiado cuando la guardada envejeció más que su granularidad, y se escribe dentro del período: después de un crash, una demanda restaurada puede vencer hasta un período antes (en `ARCHITECTURE.md`); `snapshot_demand_survives_a_room_restart` pasa. **La designación**, solo en memoria, no cambia.
- **La poda por cursores y D56:** una poda que borra escribe el roster antes, sin esperar el período, y su escritura empieza el período (`a_prune_that_deletes_writes_the_roster_before_its_period_ends`, `pruning_on_ack_persists_cursor_before_deleting_segments`). Una poda que no borra no escribe, como antes (`acks_that_prune_no_segment_do_not_write_the_roster`).
- **La parada** por apagado, inactividad o fallo escribe el roster sin esperar el período; **el pánico**, no, como antes. Un apagado que pasa sus 10 s de gracia pierde, en las salas a las que no llegó, lo de un crash: ahora, hasta un período (en `ARCHITECTURE.md`, *Graceful Shutdown*).
- **Una baja que se pierde en un crash,** ahora dentro de un período en lugar de un segundo: el cliente vuelve al roster, pasa a `Disconnected` con su lease y retiene la poda por cursores hasta que el TTL o la cuota mueven la cola más allá de su cursor (o hasta `dormant_after_secs`, si está fijado). Es más retención, la consecuencia que acepta INV-R3, y está en `ARCHITECTURE.md` desde antes.
- **Una escritura que falla** no empieza el período, y la reintenta el tick siguiente (`a_failed_roster_write_is_retried_by_the_next_tick`, que atrapó dos mutaciones).
- **La espera en el pool de E/S:** la escritura espera su turno como antes; con el pool lleno, lo que se pierde puede pasar del período por esa espera (INV-R3).
- **La apertura con un roster viejo:** el mismo camino; el test del crash reabre y mira las consecuencias.
- **Una sala que se reabre seguido** (por inactividad): cada apertura de la misma encarnación tiene la misma fase, y la parada escribe el roster, así que no pierde nada; una sala recreada tiene otra encarnación y otra fase.
- **Las escrituras fuera de la grilla:** la de una poda que borra, la de un cambio después de un período sin cambios y la de la parada no la mueven, así que las salas no vuelven a quedar en fase (`a_write_off_the_grid_does_not_move_the_grid_of_a_room`).

**Checklists** (§5.3).
- **De crash-safety:** ninguna operación de archivo cambia de orden ni de lugar respecto de los locks (la escritura del roster es la misma `write_atomic` en el pool de E/S, con el log; solo es menos seguida); ninguna operación soltada aterriza después de cerrar o reabrir la sala (se espera como antes); cada `sync` sigue sobre el mismo archivo y directorio, y ningún resultado se descarta (un error se registra y se reintenta); los fail points siguen en el mismo paso, con el mismo nombre; las imágenes de crash pasan sin cambios (las pruebas largas).
- **De la medición:** la base y el resultado en la misma VM, con el mismo binario del generador; corridas válidas según DP3; el entorno, abajo.

**Verificación.**
- Los chequeos de `docs/METODOLOGIA.md` §4, regla 4: `fmt`, `clippy` y `doc` sin advertencias; 1.425 tests del workspace en verde, sobre el `main` de `7c11c10` (10 del lote); el lote no toca `core`, `storage` ni `client`.
- Las pruebas largas del servidor en release, sobre la versión final: 14 de 14 (las imágenes de crash sin cambios en sus expectativas; `fetch_cost_does_not_grow_with_the_number_of_segments` también, en la corrida de todas).
- La suite del actor, tres veces seguidas, sin tests inestables por la fase aleatoria de las salas del manager.
- **Windows:** los tests leen el roster cuando el actor no lo tiene abierto (`write_atomic` lo cierra antes del rename).

**La medición del lote** (12 de Noviembre de 2026, sobre el código final, en una VM de 4 vCPU Intel Xeon a 2,10 GHz, 15 GiB, kernel `6.18.44-fc-v378`, Rust 1.97.0; servidor en los núcleos 0 y 1, generador en 2 y 3).
- **Las variantes:**
  - La base es el servidor de `efb3ec4`, el mismo binario que el de `main` (`7c11c10`), porque los commits de la 4.3 y la 4.4 desde entonces no tocan el servidor ni `core`.
  - El resultado es el del lote, con la grilla.
  - El generador es el mismo binario para las dos.
- **Las corridas:** tres rondas intercaladas. Todas son válidas según DP3: sin errores, con el head igual a lo confirmado, sin huecos ni streams cortados, con todos los clientes al día, sin advertencias del servidor y con el generador por debajo del 90% de sus núcleos.

Mediana, y el rango de las 3 rondas:

| | Base (`efb3ec4`) | Resultado | |
|---|---|---|---|
| **E2-disco sin límite, 500 salas: escrituras por commit** (operaciones) | 4,28, de 4,21 a 4,29 (6,71) | 3,18, de 3,18 a 3,20 (5,07) | −26% |
| **E2-disco sin límite, 1.000 salas: escrituras por commit** (operaciones) | 3,98, de 3,92 a 4,02 (5,89) | 2,85, de 2,85 a 2,86 (4,36) | −28% |
| E2-disco sin límite, 1.000 salas: commit p99 | de 6,6 a 15,4 ms | de 4,8 a 7,7 ms | |
| E2-disco con 3.000 por segundo, 500 salas: commit p99 | de 43 a 54 ms | de 3,8 a 4,3 ms | |
| **E2-disco con 3.000 por segundo, 1.000 salas: commits por segundo** (de 1.002 programados) | de 834 a 847 (el 84%); p99 de 17,0 a 17,6 s | de 1.002 a 1.011 (todos); p99 de 0,07 a 0,38 s | |
| Control: E1-tmpfs con 1 pedido en vuelo, commits por segundo | 6.047, de 6.038 a 6.059 | 5.989, de 5.830 a 6.100 | −1% |
| Control: E1-tmpfs con 8, commits por segundo | 25.004, de 24.709 a 25.408 | 22.532, de 22.301 a 23.922 | −9,9% |
| Control: E2-tmpfs con 500 salas, propagación p99 | 1,00 ms, de 1,00 a 1,01 | 0,91 ms, de 0,88 a 0,94 | −9% |

- **El criterio, segunda parte (se cumple):** con 3.000 operaciones por segundo, 1.000 salas sostienen sus commits: todos los programados, contra el 84% de la base, que colapsa.
- **El criterio, primera parte (no se cumple):** sin límite, las escrituras por commit bajan un 26% con 500 salas y un 28% con 1.000, debajo del 30%.
  - La fase cuesta unos 6 puntos (abajo).
  - El resto es que la base varía entre mediciones más que el resultado. Con 500 salas, la base dio 4,22 escrituras por commit en la primera medición, 3,84 en la segunda y 4,28 en esta, las dos últimas en la misma VM; el resultado, entre 3,1 y 3,2 en la segunda, en la de la fase y en esta.
  - El dueño del proyecto aceptó el desvío el 11 de Noviembre de 2026, con los números de la segunda medición (−19% y −29,8%).
- **Los controles:** ninguno empeora más que su dispersión más un 10%.
  - E1-tmpfs con 1: −1%, con una dispersión de la base del 0,4%.
  - E1-tmpfs con 8: −9,9%, con una del 2,8%. Es el más cerca de su límite, y en un camino donde el lote solo escribe menos: en la segunda medición, el mismo control dio +32%.
  - E2-tmpfs: −9% de propagación en p99, es decir, mejor.
- **Lo que cuesta la fase,** medido aparte en la misma VM, el 11 de Noviembre de 2026, sobre la versión con la fase sola, que en régimen escribe en los mismos ticks que la grilla.
  - **Cómo:** sin límite, con la base, el resultado sin la fase y el resultado, intercalados, 3 rondas, rotando el orden.
  - **Con 500 salas:** 3,90, 2,96 y 3,19 escrituras por commit (−24% sin la fase, −18% con ella).
  - **Con 1.000 salas:** 3,73, 2,54 y 2,75 (−32% y −26%); el commit p99, de 41 a 132 ms, de 26 a 251 ms y de 9 a 27 ms.
  - **Lo que cuesta:** unas 0,2 escrituras y 0,2 flush por commit, un 7–8% más que sin ella. Con una escritura del roster cada 10 commits, son unas 2 escrituras y 2 flush más por roster: un commit del journal de ext4 propio por cada uno de sus dos `fsync`, en lugar de compartirlo con los rosters que se escriben en el mismo segundo. Es coherente en magnitud, pero no se midió directamente (`/proc/fs/jbd2/<dev>/info` lo diría).
  - **Lo que gana:** el disco no recibe ráfagas, que con un límite de operaciones por segundo es lo que pesa.
- **La primera medición del lote, sin la fase** (10 de Noviembre de 2026, en otra VM, kernel `6.18.44-fc-v338`), con el mismo script:
  - **Sin límite:** de 4,22 a 2,93 escrituras por commit con 500 salas (−31%), y de 4,17 a 2,64 con 1.000 (−37%).
  - **Con 3.000 por segundo y 1.000 salas:** de 807–819 commits por segundo (colapsa, 19 s en p99) a 1.002–1.003 (0,47–0,55 s en p99).
  - **Con 3.000 por segundo y 500 salas:** de 44–58 a 31–46 ms en p99, contra 4 ms del diagnóstico de la decisión.
  - **La ráfaga:** una corrida con las operaciones al disco segundo a segundo la mostró: dos segundos de cada 10 con unas 700 operaciones por segundo más, contra el límite. De ahí, la fase.
- **La segunda, con la fase sola** (11 de Noviembre de 2026, en la VM de esta):
  - **Sin límite:** de 3,84 a 3,10 escrituras por commit con 500 salas (−19%), y de 4,01 a 2,81 con 1.000 (−29,8%).
  - **Con 3.000 por segundo y 500 salas:** de 40–57 a 5,6–7,9 ms en p99.
  - **Con 3.000 por segundo y 1.000 salas:** de 814–822 commits por segundo (colapsa) a 1.005–1.014.
- **El orden:** en las tres mediciones del lote, la base corrió antes que el resultado en cada par; solo la medición de la fase rota el orden de las variantes.
- Los datos crudos, los scripts y el entorno: `tools/perf/results/fase3_12/lote7/` y su sección del `README`.

**Revisión de delta** (sobre la versión con la fase, antes de la grilla). Un revisor aparte, de contexto fresco y en modo solo lectura, con la pregunta 6 de §5.4. No encontró ningún hallazgo Alto ni ninguna invariante rota, y los checklists se cumplen.
- **Medio, en la nota:** daba por cumplido el −30% con 1.000 salas, que es −29,8%, y atribuía a la fase todo el desvío con 500. Corregido arriba.
- **Bajo, la fase se perdía** después de un período sin cambios o de una poda que borra. El dueño del proyecto eligió la grilla de cada sala (arriba). El test que mostraba el problema, escrito por el revisor, quedó como `a_write_off_the_grid_does_not_move_the_grid_of_a_room`.
- **Bajo, una regla sin test:** que el punto siguiente cuente desde que se pidió la escritura y no desde que terminó. Desde el final, con el pool lleno, la espera se sumaría dos veces a lo que pierde un crash. Ahora la cubre `the_next_point_counts_from_when_a_write_was_asked_for_not_from_when_the_io_threads_ran_it`.
- **Notas, aplicadas:**
  - el margen, que los tests aceptaban entre un cuarto de tick y uno entero, y ahora entre 0,4 y 0,6;
  - tres comentarios de `room.rs` que decían que el roster se escribía en el tick siguiente;
  - el apagado que pasa su gracia, en `ARCHITECTURE.md`;
  - la baja perdida en un crash, en las interacciones;
  - la fecha y la fase del encabezado de la versión de `INVARIANTES.md` del lote (la 27: la 4.3 y la 4.4 tomaron de la 23 a la 26), y una coma del `ROADMAP.md`;
  - los números, redondeados en lugar de truncados;
  - el orden de las corridas;
  - la explicación del journal de ext4, que no se midió directamente.
- **Nota, sin test:** que la escritura de una poda que borra mueva el vencimiento al punto siguiente. Deshacerlo solo agrega escrituras.

**Desvíos.**
- **La medición, en un disco de nube emulado,** no en uno real: lo eligió el dueño del proyecto, y la de un disco real queda para más adelante (la sección del Lote 8).
- **El criterio cambió al adoptar DS7,** y su primera parte no se cumple (sin límite, en la medición final, −26% de escrituras por commit con 500 salas y −28% con 1.000, contra el 30%): unos 6 puntos son de la fase, que el dueño del proyecto sumó con la primera medición del lote, y el resto, de cuánto varía la base entre mediciones. El dueño del proyecto aceptó el desvío (arriba).
- **La medición de la decisión tiene 2 rondas,** no 3: la VM se reinició durante la tercera. Las dos coinciden en cada punto, con diferencias mucho mayores que su dispersión.
- **`actor/lease.rs` no cambia** (el plan lo listaba): el período vive en el actor, donde corre el tick.
- **El diagnóstico de la decisión** (`roster-every.patch`) no va a `tools/perf/diagnostics/`: la variable del lote hace lo mismo. Queda en los resultados del lote, para repetir esa medición sobre `efb3ec4`.
- **Las salas de `test_manager`** escriben el roster cada segundo (el período más corto), como los demás tiempos acortados de los tests, para que los que ya esperaban el roster en disco no dependan de la fase de cada sala.

**Tamaño:** unas 40 líneas de producción, en `config.rs` y `actor/room.rs` (85+/11− con docstrings y comentarios), más `tools/perf/cloud_disk.sh` (39), fuera del workspace; los tests, 394+/12−.

### Lote 8 · Medición final y revisión de cierre · ✅ Hecho (su revisión de cierre no salió en verde: la siguen el 8b y el 8c)
**Objetivo:** cerrar la fase.

**Qué se hace:**
- **La medición final:** todos los escenarios de §5.2 sobre el último commit de la fase, contra la línea de base del Lote 1, en la misma máquina; una tabla de la fase entera, también en el plan de evaluaciones.
- **La revisión de cierre** (`docs/METODOLOGIA.md` §6), contra todas las invariantes que tocó la fase, mirando solo lo que la fase cambió y los mecanismos con los que interactúa (§6).
- **Los documentos:** `ARCHITECTURE.md`, `INVARIANTES.md` con el estado de cada invariante, `ROADMAP.md` y `README.md`.

**Lo que dejó el Lote 7** (su nota **Hecho**):
- **Un disco de nube real:** el Lote 7 midió en uno emulado (`tools/perf/cloud_disk.sh`: un límite de escrituras por segundo, sin la latencia de un disco de red, y contando los flush). La medición en uno real, que el dueño del proyecto dejó para más adelante, no es parte de este lote; la medición final puede correr E2-disco también en el emulado.
- **Lo que queda de cada commit en el disco:** unas 2,5 a 3 escrituras y de 1 a 1,5 flush por commit, con un commit por segundo por sala, son del propio commit (`active.wal`, el journal de ext4 y sus flush). Bajarlas (por ejemplo, con un `active.wal` preasignado y `fdatasync`, para que un commit no escriba el journal) sería otra decisión, fuera de este plan.

**Lo que dejó el Lote 6b** (su nota **Hecho**):
- **La pausa del tick lento:** en E1-actor con 64 pedidos en vuelo, los commits en vuelo esperan juntos, a los 30 s de abrir la sala, el mantenimiento de disco (`SLOW_TICK_PERIOD`): unos 30 ms sobre `cf3e374` y unos 65 ms con el Lote 6b, que escribe el triple de operaciones entre dos ticks. La medición final dice cuánto dura en cada escenario y si pide un lote.
- **E1-tmpfs con 1 pedido en vuelo:** quedó entre un 3% y un 8% abajo según las rondas, dentro del límite y en un camino que el lote no cambia; la medición final lo vuelve a mirar contra la línea de base de la fase.

**Entrega:** `docs: the final measurement and the closing review of Fase 3.12 (Fase 3.12, Lote 8)`.

**Hecho.**

**La medición final** (12 de Noviembre de 2026, en una VM de 4 vCPU Intel Xeon a 2,10 GHz, 15 GiB, kernel `6.18.44-fc-v378`, Rust 1.97.0; el servidor en los núcleos 0 y 1, el generador en el 2 y el 3).
- **Las variantes:**
  - La base es `0b03a77`, la de la línea de base de la fase (el commit anterior al Lote 2).
  - El resultado es `6b93115`, el último commit de la fase. Los commits de la Fase 4 desde `0b03a77`, y los que llegaron a `main` durante el lote, no tocan el servidor ni `core`.
  - Los dos servidores, compilados en release limpio. Las corridas por HTTP usan el mismo binario del generador, el de `6b93115`, que entiende `head_advanced` y el evento `ops`. E1-actor usa el generador de cada árbol, que lleva el servidor adentro.
- **Los escenarios:** los 12 de §5.2, más E2-disco en el disco de nube emulado del Lote 7, con 3.000 escrituras por segundo. Los datos van en tmpfs (`/dev/shm`), en el disco de la VM (`vda`, ext4) y en el emulado (`tools/perf/cloud_disk.sh`).
- **El diseño:** los escenarios corrieron uno detrás de otro. En cada uno, base y resultado intercalados, 3 rondas, así que un reinicio de la VM solo habría invalidado el escenario en curso. No hubo ninguno: todos corrieron en la misma VM, sin reiniciarse, en 1 h 46 min.
- **Las corridas son todas válidas según DP3:** sin errores, el head igual a lo confirmado, sin huecos ni errores del SSE, sin advertencias del servidor, y el generador por debajo del 60% de sus núcleos en las corridas por HTTP. Al final de cada corrida, todos los clientes están al día, salvo en el modo d, que se pone al día con el heartbeat.

Mediana, y el rango de las 3 rondas. E1, en commits por segundo:

| E1 | Base (`0b03a77`) | Resultado (`6b93115`) | |
|---|---|---|---|
| E1-tmpfs, 1 pedido en vuelo | 6.612 (6.055–6.716) | 6.068 (5.725–6.168) | −8% |
| E1-tmpfs, 8 | 21.454 (21.336–22.347) | 20.673 (20.517–21.872) | −4% |
| E1-tmpfs, 64 | 33.916 (33.820–33.964); p99 4,5 ms | 23.650 (18.713–23.996); p99 6,0 ms | −30% |
| E1-disco, 1 | 1.593 (1.465–1.596) | 1.534 (1.496–1.626) | −4% |
| E1-disco, 8 | 3.479 (3.338–3.550); p99 5,3 ms | 6.791 (6.447–6.853); p99 3,8 ms | ×2,0 |
| E1-disco, 64 | 3.004 (2.917–3.033); p99 32,9 ms | 18.230 (18.003–18.497); p99 8,0 ms | ×6,1 |
| E1-actor, 1 | 41.195 (39.935–43.950) | 16.673 (15.476–17.434) | −60% |
| E1-actor, 8 | 59.101 (54.339–60.674) | 56.886 (55.648–58.368) | −4% |
| E1-actor, 64 | 29.102 (28.009–30.426); p99 5,6 ms | 63.393 (61.889–67.584); p99 1,8 ms | ×2,2 |

E2, con 10 usuarios por sala que escriben en promedio cada 10 s (un commit por segundo por sala):

| E2 | Base: propagación p50 y p99 | Base: `503`, CPU por commit | Resultado: propagación p50 y p99 | Resultado: `503`, CPU por commit |
|---|---|---|---|---|
| E2-tmpfs, 500 salas | 1,1 y 23 ms | 14.907; 1,57 ms | 0,6 y 2,2 ms | 0; 0,50 ms |
| E2-tmpfs, 1.000 salas | 376 ms y 11,3 s | 1.002.866; 1,59 ms | 0,6 y 2,1 ms | 0; 0,44 ms |
| E2-disco, 500 salas | 2,0 y 31 ms | 55.641; 1,90 ms | 1,2 y 4,1 ms | 0; 0,67 ms |
| E2-disco, 1.000 salas | 2,5 y 21,0 s | 1.085.394; 1,74 ms | 1,2 y 4,9 ms | 0; 0,62 ms |
| E2-disco emulado con 3.000 escrituras por segundo, 500 salas | 3,0 y 55 ms (commit p99, 26 ms) | 74.664; 1,72 ms | 1,9 y 5,6 ms (commit p99, 5,5 ms) | 0; 0,60 ms |
| E2-disco emulado con 3.000 por segundo, 1.000 salas | 2,7 y 24,2 s (commit p99, 426 ms) | 934.804; 1,41 ms | 18 y 109 ms, de 88 a 705 (commit p99, 107 ms) | 0; 0,48 ms |
| E2-modos, 1.000 salas, modo a (SSE, `Ack` después de cada `/sync`) | 395 ms y 11,3 s | 1.088.316; 1,63 ms | 0,6 y 2,1 ms | 0; 0,48 ms |
| E2-modos, 1.000 salas, modo c (SSE, `Ack` periódico; en la base, señales) | 1,4 y 115 ms, de 68 a 2.312 | 194.144; 1,01 ms | 0,6 y 2,1 ms | 0; 0,49 ms |
| E2-modos, 1.000 salas, modo d (sin SSE, heartbeat cada 30 s) | 5,0 y 25,3 s | 0; 0,42 ms | 5,0 y 25,3 s | 0; 0,37 ms |

- **Más de E2:**
  - **Los hilos del servidor:** de 251 a 485 en la base, y 515 en el disco emulado con 1.000 salas (el máximo de Tokio es 512); de 36 a 37 en el resultado.
  - **La memoria del servidor con 1.000 salas en tmpfs:** de 372 a 287 MiB.
  - **Las escrituras al disco por commit,** en E2-disco: de 7,40 a 3,78 con 500 salas (−49%) y de 8,63 a 3,78 con 1.000 (−56%); los flush, de 1,48 a 1,15 y de 1,65 a 1,11. En el disco emulado con el límite no se comparan: con la cola, el kernel junta más pedidos (de 3,46 a 3,15 con 500 salas, y de 2,65 a 2,23 con 1.000, donde la base colapsa).
- **Lo que cambia la carga, no solo el servidor:** desde el Lote 5, el generador aplica el evento `ops` sin `/sync` ni `Ack`. En la base hay de 2.600 a 9.500 `/sync` por segundo, y en el resultado, ninguno (salvo los del heartbeat del modo d, en los dos). Por eso los modos a y c del resultado son la misma carga (como anotó el Lote 5).
- **La meta de la fase** (§0) se cumple en esta máquina:
  - con 1.000 salas y la configuración de fábrica, la propagación en p99 es de 2,1 ms en tmpfs y de 4,9 ms en disco, debajo de 10 ms;
  - con el evento `ops`, el servidor gasta 0,44 ms de CPU por commit en E2-tmpfs (0,48 y 0,49 en los modos a y c), debajo de 0,5.
- **E1-tmpfs con 64 pedidos en vuelo, −30%: la base hacía menos trabajo.** Lo midió un diagnóstico con un generador que cuenta los catch-ups (2 rondas, `diag-probe-*`):
  - **En la base,** el 91% de los commits recibe el catch-up vacío, con `has_more`: la reserva de un frame entero no entraba en la parte del cliente. Cada commit trae 4,1 operaciones en promedio, en unos 660 bytes por respuesta.
  - **En el resultado,** ninguno sale vacío. Cada commit trae 58,7 operaciones, en 8,5 KB: el servidor entrega unos 1,4 millones de operaciones por segundo en catch-ups, contra unas 140.000.
  - **En el perfil** (`diag-profile-*`), codificar los mensajes es el 13,1% del CPU del servidor, contra el 3,9% en la base.
  - **Con 8 en vuelo,** lo mismo en chico: el 47% de los catch-ups vacíos, y 3,4 operaciones por commit contra 7,7.
  - Es lo que anotó el Lote 2: la base se ahorraba un trabajo que un cliente real pagaría con un `/sync`.
- **Con 1 pedido en vuelo (E1-tmpfs −8%, E1-disco −4%, E1-actor −60%): el traspaso de cada `fsync` al pool de E/S** del Lote 3.
  - **El diagnóstico sin el pool** (`tools/perf/diagnostics/inline-io.patch`, 3 rondas intercaladas, `diag-inline-*`): E1-tmpfs con 1 en vuelo da 6.653 commits por segundo en la base, 6.200 en el resultado y 7.664 en el resultado sin el pool. En p50, 137, 143 y 115 µs.
  - **Cuánto cuesta el traspaso:** unos 27 µs por commit, el 7% en este escenario. Sin él, el resultado queda un 15% por encima de la base.
  - **En E1-actor,** sin HTTP y en tmpfs, el commit es casi solo ese traspaso: 20 µs en p50 en la base, 56 en el resultado.
  - Es el costo que el Lote 3 midió y aceptó para no bloquear el worker (DS2), y que el *group commit* reparte con 8 o más en vuelo.
  - Era lo que los Lotes 6b y 7 dejaron mirar contra la línea de base de la fase: E1-tmpfs con 1 queda un 8% abajo, por esto.
- **La pausa del tick lento** (lo que dejó el Lote 6b). Es la mayor demora de los commits que empiezan entre los 29 y los 33 s de abrir la sala; el generador anota los de 10 ms o más.

  | Pausa a los 30 s | 1 pedido en vuelo | 8 | 64 |
  |---|---|---|---|
  | E1-tmpfs: base / resultado | 10 ms o menos / 10 ms o menos | 19–22 / 19–30 ms | 33–52 / 25–29 ms |
  | E1-disco: base / resultado | 59–84 / 41–58 ms | 161–187 / 288–330 ms | 167–181 / 521–625 ms |
  | E1-actor: base / resultado | 41–49 / 14–16 ms | 51–75 / 51–68 ms | 31–57 / 61–96 ms |

  - **Por qué crece:** con lo que la sala escribió desde el tick anterior. El mantenimiento de disco (la compresión, la ventana de la RAM, el TTL, la cuota y la poda por cursores) corre en el pool, y el actor lo espera. En E1-disco con 64 en vuelo son unas 550.000 operaciones en 30 s, cerca de 1 µs por operación, contra unas 90.000 de la base.
  - **En el caso de uso no pesa:** una sala de E2, con un commit por segundo, escribe unas 30 operaciones por tick.
  - **No pide un lote en esta fase.** Lo recomendó el lote y lo eligió el dueño del proyecto, el 12 de Noviembre de 2026. Va a los próximos pasos del plan de evaluaciones (§6): no esperar el mantenimiento de disco para atender los commits, o repartirlo entre ticks.
- **La tabla de la fase entera,** también en el plan de evaluaciones (§4.9).
- Los datos crudos, los diagnósticos y el entorno: `tools/perf/results/fase3_12/lote8/` y su sección del `README`.

**La revisión de cierre** (`docs/METODOLOGIA.md` §6, sobre `6b93115`).
- **Los revisores:** tres agentes aparte, de contexto fresco y en modo solo lectura sobre el repo, uno por grupo de las invariantes de §6:
  - durabilidad y ciclo de vida: D1, D2, D3, D7, L3, L5, L7, R1 y R3;
  - entrega, esquema, aislamiento y formatos: Y1, Y2, Y4, Y5, E3, A1, W1 y W2;
  - recursos y disponibilidad: B1, B3, B4, B5, L1 y L2.
- **Cómo trabajaron:** sobre el diff de la fase (`0b03a77..6b93115` en `crates/server` y `crates/core`, que solo tocan commits de la fase), el plan con las notas **Hecho**, `INVARIANTES.md` y `ARCHITECTURE.md`. Lo hicieron en dos etapas: leer y escribir tests en una copia propia mientras corría la medición, y compilarlos y correrlos después.
- **El resultado: no sale en verde.** Las invariantes se sostienen, salvo dos rupturas, cada una con un test que falla en `6b93115`:
  - **INV-L1, Media, de origen (a), del Lote 3 (`9214df3`).**
    - **El escenario:** una sala cuya apertura, con su espera en el pool de E/S, pasa de `ACTOR_TIMEOUT` se descarta con el pedido que venció, y el siguiente la abre de nuevo desde cero. Una sala que tarda más de 5 s en abrir no abre nunca, y con la cola del pool cerca de 5 s, casi ninguna sala cerrada abre.
    - **Por qué antes no pasaba:** la apertura corría con `block_in_place`, sin un `.await` que el plazo pudiera cortar, y la sala quedaba registrada aunque el pedido recibiera `Timeout`.
    - **Los tests,** que fallan 3 de 3: una apertura frenada en el fail point `room_open`, y una que espera a que se libere el pool de un solo hilo, ocupado por un `fsync` de otra sala.
    - **El arreglo que propone el revisor,** probado en su copia: la apertura en una tarea propia. Con él, los dos tests pasan.
    - Lo arregla el **Lote 8c**.
  - **INV-B1 ★ e INV-W1 ★, del Lote 4b (`d2a3848`).**
    - **El escenario:** la búsqueda después de un grupo dañado (`find_frame_after_group`) decodifica entero cada frame que encuentra, sin el tope de valores de un mensaje. Un frame que un miembro guarda en un valor, con 4,1 millones de valores en 16 MiB, hace que la apertura reserve 111 MiB de una vez, unas cuatro veces la cota, si un daño cae en el header de ese registro.
    - **Por qué la severidad:** un solo disparador raro, que falla cerrado (la sala no abre, o trunca el grupo roto como acepta DS9). El revisor la propone Media, pero rompe dos ★, así que no entra en D91.
    - Lo arregla el **Lote 8b**.
  - **Cómo siguen:** el dueño del proyecto eligió, el 12 de Noviembre de 2026, arreglarlas en dos lotes nuevos, cada uno con su revisión de delta, antes de cerrar la fase (`docs/METODOLOGIA.md` §6).
- **Lo que se sostiene, con 11 tests nuevos que pasan:**
  - **Durabilidad (5):** un grupo fallido cuyos registros se perdieron; la parada por un fallo, que reabre recién después de escribir el roster; el apagado de 8 salas con un solo hilo de E/S y un período de 300 s; la poda del tick lento, que escribe el roster antes de borrar; y el daño de abajo.
  - **Entrega (5):** rechazos de cada causa dentro de un grupo; `BehindCompaction` dentro de un grupo; un grupo más grande que la ventana de la RAM, leído del segmento que sella; los eventos a través de una reapertura después de una evolución del esquema; y un `Timeout` de un commit que queda secuenciado.
  - **Recursos (1):** el presupuesto vuelve a cero con respuestas leídas de disco, terminadas o canceladas en cada `.await`.
  - **Dónde están:** pasan en `6b93115`, y los revisores de durabilidad y de entrega probaron los suyos con mutaciones inversas (cuatro y dos), que los hacen fallar. Quedan como parches en [`docs/audits/2026-11-fase3_12-cierre/`](../audits/2026-11-fase3_12-cierre/README.md), junto con los tres que fallan.
- **Un Bajo de documentos que decidió el dueño del proyecto,** de origen (a), del Lote 4b (INV-D1, D2, D3).
  - **El escenario:** un daño del medio en un grupo ya sincronizado que no es el último, y un corte durante el `fsync` del grupo siguiente que lo pierde entero. La apertura trunca registros confirmados del grupo dañado; antes del Lote 4b fallaba, sin perder nada.
  - **Por qué es Bajo:** los bytes no se distinguen de un último grupo roto con un daño del medio, que es el riesgo de D78 extendido por DS9 (§4 de `INVARIANTES.md`), y el costo es el mismo, un grupo. El test lo fija.
  - **Lo que cambia:** el texto dice ahora "el último grupo del que quedan frames válidos" (INV-D2, INV-D3 y la fila de §4), con el visto bueno del dueño del proyecto.
- **Los demás Bajos:**
  - **Corregidos en los documentos:**
    - en `INVARIANTES.md`, lo que sostiene INV-Y1 e Y5 desde el *group commit*;
    - el enunciado de INV-Y4: un reintento que cae en un grupo cuyo `fsync` falla recibe `Unavailable`;
    - el enunciado de INV-Y5: un error incierto, como un `Timeout`, no es un rechazo, y un rechazo tampoco emite `ops`;
    - el hilo principal en INV-B3;
    - lo automatizable de INV-W1 y W2;
    - la introducción y el estado de cada invariante de la fase en el resumen;
    - DS1 a DS10 en §1.6;
    - el tope de un grupo, 1 MiB más la operación que lo alcanza, en la fila de §4 y en DS9;
    - en `ARCHITECTURE.md`, los pánicos con un grupo (§5.4) y que un rechazo no emite `ops` (§7.2 *Ordering*).
  - **Al Lote 8c,** que toca ese código: el docstring de `RoomActor::announcement`, que no nombra el tope de 4 KiB del evento; y el enunciado de INV-L1 y lo que dice `ARCHITECTURE.md` del reintento de un `Timeout` (§7.2) y de la reapertura en el apagado (§5.4).
  - **Al backlog (`ROADMAP.md` §3, punto 19):**
    - `InflightBudget::take` toma primero del total y después revisa la parte del cliente, así que por un instante un cliente con su parte llena saca hasta un tramo (256 KiB) del total. Bajo, de origen (b).
    - El vencimiento de un stream se calcula una vez con la hora de pared y se espera con el reloj monotónico: un salto de la hora hacia adelante lo alarga. Nota, de origen (c).

**Lo demás del lote.**
- **`tools/perf/diagnostics/inline-io.patch`,** rehecho sobre `6b93115`: no aplicaba desde el Lote 3, parte 2, porque se había escrito sobre una versión intermedia de `io_pool.rs`. Ahora corre la E/S en el lugar desde `run_holding`, por donde pasan todas las operaciones del pool.
- **Documentos:**
  - `INVARIANTES.md`, versión 30. La 4.3 y la 4.4 tomaron la 28 y la 29 durante el lote.
  - `ARCHITECTURE.md` §5.4 y §7.2.
  - Este plan: el Lote 8, y los Lotes 8b y 8c.
  - El plan de evaluaciones: §4.9, la tabla de la fase, y §6.
  - `ROADMAP.md`.
  - Los `README` del arnés y de los resultados.
  - El informe de la revisión, en `docs/audits/2026-11-fase3_12-cierre/`.
  - `README.md` no cambia: ya describe la configuración de la fase.

**Desvíos.**
- **La fase no cierra con este lote:** la revisión de cierre no salió en verde, y la siguen los Lotes 8b y 8c (arriba). La fase cierra con el último de ellos.
- **Tres revisores, en lugar de uno,** como en los Lotes 3 y 4, por el tamaño del delta (unas 8.200 líneas, con los tests) y las 23 invariantes; cada uno, con su grupo.
- **La revisión en dos etapas,** para no competir por CPU con la medición.
- **E2-disco en el disco emulado, solo con 3.000 escrituras por segundo:** sin límite, el emulado repite lo de `vda`, y el Lote 7 ya lo midió.
- **Los diagnósticos** (el generador que cuenta los catch-ups, el servidor sin el pool, los perfiles) no estaban en el plan: explican lo que baja en E1.

### Lote 8b · La búsqueda después de un grupo, sin decodificar las operaciones · ✅ Hecho
**Objetivo:** que la apertura de `active.wal` después de un daño no reserve más que lo que permite un mensaje. La revisión de cierre del Lote 8 rompió ahí INV-B1 ★ e INV-W1 ★. El dueño del proyecto eligió arreglarlo en un lote nuevo (12 de Noviembre de 2026).

**Lo que encontró la revisión:**
- **El escenario:** desde el Lote 4b, `find_frame_after_group` (`crates/core/src/protocol/wal_frame.rs`) decodifica entero cada frame válido que encuentra después del daño (`decode_batch_payload`, con `bincode` y sin presupuesto de valores), solo para leer a cuántos bytes empieza su grupo. Un miembro puede guardar en un valor un frame válido con unas 63 operaciones de 65.535 `Null` (4,1 millones de valores en 16 MiB). Si un daño cae justo en el header de ese registro, la apertura decodifica ese frame y reserva 111 MiB de una vez, según el test de la revisión: unas cuatro veces la cota de un mensaje (`MAX_MESSAGE_VALUES`).
- **Antes del Lote 4b,** la búsqueda (`find_valid_frame`) no decodificaba.
- **Por qué pesa:** es un solo disparador raro, y falla cerrado (la sala no abre, o trunca el grupo roto como acepta DS9), pero rompe dos ★.

**Archivos:** `crates/core/src/protocol/wal_frame.rs` y sus tests.

**Qué se hace:**
- **La propuesta de la revisión:** la búsqueda decodifica cada frame que encuentra con el tope de valores de un mensaje (`ValueBudget` con `MAX_MESSAGE_VALUES`), y toma el agotamiento como un frame que no decodifica, que la apertura ya trata como corrupción. Un frame que escribió el servidor nunca pasa ese tope.
- **La alternativa:** leer solo la distancia al inicio del grupo, el primer campo del payload, sin decodificar las operaciones.
- Se decide con el dueño del proyecto al arrancar (paso 0). **Lo eligió el 13 de Noviembre de 2026: la alternativa.** El paso 0 midió que la propuesta no alcanzaba: el tope de valores no cuenta las operaciones, y un frame guardado de 541.000 `Delete` con la clave vacía (31 bytes y ningún valor cada uno) le hacía retener 81 MiB a la búsqueda, sin cobrar nada contra el tope.

**Tests exigidos:**
- El de la revisión, traído al repo con un nombre propio y sin códigos (`docs/audits/2026-11-fase3_12-cierre/`): un frame guardado con más valores que un mensaje, encontrado después de un daño, no reserva más que la cota, y la apertura lo trata como corrupción.
- Un frame de un grupo genuino, al tope de un mensaje, sigue decodificando y la apertura trunca el grupo roto como hoy.
- El fuzzer de la búsqueda y las imágenes de crash del Lote 4b, sin cambios en sus expectativas.

**Interacciones a revisar:** la apertura de `active.wal` (`classify_last_group_in`); los segmentos sellados y las colds, que no usan la búsqueda; el storage, que comparte el formato del frame pero no la búsqueda; y lo que queda en §4 de `INVARIANTES.md` sobre un frame guardado en un valor.

**Metodología:** §5.1, con el checklist de crash-safety; las versiones largas de `zemdb-core` y del servidor; la compilación a `wasm32-unknown-unknown`, porque toca `core`; y la medición de los controles.

**Documentos:** `INVARIANTES.md`, el estado de INV-B1 e INV-W1; `ARCHITECTURE.md`, si describe la búsqueda.

**Entrega:** `fix(core): the search after a damaged group reads where each group starts without decoding its operations (Fase 3.12, Lote 8b)`. El plan decía `the search after a damaged group decodes within the value budget of a message`, la propuesta de la revisión, que el dueño del proyecto no eligió (abajo).

**Hecho.**
- **La decisión** (paso 0, 13 de Noviembre de 2026): el dueño del proyecto eligió la alternativa, leer solo la distancia al inicio del grupo. Antes de elegir, el paso 0 midió que la propuesta de la revisión no alcanzaba:
  - el tope de valores (`ValueBudget` con `MAX_MESSAGE_VALUES`) cobra los valores de las filas, las claves y los deltas, pero no las operaciones;
  - un frame guardado en un valor con 541.000 `Delete` con la clave vacía y sin dueño (31 bytes y ningún valor cada uno; 96 bytes cada uno decodificado) le hacía retener 81 MiB a la búsqueda, sin cobrar nada contra el tope;
  - con la propuesta, cerrarlo pedía además un tope a la cantidad de operaciones de un frame, que hoy no existe.
- **El mecanismo:**
  - `find_frame_after_group` (`crates/core/src/protocol/wal_frame.rs`) lee el payload de cada frame que encuentra después del daño, como antes, pero de él toma solo a cuántos bytes empieza su grupo (`payload_group_offset`):
    - verifica el CRC del payload, porque una vez gastado el tope de checks fallidos la búsqueda toma headers sin chequearlos;
    - lee el primer campo con un tipo propio de un solo campo (`WalBatchPayloadGroup`), con la misma codificación de bincode que `WalBatchPayload`, que admite bytes de más: no decodifica las operaciones.
  - Un frame cuyo payload no pasa su CRC, o es más corto que ese campo, es de un grupo posterior (corrupción), como antes.
  - Uno que lo pasa es del grupo que dice, aunque sus operaciones no decodifiquen: antes era corrupción. Solo puede pasar con un frame que no escribió el servidor, y un miembro que guarda un frame en un valor ya podía hacerlo decodificable, así que ese chequeo no frenaba nada.
  - Un payload más largo que el búfer de la búsqueda va a un búfer nuevo: agrandar el anterior podía copiar el payload que tenía y retener los dos (de la revisión, abajo).
  - Lo que retiene la búsqueda es el payload de un frame (hasta `MAX_MESSAGE_SIZE`) y su ventana de 64 KiB, sea cual sea el frame.
- **Lo que no cambia:**
  - la lectura de los frames en orden (`read_frames` en la apertura, los segmentos sellados, las colds, la lectura con la sala abierta), que decodifica los frames que escribió el servidor, uno detrás de otro;
  - la búsqueda estricta (`find_valid_frame`, `FrameSearch`), que no decodifica;
  - el storage, que comparte el formato del frame y no usa esta búsqueda;
  - el formato en disco.
- **Tests agregados** (los marcados con ⊗ fallan con el comportamiento anterior; los demás cuidan reglas que ya se cumplían y respaldan las mutaciones):
  - ⊗ `the_search_after_a_damaged_group_holds_at_most_a_frame_whatever_the_frames_after_the_damage_hold` (`crates/core/tests/group_search_memory.rs`, nuevo, solo en Linux):
    - es el test de la revisión, traído con otro nombre y sin códigos, más la forma de frame que encontró el paso 0 y el caso de la revisión de delta;
    - cinco casos: un daño justo antes de un frame guardado en un valor, de cada forma (filas de 65.535 `Null`, 4,1 millones de valores, y 541.000 `Delete` sin valores), cada uno diciendo que su grupo empieza con él (un grupo posterior: corrupción) o antes del daño (del grupo: escritura rota); y dos frames del grupo dañado, de 9 MiB y de 16 MiB;
    - mide la memoria residente (`VmHWM`), con una cota de un frame más 4 MiB, y corre cada caso en un proceso propio (el mismo binario del test, con el caso en una variable de entorno), para que ninguno reuse la memoria que liberó otro: en un solo proceso, los casos que seguían arrancaban con hasta 52 MiB residentes y no veían lo que retenía la búsqueda;
    - con el código anterior, la búsqueda retenía 111, 111, 81 y 81 MiB de una vez con los frames guardados; con el lote, 15 a 16 MiB, y 24 MiB con los dos frames del grupo antes del arreglo de la revisión, 16 después.
  - `crates/core/tests/wal_frame_tests.rs`:
    - ⊗ `a_valid_frame_after_the_damage_is_of_the_group_it_says_without_decoding_its_operations`, con payloads de 8 bytes (solo el campo), 16 y 17 que no decodifican y dicen cada grupo: reemplaza a `a_valid_frame_after_the_damage_that_does_not_decode_is_not_of_the_group`, del Lote 4b, que fijaba la regla anterior (aunque su frame decía un grupo posterior, así que pasa igual);
    - `a_valid_frame_after_the_damage_too_short_to_say_where_its_group_starts_is_not_of_it`: payloads de 0, 1 y 7 bytes con el principio de un campo que diría el grupo dañado, y el resto del campo en los bytes que siguen al frame;
    - ⊗ `a_header_taken_without_its_check_whose_payload_does_not_match_is_not_of_the_group`: con el tope de checks gastado, un header tomado sin chequear cuyo payload dice el grupo dañado y no pasa su CRC es corrupción; con su CRC, es del grupo (falla con el código anterior por esa segunda parte: su payload no trae la operación que declara su header).
  - `log/tests/tiered_log.rs` (servidor): `a_torn_group_with_a_record_as_large_as_a_message_reopens`, el frame genuino al tope: un grupo roto con un registro de payload `MAX_MESSAGE_SIZE`, con el header del registro anterior perdido o un sector de su payload perdido, abre con los registros enteros anteriores y trunca el resto.
  - **El fuzzer** (`decoder_fuzz_tests.rs`, `check_search_after_group`): el modelo de la búsqueda hecha a mano (`group_start`) lee del frame su CRC y su primer campo desde los bytes, sin el código del lote, y un frame que pasa su CRC es del grupo que dice aunque no decodifique. ⊗ `random_wal_logs_never_panic_the_replay` falla con el código anterior contra ese modelo.
- **Mutaciones verificadas** (`mutate.py`, que toca cada archivo al restaurarlo), cada una con algún test que falla:
  - la búsqueda que vuelve a decodificar las operaciones (el test de la memoria, el del frame que no decodifica, el del header sin chequear y el fuzzer);
  - el campo leído sin verificar el CRC del payload (el del header sin chequear);
  - un payload corto completado con ceros (el del payload corto y el de la memoria, por la copia);
  - un payload corto tomado como del grupo dañado (el del payload corto). La primera versión de esta mutación (una distancia 0 para un payload corto) no cambiaba nada, porque una distancia 0 pone el grupo en el propio frame, después del daño, y sobrevivió; se rehízo como un payload corto tomado directamente como del grupo;
  - de la revisión: un payload de solo el campo, o de menos de 17 bytes, que no cuenta como del grupo (el del frame que no decodifica, con los payloads de 8 y 16 bytes);
  - el búfer que crece con el payload anterior adentro (el de la memoria).
- **Interacciones revisadas:**
  - **La apertura de `active.wal`** (`classify_last_group_in`): usa solo la posición que devuelve la búsqueda; el mensaje de la corrupción dice "un frame que no es de su grupo", que sigue siendo cierto.
  - **El tope de checks fallidos y la franquicia:** no cambian; un header tomado sin chequear su payload se verifica ahora en `payload_group_offset`, con el mismo resultado que antes con `decode_batch_payload` (el test del header sin chequear).
  - **Los segmentos sellados y las colds:** no usan la búsqueda del grupo (`classify_in`, estricta).
  - **El storage:** comparte el formato del frame y no la búsqueda; sus tests pasan sin cambios.
  - **La lectura en orden de `active.wal`:** decodifica cada frame entero sin tope, como antes; solo lee frames que escribió el servidor, uno detrás de otro desde el principio del archivo, así que un frame guardado en un valor nunca queda en su camino (la búsqueda es lo único que mira posiciones arbitrarias).
  - **Lo que queda en §4 de `INVARIANTES.md` sobre un frame guardado en un valor** (la fila de D79 y la del miembro que predice sus headers): no cambia. Un frame fabricado para parecer del grupo ya podía decodificar; ahora le basta con pasar su CRC, que es lo mismo para quien lo fabrica.
  - **Un frame genuino del grupo:** siempre pasa su CRC y decodifica, así que la búsqueda lo toma como del grupo, como antes (el test del registro al tope, y las imágenes de crash del Lote 4b, sin cambios).
  - **wasm32:** `zemdb-core` compila a `wasm32-unknown-unknown`; el lote no agrega dependencias.
  - De la revisión:
    - **La reanudación 17 bytes antes del final de un frame del grupo** sigue ahora también después de un frame que pasa su CRC y no decodifica, que antes cortaba la búsqueda como corrupción: depende solo del largo que declara el header, que está bajo su propio CRC.
    - **El dedup que se hidrata** sale de los frames anteriores al daño (`read_frames`); lo que se trunca pierde sus ids, como con DS9.
    - **Un caso de la fila de D79 cambia de resultado:** un frame guardado que pasa su CRC, no decodifica y dice el grupo dañado antes hacía fallar la apertura, y ahora la búsqueda sigue después de él: menos aperturas fallidas, y lo único que puede truncar es lo que ya acepta DS9 (un frame fabricado así ya podía decodificar, con 17 bytes y ninguna operación).
- **Verificación:** `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (1.510 tests) y `cargo doc` sin advertencias; la compilación a `wasm32-unknown-unknown` de core, storage y client; las pruebas largas de `zemdb-core` y del servidor (`cargo test -p zemdb-core -p zemdb-server --release -- --ignored`), las 28 en verde, las mismas que en el Lote 4b, sobre el lote integrado con `2249e0b` y otra vez después de los arreglos de la revisión (entre ellas `random_wal_logs_never_panic_the_replay_long`, con el modelo nuevo del fuzzer, y `losing_any_unsynced_sectors_of_a_group_reopens_with_the_whole_records_before_them_long`).
  - **Crash-safety:** el lote no toca ninguna operación de archivo, ni su orden, ni sus `sync`, ni sus fail points: cambia qué lee la búsqueda de un frame que ya leyó. Las imágenes de crash pasan sin cambios en sus expectativas (`losing_any_unsynced_sectors_of_a_group_reopens_with_the_whole_records_before_them` y su versión larga, `every_cut_of_a_group_written_before_its_fsync_recovers_the_whole_records_before_it`, `a_group_whose_records_each_lost_part_of_their_payload_reopens`).
- **Medición.**
  - **El entorno (DP3):** una VM de 4 vCPU (Intel Xeon a 2,30 GHz) con 15 GiB, kernel `6.18.44-fc-v445` y rustc 1.97.0, otra que la del Lote 8; el servidor en los núcleos 0 y 1 y el generador en el 2 y el 3, el mismo binario del generador (el de `6b93115`) en todas las corridas; tmpfs, `/dev/shm`.
  - **Lo que se compara:** la base es el servidor de `6b93115`, el de la medición del Lote 8, que tiene el mismo código del servidor y de `zemdb-core` que `e9f9139`, el commit anterior al lote (lo que entró en el medio solo toca el cliente); el resultado, este lote. Los dos en release con la información de depuración.
  - **El diseño:** los controles de §5.2, porque el lote no toca el camino de un commit: tres rondas intercaladas empezando por la base, tres más de E2-tmpfs empezando por el resultado, y una comparación A/A de E2-tmpfs después de la revisión (abajo). Las tablas dan la mediana y, entre paréntesis, el rango. Los datos crudos, en `tools/perf/results/fase3_12/lote8b/`.

  | Escenario | Base `6b93115` | Resultado |
  |---|---|---|
  | E1-tmpfs, 1 pedido en vuelo: commits/s, p50 | 8.250 (8.219–11.622), 115 µs | 8.392 (8.222–12.131), 115 µs |
  | E1-tmpfs, 8 pedidos en vuelo: commits/s, p50 | 24.270 (23.935–34.094), 315 µs | 27.645 (24.044–33.211), 276 µs |
  | E2-tmpfs, 500 salas, la base primero: propagación p99 | 1,20 ms (0,96–1,55) | 1,49 ms (1,30–1,80) |
  | E2-tmpfs, 500 salas, el resultado primero: propagación p99 | 1,19 ms (1,17–1,42) | 1,35 ms (1,17–1,48) |
  | E2-tmpfs, 500 salas, las seis rondas: propagación p99, CPU por commit, hilos | 1,19 ms (0,96–1,55), 0,44 ms, 36–37 | 1,41 ms (1,17–1,80), 0,44 ms, 37 |
  | E2-tmpfs, 500 salas, las seis rondas: propagación p99.9 y máxima | 3,8 ms (2,6–4,6), 10 ms (5–25) | 6,5 ms (4,1–7,2), 39 ms (15–86) |

  - **E1-tmpfs** queda igual con 1 pedido en vuelo y un 14% arriba con 8, dentro del ruido: la primera ronda de cada variante da un 40% más que las otras dos.
  - **E2-tmpfs con 500 salas, en estas seis rondas:** el p99 de propagación del resultado queda un 18% arriba en la mediana, contra una dispersión de la base del 49%. La cola sale más alta en los seis pares, en los dos órdenes: el p99.9, un 73%, más que la dispersión de la base (52%) más un 10%, y el máximo, en cinco de seis. El p50, el CPU por commit (0,44 ms) y los hilos (37) son los mismos. El código del lote no corre en esas salas (la búsqueda solo corre al abrir una sala con un daño en `active.wal`), y la revisión pidió una comparación A/A antes de llamarlo ruido.
  - **La comparación A/A** (después de la revisión, en la misma VM): E2-tmpfs con 500 salas y cuatro servidores en un cuadrado latino de cuatro rondas, cada uno una vez en cada lugar de una ronda. `a` es `main` sin el lote (`2249e0b`) y `b`, `main` con el lote ya revisado, compilados igual y en el mismo lugar; `base` y `resultado` son los binarios de las seis rondas de arriba.

  | E2-tmpfs, 500 salas | `a` (sin el lote) | `b` (con el lote) | `base` | `resultado` |
  |---|---|---|---|---|
  | Propagación p99 | 974 µs (892–1.164) | 974 µs (921–1.053) | 956 µs (925–1.112) | 1.000 µs (979–1.029) |
  | Propagación p99.9 | 3,1 ms (2,7–5,5) | 3,0 ms (2,9–4,1) | 2,7 ms (2,2–2,9) | 3,0 ms (2,7–3,5) |
  | Propagación máxima | 14 ms (7–23) | 15 ms (8–47) | 11 ms (7–17) | 12 ms (8–25) |
  | CPU por commit | 0,39 ms | 0,39 ms | 0,40 ms | 0,40 ms |

  - **El criterio:** se cumple. Con la misma compilación, el lote da lo mismo que `main` sin él, también en la cola, y la diferencia entre la base y el resultado de las seis primeras rondas no se repite (+10% en el p99.9, dentro del rango de cada uno): salió de la VM en esa media hora (el p99.9 de la base fue de 2,6 a 4,6 ms entonces, y de 2,2 a 2,9 en la A/A), no del lote. La explicación que daba el borrador, que las corridas crecían con el tiempo, no se sostenía en el bloque que empezaba por el resultado.
  - **Las corridas son válidas según DP3:** sin errores, el head igual a lo confirmado y el generador por debajo del 83% de sus núcleos. Las repeticiones que difieren en más de un 10%: E1-tmpfs con 1 y 8 en vuelo (la primera ronda) y E2-tmpfs, en todas las variantes.
- **La revisión de delta,** por un agente aparte, de contexto fresco y solo lectura, con las seis preguntas. No encontró hallazgos Altos ni Medios; encontró tres Bajos, corregidos en el lote:
  - **El búfer de la búsqueda retenía dos payloads al crecer (B1).**
    - **El caso:** el búfer en que la búsqueda lee los payloads se reusa, y agrandarlo para un payload más largo que el anterior podía copiar el anterior y retener los dos. Pasa con frames genuinos: dos frames del grupo después del daño, de 9 y de 16 MiB, le hacían retener 24 MiB de una vez, y de 16 MiB menos 4 KiB y de 16 MiB, unos 31 MiB. Venía del Lote 4b, y lo contradecía el "a lo sumo un frame" con que el lote pasa INV-B1 a ✅.
    - **El arreglo:** un payload más largo que el búfer va a un búfer nuevo. Lo prueba el quinto caso del test de la memoria, que con el código anterior daba 24 MiB y ahora 16.
  - **La frontera de 8 bytes no tenía test (B2):** un payload de solo el campo, o de menos de 17 bytes, tomado como de un grupo posterior sobrevivía a los tests. El test del frame que no decodifica suma payloads de 8 y de 16 bytes, y las dos mutaciones mueren.
  - **La medición de E2 (B3):** la cola del resultado salía más alta en los seis pares, y la explicación del borrador no se sostenía. La comparación A/A de arriba lo resuelve: el lote no cambia E2.
  - **Notas, incorporadas:**
    - el test de la memoria corría sus casos en un solo proceso, y los que seguían reusaban memoria residente que ya tenía el proceso (arrancaban con hasta 52 MiB y medían 0): ahora corre cada caso en un proceso propio. Un allocator que cuente, la otra opción que sugirió la revisión, no se puede: el workspace prohíbe el código `unsafe`;
    - tres interacciones que faltaban: la reanudación después de un frame que no decodifica, el dedup y el caso de la fila de D79 que cambia de resultado (arriba, en las interacciones);
    - el plan y `ROADMAP.md` dan el lote por cerrado: entran juntos con el commit.
  - **La pregunta 6:** las corridas son válidas según DP3 (el mismo `boot_id` y el mismo binario del generador, `taskset` 0,1 para el servidor y 2,3 para el generador, sin errores y con el head igual a lo confirmado); la base de `6b93115` vale, porque `git diff 6b93115 e9f9139` no toca el servidor, `zemdb-core` ni el storage; y las pruebas largas no se superpusieron con la medición.
  - **Lo que verificó por su cuenta:** las cuatro mutaciones del lote; cuatro más (el campo leído como varint o en big-endian mueren; las dos de la frontera de 8 bytes sobrevivían, B2); que ningún frame que escribe el servidor puede pasar su CRC y no decodificar (un registro por operación, con la cantidad de operaciones exacta), que los del storage dicen distancia 0, y que un daño accidental tendría que pasar los dos CRC (unos 2^-64): así que lo único que puede truncar algo confirmado sigue siendo la fila de §4 de un miembro que predice sus propios headers.
- **Desvíos:**
  - **La opción:** la alternativa del plan, no la propuesta de la revisión; la eligió el dueño del proyecto en el paso 0, con la medición de arriba. El título del lote y su mensaje de entrega cambian con ella.
  - **El modelo del fuzzer cambia:** el plan pedía el fuzzer de la búsqueda sin cambios en sus expectativas. Con la opción elegida, un frame que pasa su CRC y no decodifica es del grupo que dice, y el modelo de la búsqueda hecha a mano lo sigue. Las demás expectativas del fuzzer, y las de las imágenes de crash, no cambian.
- **El tamaño:** unas 20 líneas de producción agregadas y 5 quitadas, sin comentarios ni blancos.
- **Un tropiezo de entorno, sin efecto en el resultado:** mientras corrían las pruebas largas, entraron a `main` dos commits de otra sesión que sacan los códigos de los comentarios (`4f3b467`, `2249e0b`), también en dos archivos de tests del lote. Las pruebas largas se cortaron, el lote se integró con un `git stash` de esos dos archivos (sin conflictos: las líneas no se tocan) y se corrieron de nuevo desde el principio.

### Lote 8c · Una sala que abre después de que venció su pedido · ⬜
**Objetivo:** que una sala cuya apertura pasa de `ACTOR_TIMEOUT` abra igual. La revisión de cierre del Lote 8 rompió ahí INV-L1. El dueño del proyecto eligió arreglarlo en un lote nuevo (12 de Noviembre de 2026). Cierra la fase.

**Lo que encontró la revisión:**
- **El mecanismo:** desde el Lote 3, parte 2 (`9214df3`), `RoomManager::ask` pone el plazo de `ACTOR_TIMEOUT` sobre `slot()`, y la apertura (`spawn_actor`, que corre `RoomActor::open` en el pool con `io_locked`) es un `.await` de ese pedido. Si el plazo vence, el actor recién abierto se descarta, y el pedido siguiente abre la sala de nuevo desde cero.
- **Las consecuencias:** una sala cuya apertura, con su espera en el pool, dura más de 5 s no abre nunca. Con la cola del pool cerca de 5 s (como la que midió el Lote 7 en un disco de nube antes de DS7), casi toda apertura se descarta.
- **Antes del Lote 3,** la apertura corría con `block_in_place`, sin un `.await`, y la sala quedaba registrada aunque el pedido recibiera `Timeout`.
- **Los tests de la revisión** fallan en `6b93115` (3 de 3) y pasan con el arreglo que propone.

**Archivos:** `crates/server/src/actor/manager.rs` y sus tests; el docstring de `RoomActor::announcement` (`actor/room.rs`), un Bajo de la revisión.

**Qué se hace:**
- **La propuesta de la revisión,** probada en su copia (unas 40 líneas): la apertura y el registro de su actor corren en una tarea propia (`tokio::spawn`), que se lleva el lock de spawn y `Listed`, y quien la lanzó espera su `JoinHandle`. Una apertura que nadie espera registra igual su actor, y el pedido sigue recibiendo `Timeout` a los 5 s.
- **La alternativa:** no cortar la apertura.
- Se decide con el dueño del proyecto al arrancar (paso 0).

**Tests exigidos:**
- Los dos de la revisión, traídos al repo con un nombre propio y sin códigos (`docs/audits/2026-11-fase3_12-cierre/`): una apertura frenada, y una que espera el pool lleno.
- `a_spawn_dropped_while_its_room_opens_leaves_nothing_and_keeps_the_lock_until_the_end` pasa a fijar lo nuevo: una apertura cortada termina, registra la sala y retiene el lock hasta el final.
- Una apertura que falla deja la sala sin registrar y el lock libre.
- Una sala que se borra mientras su apertura corre.

**Interacciones a revisar:**
- INV-L2: una sala abierta por un pedido cortado no es estado a medias.
- INV-L7: el lock, hasta que termina la operación que serializa.
- Las recargas del esquema (`Listed`) y el borrado de una sala.
- El apagado (`shutdown_all`) con una apertura en vuelo.
- `wait_for_previous_actor`, y el relay, que también abre salas.

**Metodología:** §5.1, con el checklist de concurrencia del servidor; las versiones largas del servidor; y la medición de los controles.

**Documentos:**
- `INVARIANTES.md`: el estado y el enunciado de INV-L1, y el cierre de la fase.
- `ARCHITECTURE.md`: el reintento de un `Timeout` (§7.2) y la reapertura en el apagado (§5.4).
- Este plan, con el cierre de la fase, el plan de evaluaciones y `ROADMAP.md`.

**Entrega:** `fix(server): a room whose opening outlasts its request stays open, and close Fase 3.12 (Fase 3.12, Lote 8c)`.

**Hecho.** *Pendiente.*

---

## 6. Cómo se audita y cómo se prueba

- **Revisión de delta en cada lote,** con la pregunta de §5.4; en los Lotes 3 y 4, con dos revisores independientes (durabilidad y concurrencia).
- **Revisión de cierre de la fase** en el Lote 8, contra INV-B1, B3, B4, B5, D1, D2, D3, D7, E3, L1, L2, L3, L5, L7, R1, R3, Y1, Y2, Y4, Y5, A1, W1 y W2. No salió en verde (INV-L1, B1 y W1): la siguen los Lotes 8b (B1 y W1, cerrado) y 8c, cada uno con su revisión de delta (`docs/METODOLOGIA.md` §6). No hace falta una auditoría amplia: el servidor ya está en el alcance auditado y el modelo de amenaza no cambia (`INVARIANTES.md` §1.4).
- **Las cuatro capas de pruebas** (`docs/METODOLOGIA.md` §7): una prueba por regla en cada lote; las imágenes de crash y los fail points para los Lotes 3, 4 y 7; los fuzzers de mensajes para el Lote 5; y la medición como una capa más, que no prueba corrección pero sí que el lote hace lo que promete.

---

## 7. Riesgos

| Riesgo | Mitigación |
|---|---|
| El pool de E/S o el *group commit* rompen la durabilidad de un `CommitAck` | Los checklists de crash-safety y de concurrencia, las imágenes de crash antes y después, y dos revisores de delta en los Lotes 3 y 4 |
| El pool o el grupo suman latencia a un commit aislado | El criterio del Lote 4 incluye la latencia con un pedido en vuelo; el control E1-tmpfs con 1 pedido en vuelo, en todos los lotes |
| La memoria del canal de eventos con el evento `ops` | El tope de 4 KiB por evento y el tope total de los eventos retenidos (DS5, INV-B3); un test con streams que no leen en muchas salas |
| El evento `ops` y la sub-fase 4.3 se desalinean | El Lote 5 va antes de 4.3, y el formato del evento se escribe en `ARCHITECTURE.md` en el mismo lote |
| La máquina de las mediciones cambia entre lotes | La línea de base se mide en la misma sesión que el resultado (§5.1, paso 0) |
| Un disco de nube cambia las conclusiones | El Lote 7 mide ahí antes de decidir DS7, y la medición final lo menciona si no se pudo hacer |

---

## 8. Seguimiento

| Lote | Qué | Estado |
|---|---|---|
| 0 | Contrato | ✅ |
| 1 | El arnés en `main` y la línea de base | ✅ (la línea de base, medida con el Lote 2) |
| 2 | El presupuesto de respuestas | ✅ |
| 3 | La E/S fuera de los workers | ✅ |
| 4 | *Group commit* | ✅ |
| 4b | El último grupo dañado | ✅ |
| 5 | El SSE: apagarlo y el evento `ops` | ✅ |
| 6 | El catch-up sin copias | ✅ (sin cumplir su criterio: lo sigue el 6b) |
| 6b | La reserva por tramos | ✅ |
| 7 | El roster | ✅ (sin cumplir la primera parte de su criterio, con el visto bueno del dueño del proyecto) |
| 8 | Medición final y revisión de cierre | ✅ (su revisión de cierre no salió en verde: la siguen el 8b y el 8c) |
| 8b | La búsqueda después de un grupo, sin decodificar las operaciones | ✅ |
| 8c | Una sala que abre después de que venció su pedido | ⬜ |

**Avance:** plan escrito el 1 de Noviembre de 2026, a partir de las Evaluaciones 1 y 2. Lote 1 cerrado el mismo día, adelantado al Lote 0 (el arnés en `main`; la línea de base se mide con el Lote 2). Lote 0 cerrado el mismo día: DS1 a DS8 adoptadas y `INVARIANTES.md` en su versión 12. Lote 2 cerrado el 2 de Noviembre de 2026: el presupuesto de respuestas reserva por lo que se lee, sin `503` del presupuesto con 1.000 salas; con él se midió la línea de base de la fase. Lote 3 cerrado el mismo día: la E/S de archivos de las salas corre en pools acotados, fuera de los workers de Tokio, con los hilos del proceso fijos; los criterios de 1.000 salas no se cumplen en la máquina de medición, saturada de CPU por el SSE de señales. Lote 4 cerrado el mismo día: los commits que esperan en una sala comparten un `fsync`, y una sala en disco pasa de unos 2.200 commits por segundo a 5.600 con 8 pedidos en vuelo y a 8.200 con 64; abre un riesgo, aceptado hasta el Lote 4b (DS9, agregado al plan el 3 de Noviembre de 2026): una sala cuyo grupo corta un corte de luz puede no abrir. Lote 4b cerrado el mismo día: cada registro dice a cuántos bytes de él empieza su grupo, y la apertura trunca un último grupo roto como un último registro roto, sin cambiar el rendimiento; la revisión de delta dejó un riesgo menor en `INVARIANTES.md` §4 (un miembro que predice sus propios registros siguientes). Lote 5 cerrado el mismo día: el SSE se puede apagar (`EventsDisabled`) y trae las operaciones de cada grupo de commits en el evento `ops`, con la encarnación de la sala y dos topes, el del evento y el total de los retenidos; con 1.000 salas, el CPU del servidor por commit baja de 1,16 a 0,47 ms y la propagación en p99, de 5,9 a 1,5 ms. De la revisión de delta: el stream termina cuando vence su token. Lote 6 cerrado el 6 de Noviembre de 2026 (medido el 4): el catch-up y el `/sync` comparten las operaciones del búfer en RAM en lugar de copiarlas, y E1-actor con 64 pedidos en vuelo pasa de 10.900 a 31.300 commits por segundo (con 8, de 42.400 a 60.100); su criterio (con 64, el 80% de 8) no se cumple, con el 52%, por dos costos por operación que no son copia, y el dueño del proyecto aceptó el desvío y agregó el Lote 6b (DS10, la reserva por tramos). Lote 6b cerrado el 8 de Noviembre de 2026: el dueño del proyecto adoptó DS10 al arrancarlo, y la lectura del log que arma una respuesta hace crecer su reserva de a tramos de 256 KiB, en lugar de una vez por operación, y la respuesta no vuelve a medir lo que lleva; E1-actor con 64 pedidos en vuelo pasa de 30.900 a 93.200 commits por segundo (con 8, de 66.800 a 73.200), el 127% de los de 8, y cumple el criterio que el Lote 6 no cumplió. Lote 7 cerrado el 11 de Noviembre de 2026: medido en un disco de nube emulado (con un límite de escrituras por segundo), el roster resultó el 30% de las unas 4 escrituras al disco de cada commit, y con 3.000 por segundo decidía si 1.000 salas aguantaban; el dueño del proyecto adoptó DS7 con N = 10 s, otro criterio y una grilla por sala, y el roster de cada sala se escribe como mucho cada 10 s, en los puntos de su grilla: con 3.000 escrituras por segundo, 1.000 salas pasan de colapsar (el 84% de sus commits, 17 s en p99) a sostenerlos todos, y 500, de 43–54 a 4 ms en p99; sin límite, las escrituras por commit bajan un 28% con 1.000 salas y un 26% con 500, debajo del 30% que pedía su criterio (la fase de la grilla cuesta unos 6 puntos), con el visto bueno del dueño del proyecto. Lote 8 cerrado el 12 de Noviembre de 2026: la medición final de la fase, contra su línea de base y en la misma VM, cumple la meta de §0 (con 1.000 salas, la propagación en p99 pasa de 11,3 s a 2,1 ms en tmpfs y de 21 s a 4,9 ms en disco, sin `503`, con 0,44 ms de CPU del servidor por commit y 37 hilos), y una sala en disco con 64 pedidos en vuelo pasa de 3.000 a 18.200 commits por segundo. Su revisión de cierre, con tres revisores, no salió en verde: una sala cuya apertura pasa de `ACTOR_TIMEOUT` no abre (INV-L1, desde el Lote 3), y la búsqueda después de un grupo dañado puede reservar 111 MiB (INV-B1 y W1, desde el Lote 4b). El dueño del proyecto eligió arreglarlas en los Lotes 8b y 8c. Lote 8b cerrado el 13 de Noviembre de 2026: la búsqueda después de un grupo dañado lee de cada frame que encuentra solo dónde empieza su grupo, sin decodificar sus operaciones, y retiene a lo sumo un frame (16 MiB, contra 111 MiB con un frame guardado lleno de `Null` y 81 MiB con uno de 541.000 operaciones sin valores, que el tope de valores de un mensaje, la propuesta de la revisión, no habría frenado). El próximo paso es el Lote 8c.
