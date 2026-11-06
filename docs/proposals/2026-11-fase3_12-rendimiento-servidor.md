# Rendimiento del servidor — Plan de la Fase 3.12

**Fecha:** 1 de Noviembre de 2026
**Tipo:** Optimización del servidor y del protocolo. Documento vivo: plan, lotes y seguimiento. La fase es chica y no tiene sub-fases: este plan baja directamente a lotes (`docs/METODOLOGIA.md` §1).
**Estado:** En curso. Las decisiones DS1 a DS8 las adoptó el dueño del proyecto en el Lote 0 (1 de Noviembre de 2026); DS7, condicionada a la medición del Lote 7. DS9 salió del Lote 4 y la adoptó el dueño el 3 de Noviembre de 2026, con el mecanismo que eligió al arrancar el Lote 4b (la distancia en bytes al inicio del grupo). DS10 salió de la medición del Lote 6, y el dueño del proyecto la dejó para un Lote 6b el 6 de Noviembre de 2026: es una *Propuesta* que se confirma al arrancarlo. El contrato está en la versión 17 de `INVARIANTES.md`.
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
| **DS7** | El roster, menos seguido, si la medición lo justifica | 7 | Adoptada (1 de Noviembre de 2026), condicionada a la medición del Lote 7 |
| **DS8** | El arnés en `main` y la medición de cada lote | 1 | Adoptada (1 de Noviembre de 2026) |
| **DS9** | El último grupo de commits dañado es una escritura rota, como el último registro | 4b | Adoptada (3 de Noviembre de 2026; el mecanismo, al arrancar el Lote 4b) |
| **DS10** | La reserva de una lectura, por tramos, y sin una segunda medición | 6b | Propuesta (6 de Noviembre de 2026), a confirmar al arrancar el Lote 6b |

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
- **El costo aceptado:** si el disco daña el último grupo después de su `fsync` (una falla del medio, no un corte), se truncan hasta un grupo de commits confirmados (64 commits o 1 MiB de operaciones) en lugar de un registro: la excepción de D78 pasa del último registro al último grupo, con la misma justificación (no se distingue de una escritura rota).
- **Lo que exige:** un cambio del formato del registro del WAL del servidor (antes de la primera versión no hay compatibilidad que conservar, `GEMINI.md`): 8 bytes más por registro. El WAL del storage escribe un frame por `fsync`, así que no lo necesita; como comparte el formato del frame, sus frames llevan el campo en 0 (un grupo de uno), y sus reglas no cambian.
- **Alternativas descartadas.** Aceptar el riesgo para siempre: el grupo sirve justamente a las salas con commits a la vez, que son las expuestas. Tratar como escritura rota cualquier daño seguido de frames válidos: truncaría commits confirmados ante una corrupción real (INV-D1). Un grupo en un solo frame: el payload lleva un solo `mutation_id`, así que también cambia el formato, y más.
- **Invariantes:** INV-D2 (la excepción de D78), INV-D3, INV-D1, INV-D8, INV-D10.

### DS10 · La reserva de una lectura, por tramos, y sin una segunda medición
Salió de la medición del Lote 6 (su nota **Hecho**). El dueño del proyecto eligió, el 6 de Noviembre de 2026, cerrar el Lote 6 con su criterio sin cumplir y dejar esto para un Lote 6b; la decisión se confirma al arrancarlo.
- **Desde el Lote 6.** Con 64 pedidos en vuelo, cada commit de un grupo trae un catch-up de hasta unas 64 operaciones (el protocolo de 1 RTT), así que lo que se hace por operación de cada catch-up crece con el cuadrado del grupo. Sin la copia (DS6), quedan dos costos así:
  - **la reserva de la respuesta** (DS1) crece una vez por operación (`ReadBudget::admit`, que llama a `InflightBudget::take`: un mutex, la parte del cliente en un `HashMap` y el semáforo del total), el 21% del CPU de E1-actor con 64;
  - **`fit_in_response` vuelve a medir** cada operación con `encoded_len` (~9%), aunque la lectura ya las acotó: el presupuesto de lectura cobra `max(wire, 24 × valores)`, que nunca es menos que el tamaño codificado.
- **Propuesta.**
  - **La reserva por tramos:** una lectura hace crecer la reserva de su respuesta de a tramos (256 KiB en el prototipo, o lo que le falte a la operación si es más), descuenta de lo reservado lo que toma cada operación que admite y, al terminar, devuelve lo que no usó. Si un tramo no entra, la reserva crece exactamente lo que le falta a la operación, como hoy: la lectura se sigue cortando en la primera operación que no tiene lugar, y cada respuesta sigue contando todas sus operaciones.
  - **Lo que cambia:** mientras dura una lectura (microsegundos en la RAM, milisegundos en disco), su respuesta retiene hasta un tramo más de lo que usa, en la parte de su cliente y en el total: unos 5 MiB en el peor caso de fábrica (las lecturas que pueden ir a la vez son unas 18, los workers y el pool de E/S), sobre 256 MiB. Con el presupuesto casi lleno, otra respuesta puede salir un poco más corta, o con `Unavailable`, un poco antes.
  - **Sin la segunda medición:** `fit_in_response` deja de medir las operaciones de una lectura del log, que ya cumplen el tope de una respuesta, con un test que lo fija.
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
| 6b | La reserva por tramos | server | DS10 | B1, B5 | ⬜ |
| 7 | El roster | server | DS7 | R3, R1, D4 | ⬜ |
| 8 | Medición final y revisión de cierre | Documentos | Todas | Las que tocó la fase | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirmaron en el Lote 0, después del Lote 1, que se adelantó (su nota **Hecho**).
- El 1 va antes de cualquier optimización: sin el arnés en `main` no hay medición.
- El 2 no depende de los demás. El 3 va antes del 4, que hace su `fsync` en el pool del 3. El 4b va después del 4 y antes del 5: cierra el riesgo que abrió el 4. El 5 va antes de que arranque la sub-fase 4.3 de la Fase 4, y después del 4, porque con el *group commit* un evento puede llevar las operaciones de un grupo. El 6, después del 2, porque cambia lo que reserva una respuesta. El 6b, después del 6, de cuya medición salió. El 7 depende de la medición en un disco de nube y puede quedar para el final. El 8 cierra.

**Tamaño estimado de producción:** el Lote 2, de 100 a 150 líneas; el 3, de 200 a 300; el 4, de 150 a 250; el 4b, de 100 a 200; el 5, unas 200 entre core y servidor (con el tope total de los eventos `ops`); el 6, unas 80; el 6b, unas 40; el 7, unas 50. El 1 no tiene código de producción.

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

### Lote 6b · La reserva por tramos · ⬜
**Objetivo:** que un catch-up no visite el presupuesto de respuestas ni vuelva a medir sus operaciones una vez por operación (DS10), con el criterio que el Lote 6 no cumplió.

**Archivos:** `crates/server/src/log/budget.rs` (`ReadBudget`), `response_budget.rs` (`Reservation`), `actor/room.rs` (`fit_in_response`), y sus tests.

**Qué se hace:** lo de DS10, con el mecanismo que se confirme al arrancar el lote (el tramo, y si se reserva por tramos o una vez al final).

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

**Hecho.** *Pendiente.*

### Lote 7 · El roster · ⬜
**Objetivo:** decidir DS7 con una medición en un disco de nube y, si se adopta, implementarla.

**Archivos, si se adopta:** `crates/server/src/actor/room.rs` (el tick), `actor/lease.rs`, `config.rs` (`ZEMDB_ROSTER_PERSIST_SECS`), y sus tests. `ARCHITECTURE.md` §5.2 (*Client Roster Persistence*).

**Qué se hace:** primero, E2-disco en una máquina con un disco de nube con límite de operaciones por segundo (las escrituras por segundo y el tiempo ocupado del disco). El dueño del proyecto decide con esos números si se adopta DS7. Si se adopta: el roster se escribe cada N segundos en lugar de cada tick, salvo antes de una poda que borre (D56).

**Tests exigidos, si se adopta.**
- Un crash pierde como mucho N segundos de cambios del roster, y eso solo causa más retención, un re-registro o un snapshot de más (INV-R3).
- Una poda que borra persiste el roster antes, como hoy (INV-R1, D56).
- Un apagado ordenado persiste el roster (INV-L5).

**Interacciones a revisar:** los cursores que reportan los commits y los `Ack`; los estados del lease (`Connected`, `Disconnected`, `Dormant`); la designación de snapshots; la apertura con un roster viejo.

**Medición:** E2-disco en el disco de nube, con 500 y 1.000 salas. **Criterio, si se adopta:** las escrituras al disco por commit bajan a menos de la mitad (hoy, entre 7 y 9).

**Metodología:** §5.1, con los checklists de crash-safety y de medición, si se adopta. Si se descarta, el lote cierra con la medición en su nota **Hecho** y DS7 pasa a *Descartada*.

**Documentos:** `ARCHITECTURE.md` §5.2; `INVARIANTES.md`: INV-R3.

**Entrega:** `perf(server): persist the client roster every few seconds instead of every tick (Fase 3.12, Lote 7)`, si se adopta.

**Hecho.** *Pendiente.*

### Lote 8 · Medición final y revisión de cierre · ⬜
**Objetivo:** cerrar la fase.

**Qué se hace:**
- **La medición final:** todos los escenarios de §5.2 sobre el último commit de la fase, contra la línea de base del Lote 1, en la misma máquina; una tabla de la fase entera, también en el plan de evaluaciones.
- **La revisión de cierre** (`docs/METODOLOGIA.md` §6), contra todas las invariantes que tocó la fase, mirando solo lo que la fase cambió y los mecanismos con los que interactúa (§6).
- **Los documentos:** `ARCHITECTURE.md`, `INVARIANTES.md` con el estado de cada invariante, `ROADMAP.md` y `README.md`.

**Entrega:** `docs: close Fase 3.12 with its final measurement and the closing review (Fase 3.12, Lote 8)`.

**Hecho.** *Pendiente.*

---

## 6. Cómo se audita y cómo se prueba

- **Revisión de delta en cada lote,** con la pregunta de §5.4; en los Lotes 3 y 4, con dos revisores independientes (durabilidad y concurrencia).
- **Revisión de cierre de la fase** en el Lote 8, contra INV-B1, B3, B4, B5, D1, D2, D3, D7, E3, L1, L2, L3, L5, L7, R1, R3, Y1, Y2, Y4, Y5, A1, W1 y W2. No hace falta una auditoría amplia: el servidor ya está en el alcance auditado y el modelo de amenaza no cambia (`INVARIANTES.md` §1.4).
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
| 6b | La reserva por tramos | ⬜ |
| 7 | El roster | ⬜ |
| 8 | Medición final y revisión de cierre | ⬜ |

**Avance:** plan escrito el 1 de Noviembre de 2026, a partir de las Evaluaciones 1 y 2. Lote 1 cerrado el mismo día, adelantado al Lote 0 (el arnés en `main`; la línea de base se mide con el Lote 2). Lote 0 cerrado el mismo día: DS1 a DS8 adoptadas y `INVARIANTES.md` en su versión 12. Lote 2 cerrado el 2 de Noviembre de 2026: el presupuesto de respuestas reserva por lo que se lee, sin `503` del presupuesto con 1.000 salas; con él se midió la línea de base de la fase. Lote 3 cerrado el mismo día: la E/S de archivos de las salas corre en pools acotados, fuera de los workers de Tokio, con los hilos del proceso fijos; los criterios de 1.000 salas no se cumplen en la máquina de medición, saturada de CPU por el SSE de señales. Lote 4 cerrado el mismo día: los commits que esperan en una sala comparten un `fsync`, y una sala en disco pasa de unos 2.200 commits por segundo a 5.600 con 8 pedidos en vuelo y a 8.200 con 64; abre un riesgo, aceptado hasta el Lote 4b (DS9, agregado al plan el 3 de Noviembre de 2026): una sala cuyo grupo corta un corte de luz puede no abrir. Lote 4b cerrado el mismo día: cada registro dice a cuántos bytes de él empieza su grupo, y la apertura trunca un último grupo roto como un último registro roto, sin cambiar el rendimiento; la revisión de delta dejó un riesgo menor en `INVARIANTES.md` §4 (un miembro que predice sus propios registros siguientes). Lote 5 cerrado el mismo día: el SSE se puede apagar (`EventsDisabled`) y trae las operaciones de cada grupo de commits en el evento `ops`, con la encarnación de la sala y dos topes, el del evento y el total de los retenidos; con 1.000 salas, el CPU del servidor por commit baja de 1,16 a 0,47 ms y la propagación en p99, de 5,9 a 1,5 ms. De la revisión de delta: el stream termina cuando vence su token. Lote 6 cerrado el 6 de Noviembre de 2026 (medido el 4): el catch-up y el `/sync` comparten las operaciones del búfer en RAM en lugar de copiarlas, y E1-actor con 64 pedidos en vuelo pasa de 10.900 a 31.300 commits por segundo (con 8, de 42.400 a 60.100); su criterio (con 64, el 80% de 8) no se cumple, con el 52%, por dos costos por operación que no son copia, y el dueño del proyecto aceptó el desvío y agregó el Lote 6b (DS10, la reserva por tramos). El próximo paso es el Lote 6b.
