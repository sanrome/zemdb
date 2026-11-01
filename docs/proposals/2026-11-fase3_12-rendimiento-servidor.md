# Rendimiento del servidor — Plan de la Fase 3.12

**Fecha:** 1 de Noviembre de 2026
**Tipo:** Optimización del servidor y del protocolo. Documento vivo: plan, lotes y seguimiento. La fase es chica y no tiene sub-fases: este plan baja directamente a lotes (`docs/METODOLOGIA.md` §1).
**Estado:** Propuesta. Las decisiones DS1 a DS8 se confirman con el dueño del proyecto en el Lote 0, antes de escribir código que dependa de ellas.
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
  - la copia de las operaciones en cada catch-up (DS6);
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
- **El catch-up sin copias** (DS6, Lote 6).
- **El roster**, si la medición en un disco de nube lo justifica (DS7, Lote 7).

### 2.2. No entra
- **Varias operaciones por commit** (el outbox, `ROADMAP.md` §3) y **una conexión WebSocket:** cambian el protocolo de escritura, y con el evento `ops` y el `periodic` del SDK la difusión deja de ser lo caro.
- **Seguir procesando comandos mientras corre un `fsync`** (*pipelining*): el *group commit* de DS3 espera el `fsync` del grupo antes de atender el próximo comando. Lo que ganaría hacerlo se mide después de este plan.
- **Un WAL compartido entre salas:** hace falta cerca del techo del modelo y en discos de nube (§4.8.3 de las evaluaciones), no ahora.
- **La replicación y la durabilidad relajada** (`ROADMAP.md` §3, punto 18).
- **Otro asignador de memoria, la cantidad de archivos de segmento** (uno cada 1.000 operaciones) **y la pausa de 5 s** de la Evaluación 1: quedan como próximos pasos del plan de evaluaciones (§6).

---

## 3. Decisiones

Prefijo **DS**. Todas son *Propuestas* hasta el Lote 0.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **DS1** | El presupuesto de respuestas reserva lo que se lee, no un frame entero | 2 | Propuesta |
| **DS2** | La E/S de los actores, en un pool fijo de hilos dedicados | 3 | Propuesta |
| **DS3** | *Group commit* sin espera, por sala | 4 | Propuesta |
| **DS4** | El SSE se puede apagar | 5 | Propuesta |
| **DS5** | El evento `ops`: el SSE trae las operaciones | 5 | Propuesta |
| **DS6** | El catch-up comparte las operaciones del búfer en RAM | 6 | Propuesta |
| **DS7** | El roster, menos seguido, si la medición lo justifica | 7 | Propuesta, condicionada a la medición del Lote 7 |
| **DS8** | El arnés en `main` y la medición de cada lote | 1 | Propuesta |

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
- **Propuesta.** Cuando un commit (o un grupo, con DS3) se vuelve durable, el actor emite un evento `ops` con sus operaciones, en lugar de `head_advanced`, si ocupan hasta un tope (por ejemplo, 4 KiB codificadas, que fija el Lote 0). El evento lleva un frame del protocolo en base64 (SSE es texto), con un mensaje nuevo de `ServerMessage`, al final, que trae la sala, las operaciones, el head y el esquema de esas operaciones (`schema_id` y `schema_version`, INV-E3). Se codifica una sola vez y lo comparten todos los streams de la sala. Si las operaciones pasan el tope, el evento es `head_advanced`, como hoy, y el cliente hace `/sync`. Un stream que se atrasa recibe `lagged`, como hoy.
- **La memoria.** El canal de eventos de cada sala retiene hasta `EVENTS_CAPACITY` (256) eventos para el stream más atrasado; con el tope de 4 KiB, hasta 1 MiB por sala con streams abiertos. El Lote 0 decide si eso entra en INV-B3 tal como está ("SSE con tope") o si hace falta un tope en bytes por sala.
- **Lo que no cambia:** la autenticación del stream y su tope; las columnas cifradas siguen cifradas (las cifra el cliente); los eventos siguen siendo un atajo, y un cliente que pierde uno lo nota por el hueco en su cursor y hace `/sync`.
- **Antes de la primera versión no hay compatibilidad que conservar** (`GEMINI.md`): `head_advanced` queda para las operaciones grandes, no como modo alternativo.
- **Invariantes:** INV-Y2 (lo que trae un evento es lo que está en el log, en orden, y solo lo durable), INV-D7, INV-E3, INV-B3, INV-B5, INV-A1, INV-W1, INV-W2.

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

---

## 4. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DS1 a DS8 | Ajustes: B1, B3, B4, B5, D1, D7, E3, L7, R3, Y2 | ⬜ |
| 1 | El arnés en `main` y la línea de base | `tools/perf` (fuera del workspace) | DS8 | — | ⬜ |
| 2 | El presupuesto de respuestas | server | DS1 | B1, B5, L1 | ⬜ |
| 3 | La E/S fuera de los workers | server | DS2 | D1, D7, L3, L5, L7, B3, B4 | ⬜ |
| 4 | *Group commit* | server | DS3 | D1, D2, D7, Y1, Y4, Y5, L3, B4 | ⬜ |
| 5 | El SSE: apagarlo y el evento `ops` | core, server | DS4, DS5 | Y2, D7, E3, B3, B5, A1, W1, W2 | ⬜ |
| 6 | El catch-up sin copias | server | DS6 | B1, W2, Y2 | ⬜ |
| 7 | El roster | server | DS7 | R3, R1, D4 | ⬜ |
| 8 | Medición final y revisión de cierre | Documentos | Todas | Las que tocó la fase | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirman antes del Lote 1.
- El 1 va antes de cualquier optimización: sin el arnés en `main` no hay medición.
- El 2 no depende de los demás. El 3 va antes del 4, que hace su `fsync` en el pool del 3. El 5 va antes de que arranque la sub-fase 4.3 de la Fase 4, y después del 4, porque con el *group commit* un evento puede llevar las operaciones de un grupo. El 6, después del 2, porque cambia lo que reserva una respuesta. El 7 depende de la medición en un disco de nube y puede quedar para el final. El 8 cierra.

**Tamaño estimado de producción:** el Lote 2, de 100 a 150 líneas; el 3, de 200 a 300; el 4, de 150 a 250; el 5, unas 150 entre core y servidor; el 6, unas 80; el 7, unas 50. El 1 no tiene código de producción.

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

### Lote 0 · Contrato · ⬜
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

**Hecho.** *Pendiente.*

### Lote 1 · El arnés en `main` y la línea de base · ⬜
**Objetivo:** que cada lote pueda medir antes y después sin ramas (DS8, DP1).

**Archivos:** `tools/perf/` (nuevo): `Cargo.toml` con su propio `[workspace]`, `src/main.rs` y `src/rooms.rs` (el generador), `run_e1.sh`, `run_e2.sh`, `e2_all.sh`, `e2f_all.sh`, `profile_e1.sh`, `summarize.sh`, `summarize_e2.sh`, `README.md`, `results/` (los datos crudos de las evaluaciones) y `diagnostics/` (los parches de diagnóstico). El `Cargo.toml` de la raíz suma `exclude = ["tools/perf"]`.

**Qué se hace:** traer el generador y los scripts de las ramas `perf/e1-commit` y `perf/e2-salas`, sin los cambios de diagnóstico del servidor, que van como parches; ajustar los caminos de los scripts; `cargo fmt` y `cargo clippy` sobre el arnés; el `README`, con cada escenario de §5.2, cómo correrlo, cómo preparar la máquina (el rango de puertos locales, el límite de archivos abiertos, `perf`) y cómo leer los resultados. Después, borrar las dos ramas y `claude/evaluaciones-performance`.

**Tests exigidos:** el arnés compila desde `tools/perf` y cada escenario de §5.2 corre en su versión corta (una repetición de pocos segundos) y da una corrida válida; el workspace no cambia (`cargo test --workspace` no compila el arnés).

**Interacciones a revisar:** la CI (el arnés no entra); el `Cargo.lock` de la raíz no cambia; los parches de diagnóstico aplican sobre el commit del lote.

**Medición:** la línea de base de la fase: todos los escenarios de §5.2 sobre el commit del lote, con 3 repeticiones, en la nota **Hecho**.

**Metodología:** el procedimiento de §5.1 sin los pasos de producción (sin tests de comportamiento ni mutación inversa); el checklist de medición de §5.3. La revisión de delta mira que el arnés no toque el workspace ni el servidor.

**Documentos:** el plan de evaluaciones (DP1: el arnés ya está en `main`).

**Entrega:** `chore(perf): the measurement harness of the performance evaluations in tools/perf (Fase 3.12, Lote 1)`.

**Hecho.** *Pendiente.*

### Lote 2 · El presupuesto de respuestas · ⬜
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

**Hecho.** *Pendiente.*

### Lote 3 · La E/S fuera de los workers · ⬜
**Objetivo:** que ningún worker de Tokio haga E/S de archivos, y que los hilos no crezcan con las salas (DS2).

**Archivos:** `crates/server/src/blocking.rs` (reemplazado por el pool), `log/warm_disk.rs`, `log/tiered_log.rs`, `log/cold_disk.rs` (la compresión), `durable.rs`, `actor/room.rs`, `actor/lease.rs`, `actor/manager.rs` (la apertura y el apagado), `config.rs` (`ZEMDB_IO_THREADS`), y sus tests. `ARCHITECTURE.md` §5.2 (*Blocking I/O in the Actor*) y la tabla de configuración.

**Qué se hace:** el pool fijo; cada operación de archivo de un actor se manda al pool y se espera; la compresión a cold en el pool o en uno propio acotado; el apagado espera las operaciones en curso. El valor de fábrica de `ZEMDB_IO_THREADS` sale de la medición.

**Tests exigidos.**
- Con cientos de salas haciendo commits a la vez, la cantidad de hilos del proceso no pasa de los workers más el pool más una cantidad fija (hoy pasa de 400).
- Un `fsync` lento (un fail point que lo demora) en una sala no demora a otra sala que no comparte su hilo del pool.
- Una sala que se detiene, se apaga o se reabre con una operación en el pool la espera antes de soltar su lock (INV-L7), y el apagado no pierde nada confirmado (INV-L5).
- Un `fsync` que falla en el pool detiene la sala como hoy (INV-L3), y un request cancelado en cada `.await` no deja estado a medias (INV-L2).
- Los tests de durabilidad y las imágenes de crash pasan sin cambios en sus expectativas.

**Interacciones a revisar:** la rotación y el sellado de segmentos; la escritura del roster en el tick; la apertura de una sala (los syncs de D77); la compresión y la poda; el relay, que tiene su propia E/S (`relay/files.rs`); los tests que corren en el runtime de un solo hilo (donde hoy `blocking_io` corre en el lugar); el apagado ordenado y su gracia de 10 s.

**Medición:** E2-tmpfs y E2-disco con 500 y 1.000 salas, con el Lote 2 ya aplicado. **Criterio:** los hilos del servidor fijos en todas las corridas; en tmpfs con 1.000 salas, la propagación en p99 por debajo de 10 ms (el diagnóstico de los dos arreglos dio 5,8 ms); en disco con 1.000 salas, sin colapso (el diagnóstico con la E/S en el worker dio 3,9 s en p99, §4.7.1 de las evaluaciones). El valor de fábrica de `ZEMDB_IO_THREADS` es el más chico que cumple el criterio en disco.

**Metodología:** §5.1, con los checklists de crash-safety, de concurrencia y de medición; las versiones largas del servidor (imágenes de crash) dos veces, antes y después del lote. Por el riesgo, la revisión de delta la hacen dos revisores independientes: uno con el foco en la durabilidad y otro en la concurrencia.

**Documentos:** `ARCHITECTURE.md` §5.2 y la configuración (§5.3 o §7.3); `INVARIANTES.md`: INV-D1, D7, L7, B3, B4; `README.md` (la variable nueva).

**Entrega:** `perf(server): run the file I/O of the room actors on a fixed pool of dedicated threads (Fase 3.12, Lote 3)`.

**Hecho.** *Pendiente.*

### Lote 4 · *Group commit* · ⬜
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

**Documentos:** `ARCHITECTURE.md` §5.2 (el append, el sellado al final del grupo); `INVARIANTES.md`: INV-D1, D7, B4.

**Entrega:** `perf(server): group commit, one fsync for the commits waiting in a room (Fase 3.12, Lote 4)`.

**Hecho.** *Pendiente.*

### Lote 5 · El SSE: apagarlo y el evento `ops` · ⬜
**Objetivo:** que el operador pueda apagar el SSE (DS4) y que el SSE traiga las operaciones (DS5), para que la sub-fase 4.3 se construya sobre él.

**Archivos:** `crates/core/src/protocol/messages.rs` (el mensaje nuevo del evento y el código de error nuevo, al final de sus enums) y los fuzzers y tests de codificación; `crates/server/src/actor/command.rs` (`RoomEvent`), `actor/room.rs` (el evento después del `fsync`), `api/sse.rs`, `config.rs` (`ZEMDB_MAX_SSE_STREAMS=0`), y sus tests. `ARCHITECTURE.md` §7 (SSE) y la configuración.

**Qué se hace:** con el SSE apagado, `GET /events` responde el código nuevo; cuando un commit o un grupo se vuelve durable, el actor codifica una vez el mensaje con sus operaciones, si entran en el tope, y lo emite como evento `ops` (base64), y si no, `head_advanced`.

**Tests exigidos.**
- Con `ZEMDB_MAX_SSE_STREAMS=0`, `GET /events` da el código nuevo y el resto del protocolo funciona igual.
- Un cliente que lee el stream recibe, en orden y sin huecos, las mismas operaciones que le daría un `/sync` (INV-Y2), con su `schema_id` y `schema_version` (INV-E3), y solo después de que son durables (INV-D7).
- Una operación más grande que el tope llega como `head_advanced`; un stream atrasado recibe `lagged`.
- Un miembro de otra sala, o sin token, no recibe nada (INV-A1).
- La ida y vuelta del mensaje nuevo y del código nuevo, y su lugar en los fuzzers de mensajes (INV-W1, W2).
- La memoria del canal de eventos de una sala con un stream que no lee queda dentro de lo que fije el Lote 0.

**Interacciones a revisar:** el tope de streams por cliente y en total; el keep-alive; el cambio de esquema en una sala abierta (`schema_reloaded` y el esquema que anuncia el evento); el apagado ordenado, que corta los streams; el *group commit* del Lote 4.

**Medición:** E2-modos, con el modo c ahora sin parche de diagnóstico, contra el modo a. **Criterio:** con 1.000 salas, menos de 0,5 ms de CPU del servidor por commit y la propagación por debajo de 2 ms en p99 (el prototipo dio 0,46 ms y 1,8 ms, §4.8.1 de las evaluaciones). El generador del arnés pasa a leer el evento nuevo en el mismo lote.

**Metodología:** §5.1, con los checklists del protocolo y de medición; las versiones largas de `zemdb-core` (los fuzzers) y del servidor.

**Documentos:** `ARCHITECTURE.md` §7 (los eventos SSE y su formato, el código nuevo) y la configuración; `INVARIANTES.md`: INV-Y2, E3, B3; `README.md`. Y el aviso a la Fase 4: el Lote 0 de 4.3 parte de este formato.

**Entrega:** `feat(server): the SSE can be turned off, and its events carry the operations (Fase 3.12, Lote 5)`.

**Hecho.** *Pendiente.*

### Lote 6 · El catch-up sin copias · ⬜
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
| La memoria del canal de eventos con el evento `ops` | El tope de DS5 y la decisión del Lote 0 sobre INV-B3; un test con un stream que no lee |
| El evento `ops` y la sub-fase 4.3 se desalinean | El Lote 5 va antes de 4.3, y el formato del evento se escribe en `ARCHITECTURE.md` en el mismo lote |
| La máquina de las mediciones cambia entre lotes | La línea de base se mide en la misma sesión que el resultado (§5.1, paso 0) |
| Un disco de nube cambia las conclusiones | El Lote 7 mide ahí antes de decidir DS7, y la medición final lo menciona si no se pudo hacer |

---

## 8. Seguimiento

| Lote | Qué | Estado |
|---|---|---|
| 0 | Contrato | ⬜ |
| 1 | El arnés en `main` y la línea de base | ⬜ |
| 2 | El presupuesto de respuestas | ⬜ |
| 3 | La E/S fuera de los workers | ⬜ |
| 4 | *Group commit* | ⬜ |
| 5 | El SSE: apagarlo y el evento `ops` | ⬜ |
| 6 | El catch-up sin copias | ⬜ |
| 7 | El roster | ⬜ |
| 8 | Medición final y revisión de cierre | ⬜ |

**Avance:** plan escrito el 1 de Noviembre de 2026, a partir de las Evaluaciones 1 y 2. El próximo paso es el Lote 0.
