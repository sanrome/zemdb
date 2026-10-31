# Evaluaciones de performance — Plan

**Fecha:** 30 de Octubre de 2026
**Tipo:** Plan de evaluaciones, fuera de las fases. Documento vivo: cada evaluación agrega su sección, sus resultados y su estado en el seguimiento (§5).
**Estado:** Propuesta. La Evaluación 1 (el commit con los clientes al día) está medida, con sus resultados en §3.7 y las conclusiones de su revisión en §3.8. La Evaluación 2 (muchas salas, con escrituras a ritmo humano) está medida, con sus resultados en §4.7. Las siguientes, a definir (§6).
**Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md); cada evaluación es un spike (§3 de la metodología).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md).

---

## 0. Resumen

- **Qué.** Medir cuán rápido y cuán liviano es `zemdb-server`, de a un flujo por vez y empezando por el más simple, para encontrar sus cuellos de botella con números y no con supuestos. Hoy el repositorio no tiene ningún benchmark.
- **Por qué ahora.** El servidor está cerrado desde la Fase 3.11 y la Fase 4 trabaja sobre el cliente: medir el servidor no se cruza con ella.
- **Qué no.** Una evaluación no optimiza nada (DP2). Tampoco reemplaza las pruebas de estrés de la Fase 5, que buscan que el sistema no se rompa bajo carga; acá se busca cuánto rinde y dónde se va el tiempo.
- **Evaluaciones:** la 1 (§3), el commit con los clientes al día en una sala, y la 2 (§4), muchas salas con escrituras a ritmo humano, las dos medidas.

---

## 1. Decisiones

| ID | Decisión | Estado |
|---|---|---|
| **DP1** | El arnés de medición es código de spike y no se mergea a `main` | Propuesta |
| **DP2** | Una evaluación mide y diagnostica; cada optimización es una propuesta aparte | Propuesta |
| **DP3** | Las condiciones de medición comunes a todas las evaluaciones | Propuesta |

### DP1 · El arnés no se mergea
- El generador de carga y lo que haga falta para medir viven en una rama propia por evaluación (`perf/e1-commit` para la primera), sin tocar el código de producción de `main`. Si una medición necesita algo dentro del servidor (por ejemplo, el perfilador de DP3), se agrega en esa rama.
- Lo que queda en `main` son los resultados, anotados en la sección de la evaluación, con el entorno en que se midieron y el commit de `main` medido.
- Si el arnés resulta útil para comparar en cada cambio, convertirlo en un benchmark permanente es uno de los próximos pasos (§6), con su propia decisión.

### DP2 · Medir no es optimizar
- Una evaluación cierra con sus preguntas respondidas y una lista de cuellos de botella ordenada por su costo medido, no con cambios en el servidor.
- Cada optimización que salga de ahí es una propuesta aparte, que sigue la metodología como cualquier otra: muchas tocan garantías (agrupar los `fsync` de varios commits, por ejemplo, toca la durabilidad de cada `CommitAck`), y esas arrancan con su Lote 0.
- Una optimización se acepta con la misma medición que la motivó, corrida antes y después en la misma máquina.

### DP3 · Condiciones de medición
- **Compilación:** `--release`, con el perfil del workspace, y logs en `warn` (`RUST_LOG=warn`).
- **El servidor es el binario real** (`zemdb-server`) configurado por variables `ZEMDB_*`, salvo en las mediciones que dicen lo contrario (como la 1d).
- **Generador y servidor en la misma máquina, en núcleos separados** (`taskset`), para que no compitan por CPU. Si el generador pasa del 90% de sus núcleos, la corrida no vale: se le dan más núcleos o se aliviana el generador.
- **Carga en lazo cerrado:** cada uno de los *k* trabajadores del generador manda su próximo pedido cuando recibe la respuesta del anterior, así que siempre hay *k* pedidos en vuelo. Sirve para encontrar el techo, no para medir latencias bajo una tasa de llegada fija (eso puede ser un próximo paso).
- **Corridas:** 5 s de calentamiento, 30 s medidos, 3 repeticiones; se reporta la mediana y, si las repeticiones difieren en más de un 10%, se anota.
- **Una corrida con errores no vale.** Toda respuesta tiene que ser un `CommitAck`, y al terminar, el head de la sala tiene que ser igual a la cantidad de commits confirmados.
- **Métricas de cada corrida:** commits/s; latencia p50, p99, p99.9 y máxima, medida en el generador, que guarda la latencia de cada commit y ordena todas al terminar (percentiles exactos, sin histograma); CPU del proceso del servidor (de `/proc/<pid>/stat`); RSS del servidor al terminar y su máximo (`VmHWM`); cambios de contexto del servidor por commit (de `/proc/<pid>/task/*/status`); y los commits que tardaron 10 ms o más, o fallaron, con el momento en que empezaron.
- **Perfil de CPU:** con `perf` si la máquina lo permite; si no (en un contenedor sin acceso a los eventos del kernel), con un perfilador de muestreo dentro del proceso (por ejemplo, el crate `pprof`), agregado en la rama del spike. Se guarda junto a los resultados. En la Evaluación 1 alcanzó con `perf` (el de `linux-tools` de Ubuntu, con el evento de software `task-clock`, a 499 Hz y pilas por DWARF, sobre un binario de release con información de depuración, que no cambia el código generado).
- **El entorno se anota con cada resultado:** CPU y núcleos, RAM, sistema de archivos y tipo de disco del directorio de datos, versión de Rust, commit de `main`, y la asignación de núcleos.
- **Los números absolutos valen para esa máquina.** Sirven para comparar entre corridas en la misma máquina. La latencia del `fsync` en particular depende del disco: la de producción se mide en una máquina como la de producción (§6).

---

## 2. Cómo se define una evaluación

Cada evaluación tiene, antes de medir nada:
- **las preguntas** que responde, escritas de antemano;
- **el flujo** que ejercita, nombrado en el código;
- **el escenario:** esquema, operaciones, clientes, salas y configuración;
- **las mediciones,** cada una con lo que cambia respecto de las otras;
- **las hipótesis** que se esperan confirmar o descartar, para que el resultado se lea contra algo;
- **cómo cierra.**

Al cerrar, agrega sus resultados y sus conclusiones en su sección, y su estado en el seguimiento (§5).

---

## 3. Evaluación 1: el commit con los clientes al día

### 3.1 Preguntas
1. ¿Cuánto tarda un commit en el mejor caso: un cliente al día, un pedido en vuelo, sin el costo del disco?
2. ¿Cuántos commits por segundo acepta una sala cuando el disco no limita, y en qué se va el CPU en ese techo?
3. ¿Cuánto de ese techo se lleva el `fsync` en un disco real?
4. ¿Cuánto cuesta la capa HTTP en cada commit?

### 3.2 El flujo
Un `POST /rooms/:room_id/commit` con un `ClientMessage::Commit` de una sola operación:
1. el borde HTTP (axum y hyper), la autenticación del token y `decode_client_message`;
2. el pedido entra al buzón del actor de la sala;
3. `RoomActor::commit` (`crates/server/src/actor/room.rs`): el registro del cliente, el cursor, la deduplicación, `validate_operation` y `check_owner`, el tamaño;
4. `TieredLog::append`: `encode_wal_record`, la escritura en `active.wal` y su `sync_data` (`WarmDiskLog::append_record`, por `blocking_io`), y el búfer en RAM;
5. el registro en la caché de deduplicación, el nuevo head, el evento SSE `HeadAdvanced`;
6. el catch-up y la codificación del `CommitAck`.

**El cliente al día.** Cuando el cursor del commit es el head anterior (`last_ack_seq + 1 == new_seq`), el catch-up es la operación recién confirmada, sin leer el log: es el camino más corto, y el que mide la 1a. Con *k* pedidos en vuelo, el cursor de cada commit puede estar hasta *k* − 1 operaciones atrás y el catch-up se lee del búfer en RAM (`catchup_after_commit`). Es lo que pasa en una sala con varios clientes al día escribiendo a la vez, así que se mide tal cual y se anota.

### 3.3 Escenario
- **Una sala, un cliente registrado,** sin streams SSE abiertos.
- **Esquema:** una tabla de cinco columnas: clave `Int`, un texto de unos 100 bytes, un `Int`, un `Bool` y un `Int` de timestamp. Sin restricciones ni dueño de fila.
- **Operaciones:** solo `Insert`, con claves crecientes sin repetir, para que ninguna se rechace. Todas con el mismo tamaño en el cable. Los frames se codifican antes de la corrida, salvo el `MutationId` y el cursor, para que el generador gaste lo menos posible.
- **Cursor:** cada trabajador manda como `last_ack_seq` la secuencia más alta que el cliente vio confirmada hasta ese momento.
- **Transporte:** HTTP/1.1 con una conexión keep-alive por trabajador, que es como un proxy como Caddy le habla al servidor por defecto (`README.md`, *Deploying the Server*). HTTP/2 queda para después (§6).
- **Configuración:** la de fábrica, salvo los secretos, `ZEMDB_DATA_DIR` y `ZEMDB_PORT`. La rotación de segmentos y el mantenimiento corren como en producción; si una corrida rota segmentos, se anota.

### 3.4 Mediciones

| | Directorio de datos | Pedidos en vuelo (*k*) | Responde |
|---|---|---|---|
| **1a** | tmpfs (`/dev/shm`) | 1 | Pregunta 1: la latencia mínima de un commit |
| **1b** | tmpfs | 1, 2, 4, 8, 16, 32, 64 | Pregunta 2: la curva de commits/s según *k*; donde deja de subir está el techo de CPU de la sala, y ahí se toma el perfil de CPU |
| **1c** | disco real | 1, 2, 4, 8, 16, 32, 64 | Pregunta 3: la diferencia con la 1b es lo que cuesta el `fsync` |
| **1d** | tmpfs, sin HTTP | 1, 2, 4, 8, 16, 32, 64 | Pregunta 4: los mismos commits, mandados directo al actor por `RoomManager`, en el mismo proceso; la diferencia con la 1b es lo que cuesta la capa HTTP |

En tmpfs el `fsync` casi no cuesta, así que la 1b muestra el costo del código. La 1d usa la API pública del crate, como los tests de integración del servidor (`crates/server/tests/common`).

### 3.5 Hipótesis
- **H1.** En disco real, el `fsync` de cada commit domina: el techo de una sala es del orden de 1 / (latencia del `fsync`), y subir *k* casi no lo mueve, porque el actor procesa los commits de a uno y cada uno espera su `fsync`.
- **H2.** En tmpfs, con una operación por pedido, la capa HTTP (y la decodificación y codificación de cada frame) se lleva una parte importante del tiempo de cada commit.
- **H3.** `block_in_place`, en cada `fsync`, tiene un costo visible cuando *k* es alto.

### 3.6 Cómo cierra
- Las cuatro preguntas respondidas con sus números y su entorno (DP3), en §3.7.
- Los cuellos de botella del flujo, ordenados por su costo medido, con el perfil de CPU que los muestra.
- Cada hipótesis, confirmada o descartada.
- Una recomendación de qué optimizar primero (cada una, una propuesta aparte, DP2) y de qué evaluar después (§6).

### 3.7 Resultados

Medidos el 30 y el 31 de Octubre de 2026. El código del spike está en la rama `perf/e1-commit`: el generador (`crates/perf`), `run_e1.sh` (una corrida por línea JSON), `summarize.sh` (la mediana de las repeticiones) y `profile_e1.sh` (el perfil de CPU).

#### Entorno
- **Máquina:** una VM (Firecracker) de 4 vCPU Intel Xeon a 2,1 GHz y 15 GiB de RAM, en la nube, con *steal time* (el host le quita CPU a ratos). Ubuntu 24.04, kernel 6.18 del huésped.
- **Disco real:** `/dev/vda` (virtio), ext4 **sin journal**. Una escritura de 300 bytes con `O_DSYNC` tarda unos 149 µs; en tmpfs, 2 µs. Sin journal, el `fsync` es más barato que en un ext4 común: en producción, el disco puede ser más lento o (con protección ante cortes de energía) mucho más rápido.
- **Compilación:** Rust 1.97.0, perfil `release` del workspace (sin LTO), con información de depuración.
- **Código medido:** `main` en `9de2c1b`, más el generador. El interruptor del experimento de §3.7.4 solo actúa con su variable de entorno.
- **Núcleos:** el servidor en 0 y 1 (Tokio arranca 2 workers), el generador en 2 y 3. En el modo sin HTTP, todo el proceso en 0 y 1.

#### 3.7.1 Las mediciones (30 s, 3 repeticiones, mediana)

| | *k* | commits/s | p50 | p99 | p99.9 | CPU del servidor |
|---|---|---|---|---|---|---|
| **1a/1b** HTTP, tmpfs | 1 | 9.362 | 99 µs | 210 µs | 637 µs | 98% |
| | 2 | 16.829 | 108 µs | 260 µs | 798 µs | 130% |
| | 4 | 21.696 | 159 µs | 707 µs | 1,9 ms | 141% |
| | 8 | 26.046 | 278 µs | 828 µs | 2,2 ms | 147% |
| | 16 | 33.139 | 438 µs | 1,2 ms | 2,5 ms | 154% |
| | 32 | 38.213 | 774 µs | 1,9 ms | 3,3 ms | 161% |
| | 64 | 43.362 | 1,4 ms | 3,2 ms | 4,6 ms | 167% |
| **1c** HTTP, disco | 1 | 3.133 | 286 µs | 696 µs | 2,2 ms | 50% |
| | 2 | 5.469 | 330 µs | 880 µs | 2,7 ms | 66% |
| | 4 | 5.262 | 706 µs | 1,6 ms | 3,6 ms | 67% |
| | 8 | 5.179 | 1,5 ms | 3,5 ms | 5,6 ms | 70% |
| | 16 | 5.216 | 2,9 ms | 5,5 ms | 9,7 ms | 73% |
| | 32 | 4.901 | 6,2 ms | 10,6 ms | 18,7 ms | 76% |
| | 64 | 4.518 | 13,8 ms | 21,2 ms | 35,9 ms | 80% |
| **1d** sin HTTP, tmpfs | 1 | 49.228 | 18 µs | 54 µs | 147 µs | 121%¹ |
| | 2 | 51.474 | 24 µs | 194 µs | 930 µs | 130%¹ |
| | 4 | 67.379 | 46 µs | 252 µs | 1,4 ms | 150%¹ |
| | 8 | 68.776 | 105 µs | 317 µs | 1,7 ms | 158%¹ |
| | 16 | 59.194 | 280 µs | 548 µs | 2,0 ms | 158%¹ |
| | 32 | 44.718 | 816 µs | 1,4 ms | 3,0 ms | 152%¹ |
| | 64 | 34.364 | 2,0 ms | 4,2 ms | 6,0 ms | 146%¹ |

¹ El proceso entero, con el generador adentro. El CPU es el porcentaje de un núcleo: el servidor tiene dos (200%).

- Las repeticiones difieren en menos del 10% en todos los puntos menos uno (HTTP, tmpfs, *k* = 8: 16%, por la corrida inválida de §3.7.5). El generador nunca pasó del 90% de sus núcleos.
- **Memoria:** el RSS máximo del servidor fue de 9 a 10 MiB con *k* = 1 y creció con *k*, hasta 48 MiB en tmpfs con *k* = 64 (§3.7.4 explica la mayor parte).
- **Latencias máximas:** la de cada corrida fue de 8 a 656 ms en tmpfs, de 30 a 86 ms sin HTTP y de 57 a 481 ms en disco. Son pocos commits (el p99.9 no pasa de 36 ms en ningún punto) y no se investigaron más (§6).

#### 3.7.2 Las preguntas
1. **Un commit en el mejor caso tarda 99 µs por HTTP (p50) y 18 µs directo al actor.** El servidor gasta unos 105 µs de CPU por commit por HTTP (98% de un núcleo para 9.362 commits/s), y el 59% de ese CPU es del kernel: el envío y la recepción por el socket, `epoll`, los `eventfd` con que Tokio despierta su driver de E/S, y dormir y despertar hilos (`futex`). Son 5 cambios de contexto por commit.
2. **Sin disco, una sala acepta unos 43.000 commits/s por HTTP con *k* = 64, y la curva todavía sube** (el servidor llega a 167% de sus 200%). Directo al actor, el techo es 68.800 con *k* = 8, y después **baja**: a 34.000 con *k* = 64 (§3.7.3). En el techo por HTTP, el CPU se reparte así (perfil con *k* = 64): 30% en el kernel (12% en `writev` de las respuestas), 17% en `malloc` y `free`, 17% en `RoomActor::commit` (4,6% en la escritura del WAL y 4,4% en el catch-up), 4% en codificar la respuesta y 2,7% en verificar el token.
3. **En disco, el `fsync` fija el techo: unos 5.400 commits/s desde *k* = 2, y no sube con más pedidos en vuelo** (con *k* = 64 baja a 4.500). Cada commit cuesta unos 180 µs de actor, y unos 150 µs son el `fsync` de este disco.
4. **La capa HTTP, con la red local, se lleva unos 80 de los 99 µs de un commit con *k* = 1**, y en el techo el rendimiento pasa de 68.800 (sin HTTP) a 43.000.

#### 3.7.3 El catch-up copia las operaciones
Con *k* pedidos en vuelo, el cursor de cada commit está hasta *k* − 1 operaciones atrás, así que cada `CommitAck` lleva hasta *k* operaciones. El búfer en RAM las devuelve copiándolas enteras (`HotBuffer`, `entry.op.clone()`): cada `Vec<Value>` de cada fila. En el perfil directo al actor con *k* = 64, `catchup_after_commit` es el 31% del CPU, y la copia de los valores el 22%. Es el costo que crece con *k* y hace bajar la curva de 1d. Por HTTP pesa menos (4,4%), porque el resto cuesta más, pero esas operaciones además se codifican y se mandan: con *k* = 64, unos 15 KB por respuesta. Mandarlas es parte del protocolo de 1 RTT; copiarlas, no.

#### 3.7.4 `block_in_place` (experimento de diagnóstico)
`blocking_io` corre la E/S de cada commit con `tokio::task::block_in_place`, que le pasa el worker a otro hilo mientras dura la E/S. Para medir cuánto cuesta, la rama del spike tiene un interruptor (`ZEMDB_PERF_INLINE_IO`) que corre la E/S en el mismo hilo, sin `block_in_place`. **No es una propuesta:** sin `block_in_place`, un `fsync` lento frena a todas las salas que comparten ese worker, que es lo que `block_in_place` evita. Mismo binario, 15 s, 3 repeticiones:

| | *k* | con `block_in_place` | sin | cambio | cambios de contexto por commit |
|---|---|---|---|---|---|
| HTTP, tmpfs | 1 | 9.465 | 11.206 | +18% | 5,0 → 3,0 |
| | 8 | 28.508 | 40.186 | +41% | 1,2 → 0,3 |
| | 64 | 45.421 | 60.784 | +34% | 0,3 → 0,1 |
| HTTP, disco | 1 | 3.345 | 3.129 | −6% | 9,1 → 6,1 |
| | 8 | 5.494 | 5.674 | +3% | 7,3 → 5,0 |
| | 64 | 4.876 | 4.959 | +2% | 7,3 → 5,0 |
| sin HTTP, tmpfs | 1 | 50.049 | 80.283 | +60% | 1,2 → 0,5 |
| | 8 | 68.565 | 80.181 | +17% | 1,1 → 0,5 |
| | 64 | 34.083 | 44.960 | +32% | 1,5 → 0,7 |

- Sin disco, `block_in_place` se lleva entre un 15% y un 40% del rendimiento, y la mitad de la latencia en p99 (HTTP, *k* = 8: de 780 a 387 µs). En disco no cambia nada: el `fsync` domina.
- **Memoria:** con *k* = 64 en tmpfs, el servidor tiene **82 hilos y 44 MiB de RSS con `block_in_place`, y 4 hilos y 12 MiB sin él**. Cada traspaso del worker puede crear un hilo en el pool de bloqueo, que queda vivo un rato antes de terminar, y glibc le da a cada hilo su propia arena de `malloc`.

#### 3.7.5 Una pausa de más de 5 s
En una de las 63 corridas (HTTP, tmpfs, *k* = 8, primera repetición), los 8 commits en vuelo recibieron a la vez `504 Timeout` ("actor did not answer in time", `ACTOR_TIMEOUT` de 5 s). Los 8 quedaron confirmados (el head terminó 8 por encima de lo confirmado al cliente), que es el comportamiento previsto: el cliente reintenta con el mismo `MutationId` y la deduplicación le devuelve la secuencia original. La corrida perdió unos 5 s de rendimiento (23.000 commits/s contra 27.000 de las otras dos). No se repitió en tres corridas más de 40 s con el registro de pausas, y el kernel no registró nada en ese intervalo. **Causa no determinada:** una pausa de la VM (que tiene *steal time*) o una del actor. El generador ya registra el momento de cada pausa para la próxima vez.

#### 3.7.6 Los segmentos se sellan cada 1.000 operaciones
`active.wal` se sella al llegar a `ram_max_ops` (1.000) o a `segment_max_bytes` (32 MiB), lo primero que pase (`ARCHITECTURE.md` §5.2). Con operaciones de unos 246 bytes, el límite es siempre el de las operaciones: un segmento de 246 KB más su `.ids` cada 1.000 commits. A 27.000 commits/s son unos 54 archivos por segundo en `segments/`, que con el TTL de 24 h del disco tibio serían del orden de millones de archivos por día con carga sostenida. Cada sellado agrega unos cuatro `fsync` (el segmento, el directorio, y el `.ids` con su directorio), poco frente a los 1.000 de sus commits, así que no limita el rendimiento de esta evaluación; lo que no se midió es cómo se comporta una sala con muchos segmentos (§6).

#### 3.7.7 Hipótesis
- **H1, confirmada.** En disco, el techo de una sala es unos 5.400 commits/s, del orden de 1 / (latencia del `fsync`), y no sube con *k*.
- **H2, confirmada.** Con *k* = 1, HTTP y la red local se llevan unos 80 de los 99 µs de un commit, y más de la mitad del CPU del servidor está en el kernel.
- **H3, confirmada, y más de lo esperado.** No solo cuando *k* es alto: `block_in_place` cuesta entre un 15% y un 60% del rendimiento sin disco, con cualquier *k*, y multiplica los hilos y la memoria.

#### 3.7.8 Cuellos de botella, de mayor a menor
1. **Un `fsync` por commit.** En disco limita una sala a unos 5.400 commits/s en esta máquina, sin importar la concurrencia. Dirección: compartir un `fsync` entre los commits que esperan (*group commit*), sin confirmar nada antes de que esté en disco; toca la durabilidad de cada `CommitAck`, así que es una propuesta con Lote 0 (DP2).
2. **El traspaso del worker en cada `fsync` (`block_in_place`).** Entre un 15% y un 60% del rendimiento sin disco, de 2 a 3 cambios de contexto por commit, y 4 veces la memoria con mucha concurrencia. Dirección: hacer la E/S del log fuera de los workers de Tokio de otra forma (por ejemplo, un hilo de E/S por sala, o uno por grupo de commits si se hace el punto 1), medida también con muchas salas a la vez.
3. **La copia de las operaciones en el catch-up.** Crece con *k* y es el 31% del CPU del actor con *k* = 64. Dirección: que el búfer en RAM comparta las operaciones (por ejemplo, con `Arc`) en lugar de copiarlas.
4. **El costo de un pedido HTTP por operación.** Unos 80 µs por commit con *k* = 1, la mayor parte en el kernel. No tiene una dirección clara todavía: depende de cómo lleguen los pedidos detrás del proxy, y de si un commit pudiera llevar varias operaciones (un cambio del protocolo). Para evaluar aparte (§6).
5. **`malloc` y `free`:** entre el 7% (*k* = 1) y el 17% (*k* = 64) del CPU del servidor. Probar otro asignador es barato y podría ser un próximo paso.

#### 3.7.9 Recomendación
- **Primero, medir el caso de uso típico (la Evaluación 2, §4).** Esta evaluación cargó una sala al máximo para encontrar los costos del código. El caso típico es otro (muchas salas, con muchos usuarios que escriben poco), y es el que decide qué optimizar.
- **Después, el punto 1 junto con el 2,** con el diseño de §3.8: un *group commit* sin espera y el `fsync` en un hilo dedicado, que hacen un solo `fsync` y un solo pasaje entre hilos por grupo. La Evaluación 2 dice cuánto importan en el caso típico y cómo repartir los hilos de E/S entre salas.
- **El punto 3 es un cambio chico y local**, sin garantías en juego, que se puede proponer por separado.
- **Para producción, el disco importa tanto como el código:** con un disco con protección ante cortes de energía, el `fsync` puede bajar de cientos de microsegundos a decenas, y el techo del punto 1 se mueve en la misma proporción.

### 3.8 Conclusiones de la revisión
Lo que se concluyó con el dueño del proyecto al revisar los resultados, el 31 de Octubre de 2026. No son decisiones adoptadas: orientan las propuestas que salgan de esta evaluación (DP2).
- **HTTP.** Ninguna solución elimina el costo fijo de cada pedido. Se achica abaratando el pedido (sacar `block_in_place` también reduce los hilos que despierta cada uno: +18% con *k* = 1 en §3.7.4) o mandando menos pedidos (varias operaciones por commit, como el outbox diferido, o una conexión persistente en lugar de un POST por commit). Para el caso de uso no es el límite: un servidor de dos núcleos acepta decenas de miles de pedidos por segundo. No se prioriza.
- **El reemplazo de `block_in_place`: ningún worker de Tokio se bloquea.** La escritura en el archivo (al page cache, unos µs) sigue en el actor, y el `fsync` lo hace un hilo dedicado (uno por sala, o unos pocos compartidos por el servidor), al que el actor le pide "sincronizá hasta acá" por un canal y cuya respuesta espera de forma asíncrona. No hay traspaso del worker, y la cantidad de hilos queda fija.
- **El *group commit* sin espera, como comportamiento de base.** El grupo son los commits que llegan mientras corre el `fsync` anterior: con poca carga es de uno, como hoy, y con mucha crece solo. Se acota por tamaño (operaciones o bytes por grupo), no por tiempo. Nada se confirma antes de su `fsync`, y una operación sin `fsync` no aparece en el catch-up de nadie ni se avisa por SSE. Esperar a propósito para juntar más (por ejemplo, 20 ms) no conviene en el caso típico: el grupo es de una sala, y una sala con 10 commits por segundo recibe en promedio 0,2 commits en 20 ms, así que se pagaría la espera sin agrupar nada. Si alguna vez existe esa espera, arranca en 0.
- **Confirmar antes del `fsync` (cada 20 ms, o cada segundo) no es seguro tal como está el sistema.** Un commit confirmado que se pierde en un corte deja libre su secuencia para otra operación, y las réplicas que lo aplicaron divergen. Achicar la ventana no cambia eso. Hoy la divergencia solo se detecta si el cliente se presenta con su cursor antes de que el head del servidor lo vuelva a alcanzar (el riesgo aceptado de INV-K1, por D78). Un modo de durabilidad relajada necesitaría que el servidor cambie la encarnación de la sala al volver de un apagado sucio, para que todas las réplicas lo detecten sin carrera y se descarten (INV-K3). Queda en el backlog (`ROADMAP.md` §3), junto con la replicación entre servidores.
- **En el caso típico, el `fsync` de cada sala no limita.** Cuesta 0,15 ms en este disco (quizás 1 o 2 ms en un disco de nube), frente a 40 ms o más de red, y una sala con 10 commits por segundo usa menos del 1% de su techo. Lo que puede limitar es el total: muchas salas, cada una con su archivo, suman `fsync` por segundo sobre el mismo disco. Es una de las preguntas de la Evaluación 2.

---

## 4. Evaluación 2: muchas salas, con escrituras a ritmo humano

El caso de uso típico: muchas salas, cada una con varios clientes que escriben de a poco, sin encadenar escrituras, y que se enteran de lo que escriben los demás. Responde cuán liviano es el servidor: cuántas salas caben en una máquina y qué se agota primero.

### 4.1 Preguntas
1. ¿Cuánta memoria cuesta una sala abierta, y un cliente con su stream SSE?
2. ¿Cuántas salas, con clientes que escriben a ritmo humano, caben en esta máquina antes de que la latencia se degrade, y qué se agota primero: CPU, disco o memoria?
3. ¿Cuántos `fsync` por segundo sostiene el disco cuando vienen de muchas salas (archivos distintos), y el sistema de archivos los agrupa?
4. En este escenario, ¿importan `block_in_place` y el `fsync` de cada commit?

### 4.2 El flujo
- **El commit** de §3.2, repartido entre muchos actores.
- **El aviso a los demás:** el commit manda `HeadAdvanced` por el stream SSE (`GET /rooms/:room_id/events`) de cada cliente de la sala, y cada uno que no está al día hace un `/sync` desde su cursor, como lo hará el `SyncWorker` de la sub-fase 4.3 (que todavía no existe: el generador lo imita). Si ya tiene un `/sync` en vuelo, hace uno más cuando termina, no uno por aviso.
- **Sin heartbeats ni snapshots:** el intervalo del heartbeat lo fija el `SyncWorker`, y los snapshots no se piden en una corrida de minutos.

### 4.3 Escenario
- ***R* salas con *C* clientes cada una;** de base, *C* = 10 (también 50, el máximo del caso de uso). Cada cliente se registra, abre su stream SSE y escribe en promedio una vez cada *T* segundos, a intervalos aleatorios (exponenciales), sin esperar a que su escritura anterior termine: carga en **lazo abierto**, en lugar del lazo cerrado de DP3. De base, *T* = 10 s, así que el total es *R* × *C* / *T* commits por segundo (1.000 salas: 1.000 commits/s).
- **Esquema y operaciones:** los de §3.3.
- **Conexiones, como las vería el servidor detrás de un proxy HTTP/1.1:** una por stream SSE, y los commits, los `/sync` y los `Ack` de todos los clientes por un conjunto compartido de 512 conexiones keep-alive. El plan decía 64, pero con 64 los pedidos hacían cola en el generador a partir de 1.000 salas, y un proxy como Caddy no limita así sus conexiones al servidor (§4.7).
- **Configuración:** la de fábrica, salvo los límites del borde, que de fábrica cortan en 10.000 conexiones (`ZEMDB_MAX_CONNECTIONS`) y 5.000 streams SSE (la mitad): se suben para la medición, y se anota. El límite de archivos abiertos del sistema también se anota.
- **La latencia se mide desde el momento programado de cada pedido**, no desde que salió, para que un generador atrasado no esconda la demora del servidor. Se miden dos latencias: la del commit (hasta el `CommitAck`) y la de **propagación** (desde el `CommitAck` hasta que cada uno de los otros clientes de la sala aplicó esa secuencia con su `/sync`), que es la que percibe un usuario mirando lo que escribe otro.
- **Pasos:** cada valor de *R* se mide 60 s después de 15 s de calentamiento, con 3 repeticiones.

### 4.4 Mediciones

| | Disco | Qué cambia | Responde |
|---|---|---|---|
| **2a** | tmpfs | *R* = 1, 100, 1.000 y 5.000 salas abiertas, con un cliente registrado cada una, sin escrituras ni SSE | Pregunta 1: RSS e hilos por sala en reposo |
| **2b** | tmpfs | 1.000 y 10.000 streams SSE abiertos en 100 salas, sin escrituras | Pregunta 1: RSS por stream |
| **2c** | tmpfs | *R* = 100, 500, 1.000, 2.000, 5.000, … con *C* = 10 y *T* = 10 s, hasta que el p99 del commit pase de 50 ms, haya errores o se agote algo | Pregunta 2 sin el costo del disco: latencias, CPU, RSS e hilos por paso; perfil de CPU en el último paso que sostiene |
| **2d** | real | Lo mismo que 2c, y un paso con *C* = 50 | Preguntas 2 y 3: además, los `fsync` por segundo del servidor, y las escrituras y el tiempo ocupado del disco (`/proc/diskstats`) |
| **2e** | real | 2d con el interruptor sin `block_in_place` de §3.7.4, en el último paso que 2d sostuvo y en el siguiente | Pregunta 4 |

### 4.5 Hipótesis
- **H4.** Una sala abierta en reposo cuesta poco (decenas de KB). Lo que crece con la actividad es su búfer en RAM, hasta `ram_max_ops` operaciones (unos cientos de KB con estas operaciones).
- **H5.** El costo de cada commit lo dominan los avisos y los *C* − 1 `/sync` que dispara, no el commit.
- **H6.** En disco, lo primero que se agota son los `fsync` por segundo del total, antes que el CPU.
- **H7.** Con muchas salas haciendo `fsync` a la vez, `block_in_place` multiplica los hilos (hasta el límite del pool de bloqueo de Tokio, 512) y la memoria.

### 4.6 Cómo cierra
- Las cuatro preguntas respondidas con sus números y su entorno (DP3), en §4.7: en particular, **cuántas salas caben en esta máquina con *C* = 10 y *T* = 10 s, y qué se agota primero**.
- Cada hipótesis, confirmada o descartada.
- Una recomendación sobre las optimizaciones de §3.7.9 a la luz del caso típico: cuánto importan el *group commit*, el reemplazo de `block_in_place` y el catch-up, y si hace falta algo que la Evaluación 1 no mostró.

### 4.7 Resultados

Medidos el 31 de Octubre de 2026, en la misma máquina que la Evaluación 1 (§3.7) y con el mismo binario del servidor. El código está en la rama `perf/e2-salas`: el modo `rooms` del generador (`crates/perf/src/rooms.rs`), `run_e2.sh` (una corrida contra un servidor nuevo), `e2_all.sh` (todas las mediciones) y `summarize_e2.sh` (la mediana de las repeticiones), con los datos crudos en `crates/perf/results/`.

#### Cómo se midió, y en qué se apartó del plan
- **78 corridas, 3 repeticiones por punto, todas válidas:** sin errores de commit, con el head de cada sala igual a los commits confirmados, sin huecos en lo que aplicó cada cliente, sin streams SSE cortados, con todos los clientes al día al terminar, sin advertencias en el log del servidor y con el generador por debajo del 90% de sus núcleos. El atraso del generador (desde el momento programado hasta que sale el commit) fue de unos 2 ms en p99, la resolución del temporizador de Tokio.
- **Los `/sync` y `Ack` rechazados se reintentan** con una espera que se duplica de 20 ms a 1 s, como el SDK. Un `503` no es un error de la corrida, pero se cuenta.
- **La latencia del commit se mide desde que sale el pedido,** no desde el momento programado (el atraso se mide aparte). La de propagación va desde que el autor arma el commit hasta que cada uno de los otros clientes de la sala aplica esa secuencia.
- **El límite de archivos abiertos por proceso es de 20.000 y en este contenedor no se puede subir.** Con un descriptor por sala y uno por stream SSE, alcanza para unas 1.750 salas de 10 clientes, así que la rampa llega a 1.500. El rango de puertos locales se amplió a 1024–65000.
- **El conjunto de conexiones del generador es de 512**, no de 64 (§4.3).
- **Las salas de 50 clientes se midieron con 20 y 50 salas**, y **2e lleva también el presupuesto de respuestas agrandado**, para que los `503` (§4.7.3) no tapen el efecto de `block_in_place`.
- **Diagnóstico:** además de 2e, en tmpfs se midieron las dos causas de §4.7.3 por separado y juntas: sin `block_in_place` (el interruptor de §3.7.4) y con un presupuesto de bytes de respuesta de 4 GiB en lugar de 256 MiB (`ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES`). No son propuestas: miden cuánto pesa cada causa.

#### 4.7.1 Las mediciones (mediana de 3 repeticiones)

**En reposo (2a y 2b, tmpfs):**

| | Servidor | RSS | Hilos | Descriptores |
|---|---|---|---|---|
| 2a | 1 sala, 1 cliente | 6 MiB | 3 | 12 |
| | 100 salas | 14 MiB | 3 | 111 |
| | 1.000 salas | 47 MiB | 3 | 1.011 |
| | 5.000 salas | 149 MiB | 3 | 5.011 |
| 2b | 100 salas, 1.000 clientes con su stream SSE | 41 MiB | 3 | 1.111 |
| | 100 salas, 10.000 clientes con su stream SSE | 229 MiB | 3 | 10.111 |

**Con carga, tmpfs (2c):** 10 clientes por sala, una escritura cada 10 s por cliente, es decir, un commit por segundo por sala.

| Salas (usuarios) | commits/s | `/sync`/s | commit p50 / p99 | propagación p50 / p99 | CPU | RSS | Hilos | `503` |
|---|---|---|---|---|---|---|---|---|
| 100 (1.000) | 98 | 946 | 0,6 / 1,8 ms | 1,0 / 2,7 ms | 18% | 69 MiB | 148 | 226 |
| 250 (2.500) | 253 | 2.421 | 0,5 / 2,2 ms | 0,9 / 4,0 ms | 41% | 117 MiB | 238 | 1.080 |
| 500 (5.000) | 503 | 4.782 | 0,6 / 4,5 ms | 1,1 / 24 ms | 78% | 194 MiB | 319 | 10.959 |
| 750 (7.500) | 753 | 7.026 | 0,7 / 24 ms | 1,5 / **742 ms** | 114% | 288 MiB | 492 | 170.797 |
| 1.000 (10.000) | 1.001 | 6.820 | 3,6 / 83 ms | 70 ms / **9,0 s** | 157% | 378 MiB | 411 | 892.261 |
| 1.500 (15.000) | 1.502 | 2.349 | 20 / 178 ms | 5,2 s / **34 s** | 165% | 518 MiB | 405 | 1.499.671 |

La columna de `503` cuenta los de toda la corrida (75 s). El CPU es el porcentaje de un núcleo: el servidor tiene dos (200%).

**Diagnóstico, tmpfs, mismas condiciones:**

| Salas | Variante | commit p99 | propagación p50 / p99 | CPU | RSS | Hilos | `503` |
|---|---|---|---|---|---|---|---|
| 1.000 | de fábrica (2c) | 83 ms | 70 ms / 9,0 s | 157% | 378 MiB | 411 | 892.261 |
| 1.000 | presupuesto de 4 GiB | 89 ms | 2,4 / 170 ms | 140% | 344 MiB | 433 | 2 |
| 1.000 | sin `block_in_place` | 34 ms | 1,3 / 473 ms | 125% | 267 MiB | 3 | 210.921 |
| 500 | **las dos cosas** | 1,5 ms | 0,9 / **2,9 ms** | 66% | 141 MiB | 3 | 0 |
| 1.000 | **las dos cosas** | 3,0 ms | 1,2 / **5,8 ms** | 118% | 265 MiB | 3 | 0 |
| 1.500 | **las dos cosas** | 76 ms | 1,9 / 144 ms | 161% | 389 MiB | 3 | 1 |

**Con carga, disco real (2d y 2e):**

| Salas × clientes | Variante | commits/s | propagación p50 / p99 | CPU | Hilos | `503` | escrituras/s | flushes/s | disco ocupado |
|---|---|---|---|---|---|---|---|---|---|
| 100 × 10 | de fábrica | 98 | 1,7 / 6,4 ms | 21% | 102 | 245 | 717 | 151 | 6% |
| 250 × 10 | de fábrica | 253 | 1,7 / 24 ms | 50% | 248 | 3.092 | 1.887 | 370 | 13% |
| 500 × 10 | de fábrica | 503 | 1,9 / 31 ms | 94% | 225 | 55.700 | 3.763 | 847 | 24% |
| 750 × 10 | de fábrica | 753 | 6,6 ms / **4,6 s** | 147% | 311 | 456.544 | 5.977 | 1.284 | 37% |
| 1.000 × 10 | de fábrica | 1.001 | 849 ms / **14 s** | 172% | 515 | 1.152.421 | 8.785 | 1.788 | 44% |
| 20 × 50 | de fábrica | 98 | 3,6 / 70 ms | 64% | 53 | 118.311 | 445 | 119 | 5% |
| 50 × 50 | de fábrica | 253 | 28 ms / **1,7 s** | 149% | 101 | 713.483 | 1.126 | 305 | 10% |
| 500 × 10 | 2e: sin `block_in_place`, presupuesto de 4 GiB | 503 | 1,6 / 24 ms | 70% | 3 | 0 | 3.760 | 1.137 | 27% |
| 1.000 × 10 | 2e: sin `block_in_place`, presupuesto de 4 GiB | 1.023 | 783 ms / **3,9 s** | 119% | 3 | 0 | 8.539 | 2.561 | 60% |

#### 4.7.2 Las preguntas
1. **Una sala abierta en reposo cuesta unos 29 KB y un descriptor de archivo** (de 1 a 5.000 salas: de 6 a 149 MiB). **Un cliente conectado, registrado y con su stream SSE, unos 21 KB y un descriptor** (de 1.000 a 10.000 streams: de 41 a 229 MiB). Con carga, 1.000 salas de 10 clientes ocupan 265 MiB sin `block_in_place`, y 378 MiB de fábrica: la diferencia son los hilos (cada uno con su arena de `malloc`).
2. **Con la configuración de fábrica, este servidor de 2 núcleos sostiene unas 500 salas de 10 usuarios (5.000 usuarios, 500 commits/s) con la propagación por debajo de 35 ms en p99, en tmpfs y en disco.** Con 750 se degrada (de 0,7 a 4,6 s en p99) y con 1.000 colapsa. **Lo primero que se agota no es el CPU ni el disco** (con 750 salas, el CPU está al 114% de 200% en tmpfs y el disco al 37% en 2d), **sino el presupuesto de respuestas y el pool de hilos** (§4.7.3). Sin esos dos problemas, 1.000 salas (10.000 usuarios) andan con 5,8 ms en p99 y el CPU al 118%. El límite pasa a ser el CPU, entre 1.000 y 1.500 salas. Con salas de 50 usuarios, el límite llega antes: 50 salas (2.500 usuarios, 250 commits/s) ya colapsan, porque cada commit dispara 49 `/sync` con sus `Ack` y los `503` se multiplican.
3. **El disco no se llenó en ningún punto: como mucho, el 44% ocupado (de fábrica) o el 60% (2e)**, con 1.000 salas. Pero **cada commit cuesta entre 7 y 9 escrituras al disco y unos 1,7 flushes**, y la mayor parte no son del commit sino del roster (§4.7.3): con 1.000 salas, 8.785 escrituras por segundo para 1.001 commits. En un disco de nube con un límite de operaciones por segundo (miles, en los discos de base), eso sí sería el límite. Hubo menos flushes que `fsync` (se estiman unos 3.000 `fsync` por segundo con 1.000 salas, contra 1.788 flushes), así que el kernel junta una parte de los flushes concurrentes.
4. **`block_in_place` importa mucho: es una de las dos causas del colapso** (H7). El `fsync` de cada commit, en cambio, no limita por su latencia: en disco, el colapso llega en el mismo punto que en tmpfs. **Pero tampoco se puede hacer en el worker:** en disco, sin `block_in_place` (2e), 1.000 salas colapsan igual (783 ms de propagación en p50), porque cada `fsync` bloquea uno de los dos workers de Tokio. Hace falta sacarlo de los workers con una cantidad fija de hilos, como dice §3.8.

#### 4.7.3 Los hallazgos nuevos
- **El presupuesto de bytes de respuesta rechaza `/sync` que pesan pocos KB.** El actor reserva un frame entero (16 MiB, `reserve_frame` en `RoomActor`) antes de armar cada respuesta, y el handler HTTP la achica a su tamaño real recién al codificarla (`budgeted_response`). Con 256 MiB de presupuesto, en todo el servidor caben 16 respuestas en ese intervalo. Con muchas salas sincronizando a la vez, el resto recibe `503`, y un commit se confirma sin su catch-up (`has_more`), lo que manda al cliente a hacer un `/sync`: con 1.000 salas, entre 746.549 y 921.721 `503` y entre 34.204 y 41.490 commits sin catch-up por corrida. Los reintentos suman carga, y la carga suma más respuestas en ese intervalo. **Con 4 GiB de presupuesto (256 ranuras) desaparecen**, pero el presupuesto deja de acotar la memoria, que es su función. Ya aparecía con 100 salas (226 `503` por corrida).
- **`block_in_place` agota el pool de bloqueo de Tokio.** Cada `fsync` de cada sala (y cada escritura del roster) traspasa su worker a otro hilo, y con cientos de salas el servidor llega a 411–515 hilos, contra el máximo de 512 del pool. Cuando no hay un hilo libre, el traspaso espera, y con él todas las tareas del worker: las latencias saltan a cientos de ms aunque el CPU no esté lleno. Sin `block_in_place`, el servidor anda con 3 hilos.
- **El roster de cada sala activa se escribe en disco cada segundo, con dos `fsync`.** El tick rápido (`FAST_TICK_PERIOD`, 1 s) persiste el roster si cambió algún cursor (`persist_if_dirty`), con una escritura atómica: el temporal con `sync_all`, el rename y el `fsync` del directorio. En una sala donde alguien escribe, los cursores cambian con cada commit y cada `Ack`, así que se escribe todos los segundos: con 1.000 salas, unos 2.000 `fsync` de roster por segundo, el doble que los de los commits. El diseño ya acepta perder hasta un tick de cambios del roster en un corte (`lease.rs`).
- **El costo de una escritura lo domina su difusión** (H5). Con 10 clientes por sala, cada commit dispara unos 9 `/sync`, 9 `Ack` y 10 avisos SSE: 19 pedidos HTTP en total. Sin los dos problemas, el servidor gasta 1,2 ms de CPU por commit (118% para 1.000 commits/s), unas 10 veces lo que cuesta un commit solo por HTTP (unos 0,1 ms, §3.7.2). El `Ack` después de cada `/sync` (así lo hace hoy el `sync` del SDK) es casi la mitad de esos pedidos.

#### 4.7.4 Hipótesis
- **H4, confirmada en reposo:** una sala cuesta unos 29 KB. Con carga, la memoria la dominan las conexiones (~21 KB por cliente) y, de fábrica, los hilos; el búfer en RAM de cada sala no llega a notarse, porque a un commit por segundo junta unas decenas de operaciones en una corrida.
- **H5, confirmada:** la difusión (los `/sync`, los `Ack` y los avisos) cuesta unas 10 veces el commit.
- **H6, descartada en esta máquina:** el disco nunca pasó del 60% ocupado; antes se agotaron el presupuesto de respuestas y el pool de hilos. Queda abierta para un disco con límite de operaciones por segundo, por las 7 a 9 escrituras por commit.
- **H7, confirmada:** con 750 salas o más, el servidor llega al límite de 512 hilos y las latencias colapsan.

#### 4.7.5 Cuellos de botella del caso típico, de mayor a menor
1. **El presupuesto de respuestas** reserva un frame entero por respuesta hasta codificarla: `503` falsos desde 100 salas, y el colapso con 750 junto con el punto 2. Dirección: que la reserva se acerque al tamaño real antes de que la respuesta espere al handler (por ejemplo, codificarla donde se lee del log, o reservar lo que ocupan las operaciones leídas), sin perder la cota de memoria que el presupuesto garantiza. Toca esa garantía: va con Lote 0.
2. **`block_in_place`:** el pool de hilos se agota con cientos de salas. Dirección: la de §3.8, el `fsync` en una cantidad fija de hilos dedicados. 2e muestra que hacerlo en el worker tampoco sirve en un disco real.
3. **El roster en disco cada segundo por sala activa:** dos `fsync` por sala por segundo, el doble que los commits. Dirección: escribirlo menos seguido, o sin el `fsync` del directorio en cada tick, dentro de lo que el diseño ya acepta perder en un corte.
4. **La difusión de cada commit:** 19 pedidos por commit con 10 clientes, unos 100 con 50. Dirección, para el `SyncWorker` de la sub-fase 4.3, que se está diseñando: no mandar un `Ack` después de cada `/sync` (el cursor ya viaja en cada commit y en cada `/sync`), y juntar los avisos que llegan mientras hay un `/sync` en vuelo. Más adelante, un aviso SSE que lleve las operaciones evitaría el `/sync`, pero es un cambio del protocolo.
5. **La copia de las operaciones en el catch-up** (§3.7.3): en este escenario pesa poco, porque los catch-up son cortos.

#### 4.7.6 Recomendación
- **Primero, los puntos 1 y 2, que juntos duplican la capacidad real del servidor.** Con los dos resueltos (diagnóstico de §4.7.1), 1.000 salas de 10 usuarios pasan de 9 s a 5,8 ms de propagación en p99, con 3 hilos y un 30% menos de memoria. El punto 2 se puede proponer junto con el *group commit* de §3.8, que comparte su diseño.
- **El punto 4 es una decisión del SDK, y conviene tomarla ahora**, mientras se diseña el `SyncWorker` de 4.3: sin el `Ack` después de cada `/sync`, la difusión baja casi a la mitad de los pedidos.
- **El punto 3, después,** con una medición en un disco de nube, donde las escrituras por segundo cuestan.
- **Volver a correr esta evaluación** después de cada uno, con el mismo arnés, para medir lo que se ganó.

---

## 5. Seguimiento

| Evaluación | Qué | Estado |
|---|---|---|
| 1 | El commit con los clientes al día, en una sala | Medida: resultados en §3.7 y conclusiones en §3.8, pendiente la revisión del dueño del proyecto |
| 2 | Muchas salas, con escrituras a ritmo humano | Medida: resultados en §4.7, pendiente la revisión del dueño del proyecto |

---

## 6. Próximos pasos a definir

Se definen con lo que muestren las evaluaciones anteriores. Lo que dejaron abierto las Evaluaciones 1 y 2:
- **Repetir la Evaluación 2 después de cada optimización** de §4.7.6, con el mismo arnés.
- **Un disco de nube con límite de operaciones por segundo** (§4.7.2, pregunta 3): las 7 a 9 escrituras por commit, sobre todo las del roster, pueden ser el límite ahí.
- **Más de 1.750 salas o más streams:** en una máquina donde se pueda subir el límite de archivos abiertos, o con el generador en otra máquina.
- **Las latencias máximas y la pausa de más de 5 s** (§3.7.1 y §3.7.5): corridas más largas con el registro de pausas, cruzado con los sellados de segmentos y el *steal time* de la máquina.
- **Una sala con muchos segmentos** (§3.7.6): una corrida larga con carga sostenida, para ver si abrir la sala, el índice de segmentos o el directorio se vuelven lentos con cientos de miles de archivos.
- **Otro asignador de memoria** (§3.7.8, punto 5): la misma Evaluación 1 con otro asignador, en la rama del spike.
- **Clientes atrasados:** el catch-up y `/sync` leyendo de RAM, de disco tibio y de disco frío (Zstd).
- **Abrir una sala:** cuánto tarda con un `active.wal` grande y muchos segmentos (rehidratar la caché de deduplicación).
- **El relay de snapshots:** subida y bajada por chunks.
- **HTTP/2 y detrás del proxy:** h2c, y el servidor detrás de Caddy con TLS.
- **Una máquina como la de producción:** sobre todo para la latencia del `fsync` de su disco y su límite de operaciones por segundo.
- **Benchmarks permanentes:** convertir el arnés en benchmarks que se corran en cada cambio, y si alguno entra a la CI.
- **Las optimizaciones** que salgan de la Evaluación 1, cada una como propuesta aparte (DP2).
