# Evaluaciones de performance — Plan

**Fecha:** 30 de Octubre de 2026
**Tipo:** Plan de evaluaciones, fuera de las fases. Documento vivo: cada evaluación agrega su sección, sus resultados y su estado en el seguimiento (§4).
**Estado:** Propuesta. Definida la Evaluación 1 (el commit con los clientes al día); las siguientes, a definir (§5).
**Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md); cada evaluación es un spike (§3 de la metodología).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md).

---

## 0. Resumen

- **Qué.** Medir cuán rápido y cuán liviano es `zemdb-server`, de a un flujo por vez y empezando por el más simple, para encontrar sus cuellos de botella con números y no con supuestos. Hoy el repositorio no tiene ningún benchmark.
- **Por qué ahora.** El servidor está cerrado desde la Fase 3.11 y la Fase 4 trabaja sobre el cliente: medir el servidor no se cruza con ella.
- **Qué no.** Una evaluación no optimiza nada (DP2). Tampoco reemplaza las pruebas de estrés de la Fase 5, que buscan que el sistema no se rompa bajo carga; acá se busca cuánto rinde y dónde se va el tiempo.
- **Primer paso:** la Evaluación 1 (§3), el commit con los clientes al día, en una sala.

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
- Si el arnés resulta útil para comparar en cada cambio, convertirlo en un benchmark permanente es uno de los próximos pasos (§5), con su propia decisión.

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
- **Métricas de cada corrida:** commits/s; latencia p50, p99, p99.9 y máxima, medida en el generador con un histograma (HdrHistogram); CPU del proceso del servidor (de `/proc/<pid>/stat`); RSS del servidor al terminar y su máximo (`VmHWM`).
- **Perfil de CPU:** con `perf` si la máquina lo permite; si no (en un contenedor sin acceso a los eventos del kernel), con un perfilador de muestreo dentro del proceso (por ejemplo, el crate `pprof`), agregado en la rama del spike. Se guarda como flamegraph junto a los resultados.
- **El entorno se anota con cada resultado:** CPU y núcleos, RAM, sistema de archivos y tipo de disco del directorio de datos, versión de Rust, commit de `main`, y la asignación de núcleos.
- **Los números absolutos valen para esa máquina.** Sirven para comparar entre corridas en la misma máquina. La latencia del `fsync` en particular depende del disco: la de producción se mide en una máquina como la de producción (§5).

---

## 2. Cómo se define una evaluación

Cada evaluación tiene, antes de medir nada:
- **las preguntas** que responde, escritas de antemano;
- **el flujo** que ejercita, nombrado en el código;
- **el escenario:** esquema, operaciones, clientes, salas y configuración;
- **las mediciones,** cada una con lo que cambia respecto de las otras;
- **las hipótesis** que se esperan confirmar o descartar, para que el resultado se lea contra algo;
- **cómo cierra.**

Al cerrar, agrega sus resultados y sus conclusiones en su sección, y su estado en el seguimiento (§4).

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
- **Transporte:** HTTP/1.1 con una conexión keep-alive por trabajador, que es como un proxy como Caddy le habla al servidor por defecto (`README.md`, *Deploying the Server*). HTTP/2 queda para después (§5).
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
- Una recomendación de qué optimizar primero (cada una, una propuesta aparte, DP2) y de qué evaluar después (§5).

### 3.7 Resultados
*A completar al cerrar la evaluación.*

---

## 4. Seguimiento

| Evaluación | Qué | Estado |
|---|---|---|
| 1 | El commit con los clientes al día, en una sala | Propuesta |

---

## 5. Próximos pasos a definir

Se definen después de la Evaluación 1, con lo que muestre. Candidatos, sin orden:
- **Muchas salas a la vez:** el techo del servidor entero, y cómo se reparten los núcleos entre actores.
- **Varios clientes por sala con SSE:** el costo de avisar a cada uno y de su catch-up.
- **Clientes atrasados:** el catch-up y `/sync` leyendo de RAM, de disco tibio y de disco frío (Zstd).
- **Memoria:** RSS por sala abierta en reposo y por stream SSE; cuántas salas caben en una máquina chica.
- **Abrir una sala:** cuánto tarda con un `active.wal` grande y muchos segmentos (rehidratar la caché de deduplicación).
- **El relay de snapshots:** subida y bajada por chunks.
- **HTTP/2 y detrás del proxy:** h2c, y el servidor detrás de Caddy con TLS.
- **Latencia bajo una tasa de llegada fija** (lazo abierto), en lugar del lazo cerrado de DP3.
- **Una máquina como la de producción:** sobre todo para la latencia del `fsync` de su disco.
- **Benchmarks permanentes:** convertir el arnés en benchmarks que se corran en cada cambio, y si alguno entra a la CI.
- **Las optimizaciones** que salgan de la Evaluación 1, cada una como propuesta aparte (DP2).
