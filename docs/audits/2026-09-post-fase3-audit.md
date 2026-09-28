# INFORME DE AUDITORÍA TÉCNICA MULTIDISCIPLINAR — RIMDB (POST-FASE 3)
**Inventario Exhaustivo de Defectos, Vulnerabilidades y Deuda Técnica**

**Fecha de Consolidación:** 25 de Septiembre de 2026  
**Proyecto:** `RimDB` (Motor de Base de Datos Distribuida Local-First)  
**Coordinador Técnico:** Arquitecto Principal de Auditoría  
**Alcance:** Repositorio completo (`crates/core`, `crates/storage`, `crates/server`, `crates/client`, `Cargo.toml`, `ARCHITECTURE.md`, `ROADMAP.md`)  
**Metodología:** Auditoría iterativa independiente mediante 4 subagentes especializados (`rust_code_specialist`, `database_engine_specialist`, `distributed_systems_specialist`, `architecture_specialist`) con aislamiento estricto de contexto (*Zero Context Leakage*).  
**Convergencia Técnica:** Alcanzada en la **Iteración 3** ($\Delta_3 = 0$ nuevos modos de falla funcionales frente al catálogo acumulado).  
**Premisa Operativa:** Modo Estricto de **SOLO LECTURA**. Cero modificaciones a código fuente. Contexto V1 (sin requerimiento de retrocompatibilidad). Enfoque exclusivo en defectos, modos de fallo y soluciones concretas.

---

## 1. RESUMEN DEL PROCESO DE AUDITORÍA Y MÉTRICAS DE CONVERGENCIA

El proceso de auditoría se ejecutó a lo largo de 3 iteraciones independientes y concurrentes, sometiendo a inspección analítica de primeros principios la totalidad de las 4 capas del workspace tras la finalización de la Fase 3 (`rimdb-server`).

### Métricas de Progresión por Iteración

```
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           PROGRESIÓN DE CONVERGENCIA TÉCNICA (Δk)                              │
├───────────┬─────────────────┬───────────────────┬────────────────────────┬─────────────────────┤
│ Iteración │ Fallos Reportados │ Duplicados/Síntomas │ Errores Canónicos (Δk) │ Catálogo Acumulado  │
├───────────┼─────────────────┼───────────────────┼────────────────────────┼─────────────────────┤
│ Ronda 1   │ 68 reportes     │ 31 subsumidos     │ 37 fallos nuevos       │ 37 fallos totales   │
│ Ronda 2   │ 63 reportes     │ 52 subsumidos     │ 11 fallos nuevos       │ 48 fallos totales   │
│ Ronda 3   │ 59 reportes     │ 59 subsumidos     │  0 fallos nuevos (Δ=0) │ 48 fallos finales   │
└───────────┴─────────────────┴───────────────────┴────────────────────────┴─────────────────────┘
```

### Distribución por Severidad del Catálogo Consolidado (48 Fallos Canónicos)
- **Críticos (11)**: Riesgo inminente de corrupción de datos, pérdida silenciosa de operaciones, secuestro de estado por falta de autenticación, o bloqueo permanente del secuenciador/actor (*bricking*).
- **Altos (17)**: Congelamiento de hilos del reactor asíncrono Tokio, vulnerabilidades de denegación de servicio (DoS) por memoria o disco sin cota, fallas en garantías Copy-on-Write y rotura de contratos de red.
- **Medios (16)**: Degradaciones algorítmicas de $O(1)$ a $O(N)$, violaciones de encapsulamiento con exposición de estado mutable, desalineaciones de especificación, y falta de validaciones defensivas.
- **Bajos (4)**: Disparidades menores de formato en respuestas de error, omisión de traits estándar de Rust e inconsistencias en la documentación del roadmap.

---

## 2. MATRIZ INTEGRAL DE DEFECTOS CONSOLIDADOS

```
┌──────┬──────────┬──────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────┐
│ ID   │ Severidad│ Módulo / Crate                       │ Resumen Técnico del Defecto                                                             │
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ C-01 │ Crítico  │ crates/server/src/actor/room.rs      │ [RESUELTO] Desfase off-by-one salta y omite sistemáticamente last_ack_seq + 1.         │
│ C-02 │ Crítico  │ crates/server/src/actor/room.rs      │ [RESUELTO] Supresión silenciosa de BehindCompaction en Commit provocando divergencia.  │
│ C-03 │ Crítico  │ crates/server/src/api/data_plane.rs  │ Ausencia total de autenticación y validación de lease en todo el Data Plane.            │
│ C-04 │ Crítico  │ crates/server/src/actor/room.rs      │ Dual-WAL desacoplado: desincronización y agujero de secuencia irrecuperable en crash.  │
│ C-05 │ Crítico  │ crates/storage/src/disk/recovery.rs  │ Replay ciego del WAL histórico sobre el snapshot base sin omitir secuencias consolidadas.│
│ C-06 │ Crítico  │ crates/core/src/protocol/wal_frame.rs│ Torn writes en EOF clasificados erróneamente como corrupción fatal por fallo de CRC32.  │
│ C-07 │ Crítico  │ crates/core/src/protocol/wal_frame.rs│ decode_wal_record_from_slice drena solo 1 op y descarta el resto del lote en WalReader. │
│ C-08 │ Crítico  │ crates/server/src/actor/lease.rs     │ [RESUELTO] Rediseño Onboarding: Bootstrapping state + Ancla de retención de snapshots. │
│ C-09 │ Crítico  │ crates/server/src/actor/manager.rs   │ Condición de carrera TOCTOU en get_or_spawn duplica actores de sala y corrompe WALs.   │
│ C-10 │ Crítico  │ crates/server/src/relay.rs           │ Inyección arbitraria de estado por upload anónimo y colisión con DefaultBodyLimit 16MB. │
│ C-11 │ Crítico  │ crates/core/src/schema/table.rs      │ Deserialización de TableSchema elude invariantes estructurales provocando pánico.       │
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ A-01 │ Alto     │ crates/server/src/actor/room.rs      │ I/O síncrono bloqueante y compresión Zstd ejecutados en el reactor asíncrono de Tokio.  │
│ A-02 │ Alto     │ crates/server/src/micro_wal.rs       │ Crecimiento ilimitado de MicroWal y lectura monolítica a RAM con riesgo de OOM en boot.│
│ A-03 │ Alto     │ crates/storage/src/disk/mod.rs       │ Falsa compactación CoW: bloqueo exclusivo de sala congela escrituras concurrentes.      │
│ A-04 │ Alto     │ crates/storage/src/disk/compactor.rs │ Carrera O_TRUNC antes de flock en compactor trunca snapshots concurrentes a 0 bytes.    │
│ A-05 │ Alto     │ crates/storage/src/disk/format.rs    │ Cabecera FileHeader carece de checksum/CRC sobre el payload comprimido del snapshot.   │
│ A-06 │ Alto     │ crates/storage/src/engine.rs         │ Ruptura de Liskov en StorageEngine: formatos incompatibles de snapshot (Memory vs Disk).│
│ A-07 │ Alto     │ crates/server/src/actor/room.rs      │ Falta de validación ack_seq <= head_seq en handle_ack permite purga catastrófica de logs│
│ A-08 │ Alto     │ crates/storage/src/disk/mod.rs       │ apply_batch omite validación de esquema, permitiendo mutaciones de tipos incompatibles. │
│ A-09 │ Alto     │ crates/storage/src/disk/mod.rs       │ Escaneo lazy por chunks libera el lock entre bloques, rompiendo Snapshot Isolation.    │
│ A-10 │ Alto     │ crates/server/src/actor/lease.rs     │ Deadlock lógico en Dead Man's Switch: clientes Disconnected bloquean poda de logs.      │
│ A-11 │ Alto     │ crates/server/src/api/data_plane.rs  │ Data Plane utiliza get_room en RAM en lugar de lazy-spawning, fallando tras reinicios.  │
│ A-12 │ Alto     │ crates/server/src/log/tiered_log.rs  │ Inversión jerárquica en fetch_deltas: escaneo síncrono de disco previo al RAM HotBuffer.│
│ A-13 │ Alto     │ crates/server/src/log/tiered_log.rs  │ Evicción destructiva del HotBuffer al sellar segmentos vacía el 100% de la memoria.    │
│ A-14 │ Alto     │ crates/server/src/actor/manager.rs   │ delete_room elimina directorio físicamente con actor Tokio en vuelo y descriptores vivos.│
│ A-15 │ Alto     │ crates/server/src/relay.rs           │ [RESUELTO] SnapshotRelay respaldado en disco con TTL configurable y purga física.       │
│ A-16 │ Alto     │ crates/client/src/lib.rs             │ Crate rimdb-client es un cascarón vacío stub sin implementación del SDK de cliente.     │
│ A-17 │ Alto     │ crates/server/tests/                 │ Suites de prueba ignoran deliberadamente catchup_ops permitiendo pérdidas de datos.     │
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ M-01 │ Medio    │ crates/core/src/mutation/squash.rs   │ Regla 1 de squashing sobrescribe celdas nulas con updates viejos violando LWW.         │
│ M-02 │ Medio    │ crates/server/src/actor/room.rs      │ [RESUELTO] Reintentos idempotentes de Commit devuelven catchup_ops con mutación propia. │
│ M-03 │ Medio    │ crates/core/src/protocol/codec.rs    │ Ausencia de magic bytes, versión de wire protocol y discriminante en codec binario.     │
│ M-04 │ Medio    │ crates/server/src/api/auth.rs        │ Comparación de firmas en tiempo variable susceptible a ataques de canal lateral (timing)│
│ M-05 │ Medio    │ crates/server/src/api/auth.rs        │ Backdoor dev-token cableado en código de autenticación de producción.                   │
│ M-06 │ Medio    │ crates/server/src/api/control_plane  │ Evolución DDL (add_column) no emite señal SSE provocando desincronización de esquemas.  │
│ M-07 │ Medio    │ crates/server/src/api/data_plane.rs  │ Omisión de validación de room_id en URL path vs payload binario (bypass de gateway).   │
│ M-08 │ Medio    │ crates/server/src/api/data_plane.rs  │ Ausencia de timeouts perimetrales en llamadas sender.send y rx.await hacia actores.     │
│ M-09 │ Medio    │ crates/server/src/api/data_plane.rs  │ max_batch_size en /sync sin límite superior permite decodificación masiva abusiva (DoS)│
│ M-10 │ Medio    │ crates/server/src/log/tiered_log.rs  │ Avance prematuro de tail_seq en prune_older_than induce BehindCompaction espurio.       │
│ M-11 │ Medio    │ crates/server/src/log/warm_disk.rs   │ Doble fsync por operación sin Group Commit ni batching en escrituras del servidor.      │
│ M-12 │ Medio    │ crates/server/src/log/warm_disk.rs   │ Ausencia de cerrojos multi-proceso (flock) sobre WALs del servidor.                     │
│ M-13 │ Medio    │ crates/storage/src/engine.rs         │ Contrato StorageEngine exige table: &str en get/scan forzando búsquedas por string.     │
│ M-14 │ Medio    │ crates/core/src/schema/global.rs     │ Sobrescritura silenciosa de tablas con igual nombre y overflow en asignación de IDs.    │
│ M-15 │ Medio    │ crates/core/src/value/row.rs         │ Fuga de encapsulamiento e implementación impropia de Deref en PrimaryKey y CompactRow.  │
│ M-16 │ Medio    │ crates/storage/src/disk/mod.rs       │ Retención de cerrojo global del motor durante I/O de sala en close_room.                │
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ B-01 │ Bajo     │ crates/core/src/protocol/messages.rs │ Discrepancia de especificación: ClientMessage::Heartbeat carece de campo last_ack_seq.  │
│ B-02 │ Bajo     │ crates/core/src/mutation/op.rs       │ Tipos fundamentales de mutación y mensajería omiten traits estándar Eq y Hash.          │
│ B-03 │ Bajo     │ crates/server/src/api/data_plane.rs  │ Handlers de Data Plane devuelven texto plano o JSON ante errores en vez de Bincode.     │
│ B-04 │ Bajo     │ crates/server/src/api/control_plane  │ GET /admin/rooms/:id produce efectos colaterales activando actores de sala en memoria.  │
└──────┴──────────┴──────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. DETALLE TÉCNICO EXHAUSTIVO POR DEFECTO

### DEFECTOS DE SEVERIDAD CRÍTICA

#### [C-01] Desfase off-by-one salta y omite sistemáticamente `last_ack_seq + 1` en catchup 1-RTT
* **Estado**: **RESUELTO**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:291-298`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L291-L298) y [`crates/server/src/actor/room.rs:236-245`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L236-L245).
* **Causa Raíz**: En `handle_commit`, el código calcula `from_seq = SequenceNumber::new(last_ack_seq.get() + 1)`. Sin embargo, `TieredLog::fetch_deltas` y las capas subyacentes (`HotBuffer`, `WarmDiskLog`, `ColdDiskLog`) filtran estrictamente con la condición `op.seq > from_seq`. Al haber sumado 1 previamente, la consulta filtra `op.seq > last_ack_seq + 1`, descartando por completo el delta correspondiente a `last_ack_seq + 1`.
* **Impacto**: Pérdida silenciosa de eventos en cada commit donde el cliente tenga atraso respecto a la sala. El cliente local rechaza el lote devuelto con `StorageError::SequenceMismatch` (`head_seq + 1 != op.seq`), rompiendo la replicación y congelando el cliente.
* **Solución Técnica / Implementada**: Se eliminó el incremento artificial `+ 1`. `handle_commit` ahora pasa `last_ack_seq` directamente a `fetch_deltas(last_ack_seq, 100)`. Al respetar el contrato semántico de `from_seq` como el cursor ya conocido por el cliente, los deltas devuelven con precisión el rango `(last_ack_seq, new_seq]`.

#### [C-02] Supresión silenciosa de `BehindCompaction` en `handle_commit` provocando divergencia de réplicas
* **Estado**: **RESUELTO**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:295`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L295).
* **Causa Raíz**: Cuando un cliente retrasado cuyo cursor fue podado físicamente del log (`last_ack_seq < tail_seq - 1`) envía una mutación, `fetch_deltas` retorna `Err(ServerError::BehindCompaction)`. La línea 295 utilizaba `.unwrap_or_else(|_| (vec![seq_op], false))`, enmascarando el error y respondiendo HTTP 200 con `CommitAck` que contiene únicamente `vec![seq_op]`. Previamente, la mutación ya fue secuenciada y persistida en WAL.
* **Impacto**: Violación del invariante de consistencia distribuida. El cliente continúa escribiendo sobre un estado base desactualizado sin haber recibido nunca la notificación de compactación ni el snapshot base, provocando divergencia silenciosa e irreparable respecto a los demás nodos.
* **Solución Técnica / Implementada**: Se valida de forma preventiva el cursor antes de secuenciar la mutación, propagando `Err(ServerError::BehindCompaction)` si `last_ack_seq < tail_seq - 1` (o si el cliente no está en estado `Connected`, rechazando clientes `Bootstrapping`/`Dormant`). Si `fetch_deltas` retorna `Err(ServerError::BehindCompaction)`, el error se propaga inmediatamente sin tragar la excepción.

#### [C-03] Ausencia total de autenticación, autorización y validación de lease en todo el Data Plane
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:131-584`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L131-L584), [`crates/server/src/api/sse.rs:15-53`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/sse.rs#L15-L53), [`crates/core/src/protocol/messages.rs:49-105`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L49-L105).
* **Causa Raíz**: La verificación del token firmado (`verify_client_token`) se ejecuta únicamente en `/rooms/:id/register`. Los handlers `/commit`, `/sync`, `/ack`, `/heartbeat`, `/deregister` y `/events` no extraen cabeceras `Authorization` ni validan firmas. Asimismo, `RoomActor` procesa mutaciones de cualquier `client_id` sin validar si posee un lease activo en `ClientLeaseTracker`.
* **Impacto**: Evasión absoluta del control de acceso. Cualquier entidad anónima en red puede inyectar mutaciones forjadas a nombre de cualquier usuario, descargar bases de datos históricas mediante `/sync`, o emitir `Ack` falsos que provoquen la purga anticipada de datos de clientes legítimos.
* **Solución Técnica**: Implementar un middleware extractor `ClientAuth` en Axum que valide el Bearer token criptográfico en todas las rutas bajo `/rooms/:id/*`. En `RoomActor`, verificar que el `client_id` solicitante se encuentre en estado `Connected` en `ClientLeaseTracker`.

#### [C-04] Dual-WAL desacoplado: desincronización y agujero de secuencia irrecuperable en reinicios
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:54-65`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L54-L65), [`crates/server/src/actor/room.rs:264-282`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L264-L282), [`crates/server/src/log/tiered_log.rs:101-107`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs#L101-L107).
* **Causa Raíz**: Persistencia dual independiente en cada commit: `micro_wal.append(new_seq)` seguido de `tiered_log.append(seq_op)`. Si ocurre un crash entre ambas escrituras, `micro_wal` registra la secuencia $N$, pero `tiered_log` solo alcanza $N-1$. Al reiniciar, `spawn` reconcilia `head_seq = max(wal_recovery.head_seq, tiered_log.head_seq())` ($N$), pero el campo interno de `TieredLog` permanece en $N-1$. La siguiente mutación ($N+1$) es rechazada por `TieredLog::append` con error de contigüidad no recuperable: `expected N, got N+1`.
* **Impacto**: Inutilización permanente de la sala (*room bricking*). Todo commit posterior falla indefinidamente.
* **Solución Técnica**: Erradicar el patrón Dual-WAL unificando la persistencia en `active.wal` incorporando `mutation_id` y `client_id` en el enmarcado de lote (`wal_frame`), o implementar una fase de reconciliación en `spawn` que trunque `MicroWal` al `head_seq` real confirmado por `TieredLog`.

#### [C-05] Replay ciego del WAL histórico sobre el snapshot base sin omitir secuencias consolidadas
* **Ubicación Exacta**: [`crates/storage/src/disk/recovery.rs:232-265`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs#L232-L265).
* **Causa Raíz**: Durante `recover_room`, el motor carga el snapshot base `room_{id}.snap` estableciendo `tables` y `snapshot_seq`. Acto seguido, abre `room_{id}.wal` desde el offset 0 y ejecuta incondicionalmente todas las operaciones sobre las tablas sin comprobar si `op.seq <= snapshot_seq`.
* **Impacto**: Corrupción y resurrección zombi de datos si el servidor experimentó una caída antes de truncar el WAL tras un snapshot. Re-aplica deltas obsoletos sobre tuplas ya compactadas y amplifica masivamente el tiempo de arranque.
* **Solución Técnica**: Introducir guarda estricta en el bucle de replay de `recover_room`:
  ```rust
  if op.seq <= snapshot_seq {
      continue;
  }
  ```

#### [C-06] Torn writes en EOF clasificados erróneamente como corrupción fatal por fallo de CRC32
* **Ubicación Exacta**: [`crates/core/src/protocol/wal_frame.rs:163-169`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/wal_frame.rs#L163-L169), [`crates/storage/src/disk/recovery.rs:214-220`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs#L214-L220), [`crates/server/src/log/warm_disk.rs:234-237`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/warm_disk.rs#L234-L237).
* **Causa Raíz**: Si un corte de energía interrumpe la escritura del último lote en el WAL, el payload queda truncado o con basura residual de bloque, provocando `crc != expected_crc`. El decodificador no clasifica el fallo como `TornWrite` en EOF si existen bytes no nulos en la cabecera, retornando `WalFrameError::Corruption`. Como resultado, `recover_room` aborta el arranque con `StorageError::WalCorruption`.
* **Impacto**: Un corte de corriente durante una escritura en el WAL inhabilita el reinicio del motor (`open_room` falla), rompiendo la promesa arquitectónica de auto-recuperación de torn writes.
* **Solución Técnica**: Si el fallo de magic bytes o CRC32 ocurre en el registro terminal del archivo y no existen tramas válidas posteriores, clasificarlo como `WalBatchDecodeResult::TornWrite`, permitiendo truncar el WAL al último desplazamiento válido (`valid_wal_bytes`) y completar el inicio.

#### [C-07] `decode_wal_record_from_slice` drena solo 1 op y descarta el resto del lote en `WalReader`
* **Ubicación Exacta**: [`crates/core/src/protocol/wal_frame.rs:188-209`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/wal_frame.rs#L188-L209), [`crates/storage/src/disk/wal.rs:71-81`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/wal.rs#L71-L81).
* **Causa Raíz**: `decode_wal_record_from_slice` decodifica un lote con múltiples operaciones pero ejecuta `ops.drain(..).next()`, devolviendo únicamente la primera operación pero retornando `bytes_consumed` igual al tamaño del lote completo. `WalReader::next_record` avanza su offset en la totalidad del lote.
* **Impacto**: Pérdida silenciosa de datos. En cualquier archivo WAL donde se agrupen mutaciones en lotes multi-operación (`write_batch`), todas las operaciones a partir de la segunda son omitidas permanentemente.
* **Solución Técnica**: Refactorizar `WalReader` para almacenar un buffer interno de operaciones pendientes (`pending_ops: VecDeque<SequencedOperation>`) que se vacíe antes de decodificar nuevos frames en disco.

#### [C-08] Código muerto en `register_client` y fallo conceptual al re-anclar cursor en onboarding / estado `Dormant`
* **Estado**: **RESUELTO (Rediseño de Flujo de Onboarding y Ancla de Retención)**
* **Ubicación Exacta**: [`crates/server/src/actor/lease.rs:88-95`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/lease.rs#L88-L95), [`crates/server/src/actor/room.rs:370-390`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L370-L390), [`crates/core/src/protocol/messages.rs:50-70`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L50-L70).
* **Causa Raíz y Análisis Crítico**:
  1. *Defecto de Código Muerto*: `entry.state = ClientState::Connected;` se ejecutaba antes de evaluar `if entry.state == ClientState::Dormant`, dejando inalcanzable la rama.
  2. *Defecto Conceptual de Re-anclaje Ciego*: La remediación preliminar de asignar `entry.last_ack_seq = current_head` adolecía de un grave defecto distribuido. Un snapshot base generado por un par activo se crea sobre una secuencia $S \le \text{current\_head}$ y es inmutable. Si se realizan mutaciones posteriores $(S, \text{current\_head}]$ y el servidor adelantara a ciegas el cursor del cliente a $\text{current\_head}$, el log de deltas intermedios $(S, \text{current\_head}]$ sería podado proactivamente. Cuando el cliente restaure el snapshot en $S$ y solicite los deltas posteriores, sufrirá un fallo catastrófico de contigüidad o un bucle infinito de `BehindCompaction`.
* **Impacto**: Clientes nuevos o en reconexión quedaban desincronizados permanentemente o bloqueaban la retención de disco de salas activas.
* **Solución Técnica / Implementada**:
  - **Nuevo Estado `ClientState::Bootstrapping`**: Introducido en `ClientLeaseTracker`. Clientes nuevos o reconectados cuyo cursor esté desfasado (`current_seq < tail_seq - 1` o `None`) ingresan en `Bootstrapping`.
  - **Aislamiento de Retención Proactiva**: Los clientes en `Bootstrapping` son excluidos del cálculo `min_connected_ack_seq`, garantizando que un cliente lento en onboarding jamás congele la poda de disco para los miembros conectados activos.
  - **Ancla de Retención de Snapshots (*Retention Anchor*)**: En `RoomActor::prune_older_than`, el límite inferior de poda se calcula como:
    $$\text{retention\_floor} = \min(\text{min\_connected\_ack}, \text{active\_snapshot\_seq})$$
    Mientras un snapshot activo en secuencia $S$ permanezca disponible en `SnapshotRelay`, el servidor retiene los deltas $(S, \text{current\_head}]$, protegiéndolos contra la poda proactiva.
  - **Handshake Extendido**: `ClientMessage::RegisterClient` incluye `current_seq: Option<SequenceNumber>` y `ServerMessage::Registered` retorna `head_seq`, `tail_seq` y `active_snapshot_seq: Option<SequenceNumber>`.
  - **Promoción Fluida a `Connected`**: Al aplicar el snapshot en $S$ y enviar su primer `/sync` o `Ack` que alcance o supere $S$, el cliente es promovido de `Bootstrapping` a `Connected`. Si el snapshot vence por TTL o es purgado del relay, la restricción se libera automáticamente.

#### [C-09] Condición de carrera TOCTOU en `get_or_spawn` duplica actores de sala y corrompe WALs
* **Ubicación Exacta**: [`crates/server/src/actor/manager.rs:63-122`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/manager.rs#L63-L122).
* **Causa Raíz**: `get_or_spawn_with_policy` consulta `self.rooms.get(room_id)`. Si no existe, libera el lock de `DashMap`, resuelve el esquema en disco y ejecuta `RoomActor::spawn`. Múltiples peticiones concurrentes para una sala no activa superan la comprobación en paralelo y lanzan dos o más tareas Tokio independientes para la misma sala sobre los mismos archivos en disco sin bloqueos `flock`.
* **Impacto**: Corrupción catastrófica de los logs WAL por escrituras intercaladas no coordinadas y generación de actores huérfanos compitiendo por el secuenciador.
* **Solución Técnica**: Utilizar el entry pattern de `DashMap` o un cerrojo shardeado de inicialización (`tokio::sync::Mutex` por `RoomId`) para garantizar que la instanciación de un `RoomActor` sea estrictamente atómica y única.

#### [C-10] Inyección arbitraria de estado por upload anónimo y colisión con `DefaultBodyLimit` (16 MB)
* **Ubicación Exacta**: [`crates/server/src/relay.rs:120-150`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/relay.rs#L120-L150), [`crates/server/src/api/router.rs:72`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/router.rs#L72).
* **Causa Raíz**:
  1. `POST /rooms/:id/snapshot/upload` no exige autenticación alguna; cualquier cliente puede subir un buffer binario arbitrario que el relay acepta y distribuye a clientes en onboarding.
  2. La subida se realiza monolíticamente en un solo POST (`body: Bytes`). El router aplica `.layer(DefaultBodyLimit::max(16 * 1024 * 1024))`. Si un snapshot supera 16 MB, es rechazado con HTTP 413, invalidando el protocolo multipart concebido para datasets grandes.
* **Impacto**: Inyección y envenenamiento de estado en clientes. Inoperabilidad absoluta del bootstrapping para bases de datos superiores a 16 MB.
* **Solución Técnica**: Exigir autenticación criptográfica en la subida e implementar un endpoint multipart por fragmentos (`POST /rooms/:id/snapshot/upload-chunk`) ensamblado en disco efímero.

#### [C-11] Deserialización de `TableSchema` elude invariantes estructurales provocando pánico
* **Ubicación Exacta**: [`crates/core/src/schema/table.rs:162-188`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/table.rs#L162-L188), [`crates/core/src/schema/validation.rs:390-395`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/validation.rs#L390-L395).
* **Causa Raíz**: La implementación de `Deserialize` para `TableSchema` deserializa campos crudos sin invocar las validaciones del builder (PK no vacía, columnas de PK existentes y no nulas, nombres de columna únicos). En `validation.rs`, el código asume que el invariante se cumple y ejecuta `.expect("primary key column must exist in column_indices")`.
* **Impacto**: Un payload JSON malicioso o corrupto enviado a `/admin/schemas` provoca el pánico del proceso del servidor al procesar mutaciones sobre el esquema deserializado.
* **Solución Técnica**: Delegar la deserialización a través de un constructor validador (`TableSchema::try_from`) y reemplazar el `.expect()` por propagación controlada con `ValidationError::UnknownColumn`.

---

### DEFECTOS DE SEVERIDAD ALTA

#### [A-01] I/O síncrono bloqueante y compresión Zstd ejecutados en el reactor asíncrono de Tokio
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:104-121`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L104-L121), [`crates/server/src/log/cold_disk.rs:26-50`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/cold_disk.rs#L26-L50), [`crates/server/src/actor/lease.rs:70-80`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/lease.rs#L70-L80).
* **Causa Raíz**: En `handle_commit`, `handle_ack` y `run_periodic_maintenance` (cada 500 ms), se invocan llamadas bloqueantes de `std::fs` (`write_all`, `sync_data`, `rename`) y compresión intensiva de CPU `zstd::stream::encode_all` directamente sobre los hilos worker de Tokio sin usar `spawn_blocking`.
* **Impacto**: Inanición del pool de hilos de Tokio (*thread starvation*), provocando picos de latencia de red y caídas de conexiones por timeouts de heartbeat.
* **Solución Técnica**: Confinar la compresión Zstd y las operaciones de disco a `tokio::task::spawn_blocking` o migrar descriptores a `tokio::fs`.

#### [A-02] Crecimiento ilimitado de `MicroWal` y lectura monolítica a RAM con riesgo de OOM en arranque
* **Ubicación Exacta**: [`crates/server/src/micro_wal.rs:75-141`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/micro_wal.rs#L75-L141).
* **Causa Raíz**: `meta_{room_id}.wal` es estrictamente append-only sin rotación ni truncado. En `recover`, ejecuta `read_to_end` a memoria completa para hidratar `DedupLruCache` (capacidad de 10.000 entradas).
* **Impacto**: Fuga continua de almacenamiento y pánico por falta de memoria (OOM Kill) al arrancar salas con millones de transacciones históricas.
* **Solución Técnica**: Truncar el micro-WAL periódicamente conservando únicamente las últimas $N$ entradas requeridas por la caché LRU, y leer en streaming o con `BufReader` inverso.

#### [A-03] Falsa compactación CoW: bloqueo exclusivo de sala congela escrituras concurrentes
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs:301-306`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L301-L306), [`crates/storage/src/disk/compactor.rs:20-88`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/compactor.rs#L20-L88).
* **Causa Raíz**: La compactación se ejecuta inline dentro de `apply_batch` manteniendo adquirido el cerrojo exclusivo `room_arc.write().await` durante toda la serialización Bincode, compresión Zstd y fsyncs.
* **Impacto**: Congelamiento de lecturas y escrituras durante la compactación. Si se intentara mover a background sin rediseñar el WAL, `wal_file.set_len(0)` truncaría y destruiría las mutaciones añadidas concurrentemente.
* **Solución Técnica**: Rotar el WAL a un segmento `wal.compacting`, abrir un nuevo WAL activo para escrituras concurrentes inmediatas y comprimir el segmento en background.

#### [A-04] Carrera `O_TRUNC` antes de `flock` en compactor trunca snapshots concurrentes a 0 bytes
* **Ubicación Exacta**: [`crates/storage/src/disk/compactor.rs:46-53`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/compactor.rs#L46-L53).
* **Causa Raíz**: El archivo temporal de snapshot utiliza una ruta fija `room_{id}.snap.tmp`. Se abre con `.truncate(true)`, lo cual ejecuta la llamada al sistema `open(O_TRUNC)` antes de solicitar el cerrojo `try_lock_exclusive()`.
* **Impacto**: Si dos procesos o workers intentan compactar la misma sala, el segundo trunca el archivo a 0 bytes mientras el primero aún escribe en él, resultando en snapshots corruptos o vacíos.
* **Solución Técnica**: Utilizar nombres temporales únicos basados en UUIDs (`snap.tmp.{uuid}`) y adquirir cerrojos de sala independientes antes de crear archivos.

#### [A-05] Cabecera `FileHeader` carece de checksum/CRC sobre el payload comprimido del snapshot
* **Ubicación Exacta**: [`crates/storage/src/disk/format.rs:60-70`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/format.rs#L60-L70), [`crates/storage/src/disk/recovery.rs:70-94`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs#L70-L94).
* **Causa Raíz**: `header_crc` en `FileHeader` solo cubre los primeros 32 bytes de metadatos. El cuerpo comprimido con Zstandard no posee ninguna suma de comprobación en disco.
* **Impacto**: Corrupción silenciosa en disco (*bit rot*) no es detectada a nivel de formato, pasando directamente a la descompresión con riesgo de fallos opacos.
* **Solución Técnica**: Utilizar bytes del campo `reserved` para incorporar `snapshot_payload_crc32: u32` o digest BLAKE3 y verificarlo antes de descomprimir.

#### [A-06] Ruptura de Liskov en `StorageEngine`: formatos incompatibles de snapshot (Memory vs Disk)
* **Ubicación Exacta**: [`crates/storage/src/engine.rs:68-78`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/engine.rs#L68-L78), [`crates/storage/src/memory/mod.rs:327`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs#L327), [`crates/storage/src/disk/mod.rs:443`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L443).
* **Causa Raíz**: `MemoryStorageEngine` emite Bincode plano sin comprimir; `DiskStorageEngine` emite Bincode comprimido con Zstandard. Ninguno incluye cabecera canónica identificadora.
* **Impacto**: Un snapshot generado por un motor no puede ser restaurado en el otro, rompiendo la sustitución de Liskov y la interoperabilidad en clientes WASM vs nativos.
* **Solución Técnica**: Estandarizar un contenedor canónico de snapshot a nivel de trait con cabecera fija que declare versión y algoritmo de compresión (`None` o `Zstd`).

#### [A-07] Falta de validación `ack_seq <= head_seq` en `handle_ack` permite purga catastrófica de logs
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:348-373`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L348-L373).
* **Causa Raíz**: `handle_ack` no valida que `ack_seq <= self.head_seq`. Si un cliente corrupto envía `ack_seq = u64::MAX`, `prune_older_than(u64::MAX)` borra todos los segmentos Warm y Cold y vacía el `HotBuffer`.
* **Impacto**: Destrucción inmediata del historial activo de la sala. Todos los demás clientes reciben `BehindCompaction` y quedan forzados a descargar snapshots completos.
* **Solución Técnica**: Validar estrictamente `if ack_seq > self.head_seq { return Err(...); }`.

#### [A-08] `apply_batch` omite validación de esquema, permitiendo mutaciones de tipos incompatibles
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs:233-247`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L233-L247), [`crates/storage/src/memory/mod.rs:118-132`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs#L118-L132).
* **Causa Raíz**: `apply_batch` verifica únicamente la existencia de la tabla y la secuencia incremental, pero nunca invoca `schema.validate_operation(&op.op)`.
* **Impacto**: Mutaciones con tipos incompatibles, violaciones de no-nulabilidad o índices de columna fuera de rango ingresan al WAL y corrompen las tablas en memoria.
* **Solución Técnica**: Incorporar `room.schema.validate_operation(&op.op)?` antes de aplicar cambios en disco y memoria.

#### [A-09] Escaneo lazy por chunks libera el lock entre bloques, rompiendo Snapshot Isolation
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs:357-418`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L357-L418), [`crates/storage/src/memory/mod.rs:227-302`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs#L227-L302).
* **Causa Raíz**: El stream de `scan` libera el cerrojo de lectura cada 64 tuplas. Mutaciones concurrentes intermedias alteran las tablas mientras el stream continúa.
* **Impacto**: Ruptura del aislamiento transaccional (*torn reads*, lecturas fantasma e inconsistencia temporal en consultas de rango).
* **Solución Técnica**: Clonar un puntero CoW inmutable del árbol de índices `Arc<BTreeMap>` al inicio del escaneo para garantizar aislamiento Snapshot Isolation sin bloquear escritores.

#### [A-10] Deadlock lógico en Dead Man's Switch: clientes `Disconnected` bloquean poda de logs
* **Ubicación Exacta**: [`crates/server/src/actor/lease.rs:177-231`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/lease.rs#L177-L231).
* **Causa Raíz**: `min_connected_ack_seq` retorna `None` si existe algún cliente en `Disconnected`. A su vez, `check_timeouts` solo pasa un cliente a `Dormant` si su cursor quedó por detrás de `tail_seq - 1`. Dado que `tail_seq` no avanza sin poda, se produce un bloqueo mutuo permanente.
* **Impacto**: Inhabilitación total de la poda de logs en disco si un cliente se desconecta sin desregistrarse, acumulando WALs hasta saturar el almacenamiento.
* **Solución Técnica**: Basar la transición a `Dormant` directamente en el tiempo transcurrido en `Disconnected` (timeout de inactividad de 90s).

#### [A-11] Data Plane utiliza `get_room` en RAM en lugar de lazy-spawning, fallando tras reinicios
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:156-544`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L156-L544).
* **Causa Raíz**: Los endpoints `/commit`, `/sync`, `/ack`, etc., consultan `state.room_manager.get_room(&room_id)`, que solo busca en el `DashMap` en RAM.
* **Impacto**: Tras un reinicio del servidor, clientes legítimos previamente registrados reciben HTTP 404 `RoomNotFound`, aunque la sala exista íntegra en disco.
* **Solución Técnica**: Utilizar `state.room_manager.get_or_spawn(&room_id, None)` en todos los handlers de datos.

#### [A-12] Inversión jerárquica en `fetch_deltas`: escaneo síncrono de disco previo al RAM `HotBuffer`
* **Ubicación Exacta**: [`crates/server/src/log/tiered_log.rs:157-205`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs#L157-L205).
* **Causa Raíz**: `fetch_deltas` escanea primero el disco Cold, luego el disco Warm con `read_dir`, y solo en último término consulta la memoria RAM.
* **Impacto**: En el 99% de las consultas de clientes activos, el servidor ejecuta llamadas síncronas a disco para deltas que ya residen en memoria, degradando el throughput.
* **Solución Técnica**: Invertir la evaluación: verificar primero si `from_seq >= hot_buffer.min_seq()` y responder inmediatamente en sub-milisegundo desde RAM.

#### [A-13] Evicción destructiva del `HotBuffer` al sellar segmentos vacía el 100% de la memoria
* **Ubicación Exacta**: [`crates/server/src/log/tiered_log.rs:121-129`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs#L121-L129).
* **Causa Raíz**: Al sellar un segmento Warm con límite `end`, ejecuta `hot_buffer.evict_older_than(end + 1)`. Al ser `end` la última mutación agregada, purga el 100% del buffer.
* **Impacto**: Caída cíclica del buffer a cero elementos (*Cache Flush Cliff*), forzando a los clientes a leer del disco tras cada rotación.
* **Solución Técnica**: Mantener una ventana deslizante basada en `ram_max_ops` o TTL en memoria en lugar de vaciar todo el buffer.

#### [A-14] `delete_room` elimina directorio físicamente con actor Tokio en vuelo y descriptores vivos
* **Ubicación Exacta**: [`crates/server/src/actor/manager.rs:196-207`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/manager.rs#L196-L207).
* **Causa Raíz**: `delete_room` remueve el sender del mapa y seguidamente ejecuta `fs::remove_dir_all`. La tarea del actor continúa ejecutándose con descriptores de archivo abiertos.
* **Impacto**: Fallos de I/O (`AccessDenied`/`EBUSY`) en Windows y creación de archivos zombi en Unix.
* **Solución Técnica**: Enviar `RoomCommand::Shutdown`, hacer `.await` sobre su `JoinHandle` y solo entonces eliminar el directorio.

#### [A-15] `SnapshotRelay` en memoria RAM sin cuota ni backpressure susceptible a ataques DoS (OOM)
* **Estado**: **RESUELTO**
* **Ubicación Exacta**: [`crates/server/src/relay.rs:26-62`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/relay.rs#L26-L62), [`crates/server/src/config.rs:32-75`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/config.rs#L32-L75).
* **Causa Raíz**: Snapshots se almacenaban en un `DashMap<RoomId, StagedSnapshot>` en memoria sin persistencia a disco ni TTL configurable ni limpieza física, arriesgando OOM en servidores de producción.
* **Impacto**: Agotamiento de la memoria RAM del servidor enviando múltiples payloads a salas aleatorias y pérdida de snapshots tras reinicios.
* **Solución Técnica / Implementada**:
  - `SnapshotRelay::new(snapshots_dir, ttl)` exige obligatoriamente un directorio en disco donde persistir los snapshots (`{room_id}_{head_seq}.snap.zst`).
  - Las subidas se escriben de manera atómica mediante staging a archivos temporales `.tmp.<nanos>` y renombrado seguro.
  - Se integró el parámetro configurable `snapshot_ttl_secs: u64` en `ServerConfig` (por defecto 600 segundos) y soporte de variable de entorno `RIMDB_SNAPSHOT_TTL_SECS`.
  - `cleanup_expired` purga tanto de la memoria RAM como del sistema de archivos con `std::fs::remove_file`, previniendo fugas en disco y agotamiento de RAM (OOM/DoS).
  - Al reiniciar el servidor, `recover_disk_snapshots()` recarga automáticamente snapshots válidos y descarta archivos temporales huérfanos o snapshots vencidos.

#### [A-16] Crate `rimdb-client` es un cascarón vacío stub sin implementación del SDK de cliente
* **Ubicación Exacta**: [`crates/client/src/lib.rs:1-15`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/client/src/lib.rs#L1-L15), [`crates/client/Cargo.toml`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/client/Cargo.toml).
* **Causa Raíz**: Contiene únicamente `pub fn add` a pesar de declarar dependencias y arquitectura para Fase 4.
* **Impacto**: Imposibilidad de ejecutar el sistema de extremo a extremo con clientes reales.
* **Solución Técnica**: Implementar la arquitectura del SDK en Fase 4 (`RimdbClient`, `RoomHandle`, `OutboxQueue`, etc.).

#### [A-17] Suites de prueba ignoran deliberadamente `catchup_ops` permitiendo pérdidas de datos
* **Ubicación Exacta**: [`crates/server/tests/server_integration_tests.rs:357-388`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/tests/server_integration_tests.rs#L357-L388).
* **Causa Raíz**: Los tests validan `assigned_seq` utilizando comodines `..` para ignorar `catchup_ops`.
* **Impacto**: El bug crítico de pérdida de operaciones en catchup (C-01) no fue detectado en las pruebas de integración.
* **Solución Técnica**: Crear pruebas que validen explícitamente el contenido, orden y completitud de `catchup_ops` ante desfases de secuencia.

---

### DEFECTOS DE SEVERIDAD MEDIA

#### [M-01] Regla 1 de squashing sobrescribe celdas nulas con updates viejos violando LWW
* **Ubicación Exacta**: [`crates/core/src/mutation/squash.rs:83-93`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/squash.rs#L83-L93).
* **Causa Raíz**: Cuando un `Update` entrante tiene timestamp menor que un `Insert` previo, la regla 1 resucita valores viejos en columnas que el `Insert` definió como `Null`.
* **Solución Técnica**: Si `incoming.timestamp < existing.timestamp`, descartar el update antiguo completamente.

#### [M-02] Reintentos idempotentes de `Commit` devuelven `catchup_ops` vacío omitiendo mutación propia
* **Estado**: **RESUELTO**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:235-252`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L235-L252).
* **Causa Raíz**: Al detectar duplicado, consultaba deltas desde `existing_seq`, devolviendo un lote vacío que no contenía la mutación ya secuenciada.
* **Solución Técnica / Implementada**: En `handle_commit`, al detectar un `mutation_id` duplicado en `dedup_cache`, se invoca `fetch_deltas(last_ack_seq, 100)` devolviendo el conjunto de operaciones a partir del cursor del cliente, incluyendo la mutación propia secuenciada originalmente.

#### [M-03] Ausencia de magic bytes, versión de wire protocol y discriminante en codec binario
* **Ubicación Exacta**: [`crates/core/src/protocol/codec.rs:8-26`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/codec.rs#L8-L26).
* **Causa Raíz**: Serializa directamente enums Bincode sin enmarcado de protocolo binario ni versión.
* **Solución Técnica**: Anteponer un prefijo fijo de 4 bytes (`[magic: 2B][version: 1B][flags: 1B]`).

#### [M-04] Comparación de firmas en tiempo variable susceptible a ataques de canal lateral (timing)
* **Ubicación Exacta**: [`crates/server/src/api/auth.rs:116`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/auth.rs#L116).
* **Causa Raíz**: Validación de firma BLAKE3 usa `!=` sobre cadenas hexadecimales en tiempo variable.
* **Solución Técnica**: Utilizar comparación en tiempo constante (`subtle::ConstantTimeEq`).

#### [M-05] Backdoor `dev-token` cableado en código de autenticación de producción
* **Ubicación Exacta**: [`crates/server/src/api/auth.rs:71-74`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/auth.rs#L71-L74).
* **Causa Raíz**: Se acepta `dev-token` incondicionalmente si el secret es el por defecto.
* **Solución Técnica**: Eliminar el bypass en modo producción y exigir secreto configurado.

#### [M-06] Evolución DDL (`add_column`) no emite señal SSE provocando desincronización de esquemas
* **Ubicación Exacta**: [`crates/server/src/api/control_plane.rs:67-85`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/control_plane.rs#L67-L85), [`crates/server/src/api/sse.rs:36-50`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/sse.rs#L36-L50).
* **Causa Raíz**: Recarga el esquema en el actor pero no notifica a clientes conectados por el canal SSE.
* **Solución Técnica**: Emitir `RoomEvent::SchemaReloaded` vía broadcast SSE para que los clientes actualicen su esquema.

#### [M-07] Omisión de validación de `room_id` en URL path vs payload binario (bypass de gateway)
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:148-583`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L148-L583).
* **Causa Raíz**: Handlers extraen `room_id_str` del path pero enrutan según el payload Bincode interno.
* **Solución Técnica**: Validar `if room_id.as_str() != room_id_str` antes de despachar el comando.

#### [M-08] Ausencia de timeouts perimetrales en llamadas `sender.send` y `rx.await` hacia actores
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:90-204`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L90-L204).
* **Causa Raíz**: Peticiones HTTP esperan indefinidamente la respuesta del buzón del actor.
* **Solución Técnica**: Envolver las operaciones en `tokio::time::timeout(Duration::from_secs(5), ...)`.

#### [M-09] `max_batch_size` en `/sync` sin límite superior permite decodificación masiva abusiva (DoS)
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:307-346`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L307-L346).
* **Causa Raíz**: El cliente puede enviar `max_batch_size: u32::MAX`, saturando memoria en deserialización.
* **Solución Técnica**: Acotar en el servidor: `let limit = max_batch_size.clamp(1, 1000) as usize;`.

#### [M-10] Avance prematuro de `tail_seq` en `prune_older_than` induce `BehindCompaction` espurio
* **Ubicación Exacta**: [`crates/server/src/log/tiered_log.rs:376-378`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs#L376-L378).
* **Causa Raíz**: Se sobreescribe `self.tail_seq = target_seq` sin comprobar si `active.wal` conserva deltas previos.
* **Solución Técnica**: Asignar `tail_seq` a partir de la secuencia real más baja físicamente disponible en disco.

#### [M-11] Doble fsync por operación sin Group Commit ni batching en escrituras del servidor
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:264-279`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L264-L279).
* **Causa Raíz**: Cada commit ejecuta dos `sync_data()` síncronos independientes a disco.
* **Solución Técnica**: Implementar Group Commit o persistencia en un solo log coordinado.

#### [M-12] Ausencia de cerrojos multi-proceso (`flock`) sobre WALs del servidor
* **Ubicación Exacta**: [`crates/server/src/micro_wal.rs:51-57`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/micro_wal.rs#L51-L57), [`crates/server/src/log/warm_disk.rs:50-56`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/warm_disk.rs#L50-L56).
* **Causa Raíz**: Los archivos se abren sin `fs2::FileExt::try_lock_exclusive`.
* **Solución Técnica**: Solicitar cerrojo exclusivo de kernel al abrir los archivos de la sala.

#### [M-13] Contrato `StorageEngine` exige `table: &str` en `get`/`scan` forzando búsquedas por string
* **Ubicación Exacta**: [`crates/storage/src/engine.rs:49-64`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/engine.rs#L49-L64).
* **Causa Raíz**: Inconsistencia con `apply_batch` que opera por `table_id: u16`.
* **Solución Técnica**: Sobrecargar o refactorizar la interfaz para aceptar `table_id: u16`.

#### [M-14] Sobrescritura silenciosa de tablas con igual nombre y overflow en asignación de IDs
* **Ubicación Exacta**: [`crates/core/src/schema/global.rs:20-31`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/global.rs#L20-L31).
* **Causa Raíz**: `add_table` inserta ciegamente sin retornar `Result` ante duplicados.
* **Solución Técnica**: Validar colisiones y retornar `Result<u16, ValidationError>`.

#### [M-15] Fuga de encapsulamiento e implementación impropia de `Deref` en `PrimaryKey` y `CompactRow`
* **Ubicación Exacta**: [`crates/core/src/value/row.rs:11`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/value/row.rs#L11), [`crates/core/src/value/row.rs:70`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/value/row.rs#L70).
* **Causa Raíz**: Campos públicos (`pub SmallVec`, `pub values`) y `Deref` que violan [C-DEREF].
* **Solución Técnica**: Ocultar campos como privados y eliminar `Deref` exponiendo getters inmutables.

#### [M-16] Retención de cerrojo global del motor durante I/O de sala en `close_room`
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs:212-221`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L212-L221).
* **Causa Raíz**: Mantiene `self.rooms.write().await` mientras ejecuta `room.wal_file.sync_all().await`.
* **Solución Técnica**: Retirar el `Arc` del mapa, liberar el cerrojo global y ejecutar el flush en el `Arc` aislado.

---

### DEFECTOS DE SEVERIDAD BAJA

#### [B-01] Discrepancia de especificación: `ClientMessage::Heartbeat` carece de campo `last_ack_seq`
* **Ubicación Exacta**: [`crates/core/src/protocol/messages.rs:75-79`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L75-L79).
* **Causa Raíz**: Divergencia frente a ARCHITECTURE.md que estipulaba ACK coalescido en heartbeats.
* **Solución Técnica**: Agregar `last_ack_seq: SequenceNumber` o sincronizar la documentación.

#### [B-02] Tipos fundamentales de mutación y mensajería omiten traits estándar `Eq` y `Hash`
* **Ubicación Exacta**: [`crates/core/src/mutation/op.rs:25-50`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/op.rs#L25-L50), [`crates/core/src/protocol/messages.rs:27-49`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L27-L49).
* **Causa Raíz**: Derivan solo `PartialEq`, impidiendo su uso en `HashSet` o como claves de mapas.
* **Solución Técnica**: Añadir `#[derive(Eq, Hash)]` en estructuras canónicas.

#### [B-03] Handlers de Data Plane devuelven texto plano o JSON ante errores en vez de Bincode
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:23-26`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L23-L26).
* **Causa Raíz**: `ServerError::into_response` emite JSON en rutas que esperan exclusivamente tramas binarias.
* **Solución Técnica**: Retornar siempre tramas serializadas con `ServerMessage::Error`.

#### [B-04] `GET /admin/rooms/:id` produce efectos colaterales activando actores de sala en memoria
* **Ubicación Exacta**: [`crates/server/src/api/control_plane.rs:100-117`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/control_plane.rs#L100-L117).
* **Causa Raíz**: Endpoint de lectura invoca `get_or_spawn` en lugar de consultar estado pasivo.
* **Solución Técnica**: Consultar únicamente memoria o metadata en disco sin levantar tareas activas.

---

*Fin del Inventario Exhaustivo de Defectos — RimDB (Post-Fase 3).*
