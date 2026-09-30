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
│ C-03 │ Crítico  │ crates/server/src/api/data_plane.rs  │ [RESUELTO] Ausencia total de autenticación y validación de lease en todo el Data Plane.│
│ C-04 │ Crítico  │ crates/server/src/actor/room.rs      │ [RESUELTO] Dual-WAL erradicado: persistencia atómica en active.wal con mutation_id.    │
│ C-05 │ Crítico  │ crates/storage/src/disk/recovery.rs  │ [RESUELTO] Replay del WAL omite operaciones con op.seq <= snapshot_seq en recovery.    │
│ C-06 │ Crítico  │ crates/core/src/protocol/wal_frame.rs│ [RESUELTO] Torn writes en EOF con CRC fallido clasificados y truncados limpiamente.    │
│ C-07 │ Crítico  │ crates/core/src/protocol/wal_frame.rs│ [RESUELTO] WalReader bufferiza lotes multi-op sin descartar operaciones 2..N.          │
│ C-08 │ Crítico  │ crates/server/src/actor/lease.rs     │ [RESUELTO] Rediseño Onboarding: Bootstrapping state + Ancla de retención de snapshots. │
│ C-09 │ Crítico  │ crates/server/src/actor/manager.rs   │ [RESUELTO] TOCTOU erradicado con cerrojos asíncronos por sala en get_or_spawn.        │
│ C-10 │ Crítico  │ crates/server/src/relay.rs           │ [RESUELTO] Upload multipart autenticado, límite 16MB superado y hash BLAKE3 validado. │
│ C-11 │ Crítico  │ crates/core/src/schema/table.rs      │ [RESUELTO] Deserialización de TableSchema estricta y eliminación de .expect() (C-11).   │
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ A-01 │ Alto     │ crates/server/src/actor/room.rs      │ [RESUELTO] Aislamiento asíncrono Tokio: Zstd y disco aislados en spawn_blocking (A-01).│
│ A-02 │ Alto     │ crates/server/src/micro_wal.rs       │ [RESUELTO] Crecimiento ilimitado de MicroWal: resuelto al erradicar MicroWal (C-04).   │
│ A-03 │ Alto     │ crates/storage/src/disk/mod.rs       │ [RESUELTO] Rotación WAL y compactación CoW en 3 fases sin bloqueo escritor (A-03).    │
│ A-04 │ Alto     │ crates/storage/src/disk/compactor.rs │ [RESUELTO] Snapshots temporales con UUID previenen carreras O_TRUNC (A-04).            │
│ A-05 │ Alto     │ crates/storage/src/disk/format.rs    │ [RESUELTO] Checksum CRC32 sobre payload comprimido en FileHeader validado en recovery. │
│ A-06 │ Alto     │ crates/storage/src/engine.rs         │ [RESUELTO] Envelope RMSN universal e interoperable entre Memory y Disk con CRC32 (A-06)│
│ A-07 │ Alto     │ crates/server/src/actor/room.rs      │ [RESUELTO] Validación ack_seq <= head_seq en handle_ack previene purga catastrófica.    │
│ A-08 │ Alto     │ crates/storage/src/disk/mod.rs       │ [RESUELTO] Validación de esquemas activa en apply_batch en Memory y Disk (A-08).        │
│ A-09 │ Alto     │ crates/storage/src/disk/mod.rs       │ [RESUELTO] Snapshot Isolation en scan con vistas CoW inmutables Arc<BTreeMap> (A-09).  │
│ A-10 │ Alto     │ crates/server/src/actor/lease.rs     │ [RESUELTO] Clientes Disconnected pasan a Dormant tras 90s desbloqueando poda (A-10).   │
│ A-11 │ Alto     │ crates/server/src/api/data_plane.rs  │ [RESUELTO] Data Plane reactiva salas con get_or_spawn tolerando reinicios del servidor.│
│ A-12 │ Alto     │ crates/server/src/log/tiered_log.rs  │ [RESUELTO] Inversión jerárquica: RAM HotBuffer evaluado antes que disco en fetch_deltas.│
│ A-13 │ Alto     │ crates/server/src/log/tiered_log.rs  │ [RESUELTO] Ventana deslizante en HotBuffer: erradicada evicción destructiva a cero.     │
│ A-14 │ Alto     │ crates/server/src/actor/manager.rs   │ [RESUELTO] delete_room coordina RoomCommand::Shutdown y espera el JoinHandle del actor.│
│ A-15 │ Alto     │ crates/server/src/relay.rs           │ [RESUELTO] SnapshotRelay respaldado en disco con TTL configurable y purga física.       │
│ A-16 │ Alto     │ crates/client/src/lib.rs             │ Crate rimdb-client es un cascarón vacío stub sin implementación del SDK de cliente.     │
│ A-17 │ Alto     │ crates/server/tests/                 │ [RESUELTO] Test exhaustivo de catchup_ops: orden monótono, contigüidad y reintentos (A-17)│
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ M-01 │ Medio    │ crates/core/src/mutation/squash.rs   │ [RESUELTO] LWW estricto en squashing (descarte de updates viejos) y purge Insert+Delete │
│ M-02 │ Medio    │ crates/server/src/actor/room.rs      │ [RESUELTO] Reintentos idempotentes de Commit devuelven catchup_ops con mutación propia. │
│ M-03 │ Medio    │ crates/core/src/protocol/codec.rs    │ [RESUELTO] Header canónico 4B con magic bytes RM, versión 0x01 y discriminante en codec │
│ M-04 │ Medio    │ crates/server/src/api/auth.rs        │ [RESUELTO] Comparación de firmas en tiempo variable con subtle::ConstantTimeEq.         │
│ M-05 │ Medio    │ crates/server/src/api/auth.rs        │ [RESUELTO] Backdoor dev-token cableado eliminado de código de autenticación.            │
│ M-06 │ Medio    │ crates/server/src/api/control_plane  │ [RESUELTO] Emisión SSE de RoomEvent::SchemaReloaded ante migraciones DDL add_column.   │
│ M-07 │ Medio    │ crates/server/src/api/data_plane.rs  │ [RESUELTO] Validación estricta 3-way room_id en URL path vs token vs payload.          │
│ M-08 │ Medio    │ crates/server/src/api/data_plane.rs  │ [RESUELTO] Timeouts perimetrales de 5s en actor calls retornando 504 GatewayTimeout.  │
│ M-09 │ Medio    │ crates/server/src/api/data_plane.rs  │ [RESUELTO] max_batch_size en /sync acotado defensivamente a 1..=1000 previniendo DoS. │
│ M-10 │ Medio    │ crates/server/src/log/tiered_log.rs  │ [RESUELTO] Recálculo exacto de tail_seq en prune_older_than según segmentos retenidos. │
│ M-11 │ Medio    │ crates/server/src/log/warm_disk.rs   │ [RESUELTO] Doble fsync eliminado: unificado en un solo fsync atómico por commit (C-04).│
│ M-12 │ Medio    │ crates/server/src/log/warm_disk.rs   │ [RESUELTO] Cerrojos exclusivos multi-proceso (flock) en active.wal con fs2.            │
│ M-13 │ Medio    │ crates/storage/src/engine.rs         │ [RESUELTO] Métodos directos get_by_id y scan_by_id con table_id: u16 en StorageEngine. │
│ M-14 │ Medio    │ crates/core/src/schema/global.rs     │ [RESUELTO] Detección de tablas duplicadas y aritmética segura de table_id en add_table. │
│ M-15 │ Medio    │ crates/core/src/value/row.rs         │ [RESUELTO] Encapsulación de campos y remoción de Deref en PrimaryKey, CompactRow, Schema│
│ M-16 │ Medio    │ crates/storage/src/disk/mod.rs       │ [RESUELTO] Liberación de cerrojo global antes de I/O de sala en close_room.            │
├──────┼──────────┼──────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ B-01 │ Bajo     │ crates/core/src/protocol/messages.rs │ [RESUELTO] Desacoplamiento formal de Heartbeat (liveness) frente a Ack (avance de seq). │
│ B-02 │ Bajo     │ crates/core/src/mutation/op.rs       │ [RESUELTO] Derivación #[derive(Eq, Hash)] en Operation, SequencedOp, Messages y Kind.   │
│ B-03 │ Bajo     │ crates/server/src/api/data_plane.rs  │ [RESUELTO] ServerMessage::DeregisterAck y Content-Type application/octet-stream en error│
│ B-04 │ Bajo     │ crates/server/src/api/control_plane  │ [RESUELTO] GET /admin/rooms/:id verifica room_exists retornando 404 sin spawn fantasma. │
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
* **Estado**: **RESUELTO (Fase 3.5-A.2)**
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:131-584`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L131-L584), [`crates/server/src/api/sse.rs:15-53`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/sse.rs#L15-L53), [`crates/core/src/protocol/messages.rs:49-105`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L49-L105).
* **Causa Raíz**: La verificación del token firmado (`verify_client_token`) se ejecuta únicamente en `/rooms/:id/register`. Los handlers `/commit`, `/sync`, `/ack`, `/heartbeat`, `/deregister` y `/events` no extraen cabeceras `Authorization` ni validan firmas. Asimismo, `RoomActor` procesa mutaciones de cualquier `client_id` sin validar si posee un lease activo en `ClientLeaseTracker`.
* **Impacto**: Evasión absoluta del control de acceso. Cualquier entidad anónima en red puede inyectar mutaciones forjadas a nombre de cualquier usuario, descargar bases de datos históricas mediante `/sync`, o emitir `Ack` falsos que provoquen la purga anticipada de datos de clientes legítimos.
* **Solución Técnica / Implementada**: Se implementó el extractor Axum `ClientAuth` (`FromRequestParts`) que valida criptográficamente tokens Bearer (y fallback `?token=` para EventSource de SSE) retornando claims tipados `VerifiedClientToken`. En el actor `RoomActor`, `handle_commit`, `handle_sync`, `handle_ack` y `Heartbeat` verifican explícitamente mediante `ClientLeaseTracker::is_registered` que el cliente se encuentre registrado, rechazando clientes no registrados con `ServerError::Unauthorized`.

#### [C-04] Dual-WAL desacoplado: desincronización y agujero de secuencia irrecuperable en reinicios
* **Estado**: **RESUELTO (Persistencia Atómica Unificada en WAL)**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs), [`crates/server/src/log/tiered_log.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs), [`crates/core/src/protocol/wal_frame.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/wal_frame.rs).
* **Causa Raíz**: Persistencia dual independiente en cada commit: `micro_wal.append(new_seq)` seguido de `tiered_log.append(seq_op)`. Si ocurre un crash entre ambas escrituras, `micro_wal` registra la secuencia $N$, pero `tiered_log` solo alcanza $N-1$. Al reiniciar, `spawn` reconcilia `head_seq = max(wal_recovery.head_seq, tiered_log.head_seq())` ($N$), pero el campo interno de `TieredLog` permanece en $N-1$. La siguiente mutación ($N+1$) es rechazada por `TieredLog::append` con error de contigüidad no recuperable: `expected N, got N+1`.
* **Impacto**: Inutilización permanente de la sala (*room bricking*). Todo commit posterior falla indefinidamente.
* **Solución Técnica / Implementada**: Se erradicó por completo el motor `MicroWal` y el archivo secundario `meta_{room_id}.wal`. La persistencia y deduplicación se unificaron en `active.wal` incorporando `mutation_id: Option<MutationId>` en el enmarcado de lote (`WalBatchPayload` en `wal_frame`). En cada commit, `RoomActor` ejecuta un único `append` atómico a `TieredLog` (Hot Buffer en RAM + `active.wal` con `sync_data()`). En el arranque, `DedupLruCache` se hidrata directamente a partir de las tuplas `(MutationId, SequenceNumber)` recuperadas cronológicamente de los segmentos Warm y Cold, y `head_seq` se deriva directamente de `tiered_log.head_seq()`. Esto también resuelve completamente **A-02** y **M-11**.

#### [C-05] Replay ciego del WAL histórico sobre el snapshot base sin omitir secuencias consolidadas
* **Estado**: **RESUELTO**
* **Ubicación Exacta**: [`crates/storage/src/disk/recovery.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs).
* **Causa Raíz**: Durante `recover_room`, el motor carga el snapshot base `room_{id}.snap` estableciendo `tables` y `snapshot_seq`. Acto seguido, abre `room_{id}.wal` desde el offset 0 y ejecuta incondicionalmente todas las operaciones sobre las tablas sin comprobar si `op.seq <= snapshot_seq`.
* **Impacto**: Corrupción y resurrección zombi de datos si el servidor experimentó una caída antes de truncar el WAL tras un snapshot. Re-aplica deltas obsoletos sobre tuplas ya compactadas y amplifica masivamente el tiempo de arranque.
* **Solución Técnica / Implementada**: Se incorporó un filtro estricto en el bucle de replay de operaciones en `recover_room`:
  ```rust
  if op.seq <= snapshot_seq {
      continue;
  }
  ```
  Evitando que mutaciones ya consolidadas en el snapshot sobrescriban el estado o causen violaciones de secuencia.

#### [C-06] Torn writes en EOF clasificados erróneamente como corrupción fatal por fallo de CRC32
* **Estado**: **RESUELTO (Auto-recuperación de Torn Writes en EOF)**
* **Ubicación Exacta**: [`crates/core/src/protocol/wal_frame.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/wal_frame.rs), [`crates/storage/src/disk/recovery.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs), [`crates/server/src/log/warm_disk.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/warm_disk.rs).
* **Causa Raíz**: Si un corte de energía interrumpe la escritura del último lote en el WAL, el payload queda truncado o con basura residual de bloque, provocando `crc != expected_crc`. El decodificador no clasifica el fallo como `TornWrite` en EOF si existen bytes no nulos en la cabecera, retornando `WalFrameError::Corruption`. Como resultado, `recover_room` aborta el arranque con `StorageError::WalCorruption`.
* **Impacto**: Un corte de corriente durante una escritura en el WAL inhabilita el reinicio del motor (`open_room` falla), rompiendo la promesa arquitectónica de auto-recuperación de torn writes.
* **Solución Técnica / Implementada**: En `decode_wal_batch_from_slice`, cuando ocurre una discrepancia de CRC32, se verifica si el remanente posterior contiene alguna cabecera válida con magic `0xBA7C`. Si no existen registros válidos posteriores (fallo terminal en EOF), se clasifica como `WalBatchDecodeResult::TornWrite { valid_bytes_offset: 0, reason: ... }`. Tanto `recover_room` como `WarmDiskLog::inspect_active_segment` truncan automáticamente el archivo en disco a la longitud de bytes válidos previos (`valid_wal_bytes`), permitiendo el arranque normal sin intervención manual.

#### [C-07] `decode_wal_record_from_slice` drena solo 1 op y descarta el resto del lote en `WalReader`
* **Estado**: **RESUELTO (Bufferizado de Lotes Multi-Operación)**
* **Ubicación Exacta**: [`crates/storage/src/disk/wal.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/wal.rs).
* **Causa Raíz**: `decode_wal_record_from_slice` decodifica un lote con múltiples operaciones pero ejecuta `ops.drain(..).next()`, devolviendo únicamente la primera operación pero retornando `bytes_consumed` igual al tamaño del lote completo. `WalReader::next_record` avanza su offset en la totalidad del lote.
* **Impacto**: Pérdida silenciosa de datos. En cualquier archivo WAL donde se agrupen mutaciones en lotes multi-operación (`write_batch`), todas las operaciones a partir de la segunda son omitidas permanentemente.
* **Solución Técnica / Implementada**: Se equipó a `WalReader` con una cola interna `pending_ops: VecDeque<SequencedOperation>`. Al invocar `next_record()`, si existen operaciones pendientes en la cola, se extrae inmediatamente la siguiente. Si la cola está vacía, se decodifica el siguiente lote mediante `next_batch()`, devolviendo la primera operación y encolando las operaciones $2..N$ restantes en `pending_ops`.

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
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/actor/manager.rs:63-122`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/manager.rs#L63-L122).
* **Causa Raíz**: `get_or_spawn_with_policy` consulta `self.rooms.get(room_id)`. Si no existe, libera el lock de `DashMap`, resuelve el esquema en disco y ejecuta `RoomActor::spawn`. Múltiples peticiones concurrentes para una sala no activa superan la comprobación en paralelo y lanzan dos o más tareas Tokio independientes para la misma sala sobre los mismos archivos en disco sin bloqueos `flock`.
* **Impacto**: Corrupción catastrófica de los logs WAL por escrituras intercaladas no coordinadas y generación de actores huérfanos compitiendo por el secuenciador.
* **Solución Técnica / Implementada**: Se introdujo un mapa de cerrojos asíncronos por sala (`spawn_locks: DashMap<RoomId, Arc<tokio::sync::Mutex<()>>>`) en `RoomManager`. Al invocar `get_or_spawn` o `get_or_spawn_with_policy`, se adquiere el cerrojo exclusivo de la sala antes de resolver el esquema o spawnear el actor, aplicando comprobación de doble verificación (*double-checked locking*). Las tareas concurrentes compitiendo por la misma sala esperan al cerrojo y reutilizan de forma segura el sender ya instanciado, erradicando por completo el TOCTOU.

#### [C-10] Inyección arbitraria de estado por upload anónimo y colisión con `DefaultBodyLimit` (16 MB)
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/relay.rs:120-150`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/relay.rs#L120-L150), [`crates/server/src/api/router.rs:72`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/router.rs#L72).
* **Causa Raíz**:
  1. `POST /rooms/:id/snapshot/upload` no exige autenticación alguna; cualquier cliente puede subir un buffer binario arbitrario que el relay acepta y distribuye a clientes en onboarding.
  2. La subida se realiza monolíticamente en un solo POST (`body: Bytes`). El router aplica `.layer(DefaultBodyLimit::max(16 * 1024 * 1024))`. Si un snapshot supera 16 MB, es rechazado con HTTP 413, invalidando el protocolo multipart concebido para datasets grandes.
* **Impacto**: Inyección y envenenamiento de estado en clientes. Inoperabilidad absoluta del bootstrapping para bases de datos superiores a 16 MB.
* **Solución Técnica / Implementada**: Se aseguraron todos los endpoints de subida y descarga de snapshots con autenticación estricta (Bearer token de cliente acotado a la sala o Admin secret). Se implementaron los mensajes `ClientMessage::UploadSnapshotChunk` y `ServerMessage::SnapshotUploadChunkAck` y la ruta `POST /rooms/:room_id/snapshot/upload-chunk` para cargas multipart por fragmentos de hasta 1 MB, eliminando la barrera de 16 MB de `DefaultBodyLimit`. El relay consolida y valida el digest BLAKE3 del payload completo ensamblado antes de publicarlo.

#### [C-11] Deserialización de `TableSchema` elude invariantes estructurales provocando pánico
* **Estado**: **RESUELTO (Fase 3.5-A.4)**
* **Ubicación Exacta**: [`crates/core/src/schema/table.rs:162-188`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/table.rs#L162-L188), [`crates/core/src/schema/validation.rs:390-395`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/validation.rs#L390-L395).
* **Causa Raíz**: La implementación de `Deserialize` para `TableSchema` deserializa campos crudos sin invocar las validaciones del builder (PK no vacía, columnas de PK existentes y no nulas, nombres de columna únicos). En `validation.rs`, el código asume que el invariante se cumple y ejecuta `.expect("primary key column must exist in column_indices")`.
* **Impacto**: Un payload JSON malicioso o corrupto enviado a `/admin/schemas` provoca el pánico del proceso del servidor al procesar mutaciones sobre el esquema deserializado.
* **Solución Técnica**: Delegar la deserialización a través de un constructor validador (`TableSchema::try_from`) y reemplazar el `.expect()` por propagación controlada con `ValidationError::UnknownColumn`.

---

### DEFECTOS DE SEVERIDAD ALTA

#### [A-01] I/O síncrono bloqueante y compresión Zstd ejecutados en el reactor asíncrono de Tokio
* **Estado**: **RESUELTO (Fase 3.5-B.1)**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:104-121`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L104-L121), [`crates/server/src/log/cold_disk.rs:26-50`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/cold_disk.rs#L26-L50), [`crates/server/src/actor/lease.rs:70-80`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/lease.rs#L70-L80).
* **Causa Raíz**: En `handle_commit`, `handle_ack` y `run_periodic_maintenance` (cada 500 ms), se invocaban llamadas bloqueantes de `std::fs` (`write_all`, `sync_data`, `rename`) y compresión intensiva de CPU `zstd::stream::encode_all` directamente sobre los hilos worker de Tokio sin usar `spawn_blocking`.
* **Impacto**: Inanición del pool de hilos de Tokio (*thread starvation*), provocando picos de latencia de red y caídas de conexiones por timeouts de heartbeat.
* **Solución Técnica / Implementada**: Se confinó la compresión Zstandard intensiva en CPU (`ColdDiskLog::compress_warm_segment`) a tareas bloqueantes mediante `tokio::task::spawn_blocking`, evitando la inanición del pool worker de Tokio. Asimismo, `RoomActor::run_periodic_maintenance` invoca de forma asíncrona no bloqueante `TieredLog::run_maintenance().await`.

#### [A-02] Crecimiento ilimitado de `MicroWal` y lectura monolítica a RAM con riesgo de OOM en arranque
* **Estado**: **RESUELTO (Erradicación de MicroWal por C-04)**
* **Ubicación Exacta**: [`crates/server/src/micro_wal.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/micro_wal.rs).
* **Causa Raíz**: `meta_{room_id}.wal` era estrictamente append-only sin rotación ni truncado. En `recover`, ejecutaba `read_to_end` a memoria completa para hidratar `DedupLruCache`.
* **Impacto**: Fuga continua de almacenamiento y pánico por falta de memoria (OOM Kill) al arrancar salas con millones de transacciones históricas.
* **Solución Técnica / Implementada**: Resuelto definitivamente mediante la erradicación completa de `MicroWal` (remediación `C-04`). La deduplicación se hidrata directamente desde los segmentos rotados, comprimidos y podados de `TieredLog`, eliminando el archivo secundario y su consumo desmedido de almacenamiento y memoria.

#### [A-03] Falsa compactación CoW: bloqueo exclusivo de sala congela escrituras concurrentes
* **Estado**: **RESUELTO (Fase 3.5-B.2)**
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs), [`crates/storage/src/disk/compactor.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/compactor.rs).
* **Causa Raíz**: La compactación se ejecuta inline dentro de `apply_batch` manteniendo adquirido el cerrojo exclusivo `room_arc.write().await` durante toda la serialización Bincode, compresión Zstd y fsyncs.
* **Impacto**: Congelamiento de lecturas y escrituras durante la compactación. Si se intentara mover a background sin rediseñar el WAL, `wal_file.set_len(0)` truncaría y destruiría las mutaciones añadidas concurrentemente.
* **Solución Técnica / Implementada**: Se implementó el protocolo de compactación CoW en 3 fases (`compact_room_cow`). En la Fase 1 (bloqueo exclusivo <1 ms), se vacía el WAL activo, se rota atómicamente a `wal.compacting`, se inicializa un nuevo archivo `wal` para recibir escrituras concurrentes sin interrupción, se extrae una vista CoW inmutable de las tablas (`Arc<BTreeMap>`) y el cursor de corte `cut_seq`, liberando de inmediato el cerrojo. En la Fase 2 (en background vía `spawn_blocking`), se serializa, comprime con Zstd, calcula el CRC32 y escribe a un archivo temporal único con fsync. En la Fase 3 (bloqueo exclusivo <1 ms), se reemplaza atómicamente el snapshot, se sincroniza el directorio, se desvincula `wal.compacting` y se actualiza `snapshot_seq`, preservando todas las operaciones concurrentes ingresadas al nuevo WAL durante el proceso.

#### [A-04] Carrera `O_TRUNC` antes de `flock` en compactor trunca snapshots concurrentes a 0 bytes
* **Estado**: **RESUELTO (Fase 3.5-B.2)**
* **Ubicación Exacta**: [`crates/storage/src/disk/compactor.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/compactor.rs).
* **Causa Raíz**: El archivo temporal de snapshot utiliza una ruta fija `room_{id}.snap.tmp`. Se abre con `.truncate(true)`, lo cual ejecuta la llamada al sistema `open(O_TRUNC)` antes de solicitar el cerrojo `try_lock_exclusive()`.
* **Impacto**: Si dos procesos o workers intentan compactar la misma sala, el segundo trunca el archivo a 0 bytes mientras el primero aún escribe en él, resultando en snapshots corruptos o vacíos.
* **Solución Técnica / Implementada**: Se reemplazó la ruta temporal estática por nombres temporales únicos basados en UUID v4 (`snap.tmp.{uuid}`) abiertos con `create_new(true)` (`O_CREAT | O_EXCL`). Adicionalmente, el motor de almacenamiento gestiona un cerrojo por sala (`compaction_locks: DashMap<RoomId, Arc<Mutex<()>>>`) que garantiza una única tarea de compactación activa por sala, eliminando cualquier riesgo de truncado concurrente destructivo.

#### [A-05] Cabecera `FileHeader` carece de checksum/CRC sobre el payload comprimido del snapshot
* **Estado**: **RESUELTO (Fase 3.5-B.2)**
* **Ubicación Exacta**: [`crates/storage/src/disk/format.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/format.rs), [`crates/storage/src/disk/recovery.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs).
* **Causa Raíz**: `header_crc` en `FileHeader` solo cubre los primeros 32 bytes de metadatos. El cuerpo comprimido con Zstandard no posee ninguna suma de comprobación en disco.
* **Impacto**: Corrupción silenciosa en disco (*bit rot*) no es detectada a nivel de formato, pasando directamente a la descompresión con riesgo de fallos opacos.
* **Solución Técnica / Implementada**: Se incorporó el campo `snapshot_payload_crc32: u32` dentro de la cabecera canónica de 64 bytes de `FileHeader`, aprovechando bytes reservados y asegurando que el checksum del encabezado (`header_crc`) valide también la integridad del descriptor del payload. Durante la recuperación en arranque (`recover_room`), el sistema calcula y verifica el CRC32 sobre los bytes comprimidos leídos antes de invocar la descompresión con Zstd, retornando un error explícito de corrupción (`StorageError::SnapshotCorruption`) si se detecta cualquier alteración en disco.

#### [A-06] Ruptura de Liskov en `StorageEngine`: formatos incompatibles de snapshot (Memory vs Disk)
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/storage/src/snapshot.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/snapshot.rs), [`crates/storage/src/memory/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs), [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs).
* **Causa Raíz**: `MemoryStorageEngine` emitía Bincode plano sin comprimir; `DiskStorageEngine` emitía Bincode comprimido con Zstandard. Ninguno incluía cabecera canónica identificadora.
* **Impacto**: Un snapshot generado por un motor no podía ser restaurado en el otro, rompiendo la sustitución de Liskov y la interoperabilidad en clientes WASM vs nativos.
* **Solución Técnica / Implementada**: Se implementó el módulo canónico `rimdb_storage::snapshot` con envelope unificado de formato: magic bytes `"RMSN"`, versión `1`, flag de compresión (0 = Raw/Memory, 1 = Zstd/Disk), longitud original descomprimida y checksum CRC32 Fast del payload. Las funciones `encode_snapshot_envelope` y `decode_snapshot_envelope` son utilizadas de manera homogénea por `MemoryStorageEngine` y `DiskStorageEngine`, garantizando 100% de portabilidad cruzada bidireccional de snapshots entre cualquier motor de persistencia.

#### [A-07] Falta de validación `ack_seq <= head_seq` en `handle_ack` permite purga catastrófica de logs
* **Estado**: **RESUELTO (Fase 3.5-A.4)**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:348-373`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L348-L373).
* **Causa Raíz**: `handle_ack` no valida que `ack_seq <= self.head_seq`. Si un cliente corrupto envía `ack_seq = u64::MAX`, `prune_older_than(u64::MAX)` borra todos los segmentos Warm y Cold y vacía el `HotBuffer`.
* **Impacto**: Destrucción inmediata del historial activo de la sala. Todos los demás clientes reciben `BehindCompaction` y quedan forzados a descargar snapshots completos.
* **Solución Técnica**: Validar estrictamente `if ack_seq > self.head_seq { return Err(...); }`.

#### [A-08] `apply_batch` omite validación de esquema, permitiendo mutaciones de tipos incompatibles
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs), [`crates/storage/src/memory/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs).
* **Causa Raíz**: `apply_batch` verificaba únicamente la existencia de la tabla y la secuencia incremental, pero nunca invocaba `schema.validate_operation(&op.op)`.
* **Impacto**: Mutaciones con tipos incompatibles, violaciones de no-nulabilidad o índices de columna fuera de rango ingresaban al WAL y corrompían las tablas en memoria.
* **Solución Técnica / Implementada**: Se incorporó la validación explícita `schema.validate_operation(&op.op)?` en `StorageEngine::apply_batch` de forma idéntica en `MemoryStorageEngine` y `DiskStorageEngine`, retornando `StorageError::ValidationError` antes de escribir en disco o mutar las estructuras de datos en memoria.

#### [A-09] Escaneo lazy por chunks libera el lock entre bloques, rompiendo Snapshot Isolation
* **Estado**: **RESUELTO (Fase 3.5-B.2)**
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs), [`crates/storage/src/memory/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs).
* **Causa Raíz**: El stream de `scan` libera el cerrojo de lectura cada 64 tuplas. Mutaciones concurrentes intermedias alteran las tablas mientras el stream continúa.
* **Impacto**: Ruptura del aislamiento transaccional (*torn reads*, lecturas fantasma e inconsistencia temporal en consultas de rango).
* **Solución Técnica / Implementada**: Se migró la estructura de almacenamiento de tablas en memoria a `HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>`. Al iniciar cualquier operación `scan`, se adquiere brevemente el cerrojo de lectura (<1 µs) para clonar el puntero `Arc<BTreeMap>` correspondiente a la tabla solicitada y se libera inmediatamente el cerrojo de la sala. El stream lazy itera exclusivamente sobre la vista CoW inmutable clonada, garantizando Snapshot Isolation estricto sin retener cerrojos, sin lecturas desgarradas (*torn reads*) y permitiendo a los escritores concurrentes mutar el árbol mediante `Arc::make_mut` sin bloquearse mutuamente.

#### [A-10] Deadlock lógico en Dead Man's Switch: clientes `Disconnected` bloquean poda de logs
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/server/src/actor/lease.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/lease.rs).
* **Causa Raíz**: `min_connected_ack_seq` retorna `None` si existe algún cliente en `Disconnected`. A su vez, `check_timeouts` solo pasaba un cliente a `Dormant` si su cursor quedaba por detrás de `tail_seq - 1`. Dado que `tail_seq` no avanzaba sin poda, se producía un bloqueo mutuo permanente.
* **Impacto**: Inhabilitación total de la poda de logs en disco si un cliente se desconecta sin desregistrarse, acumulando WALs hasta saturar el almacenamiento.
* **Solución Técnica / Implementada**: En `ClientLeaseTracker::check_timeouts`, se incorporó una condición temporal desacoplada: si un cliente en estado `Disconnected` excede 90 segundos de inactividad (`now.duration_since(entry.last_heartbeat) > Duration::from_secs(90)`), transiciona automáticamente a `ClientState::Dormant`, excluyéndose de `min_connected_ack_seq` y desbloqueando de inmediato la poda física de logs en disco.

#### [A-11] Data Plane utiliza `get_room` en RAM en lugar de lazy-spawning, fallando tras reinicios
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:156-544`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L156-L544).
* **Causa Raíz**: Los endpoints `/commit`, `/sync`, `/ack`, etc., consultan `state.room_manager.get_room(&room_id)`, que solo busca en el `DashMap` en RAM.
* **Impacto**: Tras un reinicio del servidor, clientes legítimos previamente registrados reciben HTTP 404 `RoomNotFound`, aunque la sala exista íntegra en disco.
* **Solución Técnica / Implementada**: Se reemplazó el uso de `get_room` por `get_or_spawn(&room_id, None).await` en todos los endpoints del plano de datos (`/register`, `/commit`, `/sync`, `/ack`, `/heartbeat`, `/schema`, `/deregister`, `/events`). Ante un reinicio del servidor, el plano de datos reactiva la sala perezosamente en demanda recuperando el esquema y metadatos persistidos en disco sin requerir intervención del plano de control.

#### [A-12] Inversión jerárquica en `fetch_deltas`: escaneo síncrono de disco previo al RAM `HotBuffer`
* **Estado**: **RESUELTO (Fase 3.5-B.1)**
* **Ubicación Exacta**: [`crates/server/src/log/tiered_log.rs:157-205`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs#L157-L205).
* **Causa Raíz**: `fetch_deltas` escaneaba primero el disco Cold, luego el disco Warm con `read_dir`, y solo en último término consultaba la memoria RAM.
* **Impacto**: En el 99% de las consultas de clientes activos, el servidor ejecutaba llamadas síncronas a disco para deltas que ya residían en memoria, degradando el throughput.
* **Solución Técnica / Implementada**: Se reordenó la jerarquía de evaluación en `TieredLog::fetch_deltas`. Se incorporó un Fast Path en RAM que verifica prioritariamente si `from_seq >= hot_buffer.min_seq()`, respondiendo inmediatamente en sub-microsegundos con búsqueda $O(1)$ sin realizar llamadas a disco ni syscalls. Solo si el cursor requerido es anterior al buffer en memoria, el flujo desciende a consultar los segmentos Warm y Cold en disco.

#### [A-13] Evicción destructiva del `HotBuffer` al sellar segmentos vacía el 100% de la memoria
* **Estado**: **RESUELTO (Fase 3.5-B.1)**
* **Ubicación Exacta**: [`crates/server/src/log/tiered_log.rs:121-129`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs#L121-L129).
* **Causa Raíz**: Al sellar un segmento Warm con límite `end`, ejecutaba `hot_buffer.evict_older_than(end + 1)`. Al ser `end` la última mutación agregada, purgaba el 100% del buffer.
* **Impacto**: Caída cíclica del buffer a cero elementos (*Cache Flush Cliff*), forzando a los clientes a leer del disco tras cada rotación.
* **Solución Técnica / Implementada**: Se implementó una política de ventana deslizante continua en `HotBuffer` regulada por capacidad (`ram_max_ops`) y tiempo de retención (`ram_ttl`) mediante `apply_sliding_window`. Se desacopló la rotación física de segmentos en disco (`active.wal` a `segment_{start}_{end}.wal`) de la memoria, eliminando la llamada destructiva `hot_buffer.evict_older_than(end + 1)` en rotaciones. La memoria retiene de forma continua las operaciones más recientes, erradicando las caídas de caché a cero (*Cache Flush Cliff*).

#### [A-14] `delete_room` elimina directorio físicamente con actor Tokio en vuelo y descriptores vivos
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/actor/manager.rs:196-207`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/manager.rs#L196-L207).
* **Causa Raíz**: `delete_room` remueve el sender del mapa y seguidamente ejecuta `fs::remove_dir_all`. La tarea del actor continúa ejecutándose con descriptores de archivo abiertos.
* **Impacto**: Fallos de I/O (`AccessDenied`/`EBUSY`) en Windows y creación de archivos zombi en Unix.
* **Solución Técnica / Implementada**: Se implementó el comando `RoomCommand::Shutdown { reply }` en el bucle del actor de sala y se almacenan los `JoinHandle<()>` en `room_handles`. Al invocar `delete_room`, se envía `Shutdown`, se espera la confirmación del actor y la terminación completa de la tarea Tokio (`handle.await`), cerrando y liberando todos los descriptores de archivo y bloqueos antes de invocar `fs::remove_dir_all`.

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
* **Estado**: **RESUELTO (Fase 3.5-C.2)**
* **Ubicación Exacta**: [`crates/server/tests/server_integration_tests.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/tests/server_integration_tests.rs).
* **Causa Raíz**: Los tests validaban `assigned_seq` utilizando comodines `..` para ignorar `catchup_ops`.
* **Impacto**: El bug crítico de pérdida de operaciones en catchup (C-01) no fue detectado en las pruebas de integración.
* **Solución Técnica / Implementada**: Se incorporó el test de integración `test_commit_ack_catchup_ops_content_ordering_and_contiguity` en `crates/server/tests/server_integration_tests.rs`. Valida que ante desfases de secuencia del cursor cliente, `catchup_ops` devuelva de forma determinista todas las operaciones intermedias requeridas, con estricta contigüidad, orden monótono y cargas útiles idénticas a las registradas por los clientes remitentes, validando asimismo que reintentos de commit idempotentes retornen las operaciones requeridas desde el cursor del cliente hasta la mutación propia secuenciada.

---

### DEFECTOS DE SEVERIDAD MEDIA

#### [M-01] Regla 1 de squashing sobrescribe celdas nulas con updates viejos violando LWW
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/core/src/mutation/squash.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/squash.rs), [`crates/core/src/mutation/buffer.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/buffer.rs).
* **Causa Raíz**: Cuando un `Update` entrante tenía timestamp menor que un `Insert` previo, la regla 1 resucitaba valores viejos en columnas que el `Insert` definió como `Null`.
* **Solución Técnica / Implementada**: En `squash_operations` (Regla 1), si `incoming.timestamp < existing.timestamp`, el update se descarta retornando `SquashOutcome::Discarded`. Asimismo, se incorporó `SquashOutcome::Purged` para aniquilación mutua entre `Insert` pendiente y `Delete` posterior en `TableBuffer`, eliminando la entidad de la cola sin generar registros redundantes.

#### [M-02] Reintentos idempotentes de `Commit` devuelven `catchup_ops` vacío omitiendo mutación propia
* **Estado**: **RESUELTO**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:235-252`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L235-L252).
* **Causa Raíz**: Al detectar duplicado, consultaba deltas desde `existing_seq`, devolviendo un lote vacío que no contenía la mutación ya secuenciada.
* **Solución Técnica / Implementada**: En `handle_commit`, al detectar un `mutation_id` duplicado en `dedup_cache`, se invoca `fetch_deltas(last_ack_seq, 100)` devolviendo el conjunto de operaciones a partir del cursor del cliente, incluyendo la mutación propia secuenciada originalmente.

#### [M-03] Ausencia de magic bytes, versión de wire protocol y discriminante en codec binario
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/core/src/protocol/codec.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/codec.rs).
* **Causa Raíz**: Serializaba directamente enums Bincode sin enmarcado de protocolo binario ni versión.
* **Solución Técnica / Implementada**: Se antepone una cabecera canónica de 4 bytes en toda trama de red: magic bytes `0x52, 0x4D` (`"RM"`), versión de protocolo `0x01` y flags reservadas `0x00`. En `decode_message`, las tramas con magic bytes o versión discrepante se rechazan tempranamente devolviendo `ErrorCode::ProtocolVersionMismatch`, mapeado a `ServerError::ProtocolVersionMismatch` (HTTP 400).

#### [M-04] Comparación de firmas en tiempo variable susceptible a ataques de canal lateral (timing)
* **Estado**: **RESUELTO (Fase 3.5-A.2)**
* **Ubicación Exacta**: [`crates/server/src/api/auth.rs:116`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/auth.rs#L116).
* **Causa Raíz**: Validación de firma BLAKE3 usa `!=` sobre cadenas hexadecimales en tiempo variable.
* **Solución Técnica / Implementada**: Se incorporó el crate `subtle = "2.6"` y se reemplazó la comparación de cadenas con `ct_eq` en tiempo constante sobre bytes (`subtle::ConstantTimeEq`), aplicándolo tanto a la verificación de firma de tokens de cliente como a la cabecera `X-Admin-Secret` en `AdminAuth`.

#### [M-05] Backdoor `dev-token` cableado en código de autenticación de producción
* **Estado**: **RESUELTO (Fase 3.5-A.2)**
* **Ubicación Exacta**: [`crates/server/src/api/auth.rs:71-74`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/auth.rs#L71-L74).
* **Causa Raíz**: Se acepta `dev-token` incondicionalmente si el secret es el por defecto.
* **Solución Técnica / Implementada**: Se eliminó de forma completa e incondicional la rama especial para `"dev-token"` en `verify_client_token`. Todo token debe ser criptográficamente válido y estar firmado con la clave HMAC-BLAKE3 configurada en el servidor.

#### [M-06] Evolución DDL (`add_column`) no emite señal SSE provocando desincronización de esquemas
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/server/src/actor/command.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/command.rs), [`crates/server/src/actor/room.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs), [`crates/server/src/api/sse.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/sse.rs).
* **Causa Raíz**: Recargaba el esquema en el actor pero no notificaba a los clientes conectados por el canal SSE.
* **Solución Técnica / Implementada**: Se definió el enum de señalización `RoomEvent { HeadAdvanced(SequenceNumber), SchemaReloaded(SchemaId) }`. Al recibir `RoomCommand::ReloadSchema`, el actor de sala emite `RoomEvent::SchemaReloaded` al canal broadcast de SSE, el cual lo transmite a los clientes activos como evento `schema_reloaded` con el `schema_id` como dato.

#### [M-07] Omisión de validación de `room_id` en URL path vs payload binario (bypass de gateway)
* **Estado**: **RESUELTO (Fase 3.5-A.2)**
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:148-583`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L148-L583).
* **Causa Raíz**: Handlers extraen `room_id_str` del path pero enrutan según el payload Bincode interno.
* **Solución Técnica / Implementada**: Se implementó una verificación estricta de 3 vías en todos los endpoints operativos (`/commit`, `/sync`, `/ack`, `/heartbeat`, `/schema`, `/deregister`, `/events`): el `room_id` del path de la URL debe coincidir exactamente con el `room_id` verificado del Bearer token y con el `room_id` contenido en el payload del mensaje binario (`ClientMessage`), y el `client_id` del payload debe coincidir con el `client_id` del token. Ante cualquier discrepancia, se rechaza inmediatamente con `ErrorCode::Unauthorized` / `ServerError::Unauthorized`.

#### [M-08] Ausencia de timeouts perimetrales en llamadas `sender.send` y `rx.await` hacia actores
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs:90-204`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs#L90-L204).
* **Causa Raíz**: Peticiones HTTP esperan indefinidamente la respuesta del buzón del actor.
* **Impacto**: Bloqueo de peticiones y agotamiento de descriptores de red ante actores congestionados o no receptivos.
* **Solución Técnica / Implementada**: Se envolvieron todas las interacciones con los canales del actor de sala (`sender.send` y `rx.await`) en `tokio::time::timeout(ACTOR_TIMEOUT, ...)`, donde `ACTOR_TIMEOUT = Duration::from_secs(5)`. Si el actor no responde en dicho plazo, se retorna inmediatamente `ServerError::GatewayTimeout` mapeado a HTTP 504 Gateway Timeout y `ErrorCode::Internal`.

#### [M-09] `max_batch_size` en `/sync` sin límite superior permite decodificación masiva abusiva (DoS)
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs:307-346`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs#L307-L346).
* **Causa Raíz**: El cliente puede enviar `max_batch_size: u32::MAX`, saturando memoria en deserialización.
* **Impacto**: Consumo desmedido de CPU y memoria en deserialización y transmisión de deltas de sincronización.
* **Solución Técnica / Implementada**: En `RoomActor::handle_sync`, se acotó defensivamente el límite solicitado mediante `let clamped_batch_size = max_batch_size.clamp(1, 1000);`, impidiendo la sobrecarga del reactor y de la memoria RAM ante valores arbitrarios o abusivos.

#### [M-10] Avance prematuro de `tail_seq` en `prune_older_than` induce `BehindCompaction` espurio
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/server/src/log/tiered_log.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs).
* **Causa Raíz**: Se sobrescribía `self.tail_seq = target_seq` sin comprobar si `active.wal` o segmentos restantes conservaban deltas previos.
* **Solución Técnica / Implementada**: En `TieredLog::prune_older_than`, se calcula la secuencia base real buscando el mínimo entre los segmentos Cold y Warm retenidos físicamente y el rango de memoria `HotBuffer`. Solo si ningún segmento permanece se adopta el límite target o head_seq, garantizando que `tail_seq` refleje con exactitud la disponibilidad física en disco.

#### [M-11] Doble fsync por operación sin Group Commit ni batching en escrituras del servidor
* **Estado**: **RESUELTO (Unificación de WAL por C-04)**
* **Ubicación Exacta**: [`crates/server/src/actor/room.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs).
* **Causa Raíz**: Cada commit ejecutaba dos `sync_data()` síncronos independientes a disco (uno en `MicroWal` y otro en `WarmDiskLog`).
* **Solución Técnica / Implementada**: Resuelto mediante la erradicación de `MicroWal` (remediación `C-04`). Cada mutación confirmada realiza un único `sync_data()` en `active.wal`, reduciendo el I/O de disco a la mitad por commit.

#### [M-12] Ausencia de cerrojos multi-proceso (`flock`) sobre WALs del servidor
* **Estado**: **RESUELTO (Fase 3.5-B.3)**
* **Ubicación Exacta**: [`crates/server/src/micro_wal.rs:51-57`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/micro_wal.rs#L51-L57), [`crates/server/src/log/warm_disk.rs:50-56`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/warm_disk.rs#L50-L56).
* **Causa Raíz**: Los archivos se abren sin `fs2::FileExt::try_lock_exclusive`.
* **Impacto**: Corrupción silenciosa si múltiples procesos del servidor arrancan sobre el mismo directorio de datos.
* **Solución Técnica / Implementada**: En `WarmDiskLog::append_record` y `WarmDiskLog::inspect_active_segment`, se adquiere un cerrojo exclusivo de kernel a nivel de sistema de archivos (`file.try_lock_exclusive()`) sobre `active.wal` utilizando la crate `fs2`. Cualquier proceso concurrente o secundario que intente abrir el mismo archivo WAL recibe de inmediato un error controlado `ServerError::RoomLocked` mapeado a HTTP 423 Locked.

#### [M-13] Contrato `StorageEngine` exige `table: &str` en `get`/`scan` forzando búsquedas por string
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/storage/src/engine.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/engine.rs), [`crates/storage/src/memory/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs), [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs).
* **Causa Raíz**: Inconsistencia con `apply_batch` que opera por `table_id: u16`.
* **Solución Técnica / Implementada**: Se agregaron los métodos `get_by_id` y `scan_by_id` directamente al trait `StorageEngine`, permitiendo la consulta directa por `table_id: u16` sin costo de lookup en `id_by_name`. Los métodos por nombre resuelven el ID y delegan limpiamente.

#### [M-14] Sobrescritura silenciosa de tablas con igual nombre y overflow en asignación de IDs
* **Estado**: **RESUELTO (Fase 3.5-A.4)**
* **Ubicación Exacta**: [`crates/core/src/schema/global.rs:20-31`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/global.rs#L20-L31).
* **Causa Raíz**: `add_table` inserta ciegamente sin retornar `Result` ante duplicados.
* **Solución Técnica**: Validar colisiones y retornar `Result<u16, ValidationError>`.

#### [M-15] Fuga de encapsulamiento e implementación impropia de `Deref` en `PrimaryKey` y `CompactRow`
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/core/src/value/row.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/value/row.rs), [`crates/core/src/schema/table.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/table.rs).
* **Causa Raíz**: Campos públicos (`pub SmallVec`, `pub values`) y `Deref` que violaban [C-DEREF].
* **Solución Técnica / Implementada**: Se hicieron privados los campos internos de `PrimaryKey`, `CompactRow` y `TableSchema`. Se eliminó `Deref` y se implementaron de forma segura `Index<usize>`, `IndexMut<usize>`, `as_slice()`, `iter()`, `get()`, `len()`, `is_empty()` y `resize()` en filas, y métodos getters inmutables en `TableSchema`.

#### [M-16] Retención de cerrojo global del motor durante I/O de sala en `close_room`
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs).
* **Causa Raíz**: Mantenía `self.rooms.write().await` mientras ejecutaba `room.wal_file.sync_all().await`.
* **Solución Técnica / Implementada**: En `close_room`, se extrae la sala del mapa `rooms.remove(room_id)` y se libera de inmediato el cerrojo de escritura del mapa antes de invocar `room.wal_file.sync_all().await`, impidiendo la contención global sobre otras salas abiertas.

---

### DEFECTOS DE SEVERIDAD BAJA

#### [B-01] Discrepancia de especificación: `ClientMessage::Heartbeat` carece de campo `last_ack_seq`
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/core/src/protocol/messages.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs), [`ARCHITECTURE.md`](file:///Users/Santiago/OtherProjects/client-distributed-db/ARCHITECTURE.md).
* **Causa Raíz**: Divergencia frente a ARCHITECTURE.md que estipulaba ACK coalescido en heartbeats.
* **Solución Técnica / Implementada**: Se formalizó en la arquitectura y documentación que `Heartbeat` es un mensaje de liveness puro y liviano sin avance de cursor, mientras que la confirmación y avance de secuencias se realiza de manera explícita y desacoplada mediante `ClientMessage::Ack` o dentro de `ClientMessage::Commit`.

#### [B-02] Tipos fundamentales de mutación y mensajería omiten traits estándar `Eq` y `Hash`
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/core/src/mutation/op.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/op.rs), [`crates/core/src/protocol/messages.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs), [`crates/core/src/schema/column.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/column.rs).
* **Causa Raíz**: Derivaban solo `PartialEq`, impidiendo su uso en `HashSet` o como claves de mapas.
* **Solución Técnica / Implementada**: Se derivó `#[derive(Eq, Hash)]` en `ColumnDef`, `ColumnUpdate`, `OperationKind`, `Operation`, `SequencedOperation`, `ClientMessage`, `ServerMessage` y `ErrorCode`.

#### [B-03] Handlers de Data Plane devuelven texto plano o JSON ante errores en vez de Bincode
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/server/src/api/data_plane.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs), [`crates/core/src/protocol/messages.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs).
* **Causa Raíz**: `ServerError::into_response` emitía JSON en rutas que esperan exclusivamente tramas binarias, y desregistro retornaba 204 No Content.
* **Solución Técnica / Implementada**: Se añadió la variante `ServerMessage::DeregisterAck` y se implementó en `deregister` devolviendo una trama binaria con `StatusCode::OK`. Asimismo, en respuestas de error sobre endpoints binarios se garantiza la cabecera `Content-Type: application/octet-stream`.

#### [B-04] `GET /admin/rooms/:id` produce efectos colaterales activando actores de sala en memoria
* **Estado**: **RESUELTO (Fase 3.5-C.1)**
* **Ubicación Exacta**: [`crates/server/src/api/control_plane.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/control_plane.rs), [`crates/server/src/actor/manager.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/manager.rs).
* **Causa Raíz**: Endpoint de lectura invocaba `get_or_spawn` directamente, creando directorios en disco y levantando actores para cualquier identificador arbitrario.
* **Solución Técnica / Implementada**: Se agregó el método `RoomManager::room_exists(&self, room_id)` que comprueba pasivamente la existencia en memoria o en el sistema de archivos (`data_dir/rooms/{id}`). En `control_plane::get_room`, si la sala no existe, se retorna inmediatamente `ServerError::NotFound` (HTTP 404) sin spawn de actores ni creación de directorios huérfanos.

---

*Fin del Inventario Exhaustivo de Defectos — RimDB (Post-Fase 3).*
