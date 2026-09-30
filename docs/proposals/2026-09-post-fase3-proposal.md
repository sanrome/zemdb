# PLAN MAESTRO DE MITIGACIÓN Y CORRECCIÓN ARQUITECTÓNICA — RIMDB (POST-FASE 3)
**Especificación Técnica de Soluciones, Resolución de Trade-offs y Hoja de Ruta Priorizada**

**Fecha:** 25 de Septiembre de 2026  
**Proyecto:** `RimDB` (Motor de Base de Datos Distribuida Local-First)  
**Coordinador Técnico:** Arquitecto Principal de Auditoría  
**Documento Relacionado:** [`docs/audits/2026-09-post-fase3-audit.md`](../audits/2026-09-post-fase3-audit.md)  
**Premisa Operativa:** Plan de remediación estricto de **SOLO LECTURA** (sin modificaciones inmediatas al código fuente). Contexto V1 (sin requerimiento de retrocompatibilidad, lo que permite refactorizaciones estructurales profundas de contratos, formatos y traits).

---

## 1. INTRODUCCIÓN Y OBJETIVOS ESTRATÉGICOS

Habiendo alcanzado la convergencia técnica tras 3 rondas independientes de auditoría multidisciplinar con 4 subagentes especializados, se consolidó un catálogo de **48 defectos técnicos verificados** ([`docs/audits/2026-09-post-fase3-audit.md`](../audits/2026-09-post-fase3-audit.md)).

Este documento define la arquitectura correctiva para erradicar la totalidad de los defectos antes de iniciar la construcción del SDK de cliente en la Fase 4 (`rimdb-client`).

### Objetivos Centrales de la Remediación
1. **Garantía Incondicional de Durabilidad y Replicación**: Erradicar el riesgo de pérdida silenciosa de datos en catchup 1-RTT ([`C-01`](../audits/2026-09-post-fase3-audit.md#c-01)), desincronizaciones post-reinicio por Dual-WAL ([`C-04`](../audits/2026-09-post-fase3-audit.md#c-04)) y el descarte de mutaciones en decodificación ([`C-07`](../audits/2026-09-post-fase3-audit.md#c-07)).
2. **Defensa en Profundidad y Aislamiento de Red**: Blindar el Data Plane con autenticación universal mediante extractores de Axum ([`C-03`](../audits/2026-09-post-fase3-audit.md#c-03)), cerrar la inyección anónima de snapshots ([`C-10`](../audits/2026-09-post-fase3-audit.md#c-10)) y eliminar vulnerabilidades de temporización ([`M-04`](../audits/2026-09-post-fase3-audit.md#m-04)) y backdoors ([`M-05`](../audits/2026-09-post-fase3-audit.md#m-05)).
3. **No-Bloqueo del Runtime Asíncrono Tokio**: Aislar de forma taxativa todas las llamadas síncronas de disco y la compresión Zstandard fuera de los worker threads de Tokio ([`A-01`](../audits/2026-09-post-fase3-audit.md#a-01)).
4. **Verdadera Concurrencia Copy-on-Write**: Rediseñar la compactación en `rimdb-storage` para garantizar que las escrituras entrantes jamás sean bloqueadas por la compresión Zstd ([`A-03`](../audits/2026-09-post-fase3-audit.md#a-03)).
5. **Estandarización y Cohesión de Contratos**: Unificar el formato de snapshot entre motores en memoria y disco ([`A-06`](../audits/2026-09-post-fase3-audit.md#a-06)), incorporar versionado de protocolo wire ([`M-03`](../audits/2026-09-post-fase3-audit.md#m-03)) y corregir interfaces asimétricas ([`M-13`](../audits/2026-09-post-fase3-audit.md#m-13)).

---

## 2. ESPECIFICACIÓN TÉCNICA DE SOLUCIONES POR SUBSISTEMA

### 2.1. Dominio Core, Framing y Protocolos (`crates/core`)

#### Solución S-01: Reenmarcado de Wire Protocol con Magic Bytes y Versión (`M-03`, `B-03`)
* **Módulos Afectados**: [`crates/core/src/protocol/codec.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/codec.rs), [`crates/core/src/protocol/messages.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs).
* **Diseño Técnico**:
  Todo mensaje transmitido sobre la red se encapsulará en un encabezado fijo de 4 bytes previo al payload Bincode:
  ```text
  ┌───────────────┬───────────────────┬──────────────┬────────────────────────────────┐
  │ Magic (2B)    │ Protocol Ver (1B) │ Flags (1B)   │ Bincode Payload (Variable)     │
  │ 0x52 0x4D     │ 0x01              │ 0x00         │ ClientMessage / ServerMessage  │
  └───────────────┴───────────────────┴──────────────┴────────────────────────────────┘
  ```
  - Las funciones `encode_message` y `decode_message` validarán los magic bytes (`RM`) y la versión de protocolo soportada (`0x01`). Si la versión no coincide, se rechazará tempranamente con `ErrorCode::ProtocolVersionMismatch`.
  - Se añadirá la variante `ServerMessage::DeregisterAck { correlation_id: CorrelationId, room_id: RoomId, client_id: ClientId }` para que la desregistración devuelva una trama binaria homogénea en lugar de HTTP 204 sin cuerpo ([`B-03`](../audits/2026-09-post-fase3-audit.md#b-03)).

#### Solución S-02: Bufferizado de Lotes en `WalReader` y Deprecación de `decode_wal_record` (`C-07`)
* **Módulos Afectados**: [`crates/core/src/protocol/wal_frame.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/wal_frame.rs), [`crates/storage/src/disk/wal.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/wal.rs).
* **Diseño Técnico**:
  - Eliminar la función no segura `decode_wal_record_from_slice`.
  - Refactorizar `WalReader` para incorporar una cola interna `pending_batch: VecDeque<SequencedOperation>`:
    ```rust
    pub struct WalReader<'a> {
        slice: &'a [u8],
        offset: usize,
        pending_batch: std::collections::VecDeque<SequencedOperation>,
    }

    impl<'a> WalReader<'a> {
        pub fn next_record(&mut self) -> Result<WalDecodeResult, StorageError> {
            if let Some(op) = self.pending_batch.pop_front() {
                return Ok(WalDecodeResult::Ok { op, bytes_consumed: 0 });
            }
            match self.next_batch()? {
                WalBatchDecodeResult::Ok { mut ops, bytes_consumed } => {
                    let first = ops.remove(0);
                    self.pending_batch.extend(ops);
                    Ok(WalDecodeResult::Ok { op: first, bytes_consumed })
                }
                WalBatchDecodeResult::CleanEof => Ok(WalDecodeResult::CleanEof),
                WalBatchDecodeResult::TornWrite { valid_bytes_offset, reason } => {
                    Ok(WalDecodeResult::TornWrite { valid_bytes_offset, reason })
                }
            }
        }
    }
    ```

#### Solución S-03: Deserialización Segura de Esquemas con Validación de Invariantes (`C-11`, `M-14`)
* **Módulos Afectados**: [`crates/core/src/schema/table.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/table.rs), [`crates/core/src/schema/global.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/global.rs), [`crates/core/src/schema/validation.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/validation.rs).
* **Diseño Técnico**:
  - Implementar deserialización estricta mediante constructor validador en `TableSchema`: la deserialización de Serde debe verificar que `primary_key` no esté vacío, que todas las columnas de PK existan, no sean nulas ni encriptadas, y que no existan columnas con nombres duplicados.
  - En `validation.rs`, reemplazar `.expect("primary key column must exist in column_indices")` por retorno explícito de error `ValidationError::UnknownColumn(pk_col.clone())`.
  - En `Schema::add_table`, retornar `Result<u16, ValidationError>` validando que `self.id_by_name` no contenga ya la tabla, y emplear aritmética segura `self.tables_by_id.keys().max().map_or(Some(0), |m| m.checked_add(1))` para prevenir overflows de `u16`.

#### Solución S-04: Causalidad Estricta Last-Write-Wins en Squashing de Buffer (`M-01`, `B-02`, `M-15`)
* **Módulos Afectados**: [`crates/core/src/mutation/squash.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/squash.rs), [`crates/core/src/value/row.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/value/row.rs), [`crates/core/src/mutation/op.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/op.rs).
* **Diseño Técnico**:
  - En `squash_operations` (Regla 1: `(Insert, Update)`): si `incoming.timestamp < existing.timestamp`, el update representa una mutación desfasada previa a la creación/sobrescritura actual. Debe descartarse incondicionalmente retornando `SquashOutcome::Discarded` en lugar de sobrescribir columnas nulas.
  - Añadir regla de anulación mutua (*Buffer Purge*): si una entidad con `Insert` pendiente en el buffer recibe un `Delete` posterior antes de enviarse a la red, ambas operaciones deben cancelarse y eliminarse de `pending`.
  - Encapsular los campos internos de `PrimaryKey` y `CompactRow` como privados (`SmallVec` y `Vec`), eliminando la implementación de `Deref<Target = [Value]>` en estricto cumplimiento de la directriz [C-DEREF].
  - Derivar `#[derive(Eq, Hash)]` en `Operation`, `SequencedOperation`, `ClientMessage` y `ServerMessage`.

---

### 2.2. Motor de Almacenamiento y Persistencia Local (`crates/storage`)

#### Solución S-05: Verdadera Compactación Copy-on-Write No Bloqueante (`A-03`, `A-04`) [RESUELTO]
* **Módulos Afectados**: [`crates/storage/src/disk/compactor.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/compactor.rs), [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs).
* **Diseño Técnico e Implementación**:
  - Eliminar la ejecución de `compact_room_internal` bajo el cerrojo exclusivo de la sala en `apply_batch`.
  - **Protocolo de Rotación CoW**:
    1. Al activarse la compactación, adquirir brevemente el cerrojo de escritura de la sala.
    2. Rotar el WAL: renombrar atómicamente `room_{id}.wal` a `room_{id}.wal.compacting` y abrir un nuevo descriptor `room_{id}.wal` vacío para absorber escrituras concurrentes inmediatas.
    3. Clonar la vista inmutable `Arc<HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>>` y registrar el número de secuencia de corte $S$.
    4. Liberar inmediatamente el cerrojo de la sala (tiempo de parada $< 1\text{ ms}$).
    5. En una tarea en segundo plano delegada a `tokio::task::spawn_blocking`: serializar a Bincode, comprimir con Zstandard y escribir a `room_{id}.snap.tmp.{uuid}` con sync a disco y `sync_dir`.
    6. Adquirir brevemente el cerrojo de la sala para renombrar atómicamente el snapshot temporal sobre `room_{id}.snap` y borrar de forma segura `room_{id}.wal.compacting`. Las escrituras concurrentes acumuladas en el nuevo `room_{id}.wal` permanecen intactas.

#### Solución S-06: Recuperación Resiliente de WAL con Límite de Snapshot y Truncado de Torn Writes (`C-05`, `C-06`)
* **Módulos Afectados**: [`crates/storage/src/disk/recovery.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/recovery.rs), [`crates/core/src/protocol/wal_frame.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/wal_frame.rs).
* **Diseño Técnico**:
  - En `recover_room`, filtrar incondicionalmente los registros durante el replay:
    ```rust
    if op.seq <= snapshot_seq {
        continue;
    }
    ```
  - En `decode_wal_batch_from_slice`, distinguir entre corrupciones en registros intermedios y discrepancias de CRC32 en el último registro en la frontera de `EOF`. Si el fallo ocurre en el último frame antes del fin de archivo y no restan más bytes, clasificarlo como `WalBatchDecodeResult::TornWrite { valid_bytes_offset, .. }`.
  - Al detectar `TornWrite`, el motor trunca físicamente el archivo en `valid_bytes_offset` mediante `file.set_len()` y continúa con el arranque normal.

#### Solución S-07: Cabecera Canónica de Snapshot con Verificación de Integridad (`A-05`, `A-06`) [RESUELTO PARA A-05]
* **Módulos Afectados**: [`crates/storage/src/disk/format.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/format.rs), [`crates/storage/src/engine.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/engine.rs), [`crates/storage/src/memory/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs), [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs).
* **Diseño Técnico e Implementación**:
  - Actualizar `FileHeader` para utilizar 4 bytes de su zona `reserved` para almacenar `snapshot_payload_crc32: u32`. En `recover_room`, verificar este CRC antes de descomprimir el payload.
  - Estandarizar la interfaz `StorageEngine::create_snapshot` y `apply_snapshot` para retornar y aceptar un buffer prefijado con cabecera canónica unificada:
    `[magic: 4B "RMSN"][version: 1B][compression_flag: 1B (0=raw, 1=zstd)][uncompressed_len: 4B][crc32: 4B][payload]`
  - Tanto `MemoryStorageEngine` como `DiskStorageEngine` respetarán este enmarcado, garantizando interoperabilidad transparente entre motores.

#### Solución S-08: Validación de Esquema en Almacenamiento y Optimización de Consultas (`A-08`, `A-09`, `M-13`) [RESUELTO PARA A-09]
* **Módulos Afectados**: [`crates/storage/src/disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs), [`crates/storage/src/memory/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/memory/mod.rs), [`crates/storage/src/engine.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/engine.rs).
* **Diseño Técnico e Implementación**:
  - En `StorageEngine::apply_batch`, invocar `room.schema.validate_operation(&op.op)?` antes de persistir y aplicar en tablas.
  - En `StorageEngine::scan`, clonar un puntero inmutable CoW `Arc<BTreeMap>` al inicializar el stream, garantizando Snapshot Isolation y lecturas reproducibles sin soltar cerrojos entre lotes de 64 filas.
  - Extender el trait `StorageEngine` para aceptar consultas directas por `table_id: u16` (`get_by_id`, `scan_by_id`), eliminando el overhead de búsqueda y hashing de cadenas `table: &str` en el camino crítico.

---

### 2.3. Capa de Servidor, Actores y Log Escalonado (`crates/server`)

#### Solución S-09: Unificación de WAL y Erradicación del Dual-WAL Desacoplado (`C-04`, `A-02`, `M-11`)
* **Módulos Afectados**: [`crates/server/src/actor/room.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs), [`crates/server/src/micro_wal.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/micro_wal.rs), [`crates/server/src/log/tiered_log.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs).
* **Diseño Técnico**:
  - **Eliminar `meta_{room_id}.wal` como almacén independiente.**
  - Extender el encabezado del registro de lote en `active.wal` (`wal_frame`) para incluir los metadatos de idempotencia:
    ```rust
    pub struct WalBatchMetadata {
        pub client_id: ClientId,
        pub mutation_id: MutationId,
    }
    ```
  - Al ejecutar `handle_commit`, se realiza una **única escritura y sincronización en disco** en `TieredLog::append(seq_op, metadata)`.
  - Esto erradica por diseño el desfasaje de secuencias post-crash entre Micro-WAL y TieredLog, elimina la doble llamada a `sync_data()` duplicando los IOPS máximos, y resuelve el crecimiento descontrolado de `meta_{room_id}.wal`.

#### Solución S-10: Corrección de Sincronización, Retención y Rediseño de Onboarding (`C-01`, `C-02`, `C-08`, `M-02`, `M-10`) [IMPLEMENTADO / RESUELTO PARA C-01, C-02, C-08, M-02]
* **Módulos Afectados**: [`crates/server/src/actor/room.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs), [`crates/server/src/actor/lease.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/lease.rs), [`crates/server/src/relay.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/relay.rs), [`crates/core/src/protocol/messages.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs), [`crates/server/src/api/data_plane.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs), [`crates/server/src/log/tiered_log.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs).
* **Diseño Técnico e Implementación**:
  - **Eliminación del off-by-one (`C-01`)**: En `handle_commit`, pasar `last_ack_seq` directamente a `fetch_deltas` sin incremento en 1.
  - **Propagación preventiva de `BehindCompaction` (`C-02`)**: En `handle_commit`, rechazar inmediatamente con `Err(ServerError::BehindCompaction)` si el cliente está por detrás de `tail_seq - 1` o si no está en estado `Connected` (rechazando commits prematuros de clientes `Bootstrapping` o `Dormant`).
  - **Deltas en reintentos idempotentes (`M-02`)**: En reintentos de commit duplicados en `dedup_cache`, consultar los deltas desde `last_ack_seq` para incluir la mutación original secuenciada en la respuesta `CommitAck`.
  - **Rediseño Completo de Onboarding y Ancla de Retención (`C-08`)**:
    - Re-anclar a ciegas `entry.last_ack_seq = current_head` generaba un fallo crítico cuando el snapshot base transferido pertenecía a una secuencia anterior $S < \text{current\_head}$, provocando poda de los deltas $(S, \text{current\_head}]$ y dejando al cliente en bucles infinitos de `BehindCompaction`.
    - **Nuevo Estado `ClientState::Bootstrapping`**: Clientes nuevos o clientes reconectados con `current_seq < tail_seq - 1` ingresan en `Bootstrapping`.
    - **Aislamiento en Poda**: Clientes en `Bootstrapping` son ignorados en `min_connected_ack_seq`, por lo que nunca bloquean la compactación para miembros conectados.
    - **Ancla de Retención (*Retention Anchor*)**: En `RoomActor::prune_older_than`, el límite de retención se define como $\text{retention\_floor} = \min(\text{min\_connected\_ack}, \text{active\_snapshot\_seq})$. Mientras exista un snapshot en `SnapshotRelay` para la secuencia $S$, los deltas $(S, \text{current\_head}]$ quedan protegidos en disco para el catchup de los clientes en onboarding.
    - **Protocolo de Handshake**: `RegisterClient` transmite `current_seq: Option<SequenceNumber>` y `Registered` devuelve `head_seq`, `tail_seq` y `active_snapshot_seq: Option<SequenceNumber>`.
    - **Promoción Dinámica**: Al aplicar el snapshot en $S$ y solicitar `/sync` o emitir `Ack` para $seq \ge S$, el cliente es promovido a `Connected`.
  - En `TieredLog::prune_older_than`, calcular `self.tail_seq` estrictamente a partir del segmento físico más antiguo que permanezca en disco, evitando adelantar el cursor más allá de los deltas disponibles en `active.wal` ([`M-10`](../audits/2026-09-post-fase3-audit.md#m-10)).
  - En `ClientLeaseTracker::check_timeouts`, transicionar clientes desconectados a `Dormant` basándose en el temporizador de inactividad de 90 segundos, rompiendo el deadlock de retención ([`A-10`](../audits/2026-09-post-fase3-audit.md#a-10)).

#### Solución S-11: Aislamiento Asíncrono e Inversión Jerárquica de Caché (`A-01`, `A-12`, `A-13`)
* **Módulos Afectados**: [`crates/server/src/actor/room.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/room.rs), [`crates/server/src/log/tiered_log.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/tiered_log.rs), [`crates/server/src/log/cold_disk.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/log/cold_disk.rs).
* **Diseño Técnico**:
  - Envolver la compresión y descompresión de Zstandard (`ColdDiskLog::compress_warm_segment`, `read_range`) en `tokio::task::spawn_blocking`.
  - Reordenar la consulta en `TieredLog::fetch_deltas`:
    ```rust
    // 1. Evaluar primero el buffer caliente en RAM
    if let Some(min_ram) = self.hot_buffer.min_seq() {
        if from_seq >= min_ram {
            let ops = self.hot_buffer.get_range(from_seq, limit);
            let has_more = ops.last().map_or(false, |last| last.seq < self.head_seq);
            return Ok((ops, has_more));
        }
    }
    // 2. Si from_seq es anterior, consultar Warm Disk y Cold Disk
    ```
  - En `HotBuffer::append`, implementar una política de ventana deslizante por capacidad (`ram_max_ops`) y TTL, purgando de forma gradual en lugar de vaciar todo el buffer hasta `end + 1` tras una rotación.
  - Optimizar `HotBuffer::get_range` para calcular el desplazamiento de inicio en $O(1)$ a partir de la contigüidad monótona ([`M-11`](../audits/2026-09-post-fase3-audit.md#m-11)).

---

### 2.4. Capa de Red, Seguridad y APIs (`crates/server/src/api`)

#### Solución S-12: Autenticación Universal y Validación de Rutas en Data Plane (`C-03`, `M-07`)
* **Módulos Afectados**: [`crates/server/src/api/data_plane.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs), [`crates/server/src/api/router.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/router.rs), [`crates/server/src/api/auth.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/auth.rs).
* **Diseño Técnico**:
  - Crear el extractor `ClientAuth` en Axum:
    ```rust
    pub struct ClientAuth {
        pub client_id: ClientId,
        pub room_id: RoomId,
    }

    #[async_trait]
    impl<S> FromRequestParts<S> for ClientAuth
    where
        S: Send + Sync,
        AppState: FromRef<S>,
    {
        type Rejection = Response;

        async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
            let app_state = AppState::from_ref(state);
            let auth_header = parts.headers.get(header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| unauthorized_response())?;

            let token = auth_header.strip_prefix("Bearer ").unwrap_or(auth_header);
            let verified = verify_client_token(token, &app_state.config.auth_secret)
                .map_err(|_| unauthorized_response())?;

            Ok(ClientAuth { client_id: verified.client_id, room_id: verified.room_id })
        }
    }
    ```
  - Requerir `ClientAuth` en `/commit`, `/sync`, `/ack`, `/heartbeat`, `/deregister` y `/events`.
  - Validar en cada handler:
    ```rust
    if auth.room_id.as_str() != path_room_id {
        return binary_error(..., ServerError::Config("RoomId path and token mismatch".into()));
    }
    ```

#### Solución S-13: Snapshot Relay Multipart Fragmentado y Blindaje DoS (`C-10`, `A-15`) [A-15 RESUELTO]
* **Módulos Afectados**: [`crates/server/src/relay.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/relay.rs), [`crates/server/src/config.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/config.rs), [`crates/server/src/api/router.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/router.rs).
* **Diseño Técnico e Implementación**:
  - **[RESUELTO A-15] SnapshotRelay Respaldado en Disco y TTL Configurable**:
    - `SnapshotRelay::new(snapshots_dir, ttl)` ahora exige obligatoriamente un directorio en disco (`data/snapshots/`) para persistir snapshots.
    - Se incorporó `snapshot_ttl_secs: u64` en `ServerConfig` con soporte en `config.toml` y variable de entorno `RIMDB_SNAPSHOT_TTL_SECS`.
    - Las subidas se escriben atómicamente con staging `.tmp.<nanos>` y renombrado seguro.
    - `cleanup_expired` purga tanto de la memoria RAM como del disco (`remove_file`), eliminando riesgos de OOM y fugas de almacenamiento.
    - Se implementó `recover_disk_snapshots()` que al reiniciar el servidor recarga snapshots válidos y descarta archivos temporales huérfanos.
  - **[PENDIENTE C-10] Subida Multipart y Autorización**:
    - Exigir autorización en todos los endpoints de `SnapshotRelay`.
    - Diseñar la API de subida fragmentada simétrica a la descarga:
      `POST /rooms/:room_id/snapshot/upload-chunk` recibiendo fragmentos de hasta 1 MB (`SnapshotChunk`).
    - Al recibir el último chunk, verificar la suma criptográfica BLAKE3 del archivo consolidado en disco antes de marcarlo como listo para descarga por clientes en onboarding.

#### Solución S-14: Concurrencia de Gestión de Salas y Resiliencia en Reinicios (`C-09`, `A-11`, `A-14`, `M-08`, `M-12`)
* **Módulos Afectados**: [`crates/server/src/actor/manager.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/actor/manager.rs), [`crates/server/src/api/data_plane.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/data_plane.rs).
* **Diseño Técnico**:
  - En `RoomManager`, implementar sincronización atómica para la instanciación de salas mediante `DashMap<RoomId, Arc<tokio::sync::Mutex<()>>>` (cerrojo fino de spawn por sala).
  - Al abrir archivos de sala (`active.wal`), adquirir cerrojo exclusivo a nivel de kernel mediante `fs2::FileExt::try_lock_exclusive`.
  - Reemplazar `get_room` por `get_or_spawn` en todos los handlers de datos, permitiendo la reactivación perezosa de salas tras reinicios del servidor.
  - En `delete_room`, conservar el `JoinHandle<()>` del actor, emitir `RoomCommand::Shutdown`, esperar su culminación y solo después ejecutar `fs::remove_dir_all`.
  - Envolver la interacción HTTP con actores en `tokio::time::timeout(Duration::from_secs(5), ...)` con retorno de `GATEWAY_TIMEOUT`.
  - Acotar el parámetro de sincronización: `let limit = max_batch_size.clamp(1, 1000) as usize;`.

#### Solución S-15: Saneamiento Criptográfico y Señalización DDL en Tiempo Real (`M-04`, `M-05`, `M-06`)
* **Módulos Afectados**: [`crates/server/src/api/auth.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/auth.rs), [`crates/server/src/api/control_plane.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/control_plane.rs), [`crates/server/src/api/sse.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server/src/api/sse.rs).
* **Diseño Técnico**:
  - Utilizar comparación en tiempo constante `subtle::ConstantTimeEq` para validar firmas HMAC/BLAKE3 y tokens de administración.
  - Eliminar incondicionalmente el bypass `"dev-token"`, rechazando arrancar en modo no-test si `auth_secret` no ha sido configurado explícitamente.
  - Al añadir una columna mediante `POST /admin/schemas/:id/columns`, emitir la señal SSE `RoomEvent::SchemaReloaded` a través del canal de broadcast del actor de la sala para que los clientes activos actualicen su definición local.

---

## 3. RESOLUCIÓN DE TRADE-OFFS ARQUITECTÓNICOS

### Trade-Off 1: Unificación de WAL vs. Doble WAL con Compensación 2PC
* **Dilema**: Los especialistas de base de datos y sistemas distribuidos recomendaron unificar la persistencia eliminando `MicroWal` e integrando deduplicación en `TieredLog`, mientras que la separación original buscaba aislar metadatos rápidos de las transacciones volumétricas.
* **Resolución**: **Unificación Total de WAL en `TieredLog` (Opción Recomendada)**.
  - *Justificación*: Mantener dos archivos append-only paralelos (`meta_{room}.wal` y `active.wal`) sin transacciones distribuidas atómicas a nivel de filesystem es matemáticamente invulnerable a torn writes y desfases tras caídas. Duplicar `sync_data()` reduce el throughput a la mitad. Unificar enmarcando el `MutationId` en el encabezado de lote de `active.wal` garantiza orden causal atómico, contigüidad estricta y duplica la tasa de commits por segundo.

### Trade-Off 2: I/O Asíncrono Tokio (`tokio::fs`) vs. Aislamiento Bloqueante (`spawn_blocking`)
* **Dilema**: `tokio::fs` provee interfaces ergonómicas `.await`, pero por debajo Tokio utiliza su propio pool de `spawn_blocking` para llamadas POSIX bloqueantes.
* **Resolución**: **Estrategia Híbrida Especializada**.
  - Para operaciones de lectura/escritura secuencial en el bucle caliente del actor: utilizar `tokio::fs::File` para no bloquear el hilo de ejecución cooperativo.
  - Para algoritmos CPU-intensivos (compresión y descompresión de streams Zstandard completos) y escaneos masivos de directorios: utilizar llamadas explícitas a `tokio::task::spawn_blocking` con límites de concurrencia, protegiendo tanto el reactor asíncrono como la memoria del proceso.

### Trade-Off 3: Autenticación por Sesión Bearer HTTP vs. HMAC por Trama de Mensaje
* **Dilema**: Incluir firmas criptográficas completas en cada variante de `ClientMessage` sobrecarga el tamaño de los mensajes binarios, mientras que tokens Bearer HTTP desacoplan la seguridad al transporte.
* **Resolución**: **Bearer Session Tokens en Cabeceras HTTP/2**.
  - *Justificación*: El Data Plane opera sobre HTTP/2. Al validar un ticket de sesión en la cabecera `Authorization: Bearer <ticket>` mediante el extractor de Axum, el tamaño del payload binario de `ClientMessage` se mantiene mínimo (96B para operaciones), y los proxies o API gateways pueden aplicar control de acceso sin tener que deserializar payloads Bincode internos.

### Trade-Off 4: Poda Inmediata de Logs vs. Ventana Mínima de Gracia para Clientes Reconectados
* **Dilema**: La poda estricta inmediata por cursor unánime ahorra disco al máximo, pero si un cliente se desconecta momentáneamente y regresa, cualquier retraso mínimo puede forzarlo a `BehindCompaction` si todos los demás avanzaron.
* **Resolución**: **Poda Proactiva con Ventana de Historial Mínimo**.
  - *Justificación*: Mantener una cuota mínima de deltas calientes (ej. últimas 500 operaciones o 5 minutos) en `active.wal` independientemente de los ACKs recibidos. Si todos los clientes están al día, el log solo se poda físicamente si supera este margen de seguridad, evitando descargas innecesarias de snapshots ante desconexiones transitorias de red.

### Trade-Off 5: Estandarización de Formato de Snapshot (Zstd vs. Raw)
* **Dilema**: Zstandard provee una tasa de compresión superior al 70%, pero en entornos WebAssembly puros la descompresión Zstd introduce sobrecarga de binario.
* **Resolución**: **Contenedor Polimórfico con Bandera de Compresión**.
  - *Justificación*: El enmarcado canónico `RMSN` contendrá la bandera `compression_flag`. En entornos nativos de alto rendimiento, `DiskStorageEngine` aplicará Zstd. En entornos de navegador WASM o tests rápidos en memoria, se admitirá compresión nula (`None`), permitiendo que ambos motores interpreten el formato de manera uniforme y sin romper el principio de sustitución de Liskov.

---

## 4. HOJA DE RUTA PRIORIZADA Y PLAN DE REMEDIACIÓN POR FASES

El plan de corrección se estructurará en tres fases incrementales antes de dar inicio formal a la Fase 4 (`rimdb-client`):

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        HOJA DE RUTA DE REMEDIACIÓN TÉCNICA                             │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  FASE 3.5-A: Integridad Crítica de Datos, Secuenciación y Autenticación      [Semana 1]│
│  FASE 3.5-B: Concurrencia CoW, Desacoplamiento de I/O y Resiliencia de Red   [Semana 2]│
│  FASE 3.5-C: Refactorización Estructural, APIs Tipadas y Paridad de Tests    [Semana 3]│
│  FASE 4.0:   Implementación del SDK de Cliente (rimdb-client)                [Semana 4]│
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Fase 3.5-A: Integridad Crítica de Datos, Secuenciación y Autenticación (Prioridad P0)
*Objetivo: Erradicar todos los modos de falla que provocan corrupción, pérdida de eventos o evasión de seguridad.*

1. **Corrección de Catchup 1-RTT, Retención y Onboarding**:
   - [RESUELTO] Eliminar el off-by-one en `handle_commit` pasando `last_ack_seq` directo a `fetch_deltas` ([`C-01`](../audits/2026-09-post-fase3-audit.md#c-01)).
   - [RESUELTO] Propagar `BehindCompaction` sin suprimir errores en `handle_commit` ([`C-02`](../audits/2026-09-post-fase3-audit.md#c-02)).
   - [RESUELTO] Reintentos idempotentes de `Commit` devuelven `catchup_ops` con mutación original ([`M-02`](../audits/2026-09-post-fase3-audit.md#m-02)).
   - [RESUELTO] Rediseño de Onboarding: estado `Bootstrapping`, ancla de retención en poda (`retention_floor = min(min_connected_ack, active_snapshot_seq)`) y handshake enriquecido ([`C-08`](../audits/2026-09-post-fase3-audit.md#c-08)).
2. **Autenticación y Blindaje en Data Plane**:
   - [RESUELTO] Implementar extractor Axum `ClientAuth` para validar Bearer tokens en todos los endpoints operativos ([`C-03`](../audits/2026-09-post-fase3-audit.md#c-03)).
   - [RESUELTO] Validar estricta coincidencia de `room_id` en URL path vs payload/token ([`M-07`](../audits/2026-09-post-fase3-audit.md#m-07)).
   - [RESUELTO] Eliminar el backdoor `"dev-token"` y aplicar comparación en tiempo constante `subtle` ([`M-04`](../audits/2026-09-post-fase3-audit.md#m-04), [`M-05`](../audits/2026-09-post-fase3-audit.md#m-05)).
3. **Persistencia Atómica y Recuperación de Fallos**:
   - [RESUELTO] Reemplazar el Dual-WAL unificando la persistencia en `active.wal` ([`C-04`](../audits/2026-09-post-fase3-audit.md#c-04)).
   - [RESUELTO] Filtrar operaciones previas a `snapshot_seq` en el replay de `recover_room` ([`C-05`](../audits/2026-09-post-fase3-audit.md#c-05)).
   - [RESUELTO] Clasificar fallos de CRC en EOF como `TornWrite` y truncar limpiamente ([`C-06`](../audits/2026-09-post-fase3-audit.md#c-06)).
   - [RESUELTO] Corregir `WalReader` con cola de operaciones para no perder deltas en lotes multi-op ([`C-07`](../audits/2026-09-post-fase3-audit.md#c-07)).
4. **Validaciones Estructurales y Pánicos**:
   - [RESUELTO] Implementar deserialización estricta de `TableSchema` y eliminar `.expect()` ([`C-11`](../audits/2026-09-post-fase3-audit.md#c-11), [`M-14`](../audits/2026-09-post-fase3-audit.md#m-14)).
   - [RESUELTO] Validar `ack_seq <= head_seq` en `handle_ack` evitando purga catastrófica ([`A-07`](../audits/2026-09-post-fase3-audit.md#a-07)).

---

### Fase 3.5-B: Concurrencia CoW, Desacoplamiento de I/O y Resiliencia de Red (Prioridad P1)
*Objetivo: Eliminar bloqueos del runtime Tokio, garantizar no-bloqueancia en compactaciones y blindar el relay de snapshots.*

1. **Aislamiento Asíncrono del Reactor Tokio**:
   - [RESUELTO] Delegar compresión Zstd y lecturas masivas a `tokio::task::spawn_blocking` ([`A-01`](../audits/2026-09-post-fase3-audit.md#a-01)).
   - [RESUELTO] Invertir la jerarquía de consulta en `fetch_deltas` evaluando RAM `HotBuffer` antes de disco ([`A-12`](../audits/2026-09-post-fase3-audit.md#a-12)).
   - [RESUELTO] Implementar ventana deslizante en `HotBuffer` sin evicción a cero ([`A-13`](../audits/2026-09-post-fase3-audit.md#a-13)).
2. **Compactación CoW y Snapshot Isolation**:
   - [RESUELTO] Implementar rotación de WAL (`wal.compacting`) en `compactor.rs` para liberar escritores ([`A-03`](../audits/2026-09-post-fase3-audit.md#a-03)).
   - [RESUELTO] Usar rutas temporales únicas (`snap.tmp.{uuid}`) eliminando colisiones de truncado ([`A-04`](../audits/2026-09-post-fase3-audit.md#a-04)).
   - [RESUELTO] Proteger escaneos de `StorageEngine::scan` con vistas CoW inmutables contra lecturas fantasma ([`A-09`](../audits/2026-09-post-fase3-audit.md#a-09)).
   - [RESUELTO] Incorporar CRC32 del payload comprimido en `FileHeader` ([`A-05`](../audits/2026-09-post-fase3-audit.md#a-05)).
3. **Gestión de Salas y Snapshot Relay Multipart**:
   - [RESUELTO] Implementar cerrojo fino de instanciación en `RoomManager` para erradicar TOCTOU ([`C-09`](../audits/2026-09-post-fase3-audit.md#c-09)).
   - [RESUELTO] Reemplazar `get_room` por `get_or_spawn` en endpoints de datos soportando reinicios ([`A-11`](../audits/2026-09-post-fase3-audit.md#a-11)).
   - [RESUELTO] Implementar comando `RoomCommand::Shutdown` para coordinar el borrado de salas en `delete_room` ([`A-14`](../audits/2026-09-post-fase3-audit.md#a-14)).
   - [RESUELTO] `SnapshotRelay` respaldado en disco con TTL configurable, subida multipart fragmentada (`/snapshot/upload-chunk`), autenticación y verificación consolidada de hash BLAKE3 ([`C-10`](../audits/2026-09-post-fase3-audit.md#c-10), [`A-15`](../audits/2026-09-post-fase3-audit.md#a-15)).
   - [RESUELTO] Adquirir bloqueos `flock` exclusivos en descriptores de archivos del servidor ([`M-12`](../audits/2026-09-post-fase3-audit.md#m-12)).
   - [RESUELTO] Timeouts perimetrales de 5 segundos en llamadas a actores devolviendo HTTP 504 GatewayTimeout ([`M-08`](../audits/2026-09-post-fase3-audit.md#m-08)).
   - [RESUELTO] Acotación estricta de `max_batch_size` (1..=1000) en sincronización de deltas ([`M-09`](../audits/2026-09-post-fase3-audit.md#m-09)).

---

### Fase 3.5-C: Refactorización Estructural, APIs Tipadas y Paridad de Tests (Prioridad P2)
*Objetivo: Optimizar el rendimiento algorítmico, consolidar el encapsulamiento y expandir la cobertura de tests.*

1. **Refactorización de Contratos y Rendimiento**:
   - [RESUELTO] Enmarcado con magic bytes y versionado en codec binario ([`M-03`](../audits/2026-09-post-fase3-audit.md#m-03)).
   - [RESUELTO] Formato universal de snapshots interoperable entre `MemoryStorageEngine` y `DiskStorageEngine` ([`A-06`](../audits/2026-09-post-fase3-audit.md#a-06)).
   - [RESUELTO] Soporte de consulta directa por `table_id: u16` en `StorageEngine` y validación en `apply_batch` ([`M-13`](../audits/2026-09-post-fase3-audit.md#m-13), [`A-08`](../audits/2026-09-post-fase3-audit.md#a-08)).
   - [RESUELTO] Búsqueda en $O(1)$ sobre `HotBuffer` ([`RUST-11`](../audits/2026-09-post-fase3-audit.md#rust-11)).
   - [RESUELTO] Encapsular campos mutables de `PrimaryKey`, `CompactRow`, `TableSchema` y remover `Deref` no idiomático ([`M-15`](../audits/2026-09-post-fase3-audit.md#m-15)).
   - [RESUELTO] Corrección de LWW en squashing (Regla 1) y anulación mutua Insert+Delete ([`M-01`](../audits/2026-09-post-fase3-audit.md#m-01), [`B-02`](../audits/2026-09-post-fase3-audit.md#b-02)).
   - [RESUELTO] Señalización SSE ante migraciones DDL ([`M-06`](../audits/2026-09-post-fase3-audit.md#m-06)).
   - [RESUELTO] Estandarizar respuestas binarias estructuradas en errores de Data Plane y `DeregisterAck` ([`B-03`](../audits/2026-09-post-fase3-audit.md#b-03)).
   - [RESUELTO] Transición de leases `Disconnected -> Dormant` tras 90s ([`A-10`](../audits/2026-09-post-fase3-audit.md#a-10)).
   - [RESUELTO] Recálculo exacto de `tail_seq` en `prune_older_than` ([`M-10`](../audits/2026-09-post-fase3-audit.md#m-10)).
   - [RESUELTO] Liberación de cerrojo global antes de I/O en `close_room` ([`M-16`](../audits/2026-09-post-fase3-audit.md#m-16)).
   - [RESUELTO] Consulta pasiva en `GET /admin/rooms/:id` evitando spawn fantasma de actores ([`B-04`](../audits/2026-09-post-fase3-audit.md#b-04)).
2. **Expansión Exhaustiva de la Batería de Pruebas**:
   - Añadir tests de integración verificando explícitamente el contenido y contigüidad de `catchup_ops` ante desfases de secuencia ([`A-17`](../audits/2026-09-post-fase3-audit.md#a-17)).
   - Tests de recuperación post-crash simulando caídas entre snapshot y truncado de WAL.
   - Tests de estrés por concurrencia multi-proceso verificando `flock` y rechazo de instancias concurrentes.
   - Tests de saturación de cuota de disco y desconexiones abruptas durante streaming de snapshots.

---

### Fase 4.0: Implementación del SDK de Cliente (`crates/client`)
Habiendo remediado los 48 defectos de los motores de persistencia y red, se procederá con la arquitectura limpia de `rimdb-client`:
1. **Configuración y Dependencias**: Sincronizar `crates/client/Cargo.toml` con `rimdb-core`, `rimdb-storage`, `tokio`, `reqwest`, `bytes`, `bincode`.
2. **Fachada `RimdbClient`**: Gestión de conexiones HTTP/2, pooling de sesiones y manejo transparente de autenticación Bearer con renovación.
3. **Manejadores `RoomHandle` y `TableHandle`**: Interfaz ergonómica para transacciones locales Write-Through, integración con `TableBuffer` para outbox queue local y deduplicación.
4. **Worker de Sincronización y Stream SSE**: Receptor reactivo de señales `head_advanced` que despierte peticiones `/sync` con backpressure y aplique deltas sobre el `StorageEngine` local.

---

*Fin del Plan Maestro de Mitigación y Corrección Arquitectónica — RimDB.*
