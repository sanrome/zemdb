# Propuesta Maestra de Mitigación y Corrección — Post-Fase 3.5

> ⚠️ **Documento reemplazado.** Las severidades y soluciones de este documento fueron revisadas contra el código; varias eran incorrectas. Ver [`2026-10-fase3_6-remediation-plan.md`](2026-10-fase3_6-remediation-plan.md).

**Documento:** Plan Técnico Maestro de Mitigación, Resolución de Trade-offs y Hoja de Ruta  
**Fecha:** 30 de Septiembre de 2026  
**Referencia:** [docs/audits/2026-09-fase3_5-audit.md](../audits/2026-09-fase3_5-audit.md)  
**Rol:** Arquitecto Principal y Coordinador Técnico de Auditoría  
**Modo:** Estricto Solo Lectura (sin modificaciones a código fuente)  
**Entregable Oficial:** `docs/proposals/2026-09-fase3_5-proposal.md`  

---

## 1. Introducción y Objetivos de la Propuesta

A partir del Inventario Exhaustivo de Defectos consolidado tras la auditoría técnica iterativa e independiente (57 defectos canónicos únicos catalogados), este documento establece la especificación técnica de las soluciones, dirime las compensaciones (*trade-offs*) arquitectónicas entre las propuestas de los subagentes especializados y articula una hoja de ruta estructurada en 4 subfases prioritarias.

Conforme a las directrices de diseño de ZemDB (v1, sin restricciones de retrocompatibilidad), todas las soluciones se diseñan apuntando a la máxima corrección formal, durabilidad estricta (ACID/ARIES), rendimiento libre de alocaciones redundantes y desacoplamiento limpio entre planos y capas.

---

## 2. Especificación Técnica de Soluciones por Defecto

### 2.1. Mitigaciones para Defectos Críticos (DEF-01 a DEF-06, DEF-34)

#### DEF-01: Reordenamiento y Contigüidad Monótona en Reconciliación Multicapa
* **Componentes**: `crates/server/src/log/hot_buffer.rs` y `crates/server/src/log/tiered_log.rs`.
* **Solución Técnica**:
  1. En `HotBuffer::get_range`, evaluar la precondición de contigüidad: si `from_seq.get() + 1 < min`, el buffer en RAM no contiene el inicio del rango requerido; debe retornar un vector vacío `Vec::new()`. Únicamente retornar elementos cuando `from_seq.get() + 1 >= min`.
  2. En `TieredLog::fetch_deltas`, reestructurar la secuencia de consulta para consultar las capas en estricto orden cronológico ascendente:
     $$\text{Cold Disk} \longrightarrow \text{Sealed Warm Disk} \longrightarrow \text{Active Warm Disk } (\texttt{active.wal}) \longrightarrow \text{RAM HotBuffer}$$
     Si el buffer de RAM tiene un hueco respecto al cursor actual (`current_from + 1 < min_ram`), consultar primero `active.wal` para llenar el intervalo $[current\_from + 1 \dots \min(limit, min\_ram - 1)]$ antes de extraer deltas de RAM.

#### DEF-02: Preservación Generacional de WAL y Rollback de Compactación CoW
* **Componentes**: `crates/storage/src/disk/compactor.rs`.
* **Solución Técnica**:
  1. En la Fase 1 de `compact_room_cow`, verificar si `wal_compacting_path.exists()`. Si existe un segmento de compactación previo no consolidado, fusionarlo con el WAL activo actual antes de proceder, o utilizar nombres generacionales basados en rangos de secuencia: `room_{id}_{start_seq}_{cut_seq}.wal.compacting`.
  2. Ante fallos en la Fase 2, implementar una rutina de recuperación que no abandone el archivo: si la compactación en segundo plano falla, adquirir el cerrojo de escritura de la sala, fusionar los deltas acumulados en el nuevo WAL temporal al final del segmento compacting, y restaurar dicho archivo como el WAL activo principal.
  3. En la Fase 3, asegurar la limpieza incondicional de la bandera `room.is_compacting = false` mediante un guard RAII (`CompactionGuard`) o bloque `finally`, garantizando que ningún escape prematuro con el operador `?` bloquee permanentemente el ciclo de vida de la compactación.

#### DEF-03: Fusión Atómica de WAL en Recuperación Post-Crash
* **Componentes**: `crates/storage/src/disk/recovery.rs`.
* **Solución Técnica**:
  1. Prohibir terminantemente el borrado de `wal_compacting_path` antes de que la persistencia combinada esté asegurada físicamente en disco.
  2. Eliminar la sobrescritura in-place destructiva sobre `wal_file`.
  3. Crear un archivo temporal atómico `room_{id}.wal.merge.tmp`, escribir secuencialmente `compacting_bytes` seguido de `active_wal_content`, ejecutar `file.sync_all().await`, renombrar atómicamente mediante `tokio::fs::rename(&tmp_path, &wal_path).await`, sincronizar el directorio padre con `sync_dir(parent)`, y **únicamente después** de este punto eliminar `room_{id}.wal.compacting`.

#### DEF-04: Validación Preventiva de Retención de Log en Commit
* **Componentes**: `crates/server/src/actor/room.rs`.
* **Solución Técnica**:
  1. Mover la comprobación de `last_ack_seq` contra la cota de retención `tail_seq` al inicio del método `handle_commit`, antes de interactuar con el secuenciador o el log WAL:
     ```rust
     let tail_seq = self.tiered_log.tail_seq();
     if tail_seq.get() > 0 && last_ack_seq.get() < tail_seq.get().saturating_sub(1) {
         let _ = reply.send(Err(ServerError::BehindCompaction));
         return;
     }
     ```
  2. Garantizar que ninguna mutación sea numerada (`head_seq.next()`), persistida en `active.wal` o difundida vía SSE si el cliente emisor se encuentra por detrás del suelo de retención.

#### DEF-05: Reintegración Determinista de Clientes Dormant y Bootstrapping
* **Componentes**: `crates/server/src/actor/room.rs` y `crates/server/src/actor/lease.rs`.
* **Solución Técnica**:
  1. En `handle_sync`, `handle_ack` y `handle_commit`, evaluar si el cliente presenta un cursor contemporáneo válido:
     $$\text{cursor} \ge \text{tail\_seq}.\text{saturating\_sub}(1)$$
  2. Si el cliente está marcado como `Dormant` o `Bootstrapping`, pero su cursor confirma la recepción o tenencia de un snapshot contemporáneo, promoverlo automáticamente a `ClientState::Connected` mediante `record_ack` y continuar con el procesamiento normal de la solicitud.

#### DEF-06: Retorno de Mutación y Propagación de Errores en Reintentos Idempotentes
* **Componentes**: `crates/server/src/actor/room.rs`.
* **Solución Técnica**:
  1. En la rama de deduplicación de `handle_commit`: si `last_ack_seq >= existing_seq`, retornar de inmediato `CommitResponse { assigned_seq: existing_seq, catchup_ops: Vec::new(), has_more: false }`.
  2. Si `last_ack_seq < existing_seq`, validar preventivamente `last_ack_seq >= tail_seq - 1` y consultar `fetch_deltas(last_ack_seq, 100)`. La mutación `existing_seq` estará naturalmente incluida dentro del rango $(last\_ack\_seq, existing\_seq]$.
  3. Eliminar el uso de `unwrap_or_default()` sobre el resultado de `fetch_deltas` y propagar formalmente cualquier error de almacenamiento.

#### DEF-34: Incorporación del Contrato `reload_schema` en `StorageEngine`
* **Componentes**: `crates/storage/src/engine.rs`, `crates/storage/src/memory/mod.rs` y `crates/storage/src/disk/mod.rs`.
* **Solución Técnica**:
  1. Extender el trait `StorageEngine` con el método:
     ```rust
     async fn reload_schema(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError>;
     ```
  2. En `MemoryStorageEngine` y `DiskStorageEngine`, implementar la actualización atómica de `room.schema` bajo el cerrojo exclusivo de escritura de la sala, verificando que la evolución sea append-only (mismo número de tablas o adición de columnas compatibles).

---

### 2.2. Mitigaciones para Defectos Altos (DEF-07 a DEF-16, DEF-27, DEF-28, DEF-35, DEF-51 a DEF-53)

#### DEF-07: Inclusión de `active.wal` en el Cálculo Físico de `tail_seq`
* **Componentes**: `crates/server/src/log/tiered_log.rs`.
* **Solución Técnica**: Modificar la jerarquía en `prune_and_update_tail` y `prune_older_than`:
  ```rust
  if let Some(first_cold) = remaining_cold.first() {
      self.tail_seq = first_cold.start_seq;
  } else if let Some(first_warm) = remaining_warm.first() {
      self.tail_seq = first_warm.start_seq;
  } else if let Some(active_start) = self.warm_disk.active_start_seq() {
      self.tail_seq = match self.hot_buffer.min_seq() {
          Some(min_ram) => min_ram.min(active_start),
          None => active_start,
      };
  } else if let Some(min_ram) = self.hot_buffer.min_seq() {
      self.tail_seq = min_ram;
  } else {
      self.tail_seq = self.head_seq;
  }
  ```

#### DEF-08: Desacoplamiento de I/O Asíncrono y Descompresión Zstd en Tokio
* **Componentes**: `crates/server/src/log/warm_disk.rs`, `crates/server/src/log/cold_disk.rs`, `crates/storage/src/memory/mod.rs` y `crates/server/src/relay.rs`.
* **Solución Técnica**:
  1. En `ColdDiskLog::read_range_with_mutations`, aislar `std::fs::read` y `zstd::stream::decode_all` delegándolos a `tokio::task::spawn_blocking`.
  2. En `MemoryStorageEngine::apply_snapshot`, aislar `decode_snapshot_envelope` en `spawn_blocking`.
  3. En `SnapshotRelay`, utilizar I/O asíncrono no bloqueante (`tokio::fs`) para persistir snapshots temporales.

#### DEF-09: Deprecación Segura o Validación Unitaria en `decode_wal_record_from_slice`
* **Componentes**: `crates/core/src/protocol/wal_frame.rs` y `crates/storage/src/disk/format.rs`.
* **Solución Técnica**: Validar estrictamente en `decode_wal_record_from_slice` que el lote contenga exactamente 1 operación. Si `ops.len() > 1`, retornar `Err(WalFrameError::Corruption("Multi-operation batch cannot be decoded via single-record decoder; use decode_wal_batch_from_slice"))`. Orientar a todos los componentes hacia el uso de `WalReader` o `decode_wal_batch_from_slice`.

#### DEF-10: Validación de Límites y Admisibilidad en Snapshot Relay
* **Componentes**: `crates/server/src/relay.rs` y `crates/server/src/actor/room.rs`.
* **Solución Técnica**: Interceptar subidas de snapshot en el relay despachando validación al actor de la sala:
  1. Validar que la sala exista en `RoomManager`.
  2. Verificar que `snapshot_head_seq` esté acotado: $\text{tail\_seq} \le \text{snapshot\_head\_seq} \le \text{head\_seq}$.
  3. Comprobar que el payload cumpla con la cabecera canónica `ZMSN` y coincida su checksum CRC32 antes de indexarlo en `SnapshotRelay`.

#### DEF-11: Enmarcado Binario Uniforme en Respuestas de Error de Chunks
* **Componentes**: `crates/server/src/relay.rs`.
* **Solución Técnica**: Sustituir retornos de texto plano por mensajes estructurados:
  ```rust
  let err_msg = ServerMessage::Error {
      correlation_id: None,
      room_id: Some(room_id),
      code: ErrorCode::BadRequest,
      message: "Failed to decode chunk message".to_string(),
  };
  let bytes = encode_message(&err_msg).unwrap_or_default();
  (StatusCode::BAD_REQUEST, [(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()
  ```

#### DEF-12: Guarda Defensiva contra Lotes Vacíos en `apply_batch`
* **Componentes**: `crates/storage/src/disk/mod.rs` y `crates/storage/src/memory/mod.rs`.
* **Solución Técnica**: Incorporar una guarda al inicio de `apply_batch`:
  ```rust
  if ops.is_empty() {
      return Ok(room.head_seq);
  }
  ```
  Evitando persistir tramas de 14 bytes con `count = 0` que corrompen a `WalReader`.

#### DEF-13: Encapsulamiento de Mapas Bidireccionales en `Schema`
* **Componentes**: `crates/core/src/schema/global.rs`.
* **Solución Técnica**: Hacer privados los campos `tables_by_id` e `id_by_name`. Exponer accesores inmutables:
  - `pub fn table_ids(&self) -> impl Iterator<Item = u16>`
  - `pub fn tables(&self) -> impl Iterator<Item = &TableSchema>`
  - Canalizar todas las inserciones y mutaciones a través del método validado `Schema::add_table`.

#### DEF-14: Erradicación del Centinela `table_id == 0` Mediante `Option<u16>`
* **Componentes**: `crates/core/src/schema/global.rs` y `crates/core/src/schema/table.rs`.
* **Solución Técnica**: En `TableBuilder`, tipar `table_id: Option<u16>` (inicializado en `None`). Si el usuario define `.table_id(0)`, se almacena `Some(0)`. En `Schema::add_table`, auto-asignar `max + 1` únicamente si `table_id` es `None`. En la deserialización de `Schema`, insertar directamente en los mapas validando unicidad sin auto-asignar.

#### DEF-15: Segregación de Capas en `SnapshotRelay` y Handlers de Red
* **Componentes**: `crates/server/src/relay.rs` y `crates/server/src/api/data_plane.rs`.
* **Solución Técnica**: Extraer las funciones HTTP de Axum (`upload_snapshot`, `request_chunk`, `upload_chunk`) y la autenticación ad-hoc hacia la capa de transporte (`api/data_plane.rs` o nuevo módulo `api/relay.rs`). Mantener `SnapshotRelay` como un servicio puro de almacenamiento desacoplado de Axum.

#### DEF-16: Registro de `last_ack_seq` en `ClientLeaseTracker` Durante `Commit`
* **Componentes**: `crates/server/src/actor/room.rs`.
* **Solución Técnica**: En `handle_commit`, actualizar explícitamente el cursor del cliente mediante:
  ```rust
  let _ = self.lease_tracker.record_ack(&client_id, last_ack_seq);
  ```
  Permitiendo que clientes activos en modo Write-Through avancen el suelo de retención de la sala y habiliten la poda proactiva de disco.

#### DEF-27: Liberación Previa de Iteradores `DashMap` en `reload_schema_for_rooms`
* **Componentes**: `crates/server/src/actor/manager.rs`.
* **Solución Técnica**: Recolectar previamente los pares `(RoomId, Sender)` en un vector local bajo el ámbito síncrono, liberando el iterador de `DashMap` antes de ingresar al bucle asíncrono con `.await`.

#### DEF-28: Tipado Fuerte de Errores de Protocolo en `ProtocolCodecError`
* **Componentes**: `crates/core/src/protocol/codec.rs` y `crates/server/src/api/data_plane.rs`.
* **Solución Técnica**: Definir `ProtocolCodecError` con variantes explícitas `VersionMismatch { expected: u8, got: u8 }` e `InvalidMagic`. Mapear `VersionMismatch` en `data_plane.rs` hacia `ServerError::ProtocolVersionMismatch` devolviendo HTTP 400 y `ErrorCode::ProtocolVersionMismatch`.

#### DEF-35: Hidratación Inversa y Acotada de la Caché LRU de Deduplicación
* **Componentes**: `crates/server/src/log/tiered_log.rs`.
* **Solución Técnica**: Eliminar la descompresión de Cold Disk en el arranque. Hidratar `DedupLruCache` exclusivamente a partir de `active.wal` y de los segmentos Warm más recientes leyendo en orden cronológico inverso (desde `head_seq` hacia atrás) y deteniéndose tan pronto se alcancen las 10.000 entradas de capacidad de la LRU.

#### DEF-51: Anclaje de Versión en `RequestSnapshotChunk`
* **Componentes**: `crates/core/src/protocol/messages.rs` y `crates/server/src/relay.rs`.
* **Solución Técnica**: Agregar `snapshot_head_seq: Option<SequenceNumber>` al mensaje `RequestSnapshotChunk`. En `SnapshotRelay`, indexar por `(RoomId, SequenceNumber)` y validar que cada chunk solicitado pertenezca al snapshot solicitado, evitando desgarros de versión ante cargas concurrentes.

#### DEF-52: Ordenamiento Monótono en Recuperación de Snapshots en Disco
* **Componentes**: `crates/server/src/relay.rs`.
* **Solución Técnica**: En `recover_disk_snapshots`, al iterar sobre `read_dir`, parsear `head_seq` de los archivos y ordenar descendentemente, o verificar `if recovered.head_seq > existing.head_seq` antes de insertar en `self.snapshots`. Purgar archivos residuales con secuencias inferiores.

#### DEF-53: Emisión de Señal SSE `RoomEvent::SnapshotRequested`
* **Componentes**: `crates/server/src/actor/command.rs` y `crates/server/src/api/sse.rs`.
* **Solución Técnica**: Extender `RoomEvent` con la variante `SnapshotRequested { target_seq: SequenceNumber }`. Cuando un cliente rezagado consulte deltas y no haya snapshot disponible en el relay, emitir este evento al canal SSE para que cualquier par activo conectado genere y suba un snapshot consolidado.

---

### 2.3. Mitigaciones para Defectos Medios y Bajos (DEF-17 a DEF-26, DEF-29 a DEF-33, DEF-36 a DEF-50, DEF-54 a DEF-57)

* **DEF-17 (BTreeMap Cloning)**: Reemplazar el clonado profundo por estructuras inmutables persistentes con bifurcación CoW en $O(\log N)$ o granular cerrojos por tabla en el motor de persistencia.
* **DEF-18 (Compactor RoomId)**: Incorporar `pub room_id: RoomId` explícito en `DiskRoomState` en `open_room`, erradicando el reverse-engineering desde rutas de archivo.
* **DEF-19 (Fsync y sync_dir)**: Estandarizar la rutina atómica de persistencia en metadatos: escribir `.tmp`, ejecutar `file.sync_all()`, renombrar con `fs::rename` y sincronizar el directorio contenedor con `sync_dir(parent)` propagando errores de I/O.
* **DEF-20 (TableBuffer pending)**: Hacer privado `TableBuffer::pending` y canalizar todas las mutaciones a través de `apply()`.
* **DEF-21 (Operation y ColumnDef)**: Encapsular campos privados en `Operation` y `ColumnDef`, preservando invariantes de orden DDL y alineación de PK.
* **DEF-22 (Axum IntoResponse)**: Remover `impl IntoResponse for ServerError` y definir extractores diferenciados que emitan `ServerMessage::Error` binario en el Data Plane.
* **DEF-23 (LSP en apply_snapshot)**: Unificar el contrato de `StorageEngine`: exigir explícitamente en ambas implementaciones que la sala deba haber sido abierta previamente mediante `open_room`.
* **DEF-24 (Poda en DeregisterClient)**: Invocar `min_connected_ack_seq()` y `prune_older_than` al procesar `RoomCommand::DeregisterClient`.
* **DEF-25 (Duplicación WAL)**: Delegar la decodificación de WAL en `recovery.rs` y `tiered_log.rs` a `WalReader` y `wal_frame.rs`.
* **DEF-26 (Timeout Bootstrapping)**: Transicionar clientes en `Bootstrapping` directamente a `Dormant` ante timeout, sin pasar por `Disconnected` para no bloquear la poda de clientes activos.
* **DEF-29 (Delta gaps)**: Propagar estrictamente los errores de `fetch_deltas` en `handle_commit` sin sustituciones sintéticas incompletas.
* **DEF-30 (Clonación PK en buffer)**: Consultar `self.pending.get_mut(&op.pk)` por referencia antes de clonar `op.pk`.
* **DEF-31 (Limpieza spawn_locks y relay)**: En `delete_room`, remover la clave de `spawn_locks` e invocar `snapshot_relay.purge_room(room_id)`.
* **DEF-32 (Límite bincode WAL)**: Configurar `bincode::DefaultOptions::new().with_limit(MAX_MESSAGE_SIZE)` en `decode_wal_batch_from_slice`.
* **DEF-33 (Zero-copy en encode_wal_batch)**: Serializar un struct referencial `WalBatchPayloadRef<'a> { mutation_id, ops: &'a [SequencedOperation] }` sin clonar con `to_vec()`.
* **DEF-36 (Off-by-one Fast Path)**: Ajustar la guarda a `if from_seq.get() + 1 >= min_ram.get()`.
* **DEF-37 (Fuga de memoria en reposo)**: Invocar `hot_buffer.apply_sliding_window` dentro de `run_maintenance` cada 500 ms.
* **DEF-38 (Streaming en SnapshotRelay)**: Eliminar `StagedSnapshot.data: Bytes` y leer fragmentos bajo demanda desde disco con `File::read_at`.
* **DEF-39 (Zip-Bomb)**: Validar `uncompressed_len <= MAX_SNAPSHOT_SIZE` y utilizar decodificador en streaming acotado con `Read::take`.
* **DEF-40 (Límites de chunks)**: Homogeneizar el límite de subida en `stage_chunk` a 4 MB y acotar `chunk_size` en `get_chunk` con `.clamp(64 * 1024, 4 * 1024 * 1024)`.
* **DEF-41 (Delimitadores en tokens)**: Parsear tokens desde la derecha mediante `rsplitn(3, '.')` para aislar la firma y el timestamp sin verse afectado por puntos en IDs.
* **DEF-42 (Endpoint descarga snapshot)**: Implementar `GET /rooms/:room_id/snapshot/download` en `relay.rs` y montarlo en `build_router`.
* **DEF-43 (Códigos HTTP en Data Plane)**: Retornar `StatusCode::UNAUTHORIZED` (401) ante fallos de autorización y `StatusCode::BAD_REQUEST` (400) ante payloads incompatibles.
* **DEF-44 (Doble cerrojo en queries)**: Resolver la traducción de nombre a ID y la lectura tabular bajo una única adquisición del cerrojo de lectura.
* **DEF-45 (MutationId en WalReader)**: Almacenar `(SequencedOperation, Option<MutationId>)` en la cola de `pending_ops` de `WalReader`.
* **DEF-46 (Margen HTTP 16 MB)**: Configurar `DefaultBodyLimit::max(17 * 1024 * 1024)` para permitir cargas útiles de 16 MB más los 4 bytes de cabecera wire.
* **DEF-47 (Firma StorageEngine::scan)**: Renombrar el parámetro en `scan` de `table_id: &str` a `table: &str`.
* **DEF-48 (Graceful Shutdown)**: Interceptar `tokio::signal::ctrl_c()` en `main.rs` e invocar `room_manager.shutdown_all().await` antes de salir.
* **DEF-49 (Desacoplamiento WAL lock)**: Separar el cerrojo de append a disco del WAL del cerrojo de lectura/escritura de las tablas en memoria en `DiskStorageEngine`.
* **DEF-50 (I/O storm en mantenimiento)**: Incrementar el temporizador de mantenimiento a 30 segundos y mantener contadores cacheados en memoria.
* **DEF-54 (Zstd en WebAssembly)**: Integrar una implementación Zstandard pura en Rust (`ruzstd`) condicionada a `cfg(target_arch = "wasm32")`.
* **DEF-55 (Reaper de salas inactivas)**: Implementar auto-apagado de actores Tokio tras 15 minutos sin clientes conectados ni comandos.
* **DEF-56 (Proyección CompactRow)**: Definir `ProjectedRow` o rellenar con `Value::Null` las columnas omitidas para preservar la correspondencia DDL.
* **DEF-57 (Dependencias zemdb-client)**: Declarar `zemdb-storage` en `crates/client/Cargo.toml` y aislar Tokio y Zstd bajo `cfg(not(target_arch = "wasm32"))`.

---

## 3. Resolución de Compensaciones (*Trade-offs*) Arquitectónicas

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                RESOLUCIÓN DE TRADE-OFFS                                          │
├────────────────────────────┬─────────────────────────────────────────────────────────────────────┤
│ 1. Group Commit vs Latencia│ Adopción de micro-batching cooperativo en RoomActor (max 2ms / 64)  │
│ 2. Snapshot Relay Storage  │ Streaming desacoplado en disco (cero RAM, O(1) buffer)              │
│ 3. Evolución DDL Dinámica  │ Trait reload_schema con reemplazo atómico Arc<Schema>               │
│ 4. Portabilidad WASM       │ Decodificador ruzstd puro para navegador sin bindings C             │
│ 5. Poda de Logs en 1-RTT   │ Avance de last_ack_seq en commit sin perder desacoplamiento de ACK │
└────────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

### 3.1. Trade-off 1: Group Commit vs. Latencia de Confirmación 1-RTT en Tokio
* **Conflicto**: Ejecutar `sync_data()` síncronamente en cada commit satura el disco y bloquea el hilo Tokio (DEF-08). Sin embargo, un Group Commit diferido puede incrementar la latencia individual percibida por clientes concurrentes.
* **Resolución**: Implementar un micro-batching cooperativo no bloqueante en `RoomActor`. Al procesar un comando `Commit`, drenar todas las mutaciones pendientes en el `mpsc::Receiver` mediante `try_recv()` (hasta un tope de 64 operaciones o ventana de 2 ms), escribir el lote consolidado en el WAL y emitir un único `sync_data().await`. Si no hay más comandos en cola, confirmar de inmediato sin introducir demoras artificiales.

### 3.2. Trade-off 2: Snapshot Relay en RAM vs. Streaming Desacoplado en Disco
* **Conflicto**: Mantener snapshots completos en memoria RAM minimiza la latencia de despacho en red pero vulnera el objetivo de huella de memoria ultra-baja en el servidor (DEF-38), arriesgando OOMs con datasets grandes.
* **Resolución**: Adoptar streaming desacoplado en disco con búfer acotado. `SnapshotRelay` almacena los fragmentos subidos en un archivo temporal en disco y `get_chunk` lee directamente del archivo empleando fragmentos de 2 MB a 4 MB. El servidor mantiene un uso de memoria constante $O(1)$ por transferencia concurrente.

### 3.3. Trade-off 3: Trait `StorageEngine::reload_schema` vs. Inmutabilidad de Motor
* **Conflicto**: Permitir la mutación del esquema en caliente en un motor de almacenamiento abierto puede inducir condiciones de carrera en validaciones posicionales concurrentes.
* **Resolución**: Utilizar punteros atómicos inmutables `Arc<Schema>`. `StorageEngine::reload_schema` valida que el nuevo esquema sea estrictamente compatible (append-only) y reemplaza el puntero atómicamente bajo el cerrojo de la sala, garantizando que transacciones en vuelo lean una instantánea consistente sin riesgo de lecturas corruptas.

### 3.4. Trade-off 4: Portabilidad Universal de Snapshots `ZMSN` vs. Dependencias Nativas de C
* **Conflicto**: La biblioteca estándar `zstd` depende de código C no compilable en `wasm32-unknown-unknown` (DEF-54). Desactivar la compresión en WASM rompe la interoperabilidad con nodos nativos.
* **Resolución**: Incorporar la biblioteca `ruzstd` (descompresor Zstd puro en Rust) bajo el target `wasm32`. De este modo, los navegadores pueden descomprimir snapshots generados por servidores nativos sin necesidad de alterar el formato canónico `ZMSN`.

### 3.5. Trade-off 5: Avance de Cursor de Retención en `Commit` vs. Desacoplamiento de ACK
* **Conflicto**: En la Fase 3 se desacopló `Ack` de `Commit` para evitar avances prematuros de cursor. Sin embargo, no registrar `last_ack_seq` en `Commit` congela la poda proactiva para clientes en Write-Through activo (DEF-16).
* **Resolución**: Registrar el avance del cursor confirmado hasta `last_ack_seq` (los deltas que el cliente ya aplicó antes de emitir la nueva mutación), sin avanzar optimistamente a la mutación actual. Esto preserva la garantía de que el cursor representa datos físicamente consolidados en el cliente mientras habilita la poda continua del log en disco.

---

## 4. Hoja de Ruta Priorizada de Remediación por Fases

El plan maestro se estructura en 4 subfases de implementación secuencial:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                             CRONOGRAMA DE IMPLEMENTACIÓN                                         │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│  FASE 3.6-A: Durabilidad Crítica, WAL y Consistencia de Log               [Prioridad Inmediata] │
│  FASE 3.6-B: Protocolo Wire, Framing, Control de Acceso y Redundancia     [Prioridad Alta]      │
│  FASE 3.6-C: Arquitectura de Persistencia, Concurrencia Tokio y Livelocks [Prioridad Alta]      │
│  FASE 3.6-D: Robustez de Dominio, Encapsulamiento y Portabilidad WASM     [Prioridad Media]     │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Fase 3.6-A: Durabilidad Crítica, WAL y Consistencia de Log (Prioridad Inmediata)
* **Objetivo**: Erradicar el riesgo de pérdida permanente de datos ante caídas y restaurar la contigüidad monótona estricta en el log escalonado.
* **Defectos a Subsanar**: `DEF-01`, `DEF-02`, `DEF-03`, `DEF-04`, `DEF-07`, `DEF-12`, `DEF-36`.
* **Entregables de Código**:
  1. Refactorización de `HotBuffer::get_range` y jerarquía cronológica en `TieredLog::fetch_deltas` (DEF-01, DEF-36).
  2. Implementación de reemplazo atómico con archivo temporal y guard RAII en `compact_room_cow` (DEF-02).
  3. Reestructuración de `recover_room` con archivo de fusión temporal y borrado post-sync de `wal.compacting` (DEF-03).
  4. Guarda preventiva de retención en el paso 1 de `handle_commit` (DEF-04).
  5. Inclusión de `active_start_seq()` en el cálculo de `tail_seq` en `tiered_log.rs` (DEF-07).
  6. Guarda defensiva ante lotes vacíos en `DiskStorageEngine::apply_batch` (DEF-12).
* **Pruebas de Verificación**:
  - Test de inyección de crash en medio de la fusión de WAL compacting comprobando cero pérdida de datos.
  - Test de saturación de RAM en `HotBuffer` validando que `/sync` mantenga contigüidad monótona desde `active.wal`.

### Fase 3.6-B: Protocolo Wire, Framing, Control de Acceso y Redundancia (Prioridad Alta)
* **Objetivo**: Blindar la capa de red HTTP/2 y los contratos binarios Bincode, garantizando la reintegración fluida de clientes.
* **Defectos a Subsanar**: `DEF-05`, `DEF-06`, `DEF-10`, `DEF-11`, `DEF-16`, `DEF-22`, `DEF-26`, `DEF-28`, `DEF-29`, `DEF-40`, `DEF-41`, `DEF-42`, `DEF-43`, `DEF-46`, `DEF-51`, `DEF-52`, `DEF-53`.
* **Entregables de Código**:
  1. Reintegración automática de clientes `Dormant` y `Bootstrapping` con cursores contemporáneos (DEF-05, DEF-26).
  2. Inclusión de mutaciones en deduplicación idempotente y propagación estricta de errores en `Commit` (DEF-06, DEF-29).
  3. Validación de admisibilidad y límites de secuencia en `upload_snapshot` y `upload_chunk` (DEF-10).
  4. Enmarcado binario `ServerMessage::Error` en todas las respuestas de error del Data Plane y extractor `ClientAuth` (DEF-11, DEF-22, DEF-43).
  5. Avance de `last_ack_seq` y disparo de poda proactiva en `handle_commit` (DEF-16).
  6. Tipado fuerte `ProtocolCodecError` y mapeo a HTTP 400 `ProtocolVersionMismatch` (DEF-28).
  7. Ajuste perimetral a 17 MB en Axum `DefaultBodyLimit` y soporte de chunks de 4 MB (DEF-40, DEF-46).
  8. Descomposición robusta de tokens mediante `rsplitn` (DEF-41).
  9. Montaje del endpoint `GET /rooms/:room_id/snapshot/download` en `router.rs` (DEF-42).
  10. Anclaje de versión en `RequestSnapshotChunk`, ordenamiento en `recover_disk_snapshots` y señal SSE `SnapshotRequested` (DEF-51, DEF-52, DEF-53).
* **Pruebas de Verificación**:
  - Test E2E de reintento idempotente de commit bajo partición de red con verificación de payload.
  - Test de rechazo perimetral binario con magic bytes válidos ante tokens expirados o corruptos.

### Fase 3.6-C: Arquitectura de Persistencia, Concurrencia Tokio y Livelocks (Prioridad Alta)
* **Objetivo**: Erradicar el bloqueo del reactor Tokio, resolver la inanición de lecturas y desacoplar los subsistemas de almacenamiento.
* **Defectos a Subsanar**: `DEF-08`, `DEF-17`, `DEF-18`, `DEF-19`, `DEF-23`, `DEF-27`, `DEF-34`, `DEF-35`, `DEF-38`, `DEF-44`, `DEF-48`, `DEF-49`, `DEF-50`, `DEF-55`.
* **Entregables de Código**:
  1. Aislamiento de llamadas de disco y Zstd síncronas en `tokio::task::spawn_blocking` (DEF-08).
  2. Micro-batching cooperativo en `RoomActor` e incorporación de `room_id` explícito en `DiskRoomState` (DEF-18).
  3. Estandarización de `sync_all()` y `sync_dir()` en metadata de salas, esquemas y leases (DEF-19).
  4. Unificación de precondiciones de sala abierta en `StorageEngine::apply_snapshot` (DEF-23).
  5. Liberación de iteradores `DashMap` antes de `.await` en `reload_schema_for_rooms` (DEF-27).
  6. Implementación de `StorageEngine::reload_schema` en Memory y Disk (DEF-34).
  7. Hidratación acotada de la LRU desde el final del log Warm/Active sin descomprimir Cold Disk (DEF-35).
  8. Streaming desacoplado en disco para `SnapshotRelay` erradicando retención en RAM (DEF-38).
  9. Unificación de cerrojo de lectura en consultas tabulares por nombre (DEF-44).
  10. Implementación de graceful shutdown con `ctrl_c()` en `main.rs` (DEF-48).
  11. Desacoplamiento del cerrojo WAL frente al cerrojo de tablas en RAM en `DiskStorageEngine` (DEF-49).
  12. Mitigación del storm de mantenimiento periódico y reaper de salas inactivas (DEF-50, DEF-55).
* **Pruebas de Verificación**:
  - Test de estrés de concurrencia midiendo latencia de lectura durante escrituras pesadas con `sync_data`.
  - Test de evolución de esquema DDL en cliente local verificando la aplicación continua de mutaciones.

### Fase 3.6-D: Robustez de Dominio, Encapsulamiento, Portabilidad WASM y Rendimiento Zero-Copy (Prioridad Media)
* **Objetivo**: Perfeccionar el encapsulamiento de contratos en `zemdb-core`, habilitar soporte WebAssembly y erradicar alocaciones superfluas.
* **Defectos a Subsanar**: `DEF-09`, `DEF-13`, `DEF-14`, `DEF-15`, `DEF-20`, `DEF-21`, `DEF-24`, `DEF-25`, `DEF-30`, `DEF-31`, `DEF-32`, `DEF-33`, `DEF-37`, `DEF-39`, `DEF-45`, `DEF-47`, `DEF-54`, `DEF-56`, `DEF-57`.
* **Entregables de Código**:
  1. Validación estricta en `decode_wal_record_from_slice` y soporte de `MutationId` en `WalReader` (DEF-09, DEF-45).
  2. Encapsulamiento privado de `tables_by_id`, `TableBuffer::pending`, `Operation` y `ColumnDef` (DEF-13, DEF-20, DEF-21).
  3. Erradicación del centinela `table_id == 0` mediante `Option<u16>` en builders (DEF-14).
  4. Extracción de endpoints HTTP fuera de `relay.rs` hacia la capa `api/` (DEF-15).
  5. Poda proactiva de disco en `DeregisterClient` y limpieza en `delete_room` (DEF-24, DEF-31).
  6. Unificación DRY de parsing WAL delegando a `core` (DEF-25).
  7. Búsqueda por referencia en `TableBuffer::apply` y struct referencial en `encode_wal_batch` (DEF-30, DEF-33).
  8. Límite DoS en deserialización WAL y validación de tamaño en descompresión de snapshots (DEF-32, DEF-39).
  9. Poda periódica de RAM en reposo dentro de `run_maintenance` (DEF-37).
  10. Corrección de nomenclatura `table: &str` en `StorageEngine::scan` (DEF-47).
  11. Integración de `ruzstd` en `zemdb-storage` para descompresión de snapshots en `wasm32` (DEF-54).
  12. Formalización de `ProjectedRow` en projection pushdown (DEF-56).
  13. Declaración de dependencias y aislamiento por target en `crates/client/Cargo.toml` (DEF-57).
* **Pruebas de Verificación**:
  - Compilación cruzada exitosa para target `wasm32-unknown-unknown` de `zemdb-storage` y `zemdb-client`.
  - Tests unitarios de encapsulamiento e integridad de squashing y projection pushdown.

---

## 5. Resumen de Gobernanza Técnica y Próximos Pasos

Con la emisión de este Plan Maestro de Mitigación y Corrección (`docs/proposals/2026-09-fase3_5-proposal.md`) y el Inventario Exhaustivo de Defectos (`docs/audits/2026-09-fase3_5-audit.md`), el proceso de auditoría técnica post-Fase 3.5 queda formal y rigurosamente concluido en modo de **SOLO LECTURA**.

No se ha alterado ninguna línea de código funcional (`.rs`, `Cargo.toml`). El repositorio queda en un estado limpio, ordenado y preparado para que el equipo de desarrollo inicie la ejecución de la Fase 3.6 siguiendo las especificaciones detalladas en este documento.
