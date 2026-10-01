# Bitácora y Plan de Ejecución — Fase 3: `zemdb-server`

**Documento:** Registro de Avance, Arquitectura de Detalle y Plan de Acción  
**Fecha de Creación:** 24 de Septiembre de 2026  
**Documentos de Referencia:**
* [ROADMAP.md](../ROADMAP.md) (Secciones 4.3, 5 y 6)
* [ARCHITECTURE.md](../ARCHITECTURE.md) (Secciones 5, 6 y 7)
* [docs/proposals/2026-09-fase3-proposal.md](proposals/2026-09-fase3-proposal.md)

---

## 1. Estado Global de la Fase 3

* **Estado Actual:** 🟢 **COMPLETADA — Fase 3: `zemdb-server` (100% Finalizada)**
* **Próxima Fase:** Iniciar Fase 4: `zemdb-client` (SDK Cliente, Transacciones Locales y Sincronización 1-RTT).

---

## 2. Mapa de Hitos y Tareas

```
┌────────────────────────────────────────────────────────────────────────┐
│                   ESTRATEGIA DE EJECUCIÓN FASE 3                       │
├────────────────────────────────────────────────────────────────────────┤
│  HITO 3.1: Fundaciones, Micro-WAL y Deduplicación LRU      [COMPLETADO]│
│  HITO 3.2: Log Inmutable de 4 Niveles (Tiered Delta Log)   [COMPLETADO]│
│  HITO 3.3: Concurrencia de Salas, Actores Tokio y Leases   [COMPLETADO]│
│  HITO 3.4: Capa de Red Axum, Control/Data Plane y SSE      [COMPLETADO]│
└────────────────────────────────────────────────────────────────────────┘
```

---

### Hito 3.1: Fundaciones, Micro-WAL y Deduplicación LRU

- [x] **3.1.1. Dependencias del Workspace:**
  - Agregar a `crates/server/Cargo.toml` las dependencias necesarias: `dashmap`, `lru`, `toml`, `tempfile`, `crc32fast`, `tracing`, `tracing-subscriber`, `tokio` (full), `axum`.
- [x] **3.1.2. Separación Arquitectónica `lib.rs` / `main.rs`:**
  - `src/lib.rs`: Biblioteca reutilizable que exporta la máquina de actores, buffer, micro-WAL, configuración y capas de handlers para levantar el servidor in-process en tests sin spawn de procesos externos.
  - `src/main.rs`: Entrypoint ejecutable CLI que parsea argumentos, inicializa tracing, carga configuración y lanza Tokio + Axum.
- [x] **3.1.3. Configuración (`src/config.rs`):**
  - Struct `ServerConfig` deserializable con soporte para archivo `server.toml` y variables de entorno (`ZEMDB_PORT`, `ZEMDB_DATA_DIR`, `ZEMDB_AUTH_SECRET`, `ZEMDB_ADMIN_SECRET`, etc.).
- [x] **3.1.4. Sistema de Errores Tipados (`src/error.rs`):**
  - Enum `ServerError` con conversiones idiomáticas hacia `ErrorCode` del protocolo binario de `zemdb-core` y códigos de estado HTTP de Axum (`StatusCode`).
- [x] **3.1.5. Motor de Micro-WAL Durable (`src/micro_wal.rs`):**
  - Persistencia síncrona append-only en `meta_{room_id}.wal` con formato:
    ```text
    [magic: 2B (0x4D 0x57)][seq: u64][mutation_id: [u8; 16]][client_id_len: u16][client_id_str][crc32: u32]
    ```
  - Detección de *torn writes* por ceros al EOF o truncado físico en caídas de energía.
  - Recuperación post-crash (`recover_room`): lectura secuencial para extraer el último `head_seq` emitido e hidratar la caché LRU.
- [x] **3.1.6. Deduplicación Exactly-Once (`src/dedup.rs`):**
  - Caché LRU de tamaño configurable (`LruCache<MutationId, SequenceNumber>`).
  - Semántica idempotente: ante mutaciones duplicadas por reintentos de red, retorna la secuencia ya asignada sin volver a escribir en el log.
- [x] **3.1.7. Suite de Pruebas Unitarias del Hito 3.1:**
  - `tests/micro_wal_tests.rs`: Tests de persistencia, recuperación de `head_seq`, resistencia a corrupción CRC32, truncado de escrituras incompletas y deduplicación Exactly-Once (7 tests pasando).

---

### Hito 3.2: Jerarquía del Log Inmutable de 4 Niveles (Tiered Delta Log)

- [x] **3.2.1. Tier 1: Hot Buffer en RAM (`src/log/hot_buffer.rs`):**
  - Buffer contiguo e inmutable en memoria (`VecDeque<SequencedOperation>`).
  - **Cero squashing sobre deltas secuenciados**: preservación estricta de contigüidad (`head_seq + 1`), garantizando que no existan huecos de secuencia ni tuplas zombi.
- [x] **3.2.2. Tier 2: Warm Disk Log con Write-Through (`src/log/warm_disk.rs`):**
  - Archivos append-only `.wal` planos con códec `wal_frame` de `zemdb-core` (`0xBA7C`, CRC32, longitud).
  - Write-Through: escrituras síncronas en `active.wal` garantizando durabilidad pre-ACK ante caídas del servidor.
  - Rotación y sellado a `segment_{start}_{end}.wal`.
- [x] **3.2.3. Tier 3: Cold Disk Log (`src/log/cold_disk.rs`):**
  - Compresión periódica de segmentos `.wal` antiguos a `.wal.zst` usando Zstandard.
  - Descompresión en memoria y lectura de rangos.
- [x] **3.2.4. Política de Ciclo de Vida y Evicción (`src/log/policy.rs`):**
  - Implementación de `RoomLifecyclePolicy` (`ram_max_ops`, `ram_ttl`, `warm_disk_ttl`, `cold_disk_ttl`, `max_room_disk_bytes`).
  - Mantenimiento del cursor base `tail_seq`. Si un cliente solicita sincronizar con `last_ack_seq < tail_seq`, se rechaza con `ErrorCode::BehindCompaction`.
- [x] **3.2.5. Motor Unificado de Lectura Multi-Tier (`src/log/tiered_log.rs`):**
  - Función `fetch_deltas(from_seq, max_batch_size)` que consulta de forma transparente a través de Cold Disk, Warm Disk y RAM HotBuffer, retornando un lote contiguo y el flag `has_more`.
- [x] **3.2.6. Suite de Pruebas Unitarias del Hito 3.2:**
  - `tests/tiered_log_tests.rs`: Tests de Write-Through, crash recovery, rotación, compresión Zstd, query continuo multi-tier, BehindCompaction y poda proactiva por cursores (8 tests pasando).
- [x] **3.2.7. Poda Proactiva Guiada por Cursores (`TieredLog::prune_older_than`):**
  - Truncación física inmediata de segmentos `.wal` y `.wal.zst` cuando `end_seq < target_seq`, evitando almacenar deltas innecesarios una vez leídos unánimemente.

---

### Hito 3.3: Concurrencia de Salas, Actores Tokio y Leases

- [x] **3.3.1. Catálogo de Esquemas (`src/schema_registry.rs`):**
  - Gestión concurrente en memoria (`DashMap<SchemaId, Arc<Schema>>`).
  - Persistencia durable en disco en `{data_dir}/schemas/{schema_id}.json`.
  - Soporte de evolución de esquema DDL append-only (`add_column` nullable).
- [x] **3.3.2. Gestor de Leases, Roster Persistente y Poda Proactiva (`src/actor/lease.rs`):**
  - `ClientLeaseTracker`: registra miembros registrados, cursor `last_ack_seq`, estado tripartito (`Connected`, `Disconnected`, `Dormant`) y marca temporal del último heartbeat.
  - Catálogo persistido de miembros por sala (`meta_clients_{room_id}.json` o WAL) para tracking consistente ante reinicios.
  - Poda proactiva unánime (`TieredLog::prune_older_than`) cuando todos los clientes en `Connected` confirmaron hasta la secuencia $S$.
  - Transición a `Disconnected` al perderse el heartbeat o cerrarse la app. Si regresa antes de la purga física, hace catch-up con `/sync` y vuelve a `Connected`.
  - Transición a `Dormant` cuando los deltas pendientes de un cliente desconectado son purgados físicamente por TTL/cuota (`last_ack_seq < tail_seq - 1`). Exclusión de retención de log para evitar bloqueo de disco a miembros activos.
  - Al reconectarse un cliente `Dormant`, se responde `BehindCompaction` y se requiere snapshot consolidado.
  - Soporte de desregistro voluntario inmediato con `DeregisterClient` para removerlo del roster.
- [x] **3.3.3. Contrato de Comandos (`src/actor/command.rs`):**
  - Definición de `RoomCommand` (`RegisterClient`, `GetSchema`, `Commit`, `Sync`, `Ack`, `Heartbeat`, `DeregisterClient`, `ReloadSchema`, `GetMetrics`, `GetClientCursor`, `SubscribeEvents`).
- [x] **3.3.4. Actor de Sala Tokio (`src/actor/room.rs`):**
  - Tarea Tokio aislada por sala con canal `mpsc::channel(1024)` (*lock-free sharding*).
  - Secuenciador monótono atómico (`head_seq += 1`).
  - Validación de esquema en $O(C)$ antes de secuenciar.
  - Escritura síncrona en Micro-WAL y Write-Through en TieredLog antes de emitir confirmación (`CommitAck`).
  - Separación estricta de responsabilidades: `Sync` solo lee deltas y renueva liveness; `Commit` secuenciación y 1-RTT catchup; `Heartbeat` liveness puro; `Ack` avanza el cursor confirmado e inicia poda proactiva inmediata.
  - Despacho de eventos SSE livianos `Event::HeadAdvanced(seq)` vía `tokio::sync::broadcast`.
- [x] **3.3.5. Gestor de Salas Shardeado (`src/actor/manager.rs`):**
  - `RoomManager` con `DashMap<RoomId, mpsc::Sender<RoomCommand>>`.
  - *Lazy Spawning*: inicialización perezosa del actor ante la primera petición entrante vinculada a su `SchemaId` y persistencia de metadata en `meta_room.json`.
- [x] **3.3.6. Suite de Pruebas Unitarias del Hito 3.3:**
  - `tests/room_actor_tests.rs`: Concurrencia de 5 clientes concurrentes (100 commits), validación de esquemas, idempotencia Exactly-Once, ciclo de vida Connected/Disconnected/Dormant/BehindCompaction, verificación de que el cursor avanza exclusivamente tras `Ack` explícito y recuperación post-crash (7 tests pasando).

---

### Hito 3.4: Capa de Red Axum, Control/Data Plane y SSE

- [x] **3.4.1. Control Plane REST (`src/api/control_plane.rs`):**
  - Endpoints administrativos JSON con Bearer Token:
    - `POST /admin/schemas`, `GET /admin/schemas/{id}`, `POST /admin/schemas/{id}/columns`.
    - `POST /admin/rooms`, `GET /admin/rooms/{id}`, `DELETE /admin/rooms/{id}`.
- [x] **3.4.2. Data Plane HTTP/2 Binario (`src/api/data_plane.rs`):**
  - Handlers binarios de alta velocidad serializados con `bincode`:
    - `POST /rooms/{room_id}/register`: Handshake inicial con verificación stateless de `auth_token` y entrega de `Schema` en 1 RTT.
    - `POST /rooms/{room_id}/commit`: Secuenciación y respuesta `CommitAck` con catch-up deltas en 1 RTT.
    - `POST /rooms/{room_id}/sync`: Sincronización paginada con backpressure (lectura sin efecto secundario en cursores).
    - `POST /rooms/{room_id}/ack`: Acuse explícito de persistencia local por cliente (`ack_seq`) y disparo de poda inmediata.
    - `POST /rooms/{room_id}/heartbeat`: Renovación liviana de lease de cliente (liveness puro, sin avance de cursor).
    - `POST /rooms/{room_id}/schema`: Consulta bajo demanda de esquema vigente.
    - `POST /rooms/{room_id}/deregister`: Desconexión ordenada inmediata.
- [x] **3.4.3. Canal SSE Signal-Only (`src/api/sse.rs`):**
  - `GET /rooms/{room_id}/events`: Canal Server-Sent Events que emite exclusivamente la señal liviana `Event::HeadAdvanced(seq)` vía `tokio::sync::broadcast`.
- [x] **3.4.4. Relay Efímero de Snapshots (`src/relay.rs`):**
  - Retransmisión multipart en streaming de chunks comprimidos para onboarding de salas $>16\text{ MB}$ con verificación BLAKE3.
- [x] **3.4.5. Router Axum y Ensamblado (`src/api/router.rs`):**
  - Configuración del router Axum, middlewares de tracing y límites de tamaño (16 MB DoS envelope).
- [x] **3.4.6. Batería de Pruebas de Integración Multi-Cliente E2E:**
  - `tests/server_integration_tests.rs`: Clientes concurrentes interactuando mediante HTTP/2 con el servidor in-memory/real TCP, probando commit atómico 1-RTT, sync incremental, ack explícito, reconexiones, relay BLAKE3 y reanudación SSE (8 tests pasando).

---

## 3. Registro Histórico de Decisiones y Cambios

- **2026-09-25: Desacoplamiento de ACK y Heartbeat:**
  - Se introdujo `ClientMessage::Ack` y `RoomCommand::Ack` explícito. `Sync` y `Commit` ya no avanzan optimistamente el cursor en el servidor, y `Heartbeat` queda reservado exclusivamente a liveness/keep-alive.
  - El cursor `last_ack_seq` de un cliente en el servidor avanza únicamente cuando el cliente confirma que los deltas fueron aplicados y persistidos localmente (`POST /rooms/{room_id}/ack`).
  - Esto previene pérdidas de datos por desconexión en tránsito y permite poda proactiva de logs en disco a latencia cero una vez que todos los clientes conectados envían su ACK.


| Fecha | Tarea / Hito | Archivos Modificados | Resumen de Cambios / Decisiones |
| :--- | :--- | :--- | :--- |
| **2026-09-24** | Inicialización Fase 3 | `docs/FASE3_TRACKING.md` | Creación de la bitácora y plan de ejecución detallado en 4 hitos. |
| **2026-09-25** | Hito 3.1: Fundaciones, Micro-WAL y Deduplicación LRU | `Cargo.toml`, `crates/server/Cargo.toml`, `src/lib.rs`, `src/main.rs`, `src/config.rs`, `src/error.rs`, `src/micro_wal.rs`, `src/dedup.rs`, `tests/micro_wal_tests.rs` | Implementación completa de Micro-WAL durable con CRC32 y torn write recovery, DedupLruCache Exactly-Once, ServerConfig con TOML/env y ServerError con mapeo a Axum/ErrorCode. 7 tests unitarios pasando. |
| **2026-09-25** | Hito 3.2: Log Inmutable de 4 Niveles (Tiered Delta Log) | `crates/server/Cargo.toml`, `src/log/policy.rs`, `src/log/hot_buffer.rs`, `src/log/warm_disk.rs`, `src/log/cold_disk.rs`, `src/log/tiered_log.rs`, `src/log/mod.rs`, `src/lib.rs`, `tests/tiered_log_tests.rs` | Implementación de la jerarquía de 4 niveles con Write-Through duradero (HotBuffer RAM + active.wal), rotación y sellado de segmentos, compresión Zstd en background, poda por cuota/retención con límite BehindCompaction, poda proactiva por cursor unánime (prune_older_than) y motor unificado fetch_deltas. 8 tests unitarios pasando. |
| **2026-09-25** | Hito 3.3: Concurrencia de Salas, Actores Tokio y Leases | `src/schema_registry.rs`, `src/actor/lease.rs`, `src/actor/command.rs`, `src/actor/room.rs`, `src/actor/manager.rs`, `src/actor/mod.rs`, `src/lib.rs`, `tests/room_actor_tests.rs` | Implementación del modelo de actores Tokio por sala con canal mpsc, SchemaRegistry con persistencia JSON y evolución append-only, ClientLeaseTracker con ciclo tripartito (Connected/Disconnected/Dormant), sincronización 1-RTT, canales broadcast SSE, lazy spawning en RoomManager y persistencia de metadatos de sala. Desacoplamiento estricto de Ack y Heartbeat. 7 tests de integración pasando. |
| **2026-09-25** | Hito 3.4: Capa de Red Axum, Control/Data Plane y SSE | `crates/server/Cargo.toml`, `src/api/auth.rs`, `src/api/control_plane.rs`, `src/api/data_plane.rs`, `src/api/sse.rs`, `src/relay.rs`, `src/api/router.rs`, `src/api/mod.rs`, `src/lib.rs`, `src/main.rs`, `tests/server_integration_tests.rs` | Implementación de la capa de red Axum con Control Plane REST JSON (schemas, evolution, rooms, metrics), Data Plane binario con bincode (register, commit, sync, ack, heartbeat, deregister), canal SSE signal-only (head_advanced), relay de snapshots multipart con chunks y validación criptográfica BLAKE3, y suite de integración E2E completa. 8 tests de integración pasando (93 tests totales en workspace). |
| **2026-09-30** | Fase 3.5-C.1: Refactorización Estructural, Contratos y Rendimiento | `crates/core`, `crates/storage`, `crates/server` | Enmarcado de wire protocol con magic bytes `RM` y versión `0x01` (`codec.rs`), snapshots canónicos universales interoperables `ZMSN` con CRC32 (`snapshot.rs`), acceso directo por `table_id: u16` (`get_by_id`, `scan_by_id`) y validación de esquemas en `apply_batch`, encapsulación de `PrimaryKey`, `CompactRow` y `TableSchema` con remoción de `Deref`, corrección de LWW en squashing y anulación mutua `Insert + Delete -> Purged`, emisión SSE de `RoomEvent::SchemaReloaded` ante DDL, estandarización de `ServerMessage::DeregisterAck`, transición de leases `Disconnected -> Dormant` tras 90s, recálculo exacto de `tail_seq` en `tiered_log.rs`, liberación de locks antes de sync en `close_room`, y prevención de salas fantasma en `GET /admin/rooms/:id`. Suite completa pasando sin warnings. |

