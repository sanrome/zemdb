# INFORME DE EVALUACIÓN TÉCNICA MULTIDISCIPLINAR — ZEMDB (FASE 3 - MIDPOINT)

**Fecha de Evaluación:** 24 de Septiembre de 2026  
**Proyecto:** `ZemDB` (Motor de Base de Datos Distribuida Local-First)  
**Estado del Repositorio:** Mitad de desarrollo — Fases 1 a 2.5 completadas (`crates/core` y `crates/storage`); Fase 3 activa en scaffolding (`crates/server`); Fases 4 y 5 planificadas (`crates/client`, suites E2E y WASM).  
**Premisa Arquitectónica:** Versión v1 en desarrollo inicial activo; **no se requiere retrocompatibilidad**, lo que permite refactorizaciones profundas, cambios de contratos de red y rediseño de almacenamiento sin condicionamientos de versiones previas.  
**Comité de Auditoría Técnica Especializada:**
1. **Especialista en Rust y Calidad de Código** (`rust_code_specialist`)
2. **Especialista en Motores de Bases de Datos y Almacenamiento** (`database_engine_specialist`)
3. **Especialista en Sistemas Distribuidos y Protocolos de Red** (`distributed_systems_specialist`)
4. **Especialista en Arquitectura de Software y Modularidad** (`architecture_specialist`)

---

## 1. RESUMEN EJECUTIVO DEL ESTADO DEL PROYECTO

ZemDB ha completado exitosamente sus cimientos de bajo nivel a lo largo de las Fases 1, 1.5, 2 y 2.5. El repositorio cuenta actualmente con dos crates medulares plenamente funcionales y verificados: [`crates/core`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core) (`zemdb-core`) y [`crates/storage`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage) (`zemdb-storage`), respaldados por una batería de **56 tests automatizados**, cero advertencias de Clippy (`-D warnings`) y cumplimiento estricto de `#![forbid(unsafe_code)]`.

### Principales Logros Consolidados:
- **Densidad de Memoria y Optimización de Hardware:**
  - [`Value`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/value/scalar.rs#L13-L23) acotado estrictamente a **24 bytes** en 64 bits mediante boxing de tipos dinámicos (`String(Box<str>)` y `Bytes(Box<Bytes>)`), permitiendo almacenar `Uuid([u8; 16])` inline sin alocación.
  - [`PrimaryKey`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/value/row.rs#L10-L12) acotado a **40 bytes** en stack mediante `SmallVec<[Value; 1]>`, garantizando que claves de una columna no dividan líneas de caché L1 de CPU ($40\text{B} < 64\text{B}$).
  - [`Operation`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/op.rs#L44-L50) ocupa **88 bytes exactos** en stack sin punteros heap de metadatos mediante identificadores numéricos de tabla de 2 bytes (`table_id: u16`).
  - [`SequencedOperation`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L27-L31) ocupa **96 bytes exactos** con alineación a 8 bytes, reduciendo el ancho de banda y el consumo de RAM del log histórico en más de un 40%.
- **Resiliencia de Almacenamiento Local:**
  - Formato WAL append-only con enmarcado atómico por lote ([`0xBA7C`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/format.rs#L137)) y suma de verificación CRC32 unificada.
  - Sincronización POSIX de directorio ([`sync_dir`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/sys.rs#L5-L21)) para proteger las entradas de directorio (*dentries*).
  - Bloqueo exclusivo multi-proceso a nivel de kernel mediante `flock` ([`StorageError::RoomLocked`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/error.rs#L14)).
  - Aislamiento de compresión Zstandard con `tokio::task::spawn_blocking` y recuperación por replay en streaming con `BufReader` de 64 KB.
- **Topología Distribuida y Consistencia:**
  - Aislamiento total de consistencia por sala ([`RoomId`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/id.rs#L6-L8)), erradicando transacciones distribuidas inter-room.
  - Autoridad suprema de orden total atribuida al secuenciador monótono central ([`SequenceNumber`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/id.rs#L144-L146)), descartando anomalías por deriva de relojes de cliente (clock skew).

### El Desafío del Punto Medio (Midpoint):
El proyecto se encuentra en la transición crucial desde los motores locales hacia los subsistemas de red y cliente ([`crates/server`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/server) y [`crates/client`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/client)). La auditoría integral revela que, si bien los cimientos son de alta calidad, existen **desalineaciones de contratos, riesgos de concurrencia y contradicciones documentales** que deben subsanarse de inmediato para evitar que las Fases 3 y 4 se construyan sobre interfaces inviables.

---

## 2. MATRIZ COMPARATIVA DE HALLAZGOS POR IMPACTO Y SEVERIDAD

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   MATRIZ DE RIESGOS Y DEUDA TÉCNICA (MIDPOINT)                                   │
├────┬─────────────────────────────┬────────────────────────────────────────────────┬────────────┬────────────────┤
│ ID │ Hallazgo Técnico            │ Componente Afectado                            │ Severidad  │ Especialistas  │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H1 │ Brecha Crítica en Protocolo │ crates/core/src/protocol/messages.rs           │ CRÍTICA    │ Distribuido    │
│    │ 1-RTT Commit & Catch-Up     │ (Falta last_ack_seq en Commit y deltas en Ack) │            │ Arquitectura   │
│    │                             │                                                │            │ Rust           │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H2 │ Inanición de Bloqueos y     │ crates/storage/src/disk/mod.rs: L339-L362       │ CRÍTICA    │ DB             │
│    │ Deadlock Hazard en scan     │ (Retiene read lock a través de canal Tokio)    │            │ Rust           │
│    │                             │                                                │            │ Arquitectura   │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H3 │ Ventana de Carrera de flock │ crates/storage/src/disk/compactor.rs: L68-L91  │ ALTA       │ DB             │
│    │ en Compactación             │ (Rename antes de bloquear archivo resultante)  │            │ Rust           │
│    │                             │                                                │            │ Arquitectura   │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H4 │ Contradicción Documental:   │ ROADMAP.md vs ARCHITECTURE.md                  │ ALTA       │ Distribuido    │
│    │ Squashing en Servidor       │ (CompactionBuffer server_squash rompe log)     │            │ Arquitectura   │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H5 │ Rotura de Evolución         │ crates/core/src/schema/table.rs: L308          │ ALTA       │ DB             │
│    │ add_column en CompactRow    │ crates/storage/src/disk/mod.rs: L273           │            │                │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H6 │ Atrapamiento de Framing     │ crates/storage/src/disk/format.rs              │ MEDIA-ALTA │ Arquitectura   │
│    │ WAL 0xBA7C en Storage       │ (Servidor no puede usarlo para Tier 2 Warm)    │            │ DB             │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H7 │ Falta de Validación Nativa  │ crates/core/src/schema/validation.rs           │ MEDIA-ALTA │ Distribuido    │
│    │ de Operation en Core        │ (Solo valida Row con HashMap de Strings)       │            │                │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H8 │ Bloqueo Incondicional Send  │ crates/core/src/crypto.rs: L16                 │ MEDIA-ALTA │ Arquitectura   │
│    │ + Sync en CryptoEngine WASM │ (Impide usar WebCrypto en navegadores)         │            │ Rust           │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H9 │ Falso "Non-Blocking CoW" y  │ crates/storage/src/disk/mod.rs: L231-L300      │ MEDIA-ALTA │ DB             │
│    │ Archivo Monolítico .zemdb   │ (Bloquea todo el motor durante Zstd y sync)    │            │                │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H10│ Desactualización de         │ crates/storage/src/disk/mod.rs: L254-L256      │ MEDIA      │ DB             │
│    │ FileHeader.head_seq         │ crates/storage/src/disk/format.rs: L45         │            │                │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H11│ Ausencia de lib.rs en       │ crates/server/src/ (Solo existe main.rs)       │ MEDIA      │ Arquitectura   │
│    │ zemdb-server                │ (Impide tests de integración en memoria)       │            │ Rust           │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H12│ Código Huérfano en          │ crates/storage/src/index/primary.rs            │ BAJA-MEDIA │ Rust           │
│    │ PrimaryIndex                │ (No se usa ni en Memory ni en Disk storage)    │            │ Arquitectura   │
│    │                             │                                                │            │ DB             │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H13│ Doble Indirección Heap en   │ crates/core/src/value/scalar.rs: L21           │ BAJA-MEDIA │ Rust           │
│    │ Value::Bytes(Box<Bytes>)    │ (Reemplazable por Box<[u8]> zero-overhead)     │            │                │
├────┼─────────────────────────────┼────────────────────────────────────────────────┼────────────┼────────────────┤
│ H14│ Permisividad de Bytes en    │ crates/core/src/protocol/codec.rs: L24         │ BAJA-MEDIA │ Distribuido    │
│    │ Codec (allow_trailing_bytes)│ (Riesgo de desincronización de stream de red)  │            │                │
└────┴─────────────────────────────┴────────────────────────────────────────────────┴────────────┴────────────────┘
```

---

## 3. SECCIONES DETALLADAS POR ESPECIALISTA

### 3.1. Especialista en Rust y Calidad de Código (`rust_code_specialist`)

#### A. Fortalezas
1. **Layout y Ergonomía de Memoria:**
   - La estructura de tipos en [`crates/core`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core) está optimizada para hardware: `Value` (24B) y `PrimaryKey` (40B) evitan la fragmentación de la memoria. La discriminación numérica en `type_order(&self) -> u8` permite un ordenamiento lexicográfico en $O(1)$ sin alocaciones intermedias de cadenas.
2. **Semántica de Movimiento:**
   - `apply_batch` recibe `Vec<SequencedOperation>` por propiedad de valor, eliminando clonaciones entre capas de transporte y persistencia.
3. **Aislamiento Tokio:**
   - Correcta separación de operaciones de CPU bloqueantes (`zstd`) mediante `tokio::task::spawn_blocking`.
4. **Seguridad Absoluta:**
   - `#![forbid(unsafe_code)]` respetado al 100% en todos los crates sin excepciones.

#### B. Deficiencias Detectadas
1. **Inanición en `DiskStorageEngine::scan`:**
   - En [`disk/mod.rs`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L339-L361), la tarea Tokio retiene `room_arc.read().await` mientras envía tuplas por un canal acotado `mpsc::channel(64)`. Si el consumidor es lento, la tarea se suspende cediendo el hilo del scheduler pero **reteniendo el cerrojo de lectura**. Cualquier invocación concurrente a `apply_batch` (que solicita `write().await`) queda suspendida indefinidamente.
2. **Doble Indirección en `Value::Bytes`:**
   - `Value::Bytes(Box<Bytes>)` envuelve una estructura `Bytes` (que ya contiene puntero heap, longitud, capacidad y puntero atómico de control) dentro de un `Box`. Esto agrega 8 bytes adicionales de puntero en el heap y obliga a clonar el `Box` en vez de incrementar el contador de referencias atómico.
3. **Pico de Memoria 4x en Snapshots:**
   - [`create_snapshot`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L381-L390) clona todas las tuplas de todas las tablas a una estructura temporal `RoomSnapshotPayload` antes de serializar a Bincode y comprimir con Zstd, triplicando o cuadruplicando la memoria requerida en salas grandes.
4. **Antipatrón de Newtypes con Campos Públicos y `Deref`:**
   - `pub struct RoomId(pub String)` expone mutabilidad de campos internos. Implementar `Deref for RoomId { type Target = str; }` y `Deref for SequenceNumber { type Target = u64; }` vulnera la directriz Rust API [C-DEREF], ya que solo los punteros inteligentes deben implementar `Deref`.

---

### 3.2. Especialista en Motores de Bases de Datos y Almacenamiento (`database_engine_specialist`)

#### A. Fortalezas
1. **Enmarcado WAL Atómico (`0xBA7C`):**
   - La cabecera fija de 14 bytes con suma de verificación CRC32 unificada por lote proporciona garantías de atomicidad estricta (todo-o-nada) ante caídas de tensión o cortes de energía.
2. **Cabecera de Sala Alineada a CPU Cache Line:**
   - `FileHeader` (64 bytes) coincide exactamente con la línea de caché L1 de las CPUs modernas (x86_64 y ARM64), almacenando identificadores mágicos `b"ZEM1"`, secuencias base y CRC32 autónomo.
3. **Durabilidad POSIX:**
   - `sync_dir` garantiza que los metadatos del directorio contenedor persistan en disco tras renames o creaciones de archivos.
4. **Streaming en Recuperación:**
   - `BufReader` de 64 KB en `recover_room` erradica la ingestión monolítica de archivos grandes a RAM.

#### B. Deficiencias Detectadas
1. **Acoplamiento de Snapshot y WAL en un Solo Archivo (`room_{id}.zemdb`):**
   - El formato monolítico ubica el snapshot base al principio y el WAL al final. Esto impide podar o truncar el WAL *in-place*. Para truncar, el motor está obligado a crear un archivo temporal completo (`.tmp`) y reescribir todo el fichero.
2. **Falso "Non-Blocking CoW Compaction":**
   - [`compact_room_internal`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/compactor.rs#L25-L104) adquiere `room_arc.write().await` y retiene el cerrojo de toda la sala durante la serialización, compresión Zstd, escritura del temporal, `sync_all()`, `rename()` y reapertura. La compactación paraliza completamente todas las lecturas y escrituras ("stop-the-world").
3. **Rotura de Evolución de Esquema (`add_column`):**
   - En [`table.rs#L308`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/table.rs#L308), `compact_into_row` exige `compact.len() == self.columns.len()`. Si se añade una columna, las filas históricas persistidas tienen longitud $N$ frente a la aridad nueva $N+1$, fallando inmediatamente con `CompactRowArityMismatch`.
   - En [`disk/mod.rs#L273`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L273), si un `Update` muta la nueva columna ($idx = N$), la condición `idx < existing.values.len()` es falsa y la mutación se descarta silenciosamente.
4. **Fallo ante Ceros en EOF tras Caídas de Tensión:**
   - Si un apagón abrupto provoca que el sistema de archivos rellene el último bloque de disco con ceros (`\0\0...`), el recovery encuentra un magic distinto de `0xBA7C` y retorna `Err(StorageError::WalCorruption)` en vez de clasificarlo como `TornWrite` y truncar el archivo en el último byte consistente.

---

### 3.3. Especialista en Sistemas Distribuidos y Protocolos (`distributed_systems_specialist`)

#### A. Fortalezas
1. **Secuenciador Monótono Central vs. HLC:**
   - La elección del servidor como árbitro absoluto de Total Order erradica el problema del sesgo de relojes en clientes (clock skew) y elimina la necesidad de sincronización física NTP o algoritmos complejos de Relojes Lógicos Híbridos.
2. **Particionado Físico por Salas:**
   - La topología de salas aisladas (`RoomId`) garantiza cero contención distribuida: no se requieren consensos entre salas distintas ni bloqueos globales en el clúster.
3. **Contrato de Señalización Liviana (Signal-Only SSE):**
   - El canal push Server-Sent Events transmite únicamente notificaciones de avance `Event::HeadAdvanced(head_seq)`, forzando al cliente a realizar pulls paginados controlados con contrapresión (`/sync`), protegiendo a la red contra tormentas de payloads masivos.

#### B. Deficiencias Detectadas
1. **Brecha Crítica en el Contrato 1-RTT Commit & Catch-Up:**
   - [`ClientMessage::Commit`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L51-L57) carece de `last_ack_seq: SequenceNumber`.
   - [`ServerMessage::CommitAck`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/protocol/messages.rs#L104-L109) carece de `catchup_ops: Vec<SequencedOperation>`.
   - **Impacto Sistémico:** Si un cliente commitea una mutación cuando existen deltas remotos intermedios generados por otros clientes, el servidor le confirmará la secuencia asignada (ej. `#15`), pero al recibir este ACK sin los deltas intermedios (`#11..#14`), el cliente no podrá aplicar la mutación en su almacenamiento local, porque [`StorageEngine::apply_batch`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/storage/src/disk/mod.rs#L243) abortará con `SequenceMismatch { expected: 11, actual: 15 }`. Se rompe el modelo Write-Through en 1 RTT prometido en [`ARCHITECTURE.md`](file:///Users/Santiago/OtherProjects/client-distributed-db/ARCHITECTURE.md#72-data-plane-client-synchronization-protocol---binary-http2).
2. **Inexistencia de Validación Nativa de `Operation`:**
   - [`TableSchema::validate_row`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/schema/validation.rs#L6-L45) requiere un `Row` basado en `HashMap<String, Value>`. El servidor no cuenta con un validador en $O(C)$ que verifique directamente la tupla posicional `CompactRow` ni la lista ordenada de `ColumnUpdate` de una `Operation`. Reconstruir `Row` para cada mutación destruiría las ganancias de CPU y memoria de `CompactRow`.
3. **Ausencia de Suma Criptográfica en Snapshots Multipart:**
   - `SnapshotChunk` no incluye un hash global de integridad (BLAKE3 / SHA-256). Si un fragmento se corrompe en tránsito, el fallo solo se manifiesta al intentar descomprimir el snapshot consolidado con Zstd, forzando a descartar todos los fragmentos sin saber cuál falló.
4. **Permisividad Peligrosa en Codec:**
   - `codec.rs` utiliza `.allow_trailing_bytes()`. En protocolos orientados a stream sobre HTTP/2 o TCP, esto permite tolerar datos residuales espurios, enmascarando desalineaciones de tramas binarias.

---

### 3.4. Especialista en Arquitectura de Software y Modularidad (`architecture_specialist`)

#### A. Fortalezas
1. **Pureza Conceptual de Core (Zero-I/O):**
   - [`crates/core`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core) se mantiene como una biblioteca de dominio puro sin dependencias de red o sistema de archivos, garantizando compatibilidad natural con WebAssembly.
2. **Separación de Responsabilidades:**
   - Fronteras nítidas entre dominio/esquemas (`zemdb-core`), persistencia tabular local (`zemdb-storage`), servidor coordinador (`zemdb-server`) y SDK cliente (`zemdb-client`).
3. **Two-Pointer Merge Algorítmico:**
   - La consolidación de deltas en [`merge_sorted_column_updates`](file:///Users/Santiago/OtherProjects/client-distributed-db/crates/core/src/mutation/squash.rs#L20) se ejecuta en $O(M+N)$ sin reasignaciones en el heap.

#### B. Deficiencias Detectadas
1. **Contradicción sobre Squashing en Servidor:**
   - [`ROADMAP.md`](file:///Users/Santiago/OtherProjects/client-distributed-db/ROADMAP.md#fase-3-servidor-coordinador-y-secuenciador-zemdb-server-activa) (líneas 560-561 y 631) menciona implementar un "CompactionBuffer con server_squash en RoomActor", mientras que [`ARCHITECTURE.md`](file:///Users/Santiago/OtherProjects/client-distributed-db/ARCHITECTURE.md#52-four-tier-storage-architecture--mutation-lifecycle) (línea 106) y la Fase 2.5 establecen taxativamente que el log del servidor es 100% inmutable y contiguo, sin squashing. El squashing en el servidor crearía huecos de secuencia que detonarían `SequenceMismatch` en los clientes.
2. **Atrapamiento del Framing WAL en Storage:**
   - El enmarcado físico `0xBA7C` y las funciones de codificación/decodificación residen en `crates/storage/src/disk/format.rs`. Dado que `zemdb-server` no debe depender del motor de almacenamiento del cliente, no puede persistir sus segmentos de Tier 2 (Warm Disk Log) sin duplicar código o romper la modularidad.
3. **Incompatibilidad WASM en `CryptoEngine`:**
   - El trait `CryptoEngine` impone `Send + Sync` incondicionalmente, rompiendo la compilación hacia `wasm32-unknown-unknown` para implementaciones basadas en WebCrypto.
4. **Falta de `src/lib.rs` en Servidor:**
   - `crates/server` solo contiene `main.rs`, imposibilitando levantar instancias del servidor en memoria o sobre puertos efímeros en tests de integración concurrentes.

---

## 4. CONVERGENCIAS Y DIVERGENCIAS ENTRE ESPECIALISTAS

### 4.1. Puntos de Convergencia Unánime
1. **Reparación Inmediata del Contrato 1-RTT Commit & Catch-Up (H1):**
   - Los 4 especialistas concuerdan en que `ClientMessage::Commit` debe incluir `last_ack_seq: SequenceNumber` y `ServerMessage::CommitAck` debe incluir `catchup_ops: Vec<SequencedOperation>`. Es el bloqueo más crítico para las Fases 3 y 4.
2. **Corrección de la Inanición en `DiskStorageEngine::scan` (H2):**
   - Unanimidad en erradicar la retención del cerrojo de lectura a lo largo del canal `mpsc`. Se debe adoptar el patrón de cursor paginado de `MemoryStorageEngine` (bloques de 64 elementos liberando el lock entre iteraciones).
3. **Cierre de la Ventana de Carrera de `flock` (H3):**
   - Unanimidad en adquirir el cerrojo exclusivo sobre el archivo temporal `.tmp` antes de invocar `rename()`.
4. **Eliminación Definitiva del Squashing en Servidor (H4):**
   - Unanimidad en que el log histórico de deltas del servidor debe ser 100% append-only, inmutable y contiguo. El squashing pertenece única y exclusivamente a la cola de salida local del cliente (*Outbox Queue*).
5. **Relajación de Concurrencia en `CryptoEngine` para WASM (H8):**
   - Unanimidad en adoptar el trait `CryptoConcurrencyBounds` (`Send + Sync` en nativo, vacío en wasm32).

### 4.2. Puntos de Divergencia y Análisis Técnico
1. **Arquitectura de Archivos en Disco: Archivo Monolítico (`.zemdb`) vs. Dual-File (`.snap` + `.wal`):**
   - *Especialista en Bases de Datos:* Propone dividir inmediatamente el formato en dos archivos por sala: `room_{id}.snap` (snapshot base Zstd) y `room_{id}.wal` (log de deltas append-only). Argumenta que esto habilita una compactación Copy-on-Write verdaderamente no bloqueante y permite truncar el WAL sin reescribir snapshots de 50 MB.
   - *Especialista en Arquitectura:* Señala que el formato único simplifica los backups atómicos y el file-locking de sala, pero coincide en que la compactación actual es stop-the-world.
   - *Dictamen de Coordinación:* Dado que en v1 no se requiere retrocompatibilidad, la adopción de la arquitectura dual-file (`.snap` + `.wal`) es la solución técnicamente superior y definitiva para motores de bases de datos de alto rendimiento.
2. **Ubicación del Enmarcado WAL (`0xBA7C`):**
   - *Especialista en Arquitectura:* Propone extraer el enmarcado de lotes WAL de `crates/storage` y trasladarlo a `crates/core/src/protocol/wal_frame.rs` para que tanto el servidor (Tier 2 Warm Disk Log) como el cliente (`zemdb-storage`) lo reutilicen.
   - *Especialista en Bases de Datos:* Sugiere crear un crate independiente `zemdb-wal`.
   - *Dictamen de Coordinación:* Moverlo a `core::protocol::wal_frame` evita la proliferación innecesaria de micro-crates en el workspace y mantiene a `zemdb-core` como el único dueño de los contratos y enmarcados binarios.
3. **Destino de `PrimaryIndex`:**
   - *Especialista en Rust:* Recomienda integrarlo formalmente o purgarlo si no se usa.
   - *Especialista en Bases de Datos:* Propone mantenerlo solo si almacena punteros a offsets de archivo (`HashMap<(u16, PK), FileOffset>`).
   - *Dictamen de Coordinación:* Dado que los motores actuales operan directamente sobre `HashMap<u16, BTreeMap<PrimaryKey, CompactRow>>` con excelente rendimiento y sin fallas, `PrimaryIndex` constituye código muerto que debe ser saneado o integrado en el refactor de Fase 3.
