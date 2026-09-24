# Architectural Specification: RimDB (Local-First Distributed Database Engine)

## 1. Overview
The goal of this project is to build a client-centric (**Local-First**) distributed database engine powered by a **minimal, lightweight coordination server**.

* **Primary Storage:** 100% stored locally on clients in a structured, persistent format.
* **Server Role:** Acts as a sequencer, schema/type validator, and ephemeral mutation buffer. The server **does not maintain the long-term historical state** of each database; it only holds unread mutations until clients acknowledge them or a retention threshold expires.
* **Performance Objective:** Ultra-low server memory and CPU footprint, capable of coordinating numerous groups with minimal resource consumption.

---

## 2. Topology & Isolation (Rooms)
The system adopts an isolated **Room** topology:
* Each database is an independent compartment identified by a strongly-typed `RoomId` (e.g., a chat group, team workspace, or specific collaborative project).
* A room typically consists of a small-to-medium group of clients (e.g., 2 to 50 participants).
* Operations, schemas, and sequence numbers are strictly scoped to their respective Room.

---

## 3. Data Model & Schema

### 3.1. Structured Typing & Schema Definitions
Every table within a Room has an explicit schema definition with typed fields:
* **Primitive Data Types:** `Int`, `Float`, `String`, `Bool`, `Bytes`, `Null`, `Timestamp` (domain date/time), and `Uuid` (`[u8; 16]` fixed-size, ideal for distributed primary keys). Arbitrary-precision `Decimal` is deferred to post-v0.1.
* **Strongly-Typed Domain Identifiers (Newtypes):** Domain identities (`RoomId`, `ClientId`, `SequenceNumber`, `MutationId`, `CorrelationId`) are strictly typed to prevent parameter transposition bugs while maintaining zero-overhead binary representations.
* **In-Memory Density & Cache-Line Alignment:** Tuples are stored positionally (`CompactRow`) without duplicating column names across rows. The dynamic `Value` enum is strictly bounded to **24 bytes** on 64-bit platforms (by boxing heap payloads: `String(Box<str>)` and `Bytes(Box<Bytes>)`). Primary keys are optimized with `SmallVec<[Value; 1]>` occupying **40 bytes** on the stack, strictly fitting within a single 64-byte CPU L1 cache line to prevent cache line splits during index lookups.
* **Compact Table Identifiers (`table_id: u16`):** Every table in a Room is assigned a compact 2-byte numerical identifier (`table_id: u16`). Strings are completely eliminated from the inner mutation loop and memory storage structures. The `Schema` maintains a bidirectional catalog with explicit, unambiguous lookup methods: `get_table_by_id`, `get_table_by_name`, `get_table_id`, `has_table_by_id`, and `has_table_by_name`.
* **Unified Stack-Allocated Mutation Unit (`Operation`):** All row mutations are represented by a single unified, stack-allocated `Operation` struct (`table_id: u16`, `timestamp: u64`, `pk: PrimaryKey`, `kind: OperationKind`) occupying **exactly 88 bytes** on 64-bit systems with 0 bytes heap overhead for metadata.
* **Dense Sequenced Operations (`SequencedOperation`):** Historical log records compose a 64-bit monotonic sequence number with the mutation: `SequencedOperation { seq: SequenceNumber, op: Operation }`, occupying **exactly 96 bytes**. Ephemeral client metadata (`client_id`, `mutation_id`) is retained strictly in transit during `Commit` / `CommitAck` for idempotency and 1-RTT synchronization, eliminating 24 bytes of overhead per historical operation in RAM and persistent storage.
* **Deterministic Column Ordering (Append-Only DDL):** `TableSchema` preserves physical column declaration order in `columns: Vec<ColumnDef>`, ensuring stable positional binary offsets in `CompactRow` across schema migrations, paired with zero-copy row transformations (`row_into_compact` and `compact_into_row`).
* **End-to-End Encryption (E2EE):** Handled via metadata on `ColumnDef` (`encrypted: bool`). The schema preserves the real underlying `data_type` for client-side decryption, while the coordination server validates that encrypted fields in transit are transmitted strictly as opaque `Value::Bytes` without inspecting payload contents. Primary key columns cannot be encrypted.
* **Primary Key (PK):** Supports both single-column and composite (multi-column) primary keys.
* **Soft Foreign Keys:** Relational references are supported without distributed locking or strict server-side validation. Dangling references are permitted to maintain eventual consistency without coordination bottlenecks.
* **Schema Evolution:** Supports adding new fields over time without breaking backward compatibility.

---

## 4. Mutation Operations & Semantics

All mutations are represented by elemental operations structured with decoupled metadata (`table_id: u16`, `pk: PrimaryKey`, `timestamp: u64`) and payload (`OperationKind`):

1. **`INSERT` (`OperationKind::Insert { row: CompactRow }`):**
   * Inserts a new tuple stored positionally in DDL order, eliminating column names from the wire and in-memory tuples.
   * **Semantics on existing PK:** If an `INSERT` is performed on an existing primary key, it operates as a full overwrite (upserting all fields).
2. **`UPDATE` (`OperationKind::Update { updates: Vec<ColumnUpdate> }`):**
   * Granular, field-level modification transmitting only modified fields as 32-byte positional deltas (`ColumnUpdate { column_idx: u16, value: Value }`).
   * **Sorted Invariant & $O(M+N)$ Merges:** Deltas are maintained strictly ordered by ascending `column_idx`, guaranteeing linear sorted merges without heap reallocation or unstable sorting.
   * **Concurrent Conflict Resolution:** If two clients update distinct fields of the same tuple concurrently, the server performs a **field-level merge**. If both clients modify the exact same field, a *Last-Write-Wins (LWW)* policy applies based strictly on the server arrival order and assigned `SequenceNumber` (see Section 10).
3. **`DELETE` (`OperationKind::Delete`):**
   * Logical deletion via a tombstone marker.

### 4.1. Client-Side Consolidation (Outbox Queue Squashing)
In offline scenarios, the client buffers uncommitted mutations in its local outbox queue before transmission to the server. To optimize bandwidth and eliminate redundant operations on the wire, the client can consolidate pending mutations for the same table and primary key using `squash_operations`:
* **Zero-Copy Move Semantics:** Values are transferred by ownership movement (`drain(..)` and slot replacement), eliminating redundant cloning of strings and bytes.
* **Anti-Zombie Rule:** An `UPDATE` operation arriving after a `DELETE` on the same primary key cannot resurrect deleted records (obsolete updates are `Discarded`; newer updates are `Incompatible`).
* **Base Entity Preservation (Old Insert into Newer Update):** An `INSERT` arriving with an older timestamp than an existing pending `UPDATE` preserves full entity data: the newer update deltas are applied directly over the incoming base `CompactRow`, preventing entity or column loss.
* **Buffer Purge Rule:** Receiving a newer `DELETE` for a primary key consolidates and purges prior unacknowledged `INSERT` or `UPDATE` operations for that same PK in the client buffer.
* **Table-Partitioned Buffering (`TableBuffer`):** Buffers hold `table_id: u16` and manage their own `HashMap<PrimaryKey, Operation>`, ensuring $O(1)$ amortized squashing and zero lock contention between distinct tables.
*(Note: Squashing is strictly a client-side optimization performed prior to transmission. The coordination server never alters, merges, or deletes sequenced operations in the historical log).*

---

## 5. The Coordination Server (`rimdb-server`)

### 5.1. Core Responsibilities
1. **Monotonic Sequencer:** Assigns an incremental, globally ordered sequence number (`SequenceNumber`: 1, 2, 3...) per Room.
2. **Actor-Based Concurrency:** Manages each Room via an isolated Tokio actor, eliminating global lock contention across rooms.
3. **Type & Schema Validation:** Enforces schema integrity, required non-null columns, and primary key type constraints before accepting operations.
4. **Idempotency & Deduplication:** Maintains an in-memory LRU cache of recent `MutationId`s to guarantee *Exactly-Once* semantics and safe network retries.
5. **Immutable Tiered Delta Log:** Stores and serves contiguous, immutable `SequencedOperation { seq, op }` deltas across a 4-tier hierarchy without modifying or squashing sequenced operations.
6. **Micro-WAL:** Lightweight append log ensuring safe persistence and recovery of the latest `SequenceNumber` across server restarts.
7. **Ephemeral Snapshot Relay:** Relays compressed multipart snapshot chunks between active clients and onboarding/reconnecting peers without constructing or storing consolidated database states itself.

### 5.2. Four-Tier Storage Architecture & Mutation Lifecycle
To balance minimal RAM usage with flexible offline support, the coordination server manages room mutations across four distinct storage tiers as an **immutable append-only log**:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          4-TIER SERVER STORAGE HIERARCHY                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 1: Hot RAM Buffer (Minutes)                                                │
│  - Active connected clients, lowest latency                                     │
│  - Contiguous in-memory log: VecDeque / BTreeMap<SequenceNumber, SequencedOp>   │
│  - Flushed to Warm Disk upon TTL expiration or buffer size threshold            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 2: Warm Disk Log [.wal] (Days)                                             │
│  - Uncompressed append-only log with 0xBA7C framing and per-batch CRC32         │
│  - Zero CPU overhead, fast sequential streaming for recently offline clients    │
│  - Identical binary representation to in-memory batch payloads                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 3: Cold Disk Log [.wal.zst] (Weeks)                                        │
│  - Background Zstandard compression into immutable delta segments               │
│  - High compression ratio saves disk space for long-term offline retention      │
│  - Streamed to reconnecting clients that have been offline for days/weeks       │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 4: Eviction & Compaction Boundary [BehindCompaction]                       │
│  - Operations exceeding the maximum cold retention duration are pruned          │
│  - Reconnecting clients with last_ack_seq < tail_seq receive BehindCompaction   │
│  - Client recovers state via full base snapshot transfer from an active peer    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

1. **Tier 1 (Hot RAM Buffer):**
   * Serves real-time queries and fast synchronization for currently connected clients.
   * Holds an immutable contiguous buffer of recent `SequencedOperation`s. No squashing is performed on sequenced deltas, ensuring absolute sequence contiguity (`head_seq + 1`) and preventing sequence gaps or zombie record anomalies.
   * Promoted/flushed to Warm Disk when the batch reaches the memory threshold or RAM TTL expires.
2. **Tier 2 (Warm Disk Log - Uncompressed `.wal`):**
   * Stored directly on disk using the standard batch framing (`0xBA7C` magic, length, unified CRC32, operation count).
   * Remains uncompressed to completely avoid CPU decompression spikes when clients reconnect after hours or a few days.
   * Sequential I/O enables high-speed streaming during `/sync`.
3. **Tier 3 (Cold Disk Log - Compressed `.wal.zst`):**
   * As segments age past the Warm retention threshold, a background task compresses older `.wal` files into `.wal.zst` segments using Zstandard.
   * Allows rooms to retain weeks of historical deltas without consuming substantial disk storage.
4. **Tier 4 (Eviction & Compaction Boundary):**
   * Historical operations that exceed the Cold Disk retention period or room storage quota are purged from disk.
   * The server tracks the oldest available sequence (`tail_seq` / compaction boundary).
   * Any client connecting with `last_ack_seq < tail_seq` is marked desynchronized and receives an `ErrorCode::BehindCompaction` response.
   * Because the server does not hold long-term historical table states, the desynchronized client requests a full base snapshot from an **active client** in the Room (transferred directly P2P or via the server's ephemeral snapshot relay).

### 5.3. Configurable Lifecycle Policy (`RoomLifecyclePolicy`)
All tier retention thresholds and size boundaries are fully configurable per deployment and room:

```rust
pub struct RoomLifecyclePolicy {
    /// Maximum operations kept in RAM before flushing to Warm Disk.
    pub ram_max_ops: usize,
    /// Time-to-live for operations in RAM before batch flush.
    pub ram_ttl: std::time::Duration,
    /// Retention duration on Warm Disk (uncompressed .wal).
    pub warm_disk_ttl: std::time::Duration,
    /// Retention duration on Cold Disk (compressed .wal.zst).
    pub cold_disk_ttl: std::time::Duration,
    /// Maximum total disk usage per Room before early eviction.
    pub max_room_disk_bytes: u64,
}
```

This ensures the system is adaptable across diverse deployment profiles, from resource-constrained edge gateways to large-scale enterprise coordination hubs.

---

## 6. Onboarding & Invitations (State Transfer)

Because the server does not store the full persistent historical state:
1. **Invitation Flow:** An existing active client can invite a new client into the Room.
2. **Snapshot Generation:** The inviting client creates an export (snapshot) of its local database state up to a specific base `SequenceNumber` (e.g., `#500`), applying high-ratio Zstandard compression.
3. **Transfer:** The compressed snapshot is transferred to the new client (via short-lived ephemeral server relay or direct P2P).
4. **Subsequent Catch-up:** Once the snapshot is restored locally, the new client connects to the server and pulls delta changes starting from sequence `#501` onwards.

### Multipart Chunked Snapshot Transfer Protocol (> 16 MB)

When database rooms grow large such that compressed snapshots exceed the strict DoS envelope (`MAX_MESSAGE_SIZE = 16 MB`), state transfer is partitioned into framed chunks:
1. **Chunk Negotiation & Request:** The bootstrapping client issues `ClientMessage::RequestSnapshotChunk { correlation_id, room_id, chunk_index, chunk_size }` (typically requesting $2\text{ MB}$ to $4\text{ MB}$ chunks).
2. **Chunk Streaming:** The server or relay peer streams `ServerMessage::SnapshotChunk { correlation_id, room_id, snapshot_head_seq, chunk_index, total_chunks, total_bytes, data }`.
3. **Atomic Reconstruction:** Once all chunks `0..total_chunks` are assembled in a temporary staging buffer, the client verifies payload size and applies the snapshot in a single atomic transaction via `StorageEngine::apply_snapshot`.
4. **Resumed Delta Catch-up:** The client sets its local cursor to `snapshot_head_seq` and seamlessly queries `/sync` starting from `snapshot_head_seq + 1`.

---

## 7. Client-Server Communication Protocol

To prevent the resource exhaustion of thousands of idle persistent connections:
* **Pull-Based HTTP/2 Binary Protocol:**
  * Transport: HTTP/2 over TLS with binary payloads serialized via `bincode`.
  * Wire Efficiency (50%+ Bandwidth Reduction): Replacing string-keyed dictionaries with `CompactRow` and `ColumnUpdate` deltas eliminates column names from the wire, reducing serialized insert/update payloads by 38% to 60%.
  * Multiplexing: Multiple sync/commit streams share a single underlying TCP connection using explicit correlation identifiers (`CorrelationId`).
  * Defensive Bounding: Codecs enforce an explicit message size limit (16 MB) to prevent denial-of-service memory exhaustion attacks.
* **Core Interaction Contracts:**
  * **Mutation Commit with Unified Sync (1 RTT):** Client submits an operation with its `MutationId`, `CorrelationId`, and its current cursor `last_ack_seq`. The server validates the mutation against schemas and constraints. If valid, the server assigns a monotonic `SequenceNumber` and returns `CommitAck` containing the assigned `SequenceNumber` along with any remote catchup deltas that occurred between `last_ack_seq` and the new sequence. This allows the client to register the write, sync pending state, and apply canonical data locally in a single network roundtrip (1 RTT) without risk of local state corruption.
  * **Synchronization (Standalone / Polling):** Client requests operations starting from its `last_ack_seq` specifying a maximum batch size (`ClientMessage::Sync`). The server streams ordered `SequencedOperation` batches with pagination flags (`has_more`).
  * **Acknowledgment / Heartbeat:** Client periodically reports processed sequences (`ClientMessage::Heartbeat`), allowing the server to prune its compaction buffer.
  * **Error Handling:** Typed error responses communicate states such as invalid payloads (`ErrorCode::SchemaViolation`), authorization failures, or `BehindCompaction`.
* **Optional Foreground Streaming:** While a client application is actively in the foreground, it can establish an ephemeral push channel (SSE) to receive real-time notifications of new commits.

---

## 8. Storage Engine (`rimdb-storage`)

Persisting data locally on clients and managing snapshots is decoupled into a dedicated storage crate implementing a common `StorageEngine` abstraction:
* **The `StorageEngine` Contract:**
  * Clean, network-agnostic async persistence contract: `open_room`, `close_room`, `apply_batch`, `get`, `scan`, `get_head_seq`, `create_snapshot`, `apply_snapshot`.
  * **Zero-Copy Move Semantics:** `apply_batch` takes `Vec<SequencedOperation>` by ownership value, eliminating redundant cloning between client network pipelines and local storage.
  * **WebAssembly (WASM) Ready:** Defined with conditional concurrency bounds `#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]` and platform-specific `RowStream<'a>` definitions, allowing native execution in single-threaded browser environments (e.g. IndexedDB).
* **Pushdown Query Capabilities (`ScanOptions`):**
  * Modern storage abstraction supporting query execution without bloating the storage engine with SQL parsers:
    * `KeyRange`: Flexible range bounds conforming to Rust standard range syntax (`..`, `a..b`, `a..=b`, `a..`).
    * `ScanDirection`: Forward (`ASC`) and Backward (`DESC`) for $O(1)$ memory `ORDER BY pk DESC` traversal.
    * `limit: Option<usize>`: Early I/O termination pushdown avoiding scanning unwanted records.
    * `projection: Option<Vec<u16>>`: Projection pushdown returning compact rows with only requested column indices, preventing unnecessary deserialization.
* **In-Memory Reference Engine (`MemoryStorageEngine`):**
  * Thread-safe memory backend with **per-room lock isolation** (`Arc<RwLock<HashMap<RoomId, Arc<RwLock<RoomState>>>>>`).
  * Operations across different rooms run 100% concurrently without lock contention. Within each room, multiple concurrent readers execute in shared mode (`read()`) while mutation batches take an exclusive lock (`write()`) on that specific room only.
  * Tabular data stored in `BTreeMap<PrimaryKey, CompactRow>` with binary snapshot serialization.
* **On-Disk Engine (`DiskStorageEngine`) [IMPLEMENTED - Phase 2B]:**
  * Local file-per-room architecture (`room_{id}.rimdb` with magic header `RIM1`).
  * Append-only Write-Ahead Log (WAL) with batch framing (`0xBA7C`), per-batch CRC32 checksums, and POSIX `sync_dir` for crash resilience.
  * Multi-process exclusive file locking (`flock`) preventing simultaneous database file mutations.
  * RAM-resident primary index enabling $O(1)$ point lookups without requiring full disk scans.
  * Non-blocking background Copy-on-Write (CoW) compaction with Zstandard block compression.

---

## 9. Technology Stack & Workspace Structure

### 9.1. Language & Ecosystem: Rust
* Zero-cost abstractions and deterministic memory management without garbage collection pauses.
* High-performance asynchronous networking with `tokio` and `axum`.
* Fast, size-bounded binary serialization with `bincode`.
* Universal compilation: native binaries for desktop/mobile and **WebAssembly (WASM)** targets for browsers.
* Strict workspace security policy: `#![forbid(unsafe_code)]` enforced across all crates.

### 9.2. Workspace Layout
```text
rimdb/
├── README.md                    # Project landing, crate architecture, phase status and navigation
├── ARCHITECTURE.md              # High-level architectural specification and ADRs
├── ROADMAP.md                   # Implementation roadmap, milestone checklists, and backlog
├── Cargo.toml                   # Root workspace manifest with centralized dependencies
├── docs/                        # Historical audits and technical proposals
│   ├── audits/                  # Multidisciplinary specialist audit reports
│   └── proposals/               # Architectural hardening and phase proposals
├── crates/
│   ├── core/                    # Pure domain models, schemas, operations, Newtypes, protocol (Zero I/O, WASM)
│   ├── storage/                 # Tabular storage, WAL with CRC32, RAM index, Zstd snapshots (StorageEngine trait)
│   ├── server/                  # Coordination server, Tokio room actors, LRU dedup, micro-WAL, HTTP/2 API
│   └── client/                  # Client SDK, Write-Through with 1-RTT Sync, canonical storage, dual transport
```

---

## 10. Architectural Decisions: Time, Ordering & Authority

### 10.1. Central Server Sequencer as the Sole Authority of Total Order
A foundational architectural decision of RimDB is that **the coordination server is the single source of truth for global ordering**:
* **Sequence-Based Total Order:** When an operation is accepted by the Room actor on the server, it is assigned an atomically increasing `SequenceNumber`. The global order of events in the Room is defined 100% by this monotonic sequence number.
* **No Trust in Client Wall Clocks:** Client devices frequently suffer from clock skew (misconfigured system times, zone errors, drift). To eliminate the risk of a misconfigured device clock dominating or corrupting the room's history, **client timestamps are never used to determine global causal precedence**.
* **Deterministic Last-Write-Wins (LWW):** In the event of concurrent modifications to the exact same field, the operation with the higher `SequenceNumber` (the one processed later by the server sequencer) takes precedence. This eliminates the necessity of complex Hybrid Logical Clocks (HLC) while guaranteeing absolute convergence across all nodes.

### 10.2. The Three Concepts of Time in RimDB
To prevent conceptual ambiguity, the architecture distinguishes three independent notions of time:
1. **`SequenceNumber` (Total Order Authority):** Monotonic integer issued exclusively by the server. Defines causal precedence, commit ordering, and conflict resolution across all clients.
2. **Mutation `timestamp: u64` (Client Metadata):** Client-generated timestamp attached to mutations for audit/tracing purposes. Not used for causal precedence or local optimistic rebase.
3. **Domain `Value::Timestamp(i64)` (Application Data):** Structured column data type used by end-user schemas to store business dates and timestamps (e.g., `created_at`, `due_date`).
