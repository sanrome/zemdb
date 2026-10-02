# Architectural Specification: ZemDB (Local-First Distributed Database Engine)

## 1. Overview
The goal of this project is to build a client-centric (**Local-First**) distributed database engine powered by a **minimal, lightweight coordination server**.

* **Primary Storage:** 100% stored locally on clients in a structured, persistent format.
* **Server Role:** Acts as a sequencer, schema/type validator, and ephemeral mutation buffer. The server **does not maintain the long-term historical state** of each database; it only holds unread mutations until clients acknowledge them or a retention threshold expires.
* **Performance Objective:** Ultra-low server memory and CPU footprint, capable of coordinating numerous groups with minimal resource consumption.

---

## 2. Topology & Isolation (Rooms & Schema Catalog)
The system adopts an isolated **Room** topology governed by a centralized **Schema Catalog**:
* **Compartmentalized Rooms:** Each database is an independent compartment identified by a strongly-typed `RoomId` (e.g., a chat group, team workspace, or specific collaborative project).
* **Small-to-Medium Groups:** A room typically consists of a small-to-medium group of clients (e.g., 2 to 50 participants).
* **Centralized Schema Catalog (`SchemaId` Hierarchy):** A `Schema` is an independent template identified by a `SchemaId` (e.g., `workspace_v1`). Multiple rooms share the same `SchemaId`, allowing the server to maintain a single `Arc<Schema>` in memory across thousands of rooms without duplicating table definitions or memory overhead. Each room contains exactly one flat schema (no sub-namespaces or sub-schemas), keeping room lookups and storage paths predictable and fast.
* **Per-Room Scoping:** Operations, physical WAL files, and sequence numbers are strictly scoped to their respective Room.

---

## 3. Data Model & Schema

### 3.1. Structured Typing & Schema Definitions
Every table within a Schema has an explicit definition with typed fields:
* **Primitive Data Types:** `Int`, `Float`, `String`, `Bool`, `Bytes`, `Null`, `Timestamp` (domain date/time), and `Uuid` (`[u8; 16]` fixed-size, ideal for distributed primary keys). Arbitrary-precision `Decimal` is deferred to post-v0.1.
* **Strongly-Typed Domain Identifiers & Encapsulated Models (C-DEREF Compliance):** Domain identities (`RoomId`, `SchemaId`, `ClientId`, `SequenceNumber`, `MutationId`, `CorrelationId`) and data models (`PrimaryKey`, `CompactRow`, `TableSchema`) are fully encapsulated with private inner representations. In strict accordance with Rust API Guidelines [C-DEREF], implicit `Deref` coercions are eradicated across all core structures to protect type abstraction boundaries and prevent accidental mutation of internal vectors or strings. Ergonomics and zero-overhead performance are maintained through explicit accessors (`as_str()`, `get()`, `as_bytes()`, `columns()`, `column_indices()`), standard trait indexing (`Index<usize>`, `IndexMut<usize>`), iterator APIs (`iter()`, `as_slice()`), trait conversions (`AsRef<str>`, `AsRef<[u8]>`, `AsRef<[Value]>`, `From`), and transparent Serde representations.
* **In-Memory Density & Cache-Line Alignment:** Tuples are stored positionally (`CompactRow`) without duplicating column names across rows. The dynamic `Value` enum is strictly bounded to **24 bytes** on 64-bit platforms (by boxing heap payloads: `String(Box<str>)` and `Bytes(Box<[u8]>)`). Primary keys are optimized with `SmallVec<[Value; 1]>` occupying **40 bytes** on the stack, strictly fitting within a single 64-byte CPU L1 cache line to prevent cache line splits during index lookups.
* **Compact Table Identifiers (`table_id: u16`):** Every table in a Room is assigned a compact 2-byte numerical identifier (`table_id: u16`). Strings are completely eliminated from the inner mutation loop and memory storage structures. The `Schema` maintains a bidirectional catalog with explicit, unambiguous lookup methods: `get_table_by_id`, `get_table_by_name`, `get_table_id`, `has_table_by_id`, and `has_table_by_name`. Secondary indexing (`id_by_name`) is dynamically constructed upon deserialization and omitted from network/disk serialization. `TableSchema` encapsulates fields privately, exposing immutable getters and `set_table_id`.
* **Unified Stack-Allocated Mutation Unit (`Operation`):** All row mutations are represented by a single unified, stack-allocated `Operation` struct (`table_id: u16`, `timestamp: u64`, `pk: PrimaryKey`, `kind: OperationKind`) occupying **exactly 88 bytes** on 64-bit systems with 0 bytes heap overhead for metadata.
* **Dense Sequenced Operations (`SequencedOperation`):** Historical log records compose a 64-bit monotonic sequence number with the mutation: `SequencedOperation { seq: SequenceNumber, op: Operation }`, occupying **exactly 96 bytes**. Ephemeral client metadata (`client_id`, `mutation_id`) is retained strictly in transit during `Commit` / `CommitAck` for idempotency and 1-RTT synchronization, eliminating 24 bytes of overhead per historical operation in RAM and persistent storage.
* **Deterministic Column Ordering (Append-Only DDL):** `TableSchema` preserves physical column declaration order in `columns: Vec<ColumnDef>`, ensuring stable positional binary offsets in `CompactRow` across schema migrations, paired with zero-copy row transformations (`row_into_compact` and `compact_into_row`). `compact_into_row` and `from_compact_row` support backward evolution where `compact.len() <= self.columns.len()` by treating omitted columns as `Value::Null`. In `StorageEngine::apply_batch`, tuples are dynamically resized upon `Update` operations targeting newly added column indices. Dynamic schema evolution (`TableSchema::add_column`) strictly enforces that newly appended columns must be nullable (`nullable: true`) to preserve backward integrity over historical records.
* **Native $O(C)$ Positional Mutation Validation:** Positional validation algorithms (`TableSchema::validate_operation` and `TableSchema::validate_column_updates`) evaluate compact rows and sorted `ColumnUpdate` deltas directly in $O(C)$ linear time against column definitions, verifying types, nullability, E2EE constraints, and strictly ascending column index order without allocating or reconstructing intermediate `HashMap<String, Value>` instances.
* **End-to-End Encryption (E2EE):** Handled via metadata on `ColumnDef` (`encrypted: bool`). The schema preserves the real underlying `data_type` for client-side decryption, while the coordination server validates that encrypted fields in transit are transmitted strictly as opaque `Value::Bytes` without inspecting payload contents. Primary key columns cannot be encrypted. The `CryptoEngine` contract incorporates conditional `CryptoConcurrencyBounds` (`Send + Sync` on native, relaxed on `wasm32`) and authenticated additional data (`aad: &[u8]`, providing `(table_id, pk, column_idx)`) to cryptographically defend against ciphertext column substitution attacks.
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
* **Strict Last-Write-Wins (LWW) Causality:** When consolidating an incoming `UPDATE` over an existing `INSERT` (Rule 1), if `incoming.timestamp < existing.timestamp`, the stale update is discarded (`SquashOutcome::Discarded`), preventing obsolete updates from overwriting non-null or null columns.
* **Anti-Zombie Rule:** An `UPDATE` operation arriving after a `DELETE` on the same primary key cannot resurrect deleted records (obsolete updates are `Discarded`; newer updates are `Incompatible`).
* **Base Entity Preservation (Old Insert into Newer Update):** An `INSERT` arriving with an older timestamp than an existing pending `UPDATE` preserves full entity data: the newer update deltas are applied directly over the incoming base `CompactRow`, preventing entity or column loss.
* **Mutual Annihilation & Buffer Purge:** When a pending uncommitted `INSERT` receives a subsequent `DELETE` for the same PK before transmission to the network, both operations mutually annihilate (`SquashOutcome::Purged`). The primary key is completely removed from `TableBuffer::pending`, eliminating unnecessary network traffic and avoiding spurious tombstones.
* **Table-Partitioned Buffering (`TableBuffer`):** Buffers hold `table_id: u16` and manage their own `HashMap<PrimaryKey, Operation>`, ensuring $O(1)$ amortized squashing and zero lock contention between distinct tables.
*(Note: Squashing is strictly a client-side optimization performed prior to transmission. The coordination server never alters, merges, or deletes sequenced operations in the historical log).*

---

## 5. The Coordination Server (`zemdb-server`)

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
   * Serves real-time queries and sub-millisecond synchronization for currently connected clients.
   * Holds an immutable contiguous buffer of recent `SequencedOperation`s (`VecDeque`). No squashing is performed on sequenced deltas, ensuring absolute sequence contiguity (`head_seq + 1`) and preventing sequence gaps or zombie record anomalies.
   * Rotates and evicts older operations from RAM when the batch reaches the memory threshold (`ram_max_ops`) or RAM TTL expires, knowing data is already physically safe on disk.
2. **Tier 2 (Warm Disk Log - Uncompressed `.wal` con Write-Through):**
   * Stored directly on disk using the standard batch framing (`0xBA7C` magic, length, unified CRC32, operation count, and optional `mutation_id` in `WalBatchPayload`).
   * **Unified Single-WAL Persistence (Zero Desync & Single `fsync`):** Eradicates Dual-WAL and secondary metadata files (`meta_{room_id}.wal` / `MicroWal`). Every accepted mutation is synchronously written to `active.wal` with a single `sync_data()` before confirming `CommitAck`. This eliminates crashes between sequence allocation and operation delta logging, halves commit I/O latency, and hydrates `DedupLruCache` directly from the Warm and Cold segments on boot.
   * When HotBuffer rotates, `active.wal` is sealed as `segment_{start}_{end}.wal`. Remains uncompressed to avoid CPU decompression spikes when clients reconnect after hours or days.
   * Sequential I/O enables high-speed streaming during `/sync`.
3. **Tier 3 (Cold Disk Log - Compressed `.wal.zst`):**
   * As segments age past the Warm retention threshold, a background task compresses older `.wal` files into `.wal.zst` segments using Zstandard (isolated in `spawn_blocking`).
   * Allows rooms to retain historical deltas with minimal disk consumption.
4. **Tier 4 (Eviction & Compaction Boundary - Dual Mode):**
   * **Proactive Cursor-Driven Pruning (`prune_older_than`):** In active rooms, once all registered members confirm having processed up to sequence $S$, segments with `end_seq < S` are immediately purged from disk, reclaiming physical space without waiting for arbitrary TTLs.
   * **Client Lifecycle States (`Connected`, `Disconnected`, `Dormant`, `Bootstrapping`):**
     - **`Connected`:** The client is actively communicating (heartbeats, commits, syncs). If all registered clients are `Connected` and have acknowledged sequence $S$, segments older than $S$ are pruned immediately.
     - **`Disconnected`:** The client closed the application or lost network connectivity, but its cursor remains within the log range preserved on disk (`last_ack_seq >= tail_seq - 1`). Upon reconnecting within 90 seconds, it fetches missing deltas via standard `/sync` and transitions back to `Connected` without needing a snapshot.
     - **`Dormant`:** The client remained offline so long that either its pending deltas were physically purged (`last_ack_seq < tail_seq - 1`) OR its disconnected duration exceeded the 90-second lease timeout limit (`check_timeouts`). In either condition, the client transitions to `Dormant`. Crucially, `Dormant` clients are excluded from log retention calculations (`min_connected_ack_seq`), preventing abandoned clients from holding global log locks or causing unbounded disk bloat (Anti-Disk-Bloat / Deadlock Resolution).
     - **`Bootstrapping`:** The client is newly registered or reconnecting with `current_seq < tail_seq - 1`. It is currently downloading or applying a base snapshot. `Bootstrapping` clients are excluded from `min_connected_ack_seq` so they never block disk compaction for active peers.
   * **Accurate Physical `tail_seq` Computation:** On open and after every pruning pass, the server computes `tail_seq` strictly from the oldest physically retained operation on disk: the first Cold segment, else the first sealed Warm segment, else the start of `active.wal`. Every operation is written to disk before entering RAM, so the Hot Buffer never defines retention, and RAM TTL eviction never causes spurious `BehindCompaction` rejections. When nothing is retained, `tail_seq = head_seq + 1`, so a cursor at `head_seq - 1` is rejected instead of silently missing the last operation. A gap found while reading a range the log claims to retain is reported as `BehindCompaction`.
   * **Durable Sequence Floor (`log_meta.json`):** Before any segment is deleted, the highest pruned sequence is written atomically to `log_meta.json`. Recovery takes the maximum of that value and the retained segments, so a restart after every segment was pruned never reuses sequence numbers already delivered to clients.
   * **Snapshot Retention Anchor (*Ancla de Retención*):** To protect onboarding clients against race conditions where active peers continue appending commits and pruning logs while a snapshot is being downloaded, the server anchors proactive pruning to:
     $$\text{retention\_floor} = \min(\text{min\_connected\_ack}, \text{active\_snapshot\_seq})$$
     As long as an active snapshot at sequence $S$ is published in `SnapshotRelay`, operations $(S, \text{head\_seq}]$ are preserved on disk. When the bootstrapping client finishes applying snapshot $S$ and fetches deltas or ACKs $\ge S$, it is automatically promoted to `Connected`.
   * **TTL & Quota Compaction:** Historical operations exceeding `cold_disk_ttl` or the room disk quota (`max_room_disk_bytes`) are automatically pruned. The server tracks the oldest available sequence (`tail_seq`). Any sync request with `last_ack_seq < tail_seq - 1` receives `BehindCompaction`, recovering state via full base snapshot transfer from an active client (via server relay or P2P).

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

## 6. Onboarding, Handshake & Stateless Authentication

Because the server does not store the full persistent historical state and must remain completely decoupled from application user/password databases:

1. **Stateless Ticket Authentication & Registration Handshake:**
   * The application backend authenticates the end-user (OAuth, email/password, etc.) and issues a signed, time-bounded ticket:
     `auth_token = sign({ client_id, room_id, exp })` using a shared cluster secret (`ZEMDB_AUTH_SECRET`, HMAC-SHA256) or asymmetric key pair (Ed25519).
   * The client connects and issues `ClientMessage::RegisterClient { correlation_id, room_id, client_id, auth_token, current_seq: Option<SequenceNumber> }`.
   * The server validates the cryptographic signature in microsecond CPU time without querying any database or storing user passwords.
   * The server responds with `ServerMessage::Registered { head_seq, tail_seq, active_snapshot_seq: Option<SequenceNumber>, schema_id, schema }`, delivering room boundary coordinates and the full schema in 1 RTT.

2. **Client State & Synchronization Path Determination:**
   * **Direct Delta Catch-up (`Connected`):** If the client is already known or its `current_seq >= tail_seq - 1` (or the room is unpruned), the client enters `ClientState::Connected`. It immediately queries `/sync` starting from `current_seq` to catch up with all recent deltas.
   * **Snapshot Bootstrapping (`Bootstrapping`):** If the client is brand new or reconnecting with `current_seq < tail_seq - 1` (or `None` in a room where `tail_seq > 1`), it enters `ClientState::Bootstrapping`. The client is excluded from log retention tracking, preventing it from blocking active members.

3. **Snapshot Delivery & Retention Anchor:**
   * If an active base snapshot is already uploaded to `SnapshotRelay` (indicated by `active_snapshot_seq: Some(S)`), the client downloads it directly (via `GET /rooms/:id/snapshot/download` or the chunked protocol).
   * If no snapshot exists on the relay, an active connected peer generates an immutable Zstandard snapshot at sequence $S$ and uploads it (`POST /rooms/:id/snapshot/upload` with header `x-snapshot-head-seq: S`).
   * **Retention Anchor Protection:** While this snapshot remains active on the server relay, the room actor anchors its proactive pruning floor to $\min(\text{min\_connected\_ack}, S)$. This guarantees that all operations $(S, \text{head\_seq}]$ are preserved in the log.
   * Multiple clients can onboard simultaneously against the same snapshot without race conditions.

4. **Snapshot Restoration & Seamless Promotion:**
   * The bootstrapping client restores the snapshot locally via `StorageEngine::apply_snapshot`, setting its local sequence to $S$.
   * It then issues `ClientMessage::Sync { from_seq: S, .. }` or commits a mutation.
   * Upon processing the deltas $(S, \dots]$, the client's ACK or sync request automatically promotes it from `Bootstrapping` to `Connected`, and normal cursor tracking resumes.

### Multipart Chunked Snapshot Transfer Protocol (> 16 MB)

When database rooms grow large such that compressed snapshots exceed the strict DoS envelope (`MAX_MESSAGE_SIZE = 16 MB`), state transfer is partitioned into framed chunks:
1. **Chunk Negotiation & Request:** The bootstrapping client issues `ClientMessage::RequestSnapshotChunk { correlation_id, room_id, chunk_index, chunk_size }` (typically requesting $2\text{ MB}$ to $4\text{ MB}$ chunks).
2. **Chunk Streaming:** The server or relay peer streams `ServerMessage::SnapshotChunk { correlation_id, room_id, snapshot_head_seq, chunk_index, total_chunks, total_bytes, snapshot_hash, data }`.
3. **Cryptographic Integrity & Atomic Reconstruction:** All chunks `0..total_chunks` are assembled in a temporary staging buffer. The client validates the assembled byte stream against `snapshot_hash` using BLAKE3 (`ServerMessage::compute_snapshot_hash`), immediately rejecting corrupted transfers before decompression. If verified, it applies the snapshot in a single atomic transaction via `StorageEngine::apply_snapshot`.
4. **Resumed Delta Catch-up:** The client sets its local cursor to `snapshot_head_seq` and seamlessly queries `/sync` starting from `snapshot_head_seq + 1`.

---

## 7. Communication Protocol: Control Plane & Data Plane

The system strictly decouples administrative lifecycle management from high-throughput client synchronization:

### 7.1. Control Plane (Admin REST API - HTTP/JSON)
Dedicated administrative interface intended for application backends, CLI tools, and automated deployment pipelines:
* **Transport:** HTTP REST over TLS with JSON payloads.
* **Security:** Authenticated via administrative bearer token (`Authorization: Bearer <ADMIN_SECRET>`).
* **Endpoints:**
  * `POST /admin/schemas`: Declares or updates a schema template identified by `SchemaId`, containing table and column definitions.
  * `GET /admin/schemas/{schema_id}`: Retrieves schema metadata.
  * `POST /admin/schemas/{schema_id}/columns`: Append-only DDL evolution (`TableSchema::add_column`), enforcing nullable column additions and broadcasting `RoomEvent::SchemaReloaded` across all active rooms.
  * `POST /admin/rooms`: Provisions an isolated room binding `RoomId` to a `SchemaId` and `RoomLifecyclePolicy`.
  * `GET /admin/rooms/{room_id}`: Inspects room head sequence, disk usage, and active client leases. Verifies room existence passively with `room_exists`, returning HTTP 404 `NotFound` for non-existent rooms without spawning actors or creating directories on disk.
  * `DELETE /admin/rooms/{room_id}`: Archives or purges a room and its WAL.

### 7.2. Data Plane (Client Synchronization Protocol - Binary HTTP/2)
To prevent the resource exhaustion of thousands of idle persistent connections:
* **Pull-Based HTTP/2 Binary Protocol:**
  * Transport: HTTP/2 over TLS with binary payloads serialized via `bincode`.
  * **Canonical Wire Protocol Framing:** Every network frame is encapsulated in a fixed 4-byte header: Magic bytes `0x5A, 0x4D` (`"ZM"`), Protocol Version `0x01`, and Reserved Flags `0x00`. The codec validates this header before deserialization, rejecting unknown protocols or version mismatches with `ErrorCode::ProtocolVersionMismatch` (mapped to HTTP 400 `BadRequest`).
  * Wire Efficiency (50%+ Bandwidth Reduction): Replacing string-keyed dictionaries with `CompactRow` and `ColumnUpdate` deltas eliminates column names from the wire, reducing serialized insert/update payloads by 38% to 60%.
  * Multiplexing: Multiple sync/commit streams share a single underlying TCP connection using explicit correlation identifiers (`CorrelationId`).
  * Defensive Bounding: Codecs enforce an explicit message size limit (16 MB) to prevent denial-of-service memory exhaustion attacks.
* **Universal Security & Defense-in-Depth (`ClientAuth`):**
  * **Cryptographic Extractor (`ClientAuth`):** All operational Data Plane endpoints (`/commit`, `/sync`, `/ack`, `/heartbeat`, `/schema`, `/deregister`, `/events`) enforce cryptographic authentication via Axum's `FromRequestParts` extractor. Requests must supply `Authorization: Bearer <token>` (with query parameter `?token=<token>` supported for SSE `EventSource` connections).
  * **Constant-Time Verification (`subtle::ConstantTimeEq`):** Token HMAC-BLAKE3 signatures and admin secrets are verified using constant-time byte comparisons, eliminating side-channel timing attack vulnerabilities (`M-04`).
  * **Zero Backdoors:** Development bypasses (`"dev-token"`) are eliminated; all incoming tokens require valid cryptographic signatures issued with the configured secret (`M-05`).
  * **3-Way Route and Token Validation (`M-07`):** Handlers enforce that:
    1. The `room_id` in the URL path matches the `room_id` claim in the verified token.
    2. The `room_id` in the URL path matches the `room_id` in the binary `ClientMessage` payload.
    3. The `client_id` in the binary `ClientMessage` payload matches the `client_id` claim in the verified token.
    Any mismatch immediately aborts with `ErrorCode::Unauthorized` / `ServerError::Unauthorized` prior to actor dispatch, preventing cross-room spoofing and gateway bypasses.
  * **Actor-Level Lease Validation (`C-03`):** Even with a cryptographically valid token, `RoomActor` validates that the requesting client possesses an active registration in `ClientLeaseTracker` before processing `Commit`, `Sync`, `Ack`, or `Heartbeat`. Unregistered clients are rejected with `ServerError::Unauthorized`.
* **Core Interaction Contracts:**
  * **Handshake & Schema Delivery (`RegisterClient` -> `Registered`):** Client presents `auth_token` and optional `current_seq`, receiving current `head_seq`, `tail_seq`, `active_snapshot_seq`, `schema_id`, and `Schema` in 1 RTT.
  * **On-Demand Schema Refresh (`GetSchema` -> `Schema`):** Allows clients to refresh schema definitions during active DDL evolution without reconnecting (`GET /rooms/{room_id}/schema` authenticated via `ClientAuth`).
  * **Mutation Commit with Unified Sync (1 RTT):** Client submits an operation with its `MutationId`, `CorrelationId`, and its current cursor `last_ack_seq`. The server validates the mutation against schemas and constraints. If valid, the server assigns a monotonic `SequenceNumber` and returns `CommitAck` containing the assigned `SequenceNumber` along with any remote catchup deltas (`catchup_ops: Vec<SequencedOperation>`) that occurred between `last_ack_seq` and the new sequence (and `has_more: bool` pagination indicator). This allows the client to register the write, sync pending state, and apply canonical data locally in a single network roundtrip (1 RTT) without risk of local state corruption.
    - **Ordering:** a retried `MutationId` is acknowledged first, with its original `SequenceNumber` and a catch-up starting at the client's `last_ack_seq`. Only then is the client checked against the retention boundary: if `last_ack_seq < tail_seq - 1` the commit is rejected with `BehindCompaction` before being sequenced, leaving no trace in the log, the head or the SSE stream.
    - **Durable commits are always acknowledged:** once the mutation is written to `active.wal`, the reply is always a `CommitAck`. If the catch-up batch cannot be built, it is returned empty with `has_more: true`, directing the client to `/sync`, which reports the underlying error.
  * **Synchronization (Standalone / Polling):** Client requests operations starting from its `last_ack_seq` specifying a maximum batch size (`ClientMessage::Sync`). The server streams ordered `SequencedOperation` batches with pagination flags (`has_more`).
  * **Acknowledgment / Heartbeat:** Client periodically reports processed sequences (`ClientMessage::Heartbeat`), allowing the server to advance client leases and retention windows. Heartbeat is strictly lightweight liveness, while sequence cursor advancement occurs exclusively via explicit `ClientMessage::Ack` or inside `ClientMessage::Commit`.
  * **Orderly Deregistration (`DeregisterClient` -> `DeregisterAck`):** Client notifies orderly departure via `POST /rooms/{room_id}/deregister`, receiving `ServerMessage::DeregisterAck` over `application/octet-stream` with `StatusCode::OK` and removing the client from the room roster.
  * **Error Handling:** Typed error responses communicate states such as invalid payloads (`ErrorCode::SchemaViolation`), authorization failures (`ErrorCode::Unauthorized`), version mismatches (`ErrorCode::ProtocolVersionMismatch`), or `BehindCompaction`.
* **Optional Foreground Streaming:** While a client application is actively in the foreground, it can establish an ephemeral push channel (SSE: `GET /rooms/{room_id}/events` authenticated via `ClientAuth`) to receive real-time notifications of new commits (`event: head_advanced`) and DDL schema migrations (`event: schema_reloaded`).

---

## 8. Storage Engine (`zemdb-storage`)

Persisting data locally on clients and managing snapshots is decoupled into a dedicated storage crate implementing a common `StorageEngine` abstraction:
* **The `StorageEngine` Contract:**
  * Clean, network-agnostic async persistence contract: `open_room`, `close_room`, `apply_batch`, `get`, `get_by_id`, `scan`, `scan_by_id`, `get_head_seq`, `create_snapshot`, `apply_snapshot`.
  * **Direct ID Queries (`get_by_id` & `scan_by_id`):** Trait methods accept direct numerical table identifiers (`table_id: u16`), eliminating secondary string lookup overhead in the inner query pipeline.
  * **Active Storage Schema Validation:** `StorageEngine::apply_batch` validates every sequenced operation directly against `schema.validate_operation(&op.op)?` across both `MemoryStorageEngine` and `DiskStorageEngine`, preventing corrupt or mismatched rows from being committed to WAL or memory state.
  * **Universal Canonical Snapshot Envelope (`ZMSN`):** Snapshots across all storage engines share a standardized envelope with 4-byte magic `"ZMSN"`, version `1`, compression algorithm flag (0 = Raw/Memory, 1 = Zstd/Disk), original uncompressed length, and CRC32 checksum. This guarantees 100% cross-engine snapshot portability: snapshots created by `MemoryStorageEngine` can be applied directly by `DiskStorageEngine` and vice versa with automatic Zstd decompression.
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
* **On-Disk Engine (`DiskStorageEngine`) [IMPLEMENTED - Phase 2B & 2.8]:**
  * **Dual-File Architecture:** Separate physical files per room:
    * `room_{id}.snap`: Immutable base snapshot with 64-byte `ZEM1` header, schema fingerprint, and Zstandard block compression.
    * `room_{id}.wal`: Append-only Write-Ahead Log (WAL) containing batched mutational deltas framed with `0xBA7C` magic, payload length, unified CRC32, and operation count.
  * **Physical WAL Framing in Core, Zero-Filled EOF Detection & Multi-Op Buffering:** Framing definitions and batch codecs are centralized in `zemdb-core` (`protocol::wal_frame`) and shared directly by `zemdb-storage` and `zemdb-server` (Tier 2 Warm Disk Log). When modern thin-provisioned or pre-allocated filesystems crash and leave zero-padded trailing blocks or partial writes at EOF with CRC32 mismatch, `recover_room` and `decode_wal_batch_from_slice` detect terminal failures as a `TornWrite`, cleanly truncating the file in-place to `valid_wal_bytes` without aborting with corruption. `WalReader` buffers multi-operation batches internally in a FIFO queue so sequential record reads never discard operations #2..N of multi-operation batches. During recovery, operations with `op.seq <= snapshot_seq` are strictly skipped to prevent replaying deltas on consolidated snapshot state.
  * **Zero-Copy Snapshot Streaming (Anti-4x RAM Spike):** Both in-memory and on-disk snapshot generators serialize tables directly by reference (`RoomSnapshotRef<'a>`) into the Zstandard compressor, eliminating full database allocations and row cloning. Snapshot hydration assigns deserialized table trees (`RoomSnapshotPayload`) directly into memory state without intermediate vector-to-map transformations.
  * **Non-Blocking Background Copy-on-Write (CoW) Compaction:**
    * In-flight writes to `room_{id}.wal` are never blocked during Zstd compression.
    * New snapshot is written to `room_{id}.snap.tmp`.
    * Exclusive kernel `flock` is acquired on `.tmp` *before* atomic `rename` over `room_{id}.snap`.
    * Directory metadata is synced via POSIX `sync_dir`.
    * The active `room_{id}.wal` file is truncated in-place (`set_len(0)`), allowing active writers to proceed seamlessly without file recreation race conditions.
  * **Non-Blocking Chunked Scan Streams:** Scans stream items in chunks (64 tuples), releasing read locks between chunks to eliminate writer starvation.
  * **Multi-process exclusive file locking (`flock`):** Active process holds exclusive lock on the `.wal` file, preventing simultaneous mutations from concurrent processes.
  * **RAM-Resident Primary Index:** Tabular memory state in `BTreeMap<PrimaryKey, CompactRow>` enables $O(1)$ point lookups without full disk scans.
  * **Zero Global Lock Contention on File Sync (`close_room`):** In `DiskStorageEngine::close_room`, the room is extracted and the engine-level write lock is dropped *before* executing asynchronous disk sync (`wal_file.sync_all().await`), preventing file I/O operations from blocking access to other rooms across the engine.

---

## 9. Technology Stack & Workspace Structure

### 9.1. Language & Ecosystem: Rust
* Zero-cost abstractions and deterministic memory management without garbage collection pauses.
* High-performance asynchronous networking with `tokio` and `axum`.
* Fast, size-bounded binary serialization with `bincode` (strictly configured with `reject_trailing_bytes()` to prevent network stream desynchronization attacks).
* Universal compilation: native binaries for desktop/mobile and **WebAssembly (WASM)** targets for browsers.
* Strict workspace security policy: `#![forbid(unsafe_code)]` enforced across all crates.

### 9.2. Workspace Layout
```text
zemdb/
├── README.md                    # Project landing, crate architecture, phase status and navigation
├── ARCHITECTURE.md              # High-level architectural specification and ADRs
├── ROADMAP.md                   # Implementation roadmap, milestone checklists, and backlog
├── Cargo.toml                   # Root workspace manifest with centralized dependencies
├── docs/                        # Historical audits and technical proposals
│   ├── audits/                  # Multidisciplinary specialist audit reports
│   └── proposals/               # Architectural hardening and phase proposals
├── crates/
│   ├── core/                    # Pure domain models, schemas, operations, Newtypes, protocol (1-RTT messages, wal_frame 0xBA7C, Zero I/O, WASM)
│   ├── storage/                 # Tabular storage, Dual-File (.snap + .wal), RAM index, Zstd snapshots (StorageEngine trait)
│   ├── server/                  # Coordination server, Tokio room actors, LRU dedup, micro-WAL, HTTP/2 API
│   └── client/                  # Client SDK, Write-Through with 1-RTT Sync, canonical storage, dual transport
```

---

## 10. Architectural Decisions: Time, Ordering & Authority

### 10.1. Central Server Sequencer as the Sole Authority of Total Order
A foundational architectural decision of ZemDB is that **the coordination server is the single source of truth for global ordering**:
* **Sequence-Based Total Order:** When an operation is accepted by the Room actor on the server, it is assigned an atomically increasing `SequenceNumber`. The global order of events in the Room is defined 100% by this monotonic sequence number.
* **No Trust in Client Wall Clocks:** Client devices frequently suffer from clock skew (misconfigured system times, zone errors, drift). To eliminate the risk of a misconfigured device clock dominating or corrupting the room's history, **client timestamps are never used to determine global causal precedence**.
* **Deterministic Last-Write-Wins (LWW):** In the event of concurrent modifications to the exact same field, the operation with the higher `SequenceNumber` (the one processed later by the server sequencer) takes precedence. This eliminates the necessity of complex Hybrid Logical Clocks (HLC) while guaranteeing absolute convergence across all nodes.

### 10.2. The Three Concepts of Time in ZemDB
To prevent conceptual ambiguity, the architecture distinguishes three independent notions of time:
1. **`SequenceNumber` (Total Order Authority):** Monotonic integer issued exclusively by the server. Defines causal precedence, commit ordering, and conflict resolution across all clients.
2. **Mutation `timestamp: u64` (Client Metadata):** Client-generated timestamp attached to mutations for audit/tracing purposes. Not used for causal precedence or local optimistic rebase.
3. **Domain `Value::Timestamp(i64)` (Application Data):** Structured column data type used by end-user schemas to store business dates and timestamps (e.g., `created_at`, `due_date`).
