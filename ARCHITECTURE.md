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
* **Strongly-Typed Domain Identifiers & Encapsulated Models (C-DEREF Compliance):** Domain identities (`RoomId`, `SchemaId`, `ClientId`, `SequenceNumber`, `MutationId`, `CorrelationId`) and data models (`PrimaryKey`, `CompactRow`, `TableSchema`) are fully encapsulated with private inner representations. In strict accordance with Rust API Guidelines [C-DEREF], implicit `Deref` coercions are eradicated across all core structures to protect type abstraction boundaries and prevent accidental mutation of internal vectors or strings. String identifiers are validated by construction ("parse, don't validate"): `RoomId::new`, `SchemaId::new` and `ClientId::new` return `Result`, and deserialization (JSON, bincode) validates as well, so no code path can hold an invalid identifier, whatever its source (URL, binary payload, admin JSON, files on disk, or the `zemdb-storage` API). `RoomId` and `SchemaId` are used as file and directory names, so they are restricted to lowercase ASCII letters, digits, `-` and `_` (1 to 64 characters), excluding Windows reserved device names: this rules out path traversal (`/`, `..`) and collisions on case-insensitive file systems (macOS, Windows). `ClientId` is never used in paths and accepts any text of 1 to 256 bytes without control characters, without leading or trailing whitespace, and without the invisible, formatting, filler, variation-selector, tag and bidirectional control characters most used for spoofing (such as zero-width spaces, soft hyphens, BOM and bidi overrides). This is a denylist, not a guarantee that two distinct client IDs never look alike: no Unicode normalization is applied. Tokens bind the exact bytes, so this only affects display, never authentication. Validation errors echo at most 80 characters of the rejected input. Ergonomics and zero-overhead performance are maintained through explicit accessors (`as_str()`, `get()`, `as_bytes()`, `columns()`, `column_indices()`), standard trait indexing (`Index<usize>`, `IndexMut<usize>`), iterator APIs (`iter()`, `as_slice()`), trait conversions (`AsRef<str>`, `AsRef<[u8]>`, `AsRef<[Value]>`, `From`), and compact Serde representations: numeric and byte identifiers are `#[serde(transparent)]`, while the string identifiers use `#[serde(try_from = "String", into = "String")]`, so they are still serialized as plain strings but validated on deserialization.
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
7. **Ephemeral Snapshot Relay:** Relays compressed multipart snapshot chunks between active clients and onboarding/reconnecting peers without constructing or interpreting database states itself. It keeps at most one snapshot per room, on disk only, for a limited time (see [Snapshot Relay](#snapshot-relay-storage-acceptance-and-limits)).

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
   * **Durable directory entries:** the segments directory is synced after `active.wal` is sealed by rename, and when a fresh `active.wal` is created (before its first record is acknowledged), so a power loss can neither lose a sealed segment nor the file holding acknowledged commits.
   * **Failed append stops the room:** if writing or syncing a record to `active.wal` fails, the on-disk state is unknown (a failed `fsync` cannot be retried safely). The room actor replies with an internal (retryable) error, closes its mailbox and terminates. If the record was already synced and only sealing the segment afterwards fails, the commit is acknowledged normally and the actor then terminates the same way. In both cases commands already queued behind it are dropped, so their callers get an error instead of hanging. The next request makes `RoomManager` wait for the old actor to release the room's files and respawn it from disk. If the record did reach disk, deduplication recognizes it, so a retry with the same `MutationId` receives a `CommitAck` with the sequence that was written.
   * Sequential I/O enables high-speed streaming during `/sync`.
3. **Tier 3 (Cold Disk Log - Compressed `.wal.zst`):**
   * As segments age past the Warm retention threshold, a background task compresses older `.wal` files into `.wal.zst` segments using Zstandard (isolated in `spawn_blocking`).
   * Allows rooms to retain historical deltas with minimal disk consumption.
   * The compressed segment is written to a temporary file, synced and renamed into place, and the directory is synced before the warm segment is deleted (and again after the deletion), so a power loss never persists the deletion without the replacement.
4. **Tier 4 (Eviction & Compaction Boundary - Dual Mode):**
   * **Proactive Cursor-Driven Pruning (`prune_older_than`):** In active rooms, once all registered members confirm having processed up to sequence $S$, segments with `end_seq < S` are immediately purged from disk, reclaiming physical space without waiting for arbitrary TTLs.
   * **Client Lifecycle States (`Connected`, `Disconnected`, `Dormant`, `Bootstrapping`):**
     - **`Connected`:** The client is actively communicating (heartbeats, commits, syncs). If all registered clients are `Connected` and have acknowledged sequence $S$, segments older than $S$ are pruned immediately.
     - **`Disconnected`:** The client closed the application or lost network connectivity, but its cursor remains within the log range preserved on disk (`last_ack_seq >= tail_seq - 1`). Upon reconnecting within 90 seconds, it fetches missing deltas via standard `/sync` and transitions back to `Connected` without needing a snapshot.
     - **`Dormant`:** The client remained offline so long that either its pending deltas were physically purged (`last_ack_seq < tail_seq - 1`) OR its disconnected duration exceeded the 90-second lease timeout limit (`check_timeouts`). In either condition, the client transitions to `Dormant`. Crucially, `Dormant` clients are excluded from log retention calculations (`min_connected_ack_seq`), preventing abandoned clients from holding global log locks or causing unbounded disk bloat (Anti-Disk-Bloat / Deadlock Resolution).
     - **`Bootstrapping`:** The client is newly registered or reconnecting with `current_seq < tail_seq - 1`. It is currently downloading or applying a base snapshot. `Bootstrapping` clients are excluded from `min_connected_ack_seq` so they never block disk compaction for active peers.
   * **Accurate Physical `tail_seq` Computation:** On open and after every pruning pass, the server computes `tail_seq` strictly from the oldest physically retained operation on disk: the first Cold segment, else the first sealed Warm segment, else the start of `active.wal`. Every operation is written to disk before entering RAM, so the Hot Buffer never defines retention, and RAM TTL eviction never causes spurious `BehindCompaction` rejections. When nothing is retained, `tail_seq = head_seq + 1`, so a cursor at `head_seq - 1` is rejected instead of silently missing the last operation. A gap found while reading a range the log claims to retain is reported as `BehindCompaction`.
   * **Client Roster Persistence (`meta_clients_{room_id}.json`):** Registration and deregistration are written immediately. Cursor and lease-state changes (ACKs, cursors reported by commits, heartbeats, timeouts) update memory and mark the roster dirty; the room actor writes it on its 500 ms maintenance tick, when it stops (shutdown or failure), and before any proactive prune that deletes segments, so the roster on disk never falls behind the retained log. A crash therefore loses at most ~500 ms of cursor progress, which only makes the server retain more log. A roster that cannot be parsed at open is discarded with a warning: the room starts with an empty roster, clients register again, and proactive pruning stays stopped until they do.
   * **Atomic Server Metadata:** `meta_room.json`, the client roster and the schema files (`schemas/{schema_id}.json`) are always replaced through a temporary file that is synced, renamed over the destination, followed by a sync of the parent directory. Leftover `.tmp` files from an interrupted write are removed on load. Directories created by the server (room, segments, schemas) are synced into their parent directory. An unreadable `meta_room.json` or schema file makes loading fail rather than serving a room with an unknown schema.
   * **Durable Sequence Floor (`log_meta.json`):** Before any segment is deleted, the highest pruned sequence is written atomically to `log_meta.json`. Recovery takes the maximum of that value and the retained segments, so a restart after every segment was pruned never reuses sequence numbers already delivered to clients.
   * **Snapshot Retention Anchor (*Ancla de Retención*):** To protect onboarding clients against race conditions where active peers continue appending commits and pruning logs while a snapshot is being downloaded, the server anchors proactive pruning to:
     $$\text{retention\_floor} = \min(\text{min\_connected\_ack}, \text{active\_snapshot\_seq})$$
     As long as an active snapshot at sequence $S$ is published in `SnapshotRelay`, operations $(S, \text{head\_seq}]$ are preserved on disk. The relay only accepts snapshots inside the retained range ($\text{tail\_seq} - 1 \le S \le \text{head\_seq}$); if TTL or quota compaction later moves `tail_seq` past $S + 1$, the snapshot no longer anchors pruning. When the bootstrapping client finishes applying snapshot $S$ and fetches deltas or ACKs $\ge S$, it is automatically promoted to `Connected`.
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
     `auth_token` signed with the shared cluster secret (`ZEMDB_AUTH_SECRET`). Format: `v1.<base64url(client_id)>.<base64url(room_id)>.<expires_at>.<hex_signature>`. The IDs are base64url-encoded (an alphabet without `.`), so the token is unambiguous for any valid ID. The signature is keyed BLAKE3 over the length-prefixed claims `(client_id, room_id, expires_at)`, with a signing key derived from the secret via `blake3::derive_key` under a dedicated context string, so the key is never shared with other uses of the secret. `expires_at` must be in canonical decimal form (no sign, no leading zeros), so each claim set has exactly one valid token string. The version prefix allows the format to evolve; tokens in any other format are rejected.
   * The client connects and issues `ClientMessage::RegisterClient { correlation_id, room_id, client_id, auth_token, current_seq: Option<SequenceNumber> }`.
   * The server validates the cryptographic signature in microsecond CPU time without querying any database or storing user passwords.
   * The server responds with `ServerMessage::Registered { head_seq, tail_seq, active_snapshot_seq: Option<SequenceNumber>, schema_id, schema }`, delivering room boundary coordinates and the full schema in 1 RTT.

2. **Client State & Synchronization Path Determination:**
   * **Direct Delta Catch-up (`Connected`):** If the client is already known or its `current_seq >= tail_seq - 1` (or the room is unpruned), the client enters `ClientState::Connected`. It immediately queries `/sync` starting from `current_seq` to catch up with all recent deltas.
   * **Snapshot Bootstrapping (`Bootstrapping`):** If the client is brand new or reconnecting with `current_seq < tail_seq - 1` (or `None` in a room where `tail_seq > 1`), it enters `ClientState::Bootstrapping`. The client is excluded from log retention tracking, preventing it from blocking active members.

3. **Snapshot Delivery & Retention Anchor:**
   * If an active base snapshot is already uploaded to `SnapshotRelay` (indicated by `active_snapshot_seq: Some(S)`), the client downloads it with the chunked protocol (`POST /rooms/:id/snapshot/chunk`).
   * If no snapshot exists on the relay, an active connected peer generates an immutable Zstandard snapshot at sequence $S$ and uploads it, in one request (`POST /rooms/:id/snapshot/upload` with header `x-snapshot-head-seq: S`, up to the 16 MB body limit) or in chunks (`POST /rooms/:id/snapshot/upload-chunk`).
   * **Retention Anchor Protection:** While this snapshot remains active on the server relay, the room actor anchors its proactive pruning floor to $\min(\text{min\_connected\_ack}, S)$. This guarantees that all operations $(S, \text{head\_seq}]$ are preserved in the log.
   * Multiple clients can onboard simultaneously against the same snapshot without race conditions.

4. **Snapshot Restoration & Seamless Promotion:**
   * The bootstrapping client restores the snapshot locally via `StorageEngine::apply_snapshot`, setting its local sequence to $S$.
   * It then issues `ClientMessage::Sync { from_seq: S, .. }` or commits a mutation.
   * Upon processing the deltas $(S, \dots]$, the client's ACK or sync request automatically promotes it from `Bootstrapping` to `Connected`, and normal cursor tracking resumes.

### Multipart Chunked Snapshot Transfer Protocol (> 16 MB)

When database rooms grow large such that compressed snapshots exceed the strict DoS envelope (`MAX_MESSAGE_SIZE = 16 MB`), state transfer is partitioned into framed chunks:
1. **Anchored Chunk Requests:** The bootstrapping client issues `ClientMessage::RequestSnapshotChunk { correlation_id, room_id, chunk_index, chunk_size, snapshot_hash }`. The first request of a download (chunk 0) carries `snapshot_hash: None` and gets the room's active snapshot; every following request must carry the `snapshot_hash` returned by the first reply (a request for a later chunk without it is `BadRequest`, 400), anchoring the whole download to that snapshot. The unanchored first request also asks the room actor for the retained log range: a snapshot that fell below it ($S < \text{tail\_seq} - 1$, so the client could not catch up from the log) is dropped and its file deleted, and the answer is `RoomNotFound` (404), as when no snapshot was ever staged. The server clamps `chunk_size` to 64 KiB–4 MiB; the client learns the size actually used from `total_chunks` and `total_bytes`.
2. **Chunk Streaming:** The relay answers `ServerMessage::SnapshotChunk { correlation_id, room_id, snapshot_head_seq, chunk_index, total_chunks, total_bytes, snapshot_hash, data }`, reading the chunk from the snapshot file.
3. **Superseded Snapshots:** If the anchored snapshot is no longer the active one (a newer snapshot replaced it, or it expired), including when reading its file fails because it was replaced during the read, the relay answers `ErrorCode::SnapshotSuperseded` (HTTP 409). The client discards the chunks received so far and restarts from chunk 0 without `snapshot_hash`. Anchoring by hash rather than by sequence also catches a replacement with the same sequence.
4. **Cryptographic Integrity & Atomic Reconstruction:** All chunks `0..total_chunks` are assembled in a temporary staging buffer. The client validates the assembled byte stream against `snapshot_hash` using BLAKE3 (`ServerMessage::compute_snapshot_hash`), immediately rejecting corrupted transfers before decompression. If verified, it applies the snapshot in a single atomic transaction via `StorageEngine::apply_snapshot`.
5. **Resumed Delta Catch-up:** The client sets its local cursor to `snapshot_head_seq` and seamlessly queries `/sync` starting from `snapshot_head_seq + 1`.

Uploads in chunks use `ClientMessage::UploadSnapshotChunk { snapshot_head_seq, chunk_index, total_chunks, total_bytes, snapshot_hash, data }`, acknowledged with `ServerMessage::SnapshotUploadChunkAck { staged }` (`staged: true` on the chunk that completes the upload):
* Every chunk except the last has exactly `ceil(total_bytes / total_chunks)` bytes; the last has the exact remainder. Chunks are at most 4 MiB and, when there is more than one, at least 64 KiB (a smaller snapshot is uploaded in fewer chunks). `total_bytes` is at most `max_snapshot_bytes`. Chunks may arrive in any order; resending a chunk overwrites it. Resending a chunk of an upload that already completed (same sequence and hash as the active snapshot, for example after a lost acknowledgment) is acknowledged again with `staged: true`; likewise, repeating a single-request upload of the active snapshot returns success.
* A room has at most one upload in progress, and it records who started it (the snapshot worker, authenticated with the admin secret, or a member's client id). A chunk for a higher sequence replaces the current upload (its partial file is deleted). A chunk for the same sequence with different parameters replaces it only if it comes from the snapshot worker or from the member who started it (to restart its own upload); otherwise, and for a lower sequence, it is rejected with `SnapshotSuperseded` (409). So a member keeping a bogus upload alive cannot block the snapshot worker. An upload that receives no chunk for 2 minutes is discarded.
* When the last chunk arrives, the relay hashes the assembled file with BLAKE3 and rejects it (`BadRequest`) unless it matches `snapshot_hash`.

### Snapshot Relay: Storage, Acceptance and Limits

* **Disk only:** Staged snapshots are never held in memory. The relay keeps their metadata (sequence, size, BLAKE3 hash, path) and serves each download chunk by opening the file, seeking and reading the range on Tokio's blocking pool. Files live in `<data_dir>/snapshots/` as `<room>_<seq>_<blake3>.snap`; partial uploads live in `<data_dir>/snapshots/uploads/`. A snapshot is written to a temporary file, synced, renamed into place and the directories synced; the previous snapshot file of the room is deleted only after that. If any step fails the previous snapshot stays active.
* **Acceptance:** A snapshot at sequence $S$ (single request, or a completed chunked upload) is accepted only if the room exists (`RoomNotFound`, 404, otherwise), $\text{tail\_seq} - 1 \le S \le \text{head\_seq}$ as reported by the room actor (`BadRequest`, 400, otherwise; for chunked uploads checked when the upload starts and again when it completes, since both bounds move), $S$ is strictly greater than the active snapshot's sequence (`SnapshotSuperseded`, 409, otherwise), and the bytes form a `ZMSN` envelope with a valid header and a CRC32 matching the body (`BadRequest` otherwise). Changes to a room's snapshot and upload are serialized per room, so concurrent uploads can neither delete each other's files nor move the active snapshot backwards.
* **What the server does not check:** The relay validates only the envelope header and checksum; it never decompresses the snapshot and has no room state to compare it with. A snapshot can therefore be well-formed and still wrong: the client must validate its structure when applying it (Phase 4), and a malicious room member (or the holder of the admin secret) can still upload a poisoned snapshot within the acceptance rules. Defending against that is out of scope for v0.1.
* **Limits:** `max_snapshot_bytes` (default 512 MiB, `ZEMDB_MAX_SNAPSHOT_BYTES`) bounds every snapshot; it must lie between 1 MiB and 64 GiB, or the server refuses to start. Single-request uploads are additionally bounded by the 16 MB request body limit (a larger body is a binary `BadRequest` frame with HTTP 413), and at most 2 single-request uploads per room may be in progress at once, since each holds its body in memory while it waits for the room; a third gets `RateLimited` (HTTP 429). Staged snapshots expire after `snapshot_ttl_secs` (default 600 s, `ZEMDB_SNAPSHOT_TTL_SECS`); a background task deletes expired snapshots and idle uploads every 30 s.
* **Startup recovery:** On start the relay deletes every partial upload and, for each room, keeps only the highest-sequence snapshot file that is within the TTL (by modification time) and intact (valid envelope, content hash equal to the one in its name); all other snapshot files are deleted. A file that cannot be read (an I/O error rather than invalid content) is left in place and not served, and no older snapshot of that room is activated in its place, so the active snapshot never moves backwards. Failing to delete a stale file is logged and does not stop the server; the next start retries.
* **Room deletion:** Deleting a room removes its staged snapshot, its upload in progress and their files before the room directory, so a room recreated with the same id never serves the old room's snapshot. An upload that was under way when the room was deleted fails with `RoomNotFound`.
* **Client errors are not server errors:** Malformed chunk parameters, out-of-range indexes, oversized snapshots and unacceptable sequences are `BadRequest` (400); a request for a room without a usable staged snapshot is `RoomNotFound` (404); a superseded snapshot or upload is `SnapshotSuperseded` (409); a body over the size limit is 413 and too many concurrent single-request uploads is `RateLimited` (429). As everywhere in the Data Plane, these are binary `ServerMessage::Error` frames.

---

## 7. Communication Protocol: Control Plane & Data Plane

The system strictly decouples administrative lifecycle management from high-throughput client synchronization:

### 7.1. Control Plane (Admin REST API - HTTP/JSON)
Dedicated administrative interface intended for application backends, CLI tools, and automated deployment pipelines:
* **Transport:** HTTP REST over TLS with JSON payloads.
* **Security:** Authenticated via administrative bearer token (`Authorization: Bearer <ADMIN_SECRET>`).
* **Secret requirements:** On startup the server refuses to run unless both secrets (`ZEMDB_AUTH_SECRET` for client tokens, `ZEMDB_ADMIN_SECRET` for administration and snapshot workers) are set, at least 32 bytes long, different from each other, and different from the built-in development values (which are public in the source code). Error messages name the offending setting but never include the secret.
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
  * **Canonical Wire Protocol Framing:** Every network frame is encapsulated in a fixed 4-byte header: Magic bytes `0x5A, 0x4D` (`"ZM"`), Protocol Version `0x01`, and Reserved Flags `0x00`. The codec validates this header before deserialization, rejecting unknown protocols or version mismatches; the server answers any request body it cannot decode with `ErrorCode::BadRequest` (HTTP 400). The protocol version stays `0x01` until the first release; from then on it changes with every incompatible wire change between published releases.
  * Wire Efficiency (50%+ Bandwidth Reduction): Replacing string-keyed dictionaries with `CompactRow` and `ColumnUpdate` deltas eliminates column names from the wire, reducing serialized insert/update payloads by 38% to 60%.
  * Multiplexing: Multiple sync/commit streams share a single underlying TCP connection using explicit correlation identifiers (`CorrelationId`).
  * Defensive Bounding: Codecs enforce an explicit message size limit (16 MB) to prevent denial-of-service memory exhaustion attacks.
* **Universal Security & Defense-in-Depth (edge extractors, `api/extract.rs`):**
  * **Edge Validation Extractors:** Untrusted input is parsed once, by Axum extractors, before any handler code runs:
    * `RoomPath` parses the `:room_id` path segment into a `RoomId` (invalid → binary `ErrorCode::BadRequest`, HTTP 400).
    * `AuthenticatedRoom { room_id, client_id }` adds cryptographic authentication: all operational Data Plane endpoints (`/commit`, `/sync`, `/ack`, `/heartbeat`, `/schema`, `/deregister`, `/events`) require a client token in `Authorization: Bearer <token>`, validly signed, unexpired and issued for the path room. A missing or invalid token, or a token for another room, is rejected with a binary `ErrorCode::Unauthorized` frame (HTTP 401). Tokens in the URL are not accepted on these endpoints (they would end up in access logs); the only exception is `/events`, through `EventStreamAuth`, which also accepts `?token=<token>` because browser `EventSource` connections cannot set headers.
    * `RelayAuth` does the same for the snapshot relay endpoints (`/snapshot/upload`, `/snapshot/chunk`, `/snapshot/upload-chunk`), which additionally accept the admin secret as Bearer token (automated snapshot workers).
    * `BinaryMessage<T>` decodes the request body with the wire codec, within the router's 16 MB body limit; any decode failure, including an invalid identifier inside the payload, is a binary `ErrorCode::BadRequest` (HTTP 400); a body that cannot be read keeps its status (413 when too large). The error frame carries the path room id when it is valid. All Data Plane errors, including authentication failures and the snapshot relay's, are binary `ServerMessage::Error` frames; only some successful responses (the snapshot upload acknowledgment) are JSON.
    * Admin endpoints use `AdminPath<T>` (validated `RoomId`/`SchemaId` path parameters) and `AdminJson<T>` (JSON bodies), whose rejections are JSON `ErrorCode::BadRequest` responses: HTTP 400, or 413/415 for an oversized body or a missing JSON content type.
    * `register` carries its token inside the `RegisterClient` message, so it uses `RoomPath` + `BinaryMessage` and verifies the token bound to the message's client and room.
  * **Constant-Time Verification (`subtle::ConstantTimeEq`):** Token keyed-BLAKE3 signatures and admin secrets are verified using constant-time byte comparisons, eliminating side-channel timing attack vulnerabilities (`M-04`).
  * **Zero Backdoors:** Development bypasses (`"dev-token"`) are eliminated; all incoming tokens require valid cryptographic signatures issued with the configured secret (`M-05`).
  * **3-Way Route and Token Validation (`M-07`):** Before actor dispatch:
    1. The `room_id` in the URL path must match the `room_id` claim in the verified token (checked by the extractor; mismatch → `ErrorCode::Unauthorized`, HTTP 401).
    2. The `room_id` in the binary `ClientMessage` payload must match the URL path (checked by the handler; mismatch → `ErrorCode::BadRequest`, HTTP 400).
    3. The `client_id` in the binary `ClientMessage` payload must match the `client_id` claim in the verified token (checked by the handler; mismatch → `ErrorCode::Unauthorized`, HTTP 401).
    This prevents cross-room spoofing and client impersonation.
  * **Actor-Level Lease Validation (`C-03`):** Even with a cryptographically valid token, `RoomActor` validates that the requesting client possesses an active registration in `ClientLeaseTracker` before processing `Commit`, `Sync`, `Ack`, or `Heartbeat`. Unregistered clients are rejected with `ServerError::Unauthorized`.
* **Core Interaction Contracts:**
  * **Handshake & Schema Delivery (`RegisterClient` -> `Registered`):** Client presents `auth_token` and optional `current_seq`, receiving current `head_seq`, `tail_seq`, `active_snapshot_seq`, `schema_id`, and `Schema` in 1 RTT.
  * **On-Demand Schema Refresh (`GetSchema` -> `Schema`):** Allows clients to refresh schema definitions during active DDL evolution without reconnecting (`POST /rooms/{room_id}/schema` authenticated via `AuthenticatedRoom`).
  * **Mutation Commit with Unified Sync (1 RTT):** Client submits an operation with its `MutationId`, `CorrelationId`, and its current cursor `last_ack_seq`. The server validates the mutation against schemas and constraints. If valid, the server assigns a monotonic `SequenceNumber` and returns `CommitAck` containing the assigned `SequenceNumber` along with any remote catchup deltas (`catchup_ops: Vec<SequencedOperation>`) that occurred between `last_ack_seq` and the new sequence (and `has_more: bool` pagination indicator). This allows the client to register the write, sync pending state, and apply canonical data locally in a single network roundtrip (1 RTT) without risk of local state corruption.
    - **Ordering:** a retried `MutationId` is acknowledged first, with its original `SequenceNumber` and a catch-up starting at the client's `last_ack_seq`. Only then is the client checked against the retention boundary: if `last_ack_seq < tail_seq - 1` the commit is rejected with `BehindCompaction` before being sequenced, leaving no trace in the log, the head or the SSE stream.
    - **Cursor advance:** an accepted commit advances the client's cursor to its `last_ack_seq` (never backwards), so clients that only write still move the retention floor. A fresh commit also refreshes the client's lease and marks it `Connected`; a deduplicated retry only advances the cursor and leaves the lifecycle state unchanged. Rejected commits never touch the cursor.
    - **Durable commits are always acknowledged:** once the mutation is written to `active.wal`, the reply is always a `CommitAck`. If the catch-up batch cannot be built, it is returned empty with `has_more: true`, directing the client to `/sync`, which reports the underlying error.
  * **Synchronization (Standalone / Polling):** Client requests operations starting from its `last_ack_seq` specifying a maximum batch size (`ClientMessage::Sync`). The server streams ordered `SequencedOperation` batches with pagination flags (`has_more`).
  * **Acknowledgment / Heartbeat:** Client periodically reports processed sequences (`ClientMessage::Heartbeat`), allowing the server to advance client leases and retention windows. Heartbeat is strictly lightweight liveness, while sequence cursor advancement occurs exclusively via explicit `ClientMessage::Ack` or inside `ClientMessage::Commit`.
  * **Orderly Deregistration (`DeregisterClient` -> `DeregisterAck`):** Client notifies orderly departure via `POST /rooms/{room_id}/deregister`, receiving `ServerMessage::DeregisterAck` over `application/octet-stream` with `StatusCode::OK` and removing the client from the room roster.
  * **Error Handling:** Typed error responses communicate states such as invalid payloads (`ErrorCode::SchemaViolation`), authorization failures (`ErrorCode::Unauthorized`), version mismatches (`ErrorCode::ProtocolVersionMismatch`), malformed requests (`ErrorCode::BadRequest`, HTTP 400: an invalid identifier in the URL path, the binary payload or admin JSON, an undecodable body, a message of the wrong kind for the endpoint, or a payload `room_id` that differs from the path), `BehindCompaction`, or a snapshot that is no longer the active one (`ErrorCode::SnapshotSuperseded`, HTTP 409).
* **Optional Foreground Streaming:** While a client application is actively in the foreground, it can establish an ephemeral push channel (SSE: `GET /rooms/{room_id}/events` authenticated via `AuthenticatedRoom`) to receive real-time notifications of new commits (`event: head_advanced`) and DDL schema migrations (`event: schema_reloaded`).
* **Graceful Shutdown (`serve_until_shutdown`):** On SIGINT or SIGTERM (on Windows: Ctrl+C, console close or system shutdown) the server stops accepting connections and fires a shared `ShutdownSignal` that ends every open SSE stream, waits for in-flight requests, then shuts down all room actors, each of which persists its client roster. If this takes longer than 10 s the process exits anyway; acknowledged commits are already durable.

---

## 8. Storage Engine (`zemdb-storage`)

Persisting data locally on clients and managing snapshots is decoupled into a dedicated storage crate implementing a common `StorageEngine` abstraction:
* **The `StorageEngine` Contract:**
  * Clean, network-agnostic async persistence contract: `open_room`, `close_room`, `apply_batch`, `get`, `get_by_id`, `scan`, `scan_by_id`, `get_head_seq`, `create_snapshot`, `apply_snapshot`.
  * **Direct ID Queries (`get_by_id` & `scan_by_id`):** Trait methods accept direct numerical table identifiers (`table_id: u16`), eliminating secondary string lookup overhead in the inner query pipeline.
  * **Active Storage Schema Validation:** `StorageEngine::apply_batch` validates every sequenced operation directly against `schema.validate_operation(&op.op)?` across both `MemoryStorageEngine` and `DiskStorageEngine`, preventing corrupt or mismatched rows from being committed to WAL or memory state.
  * **Universal Canonical Snapshot Envelope (`ZMSN`):** Snapshots across all storage engines share a standardized envelope with 4-byte magic `"ZMSN"`, version `1`, compression algorithm flag (0 = Raw/Memory, 1 = Zstd/Disk), original uncompressed length, and CRC32 checksum. This guarantees 100% cross-engine snapshot portability: snapshots created by `MemoryStorageEngine` can be applied directly by `DiskStorageEngine` and vice versa with automatic Zstd decompression. The header definition and its validation (`SnapshotEnvelopeHeader`, `validate_snapshot_envelope`, the incremental `SnapshotEnvelopeValidator`) live in `zemdb-core` (`protocol::snapshot_envelope`), shared by `zemdb-storage` and the server's snapshot relay; storage adds compression and decompression.
  * **Bounded Snapshot Decompression:** The declared uncompressed length is never trusted. `decode_snapshot_envelope` rejects an envelope declaring more than 2 GiB (`DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES`; `decode_snapshot_envelope_with_limit` and `DiskStorageOptions::max_snapshot_uncompressed_bytes` set another limit) before decompressing, decompresses with a streaming decoder that stops one byte past the declared length, and fails unless the output has exactly the declared length. The up-front buffer reservation is capped at 64 MiB.
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
  * **Physical WAL Framing in Core, Zero-Filled EOF Detection & Multi-Op Buffering:** Framing definitions and batch codecs are centralized in `zemdb-core` (`protocol::wal_frame`) and shared directly by `zemdb-storage` and `zemdb-server` (Tier 2 Warm Disk Log). When modern thin-provisioned or pre-allocated filesystems crash and leave zero-padded trailing blocks or partial writes at EOF with CRC32 mismatch, `recover_room` and `decode_wal_batch_from_slice` detect terminal failures as a `TornWrite`, cleanly truncating the file in-place to `valid_wal_bytes` without aborting with corruption. `WalReader` buffers multi-operation batches internally in a FIFO queue so sequential record reads never discard operations #2..N of multi-operation batches. During recovery, operations at or below the current `head_seq` are skipped, so records already in the snapshot, or duplicated across files by a crash during compaction, are never applied twice. A record that does not continue the sequence exactly (`head_seq + 1`) is a gap and fails recovery with `WalCorruption`.
  * **Zero-Copy Snapshot Streaming (Anti-4x RAM Spike):** Both in-memory and on-disk snapshot generators serialize tables directly by reference (`RoomSnapshotRef<'a>`) into the Zstandard compressor, eliminating full database allocations and row cloning. Snapshot hydration assigns deserialized table trees (`RoomSnapshotPayload`) directly into memory state without intermediate vector-to-map transformations.
  * **Non-Blocking Background Copy-on-Write (CoW) Compaction:**
    * Phase 1 (brief lock): the active `room_{id}.wal` is renamed to `room_{id}.wal.compacting` and a fresh, locked `room_{id}.wal` takes incoming writes; the directory is synced. If the fresh WAL cannot be opened, the rename is rolled back so writes keep landing in the active WAL. If a previous compaction failed and left `.wal.compacting` behind, the active WAL is appended to it (and synced) before being truncated, so the orphan is never overwritten.
    * Phase 2 (background): the captured tables are compressed into a synced `room_{id}.snap.tmp.{uuid}`. In-flight writes to `room_{id}.wal` are never blocked.
    * Phase 3 (brief lock): the temporary file is atomically renamed over `room_{id}.snap`, then `.wal.compacting` is removed, syncing the directory after each step.
    * The compaction flag is an atomic released by a guard on drop, so a failure at any step never leaves compaction permanently disabled.
  * **Crash-Safe Recovery of an Interrupted Compaction:** On open, a leftover `.wal.compacting` is replayed before the active WAL. If it held live records, a fresh snapshot of the recovered state is written through the atomic path, the active WAL is emptied, and only then is `.wal.compacting` removed. Files are never rewritten in place and the locked WAL handle is never swapped, so a crash at any step leaves files that a later recovery replays to the same state.
  * **Failed Room State on Uncertain I/O:** If a WAL write or `fsync` fails, if a rotated WAL cannot be restored after a failed rotation, or if an applied snapshot cannot be persisted, the on-disk state is no longer known from inside the process (a failed `fsync` cannot be safely retried). The room is marked as failed: writes and compactions return `StorageError::RoomFailed` until it is closed and reopened, so that recovery rebuilds it from what is actually on disk. During compaction the active WAL is read through its own locked handle (a second handle cannot read a locked file on Windows), and a partially failed append to `.wal.compacting` is truncated back to its previous length.
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
