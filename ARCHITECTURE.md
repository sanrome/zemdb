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
* **Primitive Data Types:** `Int`, `Float`, `String`, `Bool`, `Bytes`, `Null`, and `Timestamp` (domain date/time). Additional types like `Uuid` are planned for Phase 2, while arbitrary-precision `Decimal` is deferred to post-v0.1.
* **Strongly-Typed Domain Identifiers (Newtypes):** Domain identities (`RoomId`, `ClientId`, `SequenceNumber`, `MutationId`, `CorrelationId`) are strictly typed to prevent parameter transposition bugs while maintaining zero-overhead binary representations.
* **In-Memory Density:** Tuples are stored positionally (`CompactRow`) without duplicating column names across rows, and primary keys are optimized to reside on the stack (`SmallVec`) for unconstrained $O(1)$ comparisons.
* **End-to-End Encryption (E2EE):** Handled via metadata on `ColumnDef` (`encrypted: bool`). The schema preserves the real underlying `data_type` for client-side decryption, while the coordination server validates that encrypted fields in transit are transmitted strictly as opaque `Value::Bytes` without inspecting payload contents. Primary key columns cannot be encrypted.
* **Primary Key (PK):** Supports both single-column and composite (multi-column) primary keys.
* **Soft Foreign Keys:** Relational references are supported without distributed locking or strict server-side validation. Dangling references are permitted to maintain eventual consistency without coordination bottlenecks.
* **Schema Evolution:** Supports adding new fields over time without breaking backward compatibility.

---

## 4. Mutation Operations & Semantics

All mutations are represented by three elemental operations:

1. **`INSERT`:**
   * Inserts a new tuple with defined field values and local mutation timestamp metadata.
   * **Semantics on existing PK:** If an `INSERT` is performed on an existing primary key, it operates as a full overwrite (upserting all fields).
2. **`UPDATE`:**
   * Granular, field-level modification transmitting only modified fields.
   * **Concurrent Conflict Resolution:** If two clients update distinct fields of the same tuple concurrently, the server performs a **field-level merge**. If both clients modify the exact same field, a *Last-Write-Wins (LWW)* policy applies based strictly on the server arrival order and assigned `SequenceNumber` (see Section 10).
3. **`DELETE`:**
   * Logical deletion via a tombstone marker.

### 4.1. Consolidation & Anti-Zombie Invariants
When mutations are squashed or processed concurrently:
* **Anti-Zombie Rule:** An `UPDATE` operation arriving after a `DELETE` on the same primary key is strictly incompatible and rejected. Partial updates cannot resurrect deleted records.
* **Buffer Purge Rule:** Receiving a `DELETE` for a primary key consolidates and purges prior unacknowledged `INSERT` or `UPDATE` operations for that same PK in the server buffer.

---

## 5. The Coordination Server (`rimdb-server`)

### 5.1. Core Responsibilities
1. **Monotonic Sequencer:** Assigns an incremental, globally ordered sequence number (`SequenceNumber`: 1, 2, 3...) per Room.
2. **Actor-Based Concurrency:** Manages each Room via an isolated Tokio actor, eliminating global lock contention across rooms.
3. **Type & Schema Validation:** Enforces schema integrity, required non-null columns, and primary key type constraints before accepting operations.
4. **Idempotency & Deduplication:** Maintains an in-memory LRU cache of recent `MutationId`s to guarantee *Exactly-Once* semantics and safe network retries.
5. **Consolidation & Compaction Buffer (Squashing):** Merges multiple unacknowledged mutations targeting the same tuple into a single effective operation respecting LWW and anti-zombie rules.
6. **Micro-WAL:** Lightweight append log ensuring safe persistence and recovery of the latest `SequenceNumber` across server restarts.

### 5.2. Log Lifecycle & Lagging Clients
* **Acknowledgments (ACKs):** Each client reports its current read cursor (`last_ack_seq`).
* **Log Truncation:** Once an operation is acknowledged by active clients, it is purged from the server buffer.
* **Compaction Boundary:** If a client remains offline and subsequently requests synchronization from a sequence older than the server's compaction boundary, the server responds with a `BehindCompaction` error, signaling the client to catch up using a base state snapshot rather than individual deltas.

---

## 6. Onboarding & Invitations (State Transfer)

Because the server does not store the full persistent historical state:
1. **Invitation Flow:** An existing active client can invite a new client into the Room.
2. **Snapshot Generation:** The inviting client creates an export (snapshot) of its local database state up to a specific base `SequenceNumber` (e.g., `#500`), applying high-ratio Zstandard compression.
3. **Transfer:** The compressed snapshot is transferred to the new client (via short-lived ephemeral server relay or direct P2P).
4. **Subsequent Catch-up:** Once the snapshot is restored locally, the new client connects to the server and pulls delta changes starting from sequence `#501` onwards.

---

## 7. Client-Server Communication Protocol

To prevent the resource exhaustion of thousands of idle persistent connections:
* **Pull-Based HTTP/2 Binary Protocol:**
  * Transport: HTTP/2 over TLS with binary payloads serialized via `bincode`.
  * Multiplexing: Multiple sync/commit streams share a single underlying TCP connection using explicit correlation identifiers (`CorrelationId`).
  * Defensive Bounding: Codecs enforce an explicit message size limit (16 MB) to prevent denial-of-service memory exhaustion attacks.
* **Core Interaction Contracts:**
  * **Mutation Commit:** Client submits an operation with its `MutationId` and `CorrelationId`. The server responds with the assigned `SequenceNumber` (or the cached sequence if re-submitted).
  * **Synchronization:** Client requests operations starting from its `last_ack_seq` specifying a maximum batch size. The server streams ordered `SequencedOperation` batches with pagination flags (`has_more`).
  * **Acknowledgment:** Client confirms processed sequences, allowing the server to prune its compaction buffer.
  * **Error Handling:** Typed error responses communicate states such as invalid payloads, authorization failures, or `BehindCompaction`.
* **Optional Foreground Streaming:** While a client application is actively in the foreground, it can establish an ephemeral push channel (SSE or short-lived WebSocket) to receive real-time notifications of new commits.

---

## 8. Storage Engine (`rimdb-storage`)

Persisting data locally on clients and managing snapshots is decoupled into a dedicated storage crate implementing a common `StorageEngine` abstraction:
* **Native Structured Storage:**
  * Custom local storage engine built in Rust.
  * Local file-per-room architecture (`room_{id}.rimdb` with magic header `RIM1`).
  * Optimized tabular layout for fast primary key lookups and in-place field updates.
* **Write-Ahead Log (WAL):**
  * Append-only transaction log with per-record CRC32 checksums for crash resilience and recovery.
* **In-Memory Primary Index:**
  * RAM-resident index enabling $O(1)$ point lookups without requiring full disk scans.
* **Block Compression (Zstandard - `zstd`):**
  * Leverages schema-aware data structures with Zstandard block compression to produce minimal snapshot payloads for peer onboarding and backups.
* **Pluggable Architecture:**
  * The `StorageEngine` trait permits interchangeable backends, including in-memory mock engines for fast unit and integration testing.

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
├── ARCHITECTURE.md              # High-level architectural specification and ADRs
├── ROADMAP.md                   # Implementation roadmap, milestone checklists, and backlog
├── Cargo.toml                   # Root workspace manifest with centralized dependencies
├── crates/
│   ├── core/                   # Pure domain models, schemas, operations, Newtypes, protocol (Zero I/O, WASM)
│   ├── storage/                # Tabular storage, WAL with CRC32, RAM index, Zstd snapshots (StorageEngine trait)
│   ├── server/                 # Coordination server, Tokio room actors, LRU dedup, micro-WAL, HTTP/2 API
│   └── client/                 # Local-First client SDK, Outbox queue, optimistic rebase engine, dual transport
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
2. **Mutation `timestamp: u64` (Local Outbox Metadata):** Client-generated timestamp attached to mutations. Used strictly within the client's local Outbox to order uncommitted offline mutations prior to server synchronization.
3. **Domain `Value::Timestamp(i64)` (Application Data):** Structured column data type used by end-user schemas to store business dates and timestamps (e.g., `created_at`, `due_date`).
