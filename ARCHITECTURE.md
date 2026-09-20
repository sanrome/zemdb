# Architectural Specification: RimDB (Local-First Distributed Database Engine)

## 1. Overview
The goal of this project is to build a client-centric (**Local-First**) distributed database engine powered by a **minimal, lightweight coordination server**.

* **Primary Storage:** 100% stored locally on clients in a structured, persistent format.
* **Server Role:** Acts as a sequencer, schema/type validator, and ephemeral mutation buffer. The server **does not maintain the long-term historical state** of each database; it only holds unread mutations until clients acknowledge them or a retention threshold expires.
* **Performance Objective:** Ultra-low server memory and CPU footprint, capable of coordinating numerous groups with minimal resource consumption.

---

## 2. Topology & Isolation (Rooms)
The system adopts an isolated **Room** topology:
* Each database is an independent compartment (e.g., a chat group, team workspace, or specific collaborative project).
* A room typically consists of a small-to-medium group of clients (e.g., 2 to 50 participants).
* Operations, schemas, and sequence numbers are strictly scoped to their respective Room.

---

## 3. Data Model & Schema

### 3.1. Structured Typing & Schema Definitions
Every table within a Room has an explicit schema definition with typed fields:
* **Primitive Types:** `Int`, `Float`, `String`, `Bool`, `Bytes`.
* **End-to-End Encryption (E2EE):** Handled via metadata on `ColumnDef` (`encrypted: bool`). The schema preserves the real underlying `data_type` for client-side decryption and deserialization, while the coordination server validates that encrypted fields in transit are transmitted strictly as opaque `Value::Bytes` without inspecting or altering the payload. Primary key columns cannot be encrypted.
* **Primary Key (PK):** Supports both single-column and composite (multi-column) primary keys.
* **Soft Foreign Keys:** Relational references are supported without distributed locking or strict server-side validation. Dangling references (pointing to non-existent or delayed parent tuples) are permitted to maintain eventual consistency without coordination bottlenecks.
* **Schema Evolution:** Supports adding new fields over time without breaking backward compatibility.

---

## 4. Mutation Operations & Semantics

All mutations are represented by three elemental operations:

1. **`INSERT`:**
   * Inserts a new tuple with defined field values.
   * **Semantics on existing PK:** If an `INSERT` is performed on an existing primary key, it operates as a full overwrite (upserting all fields).
2. **`UPDATE`:**
   * Granular, field-level modification.
   * Only transmits modified fields, avoiding the overhead of transferring the entire tuple.
   * **Concurrent Conflict Resolution:** If two clients update distinct fields of the same tuple concurrently, the server performs a **field-level merge**. If both clients modify the exact same field, a *Last-Write-Wins (LWW)* policy applies based on the server arrival order and logical timestamp.
3. **`DELETE`:**
   * Logical deletion via a tombstone marker.
   * **Buffer Purge Rule:** Receiving a `DELETE` for a primary key immediately purges and discards any prior unacknowledged `INSERT` or `UPDATE` operations for that same PK in the server buffer.

---

## 5. The Coordination Server

### 5.1. Core Responsibilities
1. **Monotonic Sequencer:** Assigns an incremental, globally ordered sequence number per Room (`sequence_id`: 1, 2, 3...).
2. **Type & Schema Validation:** Enforces schema integrity by ensuring all incoming operations adhere to expected column types and primary key requirements before acceptance.
3. **Consolidation & Compaction Buffer (Squashing):**
   * Operates an in-memory/ephemeral disk buffer per Room.
   * If a single tuple receives multiple mutations before all clients read them, the server **squashes/merges** them (e.g., an `INSERT` followed by two `UPDATE`s are consolidated into a single effective operation).
   * A `DELETE` removes earlier pending operations for that tuple.

### 5.2. Log Lifecycle & Inactive Clients
* **Acknowledgments (ACKs):** Each client reports its current read cursor (`last_ack_seq`).
* **Log Truncation:** Once an operation is acknowledged by **all active clients**, it is purged from the server.
* **Retention Policies & Inactive Clients:**
   * If a client stops reporting activity or reading past a configurable time threshold (TTL), it transitions to an `INACTIVE` state.
   * The server stops waiting for inactive clients when determining log truncation, preventing buffer bloat.
   * If a client stays offline beyond a maximum threshold, it is deregistered and must rejoin from scratch.

---

## 6. Onboarding & Invitations (State Transfer)

Because the server does not store the full persistent state:
1. **Invitation Flow:** An existing active client can invite a new client into the Room.
2. **Snapshot Generation:** The inviting client creates an export (snapshot) of its local database state up to a specific base `sequence_id` (e.g., `#500`), applying high-ratio compression.
3. **Transfer:** The compressed snapshot is transferred to the new client (via short-lived ephemeral server relay or direct P2P).
4. **Subsequent Catch-up:** Once the snapshot is restored locally, the new client connects to the server and pulls delta changes starting from sequence `#501` onwards.

---

## 7. Client-Server Communication Protocol

To prevent the overhead and connection resource exhaustion of thousands of idle persistent connections (e.g., long-lived WebSockets):
* **Pull-Based HTTP/2 Binary Protocol:**
  * Transport: HTTP/2 over TLS with binary payloads (`Content-Type: application/octet-stream`) serialized via high-performance binary encoding (`bincode`).
  * Multiplexing: Multiple sync/commit streams can share a single underlying TCP connection without extra socket overhead.
  * Extensibility: Architecture allows seamless future upgrade to HTTP/3 (QUIC) without altering the application layer or database logic.
  * **Endpoints:**
    * **Synchronization:** `POST /rooms/{room_id}/sync`
      * Request: `SyncRequest { client_id, last_ack_seq }`
      * Response: `SyncResponse { current_seq, operations: Vec<CompactOperation> }`
    * **Mutation Commit:** `POST /rooms/{room_id}/commit`
      * Request: `CommitRequest { client_id, operation: Operation }`
      * Response: `CommitResponse { assigned_seq }`
* **Optional Foreground Streaming:** While a client application is actively in the foreground and focused, it can optionally establish an ephemeral streaming channel (Server-Sent Events or short-lived WebSocket) to receive real-time push notifications of new commits. The connection is severed when the app is minimized or suspended.

---

## 8. Client-Side Persistence Engine

* **Native Structured Storage:**
  * Custom local storage engine built in Rust.
  * Local file-per-room architecture.
  * Optimized tabular layout for fast primary key lookups and in-place field updates.
* **Block Compression (Zstandard - `zstd`):**
  * Leverages schema-aware data structures with Zstandard block compression to produce minimal snapshot payloads for peer onboarding and backups.

---

## 9. Technology Stack & Workspace Structure

### 9.1. Language: Rust
* Zero-cost abstractions and deterministic memory management without garbage collection pauses.
* High-performance asynchronous networking (`tokio`, `axum`).
* Fast binary serialization (`bincode` / `postcard`).
* Universal compilation: native binaries for desktop/mobile and **WebAssembly (WASM)** targets for browsers.

### 9.2. Workspace Layout
```text
rimdb/
├── ARCHITECTURE.md              # Project specification and design document
├── Cargo.toml                   # Root workspace manifest
├── crates/
│   ├── core/                   # Shared data types, Schemas, Operations, Serialization
│   ├── server/                 # Sequencer, Compaction buffer, HTTP API
│   └── client/                 # Local storage engine, Sync engine & replication logic
```
