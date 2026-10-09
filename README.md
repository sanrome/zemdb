# ZemDB ⚡🦀

**ZemDB** is a high-performance, low-overhead **Local-First** distributed database engine written 100% in idiomatic Rust under a strict `#![forbid(unsafe_code)]` policy.

It is designed for small-to-medium collaborative groups (2 to 50 clients per room) with 100% primary storage local to each node and an ultra-lightweight coordination server acting as a **monotonic total-order sequencer** and ephemeral mutation buffer.

---

## 📌 Project Status

| Phase | Status | Key Capabilities |
| :--- | :---: | :--- |
| **Phase 1: Domain & Core** | ✅ **Completed** | Pure Zero I/O domain models, Newtypes, positional `CompactRow` tuples, 24-byte `Value`, 40-byte `PrimaryKey` (L1 cache-line aligned), schema validations, and size-bounded binary protocol. |
| **Phase 2: Storage Engine** | ✅ **Completed** | Asynchronous `StorageEngine` contract, multi-threaded in-memory room isolation, native on-disk Dual-File format (`.snap` + `.wal`), CRC32 checksums, and Zstandard-compressed snapshots. |
| **Phase 2.5 & 2.8: Hardening & Core/Storage Contracts** | ✅ **Completed** | Linear $O(M+N)$ column delta merge, POSIX directory sync (`sync_dir`), physical batch framing in core (`0xBA7C`), Dual-File architecture with in-place WAL truncation, atomic flock pre-rename, non-blocking chunked cursor scans, 1-RTT Commit & Catch-Up protocol, external integration test suites (60 tests passing). |
| **Phase 3: Coordination Server** | ✅ **Completed** | Tokio room actor model (`RoomActor` with bounded channels), single atomic WAL with Exactly-Once idempotency, 4-tier immutable log (Hot RAM -> Warm Disk -> Cold Disk), and Axum HTTP/2 binary endpoints with signal-only SSE. |
| **Phase 3.5: Hardening, Resiliencia y Contratos** | ✅ **Completed** | Framing de wire protocol (`RM`, ver `0x01`), snapshots universales `ZMSN` con CRC32, concurrencia CoW sin bloqueos, Snapshot Isolation, relay multipart con BLAKE3, consultas directas `get_by_id`/`scan_by_id`, validación en almacenamiento, LWW estricto, encapsulación de modelos centrales y desregistro binario `DeregisterAck`. |
| **Phase 4: Client SDK & 1-RTT Sync** | ⏳ **Planned** | Ergonomic public facade (`ZemdbClient` -> `RoomHandle` -> `TableHandle`), server-authoritative 1-RTT commit-and-sync, strictly canonical storage via `zemdb-storage`, dual transport (native Reqwest / browser WebFetch), and real-time live queries (`watch`). Offline draft queue deferred to post-v0.1. |
| **Phase 5: E2E Verification & Universality** | ⏳ **Planned** | End-to-end integration test suites with network partition simulation, stress testing, and WebAssembly compilation (`wasm32-unknown-unknown`). |

---

## 🏗️ Workspace Layout

The repository is organized as a Cargo workspace with clean boundary separation and zero circular dependencies:

```text
client-distributed-db/
├── crates/
│   ├── core/           # [zemdb-core] Pure algebraic domain, schemas, types, squashing, and protocol (Zero I/O, WASM)
│   ├── storage/        # [zemdb-storage] Tabular storage engine, ZEM1 on-disk format, WAL, and Zstd snapshots
│   ├── server/         # [zemdb-server] Coordination server, Tokio room actors, micro-WAL, and HTTP/2 API (Phase 3)
│   └── client/         # [zemdb-client] Local-First client SDK, reactive synchronization, and dual transport (Phase 4)
│
├── ARCHITECTURE.md     # Living architectural specification: topology, models, protocol, and ADRs
├── ROADMAP.md          # Agile development board: active phases, detailed task checklists, and backlog
├── README.md           # Project entry point and navigation guide
│
└── docs/               # Historical archives and deep technical proposals
    ├── audits/         # Multidisciplinary specialist audit reports (e.g. 2026-09-fase2-audit.md)
    └── proposals/      # Detailed architectural proposals and RFCs (e.g. 2026-09-fase2_5-hardening-proposal.md)
```

---

## 🚀 Quickstart & Verification

### Prerequisites
* Rust 1.78+ (2021 Edition)
* Cargo

### Running the Server
The server requires two private secrets of at least 32 bytes each, different from each other. It refuses to start without them:
```bash
export ZEMDB_AUTH_SECRET="$(openssl rand -hex 32)"
export ZEMDB_ADMIN_SECRET="$(openssl rand -hex 32)"
cargo run -p zemdb-server
```
They can also be set in a TOML file passed as the first argument (`auth_secret`, `admin_secret`).

The server defaults of the room lifecycle policy (log retention, client lease timeouts, shutdown of inactive rooms) come from `ZEMDB_*` environment variables or the same TOML file; see [`ARCHITECTURE.md` §5.3](ARCHITECTURE.md#53-configurable-lifecycle-policy-roomlifecyclepolicy) for the variables and their accepted ranges. Numeric `ZEMDB_*` variables are parsed strictly: a value that is not a valid number for the setting, or is out of its range, makes the server refuse to start with an error naming the variable (an empty value counts as unset).

### Building and Testing
```bash
# Build entire workspace
cargo build --workspace

# Run all unit, integration, and contract tests (100+ tests passing)
cargo test --workspace

# Verify clippy lints with zero warnings allowed
cargo clippy --workspace --all-targets -- -D warnings
```

---

## 📚 Documentation Map

Quick navigation depending on your goal:

* **System Design & Architectural Decisions:**  
  👉 See **[`ARCHITECTURE.md`](ARCHITECTURE.md)** for room topology, server total-order authority, on-disk file formats, and Architectural Decision Records (ADRs).

* **Current Implementation Progress & Checklists:**  
  👉 See **[`ROADMAP.md`](ROADMAP.md)** for milestone progress, Phase 3 and Phase 4 actionable task checklists, and deferred backlog items.

* **Technical Audits & In-Depth Proposals:**  
  👉 Explore the **[`docs/`](docs/)** directory:
  - Multidisciplinary Specialist Audit: [`docs/audits/2026-09-fase2-audit.md`](docs/audits/2026-09-fase2-audit.md)
  - Unified Technical Hardening Proposal: [`docs/proposals/2026-09-fase2_5-hardening-proposal.md`](docs/proposals/2026-09-fase2_5-hardening-proposal.md)
