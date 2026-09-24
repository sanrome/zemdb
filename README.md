# RimDB ⚡🦀

**RimDB** is a high-performance, low-overhead **Local-First** distributed database engine written 100% in idiomatic Rust under a strict `#![forbid(unsafe_code)]` policy.

It is designed for small-to-medium collaborative groups (2 to 50 clients per room) with 100% primary storage local to each node and an ultra-lightweight coordination server acting as a **monotonic total-order sequencer** and ephemeral mutation buffer.

---

## 📌 Project Status

| Phase | Status | Key Capabilities |
| :--- | :---: | :--- |
| **Phase 1: Domain & Core** | ✅ **Completed** | Pure Zero I/O domain models, Newtypes, positional `CompactRow` tuples, 24-byte `Value`, 40-byte `PrimaryKey` (L1 cache-line aligned), schema validations, and size-bounded binary protocol. |
| **Phase 2: Storage Engine** | ✅ **Completed** | Asynchronous `StorageEngine` contract, multi-threaded in-memory room isolation, native on-disk format (`room_{id}.rimdb`) with append-only WAL, CRC32 checksums, and Zstandard-compressed snapshots. |
| **Phase 2.5: Hardening & Modularization** | ✅ **Completed** | Linear $O(M+N)$ column delta merge, POSIX directory sync (`sync_dir`), atomic batch framing in WAL (`0xBA7C`), multi-process file locking (`flock`), true lazy streaming in `scan()`, external integration test suites (57 tests passing). |
| **Phase 3: Coordination Server** | 🚀 **In Development** | Tokio room actor model (`RoomActor` with bounded channels), durable on-disk Micro-WAL for Exactly-Once idempotency, compaction buffer in RAM, and Axum HTTP/2 binary endpoints with signal-only SSE. |
| **Phase 4: Client SDK & 1-RTT Sync** | ⏳ **Planned** | Ergonomic public facade (`RimdbClient` -> `RoomHandle` -> `TableHandle`), server-authoritative 1-RTT commit-and-sync, strictly canonical storage via `rimdb-storage`, dual transport (native Reqwest / browser WebFetch), and real-time live queries (`watch`). Offline draft queue deferred to post-v0.1. |
| **Phase 5: E2E Verification & Universality** | ⏳ **Planned** | End-to-end integration test suites with network partition simulation, stress testing, and WebAssembly compilation (`wasm32-unknown-unknown`). |

---

## 🏗️ Workspace Layout

The repository is organized as a Cargo workspace with clean boundary separation and zero circular dependencies:

```text
client-distributed-db/
├── crates/
│   ├── core/           # [rimdb-core] Pure algebraic domain, schemas, types, squashing, and protocol (Zero I/O, WASM)
│   ├── storage/        # [rimdb-storage] Tabular storage engine, RIM1 on-disk format, WAL, and Zstd snapshots
│   ├── server/         # [rimdb-server] Coordination server, Tokio room actors, micro-WAL, and HTTP/2 API (Phase 3)
│   └── client/         # [rimdb-client] Local-First client SDK, reactive synchronization, and dual transport (Phase 4)
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

### Building and Testing
```bash
# Build entire workspace
cargo build --workspace

# Run all unit, integration, and contract tests (57 tests)
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
