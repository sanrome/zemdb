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
| **Phase 3: Coordination Server** | ✅ **Completed** | Tokio room actor model (`RoomActor` with bounded channels), single atomic WAL with Exactly-Once idempotency, 4-tier immutable log (Hot RAM -> Warm Disk -> Cold Disk), and Axum binary HTTP endpoints (HTTP/1.1 and HTTP/2 without TLS, behind a TLS-terminating reverse proxy) with signal-only SSE. |
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
│   ├── server/         # [zemdb-server] Coordination server, Tokio room actors, micro-WAL, and HTTP API (Phase 3)
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

### Deploying the Server
The server speaks HTTP/1.1 and HTTP/2 in clear text (h2c) and does not terminate TLS: put it behind a reverse proxy that does. **Without a TLS proxy in front, client tokens and the admin secret travel in clear text.** A minimal [Caddy](https://caddyserver.com) configuration, with automatic TLS certificates and HTTP/2 towards the clients:
```text
db.example.com {
    reverse_proxy 127.0.0.1:8080 {
        transport http {
            keepalive 5s
        }
    }
}
```
The proxy's idle timeout for the connections it reuses (`keepalive`) must be shorter than the server's header timeout (`ZEMDB_HEADER_READ_TIMEOUT_SECS`, 10 s): otherwise the proxy can send a request on a connection the server is closing and answer 502. Keep the server bound to `127.0.0.1` (the default `ZEMDB_HOST`) or to a private network, so that it is reachable only through the proxy. Registration (`/register`) needs no `Authorization` header, so limit connections or requests per client IP at the proxy (for example with a rate-limiting module) or the firewall.

The HTTP edge limits apply to every connection (see [`ARCHITECTURE.md` §7.3](ARCHITECTURE.md#73-http-edge-transport-and-limits)):

| Variable | Default | Accepted | |
|---|---|---|---|
| `ZEMDB_HEADER_READ_TIMEOUT_SECS` | 10 | 1 – 300 | Time to deliver the headers of each request; slower or idle connections are closed. |
| `ZEMDB_BODY_READ_TIMEOUT_SECS` | 60 | 1 – 3,600 | Base time for a request body to arrive in full, extended as data arrives; otherwise HTTP 408 (`RequestTimeout`, retryable). SSE streams are not limited. |
| `ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC` | 32,768 | 1,024 – 1,073,741,824 | Minimum body rate: every this many bytes received extend the body's deadline by one second, so a body that keeps at least this rate is never cut (a 16 MiB body may take up to 60 s + 512 s with the defaults), while a trickle is cut close to the base time. |
| `ZEMDB_MAX_CONNECTIONS` | 10,000 | 1 – 1,000,000 | Connections served at once; at the limit new connections wait until one closes. An HTTP/2 connection carries at most 100 requests at once. |

Request bodies are limited to 16 MiB, and registration (`/register`, unauthenticated) to 64 KiB; a larger body is answered with HTTP 413.

Each connection takes a file descriptor. Raise the open files limit of the server process above `ZEMDB_MAX_CONNECTIONS` (it is often 1,024 by default on Linux), for example with `ulimit -n 65536` or `LimitNOFILE=65536` in a systemd unit.

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
