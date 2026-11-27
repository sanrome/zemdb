# The browser harness

The tests of the SDK in a browser (DC44 of the [plan of Fase 4.5](../../docs/proposals/2026-11-fase4_5-navegador.md)): what depends on the OPFS, `fetch`, the Web Locks, WebCrypto, `postMessage` and the life cycle of a page. It is a Cargo workspace of its own, outside the workspace of the root (its `exclude`), with its own `Cargo.lock` and `package.json`. The `browser` job of the CI runs it, in the Chromium and the Firefox of Playwright, which are builds of its own.

## The pieces

- **`steps/`**, the steps crate, compiled to wasm32 with `wasm-bindgen`. It depends on the crates of the root by path (`zemdb-core`, and `zemdb-storage` for its file layer over the OPFS and its disk engine).
  - Each exported `async` function is a *step*: it takes the id of the run, and fails with an error that says what failed.
  - A panic fails its step with the message of the panic. After it the instance is unusable, and the run ends there.
  - The unit tests (`src/tests/`) reach what a step does not. They run in a dedicated Worker, with `wasm-bindgen-test-runner`.
- **`server/`**, the test server. It serves one origin on `127.0.0.1` over HTTP/1.1: a secure context, without CORS. It has:
  - the router of `zemdb-server` (`build_router`) over a temporary data directory, built as the simulation builds it;
  - the files of `www/`;
  - the routes of the harness:
    - `POST /harness/admin/...`: the Control Plane request `/admin/...`, with the admin secret;
    - `POST /harness/token`: a client token for `{"client_id", "room_id", "ttl_secs"}`;
    - `POST /harness/network`: `{"up": false}` cuts the network between the page and the router, and `{"up": true}` restores it. While it is down, a request to the router gets its connection closed without an answer, and a response in progress (an event stream) is cut: the page sees a network error. The page and the routes of the harness are still served, so the page can reload offline;
    - `POST /harness/snapshot`: the snapshot of a room of a native `DiskStorageEngine`, compressed with `zstd`, with the rows of `{"rows": n}` of the schema of the suite `engine` (a table `users` with an integer key and a string, the rows `"user {key}"`);
    - `POST /harness/snapshot/check?head=<n>`: applies the snapshot of the body, made in the browser, to a native `MemoryStorageEngine` at the head `n`, and answers `{same} of {rows}`: how many of the rows it holds are the rows up to `n` as the browser wrote them, and how many rows it holds, so a row of more counts too.

  It prints `listening on http://127.0.0.1:<port>/`. It stops when its standard input closes, or on Ctrl+C: it closes the connections, shuts its rooms down and deletes its directory. So it does not run with `nohup`, nor in the background of a shell, where its input is closed or it stops when it reads it.

  It exits with an error if a connection panicked: a panic of the router reaches the page only as a closed connection, like a cut of the network.

  It serves the router with its own loop, as the simulation does, without what `serve_until_shutdown` adds:
  - the limits of the connections;
  - the timeouts of the headers and of a stalled response;
  - HTTP/2 without TLS;
  - the reservation of a response held until its body ends (here hyper drops it with the headers);
  - the sweeper of expired snapshots.
- **`www/`**, the page. It runs, in order, the actions of the suites that its URL names (`suites.js`). An action can:
  - run a step in its dedicated Worker (`worker.js`);
  - reload the page;
  - run a step in a second page of the same context, which it opens (`peer.html`);
  - terminate its Worker;
  - close the second page.

  It keeps its progress in `sessionStorage` across its reloads, stops at the first step that fails, and leaves the result in the page. Each load of a page names its Workers with a random id, by which a step knows whether it runs in a page that reloaded, or in the second page.
- **`run.mjs`**, the runner, with Playwright:
  - it starts the test server on a free port;
  - it opens the page in a browser without a window, and runs the default suites;
  - it runs the suite `isolation` in another context of the browser, with the same run id, which must not see the OPFS of the first;
  - it runs the suite `rooms` in another context, so that its steps, which measure, have the time of a page of their own;
  - in Chromium, it runs the suite `quota` in a fourth context, whose quota it makes small; `--suites quota` runs it alone, with its quota, and the runner refuses it with other suites or in another browser;
  - it prints as a note of the run a console message of a step that starts with `harness note: `;
  - it fails if:
    - a step fails (it names the step);
    - a step ran in a second page without the context opening one;
    - a page took another run id than the one of its URL;
    - the server does not stop in time and with success.

  `--self-check` checks the runner itself. A run of each of the suites `fails`, `fails_in_peer` and `panics` has to fail, naming its step and giving its error. Their steps fail, in the first page and in the second, or panic, on purpose.

## The suites

The default ones, in this order:
- **`reload`:** what a step writes in the OPFS is there after the page reloads.
- **`tabs`:** a second page of the same context sees the same OPFS and cannot open the file that the first one has open; once the first one terminates its Worker, it opens it.
- **`server`:** the routes of the test server: a schema and a room created through the Control Plane, the registration of a client with a token of the harness, and the cut of the network.
- **`files`:** the directory of the file layer of the storage over the OPFS (`zemdb_storage::files::OpfsFiles`), on the real layer. The steps make the files with the browser, under `harness/files-{run}/`, as a cut leaves them, and check the entries that each operation leaves by their names, generations included (the name, and then the name with `~1`, `~2`...):
  - `files_write`: each operation (`create_dir_all`, `exists`, `list`, `rename` to a free name, onto one that exists and to another directory, `remove`, `sync_dir`); then the generations that a cut leaves, made by the step;
  - `files_after_reload`, after the page reloads: what the operations left, and each name holding its newest generation, which a rename moves, removing the old ones of its source; a removal removes every generation of exactly its name, never of one that starts with it;
  - `files_taken`: with a handle of the browser open on the source, on an old generation of it, or on the newest, the rename and the removal fail with the error of a taken file (`FileTaken`), and no value changes; with the destination open, the rename succeeds, and its old generation stays;
  - `files_refused`: an absolute path, one with `..`, one with a name with `~` or `\`, the path of a file that ends in `/` or `.`, and a listing for a prefix with `~` are refused, and nothing in the OPFS changes;
  - `files_dropped`: a rename dropped once it started runs to its end, and the next operation waits for it, also for the removals that come after its move; a removal dropped before its turn never runs;
  - `files_errors`: what is missing is `NotFound`, with the name of the error of the browser when it says so, and a rename to a missing directory touches nothing; a removal skips a generation that something else removed in the meantime.
- **`handles`:** the files of the same layer (`OpfsFile`), on the real layer, under `harness/files-{run}/open-*`. Each step fails if it does not end within 20 s, so an operation that hangs fails it there:
  - `handles_write`: reading and writing at an offset, the size, truncating and extending, `sync_data` and `sync_all`; a read past the end is `UnexpectedEof`, never a short read; `create` empties a file, and creates one that does not exist; once closed, the browser finds what the layer wrote, there and nowhere else; and a write that ends at 2^53, the end of the offsets of the browser, fails, whatever count the browser gives, and writes nothing (it notes the error it saw: the count in Chromium 153, `StorageFull` in a headless shell of Chromium 157, and 0 bytes written, `WriteZero`, in Firefox 155), while one past it is refused; a read of no bytes past the end gives none, and one of `usize::MAX` bytes is `UnexpectedEof`;
  - `handles_rename`: renaming an open file, as the rotation of a WAL and its rollback do: the handle follows it to a free name, back, onto a name that exists (to its next generation, removing the old ones), to another directory and within it, from a name with old generations, and from one with a generation newer than the one open, which goes too; a rename whose first step fails (an old generation of the source open), to a missing directory or to a refused path fails with the file still open where it was;
  - `handles_after_reload`, after the page reloads: what the two steps before it left, read through the openings;
  - `handles_generations`: each opening opens the newest generation of its name; `create` empties it, and `create_locked` is `AlreadyExists` with any generation and truncates nothing; what is missing is `NotFound`, and a refused path touches nothing;
  - `handles_hold`, `handles_release` and, in the second page, `handles_taken` and `handles_free`: with a file open through the layer in the first page (and with a handle of the browser), `open_locked` is `None`, `create_locked` is `AlreadyExists`, and `open_read` and `create` are the error of a taken file, also from the Worker of the second page; once the first page closes it, the second one opens it, with what the first one wrote;
  - `handles_dropped`: an opening dropped once it started ends, and closes its file; the rename of an open file dropped once it started ends, and the next operation on the file waits for it, or gives its error, and does not run, if it failed (also once it had sent its error); and a file dropped while its rename is in flight is closed once the rename ends.
- **`engine`:** the disk engine of the storage (`zemdb_storage::DiskStorageEngine`) on the file layer over the OPFS, the real engine on the real layer, under `harness/engine-{run}/`, each scenario in a data directory of its own. Each step fails if it does not end within 20 s, and a step that opens a room after a Worker ended retries for 5 s while its files are still taken (`RoomLocked`, or the error of a taken file):
  - `engine_write` and, after the page reloads with the room open, `engine_after_reload`: a room that applies 1000 rows in batches, with its automatic compactions, which nobody awaits (one has to have written the snapshot), reopens with every row and goes on; a reload of its schema to one that does not evolve from it is refused, and one to a schema that does is taken, which the room reopens with;
  - `engine_batches_in_flight`, `engine_compactions_in_flight`, `engine_snapshots_in_flight` and `engine_deletes_in_flight`, each followed by the termination of the Worker and its `_cut` step: the step leaves a loop of batches of 50 rows, of a batch and a compaction, of snapshot installations (of a memory engine, one row ahead each), or of cycles that write, compact, close and delete the room, running in the Worker, which records in a file of the harness, outside the data directory, how far it got (8 bytes, written at once and flushed, after each step the engine acknowledged). It returns once the loop ran three steps and then for a random part of the time those took, so the cut falls at a random point of a later one, never before the loop started. The loop does not end while the Worker lives: a step of it that fails ends it, and then it records so (`u64::MAX`), and the step that started it fails if it sees it, and so does the `_cut` step that reads the record, so the cut never falls after its end unseen. The `_cut` step, in a new Worker, reopens the room and checks it against the record, whatever the point of the cut: every acknowledged row and at most the batch in flight, whole; the last snapshot installed or the one in flight; a room whose deletion was cut, with all the rows of its cycle, the snapshot alone or nothing, and one cut once its cycle deleted it, at a prefix of the log of the next cycle (each cycle writes rows of its own). The recovery removes what the cut left (the room's directory holds its lock, its snapshot and its WAL, and nothing else), and the room goes on. Each notes where the cut fell (`harness note:`), with the entries it left. In Chromium a terminated Worker ends at the end of its current task, so the cut falls between two turns of its event loop: between two promises of an operation, never inside the synchronous part of a batch (its write, its flush and its application), so the loop of batches gives the event loop a turn before it records each acknowledgement;
  - `engine_snapshots` and, after the page reloads with the rooms open, `engine_snapshots_after_reload`: `apply_snapshot` of a snapshot of the native engine of the test server (with `zstd`) into a new room, and of one that the browser makes of it (with `ruzstd`), which the native engine of the test server reads with the same rows, into another; and of a native one ahead of a room with rows. After the reload they reopen with their snapshots, and go on;
  - `engine_hold` and `engine_release`, in the first page, and `engine_locked` and `engine_free`, in the second: with a room open in the first page, opening it and deleting it from the Worker of the second page is `RoomLocked`, and changes no entry of its directory; once the first page closes it, the second one opens it, with what the first one wrote;
  - `engine_dropped`: `open_room`, `compact_room`, `apply_snapshot` and `delete_room`, each dropped once it started, run to their end in their tasks: another open finds the room open, closing waits for the compaction and the installation, which the room reopens with, and an open after the dropped deletion finds the room empty.
- **`rooms`**, which the runner runs in a context of its own: `engine_rooms_10`, `engine_rooms_30` and `engine_rooms_60` open that many rooms one after another in a directory of their own, close them and open them again with another engine, and note how long each pass took (`harness note:`): each operation of the layer lists the directory, with the three entries of each room. A pass stops opening once it took 20 s, and the note says how far it got, so that a slow browser gives its number instead of failing the step.
- **`quota`**, which only the runner runs, in Chromium, always with its quota: a write over the quota of the origin, which the runner makes small through CDP (`Storage.overrideQuotaForOrigin`) in a context of its own, fails, and the file goes on. The step notes how the browser said it: with a `QuotaExceededError`, which is `StorageFull` (the headless shell of Chromium 157 of some environments), or, in Chromium 153, the one of Playwright 1.63, with a count of bytes written larger than what it was given (4294967288), which the layer refuses with an error that has the count. In both, the write left part of its bytes in the file. Firefox has no CDP in Playwright, so there it does not run, and the quota in Firefox is not tested.

The runner runs `isolation`, `rooms` and `quota` on their own, and `fails`, `fails_in_peer` and `panics` in its self-check.

## Running it

It needs:
- Rust with the `wasm32-unknown-unknown` target;
- the CLI of `wasm-bindgen` of the version in `Cargo.lock`, which `build.sh` checks is also the one in the lock of the root;
- Node 22.

```sh
cargo install wasm-bindgen-cli --locked --version 0.2.129
sh build.sh                     # the steps in www/pkg, and the test server
npm ci
npx playwright install chromium firefox
node run.mjs                    # --suites a,b, --headed; --browser firefox|webkit (Chromium without it)
node run.mjs --executable /path/to/chrome   # another build of the browser than Playwright's
node run.mjs --self-check
node run.mjs --browser firefox
node run.mjs --browser firefox --self-check
cargo test -p zemdb-browser-server
```

Playwright 1.63 installs Chromium 153.0.8010.12, the one of the CI. Some environments give Playwright another build instead, such as a headless shell of Chromium 157, which does not behave the same (the suite `quota`): `--executable` runs the runner, and its self-check, with a given executable of the browser.

By hand, in any browser: start `target/debug/zemdb-browser-server --www www` and open the URL it prints. The page runs the default suites and shows the result. The second page is a pop-up, so the browser has to allow them for the origin. The suite `isolation` needs another context of the browser, and only the runner runs it.

The unit tests in wasm32 need a WebDriver of the browser. In the CI it is the chromedriver of the runner:

```sh
CHROMEDRIVER=/path/to/chromedriver cargo test -p zemdb-browser-steps --target wasm32-unknown-unknown
```

`CHROMEDRIVER_REMOTE=http://127.0.0.1:<port>` uses a chromedriver that is already running. A `steps/webdriver.json` adds capabilities to the browser.

The unit tests in wasm32 of the storage (its layer over the OPFS: the errors as the layer makes them, a reopening that fails, also in a rotation of the disk engine, a move that fails, a file taken between the listing of `create_locked` and its opening; and its task, its randomness, the levels of `zstd_level` in wasm32, the `.snap` that `ruzstd` compresses, the automatic compaction in a task that nobody awaits, and a staged snapshot that the recovery removes) run the same way, from the root of the repository, whose `.cargo/config.toml` names `wasm-bindgen-test-runner` as the runner of wasm32 (natively nothing changes). The CI runs them in the `browser` job, which has the CLI:

```sh
CHROMEDRIVER=/path/to/chromedriver cargo test --target wasm32-unknown-unknown -p zemdb-storage --lib
```

The native unit tests of the storage are not built in wasm32 (they use Tokio's runtime, `zstd` and the native file layer), nor are its integration tests, so in wasm32 its clippy takes the unit tests alone: `cargo clippy -p zemdb-storage --target wasm32-unknown-unknown --lib --profile test`.

## Adding steps

A step is an exported `async fn name(run: String) -> Result<(), JsError>` of the steps crate. It goes in a suite of `www/suites.js`. A suite does not reload while its second page is open: the page would lose it. What a lot of 4.5 tests in the browser goes here as its steps (DC44). What does not depend on the browser goes in the usual suite.
