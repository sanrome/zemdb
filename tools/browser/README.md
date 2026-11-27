# The browser harness

The tests of the SDK in a browser (DC44 of the [plan of Fase 4.5](../../docs/proposals/2026-11-fase4_5-navegador.md)): what depends on the OPFS, `fetch`, the Web Locks, WebCrypto, `postMessage` and the life cycle of a page. It is a Cargo workspace of its own, outside the workspace of the root (its `exclude`), with its own `Cargo.lock` and `package.json`. The `browser` job of the CI runs it, in the Chromium and the Firefox of Playwright, which are builds of its own.

## The pieces

- **`steps/`**, the steps crate, compiled to wasm32 with `wasm-bindgen`. It depends on the crates of the root by path (`zemdb-core`, and `zemdb-storage` for its file layer over the OPFS).
  - Each exported `async` function is a *step*: it takes the id of the run, and fails with an error that says what failed.
  - A panic fails its step with the message of the panic. After it the instance is unusable, and the run ends there.
  - The unit tests (`src/tests/`) reach what a step does not. They run in a dedicated Worker, with `wasm-bindgen-test-runner`.
- **`server/`**, the test server. It serves one origin on `127.0.0.1` over HTTP/1.1: a secure context, without CORS. It has:
  - the router of `zemdb-server` (`build_router`) over a temporary data directory, built as the simulation builds it;
  - the files of `www/`;
  - the routes of the harness:
    - `POST /harness/admin/...`: the Control Plane request `/admin/...`, with the admin secret;
    - `POST /harness/token`: a client token for `{"client_id", "room_id", "ttl_secs"}`;
    - `POST /harness/network`: `{"up": false}` cuts the network between the page and the router, and `{"up": true}` restores it. While it is down, a request to the router gets its connection closed without an answer, and a response in progress (an event stream) is cut: the page sees a network error. The page and the routes of the harness are still served, so the page can reload offline.

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
  - in Chromium, it runs the suite `quota` in a third context, whose quota it makes small;
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
  - `handles_write`: reading and writing at an offset, the size, truncating and extending, `sync_data` and `sync_all`; a read past the end is `UnexpectedEof`, never a short read; `create` empties a file; and once closed, the browser finds what the layer wrote, there and nowhere else;
  - `handles_rename`: renaming an open file, as the rotation of a WAL and its rollback do: the handle follows it to a free name, back, onto a name that exists (to its next generation, removing the old ones), to another directory, and from a name with old generations; a rename whose first step fails (an old generation of the source open), to a missing directory or to a refused path fails with the file still open where it was;
  - `handles_after_reload`, after the page reloads: what the two steps before it left, read through the openings;
  - `handles_generations`: each opening opens the newest generation of its name; `create` empties it, and `create_locked` is `AlreadyExists` with any generation and truncates nothing; what is missing is `NotFound`, and a refused path touches nothing;
  - `handles_hold`, `handles_release` and, in the second page, `handles_taken` and `handles_free`: with a file open through the layer in the first page (and with a handle of the browser), `open_locked` is `None`, `create_locked` is `AlreadyExists`, and `open_read` and `create` are the error of a taken file, also from the Worker of the second page; once the first page closes it, the second one opens it, with what the first one wrote;
  - `handles_dropped`: an opening dropped once it started ends, and closes its file; the rename of an open file dropped once it started ends, and the next operation on the file waits for it, or gives its error, and does not run, if it failed (also once it had sent its error); and a file dropped while its rename is in flight is closed once the rename ends.
- **`quota`**, which only the runner runs, in Chromium: a write over the quota of the origin, which the runner makes small through CDP (`Storage.overrideQuotaForOrigin`) in a context of its own, is `StorageFull`, from the `QuotaExceededError` of the browser. Firefox has no CDP in Playwright, so there it does not run.

The runner runs `isolation` and `quota` on their own, and `fails`, `fails_in_peer` and `panics` in its self-check.

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
node run.mjs --self-check
node run.mjs --browser firefox
node run.mjs --browser firefox --self-check
cargo test -p zemdb-browser-server
```

By hand, in any browser: start `target/debug/zemdb-browser-server --www www` and open the URL it prints. The page runs the default suites and shows the result. The second page is a pop-up, so the browser has to allow them for the origin. The suite `isolation` needs another context of the browser, and only the runner runs it.

The unit tests in wasm32 need a WebDriver of the browser. In the CI it is the chromedriver of the runner:

```sh
CHROMEDRIVER=/path/to/chromedriver cargo test -p zemdb-browser-steps --target wasm32-unknown-unknown
```

`CHROMEDRIVER_REMOTE=http://127.0.0.1:<port>` uses a chromedriver that is already running. A `steps/webdriver.json` adds capabilities to the browser.

The unit tests in wasm32 of the storage (its layer over the OPFS: the errors as the layer makes them, a reopening that fails, a move that fails, a file taken between the listing of `create_locked` and its opening) run the same way, from the root of the repository, whose `.cargo/config.toml` names `wasm-bindgen-test-runner` as the runner of wasm32 (natively nothing changes). The CI runs them in the `browser` job, which has the CLI:

```sh
CHROMEDRIVER=/path/to/chromedriver cargo test --target wasm32-unknown-unknown -p zemdb-storage --lib
```

The native unit tests of the storage are not built in wasm32 (they use Tokio's runtime, `zstd`, the disk engine and the fail points), nor are its integration tests, so in wasm32 its clippy takes the unit tests alone: `cargo clippy -p zemdb-storage --target wasm32-unknown-unknown --lib --profile test`.

## Adding steps

A step is an exported `async fn name(run: String) -> Result<(), JsError>` of the steps crate. It goes in a suite of `www/suites.js`. A suite does not reload while its second page is open: the page would lose it. What a lot of 4.5 tests in the browser goes here as its steps (DC44). What does not depend on the browser goes in the usual suite.
