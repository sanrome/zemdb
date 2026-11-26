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
- **`files`:** the directory of the file layer of the storage over the OPFS (`zemdb_storage::files::OpfsFiles`), on the real layer. The layer opens no file yet, so the steps make the files with the browser, under `harness/files-{run}/`, and check the entries that each operation leaves by their names, generations included (the name, and then the name with `~1`, `~2`...):
  - `files_write`: each operation (`create_dir_all`, `exists`, `list`, `rename` to a free name and onto one that exists, `remove`, `sync_dir`); then the generations that a cut leaves, made by the step;
  - `files_after_reload`, after the page reloads: what the operations left, and each name holding its newest generation, which a rename moves, removing the old ones of its source; a removal removes every generation of exactly its name, never of one that starts with it;
  - `files_taken`: with a handle of the browser open on the source, on an old generation of it, or on the newest, the rename and the removal fail with the error of a taken file (`FileTaken`), and no value changes; with the destination open, the rename succeeds, and its old generation stays;
  - `files_refused`: an absolute path, one with `..` and one with `~` are refused, and nothing in the OPFS changes;
  - `files_dropped`: a rename dropped once it started runs to its end, and the next operation waits for it; a removal dropped before its turn never runs;
  - `files_errors`: what is missing is `NotFound`, with the name of the error of the browser when it says so.

The runner runs `isolation` on its own, and `fails`, `fails_in_peer` and `panics` in its self-check.

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

## Adding steps

A step is an exported `async fn name(run: String) -> Result<(), JsError>` of the steps crate. It goes in a suite of `www/suites.js`. A suite does not reload while its second page is open: the page would lose it. What a lot of 4.5 tests in the browser goes here as its steps (DC44). What does not depend on the browser goes in the usual suite.
