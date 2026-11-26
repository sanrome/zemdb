#!/bin/sh
# Builds what the runner needs (README.md): the steps in wasm32, bound with wasm-bindgen into
# www/pkg, and the test server. First it checks that the version of wasm-bindgen is the same in
# the Cargo.lock of the harness, in the one of the root and in the CLI.
set -eu
cd "$(dirname "$0")"

lock_version() {
    awk '$0 == "name = \"wasm-bindgen\"" { getline; gsub(/version = |"/, ""); print }' "$1"
}
harness=$(lock_version Cargo.lock)
root=$(lock_version ../../Cargo.lock)
cli=$(wasm-bindgen --version | cut -d ' ' -f 2)
if [ "$harness" != "$root" ] || [ "$harness" != "$cli" ]; then
    echo "wasm-bindgen differs: $harness in the harness, $root in the root, $cli in the CLI" >&2
    exit 1
fi

cargo build -p zemdb-browser-steps --target wasm32-unknown-unknown
wasm-bindgen --target web --no-typescript --out-dir www/pkg \
    target/wasm32-unknown-unknown/debug/zemdb_browser_steps.wasm
cargo build -p zemdb-browser-server
