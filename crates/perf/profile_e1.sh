#!/usr/bin/env bash
# Records a CPU profile of the server (or of the in-process run) during the measured window.
#   profile_e1.sh http|inproc K OUT_PREFIX [DATA_ROOT]
set -euo pipefail
MODE=$1; K=$2; OUT=$3; ROOT=${4:-/dev/shm/zemdb-prof}
PERF=${PERF:-/usr/lib/linux-tools/6.8.0-147-generic/perf}
BIN=$(cd "$(dirname "$0")/../.." && pwd)/target/release
export ZEMDB_AUTH_SECRET=perf_auth_secret_0123456789_0123456789
export ZEMDB_ADMIN_SECRET=perf_admin_secret_0123456789_012345678
export RUST_LOG=warn
rm -rf "$ROOT" && mkdir -p "$ROOT"
if [ "$MODE" = inproc ]; then
  taskset -c 0,1 "$BIN/zemdb-perf" inproc --data-dir "$ROOT/data" --k "$K" --warmup 3 --secs 15 > "$OUT.json" &
  gen=$!; sleep 0.5; pid=$gen
else
  ZEMDB_DATA_DIR="$ROOT/data" ZEMDB_PORT=18082 taskset -c 0,1 "$BIN/zemdb-server" > "$ROOT/server.log" 2>&1 &
  pid=$!
  until (exec 3<>/dev/tcp/127.0.0.1/18082) 2>/dev/null; do sleep 0.1; done
  taskset -c 2,3 "$BIN/zemdb-perf" http --addr 127.0.0.1:18082 --room prof --k "$K" --warmup 3 --secs 15 --server-pid "$pid" > "$OUT.json" &
  gen=$!
fi
sleep 4
taskset -c 2,3 "$PERF" record -q -F 499 --call-graph dwarf,16384 -p "$pid" -o "$OUT.data" -- sleep 8 2>/dev/null
wait "$gen"
[ "$MODE" = inproc ] || { kill -TERM "$pid"; wait "$pid" || true; }
rm -rf "$ROOT"
