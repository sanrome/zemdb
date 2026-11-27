#!/usr/bin/env bash
# Runs Evaluation 1 with the stage probes of diagnostics/write-stages.patch (applied, and the
# server and the generator built with it), and appends to OUT a JSON line per run with the
# generator's results and the histograms of the stages.
#   run_stages.sh CASE OUT
# CASE: tmpfs (files in memory) | disk (fsync, as the server ships) | nofsync (the disk, no
#       fsync: the OS writes back) | nowal (no file written by a commit) | inproc-tmpfs |
#       inproc-nowal (the room actor without HTTP). A suffix `+inline` runs the file operations
#       of a room on the worker (no I/O pool).
# Environment: as run_e1.sh, plus PROBES (1; 0 runs without recording, to measure what the
# probes cost).
set -euo pipefail

CASE=$1
OUT=$2
KS=${KS:-"1 8 64"}
REPS=${REPS:-3}
WARMUP=${WARMUP:-5}
SECS=${SECS:-20}
SERVER_CPUS=${SERVER_CPUS:-0,1}
GEN_CPUS=${GEN_CPUS:-2,3}
PORT=${PORT:-18080}
PROBES=${PROBES:-1}
HERE=$(cd "$(dirname "$0")" && pwd)
SERVER_BIN=${SERVER_BIN:-$(cd "$HERE/../.." && pwd)/target/release/zemdb-server}
PERF_BIN=${PERF_BIN:-$HERE/target/release/zemdb-perf}

BASE=${CASE%+inline}
[ "$BASE" != "$CASE" ] && export ZEMDB_PERF_INLINE_IO=1
case $BASE in
  tmpfs | inproc-tmpfs) ROOT=${TMPFS_ROOT:-/dev/shm/zemdb-e1} ;;
  nowal | inproc-nowal) ROOT=${TMPFS_ROOT:-/dev/shm/zemdb-e1}; export ZEMDB_PERF_NO_WAL=1 ;;
  disk) ROOT=${DISK_ROOT:-/home/user/zemdb-e1-data} ;;
  nofsync) ROOT=${DISK_ROOT:-/home/user/zemdb-e1-data}; export ZEMDB_PERF_NO_FSYNC=1 ;;
  *) echo "unknown case $CASE" >&2; exit 1 ;;
esac

export ZEMDB_AUTH_SECRET=perf_auth_secret_0123456789_0123456789
export ZEMDB_ADMIN_SECRET=perf_admin_secret_0123456789_012345678
export RUST_LOG=warn

for rep in $(seq 1 "$REPS"); do
  for k in $KS; do
    rm -rf "$ROOT" && mkdir -p "$ROOT"
    stages=null
    [ "$PROBES" = 1 ] && export ZEMDB_PERF_STAGES="$ROOT/stages.json"
    if [ "${BASE#inproc-}" != "$BASE" ]; then
      line=$(taskset -c "$SERVER_CPUS" "$PERF_BIN" inproc --data-dir "$ROOT/data" \
        --k "$k" --warmup "$WARMUP" --secs "$SECS")
    else
      ZEMDB_DATA_DIR="$ROOT/data" ZEMDB_PORT=$PORT taskset -c "$SERVER_CPUS" \
        "$SERVER_BIN" > "$ROOT/server.log" 2>&1 &
      pid=$!
      until (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; do sleep 0.1; done
      line=$(taskset -c "$GEN_CPUS" "$PERF_BIN" http --addr 127.0.0.1:"$PORT" \
        --room "e1-k$k-r$rep" --k "$k" --warmup "$WARMUP" --secs "$SECS" --server-pid "$pid")
      if [ "$PROBES" = 1 ]; then
        until [ -s "$ROOT/stages.json" ]; do sleep 0.05; done
      fi
      kill -TERM "$pid" && wait "$pid" || true
    fi
    [ "$PROBES" = 1 ] && stages=$(cat "$ROOT/stages.json")
    echo "{\"case\":\"$CASE\",\"rep\":$rep,\"result\":$line,\"stages\":$stages}" >> "$OUT"
    echo "$CASE k=$k rep=$rep $(jq -c '{cps: .commits_per_sec, mean_us, p99_us, cpu: .server_cpu_pct}' <<< "$line")"
  done
done
rm -rf "$ROOT"
