#!/usr/bin/env bash
# Runs one configuration of Evaluation 2 against a fresh server and appends a JSON line per run.
#   run_e2.sh tmpfs|disk OUT LABEL [zemdb-perf rooms arguments...]
# Environment: REPS (1), SERVER_CPUS (0,1), GEN_CPUS (2,3), PORT (18090), TMPFS_ROOT, DISK_ROOT,
# ZEMDB_PERF_INLINE_IO (passed to the server when set: the diagnostic switch of Evaluation 1).
set -euo pipefail
KIND=$1; OUT=$2; LABEL=$3; shift 3
REPS=${REPS:-1}
SERVER_CPUS=${SERVER_CPUS:-0,1}
GEN_CPUS=${GEN_CPUS:-2,3}
PORT=${PORT:-18090}
BIN=$(cd "$(dirname "$0")/../.." && pwd)/target/release
case $KIND in
  tmpfs) ROOT=${TMPFS_ROOT:-/dev/shm/zemdb-e2} ;;
  disk) ROOT=${DISK_ROOT:-/home/user/zemdb-e2-data} ;;
  *) echo "unknown kind $KIND" >&2; exit 1 ;;
esac
export ZEMDB_AUTH_SECRET=perf_auth_secret_0123456789_0123456789
export ZEMDB_ADMIN_SECRET=perf_admin_secret_0123456789_012345678
export RUST_LOG=warn
[ -n "${ZEMDB_PERF_INLINE_IO:-}" ] && export ZEMDB_PERF_INLINE_IO
for rep in $(seq 1 "$REPS"); do
  rm -rf "$ROOT" && mkdir -p "$ROOT"
  ZEMDB_DATA_DIR="$ROOT/data" ZEMDB_PORT=$PORT ZEMDB_MAX_CONNECTIONS=19500 ZEMDB_MAX_SSE_STREAMS=19000 \
    taskset -c "$SERVER_CPUS" "$BIN/zemdb-server" > "$ROOT/server.log" 2>&1 &
  pid=$!
  until (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; do sleep 0.1; done
  line=$(taskset -c "$GEN_CPUS" "$BIN/zemdb-perf" rooms --addr 127.0.0.1:"$PORT" --server-pid "$pid" "$@")
  kill -TERM "$pid" && wait "$pid" || true
  warnings=$(grep -c WARN "$ROOT/server.log" || true)
  echo "{\"label\":\"$LABEL\",\"kind\":\"$KIND\",\"rep\":$rep,\"server_warnings\":$warnings,\"result\":$line}" | tee -a "$OUT"
done
rm -rf "$ROOT"
