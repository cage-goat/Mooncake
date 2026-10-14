#!/usr/bin/env bash
# Real SPDK stale-I/O + controller recovery (TCP by default; RDMA optional).
# Only manages its own children. Uses the host's existing hugepage setup.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
SPDK_DIR=${SPDK_DIR:-"$REPO_ROOT/extern/spdk"}
LOG_DIR=${LOG_DIR:-$(mktemp -d /tmp/mooncake_nof_stale_e2e.XXXXXX)}
MODE=${MODE:-resume}
CYCLES=${CYCLES:-3}
WORKERS=${WORKERS:-2}
POOLS=${POOLS:-2}
IO_TIMEOUT_MS=${IO_TIMEOUT_MS:-1000}
TIMEOUT_SEC=${TIMEOUT_SEC:-20}
TARGET_TRANSPORT=${TARGET_TRANSPORT:-TCP}
TARGET_HOST=${TARGET_HOST:-127.0.0.1}
TARGET_PORT=${TARGET_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)}
TARGET_NQN=${TARGET_NQN:-nqn.2016-06.io.spdk:mooncake-stale-$$}
TARGET_CORE_MASK=${TARGET_CORE_MASK:-0x4}
TARGET_PID=""
CLIENT_PID=""
RPC_DIR=""
CLIENT_BIN="$BUILD_DIR/mooncake-store/tests/e2e/nof_stale_io_recovery_e2e"
source "$SCRIPT_DIR/nof_target.sh"

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -n "$CLIENT_PID" ]]; then
    kill -KILL -- "-$CLIENT_PID" 2>/dev/null || true
    wait "$CLIENT_PID" 2>/dev/null || true
  fi
  nof_target_stop
  [[ -z "$RPC_DIR" ]] || rmdir "$RPC_DIR"
  if (( rc != 0 )); then
    echo "FAILED mode=$MODE logs=$LOG_DIR" >&2
    tail -n 60 "$LOG_DIR/client.log" >&2 || true
  fi
  exit "$rc"
}

case "$MODE" in
  resume|restart) ;;
  disabled) IO_TIMEOUT_MS=0 ;;
  short) IO_TIMEOUT_MS=3000 ;;
  shutdown|pool_exit|shutdown_recovery|pool_exit_recovery) CYCLES=1 ;;
  *) echo "Invalid MODE: $MODE" >&2; exit 2 ;;
esac
[[ "$CYCLES" =~ ^[1-9][0-9]*$ && "$TIMEOUT_SEC" =~ ^[1-9][0-9]*$ &&
   "$IO_TIMEOUT_MS" =~ ^[0-9]+$ && "$WORKERS" =~ ^[12]$ && "$POOLS" =~ ^[12]$ ]] || exit 2
[[ -x "$CLIENT_BIN" && -x "$SPDK_DIR/build/bin/nvmf_tgt" ]] || {
  echo "Build nof_stale_io_recovery_e2e (USE_NOF=ON) and SPDK nvmf_tgt" >&2
  exit 2
}
mkdir -p "$LOG_DIR"
[[ -z "$(ls -A "$LOG_DIR")" ]] || { echo "LOG_DIR must be empty" >&2; exit 2; }
LOG_DIR=$(cd -- "$LOG_DIR" && pwd)
RPC_DIR=$(mktemp -d /tmp/nof-stale-rpc.XXXXXX)
RPC_SOCKET="$RPC_DIR/spdk.sock"
trap cleanup EXIT
export LD_LIBRARY_PATH="$BUILD_DIR/mooncake-common${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

start_target() {
  nof_target_start "$LOG_DIR/target-$1.log" || return
  for ns in 1 2 3 4; do
    local block_size=512
    (( ns % 2 == 0 )) && block_size=4096
    nof_target_add_malloc_ns "$ns" 32 "$block_size" || return
  done
  nof_target_start_listener
}

wait_for_pattern() {
  local pattern=$1 deadline=$((SECONDS + TIMEOUT_SEC))
  until grep -q "$pattern" "$LOG_DIR/client.log"; do
    kill -0 "$CLIENT_PID" 2>/dev/null && (( SECONDS < deadline )) || return 1
    sleep 0.05
  done
}

require_paused() {
  grep -q '^State:.*T' "/proc/$TARGET_PID/status"
}

start_target 0
ENDPOINT="trtype:$TARGET_TRANSPORT adrfam:IPv4 traddr:$TARGET_HOST trsvcid:$TARGET_PORT subnqn:$TARGET_NQN"
MC_NVME_FABRICS_CONNECT_TIMEOUT_US=500000 MC_NOF_SUBMIT_CHUNK_BYTES=65536 \
MC_NOF_INFLIGHT_BYTES_LIMIT=131072 MC_NOF_IO_TIMEOUT_MS="$IO_TIMEOUT_MS" \
MC_NOF_DEBUG=1 MC_NOF_DEBUG_INTERVAL_MS=100 \
  timeout --kill-after=5 "$((TIMEOUT_SEC * (CYCLES * 4 + 2)))" "$CLIENT_BIN" \
  --endpoint="$ENDPOINT" --control_dir="$LOG_DIR" --mode="$MODE" \
  --cycles="$CYCLES" --workers="$WORKERS" --pools="$POOLS" --timeout_sec="$TIMEOUT_SEC" \
  >"$LOG_DIR/client.log" 2>&1 &
CLIENT_PID=$!
wait_for_pattern '^PASS initial_io$'

for ((cycle=1; cycle<=CYCLES; ++cycle)); do
  wait_for_pattern "^READY cycle=$cycle$"
  kill -STOP "$TARGET_PID"
  deadline=$((SECONDS + TIMEOUT_SEC))
  until require_paused; do
    (( SECONDS < deadline )) || exit 1
    sleep 0.01
  done
  touch "$LOG_DIR/paused-$cycle"
  wait_for_pattern "^READY pending cycle=$cycle$"
  if [[ "$MODE" == disabled || "$MODE" == short ]]; then
    # Disabled: remain paused beyond the enabled tests' timeout. Short:
    # return before its configured 3 s timeout, without causing recovery.
    sleep 1.5
    require_paused
    touch "$LOG_DIR/check-pending-$cycle"
    wait_for_pattern "^PASS still_pending cycle=$cycle$"
    kill -CONT "$TARGET_PID"
    touch "$LOG_DIR/resumed-$cycle"
  else
    wait_for_pattern "^PASS drained cycle=$cycle "
    require_paused
    if [[ "$MODE" == shutdown ]]; then
      break
    fi
    if [[ "$MODE" == shutdown_recovery || "$MODE" == pool_exit_recovery ]]; then
      wait_for_pattern 'nof_recovery retry '
      touch "$LOG_DIR/exit-recovery-$cycle"
      if [[ "$MODE" == shutdown_recovery ]]; then
        break
      fi
      wait_for_pattern "^PASS pool_exited cycle=$cycle$"
    fi
    grep -q 'nof_io_timeout ' "$LOG_DIR/client.log"
    grep -q 'nof_recovery begin ' "$LOG_DIR/client.log"
    if [[ "$MODE" == restart ]]; then
      nof_target_stop
      start_target "$cycle"
    else
      kill -CONT "$TARGET_PID"
    fi
    touch "$LOG_DIR/resumed-$cycle"
  fi
  wait_for_pattern "^PASS recovered cycle=$cycle$"
done
wait_for_pattern "^PASS stale_io_recovery mode=$MODE$"
deadline=$((SECONDS + TIMEOUT_SEC))
while kill -0 "$CLIENT_PID" 2>/dev/null; do
  (( SECONDS < deadline )) || { echo "Client shutdown timed out" >&2; exit 1; }
  sleep 0.05
done
wait "$CLIENT_PID"
CLIENT_PID=""
if [[ "$MODE" == disabled || "$MODE" == short ]]; then
  if grep -Eq 'nof_io_timeout |nof_recovery begin ' "$LOG_DIR/client.log"; then
    echo "Unexpected recovery with MODE=$MODE" >&2
    exit 1
  fi
fi
if grep -Eq 'outstanding io < 0|inflight blocks < 0|Assertion.*failed|AddressSanitizer|runtime error:' "$LOG_DIR/client.log"; then
  echo "Accounting/assertion/sanitizer failure" >&2
  exit 1
fi
python3 "$SCRIPT_DIR/validate_nof_stale_io_log.py" "$LOG_DIR/client.log" "$MODE" "$CYCLES" "$POOLS" "$IO_TIMEOUT_MS"
echo "PASSED mode=$MODE cycles=$CYCLES pools=$POOLS workers=$WORKERS logs=$LOG_DIR"
