#!/usr/bin/env bash
# Save one log directory per topology/fault; reuse the standard target helper.
set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
LOG_ROOT=${LOG_ROOT:-$(mktemp -d /tmp/mooncake_nof_stale_matrix.XXXXXX)}
mkdir -p "$LOG_ROOT"
for spec in 'resume 1 1 3' 'resume 1 2 3' 'resume 2 2 3' \
            'restart 2 2 3' 'disabled 2 2 1' 'short 2 2 1' \
            'shutdown 2 2 1' 'pool_exit 2 2 1' \
            'shutdown_recovery 2 2 1' 'pool_exit_recovery 2 2 1'; do
  read -r mode pools workers cycles <<< "$spec"
  name="$mode-p$pools-w$workers"
  MODE="$mode" POOLS="$pools" WORKERS="$workers" CYCLES="$cycles" \
    LOG_DIR="$LOG_ROOT/$name" bash "$SCRIPT_DIR/run_nof_stalled_io_tcp_e2e.sh" \
    >"$LOG_ROOT/$name.log" 2>&1
  tail -n 1 "$LOG_ROOT/$name.log"
done
echo "PASSED stale-I/O/recovery matrix: $LOG_ROOT"
