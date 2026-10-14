"""Check real fault/recovery evidence in a completed NoF E2E client log."""

import re
import sys
from pathlib import Path


def validate(path, mode, cycles, pools, timeout_ms):
    text = Path(path).read_text()
    lines = text.splitlines()
    begins = [i for i, line in enumerate(lines) if "nof_recovery begin " in line]
    ready = [i for i, line in enumerate(lines) if "nof_recovery ready " in line]
    timeouts = [i for i, line in enumerate(lines) if "nof_io_timeout " in line]
    pool_exits = mode in ("pool_exit", "pool_exit_recovery")
    should_recover = mode in ("resume", "restart") or pool_exits
    if should_recover:
        assert len(begins) == len(ready) == cycles, (begins, ready)
        assert len(timeouts) >= cycles, timeouts
        for cycle in range(1, cycles + 1):
            pending = next(
                i
                for i, line in enumerate(lines)
                if line == f"READY pending cycle={cycle}"
            )
            drained = next(
                i
                for i, line in enumerate(lines)
                if line.startswith(f"PASS drained cycle={cycle} ")
            )
            recovered = next(
                i
                for i, line in enumerate(lines)
                if line == f"PASS recovered cycle={cycle}"
            )
            assert pending < begins[cycle - 1] < drained < ready[cycle - 1] < recovered
            assert any(pending < i < drained for i in timeouts)
            elapsed = int(re.search(r"elapsed_ms=(\d+)", lines[drained])[1])
            assert timeout_ms // 2 <= elapsed <= timeout_ms + 5000, elapsed
            connects = sum(
                "nof_qpair_async_connect_start " in line
                for line in lines[ready[cycle - 1] : recovered]
            )
            assert connects == (2 if pool_exits else 4), connects
    elif mode == "shutdown":
        # Sequential pool destruction can outlast the timeout on another pool
        # (notably during asynchronous RDMA drain). Recovery may coalesce once.
        assert len(begins) <= 1 and not ready, (begins, ready)
        assert bool(begins) == bool(timeouts), (begins, timeouts)
    elif mode == "shutdown_recovery":
        assert len(begins) == 1 and not ready and timeouts, (begins, ready, timeouts)
    else:
        assert not begins and not timeouts, (begins, timeouts)

    # Final debug snapshots show that both QoS lists and worker accounting
    # returned to zero. Exited pools are checked by their cleanup assertions.
    if not mode.startswith("shutdown"):
        active = {}
        last = {}
        for line in lines:
            if line.startswith("TEST namespace "):
                fields = dict(re.findall(r"(\w+)=(\w+)", line))
                if not pool_exits or fields["pool_idx"] != "0":
                    active[fields["seg_handle"]] = fields["nsid"]
            if "nof_qos_state " in line:
                fields = dict(re.findall(r"(\w+)=(\w+)", line))
                last[fields["seg_handle"]] = fields
        assert len(active) == (2 if pool_exits else 4), active
        for seg in active:
            fields = last[seg]
            for key in (
                "inflight_read",
                "inflight_write",
                "queued_read",
                "queued_write",
                "total_outstanding_io",
            ):
                assert fields[key] == "0", (seg, fields)
    print(f"PASS log_evidence mode={mode} cycles={cycles} pools={pools}")


if __name__ == "__main__":
    validate(
        sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
    )
