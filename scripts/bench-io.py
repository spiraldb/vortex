#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors
"""Compare scan I/O and warmed compressed-segment caches using query wall time.

Build datafusion-bench from the current sources first. Historical trace fields remain
supported, while configurations for rejected backend prototypes have been removed. Example:
  python3 scripts/bench-io.py --data-root /path/to/vortex-bench/data --output /tmp/io-run

Runs TPC-H Q19 SF10 and partitioned ClickBench Q23 by default. Measurements are
serial, with rotated configuration order. Two initial iterations per process are
excluded from timing. Diagnostic processes are separate from timing processes;
read-range coverage and exact scan counters describe their final iteration.
Completion traces can include cancelled background reads; discrepancies against
scan counters are saved explicitly, and overlap is a completion-window diagnostic.
Use --driver-trace to save V2 state-machine reports and Perfetto timelines. Tracing
is enabled only in diagnostic processes. Load averages and timing spreads are saved
to make contention on shared machines visible; --note records known conditions.

The cached mode preloads all segments while visiting every table file before
query timing, then keeps Moka caches across opens, capped PER FILE.
It retains compressed bytes, not decoded arrays or query results. The adapter
control uses the same segment-source routing with NoOpSegmentCache. Direct mode
leaves the branch's default I/O routing unchanged. Cache hits are compute-only
only when the measured scan reports zero reads AND zero misses. The OS page cache
is left warm unless --evict-local-files is selected. Cold runs use one iteration
per fresh process and verify file-page residency after eviction. Scan byte counts
measure application reads; Linux child resource counters also record storage bytes
for the whole process, including setup and readahead.
"""

from __future__ import annotations

import argparse
import collections
import ctypes
import hashlib
import json
import mmap
import os
import re
import resource
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

VARIANTS = {
    "v1": {},
    "v2": {"VORTEX_SCAN_V2": "1"},
    "v2-unlimited": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
    },
    "v2-previous": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
    },
    "v2-previous-demanded": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "32",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
    },
    "v2-previous-boxed": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_INLINE_FETCH": "0",
        "VORTEX_SCAN_GROUP_SPARSE_PROJECTION": "0",
        "VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION": "0",
    },
    "v2-ready-fetch": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "1",
    },
    "v2-io-prune-interests": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION": "1",
    },
    "v2-io-4m": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_COALESCE_MAX_BYTES": "4194304",
    },
    "v2-io-read-slots-1": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_READ_CONCURRENCY": "1",
    },
    "v2-io-read-slots-4": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_READ_CONCURRENCY": "4",
    },
    "v2-io-file-handle": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
    },
    "v2-io-file-handle-limited": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
    },
    "v2-io-file-handle-no-announcements": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_EARLY_ANNOUNCE": "0",
    },
    "v2-io-file-handle-limited-no-announcements": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_EARLY_ANNOUNCE": "0",
    },
    "v2-io-file-handle-read-slots-4": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_IO_READ_CONCURRENCY": "4",
    },
    "v2-io-file-handle-random": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_LOCAL_READ_RANDOM": "1",
    },
    "v2-io-demanded-coalescing": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
    },
    "v2-io-file-handle-demanded-coalescing": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
    },
    **{
        f"v2-io-file-handle-demanded-gap-{distance}": {
            "VORTEX_SCAN_V2": "1",
            "VORTEX_LOCAL_READ_CONCURRENCY": "32",
            "VORTEX_SCAN_IO_READY_FETCH": "0",
            "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
            "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
            "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES": str(distance),
        }
        for distance in (0, 4096, 65536, 262144)
    },
    **{
        f"v2-io-file-handle-demanded-window-{maximum}-gap-{distance}": {
            "VORTEX_SCAN_V2": "1",
            "VORTEX_LOCAL_READ_CONCURRENCY": "32",
            "VORTEX_SCAN_IO_READY_FETCH": "0",
            "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
            "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
            "VORTEX_SCAN_IO_COALESCE_MAX_BYTES": str(maximum),
            "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES": str(distance),
        }
        for maximum in (4 * 1024 * 1024, 16 * 1024 * 1024)
        for distance in (64 * 1024, 1024 * 1024)
    },
    "v2-io-file-handle-limited-demanded-coalescing": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
    },
    "v2-io-file-handle-demanded-no-announcements": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "32",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
        "VORTEX_SCAN_EARLY_ANNOUNCE": "0",
    },
    **{
        f"v2-io-file-handle-demanded-native-{limit}": {
            "VORTEX_SCAN_V2": "1",
            "VORTEX_LOCAL_READ_CONCURRENCY": str(limit),
            "VORTEX_SCAN_IO_READY_FETCH": "0",
            "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
            "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
        }
        for limit in (8, 16, 32, 64, 128)
    },
    **{
        f"v2-io-file-handle-demanded-driver-{limit}": {
            "VORTEX_SCAN_V2": "1",
            "VORTEX_LOCAL_READ_CONCURRENCY": "32",
            "VORTEX_SCAN_IO_READY_FETCH": "0",
            "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
            "VORTEX_SCAN_IO_COALESCE_OPTIONAL": "0",
            "VORTEX_SCAN_IO_READ_CONCURRENCY": str(limit),
        }
        for limit in (1, 2, 4, 8, 16, 32, 64)
    },
    "v2-io-file-handle-limited-random": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_LOCAL_READ_RANDOM": "1",
    },
    "v2-io-file-handle-limited-no-announcements-random": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_EARLY_ANNOUNCE": "0",
        "VORTEX_LOCAL_READ_RANDOM": "1",
    },
    "v2-io-file-handle-tight-gap": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_LOCAL_FILE_HANDLE_REUSE": "1",
        "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES": "65536",
    },
    "v2-io-tight-gap": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES": "65536",
    },
    "v2-io-4m-tight-gap": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_IO_COALESCE_MAX_BYTES": "4194304",
        "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES": "65536",
    },
    "v2-file-pruning": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_FILE_PRUNING": "1",
    },
    "v2-project-early": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_PROJECT_ANNOUNCE": "1",
        "VORTEX_SCAN_IO_INLINE_FETCH": "0",
    },
    "v2-project-late": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_PROJECT_ANNOUNCE": "0",
        "VORTEX_SCAN_IO_INLINE_FETCH": "0",
    },
    "v2-io-project-late": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_PROJECT_ANNOUNCE": "0",
    },
    "v2-io-project-late-read-slots-1": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_PROJECT_ANNOUNCE": "0",
        "VORTEX_SCAN_IO_READ_CONCURRENCY": "1",
    },
    "v2-io-no-announcements": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_EARLY_ANNOUNCE": "0",
    },
    "v2-io-project-late-tight-gap": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_READY_FETCH": "0",
        "VORTEX_SCAN_PROJECT_ANNOUNCE": "0",
        "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES": "65536",
    },
    "v2-inline-fetch": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_PROJECT_ANNOUNCE": "1",
        "VORTEX_SCAN_IO_INLINE_FETCH": "1",
    },
    "v2-baseline": {
        "VORTEX_SCAN_V2": "1",
        "VORTEX_LOCAL_READ_CONCURRENCY": "0",
        "VORTEX_SCAN_IO_LOOKAHEAD": "1",
    },
    "v2-lookahead": {"VORTEX_SCAN_V2": "1", "VORTEX_SCAN_IO_LOOKAHEAD": "1"},
    "late": {"VORTEX_SCAN_V2": "1", "VORTEX_SCAN_EARLY_ANNOUNCE": "0"},
}
# Clear archived experiments so saved executables cannot enable them through ambient env.
TOGGLES = (
    "VORTEX_SCAN_V2",
    "VORTEX_SCAN_EARLY_ANNOUNCE",
    "VORTEX_SCAN_PROJECT_ANNOUNCE",
    "VORTEX_SCAN_IO_INLINE_FETCH",
    "VORTEX_SCAN_IO_READY_FETCH",
    "VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION",
    "VORTEX_SCAN_GROUP_SPARSE_PROJECTION",
    "VORTEX_SCAN_FILE_PRUNING",
    "VORTEX_SCAN_SPLIT_CONCURRENCY",
    "VORTEX_ONPAIR_COMPRESSED_LIKE",
    "VORTEX_SCAN_EXEC",
    "VORTEX_SCAN_BATCH_IO",
    "VORTEX_SCAN_IO_COMPLETION_BATCH",
    "VORTEX_SCAN_IO_PREFETCH_MAX_BYTES",
    "VORTEX_LOCAL_READ_CONCURRENCY",
    "VORTEX_LOCAL_FILE_HANDLE_REUSE",
    "VORTEX_LOCAL_READ_RANGES_DIRECT",
    "VORTEX_LOCAL_READ_RESERVED_SMALL",
    "VORTEX_LOCAL_READ_SMALL_BYTES",
    "VORTEX_LOCAL_READ_SMALL_PRIORITY",
    "VORTEX_LOCAL_READ_NOWAIT_MAX_BYTES",
    "VORTEX_LOCAL_READ_ALLOCATE_IN_BLOCKING",
    "VORTEX_LOCAL_READ_RANDOM",
    "VORTEX_SCAN_IO_LOOKAHEAD",
    "VORTEX_SCAN_IO_READ_CONCURRENCY",
    "VORTEX_SCAN_IO_COALESCE_MAX_BYTES",
    "VORTEX_SCAN_IO_COALESCE_OPTIONAL",
    "VORTEX_SCAN_IO_COALESCE_COST_BYTES",
    "VORTEX_SCAN_IO_COALESCE_USE_PERCENT",
    "VORTEX_SCAN_IO_COALESCE_SPECULATIVE_BYTES",
    "VORTEX_SCAN_IO_COALESCE_DISTANCE_BYTES",
    "VORTEX_BENCH_SEGMENT_CACHE_MB",
    "VORTEX_USE_SCAN_API",
    "VORTEX_BENCH_PRELOAD_SEGMENTS",
    "RUST_LOG",
)
WORKLOADS = {
    **{f"tpch-q{query}": ("tpch", query) for query in range(1, 23)},
    **{f"clickbench-q{query}": ("clickbench", query) for query in range(43)},
}
DEFAULT_WORKLOADS = ["tpch-q19", "clickbench-q23"]
ANSI = re.compile(r"\x1b\[[0-9;]*m")
COORDINATOR_SAMPLES: dict[int, tuple[int, float, float]] = {}
COORDINATOR_LOCK = threading.Lock()


def cpu_snapshot() -> list[int] | None:
    """Read aggregate Linux CPU counters, including host steal time when available."""
    stat = Path("/proc/stat")
    if not stat.exists():
        return None
    return [int(value) for value in stat.read_text().splitlines()[0].split()[1:9]]


def cpu_usage(before: list[int] | None, after: list[int] | None) -> dict[str, float] | None:
    if before is None or after is None:
        return None
    delta = [b - a for a, b in zip(before, after, strict=True)]
    total = sum(delta)
    if total <= 0:
        return None
    return {
        "busy_fraction": 1 - (delta[3] + delta[4]) / total,
        "iowait_fraction": delta[4] / total,
        "steal_fraction": delta[7] / total,
    }


def pressure_snapshot() -> dict[str, str]:
    return {
        name: path.read_text().strip()
        for name in ("cpu", "io", "memory")
        if (path := Path("/proc/pressure") / name).exists()
    }


def vm_snapshot() -> dict[str, int]:
    """Record host reclaim and compaction events that can delay cold-read allocations."""
    path = Path("/proc/vmstat")
    if not path.exists():
        return {}
    return {
        name: int(value)
        for line in path.read_text().splitlines()
        for name, value in [line.split()]
        if name.startswith(("allocstall_", "pgscan_", "pgsteal_", "compact_", "thp_fault_"))
        or name in ("pgfault", "pgmajfault", "workingset_refault_file")
    }


def counter_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int] | None:
    """Preserve unavailable/reset counters instead of describing them as zero events."""
    if not before or before.keys() != after.keys():
        return None
    delta = {name: after[name] - value for name, value in before.items()}
    return delta if all(value >= 0 for value in delta.values()) else None


def pressure_totals(snapshot: dict[str, str]) -> dict[str, int]:
    """Extract cumulative microseconds; decayed PSI averages are not window measurements."""
    return {
        f"{resource}/{line.split()[0]}": int(match[1])
        for resource, contents in snapshot.items()
        for line in contents.splitlines()
        if (match := re.search(r"\btotal=(\d+)", line)) is not None
    }


def page_cache_residency(files: list[Path]) -> dict[str, int | float]:
    """Inspect Linux file-page residency without faulting in file contents."""
    if not sys.platform.startswith("linux"):
        raise ValueError("File-page residency checks require Linux mincore")
    library = ctypes.CDLL(None, use_errno=True)
    mincore = library.mincore
    mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte)]
    mincore.restype = ctypes.c_int
    page_size = os.sysconf("SC_PAGE_SIZE")
    total_pages = resident_pages = total_bytes = 0
    for path in files:
        with path.open("rb") as file:
            size = os.fstat(file.fileno()).st_size
            total_bytes += size
            if size == 0:
                continue
            pages = (size + page_size - 1) // page_size
            residency = (ctypes.c_ubyte * pages)()
            # A private writable mapping permits taking its address without touching the
            # contents or changing the file. mincore itself does not fault in those pages.
            with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_COPY) as mapping:
                address = ctypes.addressof(ctypes.c_char.from_buffer(mapping))
                if mincore(address, size, residency) != 0:
                    error = ctypes.get_errno()
                    raise OSError(error, os.strerror(error), str(path))
            total_pages += pages
            resident_pages += sum(value & 1 for value in residency)
    return {
        "files": len(files),
        "file_bytes": total_bytes,
        "page_size": page_size,
        "total_pages": total_pages,
        "resident_pages": resident_pages,
        "resident_fraction": resident_pages / total_pages if total_pages else 0.0,
    }


def evict_local_files(data: Path) -> dict[str, Any]:
    """Evict this dataset's Vortex pages and verify the advisory request worked."""
    if not hasattr(os, "posix_fadvise"):
        raise ValueError("Local eviction requires posix_fadvise")
    files = sorted(data.rglob("*.vortex"))
    if not files:
        raise ValueError(f"No Vortex files to evict under {data}")
    before = page_cache_residency(files)
    for path in files:
        with path.open("rb") as file:
            os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    after = page_cache_residency(files)
    if after["resident_fraction"] > 0.001:
        raise ValueError(f"Eviction left more than 0.1% of dataset pages resident: {after}")
    return {"before": before, "after": after}


def storage_snapshot(data: Path) -> dict[str, Any] | None:
    """Snapshot the dataset's Linux block device, including other host activity."""
    if not sys.platform.startswith("linux"):
        return None
    device_id = data.stat().st_dev
    try:
        device = Path(f"/sys/dev/block/{os.major(device_id)}:{os.minor(device_id)}").resolve(strict=True)
        if (device / "partition").exists():
            device = device.parent
        counters = [int(value) for value in (device / "stat").read_text().split()]
    except (FileNotFoundError, PermissionError):
        return None
    if len(counters) < 11:
        return None
    return {"device": device.name, "ts_ns": time.monotonic_ns(), "counters": counters}


def storage_delta(before: dict[str, Any] | None, after: dict[str, Any] | None) -> dict[str, Any] | None:
    """Summarize host-device IO over the process window; detect counter resets."""
    if before is None or after is None or before["device"] != after["device"]:
        return None
    duration_ms = (after["ts_ns"] - before["ts_ns"]) / 1e6
    delta = [b - a for a, b in zip(before["counters"][:11], after["counters"][:11], strict=True)]
    # Field 9 is a current IO gauge and can decrease; all other fields are cumulative.
    if duration_ms <= 0 or any(value < 0 for index, value in enumerate(delta) if index != 8):
        return None
    return {
        "device": before["device"],
        "window_ms": duration_ms,
        "read_operations": delta[0],
        "read_bytes": delta[2] * 512,
        "write_operations": delta[4],
        "write_bytes": delta[6] * 512,
        "read_MB_s": delta[2] * 512 / (duration_ms * 1000),
        "read_latency_mean_ms": delta[3] / delta[0] if delta[0] else None,
        "queue_depth_mean": delta[10] / duration_ms,
    }


def storage_locations(paths: dict[str, Path], required_device: str | None) -> dict[str, Any]:
    locations = {}
    for name, path in paths.items():
        snapshot = storage_snapshot(path)
        device = snapshot["device"] if snapshot else None
        if required_device is not None and device != required_device:
            raise ValueError(f"{name} path {path} uses {device}, expected {required_device}")
        model = Path(f"/sys/block/{device}/device/model") if device else None
        locations[name] = {
            "path": str(path.resolve()),
            "device": device,
            "model": model.read_text().strip() if model and model.exists() else None,
        }
    return locations


def is_competing_process(command: str, arguments: str) -> bool:
    executable = arguments.split("\0", 1)[0]
    native_vortex = any(directory in executable for directory in ("/target/", "/bin/")) and any(
        name in executable for name in ("vortex", "votex")
    )
    benchmark_python = command.startswith("python") and any(
        word in arguments for word in ("bench", "measure", "profile", "sweep")
    )
    return (
        native_vortex
        or command.startswith(
            (
                "datafusion",
                "duckdb",
                "lance-bench",
                "random-access",
                "compress-",
                "rustc",
                "cargo",
                "rsync",
                "samply",
                "perf",
            )
        )
        or command == "cp"
        or benchmark_python
    )


def competing_processes() -> list[tuple[int, str]]:
    """Find other benchmarks/builds, excluding this harness and its direct children."""
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            command = (proc / "comm").read_text().strip()
            arguments = (proc / "cmdline").read_bytes().decode(errors="replace").lower()
            if not is_competing_process(command, arguments):
                continue
            fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
            parent = int(fields[1])
            if parent != os.getpid():
                executable, _, timer = arguments.partition("\0")
                reservation_timer = executable.endswith("-reservation") and timer.rstrip("\0").isdigit()
                if command.startswith("python") or reservation_timer:
                    started = int(fields[19])
                    cpu_seconds = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
                    with COORDINATOR_LOCK:
                        now = time.monotonic()
                        previous = COORDINATOR_SAMPLES.get(int(proc.name))
                        COORDINATOR_SAMPLES[int(proc.name)] = (started, cpu_seconds, now)
                        idle = coordinator_is_idle(fields[0], started, cpu_seconds, now, previous)
                    if idle:
                        continue
                found.append((int(proc.name), command))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
    return sorted(found)


def coordinator_is_idle(
    state: str,
    started: int,
    cpu_seconds: float,
    now: float,
    previous: tuple[int, float, float] | None,
) -> bool:
    """Allow idle coordinators and reservation timers, while tracking native benchmark children."""
    if state != "S" or previous is None or previous[0] != started:
        return False
    elapsed = now - previous[2]
    consumed = cpu_seconds - previous[1]
    return elapsed > 0 and consumed >= 0 and consumed / elapsed < 0.02


def wait_for_quiet(output: Path, cold_data: Path | None = None, quiet_seconds: float = 60):
    """Require six quiet samples spanning the requested window; retain observations."""
    steady = 0
    previous = cpu_snapshot()
    previous_full = pressure_totals(pressure_snapshot()).get("io/full")
    previous_time = time.monotonic()
    while steady < 6:
        time.sleep(quiet_seconds / 6)
        current = cpu_snapshot()
        usage = cpu_usage(previous, current)
        previous = current
        pressure = pressure_snapshot()
        current_time = time.monotonic()
        current_full = pressure_totals(pressure).get("io/full")
        window_full = (
            (current_full - previous_full) / ((current_time - previous_time) * 1e6)
            if current_full is not None
            and previous_full is not None
            and current_full >= previous_full
            and current_time > previous_time
            else None
        )
        previous_full, previous_time = current_full, current_time
        io_full = next(
            (line for line in pressure.get("io", "").splitlines() if line.startswith("full ")), "full avg10=0"
        )
        io_avg = float(dict(field.split("=") for field in io_full.split()[1:])["avg10"])
        competitors = competing_processes()
        load = os.getloadavg()
        device = storage_snapshot(cold_data) if cold_data is not None else None
        device_idle = device["counters"][8] == 0 if device is not None else None
        quiet = (
            not competitors
            # Cold reads leave historical load high after their blocked threads have exited.
            and (cold_data is not None or load[0] < 4)
            and device_idle is not False
            and usage is not None
            and usage["busy_fraction"] < 0.1
            and usage["iowait_fraction"] < 0.02
            and (window_full < 0.01 if cold_data is not None and window_full is not None else io_avg < 1)
        )
        steady = steady + 1 if quiet else 0
        observation = {
            "time": time.time(),
            "quiet_seconds": quiet_seconds,
            "steady": steady,
            "load": load,
            "historical_load_gate": cold_data is None,
            "storage_device_idle": device_idle,
            "host_cpu": usage,
            "io_full_avg10": io_avg,
            "io_full_window_fraction": window_full,
            "io_pressure_gate": "current_window" if cold_data is not None and window_full is not None else "avg10",
            "competitors": competitors,
        }
        with (output / "quiet-samples.jsonl").open("a") as samples:
            samples.write(json.dumps(observation) + "\n")
        print(f"QUIET {steady}/6 load={load[0]:.2f} host_cpu={usage} competitors={competitors}", flush=True)


def measured_round(
    args: argparse.Namespace, workload: str, configs: list[tuple[str, str]], rnd: int, attempt: int
) -> tuple[dict[str, dict[str, Any]], list[tuple[int, str]]]:
    observed = set()
    if getattr(args, "per_process_quiet", False):
        results = {}
        individual_args = argparse.Namespace(**(vars(args) | {"per_process_quiet": False}))
        for config in configuration_order(configs, rnd):
            wait_for_quiet(args.output, args.data_root if args.evict_local_files else None, args.quiet_seconds)
            samples, competitors = measured_round(individual_args, workload, [config], rnd, attempt)
            results.update(samples)
            observed.update(competitors)
        return results, sorted(observed)
    done = threading.Event()

    def monitor():
        while not done.is_set():
            observed.update(competing_processes())
            done.wait(0.05)

    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    results = {}
    try:
        for config in configuration_order(configs, rnd):
            results["-".join(config)] = run(args, workload, config, f"round{rnd}-attempt{attempt}")
    finally:
        observed.update(competing_processes())
        done.set()
        watcher.join()
    return results, sorted(observed)


def configuration_order(configs: list[tuple[str, str]], round_idx: int) -> list[tuple[str, str]]:
    offset = round_idx % len(configs)
    ordered = configs[offset:] + configs[:offset]
    # Reverse after a full rotation cycle. Reversing every odd round would cancel the rotation
    # for a two-configuration comparison, leaving the baseline first in every round.
    if (round_idx // len(configs)) % 2:
        ordered.reverse()
    return ordered


def union_bytes(intervals: Iterable[tuple[int, int]]) -> int:
    total = end = 0
    for start, stop in sorted(intervals):
        total += max(0, stop - max(start, end))
        end = max(end, stop)
    return total


def parse_iterations(log: str) -> list[dict[str, Any]]:
    iterations: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in ANSI.sub("", log).splitlines():
        if line.startswith("IO_ITERATION_BEGIN"):
            current = {"metrics": collections.Counter(), "reads": [], "has_scan": False}
            if begin := re.search(r"ts_ns=(\d+)", line):
                current["begin_ns"] = int(begin[1])
        elif current is not None and line.startswith("IO_METRIC"):
            current["has_scan"] = True
            match = re.search(r"name=(\S+) value=(\d+)", line)
            if match is None:
                raise ValueError(f"Malformed IO_METRIC: {line}")
            name, value = match[1], int(match[2])
            if name not in ("output_rows", "output_batches") and not name.startswith(
                ("io.", "vortex.io.", "vortex.file.segments.cache.")
            ):
                continue
            metrics = current["metrics"]
            if name.endswith("_max"):
                metrics[name] = max(metrics.get(name, value), value)
            elif name.endswith("_min"):
                metrics[name] = min(metrics.get(name, value), value)
            else:
                metrics[name] += value
        elif current is not None and "local object-store read" in line:
            fields: dict[str, Any] = {
                k: int(v)
                for k, v in re.findall(
                    r"\b(offset|length|start_ns|prepare_ns|blocking_allocation_ns|allocation_ns|get_ns|cache_probe_ns|admission_ns|queue_ns|read_ns|resume_ns)=(\d+)",
                    line,
                )
            }
            path = re.search(r"path=(\S+)", line)
            if path is None:
                raise ValueError(f"Read timing is missing its path: {line}")
            fields["path"] = path[1]
            if probe_hit := re.search(r"cache_probe_hit=(true|false)", line):
                fields["cache_probe_hit"] = probe_hit[1] == "true"
            if reuse := re.search(r"file_handle_reused=(true|false)", line):
                fields["file_handle_reused"] = reuse[1] == "true"
            current["reads"].append(fields)
        elif current is not None and line.startswith("IO_ITERATION_END"):
            current.update({k: int(v) for k, v in re.findall(r"(rows|query_ns|ts_ns)=(\d+)", line)})
            iterations.append(current)
            current = None
    if not iterations:
        raise ValueError("No complete IO_ITERATION diagnostics; rebuild datafusion-bench")
    return iterations


def diagnostics(log: str) -> dict[str, Any]:
    iterations = parse_iterations(log)
    last = iterations[-1]
    by_file = collections.defaultdict(list)
    for read in last["reads"]:
        by_file[read["path"]].append((read["offset"], read["offset"] + read["length"]))
    requested = sum(r["length"] for r in last["reads"])
    unique = sum(union_bytes(intervals) for intervals in by_file.values())
    metrics = dict(last["metrics"])
    required = (
        "vortex.io.read.duration_count",
        "vortex.io.read.total_size",
        "vortex.file.segments.cache.hits",
        "vortex.file.segments.cache.misses",
    )
    missing = [name for name in required if name not in metrics]
    if missing and last["has_scan"]:
        raise ValueError(f"Missing scan counters: {missing}")
    metadata_only = not last["has_scan"]
    if metadata_only:
        metrics.update({name: 0 for name in required})
    phases = (
        "get_ns",
        "cache_probe_ns",
        "admission_ns",
        "allocation_ns",
        "queue_ns",
        "blocking_allocation_ns",
        "read_ns",
        "resume_ns",
    )
    phase_distributions = {phase: read_phase_distribution(last["reads"], phase) for phase in phases}
    size_groups = {}
    for name, low, high in (
        ("up_to_4KiB", 0, 4096),
        ("4KiB_to_64KiB", 4096, 65536),
        ("64KiB_to_1MiB", 65536, 1048576),
        ("above_1MiB", 1048576, float("inf")),
    ):
        reads = [read for read in last["reads"] if low < read["length"] <= high]
        size_groups[name] = {
            "reads": len(reads),
            "bytes": sum(read["length"] for read in reads),
            "phases_us": {phase: read_phase_distribution(reads, phase) for phase in phases},
        }
    return {
        "iterations": len(iterations),
        "metrics": metrics,
        "metadata_only": metadata_only,
        "local_reads": len(last["reads"]),
        "local_file_handle_reuse": {
            "observed_reads": sum("file_handle_reused" in read for read in last["reads"]),
            "reused_reads": sum(read.get("file_handle_reused", False) for read in last["reads"]),
        },
        "local_nowait": {
            "observed_reads": sum("cache_probe_hit" in read for read in last["reads"]),
            "cached_reads": sum(read.get("cache_probe_hit", False) for read in last["reads"]),
            "fallback_probes": sum(read.get("cache_probe_ns", 0) > 0 for read in last["reads"]),
        },
        "requested_bytes": requested,
        "distinct_bytes": unique,
        "repeated_bytes": requested - unique,
        # Cancelled reads can complete on the blocking pool after their scan future is dropped.
        # Preserve the discrepancy instead of treating completion-log events as scan counters.
        "trace_minus_scan_reads": len(last["reads"]) - metrics[required[0]],
        "trace_minus_scan_bytes": requested - metrics[required[1]],
        "compute_only": (
            metrics[required[0]] == 0
            and metrics[required[1]] == 0
            and (metrics[required[2]] > 0 or metadata_only)
            and metrics[required[3]] == 0
            and not last["reads"]
        ),
        "read_size_bytes": {
            "min": min((r["length"] for r in last["reads"]), default=0),
            "median": statistics.median([r["length"] for r in last["reads"]] or [0]),
            "max": max((r["length"] for r in last["reads"]), default=0),
        },
        "read_phases_median_us": {phase: values["median"] for phase, values in phase_distributions.items()},
        "read_phases_us": phase_distributions,
        "read_phases_by_size": size_groups,
        "observed_native_queues": native_queue_summary(last),
        "files": {
            path: {
                "reads": len(ranges),
                "requested_bytes": sum(b - a for a, b in ranges),
                "distinct_bytes": union_bytes(ranges),
            }
            for path, ranges in sorted(by_file.items())
        },
    }


def read_phase_distribution(reads: list[dict[str, Any]], phase: str) -> dict[str, int | float | None]:
    """Summarize measured phase values; older traces may omit individual phases."""
    values = sorted(read[phase] / 1000 for read in reads if phase in read)
    if not values:
        return {"samples": 0, "min": None, "median": None, "p95": None, "p99": None, "max": None}
    return {
        "samples": len(values),
        "min": values[0],
        "median": statistics.median(values),
        "p95": values[min(len(values) - 1, int(len(values) * 0.95))],
        "p99": values[min(len(values) - 1, int(len(values) * 0.99))],
        "max": values[-1],
    }


def native_queue_summary(iteration: dict[str, Any]) -> dict[str, Any] | None:
    """Estimate query-window occupancy from completed read phases, excluding unknown intervals."""
    begin, end = iteration.get("begin_ns"), iteration.get("ts_ns")
    if begin is None or end is None or end <= begin:
        return None
    intervals: dict[str, list[tuple[int, int, int | None]]] = collections.defaultdict(list)
    native_starts, native_ends = [], []
    for read in iteration["reads"]:
        if "start_ns" not in read:
            continue
        start = read["start_ns"]
        length = read.get("length")
        if "get_ns" in read and "admission_ns" in read:
            admitted = start + read["get_ns"] + read.get("cache_probe_ns", 0)
            intervals["admission"].append((admitted, admitted + read["admission_ns"], length))
        if all(phase in read for phase in ("prepare_ns", "queue_ns", "read_ns")):
            submitted = start + read["prepare_ns"]
            native = submitted + read["queue_ns"]
            allocated = native + read.get("blocking_allocation_ns", 0)
            finished = allocated + read["read_ns"]
            intervals["blocking_queue"].append((submitted, native, length))
            intervals["blocking_allocation"].append((native, allocated, length))
            intervals["native_read"].append((allocated, finished, length))
            native_starts.append(allocated)
            native_ends.append(finished)
            if "resume_ns" in read:
                intervals["async_resume"].append((finished, finished + read["resume_ns"], length))
    occupancy = {}
    for phase, spans in intervals.items():
        events: collections.Counter[int] = collections.Counter()
        byte_events: collections.Counter[int] = collections.Counter()
        byte_observed = 0
        covered = []
        for start, stop, length in spans:
            start, stop = max(start, begin), min(stop, end)
            if start < stop:
                events[start] += 1
                events[stop] -= 1
                covered.append((start, stop))
                if length is not None:
                    byte_events[start] += length
                    byte_events[stop] -= length
                    byte_observed += 1
        depth = peak = area = 0
        previous = begin
        for timestamp, delta in sorted(events.items()):
            area += depth * (timestamp - previous)
            depth += delta
            peak = max(peak, depth)
            previous = timestamp
        active_bytes = peak_bytes = byte_area = 0
        previous = begin
        for timestamp, delta in sorted(byte_events.items()):
            byte_area += active_bytes * (timestamp - previous)
            active_bytes += delta
            peak_bytes = max(peak_bytes, active_bytes)
            previous = timestamp
        occupancy[phase] = {
            "observed_intervals": len(covered),
            "mean_depth": area / (end - begin),
            "peak_depth": peak,
            "busy_union_ns": union_bytes(covered),
            "byte_observed_intervals": byte_observed,
            "mean_observed_bytes": byte_area / (end - begin) if byte_observed else None,
            "peak_observed_bytes": peak_bytes if byte_observed else None,
        }
    return {
        "window_ns": end - begin,
        "first_native_start_ns": min((start - begin for start in native_starts if begin <= start <= end), default=None),
        "first_native_completion_ns": min((stop - begin for stop in native_ends if begin <= stop <= end), default=None),
        "phases": occupancy,
        "coverage": (
            "Completed-read logs only: unfinished and unlogged cancelled reads are absent; depths are lower bounds."
        ),
    }


def run(
    args: argparse.Namespace,
    workload: str,
    config: tuple[str, str],
    tag: str,
    diagnostic: bool = False,
) -> dict[str, Any]:
    variant, mode = config
    suite, query = WORKLOADS[workload]
    data = args.data_root / (f"tpch/{args.scale_factor}" if suite == "tpch" else "clickbench_partitioned")
    stem = args.output / f"{workload}-{variant}-{mode}-{tag}"
    command = [
        str(args.baseline_binary if variant.startswith("v2-previous") else args.binary),
        suite,
        "--formats",
        "vortex",
        "--queries",
        str(query),
        "--iterations",
        str(args.diagnostic_iterations if diagnostic else args.iterations),
        "--hide-progress-bar",
        "--display-format",
        "gh-json",
        "--opt",
        f"remote-data-dir={data.as_uri()}/",
        "-o",
        str(stem.with_suffix(".jsonl")),
    ]
    if suite == "tpch":
        command += ["--opt", f"scale-factor={args.scale_factor}"]
    env = {k: v for k, v in os.environ.items() if k not in TOGGLES}
    overrides = dict(VARIANTS[variant])
    # Saved executables may contain compute experiments that the IO patch no longer includes.
    overrides["VORTEX_SCAN_GROUP_SPARSE_PROJECTION"] = "0"
    overrides["VORTEX_ONPAIR_COMPRESSED_LIKE"] = "0"
    if "VORTEX_LOCAL_READ_CONCURRENCY" not in overrides:
        overrides["VORTEX_LOCAL_READ_CONCURRENCY"] = str(args.local_read_concurrency)
    if mode != "direct":
        overrides["VORTEX_BENCH_SEGMENT_CACHE_MB"] = str(args.cache_mb if mode == "cached" else 0)
    if mode == "cached":
        overrides["VORTEX_BENCH_PRELOAD_SEGMENTS"] = "1"
    driver_trace = diagnostic and args.driver_trace and overrides.get("VORTEX_SCAN_V2") == "1"
    overrides["RUST_LOG"] = "warn,vortex_io::read_timing=debug" if diagnostic else "warn"
    if driver_trace:
        overrides["RUST_LOG"] += ",vortex_scan::driver=debug"
    if diagnostic and getattr(args, "lifetime_trace", False):
        overrides["RUST_LOG"] += ",vortex_file::scan_lifetime=debug"
    if diagnostic or getattr(args, "timing_metrics", False):
        command += ["--io-diagnostics"]
    env.update(overrides)
    metadata = {
        "command": command,
        "env": overrides,
        "load": os.getloadavg(),
        "pressure": pressure_snapshot(),
        "cpu_count": os.cpu_count(),
        "available_cpus": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count(),
        "datafusion_env": {k: v for k, v in env.items() if k.startswith("DATAFUSION_")},
    }
    if args.evict_local_files:
        metadata["page_cache"] = evict_local_files(data)
        metadata["evicted_files"] = metadata["page_cache"]["after"]["files"]
    stem.with_suffix(".command.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"START {stem.name}", flush=True)
    before_cpu = cpu_snapshot()
    before_storage = storage_snapshot(data) if args.evict_local_files else None
    before_child = resource.getrusage(resource.RUSAGE_CHILDREN)
    before_vm = vm_snapshot()
    before_pressure = pressure_snapshot()
    started = time.monotonic()
    with stem.with_suffix(".log").open("w") as output:
        subprocess.run(command, env=env, stdout=output, stderr=subprocess.STDOUT, check=True)
    after_child = resource.getrusage(resource.RUSAGE_CHILDREN)
    after_storage = storage_snapshot(data) if args.evict_local_files else None
    metadata["process_wall_s"] = time.monotonic() - started
    metadata["process_cpu_s"] = (
        after_child.ru_utime + after_child.ru_stime - before_child.ru_utime - before_child.ru_stime
    )
    metadata["process_resource"] = {
        name: getattr(after_child, name) - getattr(before_child, name)
        for name in ("ru_utime", "ru_stime", "ru_minflt", "ru_majflt", "ru_nvcsw", "ru_nivcsw")
    }
    metadata["host_vm_events"] = counter_delta(before_vm, vm_snapshot())
    after_pressure = pressure_snapshot()
    metadata["host_pressure_us"] = counter_delta(pressure_totals(before_pressure), pressure_totals(after_pressure))
    metadata["process_input_blocks"] = after_child.ru_inblock - before_child.ru_inblock
    # Linux task_io_get_inblock reports accounted storage read bytes divided by 512.
    metadata["process_storage_read_bytes"] = (
        metadata["process_input_blocks"] * 512 if sys.platform.startswith("linux") else None
    )
    metadata["storage_device"] = storage_delta(before_storage, after_storage)
    metadata["storage_device_snapshots"] = [before_storage, after_storage]
    metadata["host_cpu"] = cpu_usage(before_cpu, cpu_snapshot())
    metadata["load_after"] = os.getloadavg()
    metadata["pressure_after"] = after_pressure
    stem.with_suffix(".command.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if diagnostic:
        result = diagnostics(stem.with_suffix(".log").read_text())
        if driver_trace:
            driver_output = stem.with_suffix(".driver.json")
            trace_output = stem.with_suffix(".trace.json")
            analyzer_command = [
                sys.executable,
                str(Path(__file__).with_name("scan-io.py")),
                str(stem.with_suffix(".log")),
                "--output",
                str(driver_output),
            ]
            trace_mode = getattr(args, "driver_trace_mode", "full")
            if trace_mode != "json":
                analyzer_command += ["--queue-trace" if trace_mode == "queues" else "--trace", str(trace_output)]
            subprocess.run(analyzer_command, check=True)
            driver = json.loads(driver_output.read_text())
            result["driver"] = driver["totals"]
            result["driver_query"] = driver["query"]
            result["driver_stages"] = driver["stages"]
            result["driver_report"] = str(driver_output)
            result["driver_trace"] = str(trace_output) if trace_mode != "json" else None
        print(
            f"IO {stem.name}: reads={result['local_reads']} "
            f"MB={result['requested_bytes'] / 1e6:.2f} "
            f"repeated_MB={result['repeated_bytes'] / 1e6:.2f} "
            f"compute_only={result['compute_only']}",
            flush=True,
        )
        return result
    rows = [json.loads(line) for line in stem.with_suffix(".jsonl").read_text().splitlines() if line.startswith("{")]
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one query result in {stem}")
    times = [value / 1e6 for value in rows[0]["all_runtimes"]]
    if len(times) != args.iterations:
        raise ValueError(f"Unexpected iteration count in {stem}: {len(times)}")
    measured = times[args.discard_iterations :]
    print(f"TIME {stem.name}: median={statistics.median(measured):.3f} ms", flush=True)
    return {
        "all_ms": times,
        "measured_ms": measured,
        "median_ms": statistics.median(measured),
        "min_ms": min(measured),
        "max_ms": max(measured),
        "load_before": metadata["load"],
        "load_after": metadata["load_after"],
        "host_cpu": metadata["host_cpu"],
        "process_cpu_s": metadata["process_cpu_s"],
        "process_resource": metadata["process_resource"],
        "host_vm_events": metadata["host_vm_events"],
        "host_pressure_us": metadata["host_pressure_us"],
        "process_storage_read_bytes": metadata["process_storage_read_bytes"],
        "storage_device": metadata["storage_device"],
        "page_cache": metadata.get("page_cache"),
        "iteration_metrics": [
            {"metrics": dict(item["metrics"]), "rows": item.get("rows"), "query_ns": item.get("query_ns")}
            for item in parse_iterations(stem.with_suffix(".log").read_text())
        ]
        if getattr(args, "timing_metrics", False)
        else [],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release_debug/datafusion-bench"))
    parser.add_argument("--baseline-binary", type=Path, help="Saved executable used by the v2-previous control")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--resume", action="store_true", help="Resume accepted rounds with identical binaries and arguments"
    )
    parser.add_argument(
        "--require-storage-device", help="Require data, binary, output and temporary files on this Linux block device"
    )
    parser.add_argument(
        "--workloads",
        nargs="+",
        choices=WORKLOADS,
        default=DEFAULT_WORKLOADS,
        help="TPC-H queries are 1-indexed; ClickBench queries are 0-indexed",
    )
    parser.add_argument(
        "--variants", nargs="+", choices=VARIANTS, default=["v1", "v2"], help="Default: v1 v2; experiments are explicit"
    )
    parser.add_argument(
        "--modes", nargs="+", choices=["direct", "adapter", "cached"], default=["direct", "adapter", "cached"]
    )
    parser.add_argument("--scale-factor", default="10.0")
    parser.add_argument("--cache-mb", type=int, default=4096)
    parser.add_argument(
        "--local-read-concurrency", type=int, default=32, help="Local file limit; v2-unlimited/v2-baseline use 0"
    )
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--discard-iterations", type=int, default=2)
    parser.add_argument(
        "--evict-local-files",
        action="store_true",
        help="Request page-cache eviction before each process; requires one iteration and direct mode",
    )
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--diagnostic-iterations", type=int, default=3, help="Use 1 to keep large driver traces small")
    parser.add_argument(
        "--driver-trace", action="store_true", help="Save per-run driver reports and Perfetto traces for V2 diagnostics"
    )
    parser.add_argument(
        "--lifetime-trace",
        action="store_true",
        help="Trace registration drops, scope summaries, and IO wake/poll events",
    )
    parser.add_argument("--driver-trace-mode", choices=["full", "queues", "json"], default="queues")
    parser.add_argument(
        "--timing-metrics",
        action="store_true",
        help="Record scan counters after every timed query, with per-event tracing disabled",
    )
    parser.add_argument(
        "--wait-for-quiet",
        action="store_true",
        help="Wait for quiet before timings or after a contended round",
    )
    parser.add_argument(
        "--quiet-seconds", type=float, default=60, help="Required quiet window in seconds (default: 60)"
    )
    parser.add_argument(
        "--repeat-contended-rounds",
        action="store_true",
        help="Retain and repeat whole rounds overlapping detected benchmarks/builds",
    )
    parser.add_argument(
        "--per-process-quiet",
        action="store_true",
        help="Require quiet before every configuration; monitor only its measurement window",
    )
    parser.add_argument("--note", default="", help="Record conditions such as a noisy/shared machine in the summary")
    args = parser.parse_args()
    if any(variant.startswith("v2-previous") for variant in args.variants) and args.baseline_binary is None:
        parser.error("v2-previous variants require --baseline-binary")
    if (
        args.iterations <= args.discard_iterations
        or args.discard_iterations < 0
        or args.rounds < 1
        or args.cache_mb < 1
        or args.diagnostic_iterations < 1
        or args.local_read_concurrency < 1
        or args.quiet_seconds <= 0
    ):
        parser.error(
            "Require iterations > discard-iterations >= 0 and positive rounds, cache-mb, "
            "diagnostic-iterations, local-read-concurrency, quiet-seconds"
        )
    if args.evict_local_files and (args.iterations != 1 or args.diagnostic_iterations != 1 or args.modes != ["direct"]):
        parser.error("Local eviction requires iterations=1, diagnostic-iterations=1 and modes=direct")
    args.binary = args.binary.resolve()
    if args.baseline_binary is not None:
        args.baseline_binary = args.baseline_binary.resolve()
    args.data_root = args.data_root.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=args.resume)
    locations = storage_locations(
        {
            "data": args.data_root,
            "binary": args.binary,
            "output": args.output,
            "temporary": Path(tempfile.gettempdir()),
        },
        args.require_storage_device,
    )
    if args.baseline_binary is not None:
        locations.update(storage_locations({"baseline_binary": args.baseline_binary}, args.require_storage_device))
    configs = [(variant, mode) for mode in args.modes for variant in args.variants]
    with args.binary.open("rb") as binary:
        sha256 = hashlib.file_digest(binary, "sha256").hexdigest()
    summary: dict[str, Any] = {
        "binary": str(args.binary),
        "sha256": sha256,
        "storage_paths": locations,
        "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "results": {},
        "attempts": [],
        "timing_caveat": (
            "Shared-machine wall times include contention. Compare round medians and spread; "
            "diagnostic traces are not timing samples."
        ),
    }
    if args.baseline_binary is not None:
        with args.baseline_binary.open("rb") as binary:
            summary["baseline_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
        summary["baseline_binary"] = str(args.baseline_binary)

    if args.resume:
        previous = json.loads((args.output / "summary.json").read_text())
        for key in ("sha256", "baseline_sha256"):
            if previous.get(key) != summary.get(key):
                raise ValueError(f"Cannot resume: {key} changed")
        old_arguments = {key: value for key, value in previous["arguments"].items() if key != "resume"}
        old_arguments.setdefault("per_process_quiet", False)
        new_arguments = {key: value for key, value in summary["arguments"].items() if key != "resume"}
        if old_arguments != new_arguments:
            raise ValueError("Cannot resume: benchmark arguments changed")
        summary = previous

    def save():
        temporary = args.output / "summary.json.tmp"
        temporary.write_text(json.dumps(summary, indent=2) + "\n")
        temporary.replace(args.output / "summary.json")

    save()
    for workload in args.workloads:
        results: dict[str, dict[str, Any]] = summary["results"].setdefault(
            workload, {f"{v}-{m}": {"rounds": []} for v, m in configs}
        )
        completed_rounds = {len(entry["rounds"]) for entry in results.values()}
        if len(completed_rounds) != 1 or max(completed_rounds) > args.rounds:
            raise ValueError(f"Cannot resume: inconsistent accepted rounds for {workload}")
        for variant, mode in configs:
            if mode != "cached" or "io" in results[f"{variant}-{mode}"]:
                continue
            result = run(args, workload, (variant, mode), "diagnostic", diagnostic=True)
            results[f"{variant}-{mode}"]["io"] = result
            save()
            if mode == "cached" and not result["compute_only"]:
                raise ValueError(f"{workload}/{variant}: cache is not warm; inspect counters before timing")
        rnd = completed_rounds.pop()
        attempt = max((item["attempt"] for item in summary["attempts"] if item["workload"] == workload), default=-1) + 1
        if args.resume:
            partial_attempts = [
                int(match[1])
                for path in args.output.glob(f"{workload}-*-round*-attempt*.command.json")
                if (match := re.search(r"-attempt(\d+)\.command\.json$", path.name))
            ]
            attempt = max(attempt, max(partial_attempts, default=-1) + 1)
        if args.wait_for_quiet and rnd < args.rounds:
            wait_for_quiet(args.output, args.data_root if args.evict_local_files else None, args.quiet_seconds)
        while rnd < args.rounds:
            if args.wait_for_quiet and competing_processes():
                wait_for_quiet(args.output, args.data_root if args.evict_local_files else None, args.quiet_seconds)
            samples, observed = measured_round(args, workload, configs, rnd, attempt)
            accepted = not (args.repeat_contended_rounds and observed)
            summary["attempts"].append(
                {
                    "workload": workload,
                    "round": rnd,
                    "attempt": attempt,
                    "accepted": accepted,
                    "competitors": observed,
                    "samples": samples,
                }
            )
            attempt += 1
            if not accepted:
                print(f"REPEAT round {rnd}: competing processes {observed}; rejected samples retained", flush=True)
                save()
                if args.wait_for_quiet:
                    wait_for_quiet(args.output, args.data_root if args.evict_local_files else None, args.quiet_seconds)
                continue
            for name, result in samples.items():
                entry = results[name]
                entry["rounds"].append(result)
                entry["median_ms"] = statistics.median(value for r in entry["rounds"] for value in r["measured_ms"])
                entry["round_medians_ms"] = [r["median_ms"] for r in entry["rounds"]]
                entry["min_ms"] = min(value for r in entry["rounds"] for value in r["measured_ms"])
                entry["max_ms"] = max(value for r in entry["rounds"] for value in r["measured_ms"])
            save()
            rnd += 1
        print(
            f"TIMING COMPLETE {workload}: "
            + ", ".join(f"{name}={entry['median_ms']:.3f} ms" for name, entry in results.items()),
            flush=True,
        )

    # Finish untraced timings before capturing detailed direct/adapter diagnostics.
    for workload in args.workloads:
        results = summary["results"][workload]
        for variant, mode in configs:
            if mode == "cached" or "io" in results[f"{variant}-{mode}"]:
                continue
            if args.wait_for_quiet and competing_processes():
                wait_for_quiet(args.output, args.data_root if args.evict_local_files else None, args.quiet_seconds)
            results[f"{variant}-{mode}"]["io"] = run(args, workload, (variant, mode), "diagnostic", diagnostic=True)
            save()
        print(f"\n{workload}: query end-to-end milliseconds ({args.discard_iterations} initial iterations discarded)")
        print("configuration       median ms       min       max   reads   read MB   repeated MB")
        for name, result in results.items():
            io = result["io"]
            print(
                f"{name:20} {result['median_ms']:9.3f} {result['min_ms']:9.3f} "
                f"{result['max_ms']:9.3f} {io['local_reads']:7} "
                f"{io['requested_bytes'] / 1e6:9.2f} {io['repeated_bytes'] / 1e6:12.2f}"
            )
        print(flush=True)
        print(summary["timing_caveat"], flush=True)
    print(f"Saved {args.output / 'summary.json'}")


if __name__ == "__main__":
    main()
