#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# DO NOT MERGE: tries DataFusion with a memory limit and spilling at TPC-H SF1000.
#
# With DATAFUSION_BENCH_MEMORY_LIMIT_GIB set, aggregations and sorts spill to disk and hash join
# builds that do not fit fail with a resources-exhausted error instead of growing until the
# process is killed. prefer_hash_join=false swaps hash joins for sort-merge joins, which spill.

set -Eeuo pipefail

BIN=${BIN:-target/release_debug/datafusion-bench}
ALL_QUERIES=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)
JOIN_QUERIES=(9 18 21)
SPILL_DIR="${GITHUB_WORKSPACE:-$PWD}/df-spill"

# The memory pool does not track scan buffers or batches in flight, so leave headroom below the
# machine's 188 GiB for them.
export DATAFUSION_BENCH_MEMORY_LIMIT_GIB=${DATAFUSION_BENCH_MEMORY_LIMIT_GIB:-100}
export DATAFUSION_BENCH_SPILL_DIR="$SPILL_DIR"
export RUST_BACKTRACE=0

run_variant() {
    local name=$1 formats=$2 queries=$3
    shift 3
    local dir="$OUT_DIR/$name"
    mkdir -p "$dir" "$SPILL_DIR"
    python3 scripts/bench-memory.py monitor --output "$dir/memory.csv" --phase-file "$PHASE_FILE" --interval 1 &
    local monitor=$!
    # shellcheck disable=SC2086
    env "$@" python3 scripts/bench-memory.py run-queries \
        --binary "$BIN" --engine datafusion --formats $formats --queries $queries \
        --timeout 1800 --opt scale-factor="$SCALE_FACTOR" --out-dir "$dir" \
        --phase-file "$PHASE_FILE" --min-available-gib 8 || true
    kill "$monitor" || true
    wait "$monitor" || true
    du -sh "$SPILL_DIR" || true
    rm -rf "$SPILL_DIR"
}

run_variant parquet-spill parquet "${ALL_QUERIES[*]}"
run_variant vortex-spill vortex "${ALL_QUERIES[*]}"
run_variant vortex-spill-scan1 vortex "${ALL_QUERIES[*]}" VORTEX_SCAN_CONCURRENCY=1
run_variant parquet-spill-smj parquet "${JOIN_QUERIES[*]}" DATAFUSION_OPTIMIZER_PREFER_HASH_JOIN=false
run_variant vortex-spill-scan1-smj vortex "${JOIN_QUERIES[*]}" \
    VORTEX_SCAN_CONCURRENCY=1 DATAFUSION_OPTIMIZER_PREFER_HASH_JOIN=false

for dir in "$OUT_DIR"/*/; do
    [[ -f "$dir/queries.jsonl" ]] || continue
    python3 scripts/bench-memory.py summarize --monitor-csv "$dir/memory.csv" \
        --queries-jsonl "$dir/queries.jsonl" --title "$(basename "$dir")" >> "$GITHUB_STEP_SUMMARY"
done
