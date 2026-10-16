#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# DO NOT MERGE: checks whether DataFusion runs all of TPC-H SF1000 with sort-merge joins
# (prefer_hash_join=false), a memory limit, and a 500 GiB spill cap. Hash join builds cannot
# spill, and the default 100 GB spill cap stopped the first sort-merge join attempt. Vortex runs
# include the scan concurrency fix.

set -Eeuo pipefail

BIN=${BIN:-target/release_debug/datafusion-bench}
ALL_QUERIES=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)
SPILL_DIR="${GITHUB_WORKSPACE:-$PWD}/df-spill"
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

SMJ_ENV=(
    DATAFUSION_OPTIMIZER_PREFER_HASH_JOIN=false
    DATAFUSION_BENCH_MEMORY_LIMIT_GIB=80
    DATAFUSION_BENCH_MAX_SPILL_GIB=500
    DATAFUSION_BENCH_SPILL_DIR="$SPILL_DIR"
)
run_variant parquet-smj-spill500 parquet "${ALL_QUERIES[*]}" "${SMJ_ENV[@]}"
run_variant vortex-fix-smj-spill500 vortex "${ALL_QUERIES[*]}" "${SMJ_ENV[@]}"

for dir in "$OUT_DIR"/*/; do
    [[ -f "$dir/queries.jsonl" ]] || continue
    python3 scripts/bench-memory.py summarize --monitor-csv "$dir/memory.csv" \
        --queries-jsonl "$dir/queries.jsonl" --title "$(basename "$dir")" >> "$GITHUB_STEP_SUMMARY"
done
