#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# DO NOT MERGE: diagnoses DataFusion memory at TPC-H SF1000.
#
# 1. Runs the heaviest queries under several scan settings, each with its own memory monitor, to
#    test whether the Vortex excess scales with in-flight scan splits
#    (VORTEX_SCAN_CONCURRENCY x available parallelism per stream, one stream per partition).
# 2. Records page-fault call stacks with perf to show which code touches the new memory.
# 3. Saves EXPLAIN plans for the queries that run out of memory.

set -Eeuo pipefail

BIN=${BIN:-target/release_debug/datafusion-bench}
QUERIES=(1 2 9 16 18 21)
RUNNER="ec2_${RUNS_ON_INSTANCE_TYPE:-unknown}"

run_variant() {
    local name=$1 formats=$2
    shift 2
    local dir="$OUT_DIR/$name"
    mkdir -p "$dir"
    python3 scripts/bench-memory.py monitor --output "$dir/memory.csv" --phase-file "$PHASE_FILE" --interval 1 &
    local monitor=$!
    # shellcheck disable=SC2086
    env "$@" python3 scripts/bench-memory.py run-queries \
        --binary "$BIN" --engine datafusion --formats $formats --queries "${QUERIES[@]}" \
        --timeout 1800 --opt scale-factor="$SCALE_FACTOR" --out-dir "$dir" \
        --phase-file "$PHASE_FILE" --min-available-gib 8 || true
    kill "$monitor" || true
    wait "$monitor" || true
}

run_variant parquet-default parquet
run_variant vortex-default vortex
run_variant vortex-scan1 vortex VORTEX_SCAN_CONCURRENCY=1
run_variant vortex-tp24 vortex DATAFUSION_EXECUTION_TARGET_PARTITIONS=24
run_variant vortex-scan1-tp24 vortex VORTEX_SCAN_CONCURRENCY=1 DATAFUSION_EXECUTION_TARGET_PARTITIONS=24

echo "::group::EXPLAIN plans"
"$BIN" tpch --explain --formats parquet,vortex --queries 9,18,21 --runner "$RUNNER" \
    --opt scale-factor="$SCALE_FACTOR" > "$OUT_DIR/explain.txt" 2>&1 || true
echo "::endgroup::"

# Page faults mark the first touch of each new page, so their call stacks show what grows RSS.
sudo -n apt-get install -y -qq linux-tools-common "linux-tools-$(uname -r)" > /dev/null 2>&1 \
    || sudo -n apt-get install -y -qq linux-tools-generic > /dev/null 2>&1 || true
PERF=$(command -v perf || find /usr/lib/linux-tools* -name perf -type f 2>/dev/null | head -1 || true)
if [[ -n "$PERF" ]] && "$PERF" --version > /dev/null 2>&1; then
    sudo -n sysctl -w kernel.perf_event_paranoid=-1 > /dev/null || true
    profile() {
        local name=$1 format=$2 query=$3
        shift 3
        env "$@" "$PERF" record -e page-faults -c 2000 --call-graph fp -o "$OUT_DIR/$name.perf" -- \
            "$BIN" tpch --formats "$format" --queries "$query" --iterations 1 --hide-progress-bar \
            --runner "$RUNNER" --opt scale-factor="$SCALE_FACTOR" > /dev/null 2>&1 || true
        "$PERF" report -i "$OUT_DIR/$name.perf" --stdio --no-children --sort sym -g none \
            --percent-limit 0.5 > "$OUT_DIR/$name.flat.txt" 2>/dev/null || true
        "$PERF" report -i "$OUT_DIR/$name.perf" --stdio --no-children --sort sym \
            -g caller,1,callee,function,percent --percent-limit 3 2>/dev/null \
            | head -1500 > "$OUT_DIR/$name.graph.txt" || true
        rm -f "$OUT_DIR/$name.perf"
    }
    profile pagefaults-vortex-q01 vortex 1
    profile pagefaults-parquet-q01 parquet 1
    profile pagefaults-vortex-q16 vortex 16
    profile pagefaults-vortex-scan1-q01 vortex 1 VORTEX_SCAN_CONCURRENCY=1
else
    echo "::warning::perf unavailable; skipping page-fault profiles"
fi

for dir in "$OUT_DIR"/*/; do
    [[ -f "$dir/queries.jsonl" ]] || continue
    python3 scripts/bench-memory.py summarize --monitor-csv "$dir/memory.csv" \
        --queries-jsonl "$dir/queries.jsonl" --title "$(basename "$dir")" >> "$GITHUB_STEP_SUMMARY"
done
