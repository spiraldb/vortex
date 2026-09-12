# V1 versus push-frontier matrix runner

`run_push_frontier_matrix.py` runs the public DataFusion SQL path one query and backend per fresh
process. It supports `clickbench`, `tpch`, `fineweb`, and `tpcds` and never prepares or deletes
datasets.

The protocol is correctness-first. For each query, V1 writes a canonical Arrow result artifact and
push-frontier verifies its complete public result against it. Timing starts only after both commands
succeed. Every measured process is immediately preceded by an identical, untimed process for the
same query and backend. Measurement order alternates across query position and sample number. This
is an explicit symmetric **HOT-cache** protocol. The runner never attempts cache eviction and its
output provides no cold-cache evidence.

Build the binary first, then inspect the complete plan without running queries:

```bash
cargo build -p datafusion-bench --profile release_debug --features unstable_encodings

python3 benchmarks/datafusion-bench/scripts/run_push_frontier_matrix.py tpch \
  --binary target/release_debug/datafusion-bench \
  --input-root vortex-bench/data/tpch/1.0 \
  --output-dir /private/tmp/push-frontier-tpch \
  --partitions 8 \
  --samples 5 \
  --queries 1,6 \
  --opt scale-factor=1.0 \
  --dry-run
```

`--dry-run` does not inspect or hash `--input-root` and does not create the output directory. Remove
`--dry-run` to execute the matrix; an `--input-root` is then required. It must be the existing local
dataset directory actually consumed by the selected suite and options, must be disjoint from the
output directory, and must contain only regular files and directories. Symlinks and special files
are rejected. Use repeated `--opt KEY=VALUE` arguments for the options accepted by the benchmark
binary. Typical values are:

- ClickBench: `--opt flavor=partitioned`
- TPC-H/TPC-DS: `--opt scale-factor=1.0`
- Any supported suite with prepared remote data: `--opt remote-data-dir=...` (the immutable input
  manifest still covers only the explicitly supplied local `--input-root`)
- FineWeb needs no option for its default local data directory.

The runner always passes `--formats vortex`, removes `VORTEX_USE_SCAN_API` from every child
environment, sets `VORTEX_SCAN_BACKEND` explicitly to `v1` or `push-frontier`, and uses the
`--partitions N` runner argument for both the benchmark binary's `--threads N` option and
`DATAFUSION_EXECUTION_TARGET_PARTITIONS=N`. `--threads N` fixes both DataFusion target partitions
and Tokio worker threads to `N`; each opened Vortex file uses one builder concurrency unit and one
push worker so neither backend multiplies that process budget. It discovers the selected IDs from
the binary's `--print-queries` output. JSON environment records use `null` to mean the variable was
explicitly unset.

Before any correctness or measurement child, the runner recursively hashes `--input-root` once in
deterministic path order. `run-manifest.json` records every relative input path, byte size, and
SHA-256; an aggregate input-manifest SHA-256; the benchmark executable's absolute path, size, and
SHA-256; Git HEAD; and hashes of whole-repository (`.`) porcelain status and binary working-tree
diff. Dirty trees are allowed and explicitly recorded. The exact run-manifest file SHA-256 and its
relative path are linked from the `matrix.jsonl` header and every child record.

Reading all input bytes for this identity step may itself warm the OS page cache. That is consistent
with the explicit symmetric **HOT-cache** protocol and must not be interpreted as cold-cache
evidence. Input hashing is performed once per matrix, before any child process.

`matrix.jsonl` contains the configuration and one record per child, including argv, environment
overrides, exit status, supervisor-observed wall time, and log paths. On macOS, measured children
are wrapped with `/usr/bin/time -l` and its whole-process maximum resident set size is recorded in
bytes. Other platforms report RSS as unavailable rather than substituting a different estimate.
Each child has bounded stdout and stderr files below `logs/qNNNNNN/`; canonical artifacts are below
`results/qNNNNNN/`. The output directory must be new or empty, and execution stops immediately on
any correctness mismatch or other non-zero child exit.

Run the focused tests with:

```bash
python3 -m unittest discover \
  -s benchmarks/datafusion-bench/scripts \
  -p 'test_*.py'
```
