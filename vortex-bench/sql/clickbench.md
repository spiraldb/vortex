# ClickBench benchmark

[ClickBench](https://github.com/ClickHouse/ClickBench) is ClickHouse's web-analytics
benchmark: 43 queries over a single wide `hits` table (~100M rows of real-ish traffic
data). It is heavy on aggregations, `GROUP BY`s over high-cardinality columns, and
selective string filters, and is the main "wide table scan" workload in CI.

The queries live in [`clickbench_queries.sql`](./clickbench_queries.sql) (one query per
line, numbered from Q0 in file order). The harness lives in
[`src/clickbench`](../src/clickbench).

## CI variant

CI runs this suite from local NVMe as the `Clickbench on NVME` PR comment, comparing
DataFusion and DuckDB over Parquet, Vortex, and vortex-compact files (plus a native
DuckDB baseline).

## Sorted variant

`Clickbench Sorted on NVME` runs the same table sorted by event time, split into 100
shards whose filenames are shuffled so engines cannot rely on file order, exercising sort
pushdown and zone-map-style pruning on a subset of the queries. See
[`ClickBenchSortedBenchmark`](../src/clickbench/benchmark.rs).

## Running locally

```bash
vx-bench run clickbench --engine datafusion,duckdb --format parquet,vortex
vx-bench run clickbench-sorted --engine datafusion,duckdb --format parquet,vortex
```

## Exact V1/push-frontier result verification

`datafusion-bench` can write and verify canonical binary result artifacts. Each
artifact embeds its output schema as Arrow IPC and stores both the final encoded row
sequence and a sorted row multiset, preserving duplicate rows and exact
floating-point representations. Artifact generation and verification are one-shot
modes: they exit before benchmark timing starts and therefore do not warm a timed
process.

Build the runner, write the V1 artifacts for all 43 queries, then verify the
push-frontier results against them:

```bash
cargo build -p datafusion-bench \
  --profile release_debug --features unstable_encodings

env -u VORTEX_USE_SCAN_API DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
  VORTEX_SCAN_BACKEND=v1 \
  target/release_debug/datafusion-bench clickbench \
  --formats vortex --threads 1 \
  --opt flavor=partitioned \
  --opt queries-file=vortex-bench/sql/clickbench_correctness_queries.sql \
  --result-order multiset \
  --write-result-artifacts target/clickbench-results/v1

env -u VORTEX_USE_SCAN_API DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
  VORTEX_SCAN_BACKEND=push-frontier \
  target/release_debug/datafusion-bench clickbench \
  --formats vortex --threads 1 \
  --opt flavor=partitioned \
  --opt queries-file=vortex-bench/sql/clickbench_correctness_queries.sql \
  --result-order multiset \
  --verify-result-artifacts target/clickbench-results/v1
```

The exact phase uses one worker and partition because DataFusion parallel `Float64`
aggregate reductions can vary in their low bits even across repeated V1 executions.
The correctness query file preserves all 43 query indices. It changes only Q17 and
Q31-Q41 to make their limited results deterministic. Q17's otherwise unordered
`LIMIT 10` gains `ORDER BY "UserID", "SearchPhrase"`; Q31-Q41 append their group keys
after the existing aggregate sort so ties at the `LIMIT`/`OFFSET` boundary have a
total order. Ordinary and timed runs must omit `queries-file`; they continue to use
the canonical [`clickbench_queries.sql`](./clickbench_queries.sql) byte-for-byte
unchanged.

The artifact modes enforce these scan paths: writes require effective backend `v1`,
verification requires effective backend `push-frontier`, and both reject
`VORTEX_USE_SCAN_API=1`. The default `--result-order multiset` always checks exact
schema and logical row-multiset equality. This is the appropriate SQL comparison for
unordered queries and for `ORDER BY` ties, whose physical sequence is not unique.
Use `--result-order ordered` only when the query defines a total row order; it adds
exact final row-sequence equality. Shared DataFusion operators above both Vortex scans
own SQL `ORDER BY` semantics.

Use `--queries 0` (or a comma-separated list) to check individual queries. Each
query/format is stored separately, for example
`target/clickbench-results/v1/q00-vortex-file-compressed.arrow`. A missing,
corrupt, schema-different, or row-different artifact makes the process fail. The
canonicalization step collects and sorts the entire query result in memory, so it
should not be combined with memory or timing measurements. Artifacts carry their
canonical-format version, V1 producer provenance, and expected push-frontier verifier
provenance. Baseline creation refuses to overwrite an existing file; use a new
directory for a new baseline.
