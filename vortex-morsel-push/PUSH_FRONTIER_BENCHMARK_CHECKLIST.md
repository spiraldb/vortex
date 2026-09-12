# Push-frontier public benchmark checklist

Status as of 2026-09-12. This is the authoritative completion matrix for comparing the established
V1 scan path with the grouped-I/O push-frontier path through the public DataFusion benchmark
integration. It records source inventory separately from execution evidence. Timing is claimed only
for rows with a fresh, reproducible measurement artifact recorded below.

Legend: `[x]` means the source inventory is present and was inspected. `[ ]` means that a fresh,
reproducible artifact satisfying the contract below has not yet been attached. A row is complete
only when all six boxes are checked. Historical internal-harness results do not check a public-run
box.

| Column | Required evidence |
|---|---|
| Source | The public suite maps the query ID to a checked-in SQL template. |
| V1 | A successful one-query process using `VORTEX_SCAN_BACKEND=v1`. |
| Frontier | A successful one-query process using `VORTEX_SCAN_BACKEND=push-frontier`. |
| Exact | Equal public query-result schema and logical row multiset after deterministic canonicalization; exact row sequence only when the SQL defines a total order and ordered verification is requested. |
| RSS | Comparable whole-process peak RSS evidence from the same protocol. |
| Time | Comparable wall-time evidence from the same protocol. |

## Fairness and correctness contract

- Use the same immutable Vortex files for both paths. Record their paths, sizes, and content hashes.
- Use the same DataFusion query, projection, pushed filter, selection, limit, and default root
  fields. Backend selection alone may differ. Shared DataFusion operators above both scans own SQL
  `ORDER BY`; scan-internal physical output order is not part of this comparison.
- Keep `VORTEX_USE_SCAN_API` unset. Its V2 path bypasses this V1/push-frontier selector.
- Record the explicit backend identity: `v1` or `push-frontier`. Never infer it from a generic
  `vortex` result label.
- Run exactly one query and one measured iteration per process. Repeat with new processes rather
  than mixing cold and warm iterations in one session.
- Use one symmetric, recorded cache protocol. For hot-cache evidence, fully prewarm the same file
  set before each process. For cold-cache evidence, check a box only when cache eviction is verified;
  the Linux `/proc/sys/vm/drop_caches` helper is not proof on macOS.
- Use `--threads 1` and `DATAFUSION_EXECUTION_TARGET_PARTITIONS=1` for the separate exact
  correctness phase. Shared DataFusion casts integer `AVG` inputs to `Float64`, and its parallel
  partial-aggregate/coalesce/final-aggregate pipeline may merge floating-point partials in arrival
  order; even repeated V1 runs can therefore differ by a few low bits at four partitions. The
  single-partition correctness phase eliminates that backend-independent nondeterminism without
  weakening exact scalar comparison.
- Use fixed `--threads 4` and target partitions 4 symmetrically for the performance and RSS phase.
  `--threads N` sets both DataFusion target partitions and Tokio runtime workers to `N`. Each opened
  file uses builder concurrency `1` and one push/push-frontier executor worker, so neither scan
  applies `N` as a second multiplier. Timed/RSS output is not the exact correctness oracle.
- Measure whole-process peak RSS continuously or with the operating system high-water mark. The
  current benchmark tracker samples only the endpoints and is not sufficient peak evidence.
- Correctness is the public DataFusion result, not a scan-internal row set or frontier cursor. Always
  compare the exact schema and logical row multiset, including duplicates and exact scalar
  representations. This multiset policy is required for unordered results and `ORDER BY` ties,
  whose physical sequence is not unique. Request sequence equality only when `ORDER BY` defines a
  total order. TPC-DS Q09, Q13, Q28, Q32, Q38, Q48, Q87, Q88, and Q97 have no `ORDER BY` and require
  multiset comparison.
- Do correctness runs before timing. Do not retain every output batch in timed runs; consume an
  equivalent public-result digest or discard output symmetrically after a separate exact comparison.
- Do not silently fall back to V1/current push, change files for only one backend, or omit a failing
  query. Record failures by phase: planning, scan construction/layout, execution, exact mismatch,
  memory, or timing.

The common persistent opener applies selection, filter, limit, concurrency, projection, byte-range
translation, scan-order metadata, and Arrow conversion after choosing the backend
(`vortex-datafusion/src/persistent/opener.rs:434-615`). The root/default projection and default
selection/filter/scan-order values are defined by the two builders
(`vortex-layout/src/scan/scan_builder.rs:89-111` and
`vortex-morsel-scan/src/scan_builder.rs:51-88`).

## Known path/default differences

| Concern | V1 | Push frontier | Evidence requirement |
|---|---|---|---|
| Scan implementation | Cached `LayoutReader` plus current `ScanBuilder` | Raw footer layout plus physical `MorselScanBuilder`/`ExecPlan` | Preserve this intended path difference. |
| Backend identity | `VORTEX_SCAN_BACKEND=v1` | `VORTEX_SCAN_BACKEND=push-frontier` | Capture env and emitted label with every artifact. |
| Task concurrency | Without `--threads`: builder default `4 × available_parallelism`; with `--threads N`: `N` Tokio workers and builder factor `1` | Without `--threads`: builder default `4 × available_parallelism` and one push worker; with `--threads N`: the same `N` Tokio workers, builder factor `1`, and one push worker | Record the explicit `N`, DataFusion target partitions, Tokio workers, and host parallelism; use the same command for both backends. |
| Work unit | Layout boundaries subdivided toward 100,000 rows | Physical morsels target 128 Ki rows | Record both, or add a common experimental setting before attributing differences. |
| I/O scheduling | Current asynchronous LayoutReader path | Grouped frontier: zero extra down lookahead, zero speculative right groups, 32-range refills | Preserve production defaults; record them. |
| Pruning | V1 layout scan pruning | Push performs a fresh bounded LayoutReader zone-pruning prepass, then runs the physical plan | Include prepass cost and memory in the frontier measurement. |
| Supported layouts | General LayoutReader path | Physical root must be a non-null struct; columns must lower through zoned/legacy-stats, flat, or recursively chunked layouts | Verify every shared file; never generate a frontier-only substitute. |

Relevant source anchors are `vortex-morsel-scan/src/lib.rs:21-55`,
`vortex-morsel-scan/src/scan_builder.rs:51-88`,
`vortex-morsel-push/src/executor.rs:48-148`,
`vortex-layout/src/scan/split_by.rs:16-64`, and
`vortex-layout/src/scan/mod.rs:16-19`.

## Dataset readiness

| Suite | Local source status | Generation command |
|---|---|---|
| ClickBench Q0-Q42 | Partitioned Parquet/Vortex files are present locally; treat their hashes as the shared input identity. | `target/release_debug/data-gen clickbench --formats parquet,vortex --opt flavor=partitioned` |
| TPC-H Q1-Q22 | SF1 Parquet/Vortex files are present locally; treat their hashes as the shared input identity. | `target/release_debug/data-gen tpch --formats parquet,vortex --opt scale-factor=1.0` |
| FineWeb Q0-Q8 | **Missing locally.** Generation downloads the pinned FineWeb sample, then converts it. | `target/release_debug/data-gen fineweb --formats parquet,vortex` |
| TPC-DS Q01-Q99 | **Missing locally.** Generation requires DuckDB `dsdgen`, exports Parquet, then converts it. | `target/release_debug/data-gen tpcds --formats parquet,vortex --opt scale-factor=1.0` |

Generation is idempotent and skips existing Vortex destinations
(`vortex-bench/src/conversions.rs:269-331`). Verify provenance before reuse. This branch's default
conversion deliberately uses the same dictionary-free, zoned morsel layout for both backends
(`vortex-bench/src/lib.rs:255-314`); that shared file-format choice is not a backend difference.

## Reproducible command templates

Build once from the exact commit being measured:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  cargo build -p vortex-bench --bin data-gen \
  --profile release_debug --features unstable_encodings

RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  cargo build -p datafusion-bench \
  --profile release_debug --features unstable_encodings
```

For each matrix row set `SUITE` and `QUERY`, prewarm the exact shared Vortex directory using the
same command before each backend, and run each backend in a new process. `QUERY` is unpadded on the
CLI, including TPC-DS (`QUERY=1` selects Q01).

```bash
SUITE=clickbench
QUERY=0
THREADS=4
VORTEX_DATA_DIR=vortex-bench/data/clickbench_partitioned/vortex-file-compressed
ARTIFACT_DIR=/private/tmp/push-frontier-public

mkdir -p "$ARTIFACT_DIR"
find "$VORTEX_DATA_DIR" -type f -name '*.vortex' -exec cat {} + >/dev/null
/usr/bin/time -l env -u VORTEX_USE_SCAN_API \
  VORTEX_SCAN_BACKEND=v1 \
  target/release_debug/datafusion-bench "$SUITE" \
  --formats vortex --queries "$QUERY" --iterations 1 --threads "$THREADS" \
  --display-format gh-json --track-memory --hide-progress-bar \
  --runner "push-frontier-v1-t${THREADS}" \
  -o "$ARTIFACT_DIR/${SUITE}-q${QUERY}-v1.jsonl" \
  2>"$ARTIFACT_DIR/${SUITE}-q${QUERY}-v1.time.txt"

find "$VORTEX_DATA_DIR" -type f -name '*.vortex' -exec cat {} + >/dev/null
/usr/bin/time -l env -u VORTEX_USE_SCAN_API \
  VORTEX_SCAN_BACKEND=push-frontier \
  target/release_debug/datafusion-bench "$SUITE" \
  --formats vortex --queries "$QUERY" --iterations 1 --threads "$THREADS" \
  --display-format gh-json --track-memory --hide-progress-bar \
  --runner "push-frontier-t${THREADS}" \
  -o "$ARTIFACT_DIR/${SUITE}-q${QUERY}-frontier.jsonl" \
  2>"$ARTIFACT_DIR/${SUITE}-q${QUERY}-frontier.time.txt"
```

Use these exact suite variables; add the listed option to both timed and correctness commands.

| Suite | `SUITE` | Query values | `VORTEX_DATA_DIR` | Extra option |
|---|---|---|---|---|
| ClickBench | `clickbench` | `0`-`42` | `vortex-bench/data/clickbench_partitioned/vortex-file-compressed` | `--opt flavor=partitioned` |
| TPC-H SF1 | `tpch` | `1`-`22` | `vortex-bench/data/tpch/1.0/vortex-file-compressed` | `--opt scale-factor=1.0` |
| FineWeb | `fineweb` | `0`-`8` | `vortex-bench/data/fineweb/vortex-file-compressed` | none |
| TPC-DS SF1 | `tpcds` | `1`-`99` | `vortex-bench/data/tpcds/1.0/vortex-file-compressed` | `--opt scale-factor=1.0` |

Add the table's extra option to both backend commands. Use `/usr/bin/time -v` instead of `-l` on
Linux. Alternate backend order across repeated process pairs.
Do not check `Exact` from these JSONL files: the timed SQL runner records row counts and timings,
not complete public-result values (`vortex-bench/src/runner.rs:127-224,367-466`).

Run exact public-result comparison separately. V1 writes a canonical artifact containing the Arrow
schema, final row sequence, and sorted row multiset; frontier must verify the exact schema and
multiset against it. Duplicate rows and exact scalar representations are preserved. The default
`multiset` policy ignores batch boundaries and physical row order; use `ordered` only for a query
whose `ORDER BY` defines a total order
(`benchmarks/datafusion-bench/src/result_artifacts.rs`).

```bash
RESULT_DIR="$ARTIFACT_DIR/results-${SUITE}-q${QUERY}"

env -u VORTEX_USE_SCAN_API \
  DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
  VORTEX_SCAN_BACKEND=v1 \
  target/release_debug/datafusion-bench "$SUITE" \
  --formats vortex --queries "$QUERY" --threads 1 \
  --result-order multiset \
  --write-result-artifacts "$RESULT_DIR"

env -u VORTEX_USE_SCAN_API \
  DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
  VORTEX_SCAN_BACKEND=push-frontier \
  target/release_debug/datafusion-bench "$SUITE" \
  --formats vortex --queries "$QUERY" --threads 1 \
  --result-order multiset \
  --verify-result-artifacts "$RESULT_DIR"
```

Add the suite table's extra option to both correctness commands. These flags are wired in
`benchmarks/datafusion-bench/src/main.rs`; artifact mode rejects a write unless the effective backend
is V1, rejects verification unless it is push-frontier, and rejects `VORTEX_USE_SCAN_API=1`. Source
presence does not check any query's `V1`, `Frontier`, or `Exact` box until both commands succeed and
their artifact is retained.

The one-partition policy was validated with five independent ClickBench Q3 V1 artifacts and five
fresh push-frontier verifications per artifact. All 25 verifications produced Float64 bits
`0x43c18c52b0d921b0` and artifact SHA-256
`b0c8284e9d874ede4643bf11faee882e6ceb6310726cefe5dc61aa85cef81533`. Evidence is retained under
`/private/tmp/clickbench-q3-deterministic-v3.FFKFEm`; its `results.tsv` SHA-256 is
`8268e7b9a1b15279d7b08afa5edf689e038cb46ee805a7eee1c91dbd25930ddc`.

## ClickBench Q0-Q42

Source inventory: 43 semicolon-delimited statements are enumerated from Q0 in file order
(`vortex-bench/src/clickbench/benchmark.rs:60-85`,
`vortex-bench/sql/clickbench_queries.sql`).

| Query | Source | V1 | Frontier | Exact | RSS | Time |
|---|---:|---:|---:|---:|---:|---:|
| Q0 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q1 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q2 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q3 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q4 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q5 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q6 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q7 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q8 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q9 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q10 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q11 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q12 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q13 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q14 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q15 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q16 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q17† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q18 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q19 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q20 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q21 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q22 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q23 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q24 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q25 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q26 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q27 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q28 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q29 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q30 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q31† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q32† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q33† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q34† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q35† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q36† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q37† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q38† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q39† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q40† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q41† | [x] | [x] | [x] | [x] | [x] | [x] |
| Q42 | [x] | [x] | [x] | [x] | [x] | [x] |

Rows marked † use the checked-in correctness-only total-order query source for `Exact`: Q17 adds its
group keys to an otherwise unordered `LIMIT`, and Q31-Q41 append group-key tie-breakers after
the canonical aggregate sort. The canonical timed SQL is unchanged. Both original backends also
completed every marked query with the expected 10 rows; original Q17, Q31, Q38, Q39, and Q41
demonstrated valid non-unique subset mismatches.

Evidence for all ClickBench Q0-Q42 V1/Frontier/Exact checks:

- Artifact root: `/private/tmp/push-frontier-clickbench-all-v3-deterministic.nI0gJJ`
- Complete strict command, exit-code, artifact-hash, and status ledger:
  `/private/tmp/push-frontier-clickbench-all-v3-deterministic.nI0gJJ/completed_results.tsv`
- Complete ledger SHA-256:
  `9c4a7ffebd42262a7d6fb4eab01af8d215958089f03f3ef84b6d88917ae45eab`
- Original Q31-Q41 execution/row-count ledger:
  `/private/tmp/push-frontier-clickbench-all-v3-deterministic.nI0gJJ/original_limit_evidence.tsv`
  (SHA-256 `ec4617e3b3a98fa98b2cb7eed389c336d3cb0677a6225e81976f9bfc55cb1578`)
- Original Q17 10-row subset-mismatch ledger:
  `/private/tmp/push-frontier-clickbench-all-v3-deterministic.nI0gJJ/results.tsv`
  (SHA-256 `e59de2ecd5dac686ae84574702bd342fd40d2ecce797043dfc1b1b2ca2c747c3`)
- Q3 parallel-aggregate diagnosis and one-partition proof:
  `/private/tmp/clickbench-q3-deterministic-v3.FFKFEm/results.tsv` records 25/25 exact
  verifications, Float64 bits `0x43c18c52b0d921b0`, and SHA-256
  `8268e7b9a1b15279d7b08afa5edf689e038cb46ee805a7eee1c91dbd25930ddc`.
- Protocol: canonical format v3, explicit `multiset` policy, one worker/partition,
  `VORTEX_USE_SCAN_API` unset, and one fresh V1 write plus push-frontier verify process per
  strict query. This evidence contains no RSS or timing measurements.

Performance and RSS evidence for all ClickBench Q0-Q42 checks:

- Matrix root: `/private/tmp/push-frontier-clickbench-matrix-20260912-ae8c6384`
- Machine-readable ledger: `matrix.jsonl`; SHA-256
  `7f65f2740fd2a17631a39fb7f9ffeae6ae9504705305cc10ba98fde0a7b34358`
- Immutable run manifest: `run-manifest.json`; SHA-256
  `6a378bc0ebe00953973b301f3d70d93d70721c380475ec83a6d32dde5294fc93`
- Derived per-query statistics and explicit flag thresholds: `clickbench-summary.json`; SHA-256
  `a0787bc6870398772938a7e24a87016c76f3e2b7615bc8f012a8872c48115271`
- Input identity: 101 files, 11,616,963,496 bytes, aggregate manifest SHA-256
  `ee8afa12cca731baff218f078e17cedb1fcb5506592e64a17b3117f2274a3581`.
  The benchmark executable SHA-256 is
  `24803a50c3e5be5109c4ed8868dedb605d2ad33aaea3dc933b5abccff50f5351`.
  The clean Git identity is `ae8c638427a6bae88949a65f6492ac4f51761ac8`, with working-tree identity
  SHA-256 `4ec04ed05ae9904a6af02d83aa7147912dfd810bb51c53fd5d40ac435201b864`.
  All input, binary, and Git identities were independently recomputed after the run and matched the
  manifest.
- Protocol: global correctness gate first at one thread/partition, followed by three measured
  samples per backend/query at four threads/partitions. Correctness alone appends
  `queries-file=vortex-bench/sql/clickbench_correctness_queries.sql`; canonical prewarm/timing uses
  only `flavor=partitioned`. The ledger contains one config record, 86 successful correctness
  children, the `all-correctness-succeeded` marker at sequence 86, 258 successful symmetric
  HOT-cache prewarms, and 258 successful measured children. Every measured child has positive
  whole-process peak RSS from macOS `/usr/bin/time -l`; no cache drop was attempted. All 43
  canonical artifacts are retained.

Exact matrix command:

```bash
python3 benchmarks/datafusion-bench/scripts/run_push_frontier_matrix.py clickbench \
  --binary target/release_debug/datafusion-bench \
  --output-dir /private/tmp/push-frontier-clickbench-matrix-20260912-ae8c6384 \
  --input-root vortex-bench/data/clickbench_partitioned/vortex-file-compressed \
  --samples 3 --partitions 4 --correctness-partitions 1 \
  --opt flavor=partitioned \
  --correctness-opt queries-file=vortex-bench/sql/clickbench_correctness_queries.sql
```

Supervisor-observed wall time is the median of three fresh measured processes. RSS columns are the
minimum-maximum whole-process peaks across those processes. Ratios are push-frontier divided by V1.

| Query | V1 median ms | Frontier median ms | F/V time | V1 RSS MiB range | Frontier RSS MiB range | F/V RSS median |
|---|---:|---:|---:|---:|---:|---:|
| Q0 | 48.12 | 47.35 | 0.984 | 59.8-62.7 | 60.8-61.5 | 0.993 |
| Q1 | 80.47 | 80.44 | 1.000 | 74.2-76.5 | 121.9-134.6 | 1.726 |
| Q2 | 137.36 | 140.50 | 1.023 | 119.1-127.4 | 157.5-188.7 | 1.458 |
| Q3 | 136.42 | 148.10 | 1.086 | 141.3-165.0 | 184.5-196.4 | 1.279 |
| Q4 | 722.85 | 709.53 | 0.982 | 1761.1-1800.8 | 1886.1-1904.8 | 1.067 |
| Q5 | 665.71 | 702.34 | 1.055 | 1620.9-1641.4 | 1619.6-1785.4 | 1.088 |
| Q6 | 48.45 | 40.56 | 0.837 | 61.0-62.3 | 60.0-62.7 | 0.994 |
| Q7 | 81.37 | 86.55 | 1.064 | 79.6-80.8 | 130.8-144.8 | 1.696 |
| Q8 | 882.15 | 896.97 | 1.017 | 2018.3-2054.3 | 2084.2-2259.0 | 1.094 |
| Q9 | 1006.36 | 1000.53 | 0.994 | 1248.4-1275.5 | 1339.5-1383.8 | 1.056 |
| Q10 | 196.84 | 189.06 | 0.960 | 305.4-327.9 | 364.5-419.1 | 1.259 |
| Q11 | 310.20 | 246.76 | 0.795 | 331.4-335.1 | 415.3-438.8 | 1.282 |
| Q12 | 580.33 | 547.70 | 0.944 | 1712.5-1800.1 | 1860.1-1879.4 | 1.068 |
| Q13 | 907.36 | 777.23 | 0.857 | 2103.0-2383.8 | 2300.0-2346.5 | 1.068 |
| Q14 | 654.34 | 602.86 | 0.921 | 1589.9-1616.5 | 1744.6-1801.2 | 1.101 |
| Q15 | 813.60 | 812.40 | 0.999 | 2123.2-2259.3 | 2215.4-2326.6 | 1.076 |
| Q16 | 1497.23 | 1492.38 | 0.997 | 4309.2-4448.5 | 4400.1-4632.8 | 1.022 |
| Q17 | 1497.54 | 1459.59 | 0.975 | 4309.1-4382.3 | 4412.0-4661.2 | 1.038 |
| Q18 | 2802.71 | 2750.63 | 0.981 | 8420.6-8645.8 | 8527.3-8791.2 | 1.019 |
| Q19 | 84.15 | 82.42 | 0.979 | 120.0-131.0 | 173.6-183.7 | 1.412 |
| Q20 | 437.96 | 488.70 | 1.116 | 243.4-272.0 | 452.2-524.7 | 2.061 |
| Q21 | 587.28 | 474.07 | 0.807 | 245.3-274.2 | 526.0-659.2 | 2.448 |
| Q22 | 936.84 | 596.30 | 0.637 | 338.7-366.7 | 675.5-702.2 | 1.957 |
| Q23 | 1130.53 | 532.44 | 0.471 | 903.5-966.4 | 878.8-950.1 | 1.001 |
| Q24 | 137.86 | 90.90 | 0.659 | 174.3-188.6 | 205.9-219.5 | 1.191 |
| Q25 | 145.86 | 139.25 | 0.955 | 213.5-227.4 | 279.4-307.5 | 1.405 |
| Q26 | 138.11 | 86.30 | 0.625 | 177.7-190.6 | 205.6-220.3 | 1.124 |
| Q27 | 604.49 | 546.89 | 0.905 | 317.8-341.5 | 480.5-542.1 | 1.595 |
| Q28 | 4885.40 | 4784.63 | 0.979 | 2424.8-2539.0 | 2495.5-2645.6 | 1.055 |
| Q29 | 139.00 | 147.15 | 1.059 | 151.1-153.7 | 323.0-345.5 | 2.260 |
| Q30 | 695.79 | 539.84 | 0.776 | 1087.2-1125.4 | 1424.8-1499.5 | 1.311 |
| Q31 | 592.40 | 424.12 | 0.716 | 1458.7-1548.7 | 1706.0-1720.3 | 1.150 |
| Q32 | 2024.20 | 1984.27 | 0.980 | 9350.3-9967.4 | 9631.6-10011.9 | 1.056 |
| Q33 | 2735.27 | 2747.29 | 1.004 | 9472.1-9784.5 | 9566.5-9729.6 | 1.008 |
| Q34 | 2750.07 | 2697.36 | 0.981 | 9496.3-9708.9 | 9506.3-9816.4 | 0.999 |
| Q35 | 715.77 | 720.58 | 1.007 | 1043.2-1129.8 | 1163.3-1259.7 | 1.092 |
| Q36 | 85.66 | 76.86 | 0.897 | 223.6-225.4 | 229.1-232.1 | 1.025 |
| Q37 | 83.55 | 47.32 | 0.566 | 105.0-109.0 | 110.8-115.4 | 1.071 |
| Q38 | 47.54 | 47.32 | 0.995 | 101.3-111.6 | 119.7-125.1 | 1.225 |
| Q39 | 145.76 | 147.35 | 1.011 | 346.2-359.4 | 356.7-373.5 | 1.037 |
| Q40 | 46.05 | 45.91 | 0.997 | 92.2-96.7 | 87.4-90.4 | 0.949 |
| Q41 | 46.80 | 48.21 | 1.030 | 83.4-84.6 | 87.0-89.2 | 1.054 |
| Q42 | 46.22 | 41.92 | 0.907 | 80.4-81.4 | 85.8-87.9 | 1.079 |

The median frontier/V1 time ratio across queries is 0.980 (range 0.471-1.116); the median RSS ratio
is 1.088 (range 0.949-2.448). Time regressions occur on Q2, Q3, Q5, Q7, Q8, Q20, Q29, Q33, Q35,
Q39, and Q41; only Q20 exceeds 10%. RSS regressions occur on Q1-Q5, Q7-Q33, Q35-Q39, and Q41-Q42;
Q1, Q2, Q3, Q7, Q10, Q11, Q14, Q19-Q22, Q24-Q27, Q29-Q31, and Q38 exceed 10%.

The explicit wall-outlier rule flags frontier Q19, Q24, and Q37 plus V1 Q38; no RSS range exceeds
1.25 times its median. Strict three-sample increases are retained as flags in
`clickbench-summary.json`: wall time for V1 Q2/Q7/Q15/Q28/Q30/Q35/Q38 and frontier
Q2/Q3/Q4/Q8/Q14/Q18/Q21/Q27/Q28/Q30/Q38/Q42; RSS for V1 Q4/Q9/Q19/Q35/Q40 and frontier
Q13/Q25/Q28/Q31/Q34/Q38/Q39. Every sample is a fresh process, so these short sequences are not
cumulative within-process memory growth.

## TPC-H Q1-Q22

Source inventory: `tpch_queries()` maps Q1-Q22 to `q1.sql` through `q22.sql`
(`vortex-bench/src/tpch/mod.rs:25-36`).

| Query | Source | V1 | Frontier | Exact | RSS | Time |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q2 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q3 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q4 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q5 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q6 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q7 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q8 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q9 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q10 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q11 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q12 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q13 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q14 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q15 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q16 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q17 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q18 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q19 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q20 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q21 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q22 | [x] | [x] | [x] | [x] | [x] | [x] |

Evidence for all TPC-H Q1-Q22 V1/Frontier/Exact checks:

- Artifact root: `/private/tmp/push-frontier-tpch-all-v3.MKCyE5`
- Machine-readable command, exit-code, artifact-hash, and status ledger:
  `/private/tmp/push-frontier-tpch-all-v3.MKCyE5/results.tsv`
- Ledger SHA-256: `c6ac88bded84ae6d3d72a8ac301d072ce960b48a2625c886e0c6a1859409cd5c`
- Protocol: canonical format v3, explicit `multiset` policy, `--threads 4`, target partitions 4,
  `VORTEX_USE_SCAN_API` unset, and one fresh V1 write plus push-frontier verify process per query.
  This evidence contains no RSS or timing measurements.

Performance and RSS evidence for all TPC-H Q1-Q22 checks:

- Matrix root: `/private/tmp/push-frontier-tpch-matrix-20260912-10e61356`
- Machine-readable ledger: `matrix.jsonl`; SHA-256
  `67ded13603a9f5bfe40eb8bea3d24ff9daa87b28de8cca986d22f3ae8d5c5759`
- Immutable run manifest: `run-manifest.json`; SHA-256
  `b75ef28ae6511c5cfd57f908dd0c8d1b154e67fe4b3eb342241a932ab2eca9ea`
- Derived per-query statistics and explicit flag thresholds: `tpch-summary.json`; SHA-256
  `8a0925cc02210053b155a366ab245fa608b0bcd16a547634ae3a99f8da3efabf`
- Input identity: 9 files, 275,408,140 bytes, aggregate manifest SHA-256
  `57b06c2b942308afbb77805476e05e31f9029e69685936184d463bbe68f68d6a`.
  The benchmark executable SHA-256 is
  `24803a50c3e5be5109c4ed8868dedb605d2ad33aaea3dc933b5abccff50f5351`.
  The clean Git identity is `10e6135698d31d8fe83639b6239d2b821ed572c8`, with working-tree identity
  SHA-256 `4ec04ed05ae9904a6af02d83aa7147912dfd810bb51c53fd5d40ac435201b864`.
  All input, binary, and Git identities were independently recomputed after the run and matched the
  manifest.
- Protocol: global correctness gate first at one thread/partition, followed by three measured
  samples per backend/query at four threads/partitions. The ledger contains one config record,
  44 successful correctness children, the `all-correctness-succeeded` marker at sequence 44,
  132 successful symmetric HOT-cache prewarms, and 132 successful measured children. Every
  measured child has positive whole-process peak RSS from macOS `/usr/bin/time -l`; no cache drop
  was attempted.

Supervisor-observed wall time is the median of three fresh measured processes. RSS columns are the
minimum-maximum whole-process peaks across those processes. Ratios are push-frontier divided by V1.

| Query | V1 median ms | Frontier median ms | F/V time | V1 RSS MiB range | Frontier RSS MiB range | F/V RSS median |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 144.83 | 131.74 | 0.910 | 224.6-262.4 | 189.7-220.8 | 0.860 |
| Q2 | 48.50 | 46.80 | 0.965 | 94.4-101.3 | 117.5-123.3 | 1.278 |
| Q3 | 84.57 | 88.13 | 1.042 | 195.2-200.4 | 156.2-166.2 | 0.807 |
| Q4 | 46.86 | 46.20 | 0.986 | 98.2-104.5 | 82.6-87.3 | 0.842 |
| Q5 | 82.36 | 87.17 | 1.058 | 308.1-313.3 | 337.0-354.3 | 1.108 |
| Q6 | 40.44 | 48.15 | 1.191 | 74.8-78.3 | 57.9-61.4 | 0.770 |
| Q7 | 85.31 | 81.73 | 0.958 | 280.0-291.5 | 227.6-240.6 | 0.858 |
| Q8 | 82.64 | 81.46 | 0.986 | 216.3-269.3 | 170.1-193.3 | 0.680 |
| Q9 | 90.61 | 89.97 | 0.993 | 403.5-499.7 | 297.9-313.9 | 0.654 |
| Q10 | 82.07 | 85.42 | 1.041 | 170.9-190.1 | 176.1-179.8 | 1.011 |
| Q11 | 47.46 | 48.12 | 1.014 | 76.0-86.2 | 102.1-104.6 | 1.201 |
| Q12 | 46.98 | 45.77 | 0.974 | 120.5-123.7 | 120.5-127.1 | 1.022 |
| Q13 | 85.74 | 82.41 | 0.961 | 94.2-96.7 | 97.0-103.2 | 1.061 |
| Q14 | 45.05 | 48.17 | 1.069 | 106.4-109.8 | 89.2-90.1 | 0.841 |
| Q15 | 46.79 | 44.35 | 0.948 | 117.1-121.9 | 84.3-85.5 | 0.695 |
| Q16 | 46.81 | 41.91 | 0.895 | 102.3-107.8 | 110.3-113.4 | 1.085 |
| Q17 | 146.42 | 138.38 | 0.945 | 219.3-234.0 | 180.6-191.6 | 0.829 |
| Q18 | 190.17 | 138.84 | 0.730 | 478.2-490.2 | 492.6-521.4 | 1.065 |
| Q19 | 82.90 | 41.68 | 0.503 | 91.9-94.0 | 63.0-65.2 | 0.696 |
| Q20 | 83.56 | 79.13 | 0.947 | 176.8-180.5 | 170.5-179.0 | 0.954 |
| Q21 | 143.14 | 142.30 | 0.994 | 206.7-222.4 | 191.8-217.3 | 0.892 |
| Q22 | 40.75 | 47.54 | 1.167 | 63.6-69.5 | 76.6-77.9 | 1.144 |

The median frontier/V1 time ratio across queries is 0.980 (range 0.503-1.191); the median RSS ratio
is 0.876 (range 0.654-1.278). Time regressions occur on Q3, Q5, Q6, Q10, Q11, Q14, and Q22; only Q6
and Q22 exceed 10%. RSS regressions occur on Q2, Q5, Q10-Q13, Q16, Q18, and Q22; Q2, Q5, Q11, and
Q22 exceed 10%. The explicit outlier rule flags only Q9 V1 wall time, whose maximum is 1.566 times
its median; no RSS range exceeds 1.25 times its median. Strict three-sample increases are retained
as flags in `tpch-summary.json`: wall time for frontier Q1, V1 Q2/Q5/Q19, and both backends Q9/Q14;
RSS for frontier Q2/Q5/Q10/Q14 and V1 Q8/Q19. Every sample is a fresh process, so these short
sequences are not cumulative within-process memory growth.

## FineWeb Q0-Q8

Source inventory: nine semicolon-delimited statements are enumerated from Q0 in file order
(`vortex-bench/src/fineweb/mod.rs:54-76`, `vortex-bench/sql/fineweb.sql:1-30`).

| Query | Source | V1 | Frontier | Exact | RSS | Time |
|---|---:|---:|---:|---:|---:|---:|
| Q0 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q1 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q2 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q3 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q4 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q5 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q6 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q7 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q8 | [x] | [x] | [x] | [x] | [x] | [x] |

Evidence for all FineWeb Q0-Q8 V1/Frontier/Exact checks:

- Artifact root: `/private/tmp/push-frontier-fineweb-all-v3.om099h8u`
- Machine-readable provenance, command, exit-code, artifact-size/hash, log-path, safety-RSS, and
  free-space ledger: `/private/tmp/push-frontier-fineweb-all-v3.om099h8u/ledger.jsonl`
- Ledger SHA-256: `ed0378ef9be5a9f8d215802a8310ac380a439f9538857a8e197647164cef8f83`
- Shared Vortex input: `vortex-bench/data/fineweb/vortex-file-compressed/sample.vortex`,
  1,526,591,156 bytes, SHA-256
  `a901d5e999121ababc37bad4c741a35fadb5009b79132548d109f1ccc4b85879`.
- Protocol: canonical format v3, explicit `multiset` policy, `--threads 1`, target partitions 1,
  `VORTEX_USE_SCAN_API` unset, and one fresh V1 write plus push-frontier verify process per query.
  All 18 processes exited zero and all verifier stderr logs are empty. The polled RSS values are
  safety diagnostics only, so `RSS` and `Time` remain unchecked.

Performance and RSS evidence for all FineWeb Q0-Q8 checks:

- Matrix root: `/private/tmp/push-frontier-fineweb-matrix-20260912-568fcb50`
- Machine-readable ledger: `matrix.jsonl`; SHA-256
  `fd2f283cc4c57535173cbd3af944c18087a43e6403cebefc9ceb51d8985272ec`
- Immutable run manifest: `run-manifest.json`; SHA-256
  `b853a981656afa28eb0b6cc9a2d4d95e4e66dc020a138b347a02ee52edc34b6d`
- Derived per-query statistics and explicit flag thresholds: `fineweb-summary.json`; SHA-256
  `4a9b7a4c5a15ec107720194bc814cc8415ed5b33a9ecb2953f85dadf62b206dd`
- Input identity: 1 file, 1,526,591,156 bytes, aggregate manifest SHA-256
  `9396b928e4ae3f77c575c34354c8135a1e781913003fe2983339a1f09edcc067`.
  The benchmark executable SHA-256 is
  `24803a50c3e5be5109c4ed8868dedb605d2ad33aaea3dc933b5abccff50f5351`.
  The clean Git identity is `568fcb50531d1b634811d7fa9c8fb308bd6dc99c`, with working-tree identity
  SHA-256 `4ec04ed05ae9904a6af02d83aa7147912dfd810bb51c53fd5d40ac435201b864`.
  All input, binary, and Git identities were independently recomputed after the run and matched the
  manifest.
- Protocol: global correctness gate first at one thread/partition, followed by three measured
  samples per backend/query at four threads/partitions. Neither policy uses benchmark options. The
  ledger contains one config record, 18 successful correctness children, the
  `all-correctness-succeeded` marker at sequence 18, 54 successful symmetric HOT-cache prewarms,
  and 54 successful measured children. Every measured child has positive whole-process peak RSS
  from macOS `/usr/bin/time -l`; no cache drop was attempted. All 9 canonical artifacts are
  retained. Free space remained above the 4 GiB guard.

Exact matrix command:

```bash
python3 benchmarks/datafusion-bench/scripts/run_push_frontier_matrix.py fineweb \
  --binary target/release_debug/datafusion-bench \
  --output-dir /private/tmp/push-frontier-fineweb-matrix-20260912-568fcb50 \
  --input-root vortex-bench/data/fineweb/vortex-file-compressed \
  --samples 3 --partitions 4 --correctness-partitions 1
```

Supervisor-observed wall time is the median of three fresh measured processes. RSS columns are the
minimum-maximum whole-process peaks across those processes. Ratios are push-frontier divided by V1.

| Query | V1 median ms | Frontier median ms | F/V time | V1 RSS MiB range | Frontier RSS MiB range | F/V RSS median |
|---|---:|---:|---:|---:|---:|---:|
| Q0 | 48.00 | 48.29 | 1.006 | 50.8-55.1 | 71.5-77.2 | 1.365 |
| Q1 | 79.43 | 76.31 | 0.961 | 378.1-410.2 | 382.5-417.0 | 1.012 |
| Q2 | 86.99 | 81.02 | 0.931 | 403.8-451.6 | 431.4-435.7 | 1.044 |
| Q3 | 144.80 | 142.24 | 0.982 | 928.4-935.4 | 932.5-957.2 | 1.006 |
| Q4 | 320.72 | 322.56 | 1.006 | 1230.2-1297.4 | 1515.3-1650.5 | 1.227 |
| Q5 | 267.77 | 267.22 | 0.998 | 1210.0-1285.4 | 1330.4-1364.6 | 1.092 |
| Q6 | 152.95 | 150.85 | 0.986 | 871.2-990.7 | 869.0-915.3 | 0.930 |
| Q7 | 147.82 | 147.94 | 1.001 | 861.8-962.4 | 831.5-913.2 | 0.958 |
| Q8 | 48.46 | 48.09 | 0.992 | 115.8-119.0 | 55.2-57.6 | 0.485 |

The median frontier/V1 time ratio across queries is 0.992 (range 0.931-1.006); the median RSS ratio
is 1.012 (range 0.485-1.365). Time regressions occur on Q0, Q4, and Q7, none exceeding 10%. RSS
regressions occur on Q0-Q5; Q0 and Q4 exceed 10%. Neither wall-time maximum exceeds 1.5 times its
median and no RSS maximum exceeds 1.25 times its median. Strict three-sample increases are retained
as flags in `fineweb-summary.json`: wall time for frontier Q1, V1 Q6, and both backends Q7; RSS for
both backends Q0, frontier Q1, and V1 Q6/Q7. Every sample is a fresh process, so these short
sequences are not cumulative within-process memory growth.

## TPC-DS Q01-Q99

Source inventory: `tpcds_queries()` maps Q01-Q99 to `01.sql` through `99.sql`
(`vortex-bench/src/tpcds/mod.rs:13-24`).

| Query | Source | V1 | Frontier | Exact | RSS | Time |
|---|---:|---:|---:|---:|---:|---:|
| Q01 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q02 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q03 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q04 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q05 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q06 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q07 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q08 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q09 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q10 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q11 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q12 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q13 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q14 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q15 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q16 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q17 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q18 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q19 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q20 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q21 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q22 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q23 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q24 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q25 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q26 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q27 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q28 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q29 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q30 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q31 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q32 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q33 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q34 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q35 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q36 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q37 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q38 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q39 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q40 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q41 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q42 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q43 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q44 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q45 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q46 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q47 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q48 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q49 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q50 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q51 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q52 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q53 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q54 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q55 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q56 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q57 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q58 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q59 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q60 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q61 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q62 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q63 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q64 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q65 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q66 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q67 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q68 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q69 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q70 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q71 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q72 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q73 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q74 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q75 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q76 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q77 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q78 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q79 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q80 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q81 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q82 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q83 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q84 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q85 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q86 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q87 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q88 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q89 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q90 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q91 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q92 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q93 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q94 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q95 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q96 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q97 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q98 | [x] | [x] | [x] | [x] | [x] | [x] |
| Q99 | [x] | [x] | [x] | [x] | [x] | [x] |

Evidence for all TPC-DS SF1 Q01-Q99 V1/Frontier/Exact checks:

- Artifact root: `/private/tmp/tpcds-correctness-v3.Ifl0uQ`
- Per-process argv, environment, exit code, artifact size/hash, and stdout/stderr log provenance:
  `/private/tmp/tpcds-correctness-v3.Ifl0uQ/ledger.jsonl` (SHA-256
  `b94454caa4eb099ae02ebb85dea7f779cb67f134bb3fbe5684698e3a41c8ccd7`).
- Shared 24-file Vortex input manifest SHA-256:
  `42f21e3acb1bd3e92944583b0949a56e1db5cec8584939f1b98a908a2597a3e0`.
- Fixed binary SHA-256: `24803a50c3e5be5109c4ed8868dedb605d2ad33aaea3dc933b5abccff50f5351`.
- Protocol: canonical artifact format v3, explicit `multiset`, `--threads 1`, target
  partitions 1, `VORTEX_USE_SCAN_API` unset, and one fresh V1 write plus push-frontier verify
  process per query. All 198 processes exited zero. The 99 retained artifacts total 3,117,059
  bytes; their aggregate manifest SHA-256 is
  `3b5add394b0861e276b731d3d0d9e059777ff6079b7dc0af996091e9288eb4e5`.
  That correctness-only run collected no RSS or timing measurements; the boxes below are supported
  by the separate performance run recorded next.

Performance and peak-RSS evidence for all TPC-DS SF1 Q01-Q99:

- Matrix root: `/private/tmp/push-frontier-tpcds-matrix-20260912-4bae7220`
- Machine-readable ledger: `matrix.jsonl`; SHA-256
  `3c1e86d66b88f138c4fd3eb633d68767c73c53823d1969e9d2828e536c745c07`
- Immutable run manifest: `run-manifest.json`; SHA-256
  `531faab48bd32f559529e25ec22d5f4f3917c4c4af869a587b6097f74d598809`
- Derived per-query statistics and explicit flag thresholds: `tpcds-summary.json`; SHA-256
  `9b48df05f548943cd5ac8996f3ef8e7eaf6d71ff4375a4d737302cb487cb8ffb`
- Input identity: 24 files, 278,983,760 bytes, aggregate manifest SHA-256
  `5e7fac562423ccc6167cfa517e143dd884741385999d7245c48dbc1e57670e9b`.
  The benchmark executable SHA-256 is
  `24803a50c3e5be5109c4ed8868dedb605d2ad33aaea3dc933b5abccff50f5351`.
  The clean Git identity is `4bae72209c09ea9b4bc92c038a58df8b078ea7f8`, with working-tree identity
  SHA-256 `4ec04ed05ae9904a6af02d83aa7147912dfd810bb51c53fd5d40ac435201b864`.
  All input, binary, and Git identities were independently recomputed after the run and matched the
  manifest.
- Protocol: global correctness gate first at one thread/partition, followed by three measured
  samples per backend/query at four threads/partitions. The ledger contains one config record,
  198 successful correctness children, the `all-correctness-succeeded` marker at sequence 198,
  594 successful symmetric HOT-cache prewarms, and 594 successful measured children. Every
  measured child has positive whole-process peak RSS from macOS `/usr/bin/time -l`; no cache drop
  was attempted. All 99 canonical artifacts are retained. Free space remained above the 4 GiB
  guard.

Exact matrix command:

```bash
python3 benchmarks/datafusion-bench/scripts/run_push_frontier_matrix.py tpcds \
  --binary target/release_debug/datafusion-bench \
  --output-dir /private/tmp/push-frontier-tpcds-matrix-20260912-4bae7220 \
  --input-root vortex-bench/data/tpcds/1.0/vortex-file-compressed \
  --samples 3 --partitions 4 --correctness-partitions 1 \
  --opt scale-factor=1.0
```

Supervisor-observed wall time is the median of three fresh measured processes. RSS columns are the
minimum-maximum whole-process peaks across those processes. Ratios are push-frontier divided by V1.

| Query | V1 median ms | Frontier median ms | F/V time | V1 RSS MiB range | Frontier RSS MiB range | F/V RSS median |
|---|---:|---:|---:|---:|---:|---:|
| Q01 | 43.66 | 43.94 | 1.006 | 89.4-93.0 | 96.7-101.0 | 1.086 |
| Q02 | 82.42 | 85.74 | 1.040 | 105.8-117.8 | 120.7-126.0 | 1.094 |
| Q03 | 45.96 | 48.43 | 1.054 | 97.0-99.9 | 94.9-109.1 | 1.024 |
| Q04 | 322.53 | 315.69 | 0.979 | 459.9-489.4 | 439.5-456.3 | 0.936 |
| Q05 | 84.77 | 86.07 | 1.015 | 154.6-164.2 | 194.2-227.8 | 1.359 |
| Q06 | 83.99 | 82.35 | 0.980 | 97.3-102.0 | 101.2-109.3 | 1.036 |
| Q07 | 83.21 | 85.82 | 1.031 | 332.8-345.1 | 299.7-318.7 | 0.888 |
| Q08 | 74.55 | 73.82 | 0.990 | 112.0-114.8 | 105.6-113.7 | 1.000 |
| Q09 | 81.42 | 78.05 | 0.959 | 136.3-146.2 | 195.9-211.0 | 1.476 |
| Q10 | 81.70 | 89.92 | 1.101 | 128.8-144.3 | 157.1-170.2 | 1.220 |
| Q11 | 191.04 | 202.03 | 1.058 | 323.9-337.4 | 326.7-336.0 | 0.991 |
| Q12 | 48.44 | 44.52 | 0.919 | 92.6-94.5 | 97.4-104.0 | 1.104 |
| Q13 | 82.08 | 83.01 | 1.011 | 129.0-139.5 | 141.1-146.4 | 1.056 |
| Q14 | 263.38 | 250.17 | 0.950 | 404.1-423.0 | 426.9-435.2 | 1.035 |
| Q15 | 83.36 | 85.44 | 1.025 | 118.3-120.0 | 123.3-125.3 | 1.051 |
| Q16 | 80.04 | 87.10 | 1.088 | 95.5-102.9 | 123.5-130.3 | 1.230 |
| Q17 | 140.09 | 131.45 | 0.938 | 335.3-338.3 | 329.8-335.2 | 0.984 |
| Q18 | 90.00 | 84.90 | 0.943 | 246.8-255.0 | 250.9-278.0 | 1.027 |
| Q19 | 85.41 | 81.90 | 0.959 | 120.8-126.8 | 106.1-113.4 | 0.874 |
| Q20 | 77.68 | 48.15 | 0.620 | 102.3-111.1 | 107.6-110.6 | 0.977 |
| Q21 | 82.77 | 85.75 | 1.036 | 177.4-203.5 | 115.6-121.1 | 0.650 |
| Q22 | 199.25 | 202.93 | 1.018 | 160.6-170.3 | 169.4-175.0 | 1.008 |
| Q23 | 257.23 | 257.91 | 1.003 | 555.1-578.3 | 536.3-552.4 | 0.961 |
| Q24 | 140.26 | 132.25 | 0.943 | 323.4-341.3 | 316.0-329.1 | 0.990 |
| Q25 | 139.93 | 141.46 | 1.011 | 380.8-384.4 | 368.6-390.9 | 0.994 |
| Q26 | 84.66 | 82.52 | 0.975 | 207.9-210.0 | 208.5-217.8 | 1.040 |
| Q27 | 195.97 | 198.90 | 1.015 | 865.5-880.1 | 704.2-758.5 | 0.838 |
| Q28 | 87.44 | 87.60 | 1.002 | 159.0-162.9 | 176.3-184.8 | 1.135 |
| Q29 | 140.44 | 148.45 | 1.057 | 331.4-334.0 | 329.6-339.7 | 0.993 |
| Q30 | 79.52 | 89.90 | 1.130 | 94.8-97.9 | 108.0-111.5 | 1.147 |
| Q31 | 136.50 | 141.83 | 1.039 | 142.5-153.0 | 163.0-172.1 | 1.135 |
| Q32 | 48.20 | 48.35 | 1.003 | 81.7-87.2 | 93.0-94.0 | 1.117 |
| Q33 | 87.98 | 89.29 | 1.015 | 116.2-128.8 | 128.8-139.6 | 1.063 |
| Q34 | 82.77 | 87.42 | 1.056 | 105.5-142.7 | 123.5-132.7 | 0.873 |
| Q35 | 81.83 | 82.50 | 1.008 | 113.1-138.5 | 141.4-159.0 | 1.049 |
| Q36 | 142.07 | 89.71 | 0.631 | 201.0-224.0 | 172.9-180.0 | 0.851 |
| Q37 | 86.27 | 84.69 | 0.982 | 106.8-110.9 | 86.7-93.0 | 0.841 |
| Q38 | 84.35 | 88.74 | 1.052 | 111.4-114.3 | 124.0-127.1 | 1.131 |
| Q39 | 142.31 | 136.80 | 0.961 | 220.6-235.2 | 158.8-166.0 | 0.690 |
| Q40 | 81.02 | 84.08 | 1.038 | 146.1-151.2 | 155.0-165.4 | 1.077 |
| Q41 | 44.97 | 48.06 | 1.069 | 48.6-48.8 | 49.9-50.0 | 1.028 |
| Q42 | 44.51 | 43.26 | 0.972 | 80.0-95.2 | 82.7-103.1 | 0.926 |
| Q43 | 75.71 | 86.64 | 1.144 | 95.6-109.2 | 93.3-102.1 | 0.992 |
| Q44 | 82.59 | 83.18 | 1.007 | 108.4-118.7 | 109.4-115.6 | 0.980 |
| Q45 | 77.95 | 82.75 | 1.062 | 102.2-103.4 | 109.0-111.7 | 1.076 |
| Q46 | 89.75 | 85.69 | 0.955 | 170.5-191.8 | 172.9-181.5 | 0.924 |
| Q47 | 207.20 | 198.76 | 0.959 | 279.2-301.0 | 271.9-289.0 | 0.999 |
| Q48 | 84.69 | 83.40 | 0.985 | 121.2-140.9 | 133.7-137.6 | 0.991 |
| Q49 | 83.60 | 78.84 | 0.943 | 147.3-154.0 | 164.4-168.6 | 1.100 |
| Q50 | 82.80 | 81.33 | 0.982 | 209.1-217.4 | 215.8-223.4 | 1.043 |
| Q51 | 200.16 | 200.73 | 1.003 | 198.0-211.2 | 199.8-209.4 | 1.017 |
| Q52 | 44.70 | 45.43 | 1.016 | 93.2-106.7 | 83.2-90.0 | 0.855 |
| Q53 | 80.56 | 88.79 | 1.102 | 99.9-108.9 | 97.6-115.8 | 0.964 |
| Q54 | 82.11 | 84.96 | 1.035 | 111.6-128.7 | 114.5-126.2 | 0.992 |
| Q55 | 47.19 | 47.94 | 1.016 | 93.1-100.3 | 80.9-85.4 | 0.848 |
| Q56 | 84.53 | 82.19 | 0.972 | 124.5-127.1 | 131.2-137.7 | 1.058 |
| Q57 | 144.32 | 144.18 | 0.999 | 168.2-176.0 | 182.0-190.4 | 1.098 |
| Q58 | 84.55 | 82.86 | 0.980 | 125.0-130.6 | 116.2-146.7 | 1.008 |
| Q59 | 149.26 | 148.18 | 0.993 | 146.1-154.0 | 130.9-158.3 | 0.916 |
| Q60 | 84.51 | 90.30 | 1.069 | 120.0-155.1 | 130.3-144.6 | 1.126 |
| Q61 | 83.84 | 78.70 | 0.939 | 150.8-163.3 | 153.8-172.6 | 1.011 |
| Q62 | 46.25 | 48.16 | 1.041 | 87.7-88.4 | 94.5-98.8 | 1.109 |
| Q63 | 46.67 | 47.18 | 1.011 | 99.0-116.4 | 95.6-113.9 | 0.841 |
| Q64 | 307.34 | 311.01 | 1.012 | 625.4-676.0 | 651.7-672.5 | 1.019 |
| Q65 | 82.21 | 87.24 | 1.061 | 175.2-188.4 | 170.4-179.0 | 0.942 |
| Q66 | 141.60 | 86.17 | 0.609 | 150.4-167.1 | 186.1-192.8 | 1.202 |
| Q67 | 263.42 | 253.06 | 0.961 | 521.9-546.9 | 514.1-520.3 | 0.954 |
| Q68 | 89.82 | 90.76 | 1.010 | 172.2-200.9 | 171.9-187.0 | 0.899 |
| Q69 | 84.05 | 86.57 | 1.030 | 103.4-126.2 | 146.3-151.5 | 1.259 |
| Q70 | 143.50 | 144.42 | 1.006 | 204.7-222.6 | 211.1-215.2 | 0.979 |
| Q71 | 81.23 | 84.55 | 1.041 | 118.4-132.7 | 128.7-137.7 | 1.081 |
| Q72 | 6573.67 | 6488.62 | 0.987 | 468.4-473.5 | 568.5-575.5 | 1.214 |
| Q73 | 83.14 | 82.33 | 0.990 | 106.3-119.5 | 118.2-128.8 | 1.056 |
| Q74 | 149.64 | 139.22 | 0.930 | 200.3-210.1 | 200.7-207.8 | 0.984 |
| Q75 | 138.82 | 142.42 | 1.026 | 217.9-230.2 | 227.5-262.9 | 1.050 |
| Q76 | 47.91 | 82.47 | 1.721 | 105.6-108.9 | 106.1-110.8 | 1.017 |
| Q77 | 90.06 | 85.03 | 0.944 | 122.8-149.7 | 143.0-170.4 | 1.205 |
| Q78 | 192.84 | 202.69 | 1.051 | 419.8-426.5 | 403.5-417.5 | 0.953 |
| Q79 | 88.30 | 89.79 | 1.017 | 159.7-208.6 | 159.8-175.2 | 0.948 |
| Q80 | 141.87 | 139.17 | 0.981 | 490.3-512.5 | 450.1-470.0 | 0.910 |
| Q81 | 74.85 | 48.33 | 0.646 | 94.2-99.8 | 102.0-107.5 | 1.117 |
| Q82 | 76.68 | 86.21 | 1.124 | 124.3-128.4 | 135.3-146.1 | 1.120 |
| Q83 | 82.63 | 76.72 | 0.928 | 84.7-86.4 | 85.3-88.0 | 1.018 |
| Q84 | 46.95 | 46.00 | 0.980 | 63.4-63.8 | 67.8-69.6 | 1.090 |
| Q85 | 83.89 | 82.37 | 0.982 | 156.2-158.9 | 197.9-204.6 | 1.271 |
| Q86 | 41.85 | 45.30 | 1.082 | 76.7-79.9 | 81.1-84.3 | 1.050 |
| Q87 | 81.56 | 84.04 | 1.030 | 107.7-113.0 | 131.1-133.3 | 1.198 |
| Q88 | 147.61 | 141.31 | 0.957 | 136.3-143.6 | 137.0-151.1 | 1.003 |
| Q89 | 81.95 | 82.29 | 1.004 | 111.8-120.5 | 122.0-127.7 | 1.104 |
| Q90 | 47.87 | 44.46 | 0.929 | 66.1-68.1 | 78.8-80.9 | 1.167 |
| Q91 | 45.08 | 47.85 | 1.061 | 62.5-65.6 | 74.4-77.1 | 1.150 |
| Q92 | 47.02 | 40.85 | 0.869 | 75.5-77.8 | 98.1-105.2 | 1.277 |
| Q93 | 73.75 | 82.89 | 1.124 | 223.0-226.9 | 217.3-224.2 | 0.983 |
| Q94 | 48.12 | 73.57 | 1.529 | 87.2-94.8 | 115.4-118.4 | 1.271 |
| Q95 | 140.50 | 142.16 | 1.012 | 135.0-137.1 | 150.9-160.9 | 1.115 |
| Q96 | 82.34 | 47.83 | 0.581 | 75.2-84.7 | 79.3-85.1 | 1.011 |
| Q97 | 85.63 | 86.33 | 1.008 | 150.1-166.9 | 163.7-174.6 | 1.094 |
| Q98 | 90.64 | 82.22 | 0.907 | 134.8-151.3 | 133.3-140.7 | 0.996 |
| Q99 | 84.23 | 82.42 | 0.979 | 89.6-99.5 | 100.8-110.0 | 1.153 |

The median frontier/V1 time ratio across queries is 1.006 (range 0.581-1.721); the median RSS ratio
is 1.027 (range 0.650-1.476). Time regressions occur on Q01-Q03, Q05, Q07, Q10-Q11, Q13, Q15-Q16,
Q21-Q23, Q25, Q27-Q35, Q38, Q40-Q41, Q43-Q45, Q51-Q55, Q60, Q62-Q65, Q68-Q71, Q75-Q76,
Q78-Q79, Q82, Q86-Q87, Q89, Q91, Q93-Q95, and Q97; Q10, Q30, Q43, Q53, Q76, Q82, Q93, and
Q94 exceed 10%. RSS regressions occur on Q01-Q03, Q05-Q06, Q08-Q10, Q12-Q16, Q18, Q22, Q26,
Q28, Q30-Q33, Q35, Q38, Q40-Q41, Q45, Q49-Q51, Q56-Q58, Q60-Q62, Q64, Q66, Q69, Q71-Q73,
Q75-Q77, Q81-Q92, Q94-Q97, and Q99. Q05, Q09-Q10, Q12, Q16, Q28, Q30-Q32, Q38, Q49, Q60,
Q62, Q66, Q69, Q72, Q77, Q81-Q82, Q85, Q87, Q89-Q92, Q94-Q95, and Q99 exceed 10%.

The explicit wall-outlier rule flags V1 Q03/Q76 and frontier Q03/Q32/Q36/Q52/Q62/Q66/Q81/Q96;
the RSS-outlier rule flags only V1 Q60. Strict three-sample increases are retained as flags in
`tpcds-summary.json`: wall time for V1 Q01/Q08/Q23/Q45/Q54/Q57/Q76/Q80/Q85/Q93 and frontier
Q08/Q10/Q17-Q19/Q25/Q35-Q36/Q49-Q50/Q54/Q61-Q62/Q65/Q72/Q75-Q77/Q80/Q82-Q83/Q97-Q98;
RSS for V1 Q07/Q13-Q14/Q20/Q29/Q31/Q41-Q42/Q46/Q54/Q57/Q61-Q62/Q71-Q74/Q77/Q80/Q82/Q87/
Q97-Q98 and frontier Q14/Q19/Q27/Q32/Q37/Q42/Q45/Q53/Q58/Q64/Q71/Q74/Q77-Q78/Q80-Q81/Q88/
Q92/Q97. Every sample is a fresh process, so these short sequences are not cumulative
within-process memory growth.

## Completion gate

- [ ] The public benchmark emits backend identity and immutable input-file fingerprints.
- [x] `--threads N` reaches DataFusion target partitions and Tokio runtime workers; both scan
  builders use a per-file factor of `1`, and push/frontier retain one executor worker per file.
- [ ] The public correctness mode is validated on all result dtypes and all matrix queries.
- [ ] Peak RSS is sampled or obtained as a true process high-water mark and reset per process.
- [ ] One cache protocol is implemented, verified, and applied symmetrically.
- [ ] FineWeb data is prepared and layout provenance recorded.
- [ ] TPC-DS SF1 data is prepared and layout provenance recorded.
- [ ] Every query row above is fully checked with artifact paths recorded in the commit/PR report.
