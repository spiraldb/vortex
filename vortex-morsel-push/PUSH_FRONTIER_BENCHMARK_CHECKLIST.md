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
| Q0 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q1 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q2 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q3 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q4 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q5 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q6 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q7 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q8 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q9 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q10 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q11 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q12 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q13 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q14 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q15 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q16 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q17† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q18 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q19 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q20 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q21 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q22 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q23 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q24 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q25 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q26 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q27 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q28 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q29 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q30 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q31† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q32† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q33† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q34† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q35† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q36† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q37† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q38† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q39† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q40† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q41† | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q42 | [x] | [x] | [x] | [x] | [ ] | [ ] |

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
| Q0 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q1 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q2 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q3 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q4 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q5 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q6 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q7 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q8 | [x] | [x] | [x] | [x] | [ ] | [ ] |

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

## TPC-DS Q01-Q99

Source inventory: `tpcds_queries()` maps Q01-Q99 to `01.sql` through `99.sql`
(`vortex-bench/src/tpcds/mod.rs:13-24`).

| Query | Source | V1 | Frontier | Exact | RSS | Time |
|---|---:|---:|---:|---:|---:|---:|
| Q01 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q02 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q03 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q04 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q05 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q06 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q07 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q08 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q09 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q10 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q11 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q12 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q13 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q14 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q15 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q16 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q17 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q18 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q19 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q20 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q21 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q22 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q23 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q24 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q25 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q26 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q27 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q28 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q29 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q30 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q31 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q32 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q33 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q34 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q35 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q36 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q37 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q38 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q39 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q40 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q41 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q42 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q43 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q44 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q45 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q46 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q47 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q48 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q49 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q50 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q51 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q52 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q53 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q54 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q55 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q56 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q57 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q58 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q59 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q60 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q61 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q62 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q63 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q64 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q65 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q66 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q67 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q68 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q69 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q70 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q71 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q72 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q73 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q74 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q75 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q76 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q77 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q78 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q79 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q80 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q81 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q82 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q83 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q84 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q85 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q86 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q87 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q88 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q89 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q90 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q91 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q92 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q93 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q94 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q95 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q96 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q97 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q98 | [x] | [x] | [x] | [x] | [ ] | [ ] |
| Q99 | [x] | [x] | [x] | [x] | [ ] | [ ] |

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
  No RSS or timing measurements were collected, so those boxes remain unchecked.

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
