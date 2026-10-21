# Random Access benchmark

## Local object-store simulation

`scripts/s3_simulator.py` serves the benchmark data through a read-only S3 HTTP
subset, so Vortex and Lance use their real S3 clients against the same transport.
It supports range GET, HEAD, and LIST, ignores authentication, and binds only to
loopback. Prepare both formats with the same benchmark binary first:

```bash
cargo build -p random-access-bench --profile release_debug --features lance
target/release_debug/random-access-bench --prepare-data \
  --datasets nested-structs --formats vortex,lance
python3 benchmarks/random-access-bench/scripts/s3_simulator.py \
  --root vortex-bench/data --latency-ms 20 --mib-per-second 100 \
  --concurrency 32 --trace /tmp/s3-requests.jsonl
```

In another terminal, run each measurement sequentially:

```bash
AWS_ACCESS_KEY_ID=simulated AWS_SECRET_ACCESS_KEY=simulated \
AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 \
AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true \
VORTEX_BENCH_MATERIALIZE=arrow \
target/release_debug/random-access-bench \
  --remote-data-dir s3://bench/ --datasets nested-structs \
  --formats vortex,lance --patterns correlated,uniform --open-mode reopen \
  --time-limit 10 --display-format gh-json -o /tmp/s3-random.jsonl
```

The simulator charges latency per request, limits active requests, and shares one
bandwidth budget across responses, with a 64 KiB token-bucket burst to absorb
scheduler oversleep. Parameters are experimental assumptions, not
measured S3 specifications. It does not model TLS, connection setup, service-side
throttling, failures, or latency tails. Request traces contain monotonic arrival,
admission, and completion times, byte ranges, and response sizes. Keep cached
and reopen results separate: cached mode warms the same selection repeatedly.

`VORTEX_BENCH_MATERIALIZE=arrow` fully materializes Vortex's nested values as
Arrow, matching Lance's eager output. Without it, the historical benchmark only
canonicalizes Vortex's outer array. For paired measurements of footer opening,
set `VORTEX_BENCH_SIZE_THEN_READ=1` to reproduce the former HEAD followed by a
bounded GET; unset it to use a suffix GET carrying both footer bytes and size.
Keep the materialization setting identical across both runs.

Set `VORTEX_BENCH_SUBSEGMENTS=0` to disable sparse primitive subsegment reads.
`scripts/run_s3_random.py --output <new-directory>` runs baseline, candidate, and
Lance sequentially with alternating order, saving individual runtimes and traces.
Its baseline disables both optimizations. For a separate correctness run,
`VORTEX_BENCH_VERIFY=1` compares every fully materialized Vortex result with a
whole-segment scan. Do not use verification timings as performance measurements.

For a take requiring `N` requests, `D` serial request dependencies, `S` bytes,
request latency `L`, concurrency `C`, and aggregate bandwidth `B`, an optimistic
transport lower bound is
`max(D * L, ceil(N / C) * L, L + max(0, S - 65536) / B)` when `N > 0`.
This excludes CPU time and assumes an idle link at the start. Use the minimum
necessary requests and bytes for a format to estimate its theoretical floor;
using observed traffic only estimates a floor for the current I/O plan. Metadata
dependencies and wasted coalesced bytes must be accounted for separately. Cached
requests with no transport traffic have a zero transport floor.

## V2 sparse-read experiment

The candidate opens unknown-size objects with one suffix GET and propagates sparse
row demand into byte-range reads for verified, nonnullable integer primitive
segments. It probes the array metadata first, falls back to whole reads for other
encodings, and keeps partial reads out of the whole-segment cache. The existing
bounded I/O driver handles these requests without coalescing over omitted gaps.
Compressed float segments still use whole reads.

Measured on `nested-structs`, with approximately 100 selected rows, full Arrow
materialization, 100 MiB/s shared bandwidth, and 32 request slots. Values below
are medians of independent run medians; each run measured for three seconds.
The 20 ms reopen comparison has three repeats; the other comparisons have two.
Both formats use the same simulator and are measured sequentially.

| Mode / latency | Pattern | Baseline Vortex (ms) | Candidate (ms) | Speedup | Lance (ms) |
|---|---|---:|---:|---:|---:|
| Reopen / 20 ms | Correlated | 194.052 | 144.197 | 1.346× | 112.141 |
| Reopen / 20 ms | Uniform | 400.758 | 307.964 | 1.301× | 260.461 |
| Reopen / 50 ms | Correlated | 284.040 | 204.311 | 1.390× | 262.396 |
| Reopen / 50 ms | Uniform | 490.845 | 368.608 | 1.332× | 547.632 |
| Cached / 20 ms | Correlated | 153.579 | 123.836 | 1.240× | 24.318 |
| Cached / 20 ms | Uniform | 360.225 | 287.763 | 1.252× | 171.541 |

The 1.3× target is reached for these reopen workloads, with a small margin for
uniform access at 20 ms; one final repeat was just below 1.300×. It is not reached with cached handles. These are simulator
results on one dataset, not a claim about all S3 workloads. Lance is faster at
20 ms; its additional metadata request dependencies cost more at 50 ms.

For the 20 ms reopen case, transferred bytes fall from 14.07 MB to 10.94 MB
(correlated) and from 35.69 MB to 27.74 MB (uniform). The candidate's fixed-plan
transport floors are approximately 143.1 ms and 303.3 ms, respectively, compared
with 144.2 ms and 308.0 ms measured. To derive these bounds, the footer fits in the
64 KiB burst, the subsequent request latency refills that burst, and data transfer
then costs `max(0, data_bytes - 65536) / B`. Add two request latencies for the
candidate (footer, then data), versus three for the baseline (HEAD, footer, data).
Metadata probes add dependencies but overlap the much larger float transfers in
this workload. This is an optimistic bound for the observed bytes, excluding CPU
and scheduling overhead; it is not the format's absolute theoretical floor.
With arbitrarily fine independent decoding and sufficient concurrency, the
footer-before-data dependency alone gives a 40 ms lower bound at 20 ms latency.
The gap between that bound and 303 ms is primarily excess compressed data read;
closing it needs selective decoding of compressed segments, not just scheduling.

Reproduce the main comparison from the repository root:

```bash
cargo +1.98.0 build -p random-access-bench --profile release_debug --features lance
python3 benchmarks/random-access-bench/scripts/run_s3_random.py \
  --output /tmp/v2-s3-reopen --time-limit 3 --repeats 3
# Separately repeat with --open-mode cached or --latency-ms 50.
```

The runner records the binary SHA-256, simulator source, exact commands,
environment settings, raw iteration runtimes, and HTTP traces. Reference artifacts
for this experiment are under `/home/ec2-user/s3-random/` in
`final-reopen-20ms`, `partial-streamed-cached`, and
`partial-streamed-50ms`. Byte counts and floors come from the preceding
`partial-streamed-20ms-100mib` run with the same I/O plan. Link calibration delivered about 97 MiB/s with one flow
and 100 MiB/s with concurrent flows. Correctness runs compared candidate output
with fully materialized whole-segment output for both patterns and both open
modes. Production benchmark builds succeeded with Rust 1.98.0; the pinned 1.99.1
was unavailable in this environment. Added regression tests were not executed;
formatting, linting, and workspace-wide checks were not run.

## Benchmark overview

Measures point-lookup latency: fetching individual rows by index from a file, rather than
scanning it. This is the workload behind the `Random Access` PR comment.

[Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) is Apache
Arrow's built-in file format, formerly called Feather V2. This suite writes it without
optional buffer compression, so it provides the established constant-time access reference.
Parquet provides the established reference for a compressed columnar representation. Together,
they let the suite compare Vortex and Lance against both ends of the storage trade-off.

Two access patterns are generated with a fixed seed (see [`src/main.rs`](./src/main.rs)):

- **correlated**: several clusters of consecutive indices scattered across the dataset,
  simulating lookups with spatial locality;
- **uniform**: indices drawn from a Poisson process spread uniformly across the dataset,
  simulating lookups with no locality.

Each pattern runs over four datasets (`taxi`, `feature-vectors`, `nested-lists`,
`nested-structs`) in Arrow IPC, Parquet, Lance, and Vortex. Cached mode performs a one-second
untimed warm-up, then reuses the open file handle. Reopen mode includes file open and metadata
work in each timed iteration. CI drives the full matrix via
[`scripts/random-access-split.py`](../../scripts/random-access-split.py).

## Running locally

```bash
cargo run -p random-access-bench --profile release_debug --features lance
```

## Running against S3

The same benchmark can read its data from an object store instead of local disk. The remote
directory must mirror the layout of the local data directory (`vortex-bench/data/`), so the
files are materialized locally first and then uploaded verbatim:

```bash
cargo run -p random-access-bench --profile release_debug --features lance -- \
  --prepare-data --formats parquet,vortex,lance
aws s3 cp --recursive vortex-bench/data s3://my-bucket/my-prefix/

cargo run -p random-access-bench --profile release_debug --features lance -- \
  --remote-data-dir s3://my-bucket/my-prefix/
```

Credentials and region come from the environment (`AWS_REGION`, `AWS_PROFILE`, ...).

Remote measurements are named `...-tokio-s3` instead of `...-tokio-local-disk` and are
reported with `s3` storage, so they form a series separate from the local-disk numbers. Their
ingest records use an `-s3` dataset suffix, such as `taxi-s3/uniform`, for the same reason. Arrow
IPC has no object store reader and is skipped for remote runs. In CI
the variant runs from
[`pr-bench-random-access-s3.yml`](../../.github/workflows/pr-bench-random-access-s3.yml)
(label `action/bench-random-access-s3`) and from the `Random Access (S3)` matrix entry in
`develop-bench.yml`.
