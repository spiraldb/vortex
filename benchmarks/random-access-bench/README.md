# Random Access benchmark

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

## Runtime

Vortex takes run as tasks on the runtime installed on the Vortex session, chosen by `--runtime`:
`io-uring` (the default on Linux) is Vortex's own thread-per-core runtime, with one worker per
core, each owning an io_uring it parks on when idle; `tokio` uses the benchmark's Tokio runtime,
which every other format runs on. If the kernel refuses to create a ring, the run falls back to
Tokio with a warning.

`--vortex-reader` picks how Vortex reads local files. `blocking` (the default) runs positional
reads on the runtime's blocking pool. `io-uring` submits them to the ring of the worker that issues
them. The files sit in the page cache after the warm-up, and there reads that many blocking threads
copy in parallel finish sooner than ring reads, which copy on the submitting worker or hand off to
the kernel's io_uring workers.

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
