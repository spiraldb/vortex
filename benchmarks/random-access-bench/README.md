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
`io-uring` (the default on Linux) is Vortex's own thread-per-core runtime, with one worker pinned
to each core, each owning an io_uring it parks on when idle; `tokio` uses the benchmark's Tokio
runtime, which every other format runs on. If the kernel refuses to create a ring, the run falls
back to Tokio with a warning.

`--vortex-reader` picks how Vortex reads local files:

- `io-uring` (the default) has the io_uring runtime's workers perform reads on their own rings.
  Each segment is read on its own by the task that needs it, without a shared driver task or
  coalescing. Reads of 64 KiB or more are spread over every worker's ring, so each core copies
  its share, and while fewer reads are in flight than there are rings, reads of 256 KiB or more
  are also divided into up to one part per 256 KiB, which several rings copy at once. Read buffers are pooled per worker
  and reused. Without the io_uring runtime this falls back to `blocking`.
- `blocking` runs positional reads on the runtime's blocking pool, the reader `open_path` uses.
- `mmap` memory-maps the file, as the Vortex Python bindings open local files. Segments are slices
  of the page cache, so nothing is copied and no read is waited on.

A take selects a few rows, so the scan splits it at the file's stored chunk boundaries: each
stored chunk it touches is read and decoded by one task, rather than by every fixed-size row
chunk that overlaps it.

The files sit in the page cache after the warm-up, so a take mostly copies segment bytes out of
the page cache. Ring reads do that on every core without a thread hop per read, which makes them
faster than blocking reads; mapping the file avoids the copy altogether.

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
