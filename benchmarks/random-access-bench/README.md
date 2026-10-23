# Random Access benchmark

Measures point-lookup latency: fetching individual rows by index from a file, rather than
scanning it. This is the workload behind the `Random Access` PR comment.

[Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) is Apache
Arrow's built-in file format, formerly called Feather V2. This suite writes it without
optional buffer compression, so it provides the established constant-time access reference.
Parquet provides the established reference for a compressed columnar representation. Together,
they let the suite compare Vortex and Lance against both ends of the storage trade-off.

Each format reads only the rows it is asked for. Parquet turns the requested indices of every
touched row group into a `RowSelection`, so with the page index parquet-rs fetches and decodes
only the pages holding those rows; row groups are read concurrently, and local files go through
`object_store`'s `LocalFileSystem`, as DataFusion reads them.

Two access patterns are generated with a fixed seed (see [`src/main.rs`](./src/main.rs)):

- **correlated**: several clusters of consecutive indices scattered across the dataset,
  simulating lookups with spatial locality;
- **uniform**: indices drawn from a Poisson process spread uniformly across the dataset,
  simulating lookups with no locality.

Each pattern runs over four datasets (`taxi`, `feature-vectors`, `nested-lists`,
`nested-structs`) in Arrow IPC, Parquet, Lance, LitData, and Vortex. Cached mode performs a one-second
untimed warm-up, then reuses the open file handle. Reopen mode includes file open and metadata
work in each timed iteration. Every format is read from a single file through its own
reader: Lance datasets are written in Lance's stable file format as one fragment, and rows are
taken from that fragment directly, opened once like Vortex reuses an open `VortexFile` and
Parquet reuses its parsed footer. CI drives the full matrix via
[`scripts/random-access-split.py`](../../scripts/random-access-split.py).

## Running locally

```bash
cargo run -p random-access-bench --profile release_debug --features lance
```

## LitData

[LitData](https://github.com/Lightning-AI/litdata) is a Python-only format for ML training data,
so the benchmark drives it out of process. It spawns a Python interpreter running
[`scripts/litdata-random-access.py`](../../scripts/litdata-random-access.py). That script converts
the dataset's Parquet file into LitData's optimized chunk format with one item per row. It
checks sampled items against the Parquet rows, then times `StreamingDataset` lookups of the same
indices with the same warm-up and time limit as the other formats. The script records the times
itself, so process spawn, interpreter start and `import litdata` are not counted.

LitData gets the same resources as Vortex. The interpreter is a child of the benchmark binary,
so it inherits the CPU affinity and NUMA memory binding that
[`scripts/bench-taskset.sh`](../../scripts/bench-taskset.sh) applies to it in CI. OS page caches
are dropped before each run for every format.

LitData's defaults target streaming whole chunks into a training loop, and several of them make
point lookups far slower. The script changes these:

| Setting | Default behavior on a point lookup | Cost at the default |
| --- | --- | --- |
| Rows written as tuples, not dicts | A dict of plain values is stored as an Arrow IPC footer of 256-row batches, and each lookup runs `to_pylist` on a whole batch | ~20× on uniform lookups |
| `batch_decode=0` | Decodes the 256 items around the requested one | ~7× on feature vectors |
| `max_pre_download` ≥ chunk count | Keeps 4 chunks mapped, so lookups spread over more chunks unmap and remap one per row | ~8× on feature vectors |
| `LITDATA_POSIX_PAGE_BYTES=0` | Finds a 256KB "page" of neighboring items by walking the offset table in Python | ~15× on small rows |
| `LITDATA_POSIX_WILLNEED=0` | Reads ahead the whole 64MB chunk whenever a new one is mapped | wasted I/O on cold reads |

Lookups stay single-threaded in one process. LitData parallelizes with `DataLoader` worker
processes, and threads contend on the GIL while decoding. Both were slower or no faster for one
100-row lookup, because the per-row cost is a few microseconds and moving rows between processes
costs more. Reopen mode stays expensive: mapping a chunk converts its whole offset table into a
Python list, one entry per row. Smaller chunks only help when the lookups touch few chunks, so
chunks stay at LitData's recommended 64MB.

LitData is not in the default `--formats`. The interpreter comes from `LITDATA_PYTHON`, or
`python3` if that is unset. LitData is only benchmarked from local disk.

```bash
uv venv --python 3.12 .venv-litdata
uv pip install --python .venv-litdata --index-strategy unsafe-best-match \
  --extra-index-url https://download.pytorch.org/whl/cpu litdata==0.2.76 torch pyarrow numpy

LITDATA_PYTHON=.venv-litdata/bin/python \
  cargo run -p random-access-bench --profile release_debug -- --formats parquet,vortex,litdata
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
IPC has no object store reader and LitData is only wired up for local disk, so both are skipped
for remote runs. In CI
the variant runs from
[`pr-bench-random-access-s3.yml`](../../.github/workflows/pr-bench-random-access-s3.yml)
(label `action/bench-random-access-s3`) and from the `Random Access (S3)` matrix entry in
`develop-bench.yml`.

## Local object-store simulation

`scripts/s3_simulator.py` serves the benchmark data through a read-only S3 HTTP
subset, so Vortex, Parquet and Lance use their real S3 clients against the same
transport. It supports range GET (including suffix ranges), HEAD, and LIST,
ignores authentication, and binds only to loopback. Requests are path-style
`/bucket/key` and are served from `--root/key`.

Prepare the data, then start the simulator:

```bash
cargo build -p random-access-bench --profile release_debug --features lance
target/release_debug/random-access-bench --prepare-data \
  --datasets nested-structs --formats vortex,lance
python3 benchmarks/random-access-bench/scripts/s3_simulator.py \
  --root vortex-bench/data --latency-ms 20 --mib-per-second 100 \
  --concurrency 32 --trace /tmp/s3-requests.jsonl
```

In another terminal, point the benchmark at it:

```bash
AWS_ACCESS_KEY_ID=simulated AWS_SECRET_ACCESS_KEY=simulated \
AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 \
AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true \
target/release_debug/random-access-bench \
  --remote-data-dir s3://bench/ --datasets nested-structs \
  --formats vortex,lance --patterns correlated,uniform --open-mode reopen \
  --time-limit 10 --display-format gh-json -o /tmp/s3-random.jsonl
```

The simulator charges a fixed latency per request (including HEAD and LIST),
limits the number of active requests, and shares one bandwidth budget across all
responses, with a 64 KiB burst to absorb scheduler oversleep. Its parameters are
experimental assumptions, not measured S3 specifications. It does not model TLS,
connection setup, service-side throttling, failures, or latency tails.

The `--trace` file has one JSON line per request with its method, key, byte range,
response size, status, and monotonic arrival, admission, and completion times.
Keep cached and reopen results separate: cached mode warms the same selection
repeatedly.

For a take needing `N` requests, `D` serial request dependencies, `S` bytes,
latency `L`, concurrency `C`, and bandwidth `B`, an optimistic transport lower
bound is `max(D * L, ceil(N / C) * L, L + max(0, S - 65536) / B)` when `N > 0`.
It excludes CPU time and assumes an idle link at the start.
