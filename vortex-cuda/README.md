# vortex-cuda

`vortex-cuda` provides CUDA execution and Arrow C Device export support for Vortex arrays.

## Arrow Device export

Key files:

- `vortex-cuda/src/arrow/mod.rs`: Arrow C Device ABI, export traits, and lifetime management.
- `vortex-cuda/src/arrow/canonical.rs`: canonical-array export to `ArrowDeviceArray`.
- `vortex-test/e2e-cuda/src/lib.rs`: cuDF interop harness.

## CUDA file scans

Local pooled file reads use fixed 8 MiB chunks with concurrency 32. These are physical
file-read/H2D chunks, independent of logical row ranges or scan batch sizes. File transfers
use one session-scoped H2D stream, separate from the execution pool (four streams by default).
A reader keeps its allocation and chunk writes on the same H2D lane; device-buffer events
order consumers on execution streams. Pinned source buffers stay fenced until DMA completion.
Stream pools initialize each slot once, even under concurrent first use, while keeping the
initialized access path lock-free.

## cuDF NDS-H layout comparison

**Recommended NDS-H harness default: clustered, 16Mi-row blocks.** Vortex library defaults
are unchanged. Clustering sorts `lineitem` by `(l_shipdate, l_orderkey)` and `orders` by
`(o_orderdate, o_orderkey)`.

The patched external harness uses these defaults when the variables are unset:

| Environment variable     | Default    | Override                             |
| ------------------------ | ---------- | ------------------------------------ |
| `NDSH_CLUSTER_INPUTS`    | `1`        | `0` retains original row order       |
| `NDSH_VORTEX_BLOCK_ROWS` | `16777216` | Positive physical block size in rows |

Explicit values take precedence; empty or invalid values are rejected.

NVIDIA GH200 480GB · CUDA 13.1 · Vortex `465a9c2fb7` · NDS-H SF10 · buffered I/O.
CPU wall-time means in **milliseconds**, two timed samples per state plus warmup; lower is better.
Block sizes below are physical row counts, not file-read chunk sizes.

### Warm

| Query | Original 16Mi | Original 4Mi | Clustered 4Mi | Clustered 16Mi¹ | Clustered 32Mi |
| ----- | ------------: | -----------: | ------------: | --------------: | -------------: |
| Q1    |        42.539 |       55.533 |        49.148 |          45.221 |         46.209 |
| Q5    |        11.872 |       15.138 |        14.720 |          11.753 |         10.925 |
| Q6    |         6.030 |        9.970 |         4.263 |           3.112 |          4.092 |
| Q9    |        15.465 |       19.152 |        18.880 |          14.898 |         14.097 |
| Q10   |        11.249 |       14.510 |         8.600 |           7.564 |          8.124 |

### Cold

| Query | Original 16Mi | Original 4Mi | Clustered 4Mi | Clustered 16Mi¹ | Clustered 32Mi |
| ----- | ------------: | -----------: | ------------: | --------------: | -------------: |
| Q1    |        90.139 |       95.280 |        81.551 |          82.518 |         90.750 |
| Q5    |        50.273 |       53.560 |        52.466 |          47.912 |         48.393 |
| Q6    |        35.396 |       35.738 |         8.804 |          10.206 |         16.892 |
| Q9    |        57.770 |       62.030 |        62.892 |          59.801 |         58.068 |
| Q10   |        45.306 |       45.371 |        22.182 |          26.032 |         29.165 |

### Summary

Geometric-mean latency change versus original 16Mi; negative is faster. ¹ Recommended default.

| Configuration   | Warm change | Cold change |
| --------------- | ----------: | ----------: |
| Original 4Mi    |     +34.47% |      +4.10% |
| Clustered 4Mi   |      −1.12% |     −34.01% |
| Clustered 16Mi¹ |     −18.86% |     −31.61% |
| Clustered 32Mi  |     −14.89% |     −21.44% |

- Q6/Q10 return empty results. Sorting/writing/setup costs are excluded.
- Cold = evicted OS file pages, not GPU/controller caches. Two samples and a reused
  matched-binary control are directional evidence, not statistical guarantees.
- These environment options belong to the patched external NDS-H harness, not Vortex.
- Commands, patch, and raw evidence: local ignored `target/gpu-writer-experiments-20261020/`
  (`05-followup.md`, `05-harness.patch`, `05-factorial-comparison.json`); not shipped.
  The archived measurement patch used explicit settings; the current harness defaults to clustering.

## Building cuDF for Arrow Device interop

The `cudf-test-harness` repository provides prebuilt cuDF binaries for Arrow Device interop testing on
x86_64 and aarch64.

From the cuDF repository root, compile the Arrow Device interop target locally without exporting additional
environment variables:

```sh
cmake -E rm -rf cpp/build

cmake -S cpp -B cpp/build \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  -DCMAKE_CUDA_ARCHITECTURES=NATIVE \
  -DBUILD_TESTS=ON \
  -DDISABLE_DEPRECATION_WARNINGS=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCUDF_BUILD_STATIC_DEPS=OFF \
  -DCUDF_BUILD_STREAMS_TEST_UTIL=OFF \
  -DCUDAToolkit_ROOT=/usr/local/cuda \
  -DCMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc \
  -DCMAKE_C_COMPILER=gcc \
  -DCMAKE_CXX_COMPILER=g++ \
  # In large AArch64 Debug links, CALL26/JUMP26 relocations can exceed their branch range.
  # Use 1 MiB stub groups so GNU ld emits veneers close enough to each relocation site.
  -DCMAKE_SHARED_LINKER_FLAGS="-Wl,--stub-group-size=1048576" \
  -DCMAKE_EXE_LINKER_FLAGS="-Wl,--stub-group-size=1048576" \
  -GNinja && cmake --build cpp/build --target INTEROP_TEST --parallel
```

## Running the cuDF test harness

```sh
cargo build -p vortex-test-e2e-cuda
cmake --build /path/to/cudf-test-harness/build --target cudf-test-harness --parallel

LD_LIBRARY_PATH=/usr/local/cuda-13.1/compat \
  target/debug/cudf_harness_runner \
  /path/to/cudf-test-harness/build/cudf-test-harness \
  target/debug/libvortex_test_e2e_cuda.so
```
