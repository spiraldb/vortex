# Scalar probe benchmarks

Compare unchanged `execute_scalar` with `ArrayProbe::scalar_at` in the same binary. The
[example](../examples/probe.rs) demonstrates both `Once` and `Repeated` access. The
[design note](../../../vortex-array/PROBE_DESIGN.md) describes the caller and vtable APIs.

| Encoding | Work retained across lookups |
|---|---|
| Primitive | Materialized validity, or a child probe for lazy validity at requested rows. |
| FastLanes RLE | Typed readers for materialized primitive slots, slot IDs for other children, and the slice base offset. |
| RunEnd | The context's ends probe across binary-search comparisons and lookups, and its values probe for selected runs, including nullness. |
| PCO | Validity, prefix ranks for non-null positions, page boundaries, and the last decoded page. |

Other encodings use the existing scalar path. Encodings request child probes by slot from
`ProbeCtx`; local state does not store child probes manually. Each child gets its own
context, initialized once and reused recursively.

Construction allocates nothing. `Once` initializes no retained state, though scalar
execution can still allocate decoding buffers. On this ARM64 build, combined contexts
occupy 48 bytes for Primitive, 32 for RunEnd, 128 for PCO, and 136 for RLE. With the
128-byte inline capacity, RLE spills once on first repeated use. The other contexts fit
inline. Requesting a child allocates a slot table; PCO allocates indexes and page buffers
as needed. No child table is allocated for RLE's direct primitive readers.

## Run

```bash
RUSTC_WRAPPER= cargo bench -p vortex-pco --bench probe -- --sample-count 100 --min-time 0.1
RUSTC_WRAPPER= cargo bench -p vortex-pco --bench probe -- '\(1, (false|true), false\)' --sample-count 100 --sample-size 64 --min-time 0.2
RUSTC_WRAPPER= cargo run -p vortex-pco --example probe
```

`RUSTC_WRAPPER=` bypasses this environment's sccache permission failure. No runtime feature
toggles were set. For these measurements, the binary was first built with
`RUSTC_WRAPPER= cargo bench -p vortex-pco --bench probe --no-run`, then invoked directly as
`target/release/deps/probe-944ff408a6b9fe8f --bench` with the corresponding arguments above.

Cases are `(access_count, nullable, scattered)`. The leaf fixture has 16,384 `u32` rows
with values `row / 16`; nullable cases mark every 11th row null. PCO uses compression level
8 and 1,024 non-null values per page. Deterministic random indices are clustered within
256 logical rows starting at 4,096, or scattered over the whole array.

The stacked fixture is `RunEnd(ends=PCO, values=PCO)`: 4,096 runs of length four, covering
16,384 logical rows. Ends are `4, 8, ...`; values are run numbers, with every 11th run null
in nullable cases. Both children use PCO level 8 with 1,024 non-null values per page.

Timings include probe construction, first-use preparation, lookups, and destruction.
Compression, index generation, and execution-context creation are excluded equally.
One-access cases use `Once`; larger groups use `Repeated`. The `repeated_first` cases
measure the first lookup using `Repeated` separately.

## Results

Measured on 2026-09-11, ARM64 macOS, Rust 1.98.0, using the repository's bench profile.
Runs were serial, without concurrent builds or tests. Values are medians per complete
group of accesses. Single-access results use 64 independent probe lifetimes per sample
and are normalized per lookup; larger cases use the first command above.

| Encoding | Accesses | Nullable | Pattern | `execute_scalar` | Probe |
|---|---:|---|---|---:|---:|
| RLE | 1 | no | single | 99.31 ns | 105.8 ns |
| RLE | 1 | yes | single | 179.3 ns | 137.7 ns |
| RLE | 64 | no | clustered | 6.458 µs | 1.374 µs |
| RLE | 64 | no | scattered | 6.374 µs | 1.384 µs |
| RLE | 64 | yes | clustered | 10.95 µs | 1.541 µs |
| RLE | 64 | yes | scattered | 10.29 µs | 1.509 µs |
| RLE | 1,024 | no | clustered | 101 µs | 20.91 µs |
| RLE | 1,024 | no | scattered | 100.9 µs | 20.91 µs |
| RLE | 1,024 | yes | clustered | 168 µs | 21.16 µs |
| RLE | 1,024 | yes | scattered | 164.8 µs | 21.16 µs |
| PCO | 1 | no | single | 5.363 µs | 5.323 µs |
| PCO | 1 | yes | single | 4.404 µs | 4.418 µs |
| PCO | 64 | no | clustered | 330.9 µs | 6.208 µs |
| PCO | 64 | no | scattered | 331.2 µs | 309.1 µs |
| PCO | 64 | yes | clustered | 269.3 µs | 5.708 µs |
| PCO | 64 | yes | scattered | 241.6 µs | 192.6 µs |
| PCO | 1,024 | no | clustered | 5.419 ms | 21.08 µs |
| PCO | 1,024 | no | scattered | 5.412 ms | 4.91 ms |
| PCO | 1,024 | yes | clustered | 4.127 ms | 31.62 µs |
| PCO | 1,024 | yes | scattered | 4.056 ms | 3.161 ms |
| RunEnd(PCO, PCO) | 1 | no | single | 25.97 µs | 25.18 µs |
| RunEnd(PCO, PCO) | 1 | yes | single | 53.68 µs | 53.18 µs |
| RunEnd(PCO, PCO) | 64 | no | clustered | 2.065 ms | 245.5 µs |
| RunEnd(PCO, PCO) | 64 | no | scattered | 2.033 ms | 334.7 µs |
| RunEnd(PCO, PCO) | 64 | yes | clustered | 4.01 ms | 262.7 µs |
| RunEnd(PCO, PCO) | 64 | yes | scattered | 4.044 ms | 446.4 µs |
| RunEnd(PCO, PCO) | 1,024 | no | clustered | 34.26 ms | 3.999 ms |
| RunEnd(PCO, PCO) | 1,024 | no | scattered | 33.22 ms | 5.487 ms |
| RunEnd(PCO, PCO) | 1,024 | yes | clustered | 66.57 ms | 4.12 ms |
| RunEnd(PCO, PCO) | 1,024 | yes | scattered | 64.74 ms | 7.044 ms |

For 1,024 clustered non-nullable reads, this run shows approximately 4.8× faster RLE,
257× faster PCO, and 8.6× faster RunEnd-over-PCO access. RLE avoids scalar dispatch for
materialized primitive slots. PCO amortizes decoding across reads of the same page.
RunEnd preserves its children's preparation and avoids a separate search for nullness.

PCO retains one page. Scattered access often misses that cache, so the improvement is
much smaller: 5.412 ms → 4.91 ms for 1,024 non-nullable reads here. In stacked RunEnd,
the ends search crosses page boundaries and still causes decodes; the values child
retains its own independent page. These local measurements do not establish a general
speedup for arbitrary access patterns or encoding trees.

One-off non-nullable RLE adds about 6.5 ns in the batched measurement. Nullable RLE's
one-off path combines routing and nullness in one index lookup. PCO and stacked RunEnd
one-off timings are close to their baselines; small differences should be treated as
measurement variation rather than a reliable wrapper speedup.

The first `Repeated` lookup, including teardown, measured separately in the main run:

| Encoding | Non-nullable | Nullable |
|---|---:|---:|
| RLE | 65.87 ns | 163.5 ns |
| PCO | 5.124 µs | 4.082 µs |

A deeper regression fixture, `RunEnd(PCO, RunEnd(PCO, PCO))`, uses three single-page PCO
leaves and verifies exactly three state initializations and three decodes across repeated
lookups, with one drop per state. New root probes prepare independent state, and `Once`
initializes none. This checks reuse independently of timing.

Raw local output: `/private/tmp/vortex-probe-impl-bench.log` and
`/private/tmp/vortex-probe-impl-bench-once.log`. Context sizes were measured from the same
source and are implementation details, not ABI guarantees.
