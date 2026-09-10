# vortex-morsel

An experimental morsel-driven scan executor for Vortex layouts — the P1 spine of the design in
`docs/developer-guide/internals/scan-execution-models/morsel-based-plan-execution.md`.

A scan is cut into *morsels* (contiguous root row ranges). Each morsel is driven by a tree of
stateful `Operator` state machines, inline and depth-first, by one affinity-owning worker.
`look_ahead` names reads by registering keyed uses with live row demand through `Cx`. It can
block on dependencies needed to discover further reads. `next` can try a
caller-provided non-blocking probe for a required ticket; on a miss the read is handed out as
required demand and the morsel suspends on that exact ticket until it is completed.

The executor never touches storage. Reads the scheduler wants started leave a `MorselScan` as an
`IoDemand` stream (`MorselScan::take_io`) and are answered through `IoCompletions`. Plan
construction, planning, and execution therefore know nothing about segment sources;
`SegmentSourceDriver` serves the demand from any `SegmentSource` as one task on the caller's
runtime (or a dedicated thread), registering each planned batch together so coalescing sources
see neighbours, polling required and promoted reads eagerly and speculative reads through a
bounded window.

The crate is a prototype and is not part of the public API. It supports flat, chunked, struct,
and dictionary layouts, including nullable structs. Zoned and legacy-statistics layouts are
transparent wrappers; unsupported layouts are build errors rather than silent fallbacks.

Cross-morsel decode reuse comes from **leased shared cells**, not a cache: lease counts are
computed from the morsel cut before the scan starts, the first morsel to decode a unit publishes
it, every retiring morsel releases its lease, and the last release drops the array. No budget, no
eviction policy, nothing outliving the scan; the ledger is asserted to drain to zero. Sharing can
be disabled (`with_share_decodes(false)`), leaving no state across morsels at all — the
state-for-state fairness row against V1.

## Measured

Against the V1 `LayoutReader` on shape-matched workloads (see
`docs/.../morsel-prototype-p1-findings.md` for the full contract and caveats): geomean 0.539 at
equal thread count (0.644 with sharing disabled), 0.249 at four threads with coalesced morsels,
with every configuration validated against V1's output before timing.

## Running the evaluation

```bash
cargo run --release -p vortex-morsel --features _test-harness --bin morsel-eval
```

Set `MORSEL_EVAL_E2E=1` to compare V1 and pull from reader/plan construction through the complete
scan and scan teardown. This includes expression binding, morsel cutting, and look-ahead.
Fixture generation and output validation are outside the timer; worker pools are warmed up.
The fixtures use in-memory segment sources, so these timings do not measure disk or network
latency. The default evaluation without this switch times execution after plan construction.

```bash
MORSEL_EVAL_E2E=1 MORSEL_EVAL_THREADS=4 MORSEL_EVAL_ROWS=1000000 \
  MORSEL_EVAL_ITERATIONS=11 target/release/morsel-eval
```

## Scan observability

Observability is opt-in so timers and tracing events do not affect normal scans or benchmarks. Enable
it on a scan with `MorselScan::with_observability(true)`, then enable the tracing target:

```bash
RUST_LOG=vortex_morsel::scan=info <command>
```

The executor emits one `morsel_scan` span and one completion event per scan. The event includes
plan/flat-node and morsel counts, selected and spanned rows, selection density, planning/execution/
retirement and worker-I/O-wait time sums, read batches/requests/bytes, blocking depth, decodes and
reuse, and output batch count. Compare work counts before timings: equal bytes and decodes with
higher blocked-morsel or per-morsel I/O depth indicates scheduling shape, while higher selected
span or decode counts indicates extra compute work.

For a deterministic record of every morsel, also enable the DEBUG target:

```bash
RUST_LOG=vortex_morsel::scan=info,vortex_morsel::morsel=debug <command>
```

The per-morsel events are emitted in index order after the scan finishes. Each includes its worker,
row range, selection density, output rows, phase and wait timings, poll/block/read-registration
counts, exact segment IDs, and which segments it decoded versus reused. The same records are
available programmatically in `ScanStats::morsel_traces`. Physical request and byte totals stay at
scan scope because concurrent morsels can share one deduplicated read.

The random-access benchmark exposes the switch as
`VORTEX_RANDOM_ACCESS_MORSEL_OBSERVE=1`; combine it with the `RUST_LOG` setting above.

## SSD random-access optimization record

This is the technical handoff for the random-access work added after commit `287d0059`. All
reported runs used buffered I/O on the local NVMe SSD (`/dev/nvme1n1`); benchmark data, Cargo
outputs, and result files were also placed on that SSD. Direct I/O is deliberately not part of
this implementation or the measurements.

### Final execution path

For a take, the benchmark converts the exact row indices into sparse masks over fixed-size
131,072-row morsels. It creates additional balanced cuts by selected-row count only when the
natural cuts would not provide enough runnable morsels for the available workers. Empty masks
retire before building child execution state. Non-empty masks flow through the chunked plan,
which constructs only intersecting children, registers the resulting segment reads as one
planning wave, and then decodes the selected rows.

The important consequence is that the fixed row range is only a scheduling envelope: a sparse
morsel does not scan every row in that range. Its selection mask controls child construction,
lease counts, I/O registration, and decode work.

### Optimizations retained

- **Mask-aware planning.** Range-aware plan construction omits chunks outside the selection.
  Chunked nodes retain original chunk indices, cut only intersecting ranges, and stop an
  all-false demand before child planning. Lease counts include only morsels that touch a range.
  Sparse initial lookahead avoids serial startup, while variable-length lists prefer natural
  layout boundaries so offsets and values stay aligned.
- **Enough parallel work without a worker cap.** The executor uses a process-wide persistent
  worker pool sized from detected hardware parallelism and is bounded only by runnable morsels.
  The previous arbitrary 16-morsel cap is gone. Explicit requests above hardware parallelism use
  a dedicated pool. Fixed-size morsels remain the default; selected-row balancing is only the
  fallback needed to fill otherwise idle workers on small or highly selective inputs.
- **Deferred per-morsel work.** A worker receives the row range and mask first. Execution nodes
  and child state are created only after the mask proves that work is needed. Exact dense plans
  avoid unhelpful parallel setup for very wide node trees, while sparse plans can use all
  available workers.
- **Batched, priority-aware buffered I/O.** `SegmentSource` can register a batch of background
  ranges atomically. Shared and cached sources preserve that batch and deduplicate segment IDs.
  The file driver holds unsubmitted work long enough to coalesce adjacent ranges, submits one
  positional `read_ranges` batch up to available concurrency, and can promote a newly required
  range ahead of speculative work. Local buffered reads use a 64 KiB merge distance, a 2 MiB
  maximum merged request, and 16-way default concurrency.
- **Planning waves instead of crossbeam hand-offs.** Workers publish all reads discovered in the
  same planning wave before the wave becomes eligible for I/O. This gives coalescing visibility
  into the many small requests produced by sparse flat readers. The old crossbeam ready/urgent
  queues and wakeup channels were removed: a worker waiting for its exact I/O ticket had no other
  morsel work to consume through those channels, so the hand-off added synchronization and
  allocation without hiding latency. Required-read promotion now lives in the single I/O
  scheduler.
- **Safe startup-state reuse.** Equivalent reopened Vortex accessors use a bounded 16-entry cache
  keyed by canonical path, format, file length, and modification time. It retains natural split
  metadata, the immutable execution plan, the executor, and fixed-size per-worker arenas. Reusing
  the exact plan `Arc` lets each worker retain its arena. The cache size is a function of cached
  plans and worker state, not scanned rows or query results.
- **No data-result cache between takes.** File handles, scan-local I/O state, ordinary decoded
  segments, output arrays, and dictionary values are not retained by the reopen cache.
  Dictionary values may be shared by morsels during one take, then are dropped with that run.
  This was verified by repeated runs reporting the same dictionary decode/use/read counts.
- **Layout coverage needed by the benchmark.** Dictionary layouts, nullable struct validity,
  and transparent zoned/legacy-statistics wrappers are supported. This prevents random-access
  datasets from falling back to materially different work or rejecting valid plans.
- **Systematic observability.** Opt-in scan and per-morsel records expose selected and spanned
  rows, phase times, worker I/O wait, planning and execution polls, blocking depth, logical
  segment uses, physical requests and bytes, output batches, and decoded versus reused segment
  IDs. File reads and flat decoders also have diagnostic tracing. The instrumentation is disabled
  for normal benchmark timings.
- **Fair benchmark modes.** The harness measures both cached handles and per-take reopen, includes
  a 10x nested-struct dataset, and scales both rows and take indices for that case so selection
  density is preserved. Environment switches allow exact versus sparse cuts, natural versus
  fixed morsels, thread count, partitions, diagnostics, and observability.

Experiments intentionally not retained are direct I/O, the crossbeam I/O queues, the fixed
16-worker/morsel cap, and a cross-run dictionary-value cache. They either regressed the buffered
SSD workload, added overhead without enabling useful work, restricted valid parallelism, or
cached real decoded data across benchmark iterations.

### Final buffered-SSD results

These are clean 5-second reopen measurements in microseconds per take. Lower is better. The
morsel executor wins 10 of 11 cases against V1; the remaining nested-structs uniform case is
10.5% slower.

| Dataset / selection | V1 | Morsel | Change |
|---|---:|---:|---:|
| taxi / default | 897 | 711 | -20.7% |
| taxi / correlated | 1,144 | 849 | -25.8% |
| taxi / uniform | 4,135 | 3,694 | -10.7% |
| feature-vectors / correlated | 905 | 536 | -40.8% |
| feature-vectors / uniform | 2,791 | 1,550 | -44.5% |
| nested-lists / correlated | 643 | 522 | -18.8% |
| nested-lists / uniform | 1,410 | 938 | -33.5% |
| nested-structs / correlated | 830 | 506 | -39.0% |
| nested-structs / uniform | 1,132 | 1,251 | +10.5% |
| nested-structs-10x / correlated | 3,890 | 3,285 | -15.6% |
| nested-structs-10x / uniform | 10,782 | 6,044 | -43.9% |

The largest startup fix is visible in feature-vectors uniform reopen: the original morsel path
took 9,170 us, while plan/executor/arena reuse reduced it to 1,550 us. That is an 83.1% reduction
and 44.5% faster than V1. In cached-handle mode the final morsel path wins 7 of 11 cases; the
reopen result is stronger because V1 still rebuilds more scan execution state per take.

### Reproducing and diagnosing

Build and run the benchmark from an SSD-backed checkout and target directory:

```bash
cargo build -p random-access-bench --profile release_debug --features lance
VORTEX_RANDOM_ACCESS_MORSEL=1 \
  target/release_debug/random-access-bench --formats vortex
```

Unset `VORTEX_RANDOM_ACCESS_MORSEL` for V1. For a work-accounting trace rather than a timing run:

```bash
RUST_LOG=vortex_morsel::scan=info,vortex_morsel::morsel=debug \
VORTEX_RANDOM_ACCESS_MORSEL=1 \
VORTEX_RANDOM_ACCESS_MORSEL_OBSERVE=1 \
  target/release_debug/random-access-bench --formats vortex
```

The retained implementation was checked with the `vortex-morsel` test suite, the random-access
tests, package-scoped nightly formatting, and Clippy with all targets and features for the touched
benchmark and executor crates.
