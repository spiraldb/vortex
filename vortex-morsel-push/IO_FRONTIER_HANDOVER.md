# Grouped I/O frontier handover

Status as of 2026-09-13. This is the source of truth for resuming the grouped-I/O experiment in a
fresh task. Read [IO_FRONTIER_IMPLEMENTATION.md](IO_FRONTIER_IMPLEMENTATION.md) for the detailed
process, bugs encountered, and review checklist. Read [IO_FRONTIERS.md](IO_FRONTIERS.md) for the
benchmark history and scheduler experiments.

## Repository state

| Item | Value |
|---|---|
| Repository | `spiraldb/vortex-7` |
| Crate | `vortex-morsel-push` |
| Current branch | `ji/push-frontier-all-bench` |
| Starting comparison point | `f4ca2266da` — `wip(morsel): checkpoint I/O follow-ups` |
| Branch contents | Grouped-I/O implementation, benchmark tooling, bounded DataFusion source-sharing candidate, persistent-file-reader WIP, tests, and documentation |

The branch `ji/morsel-io-followups` points at the starting checkpoint. The branch
`ji/push-frontier-all-bench` checkpoints the complete grouped-I/O implementation and benchmark
stack committed here.

## User intent

The primary goal is a good, small, efficient implementation and API. Scheduler configuration and
benchmark matrices are supporting evidence, not the design center.

The intended model is:

1. Execution nodes define logical I/O groups with `group_begin` and `group_end`.
2. A group contains enough I/O to finish one logical task.
3. Cascade mode has one group per conjunct; parallel mode may group all conjuncts together.
4. Pruning and projection have their own groups.
5. A plan returns a cursor that can enumerate the current group incrementally.
6. The cursor can move down to another row range or right to a later speculative group.
7. I/O completion wakes sources, which push through the existing physical pipelines.
8. Pushed gates, not the frontier cursor, determine when a required CPU task is complete.
9. Retention must be bounded. Raw cells and source futures must be cleared after final use and
   between query runs.

Do not replace this with a materialized range-by-group matrix or make frontier discovery
authoritative for query correctness.

## Final core API

### Execution nodes

Group owners use:

```rust
cx.group_begin(IoGroupKind::Conjunct)?;
// Incrementally prewalk children.
cx.group_end()?;
```

Stored leaves use:

```rust
cx.read(IoKey::Segment(segment_id))?;
```

The methods are implemented by `IoPrewalkCx` in `src/node.rs`.

### Execution plans

The public cursor is:

```rust
let mut frontier = plan.frontier(range);

loop {
    let batch = frontier.next_io(budget)?;
    consume(batch.io());
    if batch.is_complete() {
        break;
    }
}

if frontier.right()? {
    // Enumerate the next group for the same range.
}

frontier.down(next_range);
```

The API is defined in `src/build.rs`:

- `ExecPlan::frontier(range) -> IoFrontierCursor`
- `IoFrontierCursor::next_io(budget) -> IoFrontierBatch<'_>`
- `IoFrontierCursor::right() -> VortexResult<bool>`
- `IoFrontierCursor::down(range)`

`IoFrontierBatch` contains only:

- the `IoGroupKind`;
- a borrowed slice of `IoKey`;
- whether the current group has been fully enumerated.

The batch does not contain source identities, activation targets, source ranges, cells, arrays, or
owned I/O vectors.

### Completion semantics

There are two different meanings of completion. Do not merge them:

| Event | Meaning |
|---|---|
| `IoFrontierBatch::is_complete()` | Every key in the current group has been discovered |
| Pushed gate | I/O, decode, and CPU work produced authoritative coverage for the next task |

`right()` is legal only after discovery of the current group is complete. It starts later keyed
cells early; it does not bypass the pushed gate. When the gate later requires those cells, they are
promoted instead of read again.

## Group placement

| Component | Behavior |
|---|---|
| `IoRootExec` | Emits applicable zoned/statistics keys as the pruning group |
| `ConjunctExec`, cascade | Opens and ends one group around each conjunct |
| `ConjunctExec`, parallel | Opens one group around all conjunct inputs |
| `FilterExec` | Propagates predicate groups, then creates one projection group |
| `ChunkedExec` | Propagates child group boundaries across cuts |
| `StructExec` | Propagates child group boundaries across fields |
| `FlatExec` | Names segment keys with `cx.read(key)`; owns no grouping policy |

The pruning wrapper is prewalk-only. It does not participate in normal pushed execution.

## Efficiency decisions already made

- One cursor owns one mutable prewalk arena and reuses it across `down()` calls.
- `next_io` returns a borrowed slice backed by a reusable `Vec<IoKey>`.
- The vector is cleared without dropping capacity between polls and ranges.
- The cursor stores keys only. Earlier source-origin, source-range, and activation-target fields
  were removed because runtime code did not consume them.
- The redundant range iterator was removed; `down()` is the only range-direction operation.
- Planning and I/O prewalking reuse the same traversal fields in each node. This is safe because
  they always run in separate arena instances.
- No registry lock is taken for a non-final raw-cell release.
- Raw-cell registry maps are sharded by key.
- Cell terminal state and waiter lists share one lock, and waiters are woken after unlocking.
- Background I/O is not duplicated in `Scheduler::io_work`; `IoCell` is authoritative.

## Raw and decoded lifetime

### Raw I/O

The scan computes exact flat-source use counts before admitting I/O. Each overlap between a flat
segment and a morsel contributes one use. `FlatExec::retire` releases all overlaps, including
sources skipped by an all-false predicate.

On the final use:

- the exact registered `IoCell` is removed;
- a ready raw buffer is dropped synchronously;
- a queued or in-flight read is cancelled;
- late completion observes a missing/released cell and drops its result.

The common non-final release decrements an atomic counter through the morsel-local cell reference.
The final release takes only the key's registry shard lock.

The source driver uses abortable futures. `IoDemand::Cancel` drops queued work or aborts an
in-flight future. `IoDemand::Shutdown` ends an owned driver and drops its remaining futures.

`SegmentSourceDriver::connect_on_thread` stores its thread handle in `MorselScan`. Dropping an owned
scan sends shutdown and joins the thread. A subscan using a shared external `IoService` does not
shut down that service; its owner is responsible for doing so.

### Decoded arrays

The existing `SharedCells` implementation shares decoded flat arrays across overlapping morsels
using exact leases. It drops the decoded array after the final overlapping morsel retires.

Not implemented:

- a general typed retain/close API for arbitrary decoded arrays;
- single-flight decoding as a public abstraction;
- dictionary-value sharing and lifetime;
- yielding one blocked decoder while another morsel publishes the shared result.

The user explicitly requested that this general decoded-retention idea remain design-only for now.
Do not implement it without a new decision.

## Important implementation caveats

1. `ExecPlan::frontier` currently instantiates a full execution-shaped arena for the cursor. The
   scheduler reuses it across many ranges, so this is not per-range allocation. If profiles show
   cursor construction is material, consider a dedicated lightweight frontier blueprint; do not
   add one merely for aesthetic reasons.
2. Exact raw lifetime uses one count entry per relevant segment key. Raw bytes are limited to
   admitted cells, but lease metadata is proportional to the plan's unique segment keys. A strict
   active-window metadata bound would require a different per-admission lease-token design.
3. Planning and prewalking share node traversal fields. Calling both modes on the same arena would
   corrupt their cursors. Preserve the separate-arena invariant or split the state again.
4. The latest API simplification happened after the final SF10 benchmark run. It removes metadata
   and allocations and passed all tests/Clippy, but SF10 was not rerun after that cleanup.
5. The large benchmark harness changes are useful evidence but should probably be separated from
   the core API/lifetime changes before review.

## Bugs encountered

The full watchlist is in [IO_FRONTIER_IMPLEMENTATION.md](IO_FRONTIER_IMPLEMENTATION.md). The most
important confirmed failures were:

- Timed benchmarks retained every output array, making memory conclusions invalid.
- Disk benchmarks retained fixture-owned generation buffers after writing the pack.
- "Cold" measurements taken after hot runs were not reliably cold.
- Ready raw cells were retained until scan teardown instead of final use.
- Unused speculative futures survived their final consumer.
- Detached I/O driver threads could survive into the next query run.
- Subscans could incorrectly shut down an externally shared I/O service.
- A refill path could widen the declared admission bound.
- A range was published before all its cells were registered, allowing worker reads to fragment
  the intended coalesced batch.
- Background reads were duplicated in a scheduler map.
- A single global cell registry lock created avoidable contention.
- Frontier batches allocated a new vector on every incremental poll.
- Frontier records duplicated source metadata that execution already owned.
- A range iterator and `next_down` represented the same operation.
- Separate planning and prewalk cursors duplicated state in every node.
- A fixed number of right moves could stop before all conjuncts or cross into projection depending
  on the plan shape.

When changing the implementation, use the categorized checklist in the process document rather
than relying only on this summary.

## Validation state

The latest core implementation and API cleanup passed:

```text
env RUSTC_WRAPPER= cargo test -p vortex-morsel-push --features _test-harness
134 passed; 0 failed

env RUSTC_WRAPPER= cargo clippy \
  -p vortex-morsel-push --all-targets --all-features -- -D warnings
passed

cargo +nightly fmt --all
passed

git diff --check
passed
```

The process and handover documentation were added after the Rust validation. They changed no Rust
behavior; `git diff --check` was rerun after the documentation edits.

Key tests:

- `cascade_frontier_cursor_moves_down_rows_and_right_groups`
- `parallel_frontier_groups_all_conjuncts_and_resumes_in_bits`
- `frontier_scheduler_depths_match_v1`
- `executor_prunes_zones_before_registering_data_io`
- `final_lease_removes_ready_cell_and_drops_retained_bytes`
- `final_lease_cancels_an_outstanding_read`
- `final_use_cancels_and_drops_source_future`
- `empty_filter_cancels_pending_speculative_io`
- `scan_wide_io_cells_deduplicate_straddled_chunks`
- `shared_cells_reuse_straddled_chunks`
- `dropping_stream_cancels_stalled_scan`
- `dropping_stream_cancels_never_ready_io`

## Final SF10 evidence

These measurements predate only the final representation/API simplification described above. All
timed paths discarded output immediately, and correctness runs separately verified exact dtype,
row count, and ordered contents against V1. Every final morsel run ended with zero live raw cells
and zero retained raw bytes.

| Environment | V1 Tokio geometric mean | Grouped push geometric mean | Speedup |
|---|---:|---:|---:|
| Hot cache | 199.983 ms | 134.961 ms | 1.482x |
| Fresh cold | 263.265 ms | 165.869 ms | 1.587x |
| 2 ms physical-read latency | 957.523 ms | 331.481 ms | 2.889x |

Artifacts:

- `/private/tmp/vortex-bounded-final-hot-sf10.log`
- `/private/tmp/vortex-bounded-final-fresh-cold2-sf10.log`
- `/private/tmp/vortex-bounded-final-latency-sf10.log`

The full per-query table and interpretation are at the end of
[IO_FRONTIERS.md](IO_FRONTIERS.md). Earlier sections in that file are explicitly marked historical
where their implementation retained raw cells or widened the admission bound.

## Public DataFusion attribution checkpoint

The 2026-09-12 Q6/Q22 checkpoint uses one frozen instrumented binary and the same SF1 files, SQL,
default fields, filters, physical plans, 14 DataFusion partitions, and 14 runtime workers for V1 and
push-frontier. `VORTEX_USE_SCAN_API` was unset. Each query passed exact public Arrow schema and row
multiset verification; normalized plans are identical. Four fresh HOT pairs per query alternated
backend order, and every measured child immediately followed its own same-query/backend prewarm.

| Query / evidence | V1 | Push-frontier |
|---|---:|---:|
| Q6 scan max partition start/end span | 8.681 ms | 8.656 ms |
| Q6 physical requests / batches | 17-32 / 11-20 | 183 / 183 |
| Q6 physical union / total bytes | 34,564,816-34,564,820 B / 34.96-43.73 MB | 34,570,564 B / 37,830,972 B |
| Q6 RSS median | 98.70 MB | 101.49 MB |
| Q22 three scan spans | 1.448 / 3.649 / 2.424 ms | 2.797 / 5.221 / 4.112 ms |
| Q22 physical requests / batches | 9-14 / 8-13 | 85 / 68 |
| Q22 physical union / total bytes | 5,165,108-5,165,112 B / 10.33-11.44 MB | 5,165,088 B / 20,821,060 B |
| Q22 RSS median | 95.82 MB | 122.81 MB |

`DataSourceExec.elapsed_compute` is not recorded; the scan values are the median maximum
per-partition start/end span. In-flight gauges are per source driver. Histogram minima are not used
because empty partitions contribute zero minima. Q22's union coverage is equal within 24 bytes, so
its extra frontier bytes are overlapping coverage rather than a wider scan.

All measured push-frontier I/O-cell-shard, I/O-cell-state, and frontier-refill contention counters
were zero. Natural-split acquisition-wall wait occurred on both backends and stayed below 2.50 ms
per run; it is an upper bound including descheduling, not lock hold time. Q22's HashJoin and Filter
compute are comparable while all three frontier scan spans are longer. This supports fragmented,
duplicated physical reads as the next mechanism to change and provides no evidence that these push
locks cause the regression.

Timing is diagnostic-only because raw trace cost scales with request count. Whole-process RSS is a
valid bounded high-water mark; instructions, cycles, and context-switch counts are retained but
also include asymmetric trace work.

Evidence root: `/private/tmp/tpch-q6-q22-diagnostics-20260912-RTO222`. `MANIFEST.md` SHA-256 is
`ac3b547534484d7f5da68bb6840055c4a097293ece993c5ed8e4184b981f3ddf`; `SHA256SUMS` SHA-256 is
`d6def0fd4bb5b0ec76b08f8f5be7efce94aba112c5344fc1d394755181198f2f`. The frozen binary SHA-256
is `5562f6cf22bd409f162089a8f03dc12a5103cf5a3a63178de1a15202afa6ead9`.

Decision: the next candidate is bounded full-identity `SegmentSource` sharing for push-frontier so
adjacent DataFusion partition requests can enter one source driver and coalesce. The sharing key
must preserve complete store/file-version identity, its retention must be constant rather than
linear in data size, and the acceptance gate must recheck exact output, plan/resource symmetry,
request overlap, RSS, and diagnostics-off 14-core timing. Do not alter V1 or plain Push, and do not
implement general decoded retention as part of this candidate.

## Bounded source-sharing candidate result

That candidate is now implemented in this checkpoint based on
`104297a310f648220b59ce152d275203e5aa96cc`. It is deliberately isolated to DataFusion's
push-frontier backend using the built-in object-store reader. V1, legacy Push, custom reader
factories, and push-frontier scans without a sharing context keep their previous paths.

The cumulative mechanism includes:

- a weak, concurrency-bounded source pool keyed by object-store instance and the complete immutable
  `ObjectMeta` identity: location, size, nanosecond last-modified time, e-tag, and version;
- push-frontier-specific exact-identity footer and natural-split reuse so a same-path replacement
  cannot combine a new source with stale split metadata;
- a `SharedSegmentSource` that shares only raw in-flight segment futures, uses generation-checked
  completion cleanup, amortized/capped stale-key sweeping, and does not share decoded arrays,
  arenas, plans, outputs, or mutable scan runtime state;
- source/file-scoped physical-I/O metrics, rather than assigning shared reads to whichever
  DataFusion partition won a race;
- a shared-source-only aggregate gap policy. It retains the discovered reader distance and maximum
  span, but accepts a prospective physical span only when internal hole bytes
  `G <= min(256 KiB, 64 KiB + floor(U / 4))`, where `U` is the union of requested child intervals
  before alignment padding. Existing alignment and maximum-size rules remain hard bounds.

The diagnostic mechanism gate passed for both focus queries. Q22 used 3 source instances, 26
physical ranges, 21 positional-read calls, 7,330,564 total bytes, 5,165,088 union bytes, and
2,165,476 overlap bytes. Q6 used 1 source instance, 171 physical ranges, 127 calls, 37,727,340
total bytes, 34,570,564 union bytes, and 3,156,776 overlap bytes. Exact Arrow schema/multiset and
normalized-plan gates passed. The artifact is
`/private/tmp/tpch-bounded-gap-mechanism-20260912.TcVwQD`; its frozen candidate diff SHA-256 is
`99f8957a8dd648dc87c2ccc69c29f9791f1b35df33388870f99c72eef08ebfc8`.

The diagnostics-off acceptance checkpoint did **not** accept the candidate. Q22 push-frontier
improved by 6.65% against the immediate parent (`Rpush = 0.933484`) with neutral V1
(`Rv1 = 1.000109`), but missed the predeclared at-least-10% requirement and candidate
push-frontier remained 10.54% slower than candidate V1. Candidate push-frontier median RSS fell
from 121.84 MB to 107.23 MB and passed the memory bound. The protocol therefore stopped before
Q6. Preserve this as a rejected performance gate, not as a claim that source sharing regressed.
Artifact: `/private/tmp/tpch-bounded-acceptance-20260913.2gIRWH`.

## Parked persistent file-payload reader WIP

An unbenchmarked persistent-file-reader experiment is parked on top of the bounded source-sharing
candidate. On Unix and Windows, and only for effective push-frontier plus the built-in reader plus
active source sharing, `ObjectStoreReadAt` can promote an object-store `File` payload into one
source-owned `Arc<File>` and use positional reads for subsequent ranges. It validates constructor
path equality, full response `ObjectMeta`, and the returned response range, supplies expected
e-tag/version in `GetOptions`, installs the file race-safely, treats a cached-read failure as
authoritative, and records fixed-cardinality diagnostics. Stream responses, identity/range
mismatches, and errors are not cached. V1, legacy Push, custom factories, nonshared
push-frontier, non-file payloads, and remote stores retain their previous behavior.

Status at checkpoint:

- the core `vortex-io` implementation compiled with
  `env RUSTC_WRAPPER= cargo check -p vortex-io --features object_store,tokio` before the test
  scaffold was added;
- the generic recording object-store test scaffold exists and test-compiles, but is currently
  unused and emits unused/dead-code warnings because the requested focused tests for concurrency,
  replacement, stream/error/cancellation behavior, backend routing, and descriptor release have
  not been written;
- no benchmark has exercised this layer, and no performance conclusion is valid;
- the initial pre-validation/pre-documentation parked WIP diff SHA-256 was
  `fbb8bcbd652c6cb1651a2ac4334f241f69a5db2910c039ac643e2a53d722dfd2`; the
  `vortex-io/src/object_store/read_at.rs` plus
  `vortex-datafusion/src/persistent/opener.rs` subset at that point was
  `fa969588e7b0edc2495298304db78f920e2fc4f8484e5b82957a2a7323bb4540`;
- after repository formatting, the current code-only diff excluding this handover has SHA-256
  `7e6b86022cd7a51774a2927f6839c8b199db2fe134f3fc4516c1bcc9cca0f9f6`; the two-file
  persistent-reader subset has SHA-256
  `903f98ff91a24339fe65eaa2da489b5e8824c7f939720c0203ee61363342611e`.

Checkpoint-only validation, without running a benchmark:

```text
cargo +nightly fmt --all
passed

env RUSTC_WRAPPER= cargo test -p vortex-io --features object_store,tokio --no-run
passed; five expected unused/dead-code warnings from the parked test scaffold

env RUSTC_WRAPPER= cargo test -p vortex-datafusion --no-run
passed; macOS linker emitted the existing oversized compact-unwind warning

git diff --check
passed
```

This checkpoint is intentionally a WIP: finish the focused tests and rerun the bounded mechanism
and acceptance gates before making any speed claim.

## DuckDB scan and local-I/O findings

DuckDB and DataFusion do not expose all-core scan work in the same shape. The DuckDB extension is
file-ordered but split/morsel-parallel within the current file:

1. `MultiFileFunction<VortexReaderInterface>` opens an `OpenFileReader`; that reader owns one
   `VortexFile`, its footer and per-file `Arc<dyn SegmentSource>`, the selected backend, and the
   remaining split futures (`vortex-duckdb/cpp/table_function.cpp` and
   `vortex-duckdb/src/file_reader.rs`).
2. Under DuckDB's global file lock, each worker pops one future from the current file. Actual scan
   execution occurs after releasing the lock. The global state advances to the next file only
   after all current-file futures have been assigned, although trailing work from the old file may
   overlap opening and assignment from the next file.
3. V1 creates one future per natural row split. Both push backends create one external future per
   logical morsel and one per-file `IoService`/segment-source driver shared by all those morsels.
4. Each external push morsel executes with a one-worker scheduler on the polling DuckDB thread.
   All-core parallelism therefore comes from DuckDB's `TaskScheduler::NumberOfThreads()`, not an
   additional push worker pool. The process-global Vortex current-thread runtime has no threads of
   its own and is cooperatively driven by waiting DuckDB workers.

Legacy Push and push-frontier share the same `MorselScanBuilder`, projection, ordering, filter,
selection, row range, file footer, per-file source, and DuckDB task topology. Their intentional
difference is I/O policy: legacy Push selects eager-lookahead scheduling, while push-frontier
selects grouped-I/O frontier scheduling. In DuckDB's external-driver mode legacy Push currently
has zero additional lookahead morsels, but it still follows the eager policy rather than the
frontier cursor/gates.

Local files currently take the object-store adapter path rather than `FileReadAt`:

`OpenFileReader` -> `ObjectStoreFileSystem::local` -> per-file `ObjectStoreReadAt` ->
`FileSegmentSource` coalescing -> one `get_opts` for each resulting physical child range -> local
open/metadata -> `File` payload -> one blocking-pool positional read -> `pread`/`read_exact_at`.

The Vortex blocking pool reuses idle threads, so this is not one `pthread_create` per read in a
steady burst. However, the local object-store path opens, stats, and closes the file for every
physical range, then submits each range separately to the blocking pool even when `read_ranges`
batched several ranges. `VortexOpenOptions::open_path` already provides a one-`Arc<File>`
`FileReadAt` path, but DuckDB does not use it. A DuckDB-local routing change to that existing path
is the smallest prospective way to keep one descriptor per live file without any data-linear byte
cache; it has not been implemented or benchmarked here.

DuckDB benchmark fairness is not yet at the DataFusion checkpoint standard:

- `duckdb-bench` reopens its connection between iterations unless `--reuse` is given, but does not
  provide the alternating fresh-process, immediate same-query/backend-prewarm matrix used by the
  DataFusion acceptance runner;
- the normal timed runner records result row count, not exact schema and row multiset. A separate
  SF1 TPC-H validation helper exists but is not invoked by the timed main path;
- backend choice comes from process environment while DuckDB thread count is an optional CLI
  setting. Any V1/Push/push-frontier comparison must pin both explicitly, use the same file hashes,
  SQL, projection/filter/aggregate pushdowns and connection policy, and capture equivalent plans;
- the current DataFusion-only source-sharing and persistent-file-reader experiments do not affect
  DuckDB. A DuckDB comparison today measures the committed V1, legacy Push, and grouped-frontier
  scan paths without those DataFusion opener changes.

## Changed-file map

| Files | Purpose |
|---|---|
| `src/build.rs` | Frontier cursor and batch API, pruning-use collection, frontier arena creation |
| `src/node.rs` | `ExecNode::next_io`, prewalk poll/state/context, group/read operations |
| `src/nodes/io_root.rs` | Prewalk-only pruning group wrapper |
| `src/nodes/conjunct.rs` | Cascade and parallel group boundaries |
| `src/nodes/filter.rs` | Predicate-group propagation and projection group |
| `src/nodes/chunked.rs` | Group propagation across row cuts |
| `src/nodes/struct_.rs` | Group propagation across fields |
| `src/nodes/flat.rs` | Incremental key enumeration and final-use retirement |
| `src/io.rs` | Sharded cell registry, exact raw leases, cancellation and lifetime metrics |
| `src/source.rs` | Queued/in-flight cancellation, abort handles, source-driver lifecycle |
| `src/driver.rs` | Cursor consumer, bounded publication, resident morsels, query teardown |
| `src/stats.rs` | Live/peak raw-cell, raw-byte, and cancellation counters |
| `src/harness.rs` | Discard-output timing paths and frontier experiment plumbing |
| `src/bin/tpch-eval.rs` | SF10 validation, cold/hot/latency evaluation, memory and I/O reporting |
| `src/bin/morsel-eval.rs` | Frontier configuration/reporting for the smaller evaluator |
| `src/tests.rs` | Group shape, correctness, lifetime, cancellation, and boundedness tests |
| `Cargo.toml` | Tokio time feature used by injected-latency evaluation |
| `README.md` | High-level behavior and links to these documents |
| `IO_FRONTIER_IMPLEMENTATION.md` | Process, confirmed bugs, and regression checklist |
| `IO_FRONTIERS.md` | Design reasoning and benchmark history |
| `../vortex-datafusion/Cargo.toml`, `../Cargo.lock` | Direct `parking_lot` dependency for the bounded source pool |
| `../vortex-datafusion/src/persistent/opener.rs` | Exact source identity/pool, PF-only routing, split/footer identity handling, source metrics, and persistent-file-reader opt-in |
| `../vortex-datafusion/src/persistent/source.rs` | Concurrency-bounded pool ownership attached to each DataFusion source |
| `../vortex-file/src/open.rs` | Per-open shared-source and coalescing-policy options |
| `../vortex-file/src/read/driver.rs` | Aggregate requested-union/gap-budget coalescing policy and tests |
| `../vortex-file/src/read/mod.rs` | Read-driver policy export |
| `../vortex-file/src/segments/source.rs` | Per-file policy routing and source diagnostics |
| `../vortex-layout/src/segments/shared.rs` | Generation-safe, bounded raw in-flight segment sharing |
| `../vortex-io/src/object_store/read_at.rs` | Parked persistent file-payload promotion and incomplete recording-store test scaffold |

## Recommended next steps

1. Complete focused tests for the parked persistent-file-reader layer, including concurrent first
   responses, exact identity/range/options, stream and sibling-error behavior, cancellation,
   same-path replacement, backend isolation, and descriptor release. Do not benchmark or accept
   this WIP before those tests and targeted Clippy pass.
2. Decide whether to retain the bounded source-sharing candidate despite its failed performance
   acceptance gate. If retained, rerun the exact mechanism gate first after the persistent-reader
   tests, then the same diagnostics-off all-core acceptance matrix. Do not relax the recorded
   threshold after observing the result.
3. For DuckDB, add an equivalent exact-output/plan/resource-symmetric V1 versus legacy Push versus
   push-frontier harness before optimizing. The first isolated implementation candidate should be
   local-file-only persistent `FileReadAt`, not a scheduler change.
4. Review the small core API. Decide whether `IoGroupKind` and the caller-provided `budget`
   belong in the eventual public surface. The current recommendation is to keep both during the
   prototype: kind is semantic plan information, and budget makes incremental behavior explicit.
5. Add direct unit tests for invalid `IoPrewalkCx` usage if the API will be used by more node types:
   nested begin, read outside a group, end without begin, and begin twice before moving right.
6. Rerun at least a focused hot and latency smoke benchmark after the API simplification. Expect
   no regression because the change removed allocations and record fields, but verify it.
7. Review the signed-off commit stack. It is split into:
   - grouped prewalk and cursor API;
   - scheduler integration, raw lifetime, cancellation, and driver teardown;
   - regression tests;
   - benchmark correctness tooling and evaluation matrices;
   - documentation.
8. Rerun the full crate validation after any rebase or conflict resolution because either can
   alter behavior.
9. Do not implement general decoded retain/close or dictionary single-flight as part of cleanup;
   keep it as a separately reviewed design.

All commits must include the repository-required signoff:

```text
Signed-off-by: "COMMITTER" <COMMITTER_EMAIL>
```

## Resume commands

```bash
git switch ji/push-frontier-all-bench
git status --short
git diff --stat

env RUSTC_WRAPPER= cargo test -p vortex-morsel-push --features _test-harness
env RUSTC_WRAPPER= cargo test -p vortex-io --features object_store,tokio --no-run
env RUSTC_WRAPPER= cargo test -p vortex-datafusion --no-run
cargo +nightly fmt --all
env RUSTC_WRAPPER= cargo clippy -p vortex-io --all-targets --all-features -- -D warnings
env RUSTC_WRAPPER= cargo clippy -p vortex-datafusion --all-targets --all-features -- -D warnings
git diff --check
```

## Fresh-task instruction

For a context-free continuation, use:

> Read `vortex-morsel-push/IO_FRONTIER_HANDOVER.md` and
> `vortex-morsel-push/IO_FRONTIER_IMPLEMENTATION.md` on `ji/push-frontier-all-bench`. The bounded
> full-identity push-frontier source-sharing candidate passed its Q6/Q22 mechanism gates but failed
> its Q22 performance acceptance gate. A persistent local file-payload reader is parked on top as
> unbenchmarked WIP with an unused recording-store test scaffold. Finish its focused tests and
> targeted Clippy before rerunning exact, request-overlap, RSS, and diagnostics-off 14-core gates.
> Keep V1, legacy Push, custom readers, and nonshared push-frontier unchanged. DuckDB does not use
> either DataFusion-only candidate; first build a fair three-backend DuckDB harness if work moves
> there.
