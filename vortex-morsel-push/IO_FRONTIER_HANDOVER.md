# Grouped I/O frontier handover

Status as of 2026-09-12. This is the source of truth for resuming the grouped-I/O experiment in a
fresh task. Read [IO_FRONTIER_IMPLEMENTATION.md](IO_FRONTIER_IMPLEMENTATION.md) for the detailed
process, bugs encountered, and review checklist. Read [IO_FRONTIERS.md](IO_FRONTIERS.md) for the
benchmark history and scheduler experiments.

## Repository state

| Item | Value |
|---|---|
| Repository | `spiraldb/vortex-7` |
| Crate | `vortex-morsel-push` |
| Current branch | `ji/morsel-io-groups` |
| Starting comparison point | `f4ca2266da` — `wip(morsel): checkpoint I/O follow-ups` |
| Branch contents | Grouped-I/O implementation, tests, benchmark tooling, and documentation |

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

## Recommended next steps

1. Review the small core API first. Decide whether `IoGroupKind` and the caller-provided `budget`
   belong in the eventual public surface. The current recommendation is to keep both during the
   prototype: kind is semantic plan information, and budget makes incremental behavior explicit.
2. Add direct unit tests for invalid `IoPrewalkCx` usage if the API will be used by more node types:
   nested begin, read outside a group, end without begin, and begin twice before moving right.
3. Rerun at least a focused hot and latency smoke benchmark after the API simplification. Expect
   no regression because the change removed allocations and record fields, but verify it.
4. Review the signed-off commit stack. It is split into:
   - grouped prewalk and cursor API;
   - scheduler integration, raw lifetime, cancellation, and driver teardown;
   - regression tests;
   - benchmark correctness tooling and evaluation matrices;
   - documentation.
5. Rerun the full crate validation after any rebase or conflict resolution because either can
   alter behavior.
6. Do not implement general decoded retain/close or dictionary single-flight as part of cleanup;
   keep it as a separately reviewed design.

All commits must include the repository-required signoff:

```text
Signed-off-by: "COMMITTER" <COMMITTER_EMAIL>
```

## Resume commands

```bash
git switch ji/morsel-io-groups
git status --short
git diff --stat

env RUSTC_WRAPPER= cargo test -p vortex-morsel-push --features _test-harness
cargo +nightly fmt --all
env RUSTC_WRAPPER= cargo clippy \
  -p vortex-morsel-push --all-targets --all-features -- -D warnings
git diff --check
```

## Fresh-task instruction

For a context-free continuation, use:

> Read `vortex-morsel-push/IO_FRONTIER_HANDOVER.md` and
> `vortex-morsel-push/IO_FRONTIER_IMPLEMENTATION.md` on `ji/morsel-io-groups`. Review the core
> grouped-I/O API before changing scheduler policy or benchmark configuration, then continue from
> the recommended next steps.
