# vortex-morsel-push

A morsel-driven push executor for Vortex layouts. A worker activates sources for a row range;
sources produce batches and push them through compiled physical pipelines.

```text
activate source → read/decode → push batch → downstream stages → output
```

`build_plan` binds expressions, identifies sources, and fuses eligible operator chains. Within
a pipeline, each batch passes directly to the next stage on the same worker. Operators with
multiple inputs retain and align batches at pipeline boundaries. Bottom-level `Chunked<Flat…>`
layouts compile into one flat source over a shared sequence of segment descriptors, removing
that chunk boundary from the pipeline. Each segment retains its own read ticket and decode lease.
The source pushes segment overlaps in row order as downstream credits arrive; its full row range
does not change morsel or batch sizes.

`next_plan` registers the reads a morsel can need. Execution begins with `push_start` on its
sources. `push_input` and `push_end` deliver batches and input completion. A stage waiting for
I/O retains its state and resumes through `push_resume` when its exact tickets complete.
`push_credit` tells a producer that downstream capacity is available again.

Predicates produce authoritative selections that activate later predicates and projected
columns. Optional demand hints can defer speculative I/O; correctness depends on the selections,
not on whether hints arrive. Selections can arrive in fragments: each segment becomes eligible
once its overlapping rows are known, allowing an ordered prefix to run while later selections
are pending. All-false selections avoid unnecessary decode work.

The executor does not poll storage futures. `SegmentSourceDriver` answers the scan's `IoDemand`
stream on a separate runtime task or thread. `MorselScan::into_stream` provides ordered output,
bounded capacity, and cancellation; `run` collects the output. Leased shared cells retain decoded
chunks until the last overlapping morsel retires. Raw segment cells carry exact planned-use
counts; their final use drops ready bytes or cancels the outstanding source future. A scan-owned
driver is shut down and joined before the scan can leave a query run.

The prototype supports flat, chunked, and non-nullable struct layouts, plus transparent zoned
and legacy-statistics wrappers. Unsupported layouts are build errors. Import provenance is in
[UPSTREAM.md](UPSTREAM.md); the [executor primer](../docs/developer-guide/internals/scan-execution-models/morsel-executor-primer.md)
explains the current contracts. The grouped cursor implementation process and bug checklist are in
[IO_FRONTIER_IMPLEMENTATION.md](IO_FRONTIER_IMPLEMENTATION.md); scheduling evidence and the
decoded-sharing follow-up are recorded in [IO_FRONTIERS.md](IO_FRONTIERS.md). The current branch
state and context-free continuation instructions are in
[IO_FRONTIER_HANDOVER.md](IO_FRONTIER_HANDOVER.md).

## Evaluation

Both evaluators run push pipelines and validate output against V1 before timing:

```bash
cargo run --release -p vortex-morsel-push --features _test-harness --bin morsel-push-eval
cargo run --release -p vortex-morsel-push --features _test-harness --bin tpch-push-eval -- 1
```

The default comparison keeps query semantics identical: both paths receive the same projection
and filter, scan the full row range with the default selection, preserve row order, use the same
session, layout, and segment source, and consume timed output immediately. The V1 rows use the
current `LayoutReader` `ScanBuilder` with its default split policy and per-worker concurrency. The
evaluator's default push rows use the grouped-I/O frontier scheduler;
`MorselConfig::frontier_defaults` changes the path selector from `None` to `Some(1)`, admitting one
additional range frontier per worker. Explicit policy matrices retain clearly labelled
zero-lookahead controls and a `push current` row when comparing the two push schedulers.

SQL integrations select this executor with `VORTEX_SCAN_BACKEND=push`. That label retains the
established eager-lookahead policy. `VORTEX_SCAN_BACKEND=push-frontier` selects the grouped-I/O
frontier scheduler with the production defaults: one additional frontier per worker, zero bounded
right speculation, one resident morsel per worker, and row-frontier refills of 32 ranges.
