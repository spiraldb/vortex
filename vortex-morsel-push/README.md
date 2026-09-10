# vortex-morsel-push

A morsel-driven push executor for Vortex layouts. A worker activates sources for a row range;
sources produce batches and push them through compiled physical pipelines.

```text
activate source → read/decode → push batch → downstream stages → output
```

`build_plan` binds expressions, identifies sources, and fuses eligible operator chains. Within
a pipeline, each batch passes directly to the next stage on the same worker. Operators with
multiple inputs retain and align batches at pipeline boundaries.

`next_plan` registers the reads a morsel can need. Execution begins with `push_start` on its
sources. `push_input` and `push_end` deliver batches and input completion. A stage waiting for
I/O retains its state and resumes through `push_resume` when its exact tickets complete.
`push_credit` tells a producer that downstream capacity is available again.

Predicates produce authoritative selections that activate later predicates and projected
columns. Optional demand hints can defer speculative I/O; correctness depends on the selections,
not on whether hints arrive. All-false selections avoid unnecessary decode work.

The executor does not poll storage futures. `SegmentSourceDriver` answers the scan's `IoDemand`
stream on a separate runtime task or thread. `MorselScan::into_stream` provides ordered output,
bounded capacity, and cancellation; `run` collects the output. Leased shared cells retain decoded
chunks until the last overlapping morsel retires.

The prototype supports flat, chunked, and non-nullable struct layouts, plus transparent zoned
and legacy-statistics wrappers. Unsupported layouts are build errors. Import provenance is in
[UPSTREAM.md](UPSTREAM.md); the [executor primer](../docs/developer-guide/internals/scan-execution-models/morsel-executor-primer.md)
explains the current contracts.

## Evaluation

Both evaluators run push pipelines and validate output against V1 before timing:

```bash
cargo run --release -p vortex-morsel-push --features _test-harness --bin morsel-push-eval
cargo run --release -p vortex-morsel-push --features _test-harness --bin tpch-push-eval -- 1
```

SQL integrations select this executor with `VORTEX_SCAN_BACKEND=push`.
