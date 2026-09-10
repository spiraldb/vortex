# Morsel Push Executor

The current executor is `vortex-morsel-push`. A worker activates sources, and each source pushes
batches through downstream operators toward the scan output. The plan compiles eligible chains
into physical pipelines so these transfers run inline on the owning worker.

```text
source activation
      ↓
flat source → chunk routing → predicate evaluation
                                    ↓ selection
                              projection sources → field alignment → projection → output
```

A morsel is a contiguous range of root rows assigned to one worker. A batch is a value produced
within that range. One morsel can contain many source batches, including batches whose column
chunk boundaries differ. Operators reconcile those boundaries before producing aligned output.

## Planning and activation

`build_plan` creates an immutable `ExecPlan`: node definitions, source ranges, typed input ports,
and physical pipelines. Workers instantiate their own arenas and reset node state for each
morsel. Plans can be shared; mutable operator state stays on its worker.

Before execution, `next_plan` registers segment uses and obtains I/O tickets. Planning is
resumable and has a per-call budget. It can traverse children to discover reads; value production
starts separately by activating sources.

An activation carries both the logical rows required for correctness and the materialized rows
needed for evaluation. They can differ: evaluating a dense predicate may be cheaper than first
compacting its input. `PushBatch` carries these domains with its coverage, so a downstream stage
can interpret its array or mask without reconstructing row identity.

Cascade conjunctions activate later predicate sources using earlier results. Parallel
conjunctions evaluate their inputs independently and intersect the resulting masks. The final
predicate selection activates projection sources. Optional demand hints help prioritize or
suppress speculative I/O; dropping or delaying them must preserve output.

## Passing batches through a pipeline

`ExecNode` exposes the following execution events:

| Event | Meaning |
| --- | --- |
| `push_start(span, rows)` | Activate a source for a range and authoritative selection. |
| `push_input(port, batch, last)` | Deliver a batch to a downstream input. |
| `push_end(port)` | Close an input without another batch. |
| `push_resume()` | Continue a stage after a dependency becomes ready or drain retained output. |
| `push_credit()` | Return downstream capacity to a producer. |

A stage returns `NodeState` and writes output into `StageOutput`. The physical runtime hands
batches to the next compiled stage inline. It retains continuations when a stage waits or
exhausts its fairness quantum. Operators with multiple inputs align or retain batches at
pipeline boundaries, and credits bound how far producers can advance.

`push_resume` continues work that was already activated. A downstream operator receives input
through events; it has no API for recursively requesting a child's next value.

## I/O and suspension

`IoPlane` registers uses and resolves tickets. `SegmentSourceDriver` consumes the scan's
`IoDemand` stream and completes reads through `IoCompletions`. The storage futures run on the
source driver's runtime or thread.

A source may use a non-blocking probe to resolve a ready segment inline. Otherwise it returns
`NodeState::Waiting` with exact tickets. The scheduler records the waiting pipeline and stage,
then resumes that continuation after completion. Generation and continuation tokens reject
stale or duplicate wakes.

Raw request cells deduplicate reads during the scan. Optional leased shared cells reuse decoded
chunks across overlapping morsels. Retirement releases each morsel's leases, and the last lease
releases the decoded array.

## Output and cancellation

Completed morsel output is emitted in morsel order. Row and byte credits bound pending output;
a producer retains its completed batch while capacity is unavailable. Stopping consumption
cancels outstanding work and releases workers. `MorselScan::run` is the collecting adapter over
this output path.

`vortex-morsel-scan::MorselScanBuilder` adapts push scans to DataFusion and DuckDB. Set
`VORTEX_SCAN_BACKEND=push` to select it. V1 remains the established backend and the correctness
reference used by evaluation tools.

## Reading the code

| File in `vortex-morsel-push/src` | Responsibility |
| --- | --- |
| `build.rs` | Source catalog, operator definitions, pipeline compilation. |
| `node.rs` | Batch domains, event contracts, arena, planning and retirement contexts. |
| `driver.rs` | Source activation, pipeline execution, scheduling, output credits. |
| `nodes/` | Source, chunk, struct, conjunction, and filter behavior. |
| `io.rs`, `source.rs` | Ticket registration, read scheduling, and storage completion. |
| `executor.rs` | Scan-builder integration over layouts and selections. |

## Validation

```bash
cargo nextest run -p vortex-morsel-push -p vortex-morsel-scan --all-features
cargo run --release -p vortex-morsel-push --features _test-harness --bin morsel-push-eval
cargo run --release -p vortex-morsel-push --features _test-harness --bin tpch-push-eval -- 1
```

The evaluators compare dtype, row count, and ordered content against V1 before timing. Tests also
cover pipeline fusion, misaligned inputs, credit exhaustion, cancellation, speculative errors,
source activation, and I/O continuation wakes.
