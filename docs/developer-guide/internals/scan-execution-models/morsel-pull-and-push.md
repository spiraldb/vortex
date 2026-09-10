# Pull and Push: The Two Morsel Execution Models

There are two morsel executors in the tree, `vortex-morsel` (pull) and `vortex-morsel-push`
(push). They started as one crate. The push crate was imported from a separate branch that added a
second way of executing a morsel, and the two have since diverged in what they support. This
document explains both models for a reader who has read the
[primer](morsel-executor-primer.md), shows the same query executing under each, and records what
was measured on 2026-09-03 so the choice between them can be made on evidence.

The short version: both crates plan a morsel the same way and do I/O the same way. They differ in
how values move. Pull asks the root for its value and the request recurses down the tree. Push
activates the leaves with an exact set of rows and their batches climb the tree along fixed edges.

## What both models share

Everything up to value production is the same design, and most of it is the same code.

- **Plan.** One immutable `ExecPlan` per scan, built from the layout tree and the query; every
  worker instantiates its own arena of node state and reuses it across morsels.
- **Morsels.** Contiguous root row ranges cut at the union of every column's chunk boundaries,
  coalesced to a target size. One worker owns one morsel at a time.
- **Planning.** `ExecNode::next_plan` names the reads a morsel will need by registering `IoUse`s
  and receiving tickets. It never reads. It can block on a wait and resumes from its cursor.
- **I/O.** A scan-wide `IoService` dedupes reads into cells, hands them out as `IoDemand`, and
  is answered through `IoCompletions`. Blocked workers park on exact cells. Each morsel's reads
  are submitted as one sorted batch so a coalescing source sees neighbours; only the pull crate
  goes further and holds a wave of workers' batches together.
- **Leases.** Decoded chunks are shared across morsels through lease counts computed from the
  cut before the scan starts, never through a cache.
- **Scheduling.** A scheduler assigns morsels to workers in index order, submits a lookahead
  window of future morsels' reads on filtered scans, and restores output order by morsel index.
- **Retirement.** Each node releases its leases exactly once when the morsel finishes.

In both crates a morsel is planned to completion before any value is produced, so by the time
execution starts every read the morsel needs has been named and, on a background-read source,
started.

## The pull model

Pull is the recursive model the design document describes. The root node's `execute` is polled;
it asks its children for their values through `ExecCx::child_array` or `child_mask`, each child
asks its own children, and values return up the call stack.

```text
ProjectExec::execute
  └─ child_array(body) ──► StructExec ──► ChunkedExec ──► FilterExec::execute
                                            ├─ child_mask(mask node) ──► ConjunctExec::execute
                                            │     └─ child_array(input) ──► StructExec ──► FlatExec (decode)
                                            └─ child_array(leaf, hint = mask slice) ──► FlatExec (decode)
                                  └─ child_array(field)  ──► ChunkedExec ──► FlatExec (decode)
```

- **A hint flows down, the filter is a node.** A node executes under a row hint its parent
  chose: the rows the parent expects to need. Only the flat leaf reads it, to skip a read nobody
  wants. The selection itself is not a parameter of anything: the plan's `Filter` is pushed to
  the leaves at planning, so every segment scan under it sits below its own filter node, and all
  of them read one mask node, the conjunct, which evaluates the predicate once per morsel from
  the morsel's own demand and answers every filter with the same mask. A filter slices the mask
  to its rows, hands the slice down as the hint, keeps the selected rows of what comes back, and
  returns empty without touching the leaf when the slice keeps nothing. A conjunct in cascade
  mode passes each conjunct the mask the previous one produced.
- **Blocking unwinds the stack.** When a leaf finds its cell not ready it returns
  `ExecPoll::Blocked(waits)` naming the exact ticket; every ancestor returns the same, the
  worker parks, and on wake the root is polled again. Each node keeps a cursor so it resumes
  where it left off rather than redoing finished children.
- **One value per node per morsel.** A node produces its complete value for the morsel at once.
  Chunked concatenates its cut children; struct zips its fields; the root emits one batch.
- **The whole contract is five methods** on `ExecNode`: `reset`, `next_plan`, `execute`,
  `retire`, `children`. The six operators together are about 1,350 lines.

What pull is good at: it is simple to reason about, every row a leaf materializes was asked for
by a parent that already knew the mask, and blocking is trivially correct because the stack is
the continuation. What it cannot do: a parent cannot start work on a child's partial output,
so a struct with one slow field waits for that field before zipping the others, and a slow
conjunct input stalls the whole morsel.

## The push model

Push keeps planning, I/O, and retirement, keeps `execute` for its pull mode, and adds a
leaf-driven runtime beside it. The plan is compiled into a physical topology at build time and,
in push mode, executed by a per-morsel `PhysicalRuntime`.

```text
sources (Flat leaves)           pipelines                              breakers
  activated with exact rows ──► push_input along fixed Routes ──► Conjunct / Filter / multi-child
  decode, emit one batch         one batch of credit per edge         Struct or Chunked
                                                                          │ Gate sidebands
                                                                          ▼
                                                              activate the deferred sources
```

- **Sources and roles.** Every flat leaf is a *source* with a role: `Predicate { slot, mode }`
  if it feeds a conjunct, `Projection` if it feeds the filter's projection input. The roles are
  computed at build time by walking each leaf's route to the root.
- **Pipelines and breakers.** Nodes are grouped into pipelines, maximal chains that end at a
  *breaker*: a conjunct with more than one slot, a filter with a predicate, or a struct or
  chunked node with more than one child. A breaker has typed input ports fixed at plan time; the
  filter's port 0 expects a mask, port 1 an array. Routes from child to parent port are static,
  so the runtime never looks anything up.
- **Activation is authoritative.** A source starts with `push_start(span, ActivationRows)`,
  where `ActivationRows` carries the logical selection and a possibly wider materialization
  domain, with the selection required to be a subset. The rows a source produces are decided by
  that activation and nothing else.
- **Deferred sources and gates.** With a filter, projection sources and later cascade conjunct
  slots are *deferred*: their reads are registered as speculative and their activation waits.
  When a conjunct finishes a slot it emits a `Gate` sideband naming the next slot with the
  refined rows; each predicate batch the filter receives becomes a `Gate` for the projection
  with those rows. A gate activates the deferred sources with exactly those rows. An all-false cascade stage
  gates every remaining slot with an empty selection so their sources emit empty batches
  without decoding.
- **Credit and routing.** Each edge carries one batch of credit; a node holds further output
  until `push_credit` returns it. A passive stage's batch is handed straight to the next stage
  when that pipeline is not blocked. A stage that finds its cell not ready returns
  `Waiting(waits)`; the runtime suspends that pipeline's frames, the worker parks, and the wake
  resumes exactly that stage.
- **Demand hints are advisory.** A gate also publishes its rows as a `DemandTarget` hint. A
  hint can mark a deferred read as required or record that none of its rows are selected, but
  it neither starts nor cancels a read on its own; only activation and parking do that.
- **Root assembly and output credit.** Root batches must cover the morsel contiguously from its
  start; the runtime assembles them into one array. `MorselScan::into_stream` adds ordered,
  credit-bounded output (`with_output_capacity`) with cancellation on drop, and the ordered head
  may bypass a full buffer so order never deadlocks.
- **External threads.** `run_on_current_thread` runs the morsel on the calling thread with a
  thread-local arena and ticks the engine's own runtime while it waits. This is how DuckDB's
  scan threads drive push morsels.

What push is good at: exact-row activation means a projection leaf never materializes rows the
predicate rejected, batches can climb as soon as they exist, and an engine can host the executor
on its own threads. What it costs: the physical runtime (frames, suspended stacks, deferred
calls, credits, gates, hints, root fragments) is where most of the crate's size lives, and the
contract on `ExecNode` grows from five methods to ten, plus a defaulted benchmark-only
`push_profile_kind`.

## The same query, step by step

`SELECT b FROM t WHERE a > 400`, one morsel, both columns chunked flat, in-memory source.

| Step | Pull | Push |
| --- | --- | --- |
| Plan | The project root plans the mask node, `a` (required), then the body, `b` (speculative). | Same planning stream. `a` becomes a predicate source, `b` a deferred projection source. |
| Start | Root `execute` runs the body; the first filter node it reaches asks the mask node for the morsel's mask. | The runtime activates `a`'s source with all rows; `b` waits for a gate. |
| Predicate | The conjunct asks `a`'s subtree for an array hinted with the morsel's rows, applies `a > 400`, keeps the mask and returns it to every filter that asks. | `a`'s source decodes and pushes one batch to the conjunct's port; the conjunct evaluates `a > 400` on arrival and pushes the mask to the filter's port 0. |
| Projection | Each filter over a `b` leaf slices the mask to its chunk, hands the slice down as the hint, and keeps the selected rows of what its leaf decodes (or returns nothing without decoding where the slice is all-false). | The filter emits a projection gate with the mask; `b`'s source is activated with exactly those rows, decodes, filters, and pushes to port 1. |
| Output | The project root applies the projection expression to the selected rows and returns one batch. | The filter waits for both ports to end, applies the projection expression, and emits one root batch. |
| A read not ready | The leaf returns `Blocked(ticket)`; the whole stack unwinds; the worker parks; the root is re-polled. | The stage returns `Waiting(ticket)`; that pipeline's frames are suspended; the worker parks; that stage is resumed. |

On this query the two models read the same bytes and decode the same chunks. Where they differ
is how much machinery moves the mask from `a` to `b`: a function argument in pull, a gate, an
activation, and a routed batch in push.

## Differences that matter today

| | Pull (`vortex-morsel`) | Push (`vortex-morsel-push`) |
| --- | --- | --- |
| Value execution | Recursive `execute` | Leaf activation, routed batches, credits |
| `ExecNode` methods | 5 | 10, plus one defaulted benchmark hook |
| Layouts | Flat, chunked, struct (incl. nullable), dictionary, zoned wrappers | Flat, chunked, non-nested struct, zoned wrappers |
| Layout extension | Lowers `vortex_layout::plan` operators; no layout dispatch | Closed match in `build.rs` |
| Sparse demand | `with_morsel_demands` (per-morsel masks) | Selection converted to row ranges |
| Output | Batches collected, or a completion sink | Ordered credit-bounded stream, sink, or collected |
| Engine threads | Worker pool, shared or dedicated | Same, plus run on the caller's thread |
| Cancellation | `ScanCancellation` | `StreamCancellation` |
| Lines (src, excluding test files) | about 6,300 | about 14,500 |
| Lines net of inline test modules | about 6,200 | about 11,200 |
| DataFusion and DuckDB suites | pass | 17 and 12 failures in this session's runs, all layout coverage |

Both crates now share the I/O handoff (`IoDemand`, `IoCompletions`, `SegmentSourceDriver`) and
the fixes from this week's reviews. Only the pull crate has the planner registry, the node
blueprints, and the `IoAnswerer` trait.

## What was measured

TPC-H `lineitem` at scale factor 1, generated in memory, real compressing write pipeline, the
scan portions of the queries, three iterations, median. Both evaluators validate every
configuration against V1's output before timing, and every configuration matched. The host has
14 cores; "x14" rows use one worker per core. The push crate ran with the working tree's
uncommitted idle-parking change to its external driver path, which these rows do not exercise.

### Push mode against pull mode, same crate

The push crate's evaluator runs both of its execution modes on the same code, which isolates
the value-execution contract from everything else the two crates differ in. Ratios are against
V1 on one thread; the last column is push wall time over pull wall time.

| Query | Selectivity | V1, 1 thread | V1, Tokio x14 | Pull mode x14 | Push mode x14 | Push / pull |
|---|--:|--:|--:|--:|--:|--:|
| Q6 | 1.9% | 15.16 ms | 0.18x | 0.10x | 0.11x | 1.02 |
| Q1 | 98.6% | 5.42 ms | 0.29x | 0.19x | 0.19x | 1.00 |
| Q14 | 1.3% | 5.92 ms | 0.22x | 0.14x | 0.13x | 0.93 |
| Q15 | 3.8% | 5.91 ms | 0.21x | 0.15x | 0.13x | 0.92 |
| Q12 | 1.8% | 15.75 ms | 0.18x | 0.12x | 0.13x | 1.13 |
| Q19 | 60.0% | 21.39 ms | 0.24x | 0.10x | 0.10x | 0.96 |
| scan-6col | 100% | 2.16 ms | 0.41x | 0.18x | 0.23x | 1.29 |
| selective | 0.0% | 8.18 ms | 0.21x | 0.15x | 0.14x | 0.97 |
| **geomean** | | | **0.23x** | **0.14x** | **0.14x** | **1.02** |

The two modes are the same within noise: a geometric mean of 1.02 across the eight queries, with
push ahead by 3 to 8 percent on four of the selective queries and behind by 13 percent on Q12
and 29 percent on the bare six-column projection, where routing batches through pipelines is
pure overhead over a recursive call. Time to first batch is also indistinguishable. On in-memory
data with morsels this size, exact activation buys nothing that the pull model's demand mask did
not already buy.

### The pull crate against the same baseline

The independent pull crate, run by its own evaluator in a separate process against a fresh V1
baseline. The last column compares its x14 rows with the push crate's pull mode above; the two
V1 baselines agree to within a few percent, so the comparison is meaningful though not exact.

| Query | V1, 1 thread | V1, Tokio x14 | Pull crate x1, natural cut | Pull crate x14, 128 Ki morsels | Pull crate x14 / push crate pull mode x14 |
|---|--:|--:|--:|--:|--:|
| Q6 | 14.64 ms | 0.19x | 0.79x | 0.10x | 0.91 |
| Q1 | 5.59 ms | 0.26x | 0.85x | 0.16x | 0.86 |
| Q14 | 5.80 ms | 0.25x | 0.82x | 0.12x | 0.86 |
| Q15 | 6.04 ms | 0.21x | 0.74x | 0.12x | 0.81 |
| Q12 | 15.23 ms | 0.19x | 1.02x | 0.13x | 1.09 |
| Q19 | 21.39 ms | 0.22x | 0.65x | 0.07x | 0.67 |
| scan-6col | 1.92 ms | 0.43x | 0.25x | 0.13x | 0.66 |
| selective | 8.02 ms | 0.20x | 0.71x | 0.11x | 0.76 |
| **geomean** | | **0.24x** | **0.68x** | **0.11x** | **0.82** |

The pull crate is about 18 percent faster than the push crate's pull mode on the same cut. That
gap is not the execution model, since both rows are recursive pull; it is the work that only
landed in the pull crate: the plan-keyed lookahead refill, planning waves across workers, and
the sparse scheduling work from the SSD random-access record. It is a measure of how far the two
forks have drifted.

The one-thread rows show the same 0.68 geometric mean against V1 that the earlier TPC-H findings
reported at 0.75, with the same shape: the executor's advantage is largest on projection-heavy
scans and smallest where decode and predicate kernels dominate.

## Reading the two together

Ask three questions of any change to either crate:

1. Does it change what rows a leaf materializes? In pull a leaf hands up its whole range and
   the filter node the plan placed above it keeps the selected rows; the hint only decides
   which chunks are read at all. In push it is the activation, which is exact for the projection
   under a filter.
2. Does it change when a read starts? Neither model starts a read during execution; planning
   names it and the scheduler hands it out. Push additionally holds deferred sources' reads as
   speculative until a gate or a park proves them needed.
3. Does it change how a blocked morsel resumes? Pull re-polls the root and relies on per-node
   cursors; push resumes the exact stage from suspended frames.

The measured gap between the modes is inside noise, so the pull model carries the same result
with roughly 55 percent of the code once inline test modules are discounted, and the wider
layout coverage. The push model's real contributions,
exact activation for deferred sources, bounded ordered output with cancellation, and running a
morsel on the engine's own thread, are features that can be added to the pull crate one at a
time, each measured on its own. That is the recommendation this document makes. What it does not
settle is behaviour against real storage latency, where deferring a projection read until the
predicate has proven it needed could matter more than it does in memory; the tools to measure
that are the evaluators' disk mode and the random-access benchmark.
