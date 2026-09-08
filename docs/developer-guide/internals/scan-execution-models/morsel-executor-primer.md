# Morsel Executor Primer

A first read of the experimental morsel scan executor: every trait and type, one at a time,
each reduced to what it is, who calls it, and the one rule that makes it work. The second half
records what the experiment taught, including the two integration steps taken on 2026-09-02:
scanning through a dedicated `MorselScanBuilder`, and handing all I/O out of plan execution.

Read this before the design documents in this directory. Those explain why; this explains what.

## The model in one paragraph

A scan is cut into *morsels*, contiguous ranges of root rows. One worker thread owns one morsel
at a time and drives a tree of small state machines over it. Planning names the reads the morsel
will need; execution consumes them; retirement releases what the morsel held. The executor itself
never reads storage: the reads it wants are handed out as a demand stream, and whoever owns
storage answers them. Cross-morsel reuse of decoded chunks comes from leases counted before the
scan starts, not from a cache.

## Part 1: the traits and types

Each card names the type, where it lives, and the rule to remember. Crates: `vortex-layout` and
`vortex-io` are production; `vortex-morsel` (pull) and `vortex-morsel-push` (push) are the two
prototype executors; `vortex-morsel-scan` is the facade the SQL engines use.

### `SegmentSource` (`vortex-layout/src/segments/source.rs`)

The storage contract: give me segment `id`, I give you a future for its bytes.

```rust
pub trait SegmentSource: 'static + Send + Sync {
    fn request(&self, id: SegmentId) -> SegmentFuture;
    fn request_background(&self, id: SegmentId) -> SegmentFuture;
    fn request_background_batch(&self, ids: &[SegmentId]) -> Vec<SegmentFuture>;
    fn request_nowait(&self, id: SegmentId) -> VortexResult<ReadAtNowait>;
    fn prefers_background_reads(&self) -> bool;
}
```

- `request` is the original, demand-driven read: the file driver does not make it eligible on
  its own until somebody polls the future, though it may ride along with a neighbouring read
  that gets coalesced.
- `request_background` and `request_background_batch` were added for the executor. A background
  read becomes eligible for physical I/O immediately, before any poll. The batch form registers
  every id before making any of them eligible, so the coalescer sees neighbours together.
- `request_nowait` is a synchronous probe that must never wait on storage. It returns bytes,
  `WouldBlock`, or `Unsupported`.
- `prefers_background_reads` says whether planned reads should be started ahead of demand. Only
  `FileSegmentSource` overrides it to say yes (remote reads reach the same type); the cache and
  shared adapters delegate, and in-memory sources keep the default and rely on the probe.

**Rule:** the default implementations keep every existing source correct. Only sources with
their own request queue need to override the new methods.

### `VortexReadAt::read_at_nowait` and `ReadAtNowait` (`vortex-io/src/read_at.rs`)

The primitive under `request_nowait`. On Linux, `FileReadAt` implements it with
`preadv2(RWF_NOWAIT)`, which reads only if the pages are already in the page cache. Everything
else returns `Unsupported`, and an in-memory `ByteBuffer` is always `Ready`.

**Rule:** a probe hit is a free read. A miss costs one syscall and tells the executor to hand the
read out instead.

### `ReadEvent` and the file read driver (`vortex-file/src/segments/source.rs`, `read/driver.rs`)

`FileSegmentSource` turns segment requests into events for a coalescing driver task:

| Event | Meaning |
| --- | --- |
| `Request` | A read exists but is not yet eligible. |
| `BackgroundRequests` | A whole batch is eligible at once, in the background class. |
| `Polled` | One registered read is eligible in the background class. |
| `Promoted` | Somebody is waiting on this read; run it before background work. |
| `Dropped` | The future was dropped; forget the read. |

The driver keeps a promoted class ahead of the background class, coalesces adjacent ranges
(64 KiB merge distance, 2 MiB maximum), and submits one positional `read_ranges` batch up to the
configured concurrency (16 for local files).

**Rule:** polling a `ReadFuture` promotes it. That is why the executor's driver polls speculative
reads through a bounded window rather than all at once.

### `ExecPlan`, `build_plan`, and `NodeBlueprint` (`vortex-morsel/src/build.rs`)

The immutable blueprint of one scan. `build_plan(layout, projection, filter, conjunct_mode)`
walks the stored layout through the registered `LayoutPlanner`s and collects one
`NodeBlueprint` per node: `FlatSpec`, `ChunkedSpec`, `StructSpec`, `DictSpec` (pull only),
`ConjunctSpec`, and the root `FilterSpec`. Unsupported layouts are errors, never fallbacks. The
plan knows the row count, the natural split boundaries, and, through `NodeBlueprint::stored_uses`,
every `(IoKey, row range)` a leaf reads, which is what lease counting and lookahead are computed
from. The node's side of that bargain is to register each reported unit while planning a morsel
that overlaps it and release it exactly once at retire.

```rust
pub trait NodeBlueprint: Send + Sync {
    fn instantiate(&self, id: NodeId) -> Box<dyn ExecNode>;
    fn stored_uses(&self) -> Vec<(IoKey, Range<u64>)> { Vec::new() }
}
```

- `plan.instantiate()` asks every blueprint for one worker's mutable node state. Each worker
  owns one arena and reuses it across morsels.
- `morsels(&plan, target_rows)` (in `driver.rs`) cuts the row space; `0` means one morsel per
  natural split.

**Rule:** the plan is shared and read-only; all mutable state lives in per-worker arenas. A node
that reads storage must say so through `stored_uses`, because nothing else inspects a blueprint.

### `LayoutPlanner`, `LayoutPlanners`, `LayoutCx`, `SplitCx` (`vortex-morsel/src/build.rs`, `layouts.rs`)

How one kind of stored layout becomes nodes.

```rust
pub trait LayoutPlanner: Send + Sync {
    fn handles(&self, layout: &LayoutRef) -> bool;
    fn natural_splits(&self, layout: &LayoutRef, root_offset: u64, cx: &mut SplitCx<'_>) -> VortexResult<()>;
    fn plan(&self, layout: &LayoutRef, root_offset: u64, cx: &mut LayoutCx<'_>) -> VortexResult<NodeId>;
}
```

`LayoutPlanners` is an ordered registry and the entry point for planning: `build_plan`,
`build_plan_for_ranges`, and `natural_morsels_for` are methods on it, and the free functions of
the same names use the default registry. The first planner whose `handles` accepts a layout owns
it, and `with` puts a new planner ahead of the built-ins. The built-ins in `layouts.rs` are
`ZonedPlanner` (transparent wrapper), `FlatPlanner`, `DictPlanner`, `StructPlanner`, and
`ChunkedPlanner`. `SplitCx` lets a planner record chunk boundaries and recurse without
materializing indivisible children; `LayoutCx` lets it push blueprints, plan children, and open a
*lease scope*, which is how the dictionary planner says its values are used by every morsel of the
codes' range.

**Rule:** a planner answers both questions about its layout, where chunks start and which nodes
execute it. The answers sit side by side, and a test checks that the cut and the plan's natural
splits agree on every fixture. `MorselScanExecutor::with_planners` is how an engine supplies a
registry.

### `ExecNode` (`vortex-morsel/src/node.rs`)

The state machine each plan node becomes inside an arena.

```rust
pub trait ExecNode: Send {
    fn reset(&mut self, range: Range<u64>);
    fn next_plan(&mut self, cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll>;
    fn execute(&mut self, cx: &mut ExecCx<'_>) -> VortexResult<ExecPoll>;
    fn retire(&mut self, cx: &mut RetireCx<'_>);
    fn children(&self) -> &[NodeId];
}
```

- `reset` prepares the node for a new morsel in its own local coordinates.
- `next_plan` *names* reads by registering `IoUse`s and receiving tickets. It never reads. It is
  resumable from its own cursor when a child yields.
- `execute` produces a value that is dense over the node's range. The row hint in the context
  is advice about which rows the parent will look at; it never changes the value's shape. It
  may try one inline probe through `ExecCx::ready`; otherwise a missing dependency must return
  `ExecPoll::Blocked` with the exact ticket. It must never block, poll a future, or transfer
  device memory.
- `retire` releases leases for the finished morsel.

The arena drives a node by taking it out of its slot, handing the rest of the arena to the
children, and putting it back. The tree shape guarantees a node is never reachable from its own
subtree, so a taken slot is never observed empty.

**Rule:** planning names, execution consumes, retirement releases. A ticket a node never named is
an error, not an inline read.

### `PlanCx`, `ExecCx`, `RetireCx` (`vortex-morsel/src/node.rs`)

The three contexts are the only way a node touches the world.

| Context | What it offers |
| --- | --- |
| `PlanCx` | `hint()`, `register(IoBatch) -> VortexResult<Vec<IoTicket>>`, `decoded_available(key)`, `plan_child(..)` |
| `ExecCx` | `hint()`, `session()`, `ready(ticket) -> VortexResult<Option<BufferHandle>>`, `shared_decoded(key)`, `publish_decoded(key, array)`, `child_value/array/mask(..)` |
| `RetireCx` | `retire_child(id)`, `release_use(key)` |

**Rule:** a node sees its own row hint, its own tickets, and its own children. Nothing else.

### `ValueBatch` and the row hint (`vortex-morsel/src/node.rs`)

What flows back up. A `ValueBatch` is a value plus the root-coordinate `coverage` it accounts
for, and an array value always holds one row per row of that coverage. Two things carry a
selection through the tree, and they are deliberately different:

- The **hint** flows down as advice. The filter node hints the projection with the mask it
  computed, and a sparse conjunct hints its input with the incoming rows. Only the flat leaf
  reads it, for two decisions: an all-false hint at planning names no read, and an all-false
  hint at execution answers with a constant placeholder of the range's length instead of
  waiting for one. Chunked concatenates, struct zips, dict indexes; none of them look at it.
- The **conjunct's mask** flows up as a value. It is the boolean array the predicate produced,
  expressed over the morsel's whole coverage, and it is the only thing anyone applies. The
  filter root applies it once, filtering each struct field and each chunk by its own slice of
  the mask (`filter_rows`), which drops the placeholder rows along with everything else.

**Rule:** a hint may spare a leaf a read, never change what a batch looks like. Every batch is
dense over its coverage, so no node has to describe what it holds.

### `IoUse`, `IoBatch`, `IoTicket`, `IoKey` (`vortex-morsel/src/io.rs`)

How a read is named. An `IoUse` is one whole stored unit (`IoKey::Segment(id)`), its extent in
the unit's rows, the inverse image of that extent in root rows, the producing node, and an
estimated size. A `PlanCx::register` call takes an `IoBatch` of uses and returns one `IoTicket`
per use. The ticket is the handle `execute` later waits on.

**Rule:** two morsels straddling the same segment name the same key and share one read.

### `IoPlane` and `IoService` (`vortex-morsel/src/io.rs`)

Two layers of the same registry.

- `IoService` is scan-wide: one cell per key holding `Unissued`, `Requested`, `Ready(bytes)`, or
  `Failed`. Cells are shared across morsels and workers, so a read is issued once per scan.
- `IoPlane` is morsel-local: the tickets this morsel named, plus the cells it registered but has
  not yet submitted. `take_reads()` hands those to the scheduler once per planning quantum, so
  reads discovered together are submitted together (a *planning wave*).

`IoPlane::ready(ticket)` returns bytes if the cell is ready and nothing if it is already
requested. For an unissued cell it tries the probe (skipped for good once a source reports
`Unsupported`); on a miss it returns nothing so the node blocks on the ticket, and the scheduler
hands the read out as required demand when it parks the worker. Only a cell nothing ever
submitted is handed out directly from here.

**Rule:** the service dedupes; the plane batches. Neither performs I/O.

### `IoDemand`, `IoRequest`, `IoDemandStream`, `IoCompletions`, `NowaitProbe` (`vortex-morsel/src/io.rs`)

The handoff added on 2026-09-02. Plan execution no longer owns a segment source.

```rust
pub struct IoRequest { pub key: IoKey, pub priority: IoPriority }
pub enum IoDemand {
    Start(Vec<IoRequest>),  // one scheduling batch; register it whole
    Promote(IoKey),         // execution is blocked on this read
}
pub type IoDemandStream = mpsc::UnboundedReceiver<IoDemand>;
pub struct IoCompletions { .. }   // complete(key, VortexResult<BufferHandle>) -> bool
pub type NowaitProbe = Arc<dyn Fn(IoKey) -> VortexResult<ReadAtNowait> + Send + Sync>;
```

- `MorselScan::take_io()` returns the stream and the completions handle exactly once.
- The stream ends when the scan is dropped; completions after that are ignored (the handle holds
  a weak reference).
- Workers park on their exact cells. A completion wakes exactly the workers parked on that cell.
- `with_nowait_probe` and `with_background_reads` tell the scan how the answering side behaves.

**Rule:** the scan says what it wants and when it is blocked; it never says how to read.

### `IoAnswerer` and `SegmentSourceDriver` (`vortex-morsel/src/source.rs`)

Whoever serves the demand stream.

```rust
pub trait IoAnswerer: Send + Sync {
    fn prefers_background_reads(&self) -> bool { false }
    fn nowait_probe(&self) -> Option<NowaitProbe> { None }
    fn serve(&self, demand: IoDemandStream, completions: IoCompletions) -> BoxFuture<'static, ()>;
}
```

`MorselScan::connect(&answerer, &handle)` spawns `serve` on a runtime and
`MorselScan::connect_on_thread(&answerer)` runs it on a dedicated thread for callers without a
runtime, such as the test harness. Both consume the scan and return it configured for the
answerer: use the returned value.

`SegmentSourceDriver` is the standard answerer over any `SegmentSource`.

- A `Start` batch becomes one `request_background_batch` call (or one `request` per id for
  demand-driven sources), so coalescing sources still see the wave.
- Required reads are polled immediately. Speculative reads wait in a queue and are polled through
  a bounded window (16 by default) in demand order; a `Promote` pulls a queued read straight into
  the polled set, bypassing the window, and a promotion that arrives before its own start batch
  is remembered until the batch does. Because polling promotes a file read, this keeps the file
  driver's promoted class meaningful.
- Device buffers are copied to the host before completion.

**Rule:** one task per scan, outside plan execution, replaceable by anything that can answer
`IoDemand`.

### `SharedCells` (`vortex-morsel/src/cells.rs`)

Cross-morsel reuse of decoded chunks by *leases*. Before the scan starts the driver counts, from
the morsel cut and the plan's flat uses alone, how many morsels will touch each unit. The first
morsel to decode publishes; every retiring morsel releases; the last release drops the array.
Units with a single lease are not registered at all. The ledger is asserted to drain to zero.

**Rule:** retention derives from demand, never from a budget. There is no eviction policy.

### `MorselScan` and `MorselExecutor` (`vortex-morsel/src/driver.rs`)

`MorselScan::new(plan, session)` configures one run: threads, morsel cut or sparse demands,
sharing, lookahead, observability, completion sink. `MorselExecutor::new(plan, threads)` owns the
worker arenas; `MorselExecutor::shared` (pull only) reuses a process-wide pool sized to the
machine, running inline for one thread and falling back to a dedicated pool when more threads
than the machine has are requested. `executor.run(&scan)` returns batches in row order plus
`ScanStats`, unless a completion sink is set, in which case batches go to the sink and the
returned list is empty. The push crate builds a worker pool per scan instead.

Inside, a `Scheduler` assigns morsels to workers, submits planning waves, keeps a lookahead
window of future morsels registered as speculative reads on filtered scans, and parks a blocked
worker until its exact cells complete.

**Rule:** a worker owns one arena and one morsel; a blocked worker does not pick up another
morsel.

### `MorselScanExecutor` and `PushMorselScanExecutor` (`*/src/executor.rs`)

The engine-facing wrapper around a layout and a segment source. `full_file_splits` exposes the
plan's natural boundaries; `build(session, projection, filter, row_range, selection, limit,
row_offset)` returns one future per morsel, completed through a sink as that morsel retires. It
caches plans by projection and filter, connects a `SegmentSourceDriver` on the session runtime,
and runs the scan on a blocking task.

**Rule:** row offsets are rejected. A limit is exact on unfiltered scans, where morsels past it
are never read and the last one is capped; a filtered scan cannot know where the limit falls, so
it returns every matching row and `MorselScanBuilder` trims. Dropping every returned future
cancels the scan.

### `MorselScanBuilder`, `ScanBackend`, `ScanExecutorOptions` (`vortex-morsel-scan`)

The facade the SQL engines use. `ScanBackend` is `V1`, `Pull`, or `Push`, read from
`VORTEX_SCAN_BACKEND` and defaulting to V1. `MorselScanBuilder` mirrors the surface of
`vortex_layout::scan::scan_builder::ScanBuilder` (projection, filter, selection, row range,
ordering, concurrency, limit, map, `into_stream`) but never constructs a `LayoutReader`.
DataFusion wraps both builders in an enum; DuckDB picks one per file.

**Rule:** V1 keeps its `LayoutReader`; the morsel backends get a raw layout and segment source.

### Push-only surface (`vortex-morsel-push`)

The push crate is a fork of the pull crate with a second value-execution contract.
`ExecutionMode::Pull` is the recursive oracle; `ExecutionMode::Push` activates leaf sources and
routes batches upward through compiled pipelines. `ExecNode` gains `push_start`, `push_input`,
`push_end`, and `push_resume`. `ActivationTarget` decisions are authoritative; `DemandTarget`
hints are advisory and may suppress or promote unissued reads. `MorselScan::into_stream` gives
ordered, credit-bounded output with cancellation, and `run_on_current_thread` lets an engine
thread (DuckDB) drive morsels itself, ticking its own runtime while it waits.

### Smaller types you will meet

| Type | Where | What it is |
| --- | --- | --- |
| `IoPriority` | `io.rs` | `Required` or `Speculative`, carried by every `IoRequest`. |
| `Wait`, `WaitSet` | `node.rs` | What `ExecPoll::Blocked` carries: the exact tickets to park on. |
| `PlanPoll`, `ExecPoll`, `ChildPoll` | `node.rs` | The poll results of planning, execution, and child calls. |
| `Value`, `ValueBatch` | `node.rs` | An array or a mask produced by `execute`, with its row count. |
| `Arena`, `NodeId` | `node.rs` | One worker's node state and the index that names a node in it. |
| `ConjunctMode` | `nodes/conjunct.rs` | `Cascade` (default) or `Parallel` predicate evaluation. |
| `CompletionSink` | `driver.rs` | `Fn(morsel index, Option<ArrayRef>)`; receives each morsel's batch as it retires. |
| `ScanStats`, `MorselTrace` | `stats.rs` | The run's counters, and the opt-in per-morsel record. |
| Executor defaults | `executor.rs` | 4 threads, 128 Ki rows per morsel, 16 lookahead morsels, `Cascade`. |
| `PushCx`, `NodeState`, `MorselStream` | push crate | Push-mode context, node lifecycle state, and the ordered output stream. |
| `ScanExecutorOptions::with_external_threads` | `vortex-morsel-scan` | Lets an engine's own threads drive morsels and tick its runtime. |

## Part 4: extension points, and which are traits

Three things vary by design and are traits. Everything else is deliberately concrete.

| Extension point | Shape | Why this shape |
| --- | --- | --- |
| A new stored layout | `LayoutPlanner`, registered in `LayoutPlanners` | Layouts are the open set in Vortex; V1 extends the same way through `LayoutVTable::new_reader`. Before this, two duplicated `match`es in `build.rs` had to be edited per layout, and the split walk and the plan walk could disagree. A planner owns both answers for its layout. |
| A new kind of node | `NodeBlueprint` plus `ExecNode` | A planner must be able to introduce node types the crate has never seen. The blueprint is the immutable half a worker instantiates; the exec node is the mutable half. Only `stored_uses` is inspected from outside, so the scheduler stays ignorant of node types. |
| A new way to perform reads | `IoAnswerer` | The scan only ever sees a demand stream and a completions handle. An engine's buffer manager, an object-store prefetcher, or a test double that scripts latency can serve it without implementing `SegmentSource`; `SegmentSourceDriver` is one answerer among possible others. |
| A new operator between layouts and output | `ExecNode` | Already the per-node contract; six operators implement it. |

Kept concrete, and why:

- **Morsel cutting.** `morsels`, `natural_morsels_for`, and `with_morsel_demands` are data:
  ranges and masks. A policy trait here would hide which rows a scan reads, which is the one
  thing every experiment has needed to see.
- **Lease cells.** `SharedCells` derives retention from the morsel cut; the design notes record
  that an earlier cache measured itself rather than the executor. A trait would invite caches
  back in.
- **Completion sink and probe.** Closures. The probe reaches the scan through `IoAnswerer`, but
  its type stays a closure; a trait would add nothing but a name.
- **The stored unit itself.** `IoKey` has one variant, a segment. Every trait above speaks in
  segments, so a layout whose unit is not one segment needs a new key variant first. That is the
  real closed set, and it is closed on purpose until a second unit type exists.
- **Worker hosting.** `MorselExecutor` has one inline and one pooled mode; the push crate's
  external-thread mode is the only other policy seen so far, and it is not yet settled enough
  to freeze behind a trait.
- **Backend selection.** `ScanBackend` is an enum of two prototypes plus V1; an enum is honest
  about that.

## Part 2: one scan, end to end

1. The engine calls `MorselScanBuilder::build` (or `into_stream`).
2. The executor builds or fetches the plan, cuts morsels, and creates a `MorselScan`.
3. It takes the scan's demand stream and spawns a `SegmentSourceDriver` on the session runtime.
4. The scan starts on a blocking task. On sources that prefer background reads the scheduler
   registers the initial window up front: every flat read of the plan as required on an
   unfiltered scan, or two morsels per worker plus the lookahead as speculative on a filtered
   one. They leave as `Start` batches of up to 64 reads.
5. A worker takes a morsel, resets its arena, and plans. Each flat node registers a use and gets
   a ticket. A wave is one morsel per worker; it flushes when all of them finish planning or one
   blocks, and the scheduler starts the reads it discovered. Required reads go out eagerly only
   when the source prefers background reads or the probe is unsupported; otherwise they wait for
   the inline probe.
6. The driver registers the batch with the source and polls required reads at once.
7. The worker executes. `ExecCx::ready` returns bytes for ready cells, tries the probe for
   unissued ones, and otherwise blocks on the exact ticket. The scheduler promotes that key.
8. The driver pulls the promoted read out of its queue into the polled set, the completion
   lands in the cell, and the parked worker wakes and resumes the same morsel.
9. The morsel retires, releasing leases, and its batch goes to the completion sink, which
   resolves the engine's per-morsel future.
10. The scan finishes; dropping it ends the demand stream, and the driver task exits.

## Part 3: what the experiment taught

### From the synthetic P1 evaluation

- Five design points survived contact with the code unchanged: nodes that never perform I/O,
  emit-once resumable planning, an immutable plan with per-thread mutable arenas, the arena
  take-and-put trick, and unsupported shapes as build errors. The first and third carry the
  most weight.
- Leases beat caches. An earlier decoded-chunk cache was removed because a budget-and-eviction
  cache is state V1 does not have; its numbers measured the cache, not the executor.
- A single mutex around the cell map made four threads slower than one. Sharding it sixteen ways
  fixed it. Anything touched per (node, morsel) must be sharded or lock-free from the start.
- With sharing disabled the executor still beat V1 at equal thread count (0.644 geomean). That
  row is the floor; sharing builds on it rather than inflating it.
- Coalescing morsels and sharing decodes are substitutes on wide numeric data and complements on
  misaligned string data.

### From real TPC-H at scale factor 1

- The executor win survives real encodings at a smaller magnitude: about 1.3x at one thread and
  1.5x at four against V1's default runtime configuration. Against a V1 swept to its best split
  concurrency (64 in-flight split tasks against four morsels) the four-thread ratio is 0.61x.
- Decode reuse is neutral on real files, because the write pipeline repartitions every column
  onto the same row blocks. It fires only on width-divergent schemas (Q19's strings) and on
  filter-and-project overlap, and the latter saves decodes without saving wall time.
- Registering a lease for every unit cost 20% on a bare projection. Single-lease units are now
  skipped entirely.
- Morsel coalescing is neutral except where columns misalign, and harmful past the working-set
  bound.
- Time to first batch is an order of magnitude earlier because a morsel emits as soon as it
  finishes.

### From the SSD random-access work

- Mask-aware planning, so a sparse morsel builds only the children its selection touches, and a
  fixed row range is only a scheduling envelope.
- A persistent process-wide worker pool with no fixed cap (pull crate only); reopen-heavy
  workloads must not spawn threads per lookup.
- Planning waves replaced crossbeam hand-off queues: a worker waiting on its exact ticket had no
  other morsel to run, so the hand-off added synchronisation without hiding latency.
- Batched, priority-aware buffered I/O with the file driver coalescing adjacent ranges won ten of
  eleven random-access cases against V1 in reopen mode, and seven of eleven with cached handles.

### From the integration on 2026-09-02

- Two forks of one crate are expensive. The push crate re-imported the pull crate at an older
  commit and diverged heavily; it lacks dictionary and nested struct support, so in this
  session's runs the push backend failed 17 DataFusion and 12 DuckDB tests that V1 and pull
  pass. Pick one.
- A dedicated `MorselScanBuilder` kept `vortex-layout`'s `ScanBuilder` and `LayoutReader`
  untouched (the segment-source trait there did change) at the cost of duplicating the builder
  surface and an enum wrapper in each engine. What it lost is zone-statistics pruning:
  the earlier hook pruned through the V1 reader, and the morsel plans treat zoned layouts as
  transparent wrappers. DuckDB's whole-file `can_prune` still runs.
- Handing I/O out of plan execution removed every `SegmentSource` reference from planning,
  execution, and the scheduler, removed the push crate's I/O work queues and re-queueing wakers,
  and made the executor testable against any demand answerer. The costs: a scan needs an
  answerer, so running one whose demand was never taken is refused; runtime-less callers need a
  thread; `io_waits` now counts reads answered through completions rather than worker poll
  attempts; a scan is cancelled, parked workers included, once every consumer has dropped its
  output future.
- Polling is promotion in V1's file driver. A driver that polls everything immediately promotes
  everything, which is why the driver windows speculative reads.
- Several global defaults changed for V1 as well and need their own benchmarks: the file
  coalesce config (1 MiB/4 MiB to 64 KiB/2 MiB), local read concurrency (32 to 16), the request
  stream batch size (fixed at 1), and a poll now sending `Promoted` instead of `Polled`.

### Open questions

- Which executor survives, and whether the push value-execution contract is worth its size.
- Whether zone-map pruning belongs in the plan (pruning the morsel cut) or in an answerer that
  can refuse reads.
- Whether the driver's background window should track the source's real concurrency instead of
  a constant.
- How the executor behaves against remote object storage, where gate E2 in the design documents
  was meant to decide when registration should be bypassed.
