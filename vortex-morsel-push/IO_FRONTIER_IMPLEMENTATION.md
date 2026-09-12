# Grouped I/O frontier implementation notes

This document describes how grouped I/O frontiers were designed and implemented, the mistakes
found during the work, and the invariants to check when changing the implementation. It focuses
on the execution-node and I/O APIs. Benchmark configurations and policy tuning are recorded
separately in [IO_FRONTIERS.md](IO_FRONTIERS.md).

The implementation is experimental. The useful part of the prototype is the small mechanism:
execution nodes define ordered groups, and a cursor enumerates those groups incrementally across
row ranges.

## Final contract

### Node-facing API

A node that owns a logical task brackets its child walk:

```rust
cx.group_begin(IoGroupKind::Conjunct)?;
// Walk one cascade conjunct, or every conjunct in parallel mode.
cx.group_end()?;
```

A stored leaf only names its keyed input:

```rust
cx.read(IoKey::Segment(segment_id))?;
```

The context rejects nested groups, reads outside a group, a second group before the caller moves
right, and reads beyond the current poll budget. The group owner, rather than a leaf or the
scheduler, decides where a logical task begins and ends.

The current owners are:

| Owner | Group shape |
|---|---|
| I/O root | One pruning group containing the applicable auxiliary statistics reads |
| Cascade conjunct node | One group per conjunct, in declared execution order |
| Parallel conjunct node | One group containing all conjunct inputs |
| Filter node | One projection group after its predicate groups |

Chunked and struct nodes do not create groups. They propagate a child group boundary through the
layout tree.

### Plan-facing API

`ExecPlan::frontier(range)` returns one owned cursor at the first group for a range:

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
    // The cursor is now at the next group for the same range.
}

frontier.down(next_range);
// The cursor is now at the first group for next_range.
```

The cursor is two-dimensional rather than an `Iterator`:

- `right()` moves to a later, potentially speculative group for the current range;
- `down(range)` moves to the first group for another range;
- `next_io(budget)` advances only within the current group.

`right()` is legal only after `next_io` has completely enumerated the current group. `down()`
resets the walk and may be used without materializing later groups in the old range.

`IoFrontierBatch` borrows its `IoKey` slice from the cursor. The cursor clears and reuses the same
`Vec` for later polls and row ranges. A caller that needs to retain keys beyond its next cursor
operation must copy them deliberately.

### Discovery is not execution completion

`IoFrontierBatch::is_complete()` means that all keys in the current logical group have been
discovered. It does **not** mean that storage I/O, decoding, or pushed CPU work has completed.

Runtime completion remains in the push path:

```text
enumerate group keys
        ↓
complete keyed I/O cells
        ↓
source decodes and pushes batches
        ↓
pipeline gate publishes authoritative coverage
        ↓
next required source is activated
```

The gate is the completion signal for correctness. Moving the frontier right only starts keyed
cells early. When a later gate requires one of those cells, it promotes the same cell rather than
submitting a duplicate read. There is no global "all pipelines idle" completion check.

### Ownership and memory

The cursor owns one mutable prewalk arena. `down()` resets that arena rather than creating one
arena per row range. Planning and I/O prewalking reuse the same traversal fields inside each node,
but they operate on separate arena instances; they must never be interleaved on one arena.

The cursor stores only:

- its arena and current range/group position;
- a reusable `Vec<IoKey>` bounded by the poll budget;
- a few booleans describing the group and walk boundary.

It does not retain arrays, raw buffers, source activations, or a range-by-group matrix.

Raw I/O lifetime is owned by the I/O plane. Before execution, the scan computes the exact number
of flat-source uses per segment. A morsel releases every overlapping use when it retires, including
uses skipped by an all-false filter. Non-final releases decrement the cell counter; the final
release removes the registry entry, drops a ready buffer, or cancels a queued/in-flight request.

This uses one small lease-count entry per relevant segment key. It does not preload those segment
bytes. The set of resident raw buffers is governed by admitted work, while the count metadata is
proportional to the plan's unique segment keys.

Decoded-array sharing is a separate existing layer. `SharedCells` retains a decoded flat segment
across overlapping morsels and drops it after its final lease. There is not yet a general typed
retain/close API for arbitrary decoded dependencies or dictionary values.

## Implementation process

### 1. Preserve the starting point

The existing work was checkpointed before changing the I/O model, and the experiment was moved to
its own branch. This made it possible to compare the grouped model with both the earlier V1 reader
and the existing push executor without rewriting their behavior in place.

### 2. Write the semantic invariants first

The initial design was reduced to four invariants:

1. A group contains enough I/O to complete one logical task.
2. Groups for one row range have a plan-defined serial order.
3. Moving down exposes independent work for another range without moving the first range right.
4. Speculation may move right, but correctness remains controlled by pushed gates.

These invariants prevented the cursor from becoming a materialized scheduling plan.

### 3. Trace the existing push path

The implementation was followed from `ExecPlan`, through morsel planning and `IoTicket`, into flat
source activation, pipeline push, gates, and morsel retirement. This established two important
facts:

- the frontier only needs keyed I/O; source identity is already carried by execution planning;
- pushed gates already provide exact task completion, so the cursor does not need another future
  or completion-token hierarchy.

### 4. Add a side-effect-free I/O prewalk

`ExecNode::next_io` was added separately from `ExecNode::next_plan`. It walks the same node shape
but does not create tickets, access storage, mutate an execution worker, or retain decoded data.
Every node keeps enough cursor state to yield when the key budget is exhausted and resume at the
same child.

The prewalk is driven in its own arena so it cannot disturb an executing morsel.

### 5. Put group boundaries at logical owners

Group boundaries were added to the nodes that understand task semantics:

- pruning at the prewalk-only I/O root;
- conjunct grouping at `ConjunctExec`, conditional on cascade versus parallel mode;
- projection at `FilterExec`.

Flat leaves remained unaware of policy and only called `read(key)`. Chunked and struct nodes only
forwarded child boundaries.

### 6. Implement the down/right cursor

The first public shape had both a range iterator and a cursor. That duplicated the down operation
and could encourage construction of one arena per row. It was replaced with one cursor returned by
`ExecPlan::frontier(range)`. The cursor now exposes exactly the operations in the model:
`next_io`, `right`, and `down`.

### 7. Integrate without making the frontier authoritative

Frontier enumeration registers the same scan-wide keyed cells used by ordinary morsel planning.
It does not activate a source directly. The existing plan/ticket/push path remains authoritative,
which means disabling frontier lookahead cannot change query results.

### 8. Validate shape before measuring speed

Focused tests were added for:

- one cascade group per conjunct;
- one parallel group spanning all conjuncts;
- incremental one-key polling inside a group;
- pruning before predicate and projection groups;
- resetting down to a later row range;
- refusing to move right before a group is fully enumerated;
- equality with V1 under different frontier traversal choices.

Only after those tests passed was end-to-end performance measured.

### 9. Audit every retained object

The memory audit separately followed:

- timed output arrays;
- fixture-owned raw segment buffers;
- scan-wide raw I/O cells;
- decoded shared cells;
- queued and in-flight source futures;
- worker arenas and scheduler bookkeeping.

This found that correct output and good timings were not sufficient evidence of bounded memory.
Raw cells and source futures needed explicit final-use and query-teardown behavior.

### 10. Audit concurrency boundaries

The registry, cell state, completion path, and source task were checked for lock ordering and
shutdown races. Registry sharding reduced unrelated-key contention. Cell state and waiter mutation
were placed under one lock, with waiter wakeups performed after unlocking. A scan-owned source
thread is explicitly shut down and joined; a shared external service is shut down only by its
owner.

### 11. Simplify after correctness was established

Tracing actual consumers showed that the first frontier records carried metadata nobody used:
source identity, source range, and activation targets. Those fields and their public types were
removed. `IoFrontierBatch` now contains only group kind, a borrowed key slice, and the group-end
bit.

The first version also moved a fresh `Vec` out of the cursor on every bounded poll and maintained a
second set of traversal cursors in every node. The final implementation reuses the key allocation
and shares node traversal fields between the separate planning and prewalk arenas.

### 12. Verify the final abstraction

The final pass ran the complete feature-enabled crate tests, formatting, Clippy with warnings
denied, and `git diff --check`. The API documentation was then reread against the implementation so
that "group complete" could not be confused with runtime completion.

## Bugs and failed approaches encountered

These were observed during this work, rather than merely hypothetical risks.

| Bug or failed approach | Consequence | Resolution |
|---|---|---|
| Timed benchmark runs retained every output batch | Apparent executor memory included the complete result | Timed paths count rows and drop batches immediately; exactness is checked separately |
| The disk fixture still owned its generated segment buffers | A disk run appeared to retain much more scan data than it did | Fixture buffers are dropped after the pack is written |
| A "cold" run followed a hot run on the same file | `F_NOCACHE` did not reliably evict pages already made hot | Valid cold measurements use a freshly written pack before any hot run |
| Ready raw I/O cells lived until scan teardown | Raw memory grew with every segment touched | Exact final-use release removes the cell and drops its buffer |
| Unused speculative futures could outlive their final consumer | Memory and storage work continued after the result was known unnecessary | Final release sends `Cancel`; the source drops queued futures or aborts in-flight futures |
| A detached source thread could survive into the next query run | Runs were not isolated and retained futures crossed benchmark boundaries | Scan-owned drivers receive `Shutdown` and are joined on drop |
| Treating every scan as the owner of a shared I/O service | One subscan could shut down service still needed by sibling scans | Shared services disable per-subscan shutdown and are closed by their owner |
| A refill could widen a smaller configured admission window | The implementation claimed bounded memory while admitting more work | Refill targets are capped by the existing window |
| Publishing a claimed range before all its cells were registered | A worker could race the batch and issue isolated reads, destroying coalescing | Claimed and ready cursors are distinct; ready is published after registration |
| Submitting one row range at a time | Adjacent segment reads were invisible to the coalescer | Equal-depth down frontiers are registered before their batch is published |
| Background reads were also retained in `Scheduler::io_work` | Duplicate maps added memory and lock traffic without owning correctness | The scan-wide `IoCell` is authoritative for background I/O |
| One registry mutex covered all segment keys | Unrelated planning and completion operations contended | The registry is sharded by key |
| Cell terminal state and waiters used separate synchronization | Completion required extra locking and made wakeup races harder to reason about | State and waiters share one lock; wakeups happen outside it |
| The frontier exposed source origin, source range, and activation targets | Larger records, more allocation, and duplicated information already owned by execution | Frontier batches now expose only `IoKey` values |
| `next_io` moved a fresh `Vec` into every returned batch | Incremental enumeration repeatedly lost its allocation | Batches borrow a cursor-owned buffer whose capacity is reused |
| The plan exposed both a range iterator and `next_down` | Two APIs represented the same direction and encouraged per-range arenas | `ExecPlan::frontier` returns one reusable cursor with `down` |
| Every node had separate planning and prewalk traversal cursors | Larger node state and duplicated reset logic | Separate arenas reuse the same traversal fields |
| A small fixed right depth was treated as plan-independent | It stopped before all conjuncts in some plans and crossed into projection in others | Group semantics remain visible through `IoGroupKind`; policy is not encoded in the cursor |

## Review checklist: bugs to watch for

### Group construction

- [ ] Every `read` occurs between exactly one `group_begin` and `group_end`.
- [ ] Groups never nest. A layout wrapper propagates child boundaries instead of wrapping them.
- [ ] Cascade mode ends the group after exactly one conjunct.
- [ ] Parallel mode ends the group only after every conjunct input has been enumerated.
- [ ] Projection is not accidentally folded into the final conjunct group.
- [ ] Pruning reads are ordered before executable predicate and projection groups.
- [ ] Pruning keys are deduplicated when several predicate paths reference the same statistics.
- [ ] Empty or storage-free subtrees still end their group; they must not cause an infinite poll.
- [ ] A group kind remains stable across every bounded piece of that group.

### Incremental cursor state

- [ ] A budget-exhausted child resumes without being reset or replaying earlier keys.
- [ ] Each new `next_io` result contains only keys discovered by that poll, not keys from the prior
      borrowed batch.
- [ ] Clearing a batch preserves the cursor buffer's capacity.
- [ ] `right` fails until the current group is fully enumerated.
- [ ] `right` returns `false` at the end without inventing an empty group.
- [ ] `down` resets the root, all lazily visited descendants, the group index, and completion bits.
- [ ] Reusing planning cursor fields remains restricted to separate planning and prewalk arenas.
- [ ] A zero key budget is rejected rather than producing a non-progressing loop.
- [ ] No API consumer keeps a borrowed batch across a mutable cursor operation.
- [ ] The cursor never materializes all ranges or all groups for a scan.

### Push and completion

- [ ] Group discovery is not used as evidence that I/O or CPU execution completed.
- [ ] The pushed gate remains the authoritative signal that a logical task completed.
- [ ] A speculative and later-required read resolve to the same keyed cell.
- [ ] Promotion does not submit a second physical read.
- [ ] An all-false predicate can suppress later source activation without leaving unreleased uses.
- [ ] Parallel conjunct completion waits for all inputs, not merely the first ready source.
- [ ] Coverage and selection carried by a gate match the source range being activated.

### Raw I/O lifetime and races

- [ ] Every overlapping flat-source use contributes one lease before a cell can be admitted.
- [ ] Morsel retirement releases all overlaps, including unplanned or skipped sources.
- [ ] A non-final release does not take the registry lock.
- [ ] The final release removes only the exact `Arc<IoCell>` currently registered for that key.
- [ ] A concurrent new scan/use can add a lease while a final release is attempting removal.
- [ ] Completion after cancellation observes `Released` or a missing registry entry and drops its
      result.
- [ ] Final release of a ready cell subtracts its retained-byte accounting exactly once.
- [ ] The non-blocking/nowait completion path performs the same retained-byte accounting.
- [ ] Final release of a requested cell sends one cancellation and wakes no live waiter.
- [ ] Lease decrement cannot underflow; debug assertions should remain close to the atomic update.
- [ ] Waiters are extracted under the cell lock and woken after the lock is released.
- [ ] Dropping a scan shuts down and joins only drivers the scan owns.
- [ ] Dropping a stream or cancelling a query eventually retires every active morsel and future.
- [ ] The source removes abort handles and in-flight bookkeeping on normal completion and abort.
- [ ] Raw bytes return to zero between query runs; count metadata is not mistaken for resident data.

### Decoded sharing

- [ ] A segment shared by several morsels has a decoded lease for each overlap.
- [ ] The final decoded lease drops the array.
- [ ] Disabling decoded sharing does not change results or raw-cell lifetime.
- [ ] Raw bytes are not tied unnecessarily to the longer decoded-array lifetime.
- [ ] Do not assume dictionary values or arbitrary encoding dependencies use `SharedCells`; the
      general retain/close API has not been implemented.
- [ ] Any future single-flight decoder must define failure, cancellation, retain, and final-drop
      behavior without allowing unbounded registry growth.

### Concurrency and performance

- [ ] No global mutex is added to the per-key non-final release path.
- [ ] Registry operations for unrelated keys remain sharded.
- [ ] The cell lock is never held while waking a worker or sending storage work.
- [ ] A background read is not duplicated in a second scan-wide ownership map.
- [ ] Publication uses release ordering after every key in a batch has been registered; consumers
      use acquire ordering before issuing required reads.
- [ ] Equal-depth groups from different row ranges can be presented together for coalescing.
- [ ] Group order is preserved when reads from several row ranges are batched.
- [ ] Any new metadata in `IoFrontierBatch` has a demonstrated consumer; do not duplicate plan data.
- [ ] Any change from borrowed batches to owned batches accounts for allocation frequency.

### Measurement hygiene

- [ ] Timed runs do not retain output arrays unless retained output is the behavior being measured.
- [ ] Correctness comparison is separate from the timed discard path.
- [ ] Disk fixtures drop generation buffers before measuring resident memory.
- [ ] A cold result comes from a fresh pack or a demonstrated cache-eviction mechanism.
- [ ] Each query run has a fresh or correctly reset source driver and zero outstanding raw cells.
- [ ] Logical bytes and physical bytes are both reported; lower time must not hide accidental
      projection over-read.
- [ ] End-of-run raw-cell and retained-byte counts are checked, not inferred from RSS.
- [ ] RSS is interpreted relative to the post-fixture baseline because allocator pages may remain
      resident after large SF10 construction buffers are dropped.

## Tests that guard the contract

The main regression coverage is:

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

Run the complete crate validation after changing the contract:

```bash
env RUSTC_WRAPPER= cargo test -p vortex-morsel-push --features _test-harness
cargo +nightly fmt --all
env RUSTC_WRAPPER= cargo clippy -p vortex-morsel-push --all-targets --all-features -- -D warnings
git diff --check
```
