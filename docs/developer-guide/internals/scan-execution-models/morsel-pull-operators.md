# Pull Operators as State Machines

The pull executor's node layer, as built on 2026-09-09: owned children, one context, three
calls, and every operator an explicit state machine. It replaced `ExecNode`, the arena, `NodeId`
dispatch, and the three contexts. It kept the planning wave, exact-ticket parking, per-worker
trees, and leases, which are where the measured wins came from. Nothing in it is a future or a
waker; a worker still parks on the cells it named.

Read the [primer](morsel-executor-primer.md) first for the model. This page is the contract and
the rules each operator follows; the operators themselves live in `vortex-morsel/src/nodes/`
and are short enough to read directly.

## The contract (`vortex-morsel/src/node.rs`)

```rust
pub struct Batch { pub coverage: Range<u64>, pub value: Value }   // Value = Array | Mask

pub enum LookAhead { Complete, Blocked }
pub enum Step { Batch(Batch), Finished, Blocked }

pub trait Operator {
    /// The row domain supplied at construction.
    fn row_domain(&self) -> &RowDomain;
    /// Register discoverable reads in the context, or block on a dependency needed to find more.
    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead>;
    /// Produce the next value. A missing dependency is `cx.wait(ticket)` plus `Blocked`.
    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step>;
    /// Release every lease held for the morsel.
    fn close(&mut self, cx: &mut Cx<'_>);
    /// One line for traces; the tree appends the range.
    fn describe(&self) -> String;
}
```

A parent owns its children as `Child` (an operator plus its pre-order position in the tree,
which is only used to name it in traces) and calls `look_ahead`, `next`, and `close` on them directly. There
is no arena, no take/put, no `children()`, and no `Send`: a tree is built on the worker that
runs it and never leaves it.

### One context

`Cx` is what an operator can reach while it runs, and the only place it pushes anything:

| Phase | Methods |
|---|---|
| look_ahead | `register(IoUse) -> ticket`, `ready(ticket)`, `wait(ticket)`, `decoded_available(key)`, `with_priority(p, f)` |
| next | `ready(ticket)`, `wait(ticket)`, `shared_decoded(key)`, `publish_decoded(key, array)`, `dictionary(slot)`, `publish_dictionary(slot, array)`, `session()`, `stats()` |
| close | `release_use(key)` |
| always | `morsel: MorselRows { range, demand }`, `tees: &mut [MaskBuffer]` |

Operators push what is not a value: named reads, tickets to park on, published decodes, stats.
Values come back as `Step::Batch`, with one deliberate exception below. Routing ordinary
batches through the context would be the push model again.

### Row domains, demand, and resumption

Each operator receives a `RowDomain` at construction. `domain.demand(range)` returns a
`DemandRef` to attach to an `IoUse`. The reference observes later mask refinements; it does not
freeze the selection at registration. Chunk children slice and rebase their parent's domain,
struct fields share it, and dictionary values have a separate domain from their codes.
Demand only narrows. The root refines the body's demand when its predicate produces a mask;
each conjunct refines the demand of later conjunct inputs.

Both look-ahead and execution use the same `Cx`. A flat leaf registers its segment once through
`cx.register`, with its demand attached. A parent calls each independent child in order,
collects all blocked dependencies, and returns `Blocked` after visiting every child. `Child`
remembers completed look-ahead, so resumption only calls children that still have work.
There are no planning budgets or yields.

`IoRequest::demands()` exposes the live uses of a deduplicated read to the storage driver.
Further uses can join after submission. A request created by scan-level prefetch may initially
have no operator uses, which does not mean that the segment is unwanted. Attaching demand does
not cancel an already-submitted physical read.

## The driver (`vortex-morsel/src/driver.rs`)

`LocalMorsel` keeps its two phases. `Tree` owns the root `Child` and the mask buffers, and lends
them to one call at a time:

```text
assign_next   plan.instantiate_with_demand(range, demand)  // fresh tree and row domains
Plan          tree.look_ahead(range, &demand, env)         // -> (LookAhead, WaitSet)
Execute       tree.next(range, &demand, env)    // -> (Step, WaitSet); Blocked parks on the set
finish        tree.close(range, &demand, env)   // leases released exactly once
```

Each worker constructs and drops a tree for each morsel it drives. Discovered reads are batched
across a planning wave. If look-ahead blocks, the wave submits its reads immediately so the
dependency can complete. Planning resumes when any named dependency settles; other children
may still be blocked. Execution starts after the root's look-ahead completes.

## The mask buffer (`vortex-morsel/src/tee.rs`)

One mask producer, many readers, and no shared node. The producer (`ConjunctExec` or
`DemandExec`) is an ordinary owned child of the root. The mask lives in the context, one
`MaskBuffer` per tee in the plan (one today), owned by the worker and cleared per morsel. The root
pushes pieces in; every filter reads its own rows out with its own cursor. Both sides hold
`&mut Cx` when they run, so reads and writes are plain borrows: no `Rc`, no `RefCell`, no lock.

- **Readers register.** A filter calls `register(coverage)` on its first `look_ahead` call,
  so it registers exactly once per morsel and before anything is pulled. Filters that a chunk cut
  leaves out never register.
- **Pieces know what they owe.** A pushed piece's `expected` servings are the sum of every
  registered reader's overlap with it. A piece is dropped when `served == expected`. This is
  exact under any polling order, and it does not assume one reader per column: a chunked column
  whose chunks lower to different layouts puts a different number of filtered leaves over
  different rows, and the registered coverages capture that.
- **Errors are precise.** A request below `produced_end` that finds no piece asked for rows every
  reader already took. A request past the producer's end asked for rows that will never exist.

The root fills the buffer eagerly: `ProjectExec::next` drains its mask child into the buffer
before it pulls the body, so a filter never finds its rows missing. Lazy refill, where the root
pulls the producer only when a filter outruns the buffer, is the follow-up that makes pieces
overlap with body work; `serve` already returns `None` for that case.

## The operators (`vortex-morsel/src/nodes/`)

| Operator | Owns | States | What it does |
|---|---|---|---|
| `ProjectExec` | mask, body | `FillMask → PullBody → Emitted` | Plans the mask under `Required`, the body under `Speculative` when filtered. Drains the mask into `cx.tees[tee]`, refines the body's demand, pulls the body, and applies the projection. |
| `FilterExec` | one leaf subtree | `PullMask{cursor, masks} → PullInput{masks} → Emitted` | Registers its coverage once during look-ahead. Serves its rows out of the buffer piece by piece, returns empty without touching the leaf when every piece is all-false, otherwise pulls the leaf once and filters per piece. |
| `ConjunctExec` | one input per slot | `Evaluating{slot, mask} → Emitted` | Plans every slot up front. Evaluates in cascade from its row domain, refines later inputs' demand, and answers once. |
| `DemandExec` | nothing | `emitted` | Answers `morsel.demand` once. |
| `FlatExec` | nothing | `Unplanned → Named{ticket} → Emitted → Closed` | Registers its segment and live demand once, or skips it when demand is all-false or the cell is already decoded. Decodes from ready bytes, publishes, and slices. `close` releases its pre-counted lease, including when look-ahead was skipped. |
| `ChunkedExec` | one child per chunk | `Pulling{cut, parts} → Emitted` | Cuts the morsel against its offsets at construction. Look-ahead visits every cut and aggregates blocking; execution collects each child's array. |
| `StructExec` | fields, optional validity | `Pulling{validity, field, fields} → Emitted` | Plans every child, continuing past blocked children. Pulls validity and fields under the same hint, then zips. |
| `DictExec` | values, codes | `Pulling{values} → Emitted` | Plans the whole values domain unless the scan's dictionary slot is filled, plus the codes domain. Publishes values scan-wide. |
| `EvalExec` | one child | `emitted` | Applies its expression; placeholder rows on an all-false hint without pulling the child. |

Every parent turns a child's `Finished` into an error, because every operator produces exactly
one value per morsel today. `Finished` is kept as the normal end of a child so that a producer
can yield pieces later without changing the contract.

## Lowering (`vortex-morsel/src/build.rs`)

There is no intermediate representation between the physical plan and the tree. `ExecPlan` is
the `PlanRef` itself, split into the projection, the conjunct slots (each a plan producing that
conjunct's input plus its scoped predicate), and the body, together with the few facts the
driver needs before any worker runs:

- `natural_splits`: every chunk end under the body and the conjunct inputs, for cutting morsels.
- `uses`: every segment with the root rows whose morsels lease it, for lease counts and
  lookahead. A `Take`'s values are leased over the codes' whole range.
- the operator and dictionary counts, for the tracing span and the scan's dictionary slots.

Two walks share one recursion shape. `Survey::visit` runs once at `from_plan` and collects the
facts above, forcing every plan child so unsupported operators fail here. `Builder::build` runs
once per worker in `instantiate` and produces the owned tree directly: a `SegmentScan` becomes a
`FlatExec`, wrapped in a `FilterExec` when the walk is under the filter's input; a `Concat`
becomes a `ChunkedExec` with one `Option<Child>` per chunk (`None` for chunks a range-scoped
plan left out); `Pack`, `Take`, and `Eval` map one to one. Root offsets, lease scopes, and
filter placement are arguments carried down the recursion, and trace ids and dictionary slots
are counters assigned in pre-order, so every worker names the same operator the same way.

`scan_plan` and `scoped_eval` are unchanged: query in, plan out.

## What this removed and what it kept

Removed: `ExecNode`, `Arena`, the epoch stamps and reset-once rule, `PlanCx`/`ExecCx`/`RetireCx`,
`PlanPoll`/`ExecPoll`/`ChildPoll`, `Progress`, `ScanCaches`, `children()`, the re-answer branches
in the mask producers, and the `Send` bound. `NodeId` survives only as the pre-order trace id that
names an operator in traces and I/O attribution.

Kept: planning the whole morsel before pulling values; parking on exact tickets and resuming
from per-operator state; trees built on their worker; leases released once at close; planning
waves, with early submission when look-ahead needs I/O to discover further reads.

## Follow-ups

- Lazy mask refill: let the root pull the producer only when a filter's `serve` returns `None`,
  by pushing a mask wait the root services before re-pulling the body. Needed only once the
  conjunct yields pieces.
- Replace the `Waker`-based park in `Scheduler::wait` and `IoCell.waiters` with a parker, which
  removes the last `futures` use from the worker.
- The primer's `ExecNode` and context cards describe the arena design and need the same update.
