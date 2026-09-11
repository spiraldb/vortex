# Short-lived scalar probes

Repeated scalar reads can redo validity evaluation, child dispatch, and decompression.
`ArrayProbe` gives an encoding a place to retain work for a short series of lookups,
without requiring callers to understand the encoding or execute the entire array.

## Caller API

```rust
let mut probe = array.probe(ProbeUsage::Repeated);
let first = probe.scalar_at(first_index, &mut ctx)?;
let second = probe.scalar_at(second_index, &mut ctx)?;

let single = array.probe(ProbeUsage::Once).scalar_at(index, &mut ctx)?;
```

The probe borrows an `ArrayRef`. Construction allocates nothing and does not execute
anything. Scalar results own their values, include nullness, and can outlive the probe.
Bounds are checked before entering the encoding hook. A separate validity probe is not
needed: each encoding can resolve validity together with the value.

`Repeated` permits preparation and reuse. `Once` requests no retained state; it is a
caching hint, so calling a one-off probe multiple times is still valid. Dropping the probe
releases its local state, child probes, and decoding resources. Probes are thread-local.

## Vtable API

The hook lives on `OperationsVTable`, selected by the array's existing `VTable`:

```rust
type ProbeState<'a>: Default + 'a;

fn probe_scalar<'a>(
    array: ArrayView<'a, V>,
    index: usize,
    probe: Option<&mut ProbeCtx<'a, Self::ProbeState<'a>>>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    array.array().execute_scalar(index, ctx)
}
```

All encodings initially select `ProbeState<'a> = ()` and use this default, including
primitive, PCO, FastLanes RLE, RunEnd, ScalarFn, and Zstd. The API change provides no
encoding-specific speedup on its own; optimized hooks are a separate follow-up.

An encoding opting into preparation chooses its own concrete `ProbeState`. `Default`
should be cheap and avoid allocation or execution. Fallible preparation belongs in the
hook. The framework initializes one context on the first in-bounds repeated lookup and
passes that same context on subsequent calls. `Once` passes `None` without constructing
local state or child storage. The hook must preserve the logical dtype and handle nulls;
its contract differs from the legacy non-null `scalar_at` hook.

## Local state and children

```rust
pub struct ProbeCtx<'a, S> {
    state: S,
    children: ProbeChildren<'a>,
}
```

Encodings access local state with `probe.state_mut()` and request a retained child probe
with `probe.child(slot)?`. The framework binds child lookup to the original source's
slots and creates each child probe on its first request. Repeated requests reuse it;
creating the child does not initialize its encoding context or execute it.

When local state and children must be borrowed simultaneously, use:

```rust
let (state, children) = probe.parts();
```

These are disjoint mutable borrows of the context's fields. Encoding-specific state does
not need to store child probes manually. A routing encoding can keep unit local state and
request the needed slots; a decoding encoding can keep owned indexes and decoded buffers.

Each child has its own context and lazy child cache, so the API supports arbitrary nesting
when every parent routes through child probes. The cache key is `(parent probe, slot)`.
Separate roots have independent caches, and two slots referencing the same array still
have independent child state. Default hooks keep their existing scalar execution path.

The child table is a `Vec<Option<ArrayProbe<'a>>>`. It starts empty and allocates cells
for the source's slots on the first valid child request. Unrequested cells remain `None`.
Missing or out-of-bounds slots return an error before allocating a table. A dense table
reserves space for all slots, so wide arrays should be considered when measuring setup
cost. The vector provides the indirection required for recursive storage.

## Lifetimes and ownership

`'a` is the borrow of the root array. Every child slot is reachable for that lifetime;
child probes borrow the original slots without cloning array handles. Local state may
borrow views into the same source tree and own anything it prepares itself.

A state cannot build a new array and store a probe borrowing that new array inside itself:
that would be self-referential. Owned decoded buffers can instead be read directly.
`use<'a>` controls capture for opaque `impl Trait` returns and does not replace the
associated state's outlives bound.

## Storage and dispatch

The entire typed `ProbeCtx<'a, S>` lives in erased storage owned by the probe. The holder
provides 128 inline bytes aligned to 16 bytes and a heap fallback for larger or more aligned
contexts. The child-cache header counts toward that capacity. Child tables, indexes, and
decoded buffers may allocate separately when an encoding needs them.

```text
ArrayProbe::scalar_at
  -> existing DynArrayData::probe_scalar dispatch
  -> ArrayData<V> retrieves ProbeCtx<'a, V's associated state>
  -> OperationsVTable::probe_scalar
```

The adapter initializes the context with a source-bound factory, called once. Retrieving
it later uses a pointer cast with a fixed concrete type rather than an `Any` lookup. The
holder records a destructor for that exact type and drops it once, freeing a spilled
allocation and recursively dropping child probes. No pointer into inline storage is kept
across moves.

All raw storage operations are confined to the holder. Its safety requires every access
to use the same concrete context type, including lifetimes. The probe's fixed source and
that source's vtable enforce this invariant. An invariant lifetime marker prevents erased
borrowed state from outliving the source. The holder is neither `Send` nor `Sync`, since
encoding state is not required to implement those traits.

Tests cover inline and spilled storage, alignment, moves, borrowed state, exactly-once
initialization and destruction, default scalar behavior, bounds and nulls, lazy child
creation, repeated slot reuse, source binding, independent slots/contexts, and simultaneous
local-state and child access. Encoding follow-ups should test recursive cache reuse and
benchmark complete probe lifetimes, including preparation and teardown.
