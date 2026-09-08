# Aggregate contracts

[Research](../../../README.md) | [Design context](../background.md) | [Validation](../validation.md)

`can_satisfy` reports compatibility without providing the conversion between partial-state shapes.
`Max` has a scalar partial. `BoundedMax`, which limits storage for variable-length maxima, has a
`{bound, unknown}` partial. Accepting one for the other therefore requires more than a compatibility
flag.

## Reproduced failures

1. `ZoneMap` unconditionally extracts `.bound` from stored `BoundedMax`, even when the request is
   for `BoundedMax` itself. Accessing `bound` on that result fails because the result is already
   scalar.
2. `Max` advertises that it can satisfy `BoundedMax`, but the binder does not construct the
   requested struct. The prototype constructs `{bound, unknown: false}` and preserves null outer
   state.
3. Default matching checks Rust type and configuration but omits the aggregate ID. Two
   `ForeignAggregateFn` values with different IDs and equal configuration match despite comparing
   unequal.

The foreign-aggregate test demonstrates the identity mismatch. `ForeignAggregateFn` cannot execute,
so that result does not establish a false negative in a Bloom query.

## Proposed contract

The prototype replaces `can_satisfy` with `resolve_stat`, which returns the conversion expression
alongside the match quality:

```rust
pub enum StatMatch {
    Exact(Expression),
    Approximate(Expression),
}
```

`resolve_stat` returns `Option<StatMatch>`. Each expression must produce the requested partial-state
representation, including its empty and unknown states. An approximate expression must preserve the
bound direction required by conservative pruning. `None` means that the aggregate cannot supply it.

`StatFn` already exposes partial state, including structs. Automatic finalization changes that
contract and does not solve the conversion problem. `ZoneMap` was the only production consumer of
`can_satisfy`, so the prototype replaces the method rather than maintaining two matching contracts.

| Alternative | Benefit | Tradeoff |
| --- | --- | --- |
| Check the requested type in ZoneMap and fix ID matching | Repairs the immediate cases | Leaves aggregate-specific conversions in the layout. |
| Aggregate-owned resolution | Matching and conversion have one implementation | Each aggregate must preserve the requested representation and bound semantics. |
| Permit only identical states | Reduces substitution rules | Loses existing useful substitutions. This alternative was not implemented. |

The first two alternatives were implemented. Aggregate-owned resolution removes the `BoundedMax`
special case from `ZoneMap` and gives other aggregates the same conversion interface.

## Other contract gaps

`ZoneMap` also uses `Display` output as an equality shortcut. Foreign aggregates can have different
metadata but identical display strings. The prototype removes that shortcut, but physical summary
fields still use those strings as names. Rejecting duplicate names or assigning physical slots
remains necessary to remove that collision risk.

The following findings come from source inspection:

- `Count`, `Mean`, `First`, `Last`, `IsConstant`, `IsSorted`, and `AllNonDistinct` have
  `unimplemented!()` serializers despite the existing `Ok(None)` contract for unsupported
  persistence. Returning `Ok(None)` permits a write error instead of a panic. Persisted
  implementations need serialization and registration with round-trip tests. A separate persistence
  trait duplicates that existing capability boundary.
- The schema helper can fall back to an extension type's storage dtype without converting the input
  sent to accumulation. The aggregate must support the actual dtype, or an adapter must convert both
  consistently. The strict writer prototype checks the actual dtype.
- A bare `Scalar` partial carries no aggregate or configuration provenance. Combining partials
  requires compatible input types and identical aggregate configuration. Tagged partials remain an
  unimplemented alternative.

The structured Mean test exercises partial access, not a persisted Mean round trip.

[Narrow repair](../experiments/aggregate-contract-narrow.patch), [generic
conversion](../experiments/aggregate-contract-generic.patch), [single-method
cleanup](../experiments/stat-match-polish.delta.patch),
[reproductions](../experiments/aggregate-contract-repros.patch), [binder and regression
tests](../../../vortex-layout/src/layouts/zoned/zone_map.rs).
