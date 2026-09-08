# Existing AggregateFn contracts

[Landing page](../../../README.md) | [Background](../background.md) | [Validation](../validation.md)

`Max` keeps a maximum value. `BoundedMax` limits the bytes retained for variable-length maxima.
Its partial state contains a conservative bound and an `unknown` flag. These different state shapes
make the distinction between compatibility and conversion observable.

`StatFn` intentionally exposes the aggregate's partial state, not its finalized result. That
supports structured index states already. Automatic finalization changes this contract and does not
repair the representation mismatch.

Three concrete defects appeared in the existing code:

1. **BoundedMax loses its state shape.** ZoneMap unconditionally extracts `.bound`, including when
   the requested aggregate is BoundedMax itself. A subsequent access to the partial's `bound` field
   fails because the intermediate result is already scalar. The regression failed before the fix and
   passed afterward.
2. **Compatibility does not supply conversion.** Max advertises that it can satisfy BoundedMax, but
   the stored scalar and requested `{bound, unknown}` partial differ. The prototype constructs the
   requested shape and preserves null outer state.
3. **The default compatibility test omits aggregate ID.** It checks Rust type and options. Two
   actual ForeignAggregateFn values with different IDs and equal options are reported as exact
   matches despite comparing unequal. This is a demonstrated identity bug. ForeignAggregateFn cannot
   execute, so it is not evidence of an observed Bloom false negative.

| Implemented alternative | Benefit | Tradeoff |
| --- | --- | --- |
| Narrow requested-type check plus ID fix | Repairs immediate cases with a small change | ZoneMap still owns aggregate-specific representation knowledge |
| Aggregate-owned stat resolution | Compatibility includes the expression that produces the requested partial | Aggregate implementations must uphold shape, nullability, and conservative-bound semantics |
| Require identical states only (design comparison) | Very simple substitution contract | Loses useful exact/approximate substitutions already represented by the API. Not implemented |

**Prefer aggregate-owned resolution.** A boolean or precision enum alone cannot express state
conversion. The final implemented API returns `Option<StatMatch>` with `Exact(Expression)` or
`Approximate(Expression)`. `None` means no usable representation. Exact requests preserve partial
state. Cross-aggregate requests explicitly convert it. The only existing production consumer of
`can_satisfy` is ZoneMap, so keeping two competing compatibility methods is unnecessary.

The generic experiment also removes ZoneMap's Display-based shortcut for semantic equality. Actual
foreign aggregates can have different metadata but identical Display output. Physical summary field
names still use Display, so the prototype does not fully solve collisions: reject duplicate names,
or move to stable physical slots. Slot metadata also supports the proposed unknown-plugin solution.

Additional source-backed concerns, separate from the tested stat-resolution failures:

- Count, Mean, First, Last, IsConstant, IsSorted, and AllNonDistinct contain `unimplemented!()`
  serializers despite the documented `Ok(None)` unsupported-serialization contract. For unsupported
  persistence, use the existing `Ok(None)` contract and return a write error. For intended persisted
  aggregates, implement serialization and registration with round-trip tests. A separate
  persistent-aggregate capability trait makes eligibility explicit but duplicates the existing
  capability boundary. Prefer repairing that boundary first. Mean's partial-state test does not
  demonstrate a persisted Mean round trip.
- The schema helper's extension-storage fallback is not matched by automatic conversion of the input
  sent to accumulation. An aggregate must support the actual extension dtype, or a separate adapter
  must convert it consistently. The strict writer prototype checks the actual dtype.
- Merging bare Scalar partials carries no aggregate/options provenance. The contract needs
  compatible input types and identical aggregate configuration. A general tagged-partial redesign
  was not implemented or shown necessary here.

Evidence: [narrow repair](../experiments/aggregate-contract-narrow.patch), [generic
experiment](../experiments/aggregate-contract-generic.patch), [single-method
cleanup](../experiments/stat-match-polish.delta.patch),
[reproductions](../experiments/aggregate-contract-repros.patch), [combined binder and regression
tests](../../../vortex-layout/src/layouts/zoned/zone_map.rs).
