# Design context

[Research](../../README.md)

`ZonedLayout` stores data alongside aggregate partials for fixed groups of rows, called zones. A
rewrite can use those partials to prove that a query cannot match a zone. The reader applies the
original query to retained rows. Retaining extra zones affects pruning efficiency, but skipping a
matching zone changes the result.

## Aggregate state

An `AggregateFnRef` binds an aggregate implementation to its configuration. The aggregate identifier
and configuration are persisted in the file. A reader session uses registered plugins to resolve
that identifier and reconstruct the state dtype.

`StatFn` exposes the aggregate's partial state rather than its finalized result. A partial can be a
struct, such as the sum and count used by a mean. An index can therefore combine several summary
components in one aggregate without changing the `SkipIndex` interface.

`SkipIndex` groups the aggregate, optional custom probe, and rewrite rules. The aggregate already
supplies its persisted identity, so the index does not need another identifier in the file format.

## Writer composition

`TableStrategy` traverses nested fields and selects struct, list, or scalar layouts.
`RepartitionStrategy` divides the stream into chunks of the required zone length. `ZonedStrategy`
computes the summaries and delegates data storage to its child writer.

```text
TableStrategy selects the data writer
  -> RepartitionStrategy
     -> ZonedStrategy
        -> data writer
        -> summary writer
```

A complete field-writer override replaces this pipeline for that field. A data-writer override
replaces the child below zoning. This distinction determines whether custom storage can compose with
index configuration.

## Rewrite and binding

A rewrite rule constructs a proof over `StatFn` expressions. The zone-map binder replaces those
expressions with references to stored partials. Bloom needs its persisted configuration during
rewriting because that configuration also determines the probe expression.

The root input is the array summarized by the current zone map. A nested field can have its own map
and root. A summary of that input does not automatically describe a derived expression, such as an
arithmetic operation on it.

| Interface | Source |
| --- | --- |
| Aggregate implementation | [`AggregateFnVTable`](../../vortex-array/src/aggregate_fn/vtable.rs) |
| Rewrite context | [`StatsRewriteCtx`](../../vortex-array/src/stats/rewrite.rs) |
| Structural writer selection | [`TableStrategy`](../../vortex-layout/src/layouts/table.rs) |
| Index components | [`SkipIndex`](../../vortex-layout/src/layouts/zoned/skip_index/mod.rs) |
| File writer configuration | [`WriteStrategyBuilder`](../../vortex-file/src/strategy.rs) |
