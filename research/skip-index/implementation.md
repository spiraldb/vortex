# Implementation map and next steps

[Research landing page](../../README.md)

The first research commit contains the combined implementation. The second commit contains this
guide and the archived alternatives. The implementation starts at reviewed PR head
`97fa19c640ffcb80e06608b0d64d358b8049c004`.

## Source map

| Change | Source | Behavior to inspect |
| --- | --- | --- |
| Register a rewrite group | [StatsSession](../../vortex-array/src/stats/session.rs) | Replaces one index implementation's rules without replacing unrelated rules. |
| Optional probe | [SkipIndex](../../vortex-layout/src/layouts/zoned/skip_index/mod.rs) | An index can use built-in functions without a custom scalar plugin. |
| Aggregate selection | [ZonedAggregates](../../vortex-layout/src/layouts/zoned/writer.rs) | Resolves defaults, replacement, or additions against the actual input dtype. |
| Field configuration | [WriteStrategyBuilder](../../vortex-file/src/strategy.rs) | Separates complete writers from data writers and checks conflicting paths. |
| Structural composition | [TableStrategy](../../vortex-layout/src/layouts/table.rs) | Chooses the natural layout before applying a wrapper. |
| Partial-state conversion | [StatMatch](../../vortex-array/src/aggregate_fn/stat_match.rs), [trait](../../vortex-array/src/aggregate_fn/vtable.rs) | A match carries the expression that produces the requested representation. |
| Physical binding | [ZoneMap](../../vortex-layout/src/layouts/zoned/zone_map.rs) | Uses aggregate-owned conversion instead of the BoundedMax special case. |
| Scoped metadata | [Rewrite context](../../vortex-array/src/stats/rewrite.rs) | Exposes aggregates only for the input that they summarize. |
| Multiple Bloom configurations | [Bloom rewrite](../../vortex-layout/src/layouts/zoned/skip_index/bloom.rs) | Combines available proofs. This is an optional pruning-quality experiment. |

## Tests that explain the contracts

The [file integration tests](../../vortex-file/tests/bloom_skip_index.rs) provide concrete examples:

- `reader_uses_bloom_options_serialized_in_file`: a default reader opens files with different writer
  configurations.
- `additive_api_preserves_defaults`: adding an index retains default statistics.
- `custom_data_writer_keeps_bloom_pruning`: manual composition and the builder hook both work.
- `index_without_custom_probe_uses_builtin_rewrites`: a custom probe is optional.
- `aggregate_override_preserves_list_decomposition`: an index does not replace the list layout.
- `list_element_aggregate_override_is_applied`: configuration reaches the element data.
- `parent_summary_and_nested_index_compose`: a parent summary and child index coexist.
- `opaque_parent_data_writer_rejects_nested_index`: opaque parent writers cannot promise child
  behavior.
- `unknown_aggregate_disables_known_pruning_in_same_zone_map`: an unknown plugin affects the whole
  shared map.
- `separate_zones_preserve_known_pruning`: separate layers retain known-index pruning in either
  order.

The [zone-map tests](../../vortex-layout/src/layouts/zoned/zone_map.rs) cover structured partials
and conversions between Max and BoundedMax. The tests use the `aggregate_contract_` prefix. The
[registration tests](../../vortex-layout/src/layouts/zoned/skip_index/tests.rs) cover repeated
registration, partial registration, concurrency, and replacement of session components.

## Suggested review order

The aggregate repairs and registration fix establish contracts that the writer API relies on. They
can be reviewed separately from the broader writer changes.

1. Review aggregate identity and partial-state conversion with their regression tests.
2. Review rewrite-group ownership and registration behavior.
3. Review optional probes and aggregate selection policy.
4. Review structural dispatch and the distinction between complete writers and data writers.
5. Decide whether the optional Bloom proof policy belongs in a separate change.

These steps are a proposed division of the work, not approval to merge the research branch.

## Work that remains outside the prototype

- Several existing aggregate serializers still panic for unsupported persistence. The report
  recommends the existing `Ok(None)` contract or real serialization with round-trip tests.
- Display strings still name physical summary fields. Removing the semantic equality shortcut does
  not remove possible physical-name collisions.
- Independent unknown-plugin handling inside one map still needs a metadata design.
- Extra Bloom probes, nested zoned layers, and the changed dispatch need performance measurements
  before any performance default changes.

The [alternative patches](experiments/README.md) preserve the narrower and competing designs. The
combined implementation demonstrates composition. It does not establish that every included policy
is the best production default.
