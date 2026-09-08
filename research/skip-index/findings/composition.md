# Custom probes and writer composition

[Landing page](../../../README.md) | [Background](../background.md) | [Validation](../validation.md)

### Probe registration

The PR requires a custom scalar vtable even when an index can express its proof using existing
functions.

| Implemented alternative | Benefit | Tradeoff |
| --- | --- | --- |
| Optional erased scalar plugin | Declarative and inspectable. No dummy function | One optional custom probe |
| `register_scalar_fns(&ScalarFnSession)` hook | Can install several helper functions | Hides dependencies in executable code |
| Mandatory associated scalar type | Statically named vtable | Imposes an unnecessary component on built-in-only proofs |

**Prefer an optional plugin for demonstrated needs.** Both alternatives passed the same composition
tests, including an index with no custom probe. Multiple helper registration can be introduced when
a real consumer needs it. It does not imply multiple aggregates per index.

### Choosing and wrapping data writers

Manual composition works: `Repartition(Zoned(custom_data, stats_writer))`. The tested example takes
about 28 lines and must coordinate zone length and repartitioning, including disabling byte-based
coalescing. A convenience method can hide those coordination requirements.

Simply merging per-field writer options is insufficient. The PR routes aggregate overrides through
the complete-field override mechanism. Experiments confirmed that this disables list decomposition,
ignores list-element requests, and rejects a parent summary alongside a child summary.

The successful implementation selects the natural struct/list/scalar data strategy first, then
applies a common wrapper factory. TableStrategy handles traversal. The wrapper adds partitioning and
zoning. TableStrategy does not need to know about aggregates. The same mechanism supports both
default and field-specific wrappers.

**Prefer structural dispatch followed by wrapping.** Tests cover list layouts, list-element indexes,
parent-plus-child summaries, and custom data writers with working Bloom pruning. The tradeoff is an
explicit distinction between a data-writer override and a complete-writer override. An opaque parent
data writer cannot promise to apply child overrides, so that combination returns an error.

Evidence: [structural dispatcher](../experiments/composition-structured-dispatch.patch), [builder
implementation](../experiments/composition-structured-builder.patch), [behavior
tests](../experiments/composition-structured-tests.patch), [optional
probe](../experiments/composition-optional-probe.patch), [registration hook
alternative](../experiments/composition-scalar-hook.patch).
