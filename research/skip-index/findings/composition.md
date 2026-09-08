# Writer composition and probes

[Research](../../../README.md) | [Design context](../background.md) | [Validation](../validation.md)

## Field writers

The PR routes aggregate configuration through the complete-field override mechanism. Tests exposed
three consequences: list decomposition is disabled, list-element requests are ignored, and a parent
summary conflicts with a child summary.

The prototype selects the natural struct, list, or scalar writer before applying the index wrapper.
`TableStrategy` owns traversal, while the wrapper adds repartitioning and zoning. This keeps
aggregate policy out of `TableStrategy` and preserves nested field configuration.

| Alternative | Result | Tradeoff |
| --- | --- | --- |
| Manual `Repartition(Zoned(custom_data, stats_writer))` | Custom data storage and pruning compose | Callers must coordinate the zone length and repartitioning, including disabling byte-based coalescing. |
| Merge aggregate and data-writer configuration at each field | Fixes the same-field case | Complete overrides still bypass structural traversal. |
| Select the data writer, then apply a wrapper | Preserves lists, element indexes, and parent-plus-child summaries | Requires separate meanings for data-writer and complete-writer overrides. |

The combined prototype uses the wrapper approach. `with_field_data_writer` replaces the data child
below zoning. `with_field_writer` retains ownership of the whole field pipeline.

An opaque writer for a parent field does not expose traversal to child overrides. Combining it with
child configuration therefore returns an error. A parent aggregate summary can still coexist with a
child index because it wraps the normal traversal.

## Custom probes

The PR requires a scalar vtable even when the rewrite can use existing scalar functions. An optional
erased plugin removes that requirement. A registration hook also works and can install several
helper functions, but leaves those dependencies inside executable registration code.

Both variants passed the same composition tests, including an index with no custom probe. The
combined prototype uses one optional plugin. The hook remains an alternative for indexes that need
several helpers. Neither choice requires multiple aggregates per index.

[Dispatcher](../experiments/composition-structured-dispatch.patch),
[builder](../experiments/composition-structured-builder.patch),
[tests](../experiments/composition-structured-tests.patch), [optional
probe](../experiments/composition-optional-probe.patch), [registration
hook](../experiments/composition-scalar-hook.patch).
