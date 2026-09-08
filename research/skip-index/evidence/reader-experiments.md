# Reader experiments

[Research](../../../README.md) | [Reader findings](../findings/read-rewrite.md)

The experiments start at PR head `97fa19c640ffcb80e06608b0d64d358b8049c004`. The
[patch](../experiments/vortex-read-rewrite.patch) contains the scoped lookup, Bloom proof policy,
and reader tests.

## Multiple Bloom configurations

The test inserts the same values into 1-block and 256-block filters. It then finds a value that
produces a false positive in only the smaller filter. This avoids assuming that an arbitrary absent
value distinguishes the two filters.

OR-all prunes that value, retains a present value, and composes with AND/OR predicates. Replacing
the loop with first-only selection fails the absent-value assertion. This demonstrates better
pruning for that case. First-only selection remains sound, and the additional probe cost was not
measured.

The rewrite uses persisted `AggregateFnRef` values directly. It does not rebind their configuration
to a new vtable. The scoped lookup returns no aggregates for derived input expressions.

## Reader configuration

The original `reader_uses_bloom_options_serialized_in_file` test configures the reader and writer
with the same index instance. It therefore does not distinguish persisted configuration from reader
configuration.

The revised test reuses a default reader for files written with 128 and 512 blocks. Both files prune
absent values and return the exact expected rows for a present value. The PR implementation already
supports this behavior. The change strengthens the test.

## Unknown plugins

The test writes a combined Min/Bloom map above plain Chunked/Flat data. It opens the file without
the Bloom plugin and with `allow_unknown`, then evaluates a less-than predicate supported by Min.
The registered reader prunes every row. The reader without Bloom retains every row, and its full
scan still returns the correct result.

The test constructs the layout reader directly from the footer. Using `file.layout_reader()` hid the
effect because `FileStatsLayoutReader` independently pruned the file.

`AggregateSpecProto` contains only ID and configuration. `try_aggregate_fns_from_specs` needs every
plugin to reconstruct the summary table's state dtypes. With an unknown plugin, the fallback uses an
empty map and sets the zone length to zero. This explains the loss of known-summary pruning within
that map.

The separate-wrapper tests retain known-summary pruning with Bloom either inside or outside the
known wrapper. Persisting the table dtype or per-aggregate slot/state metadata remains a design
alternative. Neither format change was implemented.

## Recorded checks

The independent reader prototype passed:

- `cargo test -p vortex-layout bloom --lib`: 56 tests.
- `cargo test -p vortex-file --test bloom_skip_index`: four tests.
- Clippy for `vortex-array`, `vortex-layout`, and `vortex-file`, with all targets and features.
- Array doctests: 74 passed, 21 ignored.
- Nightly formatting and `git diff --check`.

The [combined validation](../validation.md) includes the later composition tests and aggregate
contract changes.
