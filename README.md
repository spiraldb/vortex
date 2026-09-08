# Skip-index API research

One aggregate per index works with the existing `StatFn` interface, including structured partial
states. The main issues are in registration, writer composition, and conversion between aggregate
states.

This branch contains prototypes and findings for [PR
#9413](https://github.com/vortex-data/vortex/pull/9413) at
[`97fa19c640ff`](https://github.com/vortex-data/vortex/commit/97fa19c640ffcb80e06608b0d64d358b8049c004).
The [design context](research/skip-index/background.md) describes the relevant Vortex interfaces.

## Findings

| Area | Finding and proposed change |
| --- | --- |
| [Registration](research/skip-index/findings/registration.md) | An already-registered aggregate can prevent the index's probe and rewrites from being installed. Register all components and replace the index's rewrite group on each call. |
| [Writer configuration](research/skip-index/findings/writer-policy.md) | Adding Bloom can replace default statistics. Give additions and replacements separate APIs, and reject unsupported explicit requests. |
| [Composition](research/skip-index/findings/composition.md) | Field overrides can bypass list and struct traversal. Apply indexing after selecting the data writer. Make custom probes optional. |
| [Aggregate contracts](research/skip-index/findings/aggregate-contracts.md) | `can_satisfy` can accept aggregates with incompatible state shapes. Return the conversion expression with the match. |
| [Reading and rewrites](research/skip-index/findings/read-rewrite.md) | Persisted aggregate configuration belongs in the rewrite context. Independent handling of unknown aggregates in one map requires more schema metadata. |

The prototype keeps default statistics when adding an index and supports a custom data writer:

```rust
let bloom = BloomSkipIndex::new(options);
session.register_skip_index(&bloom);

let strategy = WriteStrategyBuilder::default()
    .with_field_aggregate_additions(field_path!(id), [bloom.aggregate_fn()])
    .with_field_data_writer(field_path!(id), custom_data_writer)
    .try_build()?;
```

`with_field_aggregates` still replaces the selection. `with_field_data_writer` changes the data
writer below zoning. The [integration tests](vortex-file/tests/bloom_skip_index.rs) exercise both.

## Implementation and evidence

The [implementation map](research/skip-index/implementation.md) links the changes to source and
regression tests. [Alternative patches](research/skip-index/experiments/README.md) preserve the
competing designs, including narrower fixes.

The combined prototype passed 929 tests, 76 doctests, and workspace Clippy with all targets and
features. [Validation](research/skip-index/validation.md) contains the commands and logs. The
prototype has no performance measurements and does not implement the proposed metadata changes.

The findings refer to the September 8, 2026 snapshot above. The branch includes experimental
policies, including combining multiple Bloom proofs, that can be reviewed separately from the API
changes.
