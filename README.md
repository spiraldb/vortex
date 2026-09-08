# Skip-index API research

This branch contains findings and working prototypes for [Vortex PR
#9413](https://github.com/vortex-data/vortex/pull/9413), the skip-index write interface. It
evaluates whether the proposed APIs fit the rest of Vortex. It also examines existing `AggregateFn`
contracts that affect indexes.

**Main finding: one aggregate per index is enough.** The abstraction can compose with the system.
The surrounding APIs need clearer contracts for registration, writer configuration, and aggregate
state conversion. Independent handling of unknown aggregates within one shared summary table
requires a metadata change.

The source on this branch contains the combined experiment. It is a research reference, not a single
change proposed for production. The separate alternatives remain available as patches.

## Start here

1. Read [How skip indexes work in Vortex](research/skip-index/background.md) for the terminology and
   the path from input rows to query pruning. No Vortex maintainer knowledge is assumed.
2. Use the findings table below to find the component relevant to your work.
3. Read [Implementation map and next steps](research/skip-index/implementation.md) to connect the
   recommendations to source files and tests.
4. Use [Validation and reproduction](research/skip-index/validation.md) to run the experiments.

## Findings

| Component | Recommendation | Detail |
| --- | --- | --- |
| Registration | Replace each index's rewrite group together. An existing aggregate does not prove that the whole index is registered. | [Alternatives and failure cases](research/skip-index/findings/registration.md) |
| Writer configuration | Distinguish defaults, replacements, and additions. Reject unsupported explicit requests. | [API and tradeoffs](research/skip-index/findings/writer-policy.md) |
| Writer composition and probes | Select the natural data writer before wrapping it with indexing. Make custom probes optional. | [Nested fields, lists, and custom writers](research/skip-index/findings/composition.md) |
| Aggregate contracts | Return both compatibility and the expression that converts a stored partial into the requested representation. | [Reproduced bugs and the proposed contract](research/skip-index/findings/aggregate-contracts.md) |
| Reader and rewrite context | Keep persisted aggregate instances with explicit input scope. | [Reader behavior and unknown plugins](research/skip-index/findings/read-rewrite.md) |

The prototype exposes additive indexing without requiring callers to reconstruct default statistics:

```rust
let bloom = BloomSkipIndex::new(options);
session.register_skip_index(&bloom);

let strategy = WriteStrategyBuilder::default()
    .with_field_aggregate_additions(field_path!(id), [bloom.aggregate_fn()])
    .with_field_data_writer(field_path!(id), custom_data_writer)
    .try_build()?;
```

This abbreviated example assumes existing `options` and `custom_data_writer` values. The linked
[integration tests](vortex-file/tests/bloom_skip_index.rs) contain complete examples.

## Evidence and scope

The combined experiment passed 929 tests, 76 doctests, and workspace Clippy with all targets and
features. [Commands, counts, and logs](research/skip-index/validation.md) describe the coverage.
There are no performance measurements or implemented file-format migrations in this research.

All findings refer to PR head
[`97fa19c640ff`](https://github.com/vortex-data/vortex/commit/97fa19c640ffcb80e06608b0d64d358b8049c004),
reviewed on September 8, 2026. Later changes to the PR can change these conclusions. The [original
Vortex
README](https://github.com/vortex-data/vortex/blob/97fa19c640ffcb80e06608b0d64d358b8049c004/README.md)
contains the general project introduction.
