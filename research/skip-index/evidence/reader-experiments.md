# Reader and rewrite experiments

[Landing page](../../../README.md) | [Validation](../validation.md)

Base: `97fa19c640ffcb80e06608b0d64d358b8049c004`.

## Recommendation

Keep persisted aggregate instances in the rewrite context. Their options are read from file
metadata, and they are needed before constructing the Bloom probe expression. This fits the current
two-pass design: rewrites construct proofs over abstract aggregate-state placeholders. The binder
maps those placeholders onto storage.

Make their scope explicit. The experiment implements `aggregate_fns_for(input) ->
&[AggregateFnRef]`, which exposes summaries only for the root input and returns an empty list for
derived expressions. A smaller alternative is renaming the existing getter `root_aggregate_fns()`
and documenting the same contract. Neither needs a provider trait, additional aggregate identity, or
changes to the file format. The existing raw list is sound for Bloom because Bloom explicitly checks
that its operand is root. Its weakness is the undocumented contract for other plugin authors.

A general provider trait can support summaries of computed expressions or multiple columns. No
current caller supplies such metadata. That design adds a capability that the backing representation
cannot currently fulfill. A Bloom probe needs its configuration during construction. A later binder
therefore needs to rewrite both the probe and its aggregate placeholder. The current binder only
replaces stat leaves. A richer deferred-probe representation is possible, but it weakens the clean
boundary without solving a present reader need.

## Multiple configurations

The PR selects the first stored Bloom configuration. This is sound and metadata order chooses the
pruning quality. The experiment implements an alternative that constructs a proof for each stored
Bloom filter and ORs the proofs. It uses the persisted AggregateFnRef directly rather than rebinding
options to a fresh vtable.

The focused test inserts the same values into 1-block and 256-block filters. It finds a value that
produces a false positive in only the smaller filter. It checks that the proof prunes the absent
value and retains a present value. It also checks AND/OR composition, including a present disjunct.
The test passes with the OR implementation. Replacing the loop with equivalent first-only selection
fails the absent-value assertion. This proves a quality improvement, not a false-negative bug in the
PR.

Choices:

- First matching configuration: minimal query work. Quality depends on metadata order.
- Choose largest configuration: one probe and a natural Bloom-specific policy. A larger filter is a
  probabilistic preference, not a universal ordering across all potential configurations.
- OR all configurations: uses all available evidence and is order-independent. Reads and probes
  every matching summary. Best only if multiple configurations are intentionally supported and
  useful.

Correctness does not require OR-all. Returning all configured aggregates lets each rule choose its
policy. Returning one aggregate from a generic lookup puts that policy in the context.

## Independent readers

The original `reader_uses_bloom_options_serialized_in_file` test passes the same configured index to
both write and read sessions despite its comment. The stronger test reuses one default reader for
files written with 128 and 512 blocks. It verifies absent-value pruning and exact returned rows for
a present value. Both configurations pass. The PR implementation already gets this right. The prior
test did not enforce the intended registration contract.

## Unknown plugin independence

Confirmed an existing layout-level limitation: one unknown aggregate disables every summary in the
same Zoned map. `AggregateSpecProto` contains only ID and options. `try_aggregate_fns_from_specs`
requires every plugin because the stats child dtype is reconstructed by asking every aggregate for
its state dtype. Unknown plugins with allow_unknown therefore replace the map with an empty schema
and set zone_len to zero. Reads remain correct, but known Min/Max in that map cannot prune.

The regression test writes one combined Min/Bloom map above plain Chunked/Flat data. It opens the
file without Bloom and with `allow_unknown`. It evaluates a less-than predicate that Min supports
and Bloom does not. Registered readers keep zero rows. Unknown-Bloom readers keep every row. Full
scans remain correct. The test constructs the layout reader directly from the footer to exclude
file-level statistics. Using `file.layout_reader()` originally hid the effect because its
FileStatsLayoutReader independently pruned the entire file.

Format-level options:

- Keep current format: simple schema derivation and validation. An optional plugin failure disables
  the whole map.
- Persist the complete stats-table dtype in ZonedMetadata: reconstruct the auxiliary child without
  unknown plugins, and resolve only known aggregate specs for rewriting. This preserves one combined
  table. Known resolved state dtypes must be checked against the persisted schema. Unknown fields
  remain opaque. This adds redundant metadata and a validation contract but directly decouples
  schema reconstruction from plugin availability.
- Persist each aggregate's field slot and state dtype alongside ID/options: makes each entry
  independently describable. Physical slots also avoid using Display as storage identity, but
  requires coordinated schema/binder changes and format compatibility rules.
- Separate Zoned wrappers for independent indexes: existing layout machinery can skip the unknown
  wrapper and still use a known wrapper. The combined source contains this prototype. This avoids a
  format change but adds layout nodes, separate stats streams, and repeated orchestration over the
  same zones.

Scope qualification: disabling one map does not disable other layout or file-level pruning. Other
layers can still prove the predicate false.

## Checks

Passed:

- `cargo test -p vortex-layout bloom --lib`: 56 tests, including missing summaries, multiple
  configurations, and scope lookup.
- `cargo test -p vortex-file --test bloom_skip_index`: 4 tests, including independent reader options
  and isolated unknown-plugin fallback.
- `cargo clippy -p vortex-array -p vortex-layout -p vortex-file --all-targets --all-features`.
- `cargo +nightly fmt --all`, with unrelated pre-existing formatter churn restored.
- `git diff --check`.

Build commands used `CARGO_BUILD_JOBS=3 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.
`cargo test --doc -p vortex-array` also passed: 74 passed, 21 ignored (including the separate
compile-fail test).
