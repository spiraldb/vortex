# Reading and rewrites

[Research](../../../README.md) | [Design context](../background.md) | [Validation](../validation.md)

## Persisted configuration

Bloom needs its persisted configuration when the rewrite constructs a probe. The existing sequence
fits that requirement: rewrite rules construct proofs, then the binder replaces stat leaves with
storage expressions. Deferring configuration to the binder also requires it to rewrite the probe.

The prototype keeps persisted aggregate instances in the rewrite context and exposes them through
`aggregate_fns_for(input)`. The lookup returns aggregates only for the root input they summarize.
Renaming the existing getter to `root_aggregate_fns()` expresses the same restriction with less API.

A general provider trait can describe computed-expression or multi-column summaries, but the current
metadata cannot supply them. There is no demonstrated caller for that additional interface.

## Multiple Bloom configurations

The PR uses the first matching configuration, which is sound but makes pruning quality depend on
metadata order. The prototype ORs proofs from all matching configurations. A focused test prunes a
value that is a false positive in only the smaller filter and retains values that are present.

| Policy | Benefit | Tradeoff |
| --- | --- | --- |
| First match | One probe | Metadata order selects the filter. |
| Largest filter | One probe with a Bloom-specific selection policy | Size is a probabilistic preference, not a universal quality ordering. |
| OR all proofs | Uses evidence from every filter | Reads and probes every matching summary. |

The context can expose all candidates and leave selection to the index rule. OR-all is included in
the combined experiment, but is not required for correctness and has no performance measurements.

## Unknown aggregates

The current metadata stores aggregate IDs and configuration. The reader asks each registered plugin
for its state dtype to reconstruct the summary table. With one unknown plugin and `allow_unknown`,
the entire map becomes unavailable. Known aggregates in that map cannot prune.

Scans remain correct. Other layout or file statistics can still prune, so the reproduction excludes
file-level statistics to isolate this behavior.

| Alternative | Result | Tradeoff and evidence |
| --- | --- | --- |
| Current combined map | Correct reads without the unknown plugin | All summaries in that map become unavailable. Reproduced. |
| Separate zoned wrappers | The known wrapper still prunes | Extra layout nodes and summary streams. Tested in both orders. |
| Persist the summary table dtype | Reconstructs the table without every plugin | Adds schema metadata and validation against known aggregates. Design only. |
| Persist each aggregate's slot and state dtype | Describes entries independently and separates storage identity from Display | Requires coordinated metadata and binder changes. Design only. |

Per-aggregate slots and state dtypes address both unknown-plugin independence and physical-name
collisions. Separate wrappers provide independence with the existing format, at the cost of extra
layout and summary streams.

[Experiment details](../evidence/reader-experiments.md), [reader/rewrite
patch](../experiments/vortex-read-rewrite.patch), [unknown-plugin
test](../../../vortex-file/tests/bloom_skip_index.rs#L393).
