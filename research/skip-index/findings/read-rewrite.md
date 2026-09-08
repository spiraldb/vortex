# Rewrite context and optional-reader behavior

[Landing page](../../../README.md) | [Background](../background.md) | [Validation](../validation.md)

The existing two-stage design is useful: rules construct proofs using persisted aggregate options.
The binder maps stat leaves to storage. Deferring all options to the binder requires it to rewrite
the Bloom probe and stat leaf together. That is a larger protocol with no demonstrated benefit.

**Keep persisted aggregate instances and make their scope explicit.** The implemented
`aggregate_fns_for(input)` exposes them only for the root input. A `root_aggregate_fns()` rename is
a simpler equivalent contract. A general provider trait can support computed-expression or
multi-column summaries. The current metadata does not describe those summaries.

The PR's first-matching Bloom configuration is sound. The experiment ORs proofs from every matching
configuration. It prunes an absent value that produces a false positive in only the smaller filter.
It also verifies retained hits and AND/OR composition. This costs extra summary reads and probes,
and was not benchmarked. Choosing one, choosing the largest, and using all are index-specific
quality policies. The recommendation is to expose all candidates and leave selection to the index
rule. OR-all is included as an experiment, not a correctness requirement for this PR.

### Unknown aggregate independence

The current metadata contains aggregate ID and options, and reconstructs the stats-table dtype
through registered plugins. With one unknown plugin and `allow_unknown`, the entire map becomes
unavailable. Known summaries in that map cannot prune, although scans remain correct and other
file/layout pruning can still work.

| Alternative | Benefit | Tradeoff / evidence |
| --- | --- | --- |
| Preserve current combined map | No format change | Unknown plugin disables all summaries in that map. Reproduced with file statistics excluded |
| Separate Zoned wrappers | Known wrapper still prunes | Extra layout nodes and stats streams. Tested in both nesting orders |
| Persist complete stats-table dtype | Decode auxiliary schema without every plugin | Redundant metadata and validation against each known aggregate. Design only |
| Persist per-aggregate slot and state dtype | Independently describable summaries. Decouples Display from storage | Coordinated metadata and binder change with format compatibility rules. Design only |

For independence inside a single combined map, prefer explicit slot/state metadata as the
longer-term direction. The current format cannot provide that behavior through a cleaner Rust trait
alone. Separate wrappers are a working option if avoiding a format change matters.

Evidence: [reader report](../evidence/reader-experiments.md), [reader/rewrite
patch](../experiments/vortex-read-rewrite.patch), [isolated unknown-plugin
test](../../../vortex-file/tests/bloom_skip_index.rs#L393).
