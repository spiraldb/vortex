# Registration experiments

[Research](../../../README.md) | [Registration proposal](../findings/registration.md)

Each variant starts at PR head `97fa19c640ffcb80e06608b0d64d358b8049c004`. The tests count Bloom
probes in the resulting predicate after registration completes.

| Case | PR | Remove guard | Completion cache | Rewrite group |
| --- | --- | --- | --- | --- |
| Aggregate already registered | Probe missing | Pass | Pass | Pass |
| Repeated registration through a session clone | One probe | Two probes | One probe | One probe |
| Sixteen concurrent registrations | One probe in this run | Sixteen probes | One probe | One probe |
| Different writer configurations | Pass | Pass | Pass | Pass |
| Replace StatsSession, then register again | Zero probes | Pass | Zero probes | Pass |
| Append an independent Bloom rule, then register twice | Two probes | Three probes | Two probes | Two probes |

The PR's guard is not atomic. Source inspection identifies another race: a caller can return after
aggregate insertion but before the first caller installs the scalar and rewrite rules. The
concurrent test above does not exercise that intermediate state directly.

`OnceLock` makes concurrent registrars wait for the bundle. Rewrite-group replacement lets each
registrar install the components and replaces the group's rules under the `StatsSession` lock.
Neither design gives arbitrary concurrent readers a transaction across all three registries.

## Session behavior

`VortexSession`, `AggregateFnSession`, and `StatsSession` clones share their mutable registry cells.
`SessionMut` uses clone-and-replace semantics, so concurrent updates to a plain `HashSet` can lose
registration records. The completion prototype uses `ArcSwapMap<TypeId, Arc<OnceLock<()>>>` instead.

A separate completion cache becomes stale if a component registry is replaced. The group operation
keeps ownership in the registry that holds the rules. It also preserves the append API, including
independently configured rules of the same Rust type.

The group key identifies an index implementation, not an instance configuration. Registration must
support every persisted configuration of that implementation. Independent instance-specific rule
bundles under one type are outside that contract.

Both the instance API and `register_skip_index::<BloomSkipIndex>()` passed the six cases. The static
API prevents instance-supplied registration dependencies. The combined prototype keeps the instance
API and documents the configuration-independent registration contract.

`AggregateFnSession::default` omits the Count plugin while registering its grouped kernel. Count
also has incomplete serialization. The omission alone is not established as an index defect.

## Artifacts

| Variant | Patch | Log |
| --- | --- | --- |
| PR baseline | Tests supplied by the group experiment | [Baseline](logs/registration-baseline.log) |
| Remove guard | [Patch](../experiments/remove-guard.patch) | [Log](logs/registration-remove-guard.log) |
| Completion cache | [Patch](../experiments/private-state.patch) | [Log](logs/registration-private-state.log) |
| Rewrite group | [Patch](../experiments/group.patch) | [Log](logs/registration-group.log), [final run](logs/registration-group-final.log) |
| Static registration | [Patch](../experiments/group-static.patch) | [Log](logs/registration-static.log) |

The group and static variants also passed layout doctests and affected-crate Clippy. The [combined
prototype](../validation.md) subsequently passed workspace Clippy.
