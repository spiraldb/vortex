# Registration prototypes

[Landing page](../../../README.md) | [Validation](../validation.md)

All prototypes start at PR head `97fa19c640ffcb80e06608b0d64d358b8049c004`.

| Behavior | PR | Remove guard | Private completion | Rule group |
| --- | --- | --- | --- | --- |
| Aggregate already registered | Missing probe | Pass | Pass | Pass |
| Repeated registration through session clone | One probe | Two probes | One probe | One probe |
| 16 concurrent registrations | One in this run. Guard is not atomic | 16 probes | One probe | One probe |
| Writer uses different Bloom options | Pass | Pass | Pass | Pass |
| Replace StatsSession and register again | Zero probes | Pass | Zero probes | Pass |
| Independently append a Bloom rule, then register index twice | Two probes | Three probes | Two probes | Two probes |

The concurrent test counts probes after all calls finish. The PR also has a race identified by
source inspection. A second caller can return after aggregate insertion but before scalar and
rewrite registration. The private OnceLock waits until the whole bundle is installed. Rule-group
registration lets both callers complete all components and atomically replaces the rule group under
the StatsSession lock. No variant makes arbitrary concurrent readers observe one transaction across
aggregate, scalar, and stats registries. The existing session API has no such transaction.

## Recommendation

Use a rule-group upsert owned by StatsSession. `register_rewrite_group::<I>(rules)` replaces the
rules supplied by one index implementation, leaving other groups and explicitly appended rules
intact. The index can always register its aggregate and scalar using the existing replace-by-ID
semantics, then upsert its rules. This needs no new persisted identifier and no completion cache
that can disagree with replaced session state.

The group API adds roughly 45 lines of registry implementation and changes one private
representation used by the rewrite loop. It copies rule lists while registering, not while
evaluating predicates. It preserves the existing append API, including multiple differently
configured rules of the same Rust type. Global deduplication by each rule's TypeId changes those
existing semantics.

The public contract must state that an index's registration supports all persisted configurations. A
group keyed by index type deliberately treats repeated registration as replacement of one universal
implementation bundle. It does not support several independent instance-specific rule bundles under
the same index type. The PR's aggregate-keyed early return already has that restriction implicitly.

Private completion tracking is smaller in registry scope and avoids work on repeated calls. It adds
a cache invalidation contract around public VortexSession::register, since each registry can be
replaced independently. It also preserves the first implementation instance, whereas existing
aggregate and scalar registration APIs replace by ID. Use it only if the session deliberately
forbids replacing component registries after index registration.

Deleting the guard is insufficient for an idempotent API: actual predicates grew from one Bloom
probe to sixteen in the concurrent test.

## Existing session observations

VortexSession clones, AggregateFnSession clones, and StatsSession clones share their mutable
registry cells. A fresh VortexSession::empty is independent. SessionMut uses clone-and-replace
semantics. Concurrent get_mut calls that change a plain HashSet can lose updates. It is unsuitable
for atomic registration tracking. The private completion prototype uses ArcSwapMap<TypeId,
Arc<OnceLock<()>>> instead.

The existing StatsSession supports append only. That is useful for several rules targeting the same
scalar function, but does not express installing a plugin bundle idempotently. This missing
operation is the underlying composition gap.

AggregateFnSession::default registers CountGroupedKernel under Count.id() but omits the Count
plugin. Count also has incomplete serialization. Missing default registration alone is not
established as an index defect. The prototype does not change it.

## Artifacts

- [baseline.log](logs/registration-baseline.log),
  [remove-guard.log](logs/registration-remove-guard.log),
  [private-state.log](logs/registration-private-state.log),
  [group.log](logs/registration-group.log): actual six-test outcomes.
- [remove-guard.patch](../experiments/remove-guard.patch),
  [private-state.patch](../experiments/private-state.patch),
  [group.patch](../experiments/group.patch): independent alternatives against the PR head. The group
  patch includes the tests.

`cargo +nightly fmt --all` completed. It also exposed unrelated existing format drift in DuckDB/FFI
files, which was reverted in this isolated clone. The combined experiment subsequently passed full
workspace Clippy.

The associated-function alternative is in [group-static.patch](../experiments/group-static.patch).
It also passed all six focused tests. Layout doctests passed (two active, two existing ignored).
`cargo clippy -p vortex-layout --all-targets --all-features` passed after explicitly permitting the
intentional cloned-session test. The static API is
`session.register_skip_index::<BloomSkipIndex>()`. Configured index instances are needed only for
writes. This removes a misleading registration-time options argument, but disallows
instance-supplied registration dependencies. The plain group patch retains instance methods and
documents the universal-registration contract instead.
