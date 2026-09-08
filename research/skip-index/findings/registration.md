# Registration

[Landing page](../../../README.md) | [Background](../background.md) | [Validation](../validation.md)

The PR tests whether the aggregate is registered, then skips installing the entire index.
Pre-registering the aggregate consequently prevents the probe and rewrite from being installed.
Aggregate availability is not proof of bundle availability.

| Implemented alternative | Benefit relative to PR | Tradeoff |
| --- | --- | --- |
| Remove the guard | Repairs partial registration | Repeated registration appends duplicate rewrite rules. 16 concurrent registrations produced 16 probes |
| Private TypeId/OnceLock completion tracking | Installs once, handles concurrent registrars | Completion becomes stale if the public session API replaces StatsSession. Preserves the first instance while other registries replace by ID |
| StatsSession rewrite-group upsert | Idempotent bundle replacement, repairs partial or replaced registries | Requires an explicit ownership concept for groups of rules |

**Prefer rewrite groups.** `register_rewrite_group::<I>(rules)` replaces one implementation's rules
and preserves independent groups and raw appended rules. Aggregate and scalar registries already
replace entries by ID. The group update is atomic within StatsSession. Installation across all three
registries is not a transaction for arbitrary concurrent readers.

Both `register_skip_index(&index)` and `register_skip_index::<BloomSkipIndex>()` were implemented
and passed the same six tests. Associated functions express option-independent registration clearly,
but prohibit instance-supplied dependencies that the existing vtable abstraction permits. Retain the
instance API unless the project deliberately wants that restriction, and require its installed
reader to support every persisted option set. A fresh default reader successfully opened files with
two different writer configurations.

Evidence: [comparison and six-case matrix](../evidence/registration-comparison.md), [group
implementation](../experiments/group.patch), [private completion
alternative](../experiments/private-state.patch), [static registration
alternative](../experiments/group-static.patch).
