# Registration

[Research](../../../README.md) | [Design context](../background.md) | [Validation](../validation.md)

The PR returns early from index registration when the aggregate is already registered. Registering
the aggregate separately can therefore prevent the index's probe and rewrite rules from being
installed.

The prototype registers each component on every call. Aggregate and scalar registries already
replace entries by ID. The new `StatsSession::register_rewrite_group::<I>(rules)` operation replaces
one index implementation's rules while preserving other groups and independently appended rules.

## Alternatives

| Implemented alternative | Result | Tradeoff |
| --- | --- | --- |
| Remove the guard | Installs missing components | Repeated calls append duplicate rules. Sixteen concurrent calls produced sixteen Bloom probes. |
| Track completion with TypeId/OnceLock | Registers once and coordinates concurrent callers | The completion record becomes stale if the session replaces `StatsSession`. It also preserves the first instance while the other registries replace by ID. |
| Replace a rewrite group | Repeated calls restore the complete registration | Adds group ownership to `StatsSession`. Each index type owns one group. |

Group replacement expresses the operation needed here without a separate completion cache. The
update is atomic within `StatsSession`. The existing session API does not provide a transaction
across aggregate, scalar, and rewrite registries.

## Instance or static registration

Both `register_skip_index(&index)` and `register_skip_index::<BloomSkipIndex>()` passed the same six
tests. Static registration separates reader capabilities from writer configuration, but excludes
instance-supplied dependencies permitted by the vtable interface.

The combined prototype retains the instance API. Its registration must support every persisted
configuration, rather than only the configuration held by that instance. A default reader
successfully opened files written with two different Bloom configurations.

[Comparison matrix](../evidence/registration-comparison.md), [group
implementation](../experiments/group.patch), [completion
tracking](../experiments/private-state.patch), [static
registration](../experiments/group-static.patch).
