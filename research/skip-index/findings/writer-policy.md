# Selecting aggregates and handling unsupported inputs

[Landing page](../../../README.md) | [Background](../background.md) | [Validation](../validation.md)

The PR's explicit aggregate list replaces defaults, so opting into Bloom can remove ordinary Min/Max
pruning. It also silently omits unsupported explicit requests. These are policy choices. An explicit
selection API makes their consequences visible.

| Implemented alternative | Benefit | Tradeoff |
| --- | --- | --- |
| Keep replacement only | Exact control, smallest policy surface | Caller must reconstruct dtype- and session-dependent defaults to add an index |
| Defaults / Replace / Extend enum | Explicit semantics. Defaults resolve when dtype and session are known | Three policies and defined ordering for repeated builder operations |
| Callback receiving dtype, session, and defaults | Arbitrary selection and removal | Opaque executable configuration. Harder to inspect and combine |
| Best-effort omission | Convenient across heterogeneous schemas | Successful write does not establish that an explicitly requested index was usable |
| Reject unsupported explicit requests | Invalid requests are visible | Schema-adaptive callers must select applicable fields themselves |

**Prefer the enum and strict explicit requests.** Unsupported defaults remain optional. Exact
duplicate bound aggregates are stored once. Different options remain distinct. Adding after
replacement appends to that replacement. A later replacement resets the selection. An empty
replacement is valid.

The prototype validates both `state_dtype` and `return_dtype` against the actual input type. Missing
field paths also return an error instead of disappearing. Input-dependent checks run once the stream
dtype is known, so failure can occur after file writing has begun. Strict applicability does not
promise that an empty or all-invalid summary will be physically stored.

The callback and best-effort alternatives were executed, not merely sketched. Neither is needed for
the demonstrated consumers. Evidence: [selected
implementation](../experiments/vortex-writer-policy-selected.patch), [callback and omission
alternatives](../experiments/vortex-writer-policy-alternatives.patch), [combined selection
code](../../../vortex-layout/src/layouts/zoned/writer.rs#L38).
