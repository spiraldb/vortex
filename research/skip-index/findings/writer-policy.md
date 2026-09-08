# Aggregate selection

[Research](../../../README.md) | [Design context](../background.md) | [Validation](../validation.md)

`with_field_aggregates` replaces the default aggregate list. Using it to add Bloom can remove
Min/Max pruning. The writer also omits unsupported explicit requests, so a successful write does not
establish that the requested index was usable.

The prototype distinguishes `Defaults`, `Replace`, and `Extend`. It resolves the selection once the
input dtype and session are available. Additions retain supported defaults, while explicit
replacements retain their current meaning.

## Alternatives

| Selection API | Benefit | Tradeoff |
| --- | --- | --- |
| Replacement only | Exact control over the list | Adding an index requires the caller to reconstruct dtype- and session-dependent defaults. |
| Defaults / Replace / Extend | Represents the two builder operations directly | Repeated operations need defined ordering. |
| Callback receiving dtype, session, and defaults | Supports arbitrary selection and removal | Selection becomes executable configuration whose behavior is harder to inspect and combine. |

The callback prototype works, but the demonstrated callers only need addition and replacement. The
enum keeps those operations explicit. Exact duplicate aggregates are stored once, while different
configurations remain distinct. An addition after replacement extends that replacement. A later
replacement resets the selection, and an empty replacement is valid.

## Unsupported requests

Best-effort omission is useful for a policy applied across heterogeneous schemas. Strict validation
makes an explicit request observable, but requires schema-adaptive callers to select applicable
fields. Both policies were tested.

The combined prototype rejects unsupported explicit requests and continues to omit unsupported
defaults. It checks both `state_dtype` and `return_dtype` against the actual input dtype. Missing
field paths also return an error.

These checks run once the stream dtype is known, which can be after writing begins. An applicable
aggregate can still produce an empty or all-invalid summary that the writer omits.

[Selected implementation](../experiments/vortex-writer-policy-selected.patch), [callback and
omission alternatives](../experiments/vortex-writer-policy-alternatives.patch), [combined selection
code](../../../vortex-layout/src/layouts/zoned/writer.rs#L38).
