# Alternative implementations

[Research landing page](../../../README.md) | [Validation](../validation.md)

These patches preserve alternatives to the combined implementation on this branch.

All patches target reviewed PR head `97fa19c640ffcb80e06608b0d64d358b8049c004`, except
`stat-match-polish.delta.patch`. That delta applies after `aggregate-contract-generic.patch`. Patch
application against that base was checked before publication.

| Area | Patches | Relationship |
| --- | --- | --- |
| Combined experiment | [combined.patch](combined.patch) | The complete tested source change, also committed on this branch. |
| Registration | [group.patch](group.patch), [group-static.patch](group-static.patch), [private-state.patch](private-state.patch), [remove-guard.patch](remove-guard.patch) | Competing implementations. Select one. |
| Writer selection | [selected](vortex-writer-policy-selected.patch), [callback and omission alternatives](vortex-writer-policy-alternatives.patch) | Separate experimental versions against the same base. |
| Custom probe | [optional plugin](composition-optional-probe.patch), [registration hook](composition-scalar-hook.patch) | Competing interfaces. Select one. |
| Small writer hook | [composition-data-writer.patch](composition-data-writer.patch) | Earlier implementation that does not solve nested structural dispatch. |
| Structural composition | [dispatcher](composition-structured-dispatch.patch), [builder](composition-structured-builder.patch), [tests](composition-structured-tests.patch) | Apply these together with one custom-probe alternative. |
| Aggregate contracts | [narrow fix](aggregate-contract-narrow.patch), [generic conversion](aggregate-contract-generic.patch) | Competing repair scopes. |
| Single-method aggregate API | [stat-match-polish.delta.patch](stat-match-polish.delta.patch) | Apply after generic conversion to obtain the final StatMatch API. |
| Baseline aggregate reproductions | [aggregate-contract-repros.patch](aggregate-contract-repros.patch) | Regression tests against the reviewed code. |
| Reader and rewrite context | [vortex-read-rewrite.patch](vortex-read-rewrite.patch) | Scoped lookup, independent reader tests, and multiple Bloom proof experiment. |

The composition patches split related files and must be applied together as listed above. Competing
alternatives overlap. The combined source includes the adjustments needed to integrate the selected
variants.

## Reproduce a variant

From a checkout of the research branch, create a separate checkout at the reviewed base:

```bash
git worktree add --detach ../skip-index-alternative 97fa19c640ffcb80e06608b0d64d358b8049c004
cd ../skip-index-alternative
git apply ../vortex/research/skip-index/experiments/group.patch
cargo test -p vortex-layout layouts::zoned::skip_index
```

The example assumes that the research checkout directory is named `vortex`. The [validation
page](../validation.md) lists the combined checks. The [registration
matrix](../evidence/registration-comparison.md) compares successful and failing registration
variants.
