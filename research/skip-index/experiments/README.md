# Alternative implementations

[Research landing page](../../../README.md) | [Validation](../validation.md)

The source tree already contains the combined experiment. These patches preserve the competing
implementations for comparison. Do not apply them on top of the research branch.

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
| Baseline aggregate reproductions | [aggregate-contract-repros.patch](aggregate-contract-repros.patch) | Intentionally demonstrates failures in the reviewed code. |
| Reader and rewrite context | [vortex-read-rewrite.patch](vortex-read-rewrite.patch) | Scoped lookup, independent reader tests, and multiple Bloom proof experiment. |

Applying a patch does not prove that it is a standalone feature. The composition patches split
related source files for comparison. Different alternatives overlap and are not a linear stack. The
combined source includes integration adjustments between the selected alternatives.

## Inspect one alternative

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
variants. A failure in an intentionally weaker variant is part of the evidence.
