<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# RowFn adapter optimizations

The Arrow adapter now uses string-view headers and constructs output without redundant payload
validation. Concatenation writes its parts directly into owned output. These changes remove measured
costs from the first implementation. They do not establish performance parity for every function.

The before version is the source captured after the
[performance diagnosis](../2026-09-29-diagnosis/README.md). It already uses byte comparisons for
prefixes and suffixes. The comparison does not use the older, slower `str` comparison implementation.

## Repeated measurements

All times below are microseconds per invocation at 16,384 logical rows. Each entry is the median
of three process medians. `summary.csv` includes every case, the range of those medians, and paired
before/after ratios. Scalar/array combinations, nullable and dense inputs, and packing boundaries
remain separate cases.

| Case | Before | After | Arrow baseline | After / Arrow |
| --- | ---: | ---: | ---: | ---: |
| Utf8View prefix, three bytes | 55.83 | 18.45 | 17.66 | 1.04x |
| Utf8View prefix, five bytes | 58.74 | 23.99 | 16.66 | 1.44x |
| Utf8 prefix, five bytes | 61.29 | 41.33 | 12.95 | 3.19x |
| Utf8View scalar suffix | 58.45 | 19.62 | 17.74 | 1.11x |
| Utf8View scalar containment | 136.80 | 106.30 | 93.54 | 1.14x |
| Scalar ASCII case equality | 68.41 | 42.87 | 28.12 | 1.52x |
| Short concatenation | 525.90 | 137.70 | 100.40 | 1.37x |
| Long concatenation | 792.00 | 201.20 | 129.50 | 1.55x |
| Checked multiplication | 6.12 | 6.12 | 11.08 | 0.55x |
| Long Utf8View trimming | 440.70 | 217.50 | 225.70 | 0.96x |
| Nullable checked division | 36.20 | 17.95 | 11.95 | 1.50x |
| Scalar-only addition | 7.37 | 2.02 | 1.25 | 1.62x |
| Zero-width list scaling | 9.79 | 1.03 | 0.07 | 15.43x |
| Width-four list scaling | 68.74 | 15.62 | 8.75 | 1.79x |
| Dictionary string trimming | 460.90 | 285.80 | 301.70 | 0.95x |

Short Utf8View prefix runs ranged from 17.16 to 19.29 microseconds. Nullable division ranged from
13.66 to 17.99 microseconds. These cases have more run-to-run variation than the unchanged numeric
controls, so their displayed ratios should not be treated as precise constants.

The same final binary separates collection and invocation:

| Boundary, 16,384 rows | Time |
| --- | ---: |
| Direct typed collection | 3.395 |
| Shared collection | 3.499 |
| Shared executor | 3.957 |
| Complete Arrow invocation | 4.083 |
| Arrow wrapping addition | 3.374 |

Collection is within about 3% of the direct typed loop in this case. Complete invocation adds about
0.69 microseconds. This supports separating batch overhead from the throughput of the row loop.
It does not establish that every callback or input representation has the same cost.

| Nullable Boolean predicate | Before | After | Retained Vortex executor |
| --- | ---: | ---: | ---: |
| Dense accepted | 5.92 | 5.92 | 5.33 |
| Null-only overflow, then retry | 33.62 | 32.24 | 15.91 |

The Arrow retry path improved from 38.41 to 32.83 microseconds. The valid-only Arrow reference takes
21.08 microseconds. The reference is a matching checked predicate loop, not an Arrow catalog kernel.
Boolean NOT still takes 4.71 microseconds through RowFn versus 0.070 through Arrow's bitmap kernel.
It should remain a whole-batch implementation.

The run covered 182 cases per variant in three alternating process pairs, for 1,092 timed case
results. Both variants also completed all-size fixture preflights. The compiler was Rust 1.98.0,
LLVM 22.1.8, targeting aarch64-apple-darwin on the local Apple M4 Max. Both used opt-level 3, 16 CGUs,
no LTO, limited debug information, and forced frame pointers. No target-cpu=native override was used.
The host was not CPU-pinned. No build or test process ran during timing.

## Retained changes

- `TextBinding` selects a typed view family before traversal. `TextValue` preserves stored prefixes
  on both hosts. Arrow can answer short-prefix requests from a Utf8View header without loading the
  external string. Function dispatch and row operations remain shared.
- `WriteUtf8::write_parts` lets the string sink copy several borrowed parts directly. Both adapters
  implement it without allocating an intermediate `String` for each row.
- Arrow string output uses `StringViewArray::new_unchecked`. Its private writer constructs every
  view from valid UTF-8 and retains every referenced output buffer. This removes a full validation
  pass after construction.
- Arrow publication uses `ArrayDataBuilder::build_unchecked` after checking the actual storage data,
  output label, row count, outer validity length, and nullability. It permits the same physical type
  or the supported Int64-to-Timestamp label. It does not authorize arbitrary reinterpretation.
- Arrow string reads no longer inspect null bits per row. Arrow's constructors validate the payload
  at every string position, including null positions. Outer validity still determines the result.
  Vortex retains its required input validation and null sanitation.
- `Selection` is now an unsafe adapter contract. Safe constructors establish stable length, count,
  and ordered, bounded indices. Traversal must stop at the first callback error. The executor checks
  domain lengths once, then trusts that contract when reading selected rows.
- Scalar primitive, Boolean, and Utf8View outputs use direct broadcasting instead of an allocated
  take-index array. Repeated string views retain the result's owned payload buffers.
- Fixed-size-list scaling specializes widths zero through four in shared dispatch. Other widths
  retain the generic path. Small row access and initialization methods can inline into the loop.

The strict null and error contracts are unchanged. Decoder and publication errors remain terminal.
Only rejected deferred row evidence can trigger null-based suppression or retry. Initialization
tokens still require the exact callback row and preservation of its initialized contents.

## Interpretation

The string gains come from the adapter and function implementation. The earlier measurements did
not establish an unavoidable framework penalty. The framework can retain view headers, prepare a
scalar pattern, and write string parts through a shared function definition.

There are still several different costs:

- Arrow concatenation computes output byte capacity before writing and skips null rows. The shared
  dense sink grows its arena during traversal and visits valid payloads at null positions. The
  current sink interface does not supply a whole-batch byte-capacity plan.
- Ordinary offset-based `Utf8` inputs still use a binding that handles three string layouts. Arrow
  resolves those layouts before its specialized byte iterator. Scalar Utf8 prefix performance
  remains worse even though the Utf8View short-prefix case is close.
- List scaling still visits parent rows and supplies one output slice per row. Arrow can multiply
  the contiguous child buffer in one loop. Fixed widths improve the shared path, but do not make
  those loops identical.
- Complete invocation validates metadata, plans dispatch, retains owners, and publishes a result.
  That fixed cost matters for tiny or zero-width batches even when collection itself is close.
- Boolean retry remains slower than the retained Vortex executor under 16 CGUs without LTO.
  Dense success is much closer. This is an unresolved executor/code-generation gap, not evidence
  that the Arrow output adapter accounts for every regression.

The remaining costs are not all isolated instruction by instruction. Source differences explain
what work remains, but the measurements do not assign a precise fraction to each operation.
Boolean NOT continues to favor a handwritten bitmap kernel. Whole-batch functions remain valid
plugins and do not need a RowFn implementation.

## Reverted experiments

A byte-per-row intermediate before Boolean packing slowed the short-prefix case. A generic list
zip helper did not improve list scaling. Both were removed.

Unchecked selected-output indexing did not close the Boolean retry gap. Neither an infallible
selected-callback result nor ordinary or forced inlining closed it. Those changes were reverted.
The final code keeps checked output indexing and the simpler error path. These were focused
experiments, not repeated causal estimates for the final implementation.

## Verification and artifacts

The final source passed 71 package tests in both debug and optimized profiles, four compile-fail
doctests, and four focused Vortex allocator/sink tests. Tests also validate constructed Arrow data
with `validate_full`. They cover mixed string layouts, non-ASCII boundaries, ownership after input
drop, explicit scalars, metadata, dictionary nulls, list widths, selected errors, and abandonment.

The recorded commands and results are in `verification.json`. No formatter, linter, Miri, sanitizer,
workspace-wide test, or x86 benchmark ran. Source review covered every changed Rust file and its
unsafe contracts. Passing tests do not prove memory safety or cross-platform performance.
The locked normal, build, and development dependency trees contain no Vortex package beneath `rofl`
or `rofl-arrow`. `git diff --check` also passed.

`before.tar.gz` captures the input to this optimization. `after.tar.gz` captures the final source
overlay on commit `24a96cece436409dc4f60f94c6b846d6af017804`. `implementation.diff` isolates this
optimization's Rust changes. The earlier extraction remains uncommitted on the requested branch.

`build.py` reconstructs the baseline temporarily and restores the current files before building the
candidate. It records exact commands, actual Cargo executable paths, and binary hashes. Run it only
in a disposable checkout with the captured source overlay. `run.py` alternates the captured binaries,
and `summarize.py` produces `summary.csv`. Each benchmark fixture checks its expected output before
timing. The complete fixture preflights cover all configured sizes.

From that disposable checkout's repository root:

```sh
python3 research/row-fn-engine/measurements/2026-09-29-optimization/build.py baseline
python3 research/row-fn-engine/measurements/2026-09-29-optimization/build.py candidate
python3 research/row-fn-engine/measurements/2026-09-29-optimization/run.py
python3 research/row-fn-engine/measurements/2026-09-29-optimization/summarize.py
```

Use `--profile bench` with `cargo rustc`. Omitting it builds the development profile, even with
`--bench`. Development-profile attempts and stale executable copies were excluded from the final
comparison. All reported baseline and candidate binaries were rebuilt with matching settings.

`codegen.txt` retains focused optimized-IR and assembly observations. Intermediate compiler dumps
and binary copies were removed after preserving the source, commands, hashes, logs, and results.
`exploratory.tar.gz` retains exploratory logs, including unsuccessful build and optimization attempts.
Its timings are excluded from `summary.csv` and the tables above.
