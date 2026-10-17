<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# rofl correctness and performance, September 25, 2026

The shared functions execute successfully on both hosts in the focused tests. Performance is not
neutral. On this Apple M4 Max, large-batch checked addition and timestamp adjustment beat the Arrow
baselines. String sinks, list sinks, scalar broadcasting, and selected-row retry need further work.
These are measurements of the extraction, not the earlier research prototypes.

The [follow-up run](scalar-kernels.md) adds six existing Arrow scalar kernels. Boolean NOT is a
whole-batch example, not a recommendation to rewrite packed operations into RowFn.

## Verification that ran

| Check | Result | Log |
| --- | --- | --- |
| Framework, kernels, Arrow, and shared-function package tests, debug profile | 59 passed, 0 skipped | [Debug tests](tests-debug.log) |
| Same packages, optimized bench profile | 59 passed, 0 skipped | [Optimized tests](tests-optimized.log) |
| Vortex adapter, bit, and lane tests | 833 passed, 3,859 filtered out | [Vortex tests](vortex-tests.log) |
| Compile-fail doctests | 5 passed | [Doctests](doctests.log) |
| Arrow benchmark fixture comparisons | All 128 benchmark cases completed their result comparisons | [Preflight](arrow-preflight.log) |
| Timed benchmark cases | 181 cases, each measured in three serial runs | [Commands and exits](baseline-runs.json) |

Tests cover cross-host results and errors, null-only failures, dictionary nulls, unused dictionary
entries, slices, metadata, string ownership, custom bindings, external registration, allocator reuse,
and partial abandonment. Compile-fail cases cover borrowed lifetimes and initialization contracts.
Passing these checks is not a proof that all unsafe implementations are sound.

The first compilation found source errors. Repairs corrected a qualified path, ownership of decoded
list/extension storage, a duplicate re-export, test constructors, imports, and test-harness features.
The allocator test also needed a binding to extend an owner's lifetime. The failure categories are recorded here. Temporary failed-build logs were removed during cleanup. No runtime test failure required a change to expected behavior. No performance optimization
was applied between the three timed runs.

Source review, report-link inspection, and `git diff --check` also completed. No formatter, linter,
Miri, sanitizer, full-workspace test, or x86 benchmark ran.

## Measurement method

- Apple M4 Max, 16 logical cores, 128 GiB RAM; `aarch64-apple-darwin`.
- Rust 1.98.0 (`88d9e12ae178fab0fb5cc050a94da85685d449ea`), LLVM 22.1.8; Arrow 59.3.0.
- Bench profile: optimization level 3, 16 CGUs, no LTO, limited debug information.
- Repository rustflags: `-C force-frame-pointers=yes`. No `target-cpu=native` override.
- `kache` is the configured compiler wrapper. It does not run inside the measured loops.
- Divan OS timer, 1,000 requested samples, 0.1 to 0.3 seconds per case, adaptive sample size.
- Three serial runs. The second run reverses suite and case order. No compilation ran during timing.
- Tables use the median of each run's median. [CSV](summary.csv) retains all three medians and their range.

Inputs and planned fields are constructed outside timing. Complete invocation includes operand checks,
decoding, validity, dispatch validation, allocation, traversal, and publication. Arrow-native baselines
return arrays without the adapter's field checks. The collector comparison excludes those batch costs.
Tests exercise other integer widths; these arithmetic timing fixtures use i64.

These are warm microbenchmarks on one local ARM machine. They do not measure query throughput, cold
caches, concurrent workloads, or x86 code generation. Tiny native operations approach the timer's
reported 41 ns precision, so their ratios need the absolute times alongside them.

## Arrow results

Times are microseconds per invocation at 16,384 logical rows. A ratio above 1 means rofl is slower.
Each native path has matching output values, nulls, and array types, checked before timing.

| Workload | Native baseline | rofl | rofl / native |
| --- | ---: | ---: | ---: |
| Checked integer addition | 8.166 | 3.499 | 0.43 |
| Checked addition, nullable | 11.160 | 5.249 | 0.47 |
| Checked division, nullable | 11.950 | 36.290 | 3.04 |
| Boolean NOT, sliced bitmap | 0.071 | 4.791 | 67.10 |
| Trim Utf8, long strings | 199.400 | 417.800 | 2.10 |
| Trim LargeUtf8, long strings | 196.500 | 415.400 | 2.11 |
| Trim Utf8View, short strings | 175.800 | 279.200 | 1.59 |
| Trim Utf8View, long strings | 208.900 | 418.800 | 2.00 |
| Scale Float64 lists, width 4 | 8.749 | 67.200 | 7.68 |
| Scale Float64 lists, width 0 | 0.066 | 9.957 | 150.43 |
| Checked timestamp tick adjustment | 13.790 | 5.416 | 0.39 |
| Dictionary integer addition | 41.740 | 35.950 | 0.86 |
| Dictionary string trim | 277.300 | 437.900 | 1.58 |
| All-null checked addition | 0.703 | 1.010 | 1.44 |
| Scalar-only checked addition | 1.072 | 7.207 | 6.72 |
| Nullary constant function | 1.020 | 2.114 | 2.07 |

Addition, division, and Boolean NOT use Arrow's existing kernels. Trim uses typed iteration into
`StringViewBuilder`; both sides copy results into output-owned storage. List scaling uses a contiguous
Arrow child-array unary operation. Timestamp adjustment uses checked `try_unary` and retains UTC
nanosecond metadata. Scalar-only and nullary references fill a primitive output buffer directly.
Those references are matched implementations, not claims that Arrow has dedicated kernels for them.
Both dictionary paths materialize referenced rows with the same explicit Arrow materialization path.

Nullable fixtures have seven valid rows out of eight. Boolean inputs start at bit offset 3. List
outputs retain parent validity and non-nullable children. Long strings require external storage;
short Utf8View results fit inline. Dictionary fixtures contain a referenced null value and an unused
entry. These distributions are useful comparisons, not a complete workload sweep.

### Batch size matters

| Workload | 64 rows: native / rofl | 1,024 rows: native / rofl | 16,384 rows: native / rofl |
| --- | ---: | ---: | ---: |
| Checked addition | 0.086 / 0.635 | 0.515 / 0.786 | 8.166 / 3.499 |
| Checked division, nullable | 0.107 / 0.828 | 0.786 / 2.916 | 11.950 / 36.290 |
| Trim Utf8 | 0.927 / 2.624 | 13.160 / 27.700 | 199.400 / 417.800 |
| Scale width-four lists | 0.100 / 1.354 | 0.417 / 5.374 | 8.749 / 67.200 |
| Timestamp adjustment | 0.120 / 0.812 | 0.974 / 1.062 | 13.790 / 5.416 |

A faster large-batch loop does not imply a faster small invocation. At 64 rows, the complete checked
addition invocation costs about 0.55 microseconds more than Arrow's kernel. At 16,384 rows, it saves
about 4.67 microseconds. Empty batches are included in the raw results.

### Repeatability

The large gaps persist across all three runs. At 16,384 rows:

| Case | Native run-median range, microseconds | rofl run-median range, microseconds |
| --- | ---: | ---: |
| Checked addition | 7.582 to 8.291 | 3.478 to 3.520 |
| Trim Utf8 | 197.4 to 201.4 | 412.4 to 422.6 |
| Scale width-four lists | 7.915 to 8.749 | 66.66 to 68.16 |
| Boolean NOT | 0.068 to 0.082 | 4.790 to 4.791 |

These ranges describe three local runs. They are not confidence intervals or portable budgets.

## Collection and Vortex comparisons

| Boundary, wrapping i64 addition | 64 rows, microseconds | 16,384 rows, microseconds |
| --- | ---: | ---: |
| Direct typed collection | 0.039 | 3.333 |
| Shared collection | 0.043 | 3.395 |
| Complete shared executor | 0.489 | 3.916 |

The shared collector is within about 2% of the direct loop at 16,384 rows. The complete executor adds
about 0.5 microseconds. These measurements separate traversal from batch setup; they do not establish
one universal framework overhead.

| Complete operation, 16,384 rows | Baseline, microseconds | Extracted, microseconds | Ratio |
| --- | ---: | ---: | ---: |
| Arrow wrapping add, two arrays | 3.374 | 4.082 | 1.21 |
| Arrow wrapping add, scalar RHS | 1.541 | 3.124 | 2.03 |
| Vortex wrapping add, scalar RHS | 2.749 | 3.207 | 1.17 |
| Vortex nullable Boolean, dense accepted | 5.332 | 5.874 | 1.10 |
| Vortex nullable Boolean, null-only failure and retry | 15.950 | 33.490 | 2.10 |

The Vortex baseline is the retained executor in this working tree. It shares the extracted kernels.
This is not a rebuild of the pre-extraction commit. The legacy benchmark functions bind i64 directly;
the portable functions perform runtime integer dispatch. The complete-call difference therefore
includes dispatch and adapter work, not only the row loop.

For the custom checked Boolean predicate, Arrow rofl takes 5.499 microseconds when dense evidence is
accepted and 38.240 microseconds when null payloads force retry. The matched valid-only Arrow reference
takes 21.330 and 20.910 microseconds respectively. This reference is a scalar checked loop, unlike the
word-wise NOT kernel above. All valid rows succeed; failures occur only in null payloads.

The Vortex retry range is 15.580 to 15.990 microseconds for the retained executor and 33.490 to 33.540
for the extracted executor. Both use multiversioned Boolean packing, the same compiler, target,
16-CGU profile, and no LTO.

## Source and generated-code findings

The measured regressions have different causes and should not be combined into one abstraction tax.

1. **Selected traversal retains checks.** The extracted selected-row callback checks input and output
   bounds, ordering, and prior errors per row. Optimized LLVM IR retains these checks and an output
   bounds-panic edge in the Boolean retry path. The legacy loop has input/constant checks, but does
   not perform the new selection-order validation. This is a plausible contributor to the 2.1x
   Vortex retry regression. No controlled ablation has measured its individual cost.
2. **Boolean NOT has a stronger whole-batch implementation.** Arrow inverts packed words. The row
   implementation reads individual bits and repacks them. The 67x ratio is about 4.72 microseconds
   at this size. This supports keeping whole-batch functions in the broader plugin boundary.
3. **List scaling loses the flat child loop.** The reference scales one contiguous child buffer.
   The row sink handles runtime-width slices for each parent row. A zero-width list exposes the
   difference directly: the row executor still visits rows while the baseline has no children to
   scale. Width-four scaling needs a separate loop/codegen investigation.
4. **String finalization does additional validation.** The sink constructs a validated StringView
   array, and publication rebuilds validated array data. This and row-writer overhead are candidates
   for the roughly 2x long-string gap. Their separate costs have not been measured.
5. **Scalar broadcasting uses take.** The Arrow adapter computes one result, creates indices, and
   broadcasts through Arrow take. The reference fills one output buffer. This explains an extra
   operation and allocation, but the measurement does not isolate each allocation's cost.

Optimized IR and assembly were generated for both benchmark binaries under matching settings.
Cleanup removed the full generated dumps and retained the focused IR excerpts:
[extracted Vortex retry](vortex-retry-selection.ll.txt),
[Arrow retry](arrow-retry-selection.ll.txt), and
[legacy retry](legacy-retry-owned.ll.txt). Excerpts retain original artifact names and line numbers.
They are inspection evidence, not standalone LLVM modules. No vectorizer-remark sweep or optimization
ablation ran. Compiler output was generated after timing; the extra emit flags produce separate
artifacts from the timed executables.

## Follow-up priorities

First investigate selected-row retry and list sinks, because their regressions grow with batch size.
Keep any selection fast path backed by a complete bounds, uniqueness, and lifetime contract. Do not
remove checks from the safe selection interface without replacing their proof.

Next isolate string validation and scalar broadcasting costs. Keep dispatch caching, validation
caching, and selected Boolean packing in separate changes, so each comparison has a clear cause.
Use whole-batch implementations for naturally packed operations such as Boolean NOT. Repeat on x86
before making cross-platform performance claims or migrating the existing Vortex consumers.

## Reproduction and provenance

The exact timed commands and exits are in [baseline-runs.json](baseline-runs.json).
[Environment](environment.txt), [machine](machine.txt), and [build environment](build-env.txt) record
the compiler and target settings. [Provenance](provenance.json) records the base commit, branch,
timed executable hashes, and archive hashes.

[Source snapshot](source.tar.gz) contains the new crates, Vortex adapter, manifests, lockfile, and a
patch for tracked implementation changes. Apply the patch inside the archive to the recorded
base and restore the new source directories to reproduce the measured source. The archive predates
this report's documentation updates. No dependency version was upgraded.

```sh
cargo nextest run --locked -p rofl-kernels -p rofl -p rofl-arrow -p rofl-examples
cargo nextest run --locked --cargo-profile bench -p rofl-kernels -p rofl -p rofl-arrow -p rofl-examples
cargo nextest run --locked -p vortex-array -p vortex-buffer -p vortex-compute \
  -E 'test(scalar_fn::unstable::rofl) | test(lane_kernels) | test(bit::)'
cargo test --locked --doc -p rofl -p rofl-kernels -p rofl-arrow -p rofl-examples
cargo bench --locked -p rofl-examples --bench boundaries --bench arrow_workloads --no-run
```

Pass the two executables printed by the final command to [run.py](run.py). Its `--bench` flag is
required for actual timing. Then run [summarize.py](summarize.py) to regenerate the CSV.
`--test` only checks benchmark fixtures and is not a measurement.

Compiler-output commands used:

```sh
cargo rustc --locked -p rofl-examples --profile bench --bench boundaries -- --emit=llvm-ir,asm,link
cargo rustc --locked -p rofl-examples --profile bench --bench arrow_workloads -- --emit=llvm-ir,asm,link
```

## Cleanup

Cleanup removed temporary failed-build logs, a duplicate patch, the full compiler-output archive,
and the 64 generated IR/assembly files in `target`. The focused IR excerpts, successful verification
logs, raw timings, and small source snapshots remain. [Cleanup record](cleanup.json) lists the exact
removed files and sizes. Shared Cargo build caches and the implementation were preserved.
