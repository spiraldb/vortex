<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow-rs performance opportunities from the rowfn experiment

**Prepared October 2, 2026 · Arrow 59.3.0 · Apple M4 Max · Measurements from September 30**

The rowfn experiment identifies several improvements worth investigating inside Arrow-rs, without
adopting rowfn itself. The clearest opportunities are checked arithmetic, array string predicates,
and pattern preparation. These involve changing how kernels compute and collect results, rather
than changing Arrow's public function interface.

This is a report of a historical experiment, not an audit of current Arrow mainline. The later
package split, concrete text dispatch, and rename have not been benchmarked. The linked current
functions may differ from the measured source. Use the [comparison guide](../../rowfn/COMPARING.md)
to check a lead against the intended Arrow revision before describing it as a current issue.

The latest inventory contains **27 cases with a lower rowfn median**. Of these, **18 compare public
Arrow kernels and improve in all three process comparisons** by more than the small differences
listed separately below. Four other public-kernel cases need qualification, and five use custom
references. All 27 appear in this report. Older results superseded by the September 30 refresh are
excluded. None of the seven selected x86 cases in the existing report beats Arrow, so the wins here
are ARM evidence, not a cross-platform performance claim.

The largest measured differences are:

- **Checked integer operations:** 1.70–5.88× speedup on successful inputs.
- **Array prefix, suffix, and ASCII equality predicates:** 3.32–4.78× on the tested string mixtures.
- **Alternating LIKE patterns:** 224.44×, primarily from avoiding repeated pattern compilation.
- **Array regex:** 1.15–1.20×, with redundant pattern-string allocation visible in Arrow's source.

These are measured alternatives and source-based leads. No Arrow-rs patch was implemented or
benchmarked for this report, and none of the ratios establishes the speedup of a future patch.

## Measurements against public Arrow kernels

Times are microseconds per **16,384 logical rows**, including allocation and result publication.
Each time is the median of three process medians. **Speedup = Arrow time / rowfn time**, so larger
values are better. This is the inverse of the ratio in the [full comparison](compute-options.md).
Planning and input construction are outside timing. rowfn includes its adapter invocation.

The opportunity column links to the source analysis and its confidence level. Confidence concerns
the proposed mechanism, not rowfn optimality or the speedup of an unimplemented Arrow patch.
Integer inputs are `i64`. String predicates use sliced `Utf8View` arrays unless the label states another layout.
The checked-operation fixtures contain no errors on valid rows.

| Function / fixture | Arrow (µs) | rowfn (µs) | Speedup | Opportunity |
| --- | ---: | ---: | ---: | --- |
| Checked add, array + scalar | 8.416 | 3.916 | 2.15× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked add, nullable + scalar | 11.240 | 5.291 | 2.12× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked subtract | 9.833 | 4.291 | 2.29× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked subtract, nullable | 10.040 | 4.332 | 2.32× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked negate | 18.990 | 3.229 | 5.88× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked multiply, arrays | 11.080 | 6.124 | 1.81× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked multiply, dense arrays | 10.040 | 5.541 | 1.81× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Checked multiply, scalar | 12.040 | 7.083 | 1.70× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Integer remainder | 23.870 | 9.041 | 2.64× | [A](#a-separate-arithmetic-results-from-error-reporting) |
| Starts with, array patterns, Utf8View | 74.490 | 22.410 | 3.32× | [B](#b-separate-string-predicate-values-from-null-collection) |
| Ends with, array patterns, Utf8View | 93.700 | 22.790 | 4.11× | [B](#b-separate-string-predicate-values-from-null-collection) |
| ASCII case equality, arrays | 68.370 | 14.290 | 4.78× | [B](#b-separate-string-predicate-values-from-null-collection) |
| Contains, array patterns, Utf8View | 224.700 | 193.900 | 1.16× | [B](#b-separate-string-predicate-values-from-null-collection) |
| LIKE, alternating array patterns | 102,100.000 | 454.900 | 224.44× | [C](#c-reuse-nonconsecutive-like-patterns) |
| Regex, repeated array pattern | 700.300 | 582.700 | 1.20× | [D](#d-avoid-allocating-regex-cache-keys-on-cache-hits) |
| Regex, alternating array patterns | 731.600 | 637.000 | 1.15× | [D](#d-avoid-allocating-regex-cache-keys-on-cache-hits) |
| NOT ILIKE, scalar | 187.900 | 156.800 | 1.20× | [E](#e-keep-an-ascii-fast-path-in-mixed-unicode-ilike-input) |
| Bit length, LargeUtf8 | 4.749 | 3.541 | 1.34× | [F](#f-investigate-the-largeutf8-bit-length-anomaly) |

The fixtures use small, repeated sets of strings and patterns. Their ratios are useful for finding
kernel costs, but they do not represent arbitrary string lengths, pattern cardinalities, or null
densities. Three process comparisons establish repeatability here, not statistical confidence or
a performance bound.

## Changes Arrow can investigate

### A. Separate arithmetic results from error reporting

**Confidence: strong measured lead, medium confidence in the cause of the full gap.**

In `arrow-arith/src/numeric.rs`, checked add, subtract, and multiply use `try_op!`. Array pairs reach
`try_binary`, and an array plus a scalar reaches `PrimitiveArray::try_unary`. Both accept callbacks
that return `Result`. The no-null binary loop propagates an error at each iteration. The unary and
nullable binary paths also initialize output with zeros before writing successful values.

The shared [integer functions](../../rowfn-functions/src/integer_kernels.rs) and
[add/multiply functions](../../rowfn-functions/src/functions.rs) return a value plus overflow evidence.
The executor combines the evidence and checks it after traversal. This removes the immediate error
exit from the successful loop and allows direct output initialization. These are source differences.
The contribution of vectorization, branches, and initialization has not been isolated for each row
in the table.

An Arrow-specific implementation can compute wrapping results and accumulate failure evidence, then
construct the existing error on failure. For nullable input, evidence must exclude failures that
occur only in null payloads. A diagnostic pass can recover the first valid failure and its operands
when the public error requires them. The fast path must preserve Arrow's overflow checks rather than
replace checked arithmetic with wrapping arithmetic.

Integer remainder has the same opportunity, with an additional constraint: avoid division by zero
even while computing values that will later be discarded. Preserve Arrow's zero result for signed
`MIN % -1`. Its 2.64× result does not demonstrate vectorized integer division.

This change belongs in known arithmetic kernels. Applying it to arbitrary `try_unary` or `try_binary`
callbacks would change callback counts, null-row evaluation, and early-exit behavior. Those generic
APIs have a different contract. Small batches, overflow-heavy data, and null-only failures still
need measurements before choosing a fast path.

### B. Separate string predicate values from null collection

**Confidence: strong measured lead, medium confidence in how the costs divide.**

Arrow's `arrow-string/src/like.rs::op_binary` zips two iterators of `Option<&str>` and collects
`Option<bool>` results. The prefix, suffix, and ASCII equality functions all use this path for array
pairs. The shared [predicates](../../rowfn-functions/src/text.rs) compute Boolean values through direct
packing, with validity handled separately. Their [Arrow binding](../../rowfn-arrow/src/string.rs)
also retains string-view headers. A prefix of at most four bytes can use the header without reading
the external string buffer.

An Arrow implementation can combine input validity once and collect predicate bits directly.
Known infallible predicates can evaluate initialized null payloads when Arrow's input invariants
permit that access. A valid-only alternative remains useful when evaluating null rows is expensive.
The existing scalar predicate path already uses specialized collection, so the array path is the
first target. The measurements do not justify replacing every string kernel.

For array containment, both implementations already use `memchr::memmem::find`. Its smaller 1.16×
gain therefore points toward traversal and collection rather than a new substring-search algorithm.
Prefix, suffix, and ASCII equality gains can also include differences in byte access and generated
code. A matched Arrow collector experiment is needed to separate them. The ASCII equality fixture
contains many unequal-length pairs, which makes it especially sensitive to overhead around a cheap
predicate.

### C. Reuse nonconsecutive LIKE patterns

**Confidence: high that repeated preparation is avoidable, not that 224× generalizes.**

`arrow-string/src/like.rs::binary_predicate` retains only the previous pattern and predicate.
The fixture alternates `%arrow_%` and `%Rust%`. Returning to the first pattern rebuilds its regex
because the second pattern replaced the previous entry. The rowfn implementation prepares distinct
valid patterns once per invocation. Both preparation costs are inside timing.

This explains a concrete difference between the algorithms, although no profiler apportioned the
full timing gap. Arrow can retain more than one compiled pattern, using a bounded cache or another
reuse policy that controls memory when every row has a unique pattern. An on-demand cache can
preserve row-order error reporting and avoid compiling patterns whose corresponding input is null.

The 224.44× result is a low-cardinality alternating-pattern case. A single repeated complex pattern
already benefits from Arrow's existing reuse and was faster in Arrow: 323.3 versus 508.2 µs.
Scalar LIKE was also faster in Arrow. The actionable result concerns reuse across intervening
patterns, not the LIKE API as a whole or a general framework advantage.

### D. Avoid allocating regex cache keys on cache hits

**Confidence: high that redundant allocation exists, medium that it explains the full 1.15–1.20× speedup.**

`arrow-string/src/regexp.rs::regexp_is_match` already caches compiled regexes in a `HashMap`.
However, its no-flags iterator calls `pattern.to_string()` for every non-null pattern before the
cache lookup. A repeated pattern still allocates and copies a string. The flags path constructs an
owned combined expression too. Both paths also pass through a boxed pattern iterator.

The shared [pattern implementation](../../rowfn-functions/src/like.rs) borrows strings for cache
lookups and owns keys only for distinct patterns. For Arrow's no-flags path, a borrowed-key lookup
followed by allocation only on a miss is a direct candidate. A separate flags path can reuse a key
that includes both pattern and flags. Output packing and iterator dispatch are additional leads,
but their individual costs were not measured.

The table covers the two-argument, no-flags API. It does not establish a speedup for flags, capture
groups, or scalar regex. Scalar regex remained slower through rowfn. Preserve empty-pattern behavior,
null propagation, regex errors, and the existing flags semantics when extending the change.

### E. Keep an ASCII fast path in mixed-Unicode ILIKE input

**Confidence: source-supported opportunity, medium confidence in broader benefit.**

For scalar ILIKE, Arrow passes the whole array's ASCII classification to `Predicate::ilike`.
A non-ASCII value makes that classification false, and the predicate uses regex matching throughout
the array. The fixture contains mostly ASCII strings, some Unicode strings, and the pattern `%RUST`.

rowfn prepares an ASCII suffix operation plus a Unicode regex fallback. Each ASCII row uses the
simple comparison, while non-ASCII rows use regex. This remains row-wise work and accounts for a
plausible advantage in the measured NOT ILIKE case. The negation itself is not the optimization.

Arrow can investigate this mixed-input specialization for simple ASCII patterns. Whole-array ASCII
input already has a specialized path. A per-row ASCII scan can lose on other mixtures or long
strings, so the implementation needs distribution-sensitive measurements. Unicode case folding must
retain regex semantics, including characters such as the Kelvin sign that fold to an ASCII letter.

### F. Investigate the LargeUtf8 bit-length anomaly

**Confidence: repeatable timing, low confidence in the cause.**

Arrow's `arrow-string/src/length.rs::bit_length_impl` already subtracts adjacent offsets, multiplies
by eight, and reuses nulls. This is the expected algorithm. rowfn instead obtains a row byte count
through its string binding. Both return `Int64` for `LargeUtf8` and use wrapping multiplication.

rowfn was 1.34× faster in all three comparisons, but it lost the LargeUtf8 byte-length comparison
by 2.24×. There is no demonstrated algorithmic improvement to port from these results. This is a
candidate for a small Arrow-only reproduction and matched compiler-output inspection, including
allocation and loop vectorization. Do not present it as a confirmed vectorization defect.

## Other lower medians, with qualifications

These rows complete the inventory. They are not additional established Arrow kernel improvements.
The speedup column uses the same Arrow/reference divided by rowfn convention.

| Function / fixture | Arrow / reference (µs) | rowfn (µs) | Speedup | Qualification |
| --- | ---: | ---: | ---: | --- |
| Float add | 3.458 | 3.416 | 1.01× | Small difference, direction reverses |
| Bitwise XOR | 3.458 | 3.415 | 1.01× | Small difference, direction reverses |
| Starts with, three-byte scalar, Utf8View | 18.080 | 17.200 | 1.05× | Small gain, one pair nearly equal |
| Substring by byte, Utf8 | 100.400 | 96.540 | 1.04× | Different output layouts |
| Trim, short Utf8View | 189.400 | 185.300 | 1.02× | Custom reference, direction reverses |
| Checked timestamp tick adjustment | 10.540 | 5.416 | 1.95× | Custom checked-tick reference |
| Dictionary integer add | 45.950 | 36.740 | 1.25× | Materialize then checked-add reference |
| Dictionary string trim | 286.200 | 284.900 | 1.00× | Materialize then trim, direction reverses |
| Nullable checked predicate, dense accepted | 20.910 | 5.499 | 3.80× | Custom valid-only predicate reference |

Float add and XOR reverse direction between process comparisons. Their lower aggregate medians
do not establish a faster implementation. Short-prefix matching stays faster in all three pairs,
but one pair is only about 0.5% faster. It is a secondary collector lead, not a strong standalone win.

The substring fixture returns equal logical strings using different layouts: Arrow returns `Utf8`,
while rowfn returns owned `Utf8View`. Retain the required output layout before treating its 4% lower
time as an Arrow improvement. Trim compares a handwritten `str::trim`/`StringViewBuilder` loop,
and its three pairs include a reversal.

The timestamp reference uses `try_unary` for checked nanosecond tick addition and preserves UTC
metadata. It reinforces opportunity A but is not a comparison with Arrow's timestamp arithmetic
kernel. Dictionary references materialize logical rows through the same adapter helper before
computing. The integer result can include the checked-add gain, while the string difference is
small and changes direction. Neither establishes a faster dictionary-native Arrow algorithm.

The checked Boolean predicate compares against a handwritten loop over valid Arrow values.
It demonstrates the potential of direct packing for a custom function. Arrow has no corresponding
public kernel in this comparison, and the rowfn retry fixture is slower than the same reference.

## Evidence and next experiments

The first Arrow-only experiments are checked arithmetic, direct collection for array string
predicates, and a no-flags regex lookup that borrows its key. LIKE cache reuse is a separate
preparation experiment because pattern cardinality changes its memory and performance tradeoffs.
Mixed-input ILIKE and the bit-length anomaly need narrower evidence before selecting a design.

For each candidate, retain logical values, nulls, output types, and observable errors. Compare empty,
small, and 16K-row batches, then vary null density and relevant input distributions. Arithmetic needs
overflow and zero-divisor cases. Pattern functions need repeated, alternating, and unique patterns,
including invalid patterns behind null inputs. Keep compiler, target flags, CGU, and LTO settings
matched. Confirm useful directions on x86 before making target-independent claims.

- [All measured comparisons](compute-options.md), including cases where Arrow remains faster.
- [Original per-process medians](measurements/2026-09-30-compute-options/summary.csv),
  [build metadata](measurements/2026-09-30-compute-options/build.json), and
  [run commands](measurements/2026-09-30-compute-options/runs.json).
- [The 27 selected cases and paired ratios](measurements/2026-10-02-arrow-opportunities/winning-cases.json).
- [Inspected Arrow 59.3.0 source archive](measurements/2026-10-02-arrow-opportunities/arrow-source.tar.gz)
  and [file hashes](measurements/2026-10-02-arrow-opportunities/arrow-source.json).
  Paths and function names above refer to these locally installed crate sources. Current Arrow
  mainline was not audited, so some opportunities may already have fixes upstream.
- Fixture definitions: [arithmetic and pattern families](../../rowfn-examples/benches/arrow_families.rs),
  [string predicates and multiply](../../rowfn-examples/benches/arrow_scalar.rs),
  [storage workloads](../../rowfn-examples/benches/arrow_workloads.rs), and
  [Boolean controls](../../rowfn-examples/benches/boundaries.rs).

The recorded build used Rust 1.98.0, LLVM 22.1.8, 16 code generation units, and no LTO on an Apple
M4 Max. Runs were serial, with benchmark order reversed in the middle repetition. Fixture equality
checks ran before timing. The unsafe Vortex UTF-8 experiment does not affect these Arrow execution
paths. This report used retained measurements and source inspection only. No new builds, tests,
benchmarks, or compiler-output experiments ran.
