<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# RowFn compute comparison

**Arrow and Vortex · Experimental implementation · September 30, 2026**

`rowfn` lets one visitor-style function definition run through Arrow and Vortex bindings.
The measurements support this approach for selected functions. They do not show that every scalar
function benefits, or that the current implementations have reached their performance limits.

These tables describe the recorded September implementation. The later package split, concrete
text dispatch, and rename have not been benchmarked. Their presence in today's source does not
update these numbers. The [comparison guide](../../rowfn/COMPARING.md) explains how to investigate
a new matched case and separates row work from complete invocation costs.

Three findings matter when reading the results:

- **Primitive operations are the strongest candidates.** Several checked arithmetic functions beat
  Arrow on successful inputs. Numeric comparisons are close to Arrow in the ARM fixtures.
- **Remaining string gaps have different causes.** Some functions still do avoidable work inside
  each row. Others lack an efficient storage binding or output allocation strategy.
- **The Vortex string results depend on an unsafe experiment.** The fast variant skips input view
  validation and null sanitation. It assumes valid, readable UTF-8 at every slot, including nulls.
  Vortex's current safe construction APIs do not enforce that assumption for arbitrary inputs.

## Reading the tables

All times are **microseconds per 16,384 logical rows**, including result allocation and publication.
Each time is the median of three process medians. **Ratio = rowfn time / baseline time**:
`1.25×` means 25% more time, and `0.80×` means 20% less time. Ratios use unrounded measurements.
They compare implementations within one fixture and target, not total engine performance.

ARM results below were refreshed on September 30 from the source recorded for that run. The selected
x86 results come from the September 29 EC2 runs. The groups identify their target, and each ratio uses
a matched baseline from the same run set. Do not compare absolute ARM and x86 times.

The reported `rowfn` path is the complete invocation with output planning outside the timed region.
Input construction is also outside timing. Vortex context creation is outside timing.
This is a broader boundary than a pure row loop, so a ratio alone cannot identify framework overhead.

**Confidence concerns the row work and preparation**, after setting aside fixed invocation overhead:

- **Medium:** the source uses an appropriate operation and preparation strategy, with some supporting
  measurements or code-generation evidence. A material improvement is still possible.
- **Low:** an avoidable per-row cost is known, or the evidence does not establish an efficient loop.
- **N/A:** the fixture has no substantial row computation, or a whole-bitmap operation is the
  appropriate implementation. Its invocation ratio does not assess an optimized RowFn loop.

No row receives **High** confidence. That would require stronger evidence for its exact compiled
path, including relevant vectorization, scalar specialization, and a matched direct-loop control.
These are engineering judgments, not statistical confidence intervals or proven performance bounds.
The confidence links explain what is known and what remains open.

## 1. Arrow native or reference versus rowfn

The ARM inventory covers all 96 cases in the current family, scalar, and storage suites, plus four
addition and deferred-predicate controls. Seven x86 cases show how selected results change by target.
Integer fixtures use `i64`, and floating fixtures use `f64`. String cases retain their storage layout
in the labels. The fixture sources and complete run medians are linked under [Evidence](#evidence).

Unmarked baselines call public Arrow kernels. **†** marks a handwritten Arrow reference, or a
composition that includes materialization. **‡** marks substring comparisons with equal logical
strings but different output storage: Arrow returns `Utf8`, while rowfn returns owned `Utf8View`.

| Function / fixture | Arrow native / reference (µs) | Arrow rowfn (µs) | Ratio | Loop confidence |
| --- | ---: | ---: | ---: | --- |
| **x86 · selected shared fixtures, September 29** | | | | |
| Wrapping negate | 3.329 | 3.738 | 1.12× | [Medium](#arithmetic-and-numeric-comparisons) |
| Wrapping multiply | 4.610 | 5.701 | 1.24× | [Medium](#arithmetic-and-numeric-comparisons) |
| Byte length, Utf8View | 3.682 | 4.589 | 1.25× | [Medium](#string-lengths-and-comparisons) |
| Equality, short Utf8View | 14.170 | 15.080 | 1.06× | [Low](#string-lengths-and-comparisons) |
| Less than scalar, Utf8View | 33.480 | 87.780 | 2.62× | [Low](#string-lengths-and-comparisons) |
| LIKE, literal scalar pattern | 30.310 | 115.200 | 3.80× | [Low](#literal-predicates-like-and-regex) |
| Starts with, five-byte scalar, Utf8View | 30.060 | 38.980 | 1.30× | [Medium](#literal-predicates-like-and-regex) |
| **ARM · integer arithmetic, September 30** | | | | |
| Checked add, array + scalar | 8.416 | 3.916 | 0.47× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked add, nullable + scalar | 11.240 | 5.291 | 0.47× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked subtract | 9.833 | 4.291 | 0.44× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked subtract, nullable | 10.040 | 4.332 | 0.43× | [Medium](#arithmetic-and-numeric-comparisons) |
| Wrapping subtract | 3.520 | 4.166 | 1.18× | [Medium](#arithmetic-and-numeric-comparisons) |
| Wrapping multiply | 3.666 | 4.874 | 1.33× | [Medium](#arithmetic-and-numeric-comparisons) |
| Integer remainder | 23.870 | 9.041 | 0.38× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked negate | 18.990 | 3.229 | 0.17× | [Medium](#arithmetic-and-numeric-comparisons) |
| Wrapping negate | 1.593 | 2.291 | 1.44× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked multiply, arrays | 11.080 | 6.124 | 0.55× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked multiply, dense arrays | 10.040 | 5.541 | 0.55× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked multiply, scalar | 12.040 | 7.083 | 0.59× | [Medium](#arithmetic-and-numeric-comparisons) |
| Checked divide, nullable + scalar | 11.990 | 18.240 | 1.52× | [Low](#arithmetic-and-numeric-comparisons) |
| Wrapping add, arrays | 3.416 | 4.040 | 1.18× | [Medium](#arithmetic-and-numeric-comparisons) |
| Wrapping add, array + scalar | 1.551 | 2.291 | 1.48× | [Medium](#arithmetic-and-numeric-comparisons) |
| **ARM · floating-point arithmetic, September 30** | | | | |
| Float add | 3.458 | 3.416 | 0.99× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float subtract | 3.499 | 4.166 | 1.19× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float multiply | 2.791 | 4.124 | 1.48× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float divide | 3.499 | 4.124 | 1.18× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float remainder | 78.660 | 81.080 | 1.03× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float negate | 1.541 | 2.312 | 1.50× | [Medium](#arithmetic-and-numeric-comparisons) |
| **ARM · bitwise operations, September 30** | | | | |
| Bitwise AND | 3.499 | 4.083 | 1.17× | [Medium](#arithmetic-and-numeric-comparisons) |
| Bitwise OR | 3.458 | 4.166 | 1.20× | [Medium](#arithmetic-and-numeric-comparisons) |
| Bitwise XOR | 3.458 | 3.415 | 0.99× | [Medium](#arithmetic-and-numeric-comparisons) |
| Bitwise AND NOT | 3.395 | 4.166 | 1.23× | [Medium](#arithmetic-and-numeric-comparisons) |
| Bitwise NOT | 1.478 | 2.312 | 1.56× | [Medium](#arithmetic-and-numeric-comparisons) |
| Shift left | 3.541 | 4.207 | 1.19× | [Medium](#arithmetic-and-numeric-comparisons) |
| Shift right | 3.583 | 4.249 | 1.19× | [Medium](#arithmetic-and-numeric-comparisons) |
| **ARM · numeric comparisons, September 30** | | | | |
| Integer equal | 2.833 | 3.249 | 1.15× | [Medium](#arithmetic-and-numeric-comparisons) |
| Integer not equal | 2.812 | 3.270 | 1.16× | [Medium](#arithmetic-and-numeric-comparisons) |
| Integer less than | 2.833 | 3.229 | 1.14× | [Medium](#arithmetic-and-numeric-comparisons) |
| Integer less than or equal | 2.812 | 3.249 | 1.16× | [Medium](#arithmetic-and-numeric-comparisons) |
| Integer greater than | 2.832 | 3.249 | 1.15× | [Medium](#arithmetic-and-numeric-comparisons) |
| Integer greater than or equal | 2.833 | 3.249 | 1.15× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float equal (total order) | 2.833 | 3.270 | 1.15× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float not equal (total order) | 2.833 | 3.249 | 1.15× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float less than (total order) | 4.833 | 5.166 | 1.07× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float less than or equal (total order) | 4.874 | 5.207 | 1.07× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float greater than (total order) | 4.833 | 5.166 | 1.07× | [Medium](#arithmetic-and-numeric-comparisons) |
| Float greater than or equal (total order) | 4.833 | 5.207 | 1.08× | [Medium](#arithmetic-and-numeric-comparisons) |
| **ARM · string lengths, September 30** | | | | |
| Byte length, Utf8 | 0.703 | 2.062 | 2.93× | [Low](#string-lengths-and-comparisons) |
| Byte length, LargeUtf8 | 1.562 | 3.499 | 2.24× | [Low](#string-lengths-and-comparisons) |
| Bit length, Utf8 | 0.916 | 2.062 | 2.25× | [Low](#string-lengths-and-comparisons) |
| Bit length, LargeUtf8 | 4.749 | 3.541 | 0.75× | [Low](#string-lengths-and-comparisons) |
| Byte length, Utf8View | 2.603 | 3.416 | 1.31× | [Medium](#string-lengths-and-comparisons) |
| Bit length, Utf8View | 2.833 | 3.395 | 1.20× | [Medium](#string-lengths-and-comparisons) |
| **ARM · string comparisons, September 30** | | | | |
| Equality, Utf8 | 18.040 | 35.830 | 1.99× | [Low](#string-lengths-and-comparisons) |
| Equality, LargeUtf8 | 15.200 | 34.330 | 2.26× | [Low](#string-lengths-and-comparisons) |
| Equality, short Utf8View | 6.165 | 13.740 | 2.23× | [Low](#string-lengths-and-comparisons) |
| Equality, long Utf8View | 22.290 | 35.740 | 1.60× | [Low](#string-lengths-and-comparisons) |
| Not equal, Utf8View | 16.120 | 21.910 | 1.36× | [Low](#string-lengths-and-comparisons) |
| Less than, Utf8View | 22.620 | 42.910 | 1.90× | [Low](#string-lengths-and-comparisons) |
| Less than or equal, Utf8View | 22.620 | 41.830 | 1.85× | [Low](#string-lengths-and-comparisons) |
| Greater than, Utf8View | 22.620 | 42.120 | 1.86× | [Low](#string-lengths-and-comparisons) |
| Greater than or equal, Utf8View | 22.700 | 42.410 | 1.87× | [Low](#string-lengths-and-comparisons) |
| Less than scalar, Utf8View | 15.290 | 60.910 | 3.98× | [Low](#string-lengths-and-comparisons) |
| **ARM · literal predicates, September 30** | | | | |
| Starts with, three-byte scalar, Utf8View | 18.080 | 17.200 | 0.95× | [Medium](#literal-predicates-like-and-regex) |
| Starts with, five-byte scalar, Utf8View | 18.080 | 20.430 | 1.13× | [Medium](#literal-predicates-like-and-regex) |
| Starts with, array patterns, Utf8View | 74.490 | 22.410 | 0.30× | [Medium](#literal-predicates-like-and-regex) |
| Starts with, scalar, Utf8 | 13.040 | 41.080 | 3.15× | [Low](#literal-predicates-like-and-regex) |
| Ends with, scalar, Utf8View | 18.450 | 20.740 | 1.12× | [Medium](#literal-predicates-like-and-regex) |
| Ends with, array patterns, Utf8View | 93.700 | 22.790 | 0.24× | [Medium](#literal-predicates-like-and-regex) |
| Contains, scalar, Utf8View | 93.990 | 103.100 | 1.10× | [Medium](#literal-predicates-like-and-regex) |
| Contains, scalar, Utf8View, dense | 109.100 | 119.100 | 1.09× | [Medium](#literal-predicates-like-and-regex) |
| Contains, array patterns, Utf8View | 224.700 | 193.900 | 0.86× | [Medium](#literal-predicates-like-and-regex) |
| Contains, scalar, Utf8 | 90.200 | 116.400 | 1.29× | [Medium](#literal-predicates-like-and-regex) |
| ASCII case equality, arrays | 68.370 | 14.290 | 0.21× | [Medium](#literal-predicates-like-and-regex) |
| ASCII case equality, scalar | 28.240 | 43.990 | 1.56× | [Low](#literal-predicates-like-and-regex) |
| **ARM · LIKE and regular expressions, September 30** | | | | |
| LIKE, literal scalar pattern | 17.160 | 86.490 | 5.04× | [Low](#literal-predicates-like-and-regex) |
| LIKE, literal scalar pattern, dense | 16.870 | 83.370 | 4.94× | [Low](#literal-predicates-like-and-regex) |
| LIKE, repeated complex array pattern | 323.300 | 508.200 | 1.57× | [Low](#literal-predicates-like-and-regex) |
| LIKE, alternating array patterns ⚑ | 102,100.000 | 454.900 | 0.0045× | [Low](#literal-predicates-like-and-regex) |
| ILIKE, complex scalar pattern | 360.800 | 428.700 | 1.19× | [Low](#literal-predicates-like-and-regex) |
| NOT LIKE, scalar | 38.740 | 85.990 | 2.22× | [Low](#literal-predicates-like-and-regex) |
| NOT ILIKE, scalar | 187.900 | 156.800 | 0.83× | [Low](#literal-predicates-like-and-regex) |
| Literal prefix via StartsWith | 16.830 | 25.450 | 1.51× | [Medium](#literal-predicates-like-and-regex) |
| Literal prefix via StartsWith, dense | 16.790 | 22.700 | 1.35× | [Medium](#literal-predicates-like-and-regex) |
| Regex, scalar pattern | 207.800 | 274.300 | 1.32× | [Low](#literal-predicates-like-and-regex) |
| Regex, repeated array pattern | 700.300 | 582.700 | 0.83× | [Medium](#literal-predicates-like-and-regex) |
| Regex, alternating array patterns | 731.600 | 637.000 | 0.87× | [Medium](#literal-predicates-like-and-regex) |
| **ARM · string output, September 30** | | | | |
| Concatenate, short strings | 104.600 | 133.900 | 1.28× | [Low](#string-output-and-lists) |
| Concatenate, long strings | 140.100 | 202.600 | 1.45× | [Low](#string-output-and-lists) |
| Concatenate, long strings, dense | 142.900 | 189.000 | 1.32× | [Low](#string-output-and-lists) |
| Substring by byte, Utf8 ‡ | 100.400 | 96.540 | 0.96× | [Low](#string-output-and-lists) |
| Substring by character, Utf8 ‡ | 95.160 | 114.500 | 1.20× | [Low](#string-output-and-lists) |
| Trim, Utf8 † | 207.100 | 210.600 | 1.02× | [Medium](#string-output-and-lists) |
| Trim, LargeUtf8 † | 208.100 | 212.300 | 1.02× | [Medium](#string-output-and-lists) |
| Trim, short Utf8View † | 189.400 | 185.300 | 0.98× | [Medium](#string-output-and-lists) |
| Trim, long Utf8View † | 217.200 | 218.300 | 1.01× | [Medium](#string-output-and-lists) |
| **ARM · lists, timestamps, and dictionaries, September 30** | | | | |
| Scale list, width 4 † | 8.832 | 15.990 | 1.81× | [Low](#string-output-and-lists) |
| Scale list, width 0 † | 0.068 | 1.052 | 15.45× | [N/A](#string-output-and-lists) |
| Checked timestamp tick adjustment † | 10.540 | 5.416 | 0.51× | [Medium](#deferred-boolean-execution-and-degenerate-batches) |
| Dictionary integer add † | 45.950 | 36.740 | 0.80× | [Low](#deferred-boolean-execution-and-degenerate-batches) |
| Dictionary string trim † | 286.200 | 284.900 | 1.00× | [Low](#deferred-boolean-execution-and-degenerate-batches) |
| **ARM · Boolean and degenerate cases, September 30** | | | | |
| Boolean NOT, sliced bitmap | 0.071 | 4.791 | 67.12× | [N/A](#deferred-boolean-execution-and-degenerate-batches) |
| Checked add, all-null input | 0.791 | 1.165 | 1.47× | [N/A](#deferred-boolean-execution-and-degenerate-batches) |
| Checked add, scalar-only † | 1.156 | 1.957 | 1.69× | [N/A](#deferred-boolean-execution-and-degenerate-batches) |
| Nullary constant output † | 1.156 | 2.145 | 1.86× | [N/A](#deferred-boolean-execution-and-degenerate-batches) |
| Nullable checked predicate, dense accepted † | 20.910 | 5.499 | 0.26× | [Medium](#deferred-boolean-execution-and-degenerate-batches) |
| Nullable checked predicate, retry † | 21.290 | 32.910 | 1.55× | [Low](#deferred-boolean-execution-and-degenerate-batches) |

The † references are explicit: trim uses `str::trim` and `StringViewBuilder`, list scaling uses a
flat child-buffer unary loop, and timestamp adjustment uses checked tick addition through
`try_unary`. Dictionary references materialize logical rows before computing. Scalar-only and
nullary references construct repeated outputs. The deferred-predicate reference iterates valid
Arrow values and checks addition. None of those references is a named Arrow scalar kernel.

The alternating-pattern LIKE result is mainly a **pattern-reuse comparison**. The rowfn function
caches each distinct valid pattern during an invocation. Arrow's LIKE array path reuses the previous
pattern, so alternating patterns cause repeated compilation. That large win must not be presented
as a framework speedup. Both regex implementations cache distinct patterns within an invocation.

## 2. Existing Vortex execution versus rowfn

**Matched timings for Vortex's production-native scalar kernels are not available.** Populated
baseline cells below use the retained RowFn executor with benchmark-defined functions. The column
is labeled accordingly. **N/M** means that no matching baseline was measured, not that Vortex lacks
the operation. This table does not establish a Vortex-native-versus-rowfn ranking.

The first group uses the unsafe trusted-input variant on both Vortex paths. The second shows the
same EC2 fixtures with validation enabled. The remaining controls do not consume strings, so the
UTF-8 assumption does not affect them. All EC2 ratios use matched process runs within their group.

| Function / fixture | Existing Vortex RowFn (µs) | Vortex rowfn (µs) | Ratio | Loop confidence |
| --- | ---: | ---: | ---: | --- |
| **x86 · unsafe trusted-input experiment, September 29** | | | | |
| Wrapping negate | N/M | 3.515 | N/M | [Medium](#arithmetic-and-numeric-comparisons) |
| Wrapping multiply | N/M | 5.034 | N/M | [Medium](#arithmetic-and-numeric-comparisons) |
| Byte length, Utf8View | 34.170 | 4.179 | 0.12× | [Medium](#string-lengths-and-comparisons) |
| Equality, short Utf8View | N/M | 52.200 | N/M | [Low](#string-lengths-and-comparisons) |
| Less than scalar, Utf8View | N/M | 118.200 | N/M | [Low](#string-lengths-and-comparisons) |
| LIKE, literal scalar pattern | N/M | 157.300 | N/M | [Low](#literal-predicates-like-and-regex) |
| Starts with, five-byte scalar, Utf8View | N/M | 69.410 | N/M | [Low](#literal-predicates-like-and-regex) |
| **x86 · input validation enabled, same paired experiment** | | | | |
| Byte length, Utf8View | 195.200 | 158.600 | 0.81× | [Medium](#string-lengths-and-comparisons) |
| Equality, short Utf8View | N/M | 230.100 | N/M | [Low](#string-lengths-and-comparisons) |
| Less than scalar, Utf8View | N/M | 272.000 | N/M | [Low](#string-lengths-and-comparisons) |
| LIKE, literal scalar pattern | N/M | 333.300 | N/M | [Low](#literal-predicates-like-and-regex) |
| Starts with, five-byte scalar, Utf8View | N/M | 223.200 | N/M | [Low](#literal-predicates-like-and-regex) |
| **x86 · existing RowFn controls, September 29** | | | | |
| Wrapping add, array + scalar | 3.299 | 4.035 | 1.22× | [Medium](#arithmetic-and-numeric-comparisons) |
| Nullable checked predicate, dense accepted | 6.992 | 7.769 | 1.11× | [Medium](#deferred-boolean-execution-and-degenerate-batches) |
| Nullable checked predicate, retry | 20.760 | 35.920 | 1.73× | [Low](#deferred-boolean-execution-and-degenerate-batches) |
| **ARM · existing RowFn controls, September 30** | | | | |
| Wrapping add, array + scalar | 2.790 | 3.249 | 1.16× | [Medium](#arithmetic-and-numeric-comparisons) |
| Nullable checked predicate, dense accepted | 5.374 | 5.915 | 1.10× | [Medium](#deferred-boolean-execution-and-degenerate-batches) |
| Nullable checked predicate, retry | 15.990 | 32.330 | 2.02× | [Low](#deferred-boolean-execution-and-degenerate-batches) |

The byte-length baseline requests `as_str().len()`. The portable function reads the stored view
length. Its improvement over that baseline includes a different binding operation, not just a
different executor. Neither is a measurement of Vortex's production byte-length kernel.

On EC2, removing the validation pass reduces extracted byte length from 158.600 to 4.179 µs.
The isolated input decode falls from 151.100 µs to 0.159 µs. That explains much of the original
Vortex-to-Arrow gap for this case. String equality, ordering, and predicates still need work after
the validation pass is removed. Skipping checks in this experiment does not provide an upstream
safety argument.

## Why the confidence differs

### Arithmetic and numeric comparisons

Runtime type dispatch happens before traversal, and the row bodies use native arithmetic,
overflow operations, or comparisons. The [collector investigation](measurements/2026-09-29-families/over-50-investigation.md)
identified and removed a chunking pattern that kept an Arrow arithmetic loop scalar. It recorded
vector negate instructions on ARM. Its `i64` multiply loop remained scalar on that target, which
does not provide a 64-bit integer vector-multiply instruction in the tested instruction set.
Scalar code there is not, by itself, a vectorization failure.

That investigation covers selected operations, not every current signature. No equivalent x86
code-generation audit was recorded for the EC2 rows. **Medium confidence** reflects the small row
bodies and targeted evidence. Faster checked-operation timings cover successful inputs. They do
not characterize errors or retry. Nullable division has **Low confidence** because its immediate
error and selected sink path still needs an isolated loop comparison.

### String lengths and comparisons

View length reads metadata without visiting string bytes, so it receives **Medium confidence**.
Ordinary `Utf8` and `LargeUtf8` lengths still construct string values through a binding that supports
multiple layouts. Arrow can operate directly on offsets. That is a known opportunity for an input
binding improvement, so those rows receive **Low confidence**, even when one bit-length fixture wins.

String comparisons receive **Low confidence**. Arrow's inline-view equality can select a specialized
whole-array path before traversal. The rowfn path still tests inline eligibility per row. The Vortex
binding also lacks the inline comparison keys supplied by the Arrow binding. These source differences
leave optimization opportunities even where the x86 short-equality timing is close to Arrow.
See the [shared comparisons](../../rowfn-functions/src/comparisons.rs) and
[Vortex binding](../../vortex-array/src/scalar_fn/unstable/rowfn/input.rs).

### Literal predicates, LIKE, and regex

The [literal predicates](../../rowfn-functions/src/text.rs) preserve view headers, prepare scalar
patterns, and prepare a substring searcher for scalar containment. This supports **Medium confidence**
for several view-based prefix, suffix, and containment cases. It does not prove that scalar row
access disappears from the final machine code. Offset-based prefix access and scalar ASCII equality
retain larger gaps and receive **Low confidence**.

The [LIKE implementation](../../rowfn-functions/src/like.rs) compiles scalar patterns once, but matcher
lookup and matcher-variant selection still happen in the row callback. Selecting the scalar matcher
outside the loop is a known opportunity. LIKE therefore receives **Low confidence**. This is an
optimization of row work, not merely fixed invocation overhead. Regex array preparation already
reuses distinct compiled patterns and receives **Medium confidence**. Scalar regex remains lower
confidence because the same generic matcher path handles it. String and regex work need not become
one SIMD loop to be efficient, but their preparation must avoid repeated work.

### String output and lists

Trim uses the same string operation and copies into owned `Utf8View` output on both paths. Its
near-parity measurements support **Medium confidence**, with no claim of identical generated loops.
Concatenation receives **Low confidence**: it now writes parts directly, but Arrow plans byte capacity
and skips null rows while the shared dense sink grows its arena and visits null payloads.
Substring receives **Low confidence** because the output layouts differ and the retained evidence
does not isolate the remaining traversal and allocation costs.

List scaling also receives **Low confidence**, despite retained [ARM vector-multiply evidence](measurements/2026-09-29-optimization/codegen.txt).
That evidence shows a vectorized child loop for a selected path, not an optimal complete list kernel.
The function prepares its factor and specializes small widths, but still traverses parent rows and
supplies a sink per row. Arrow's reference multiplies the flat child buffer. Zero-width lists have
no child computation and are marked **N/A**.

### Deferred Boolean execution and degenerate batches

Accepted dense execution packs Boolean results directly. Its source and timings support **Medium
confidence** for the measured successful path. Nullable retry receives **Low confidence**: it remains
slower than the retained Vortex executor with 16 code generation units and no LTO. The cause is not
fully isolated, so the report does not assert that a specific missed vectorization causes the gap.

Boolean NOT is a whole-bitmap operation and is marked **N/A**. A manual bitmap kernel is the
appropriate baseline and implementation. All-null, scalar-only, nullary, and zero-width fixtures
mostly assess setup, output construction, or broadcasting. Their ratios cannot establish the
quality of a substantive row loop. Dictionary cases receive **Low confidence** because materialization
and logical-null handling need a separate cost breakdown. Checked timestamp adjustment receives
**Medium confidence** for its tick arithmetic, without any calendar or timezone-conversion claim.

## What this says about the framework

The current ARM wrapping-add control takes 3.395 µs for direct typed collection and 3.540 µs for
shared collection. Complete Arrow invocation takes 4.040 µs versus 3.416 µs for Arrow's kernel.
This supports separating loop throughput from invocation cost for that workload. The controls are
independently compiled paths, so their times are not additive components of one execution trace.

Several remaining opportunities scale with rows or bytes: scalar matcher selection, string-layout
dispatch, view comparison specialization, dictionary materialization, and sink allocation strategy.
Excluding fixed overhead does not exclude those opportunities. The evidence supports selected
portable functions, while whole-batch kernels remain part of the broader plugin model.

## Evidence

- **Table data:** [all 125 comparisons](measurements/2026-09-30-compute-options/comparisons.csv),
  including baseline labels, source datasets, unrounded ratios, and confidence assessments.
- **Current ARM inventory:** [raw runs and per-run medians](measurements/2026-09-30-compute-options/summary.csv),
  [build metadata](measurements/2026-09-30-compute-options/build.json), and
  [run commands and exit codes](measurements/2026-09-30-compute-options/runs.json).
- **Selected x86 Arrow and Vortex string comparisons:** [paired validation experiment](measurements/2026-09-29-assume-valid-utf8/README.md).
  The table uses that experiment's candidate Arrow timings and the explicitly labeled Vortex variants.
- **x86 Vortex addition and Boolean controls:** [host comparison](measurements/2026-09-29-host-comparison/README.md)
  and its three `rowfn-boundaries-run-*.txt` logs.
- **Confidence evidence:** [collector and scalar specialization investigation](measurements/2026-09-29-families/over-50-investigation.md),
  [adapter and sink analysis](measurements/2026-09-29-optimization/README.md), and the linked source files.
- **Fixture definitions:** [families](../../rowfn-examples/benches/arrow_families.rs),
  [scalar functions](../../rowfn-examples/benches/arrow_scalar.rs),
  [storage and references](../../rowfn-examples/benches/arrow_workloads.rs),
  [execution boundaries](../../rowfn-examples/benches/boundaries.rs), and
  [shared host comparisons](../../rowfn-examples/benches/host_comparison.rs).

All measured builds use Rust 1.98.0, Arrow 59.3.0, 16 code generation units, and no LTO.
ARM runs use the Apple M4 Max and repository target flags. EC2 runs use an Intel Xeon Platinum
8488C in a `c7i.4xlarge` VM with `target-cpu=native`, pinned to CPU 2 with its SMT sibling offline.
No builds ran during timing on their respective machine. The local host was not CPU-pinned.

The September 30 refresh rebuilt four benchmark executables and ran their fixture preflights, then
three serial timing processes per suite. The middle repetition reversed benchmark order. It did
not change kernel code or run new compiler-output experiments. Diagnostic variants retained in raw
logs are excluded from the two tables because they are not the current portable implementation.
Full tests, formatting, linting, Miri, and sanitizers were not run for this report. Earlier focused
test results describe their recorded source versions, not a safety proof for the unsafe branch.

The measurements used `ct/rofl-assume-valid-utf8`. The measured source includes uncommitted extraction
work over `24a96cece436409dc4f60f94c6b846d6af017804`. The measurement directory retains a tracked patch,
an untracked source archive, and executable hashes. The earlier EC2 instances and temporary access
resources were removed after their logs were collected. This report did not create cloud resources.

Workspace cleanup on October 2 restored input validation and removed the temporary experiment branch.
The implementation remains on `ct/row-fn-engine-research`. The retained
[experiment patch](measurements/2026-09-29-assume-valid-utf8/experiment.patch) reproduces the unsafe
variant. The tables still describe their recorded source versions.
