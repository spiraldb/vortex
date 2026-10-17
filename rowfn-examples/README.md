<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# rowfn-examples

[Overview](../ROWFN.md) · [Comparison guide](../rowfn/COMPARING.md)

Cross-host tests, external registration, and comparison benchmarks. This package enables both host
mappings from `rowfn-functions` and depends on the modified Vortex checkout. For an independent Arrow
example, use [`rowfn-functions`](../rowfn-functions/README.md).

## Choose a harness

| Harness | Compares |
| --- | --- |
| `boundaries` | Direct loops, shared collection, full invocation, and Vortex retry paths. |
| `arrow_workloads` | Arithmetic, strings, lists, dictionaries, timestamps, and degenerate batches. |
| `arrow_scalar` | Multiplication, literal string predicates, and concatenation. |
| `arrow_families` | Arithmetic, bitwise operations, comparisons, patterns, substrings, and lengths. |
| `host_comparison` | Selected shared functions on Arrow and both Vortex executors. |

Fixture results are compared before timing. Some baselines are composed references, and substring
outputs have different layouts. The [comparison guide](../rowfn/COMPARING.md) explains those limits
and supplies focused commands. Use matching compiler, target, allocator, CGU, and LTO settings.

Text-length controls in `boundaries` use the same output allocation. Decoding is outside timing,
while shared collection includes view construction and length validation. These new controls have
not run. The [project status](../ROWFN.md#status) also identifies other unverified changes.

<details>
<summary>Function and path inventory</summary>

| Function | Paths exercised |
| --- | --- |
| `Add<false>` and `Add<true>`. | Eight integer widths, owned output, constants, and deferred failure. |
| `Divide`. | Immediate errors and exact scalar-slot initialization. |
| `PositiveSum<MULTIVERSIONED>`. | Packed Boolean output, checked addition, and nullable dense retry. |
| `Trim`. | Borrowed UTF-8 input and output-owned string storage. |
| `Scale`. | Float32/Float64 lists, runtime width, prepared constants, and row initialization. |
| `AdjustTicks`. | An external timestamp domain with checked ticks and retained native metadata. |
| `Seven` and `Not`. | Nullary registration and sliced Boolean inputs. |
| `Multiply`. | Checked integer multiplication and deferred overflow evidence. |
| `StringPredicate` and `Concat`. | Literal string predicates, prepared substring search, and string output. |
| `Subtract`, `MultiplyWrapping`, `Remainder`, and `Negate`. | Checked or wrapping integer arithmetic across eight widths. |
| `FloatArithmetic` and `FloatNegate`. | Float32/Float64 arithmetic with IEEE zero and NaN behavior. |
| `BitwiseAnd`, `BitwiseOr`, `BitwiseXor`, `BitwiseAndNot`, `ShiftLeft`, `ShiftRight`, and `BitwiseNot`. | Strict integer bitwise operations. |
| `Equal`, `NotEqual`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, and `GreaterThanOrEqual`. | Strict integer and floating comparison with packed Boolean output. |
| `Like` and `RegexpIsMatch`. | Valid-row pattern preparation and packed Boolean output. |
| `SubstringBytes` and `SubstringChars`. | Strict substring errors and independently owned Utf8View output. |
| `ByteLength` and `BitLength`. | Row-wise UTF-8 byte counts across three Arrow layouts. |
| `StringEqual`, `StringNotEqual`, `StringLessThan`, `StringLessThanOrEqual`, `StringGreaterThan`, and `StringGreaterThanOrEqual`. | Strict string comparison across Arrow string layouts. |

</details>

## Behavior and registration

Tests cover values, nulls, errors, metadata, explicit scalars, empty batches, dictionary nulls,
unused entries, string ownership, list shape, custom bindings, and filtering. Vortex adapter tests
cover its allocator and primitive payload reuse. Rustdoc cases cover lifetime and token failures.

`tests/arithmetic.rs` registers and deserializes `SevenPlugin` through the existing Vortex registry.
Its persistence wrapper delegates to `VortexRowFn<Seven>`. No built-in case or parallel registry is
added. Whole-batch functions can use `ScalarFnVTable` directly.

Pattern preparation belongs to one invocation. RowFn's LIKE example retains all distinct patterns,
while Arrow 59.3.0 array LIKE retains the preceding pattern. Error and output differences are listed
in the [comparison guide](../rowfn/COMPARING.md#match-the-operation).

## Recorded evidence

Earlier revisions passed focused tests and were benchmarked. They do not verify the package split,
concrete text dispatch, or rename. Raw artifacts retain the earlier `rofl` name.

<details>
<summary>Historical runs and source investigations</summary>

| Report | Contains |
| --- | --- |
| [Initial run](../research/row-fn-engine/measurements/2026-09-25-rofl/README.md) | Tests, timings, compiler output, and source provenance. |
| [Additional scalar kernels](../research/row-fn-engine/measurements/2026-09-25-rofl/scalar-kernels.md) | Multiplication and string predicate comparisons. |
| [Diagnosis](../research/row-fn-engine/measurements/2026-09-29-diagnosis/README.md) | String bindings, validation, and list traversal. |
| [Optimization](../research/row-fn-engine/measurements/2026-09-29-optimization/README.md) | Before/after measurements and remaining gaps. |
| [Scalar families](../research/row-fn-engine/measurements/2026-09-29-families/final/README.md) | Additional arithmetic, comparison, and pattern cases. |

</details>
