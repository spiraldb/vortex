<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Comparison with spiraldb/rofl

**Source review, October 2, 2026. No new benchmarks.**

The standalone repository has useful optimizations that this prototype lacks. Its published results
also include cases that are substantially slower than native kernels. They do not support a blanket
claim of zero performance regressions relative to those kernels.

This review covers the default branch, `develop`, at
[`751a57f383e85208de69e20a509fa52ef95fe773`][revision], fetched over SSH. The local comparison is the
uncommitted extraction on `ct/row-fn-engine-research`, based on
`24a96cece436409dc4f60f94c6b846d6af017804`, and its [compute report](compute-options.md).

The repository's benchmark documentation is more qualified than the claim under review. It publishes
slower cases and explains several semantic differences. The decimal [PR description][decimal-pr]
also acknowledges slower Decimal64 multiplication and division.

## What the published numbers say

These numbers come from the repository's [benchmark page][benchmarks]. They are reported results,
not measurements reproduced during this review. The page identifies an Apple M5 Max, one thread,
and September 2026. Ratio means ROFL time divided by native time, calculated from the rounded table.

| Host and case | Native | ROFL | Ratio | More time |
| --- | ---: | ---: | ---: | ---: |
| DataFusion, `starts_with`, Utf8 | 2.3 ms/query | 2.5 ms/query | 1.09× | 9% |
| DataFusion, `starts_with`, Utf8View | 1.7 ms/query | 2.3 ms/query | 1.35× | 35% |
| DataFusion, `byte_length`, dictionary | 0.83 ms/query | 1.3 ms/query | 1.57× | 57% |
| DataFusion, `lower`, dictionary | 0.84 ms/query | 0.97 ms/query | 1.15× | 15% |
| Arrow, Decimal128 add, no nulls | 0.65 ns/row | 0.89 ns/row | 1.37× | 37% |
| Arrow, Decimal64 multiply, no nulls | 0.56 ns/row | 0.90 ns/row | 1.61× | 61% |
| Arrow, Decimal64 multiply, 10% nulls | 0.61 ns/row | 0.93 ns/row | 1.52× | 52% |
| Arrow, Decimal64 divide, truncate, no nulls | 1.04 ns/row | 2.06 ns/row | 1.98× | 98% |
| Arrow, Decimal64 divide, truncate, 10% nulls | 0.83 ns/row | 3.01 ns/row | 3.63× | 263% |
| Arrow, Decimal64 rescale up, no nulls | 0.78 ns/row | 1.23 ns/row | 1.58× | 58% |

Both groups use one million rows. The dictionary has 12 distinct values. Decimal results use
Arrow 59.2 and report the fastest of 15 samples. These are selected counterexamples, not a complete
inventory or an estimate of average performance.

The decimal rows require qualification. The documented fixtures agree in value for these cases,
but the functions have different contracts outside those fixtures. ROFL uses a 128-bit intermediate
for Decimal64 division, while Arrow uses 64 bits. ROFL also checks declared decimal precision where
Arrow checks storage width. Those differences can explain extra work. They do not isolate an
executor penalty.

The table excludes two especially misleading comparisons. Default decimal division rounds in ROFL
and truncates in Arrow. ROFL's string trim removes Unicode whitespace, while DataFusion's `btrim`
removes spaces. Their timings cannot establish the cost of equivalent functions without further
controls. The repository documents both differences.

There are real wins in the same report. Decimal parsing takes about 12.5 to 12.9 ns/row versus
96 to 106 ns/row. Decimal formatting takes about 15 to 18 ns/row versus 71 to 80 ns/row. DataFusion
`contains` also improves across Utf8, Utf8View, and dictionary input. These are reasons to inspect
individual implementations, not evidence that every function improves.

## How the implementations differ

| Area | Local extraction | Standalone repository | Consequence |
| --- | --- | --- | --- |
| Function API | Visitor-style `RowFn` selects typed execution through host capabilities. | `ScalarFn` declares `Args`, `Output`, and `Return`, with a row writer. Physical input dispatch still uses visitors. | This is a different author API, not just a faster replacement executor. |
| Null semantics | Strict propagation. Valid inputs cannot produce null. | Supports strict and null-aware arguments, plus null-producing results. | It handles a broader class of functions than the accepted local contract. |
| Evaluation | Constants, preparation, and deferred retry under the strict callback contract. | Explicit `Volatile`, `Pure`, and `Dense` policies govern repeated or additional evaluations. | Authors still need to choose the correct policy for each operation. |
| Errors | Immediate or deferred row errors eventually become invocation errors. Adapter failures remain terminal. | The default Arrow `run` collects row errors separately from nulls. `ErrorMode::First` is also available. | The error mode belongs in any matched benchmark. |
| String input | Shared functions now select concrete offset or view bindings before traversal. The fixed `Utf8` fallback still matches a storage enum per row. | Dispatch selects a concrete physical column before the row loop, which reads raw slices. | Both support concrete text traversal. The local follow-up remains unverified. |
| Utf8View predicates | Specialized view functions can inspect inline data and prefixes. | `StrView` carries an optional stored prefix. Predicates can reject a mismatch before loading external bytes. | Both recognize view storage. The standalone API makes this capability reusable across hosts. |
| String lengths | Offset bindings now subtract offsets in their native width; view bindings read the stored length. The common value API returns `usize` before conversion to the output width. | A `Len` input can read offset or view metadata with the appropriate integer width. | Both avoid loading string bytes. Generated code and matched timings must establish the remaining conversion and executor costs. |
| String output | Initial Arrow sink copies strings into owned Utf8View storage. | Supports multiple output layouts, capacity hints, and borrowed backing buffers on eligible invocation paths. | Some operations copy fewer bytes or allocate less. Retained input buffers change memory retention. |
| Encodings | Arrow dictionaries materialize logical inputs for row execution. | Eligible pure functions can compute once per dictionary value and retain codes. Optional cached execution also exists. | Fewer callback evaluations can dominate a speedup. This does not measure equal row work. |
| Batch kernels | Whole-batch functions remain outside RowFn under the host plugin boundary. | `ScalarFn::map_ascii` supplies a whole-buffer path for eligible string functions. | ASCII case conversion can bypass row traversal while reusing offsets. |
| Hosts | Vortex and Arrow execute shared visitor definitions. | Arrow, DataFusion, DuckDB, and Vec bindings are present. No Vortex adapter is present. | This repository does not answer the Vortex comparison yet. |

Sources: the [function contract][function], [evaluation policies][evaluation],
[input dispatch][columns], [Arrow entry points][execute], [erased invocation][bind],
and [performance patterns][patterns]. Local behavior is described in the
[implementation record](implementation.md) and [compute report](compute-options.md).

The most useful changes to borrow are physical dispatch before traversal, direct metadata access,
capacity planning, and checks on generated code. Those improve ordinary row execution and address
specific weaknesses in the local prototype. Their source design is stronger evidence than an
unqualified claim that generic code should compile away.

Dictionary transforms, borrowed string output, and whole-buffer ASCII conversion are also useful.
However, their benefit comes partly from changing how much work occurs. The broader function system
needs those capabilities if it is intended to compete with specialized engine kernels.

The standalone implementation still requires careful optimization. Its documentation describes
rewriting checked arithmetic, moving error recording out of loops, preserving inlining, and avoiding
host-array indirection. It centralizes several solutions, but does not eliminate the problem of
writing functions in a form that LLVM can optimize.

## Choice for an experimental release

For a demonstration of strict row functions across Vortex and Arrow, the local architecture is a
reasonable foundation. It already connects shared semantic dispatch to both hosts. Its focused
bindings keep native types, allocation, metadata, and registration under each host.

That architectural fit does not establish greater robustness. The standalone writer checks that a
returned proof belongs to the current row. The local uninitialized sinks instead require an unsafe
author contract. Neither source structure alone proves memory safety or performance parity.

The local crate does not need all the standalone features to serve this purpose. The useful work is:

| Area | Local action and status |
| --- | --- |
| Independent function definitions | The cleanup moves functions into `rowfn-functions`. Its default dependencies exclude both hosts. Optional mappings stay beside their owning semantic traits. |
| Author guidance | The cleanup adds complete owned-output and safe scalar-sink examples, an Arrow-only example, and separate author and adapter guides. |
| Safe common output | The cleanup adds `ElementSink`, which initializes defaults before lending mutable values. Existing uninitialized sinks retain their unsafe contract. Avoiding the default fill with a safe writer remains separate work. |
| Physical dispatch | The follow-up selects concrete text families for shared string functions and borrows raw slices. The fixed `Utf8` fallback still supports generic authoring. Tests and performance verification remain unrun. |
| Efficient preparation | Reuse stored lengths and prefixes, specialize constants before loops, and plan output capacity where the operation supports it. Some paths implement these already. Each remaining change needs a matched comparison. |
| Evidence | Retain correctness fixtures and separate collection from invocation benchmarks. New cleanup code remains unverified. Codegen assertions on representative paths remain future work. |
| Distribution | Manifests retain `publish = false` and versioned local framework dependencies. The optional Vortex function mappings require this experimental checkout. Actual packaging and release verification remain unrun. |

The [author guide](../../rowfn/AUTHORING.md), [adapter guide](../../rowfn/ADAPTERS.md), and
[safety record](../../rowfn/SAFETY.md) describe the cleaned API and its limits. Moving functions into a
new crate can change compiler output, so earlier measurements do not verify the cleanup's performance.
The later text-layout follow-up also changes dispatch and adds collection controls. Its source
review does not establish performance parity or optimal generated code.

Null-producing functions, whole-buffer ASCII mapping, and dictionary transforms expand the scope.
They are useful for a broader scalar-function library, but are not prerequisites for this strict
RowFn demonstration. Whole-batch functions remain supported through the host plugin interface.
There is no reason to broaden the strict contract solely to reproduce the standalone feature list.

## What the evidence does and does not establish

The [code-generation checks][expectations] are useful, especially the checks for scalar fallbacks,
calls, panics, and allocations in loops. Their precise assertions matter: `vector_op` requires some
loop to contain the requested vector operation. It does not require every execution path to use it.
`no_scalar_op` is a separate assertion, and `no_allocs` concerns loops rather than all batch setup.
For example, the [masked Arrow probe][probes] documents a vectorized path for full 64-row demand
words. It does not establish vectorized execution for every mixed validity pattern.

The reviewed [CI workflow][ci] builds benchmarks with `--no-run` and runs correctness and codegen
checks. It does not enforce a timed zero-regression threshold. This review did not establish the
current CI result or reproduce those checks.

The timing boundaries also differ:

- [`string_backends`][string-bench] binds its Arrow string inputs before timing and measures typed
  `run` calls against public Arrow kernels.
- [`library`][library-bench] includes input binding through its case closures, but still calls typed
  `run`. Some functions have no Arrow baseline, and view `head`/`slice` comparisons omit that baseline.
- `library` compares values after casting ROFL output to the native output type, outside timing.
  This checks values across layouts, not equal allocation or output representation. It prints
  mismatch counts rather than failing on every mismatch, so successful process exit is insufficient.
- The published DataFusion table measures whole queries. This includes engine integration and
  encoding choices as well as row execution.

The local report uses Arrow 59.3.0, 16,384-row fixtures, and complete invocation with output planning
outside timing. Its ARM results came from an M4 Max. The standalone tables use different fixtures,
batch sizes, hardware, and Arrow versions. Dividing numbers from these two reports would not give
a meaningful comparison between the implementations.

There are three separate claims to resolve. The published results contradict “never slower than a
native kernel.” They do not establish “never slower than the local prototype,” since that needs a
common benchmark. They also do not establish “the executor adds zero overhead,” since that needs
a direct-loop control with identical row operations, storage, error handling, and setup.

A decisive comparison would run both implementations on the same machine and pinned toolchain,
with matching target, CGU, and LTO settings. It would retain the local slow cases, including string
lengths, scalar string comparisons, LIKE, substring, nullable Boolean output, and list sinks.
Typed collection and complete invocation need separate results. Dictionary transforms and
whole-buffer paths need their own comparisons against equivalent native optimizations.

Only Git inspection, source review, and arithmetic on the published table ran for this report.
No tests, builds, code-generation experiments, or benchmarks ran. This is not a complete unsafe-code
audit or a finding that either implementation has reached its performance limit.

[revision]: https://github.com/spiraldb/rofl/tree/751a57f383e85208de69e20a509fa52ef95fe773
[decimal-pr]: https://github.com/spiraldb/rofl/pull/2
[benchmarks]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/docs/src/benchmarks.md
[function]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-core/src/function/scalar.rs
[evaluation]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-core/src/function.rs
[columns]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-core/src/exec/column.rs
[execute]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-arrow/src/execute.rs
[bind]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-arrow/src/bind.rs
[patterns]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/docs/src/execution/performance.md
[expectations]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-codegen/src/expect.rs
[probes]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-arrow/tests/arrow_codegen.rs
[ci]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/.github/workflows/ci.yml
[string-bench]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-arrow/benches/string_backends.rs
[library-bench]: https://github.com/spiraldb/rofl/blob/751a57f383e85208de69e20a509fa52ef95fe773/rofl-bench/benches/library.rs
