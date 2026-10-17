<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow performance diagnosis

The [subsequent optimization report](../2026-09-29-optimization/README.md) records the implemented fixes.
This report preserves the earlier code and the experiments that identified the gaps.

The first implementation did not reproduce several useful Arrow kernel techniques. Its timings
cannot establish the cost of an optimized RowFn implementation. This investigation separates function
algorithms, input bindings, output construction, and traversal where the evidence permits.

The retained change uses Arrow's byte comparisons for portable prefix and suffix functions. The
other variants are benchmark diagnostics. They do not replace the standard adapters or add a second
function registry.

## Utf8View input

The Arrow adapter retains `StringViewArray` and borrows its strings without converting the array or
copying the input bytes. Its standard `Utf8` row kind nevertheless exposes only `&str`. That loses
the view header before the function runs. Supporting the storage format does not reproduce Arrow's
view-aware kernels.

Arrow 59.3.0 dispatches scalar predicates by storage layout before traversal. Its
`prefix_bytes_iter` can read prefixes of at most four bytes directly from the view header, including
for long strings. Longer prefixes and suffixes use specialized byte iterators. The original rofl
functions called `str::starts_with` and `str::ends_with` after decoding the complete string view.

The comparison now includes four diagnostic steps:

1. Use Arrow's length guard and byte comparison through the existing `Utf8` binding.
2. Bind `StringViewArray` directly, removing the three-layout enum from row access.
3. Prepare an owned scalar pattern once per invocation.
4. Retain the view header in a custom row domain and inspect it before reading external bytes.

The fourth variant uses the existing framework extension points. It demonstrates that RowFn can
carry layout information. It is an Arrow-only diagnostic, not a completed portable text domain with
Vortex bindings. Its short-prefix path reproduces the header technique. Its longer-prefix path
still reads `&str`, so it does not reproduce every Arrow iterator optimization.

All values below are microseconds per invocation with 16,384 logical rows. Each value is the median
of three run medians. These are local Apple M4 Max measurements, not cross-platform claims.

| Predicate | Original rofl | Retained byte comparison | Arrow, final run |
| --- | ---: | ---: | ---: |
| Scalar prefix, three bytes | 86.74 | 56.70 | 17.66 |
| Scalar prefix, five bytes | 77.16 | 58.99 | 16.74 |
| Scalar prefix, Utf8 input | 81.95 | 60.95 | 12.95 |
| Array prefixes | 89.24 | 58.37 | 71.49 |
| Scalar suffix | 80.79 | 59.66 | 17.87 |

The original and retained functions were measured in separate builds with the same compiler profile.
The original build also includes controlled variants of the same workload:

| Three-byte scalar prefix, original build | Time |
| --- | ---: |
| Original function | 86.74 |
| Arrow byte comparison, ordinary binding | 68.08 |
| Arrow byte comparison, exact view binding | 52.33 |
| Exact view binding with prepared pattern | 55.54 |
| Row domain retaining the view header | 34.95 |
| Arrow kernel | 18.04 |

The header variant takes 33.39 µs in the final build. It remains slower than Arrow. The data supports
improving both the function body and its binding, but does not isolate every remaining instruction.
Preparing the pattern alone did not improve this case. The executor still constructs the callback's
input tuple, including scalar row access, even when the callback uses prepared state.

The ordinary binding's accessor remains an out-of-line call in the original generated code. Adding
only `#[inline]` removed those calls but did not improve the original predicates. The experiment was
reverted. Header variants also changed substantially in that build despite not using the changed
accessor. That movement is an unresolved compiler or code-layout effect, not evidence that inlining
the ordinary accessor accelerated header access. The final measurements use the restored accessor.

## String output

Both implementations produce `Utf8View` output with independently owned bytes. The difference is how
they construct and publish it.

Arrow's concatenation kernel computes the required external byte capacity, writes the parts directly,
and skips null rows. The portable `Concat` first joins each row into a temporary `String`, then copies
it through `WriteUtf8::write`. The Arrow sink grows its output arena and constructs a validated
`StringViewArray`. Batch publication validates the array data again.

| Final build, 16,384 rows | Short concatenation | Long concatenation |
| --- | ---: | ---: |
| Arrow kernel | 100.30 | 132.70 |
| Arrow kernel plus two validated constructions | 206.50 | 360.20 |
| rofl invocation | 520.10 | 769.10 |

The extra-validation control includes wrapper construction and reference counts as well as UTF-8
validation. It demonstrates a material cost, but cannot assign all remaining time to allocations or
copies. Dense rofl execution also visits null rows with sanitized inputs, while Arrow skips them.

The current `WriteUtf8` capability accepts one complete string. It cannot express appending two
borrowed parts directly. An additional writer capability or a custom sink could do so without changing
RowFn's strict null contract. Batch capacity planning would need an additional path. Neither change
is implemented here.

Removing validation also requires a complete publication proof. Replacing safe constructors with
unchecked constructors alone would weaken the adapter's guarantees. The current experiment retains
validation and does not claim an optimized string-output implementation.

## Fixed-size lists

Arrow scales the contiguous child buffer with one primitive unary operation. The generic rofl
function handles each parent row, obtains input and output slices, and loops over a runtime width.
A diagnostic function specializes that width to four while keeping the same sink and executor.

| Width-four lists, 16,384 rows | Median | Range of run medians |
| --- | ---: | ---: |
| Arrow flat child loop | 6.62 | 6.58 to 9.25 |
| Generic rofl function | 69.95 | 69.45 to 70.62 |
| rofl function specialized to four elements | 56.12 | 56.04 to 56.49 |

Specialization helps but leaves a large gap. Optimized IR contains vector multiplication in both the
Arrow unary loop and the shared traversal. The generic rofl inner fill also has a vector path.
Therefore, the diagnosis is not simply that rofl disables vectorization. Row boundaries, slice
handling, loop structure, and publication remain different. Their individual costs are not isolated.
The current row-oriented sink does not expose one flat traversal over the entire child buffer.
A whole-batch implementation remains a suitable option for this operation.

## What this says about the framework

There is no single measured framework penalty. In the final scalar run, checked array multiplication
takes 6.17 µs through rofl versus 10.33 µs through Arrow. Array-prefix matching also improves with
the corrected function body. Earlier collection-only measurements were close to the direct typed loop.

The standard string domain and writer constrain the optimizations that a function can express.
The extension points permit richer domains and sinks, but the experiment does not yet provide those
as portable, fully tested capabilities. Adapter publication and selected-row traversal also need work.

The earlier nullable division and Vortex Boolean retry regressions remain unresolved. Source review
found validation in selected-row traversal, but this investigation does not quantitatively isolate
that cost. Neither those gaps nor the remaining list gap should be attributed entirely to function
authors, adapters, or the core executor without further controlled measurements.

Whole-batch scalar functions remain part of the existing plugin boundary. Boolean NOT and operations
that naturally process a contiguous child buffer do not need to be rewritten as RowFn callbacks.

## Reproduction and verification

The runs use Rust 1.98.0 (`88d9e12ae178fab0fb5cc050a94da85685d449ea`), LLVM 22.1.8, and
`aarch64-apple-darwin`. The bench profile uses optimization level 3, 16 codegen units, limited debug
information, no LTO, and `force-frame-pointers=yes`. No `target-cpu=native` override was used.
Arrow remains at 59.3.0. Baseline and candidate binaries used matching compiler and profile settings.
No build ran concurrently with timing.

`runs.json`, `final-runs.json`, and `lists-runs.json` preserve the exact invocation commands and
completion statuses. Baseline and inline runs alternate order. The final and list builds each run
three times. The second repetition reverses case order. Each run requests 1,000 samples and
0.1 to 0.3 seconds per case. `summary.csv` retains all run medians and their ranges.

The sources can be rebuilt with `cargo rustc --locked --profile bench -p rofl-examples --bench arrow_scalar --
--emit=llvm-ir,asm,link`, or the corresponding `arrow_workloads` target. Run the captured executable
with `--test` before timing. `run.py` expects captured executables named `baseline`, `inline`, `final`,
and `lists` under `target/rofl-diagnosis`. These temporary copies were removed after measurement.

`source.tar.gz` contains the measured final Rust sources, manifests, and tracked implementation patch.
`text-baseline.rs.txt` restores the original predicate bodies. `string-before.rs.txt` is the unchanged
adapter for baseline and final builds. The inline candidate adds only `#[inline]` immediately before
`StringRows::get_unchecked`. `provenance.json` records source and binary hashes. Earlier implementation
snapshots remain in the September 25 report. Focused IR and assembly excerpts are retained here.

The string binaries each passed 384 benchmark preflight cases, including logical-result comparisons.
The list binary passed 132 cases. Seven scalar-kernel integration tests passed in both debug and bench
profiles after the predicate change. The commands were `cargo nextest run --locked -p rofl-examples
--test scalar_kernels` and the same command with `--cargo-profile bench`.

No formatter, linter, Miri, sanitizer, full-workspace check, or x86 measurement ran in this investigation.
The new diagnostic bindings received source review, not exhaustive safety validation. Full temporary
compiler dumps and captured executable copies were removed after saving the evidence. The source,
raw results, focused excerpts, and test logs remain reviewable on the branch.
