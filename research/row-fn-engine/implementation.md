<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Experimental rowfn implementation

[Overview](README.md)

The extraction now has framework, kernel, Vortex, and Arrow source implementations. Shared proof
functions execute on both adapters through one visitor binder. Earlier validated revisions passed
focused tests in debug and optimized profiles. The
[first measurements](measurements/2026-09-25-rofl/README.md) show that performance is not neutral.
This page distinguishes tested behavior from unresolved safety and performance questions.

The historical `ct/rofl-assume-valid-utf8` branch ran an
[unsafe benchmark experiment](measurements/2026-09-29-assume-valid-utf8/README.md).
That decoder assumed valid string views at every slot, including null slots. The temporary branch
was removed after its evidence and patch were retained. Current Vortex decoding retains validation.
Earlier test results describe the validated implementation. The experiment ran only benchmark
fixtures that satisfy the assumption. It does not support arbitrary safe input construction.

## Crates and boundaries

| Location | Implemented responsibility |
| --- | --- |
| [`rowfn-kernels`](../../rowfn-kernels) | Shared lane sources, out-of-place loops, borrowed bitmaps, and Boolean packing. |
| [`rowfn`](../../rowfn) | Row kinds, binding capabilities, typed visitors, plans, constants, preparation, retries, and initialization evidence. |
| [`vortex-array` adapter](../../vortex-array/src/scalar_fn/unstable/rowfn) | Existing decoders, lazy validity, execution allocation, output construction, and the registry wrapper. |
| [`rowfn-arrow`](../../rowfn-arrow) | Arrow fields, explicit operands, materialization, retained storage, and Arrow output. |
| [`rowfn-functions`](../../rowfn-functions) | Portable functions and optional native domain mappings. Its default package has no host dependencies. |
| [`rowfn-examples`](../../rowfn-examples) | Cross-host fixtures, external Vortex registration, and comparison benchmarks. |

All new packages are unpublished. `vortex-array` depends directly on `rowfn`. The Vortex adapter stays
inside that crate, which avoids a dependency cycle with existing built-in functions. Vortex's buffer
and compute entry points delegate extracted algorithms to `rowfn-kernels`.

The framework, kernel, and Arrow crates have no direct or transitive Vortex dependencies.
`rowfn-functions` has optional host mappings, including the mapping to this experimental Vortex
checkout. Cross-host tests belong to `rowfn-examples`. Arrow remains at the existing lockfile version,
59.3.0. No universal owned buffer or replacement for `DType` was introduced.

## Decisions implemented

`Host` is a small associated-type family. Focused traits own native type checks, input decoding,
output allocation, sinks, validity, and publication. Row traversal is statically dispatched.
`OutputBinding::build_from` preserves the existing bulk output customization, while `OutputBuffer`
retains stable slots and host-specific publication.

Functions keep the visitor author model. Their semantic binder is shared across hosts. Small
capabilities classify integer widths, list shapes, and timestamps. The external `TimestampTicks`
row kind has bindings in `rowfn-functions`, demonstrating the downstream orphan-rule arrangement.
Unknown extensions do not enter ordinary primitive bindings through matching storage widths.

Selected traversal uses ordinary decoding for a valid scalar. Null-tolerant decoding is an array
capability, so a downstream scalar binding need not implement it. A custom-binding fixture covers
that distinction alongside filtered array decoding.

RowFn retains its strict contract: null inputs imply null output, and valid inputs cannot produce
null. Constants and retries can alter callback counts. Preparation stays within the invocation.
Immediate row errors select valid-only traversal on nullable input. Only rejected deferred evidence
can trigger null-based suppression or retry. Other adapter errors remain terminal.

The core retains decoded owners until preparation and traversal finish. Scalar and fixed-size-list
tokens still require unsafe construction for the exact callback row. Neither a forged token nor an
unrelated initialized slot is sufficient. Output buffers permit abandonment after partial writes.

Vortex dense execution retains lazy validity. Recognized masked constants decode their underlying
value, so a null first row cannot sanitize the constant used by later valid rows. Primitive output
publication delegates to the existing allocation-preserving path. Boolean dense retry retains its
combined-state mutable borrow, separate terminal loop, and multiversioning mode.

Arrow supports sliced primitives and Booleans, three UTF-8 layouts, primitive fixed-size lists, and
explicit dictionary materialization. Dictionary functions execute only on referenced logical rows,
with key and value nulls combined. String output copies into `Utf8View` storage. Zero-width lists
retain an explicit row count. Timestamp output preserves units, timezone, and field metadata even
when the batch is empty or all null.

The explicit `VortexRowFn<F>` wrapper uses the existing scalar-function registry. It does not supply
serialization by default. Function packages that need persistence or optimizer hooks can provide a
vtable that delegates to the adapter. Whole-batch functions remain supported through that registry
without changing RowFn's null contract. A function package can contain both RowFn wrappers and
handwritten whole-batch implementations. Extracting scalar functions does not require rewriting
Arrow's scalar catalog into RowFn.

The proof package now includes integer and floating arithmetic, bitwise operations, numeric and
string comparisons, substrings, and string lengths. The LIKE and regex examples prepare an owned
matcher for each distinct pattern on valid rows. This state exists only during one invocation.
Arrow 59.3.0 also caches distinct regex patterns, while its array LIKE kernel reuses the preceding
pattern. The broader LIKE cache is an implementation choice in the example, not a RowFn requirement.
Boolean bitmap operations, offset-only string lengths, and other whole-batch algorithms can stay
outside RowFn when their native execution is simpler or faster.

## Cleanup for an experimental library

The October 2 cleanup separates portable function definitions from cross-host evidence fixtures.
`rowfn-functions` owns the definitions and domain traits, with independent optional `arrow` and
`vortex` features. Keeping the optional mappings in that package satisfies Rust's orphan rules.
The Arrow feature does not enable Vortex. The existing fixture package enables both mappings and
retains its external registration example, tests, and benchmark entry points.

The framework now offers `ElementSink` for safe immediate scalar errors. It initializes output
defaults before lending `&mut T`, so callbacks require no initialization token. The uninitialized
scalar and list sinks retain their unsafe exact-row contracts and existing behavior. New Arrow-only
fixtures cover preserved defaults across views and moves, empty publication, skipped null-payload
errors, and abandonment after a later valid-row error.

The package includes [function authoring](../../rowfn/AUTHORING.md),
[adapter authoring](../../rowfn/ADAPTERS.md), and a [source safety review](../../rowfn/SAFETY.md).
An Arrow-only example uses the shared checked-add and trim definitions. Package manifests list their
distribution files, and independent path dependencies now include versions. All packages remain
unpublished. The optional Vortex function mappings still require this experimental Vortex checkout.

The cleanup retains row callbacks and compiler-sensitive traversal and packing algorithms. Moving
functions between crates can affect generated code, so existing measurements do not cover this
revision. Cargo updated the lockfile offline, adding only the new function package and its edges.
No tests, builds, formatting, linting, packaging checks, codegen experiments, or benchmarks ran for
this cleanup. The changes have source inspection only.

## Concrete text dispatch

The October 2 follow-up adds `TextLayout` and three text row families to `TextBinding`.
Shared string definitions select concrete input families during semantic dispatch. Their callbacks
remain in the function package, with the same dispatch rules on Arrow and Vortex.

Arrow's offset bindings retain their native arrays and lend matching offset and byte slices.
Length reads subtract offsets in the native width without constructing a string. View bindings lend
raw headers and backing buffers once, retaining direct length, inline, and prefix operations.
Vortex maps the families to its existing validated decoder without changing traversal or validity.
The fixed `InputBinding<Utf8>` fallback remains available and still dispatches its Arrow layout
per row. The shared trimming, concatenation, comparison, predicate, pattern, substring, and length
definitions use concrete text bindings instead.

Downstream implementations of the experimental `TextBinding` trait must supply the new families and
layout selector. More layout combinations can increase monomorphization and affect inlining.
This source change does not establish vectorization or a speedup. Existing measurements describe
the earlier implementation.

New Arrow-only fixtures cover retained sliced storage and rejection of extensions or offset-width
mismatches. Shared-function fixtures cover mixed layouts, nullable slices, prepared constants, and
owned outputs after input operands are dropped. Length collection controls compare a direct metadata
loop with the shared collector using the same output allocation. The existing Arrow family benchmarks
provide native and complete-invocation controls. None of these new fixtures or controls ran.

## Reader-facing documentation

The October 6 documentation cleanup adds a [project overview](../../ROWFN.md) aimed at Arrow
maintainers and a [kernel comparison guide](../../rowfn/COMPARING.md). The guide describes semantic
matching, baseline qualifications, fixture checks, measurement boundaries, and focused commands.
It presents RowFn as an independent comparison implementation, not a correctness oracle or a
performance upper bound.

Package READMEs now link to those entry points. Historical reports state that their tables predate
the package split, concrete text dispatch, and rename. The current checkout remains a modified
Vortex workspace; standalone distribution is separate work. No Rust source, benchmark fixture,
unsafe contract, or measurement artifact changed during this documentation cleanup. No tests,
builds, formatting, linting, codegen experiments, or benchmarks ran.

## Unsafe contract adjustment

Source review found that zipped lane traversal relies on stable source lengths, and Boolean
collectors invoke closures containing unchecked reads. `IndexedSource` and `BooleanOutput` are now
unsafe traits with those obligations stated explicitly. Existing implementations carry the proof.
This changes implementation requirements for downstream code but does not change the row algorithms.

The same review found that `ReinterpretSink::new` accepted arbitrary `Copy` types based only on size
and alignment. Those checks cannot prove bit validity. Its constructor is now unsafe, and its numeric
callers state why all written bit patterns remain valid. A compile-fail example covers the safe
construction attempt. This related contract repair leaves the existing cast loops unchanged.

## Evidence boundary

Source inspection covered the extracted loops, adapter mappings, callback lifetime boundaries,
initialization tokens, partial abandonment, and native publication paths. Cargo updated the lockfile
with `cargo update --workspace --offline`, adding only the four new workspace packages and their
dependency edges. Existing dependency versions did not change.

The authorized verification run passed 59 package tests in both debug and optimized profiles,
833 focused Vortex adapter/bit/lane tests, and five compile-fail doctests. Benchmark fixtures also
compared Arrow results before timing. Initial compilation errors were repaired without changing
expected runtime behavior. The [measurement report](measurements/2026-09-25-rofl/README.md) preserves
commands, failed and successful build logs, raw timings, generated code, and the measured source.

The benchmark run measured 181 cases in three serial repetitions on an Apple M4 Max. Shared collection
was close to the typed loop. Complete invocation added small-batch overhead. Large-batch Arrow checked
addition and timestamp adjustment beat their baselines, while list and string sinks were slower.
The extracted Vortex Boolean retry path took about 2.1 times the retained executor's time.

The [additional scalar-kernel run](measurements/2026-09-25-rofl/scalar-kernels.md) compared six more
functions against Arrow's existing kernels in 136 cases, each repeated three times. Seven new
cross-host tests passed in debug and optimized profiles. Multiplication and array-pattern ASCII
equality improved at the largest batch size. Scalar string predicates and concatenation regressed.

The [Arrow performance diagnosis](measurements/2026-09-29-diagnosis/README.md) follows those regressions
through function algorithms, string bindings, output validation, and list traversal. That version
used Arrow-style byte comparisons but exposed only `&str` through its standard string binding.
Custom diagnostic bindings showed that the framework could retain Utf8View headers. String output
still used temporary concatenation storage and repeated validation at that point.

The subsequent [optimization work](measurements/2026-09-29-optimization/README.md) replaces temporary
concatenation strings with direct part writes and removes redundant Arrow output validation.
Portable text predicates now select bindings before traversal and retain view headers on both hosts.
Arrow input reads use the native guarantee that null string payloads are also valid. Vortex decoding
keeps its validation and sanitation. `Selection` now requires an unsafe implementation with stable,
bounded, ordered traversal and immediate error propagation. This permits checks once per selected
batch instead of once per selected row.

The [scalar-family comparison](measurements/2026-09-29-families/final/README.md) tested 62 matched
cases at four row counts, with three process runs per case. At 16,384 rows, numeric comparisons
were about 1.14 to 1.16 times Arrow's time. Byte length on Utf8 was 8.0 times Arrow's time because
Arrow reads offsets without visiting string bytes. Short Utf8View equality was 5.68 times Arrow's
time, so its current row binding is not a competitive replacement for Arrow's view-aware kernel.
Regex with array patterns was within 0.91 to 0.94 times Arrow's time after pattern caching. Scalar
LIKE remained about 5 times Arrow's time even with one prepared matcher. These measurements support
choosing RowFn per function instead of replacing Arrow's scalar catalog wholesale.

The [follow-up investigation](measurements/2026-09-29-families/over-50-investigation.md)
examines the cases above 1.5 times Arrow's time. It fixed a vectorization blocker in
Arrow primitive output collection and reduced several string-binding costs. The older
table above remains the pre-change measurement. Literal LIKE still selects its matcher
inside the row loop, and scalar Utf8View ordering remains slower. These are open
implementation questions, not measured limits of the RowFn framework.

The [three-host comparison](measurements/2026-09-29-host-comparison/README.md) ran seven
shared functions on Arrow and Vortex on Apple ARM and an EC2 x86 host. Vortex's
validated UTF-8 decode dominates several string invocations. Extracted Vortex
addition remains about 22% slower than the retained executor on the x86 host.
Nullable Boolean retry with a failure only in null payloads remains about 73%
slower. These results do not support a broad consumer migration yet.

No formatter, linter, Miri, sanitizer, or full-workspace test ran. The x86 run
covered seven selected functions, not the full 62-case family suite.
Passing tests and preserved compiler-sensitive source structure do not prove safety or
performance equivalence. Earlier research timings describe earlier experiments.

## Assessment

This milestone has not reduced the amount of active Vortex code. The retained RowFn
executor remains, and the Vortex adapter still calls its UTF-8 decoder and output
code. The Arrow adapter also needs representation-specific bindings. Sharing a
visitor definition does not make the two hosts run the same machine code or pay
the same input costs.

The evidence supports `rowfn` as an optional authoring tool for selected strict
row functions. It does not support moving Arrow's whole scalar catalog or every
Vortex consumer onto it. Vortex's existing scalar registry can host whole-batch
functions without RowFn. A broad migration should wait until the Boolean retry
regression is fixed and the Vortex string validation boundary is understood.

## Remaining work

The first milestone now has executable cross-host evidence. Selected-row retry, list sinks, string
finalization, and scalar broadcasting need focused performance investigations before consumer
migration. Unsafe contracts still need review beyond the existing tests and compile-fail cases.
The current results do not establish cross-platform behavior or a production performance budget.

The old Vortex executor and its current consumers remain. This experiment does not migrate the full
scalar-function catalog, add dispatch or UTF-8 validation caching, delay rich errors, or optimize
selected Boolean packing. Pattern matchers use invocation-local caching as described above. It also
does not implement DataFusion, DuckDB, Velox, or a stable FFI.

Later extraction can move function packages with their expression helpers, optimizer rules, and
statistics rules. Registry infrastructure must remain independent of a required built-in catalog.
Functions with whole-batch needs or wider null semantics continue to use the broader plugin system.
