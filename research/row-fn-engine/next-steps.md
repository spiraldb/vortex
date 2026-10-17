<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Next steps

[Overview](README.md)

Earlier revisions of the [rowfn implementation](implementation.md) have executable evidence on
Vortex and Arrow. Focused debug, optimized, and compile-fail tests passed for their recorded source.
The later package split, concrete text dispatch, and rename have source review only. The
[first measurements](measurements/2026-09-25-rofl/README.md) and
[scalar-family comparison](measurements/2026-09-29-families/final/README.md) show substantial workload
differences in those earlier revisions.

## Resolve measured regressions

Investigate selected-row retry and runtime-width list sinks first. The extracted Vortex Boolean
retry path is about 2.1 times slower than the retained executor. Width-four Arrow list scaling is
about 7.7 times slower than a matched contiguous child loop at 16,384 rows on the measured ARM host.

The [string diagnosis](measurements/2026-09-29-diagnosis/README.md) and
[optimization run](measurements/2026-09-29-optimization/README.md) isolated string validation and
scalar broadcasting costs. String equality and scalar LIKE still have measured gaps. Preserve the
full selection and initialization contracts when changing fast paths. Keep direct collection
separate from invocation.
Use matching compiler, target, CGU, and LTO settings, including the multi-CGU, no-LTO Boolean retry
comparison. Repeat on x86 before making cross-platform claims. Earlier research timings do not
establish this implementation's performance.

## Migrate consumers and function packages

After resolving the relevant regressions, migrate existing RowFn consumers and remove the duplicate
executor. Move scalar-function implementations into external packages with their registration,
expression helpers, optimizer rules, and statistics rules. Keep registry infrastructure usable
without a mandatory built-in catalog.

Some functions need whole-batch access or permit null results from valid inputs. Those functions
remain part of the broader scalar-function plugin system. They must not broaden RowFn's intentional
strict contract.

## Extend hosts at batch boundaries

A DataFusion wrapper can use `rowfn-arrow` for typed execution. It still needs DataFusion signatures,
coercion, scalar arguments, output fields, and optimizer metadata.

DuckDB and Velox need batch-level FFI designs with explicit registration and invocation contracts.
Rust traits are not a stable binary ABI, and Arrow C Data alone does not define those contracts.

## Keep performance changes separate

Dispatch caching, UTF-8 validation caching, delayed rich errors, and selected Boolean packing remain
separate experiments. Combining them with extraction would prevent attribution of performance
changes. The [candidate list](performance/optimization-candidates.md) retains their context.

Demand propagation through conditionals also remains future work. It must reach child evaluation,
decoding, preparation, and traversal. The [definedness contract](definedness/contract.md) describes
that separate problem. Functions such as `TRY` belong outside RowFn's fixed null behavior.
