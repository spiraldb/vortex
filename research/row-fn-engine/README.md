<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# RowFn engine research

The current packages use the name `rowfn`. Archived measurement logs, commands, captured sources,
and artifact names retain `rofl`, the name used during those runs.

New readers can start with the [project overview](../../ROWFN.md) and
[Arrow comparison guide](../../rowfn/COMPARING.md). This tree preserves the research history and
measurement evidence, including alternatives that are not implemented.

The experimental `rowfn` extraction now has framework, kernel, Vortex, and Arrow source
implementations. Shared visitor definitions exercise the same dispatch and row operations on both
hosts. Earlier revisions passed focused debug and optimized tests. The October 2 cleanup adds a
portable function package, self-contained author guides, and a safe initialized scalar sink.
The package split, text-dispatch follow-up, and rename have source review only. The
[first measurements](measurements/2026-09-25-rofl/README.md)
show substantial differences across workloads, including slower list sinks and Boolean retry.
The [additional Arrow kernel comparisons](measurements/2026-09-25-rofl/scalar-kernels.md) cover
string predicates, concatenation, and multiplication.
The [scalar-family comparison](measurements/2026-09-29-families/final/README.md) covers 62 more
matched Arrow cases and shows why some simple row operations still need native batch kernels.
The [implementation record](implementation.md) separates tested behavior from remaining work.

The design keeps native runtime types in each adapter. Small semantic capabilities let portable
functions share dispatch without replacing `DType`. Host adapters retain decoding, allocation,
validity, and registration. RowFn preserves strict null propagation and cannot produce null from
valid inputs. Whole-batch functions remain supported by the broader scalar-function registry.

This tree also retains findings and design alternatives from earlier research. Those proposals are
historical where they differ from the implementation record. The [recent changes](current-system/recent-changes.md)
describe the baseline improvements on which extraction builds. The [evidence record](evidence.md)
keeps earlier measurements separate from current source findings.

## Start here

These pages contain the main argument. The other files provide supporting detail.

| Page | What it answers |
| --- | --- |
| [Compute comparison report](compute-options.md) | How do Arrow and Vortex timings compare, and how much optimization remains? |
| [Arrow performance opportunities](arrow-performance-opportunities.md) | Which measured wins suggest improvements inside Arrow-rs? |
| [Standalone rofl comparison](rofl-repository-comparison.md) | What differs in spiraldb/rofl, and does its evidence support zero regressions? |
| [Function author guide](../../rowfn/AUTHORING.md) | How do owned output, preparation, errors, and sinks fit together? |
| [Adapter guide](../../rowfn/ADAPTERS.md) | Which storage and semantic contracts belong to a host? |
| [Implementation](implementation.md) | What works, and which evidence is still missing? |
| [Initial measured revision](measurements/2026-09-25-rofl/README.md) | How did the recorded extraction compare with retained Vortex and Arrow execution? |
| [Scalar families](measurements/2026-09-29-families/final/README.md) | Which additional Arrow functions benefit from shared row execution? |
| [Recent changes](current-system/recent-changes.md) | Which research concerns have been addressed since September 21? |
| [Design](architecture.md) | What belongs in the library, and which API choices remain open? |
| [Performance](performance/README.md) | What was measured, and which costs need attention? |
| [Demand and completion](definedness/README.md) | How can conditionals avoid errors in rows they do not need? |
| [Next steps](next-steps.md) | Which changes and experiments resolve the remaining questions? |

## Main findings

**The type system can be generic.** Current row loops already operate on typed values and views.
An isolated [Rust experiment](type-system/compiled-proof.md) demonstrates shared dispatch and
borrowing across two adapter crates. A portable function still needs a semantic contract.
An `i64` timestamp, a decimal coefficient, and an ordinary integer are not interchangeable inputs.

**Outer nullability can move to the binding boundary.** The core delegates outer nullability to
native type bindings. Nested child nullability and extension metadata must survive.
The implementation uses semantic capabilities and rejects unsupported mappings.
The [type analysis](type-system/README.md) compares this with an extensible capability design.

**Most reusable logic sits between host operations.** Typed traversal, constants, preparation,
deferred failure evidence, and sink initialization belong in the core. Adapters own column access,
allocation, output construction, and host registration. `OutputElement::Buffer` and `OutputBuffer`
already separate output storage from traversal, with execution-allocator support. Extraction can
build on that boundary. Explicit wrappers such as `VortexRowFn<F>` avoid the blanket-implementation
constraints. The [source inventory](current-system/README.md) maps the boundary and its safety
obligations.

**The original measurements separate several costs.** The ARM experiment isolates about 101 to
104 ns of additional batch work with a shared collector. It also finds a larger collector difference
at 16,384 rows. The x86 sweep reports different setup costs and exposes UTF-8 validation, retry,
and nullable execution costs. These are different experiments, not one combined benchmark. The
[performance summary](performance/README.md) keeps both baselines and their limits visible. These
timings predate direct packed Boolean retry, the UTF-8 decode change, and allocator and mask fixes.

**Demand, completion, and validity are different facts.** A caller requests rows. A successful call
completes those rows, including null results. Validity says which completed results are non-null.
Demand must reach child evaluation, decoding, and preparation before the row loop. A bitmap added
only to that loop is insufficient. The [definedness contract](definedness/contract.md) states the
required behavior and safe output representation.

**The row API needs a batch alternative.** Buffer reuse, dictionary transforms, and fused kernels
can require whole-column access. Keep that path under the same function semantics. Existing
RowFn also has a narrower contract than a general UDF: strict null propagation, fixed arity,
synchronous execution, and no null result from valid inputs. Functions with wider semantics belong
to the broader plugin system.

## Supporting detail

| Topic | Detailed reading |
| --- | --- |
| Current framework | [Execution](current-system/execution.md), [contracts](current-system/contracts.md), [consumers](current-system/consumers.md), [design history](current-system/design-history.md). |
| Type design | [Alternatives](type-system/alternatives.md), [binding contract](type-system/portable-contract.md), [type inventory](type-system/dtype-inventory.md), [host mappings](type-system/mappings.md). |
| Extraction | [Dependencies](current-system/dependencies.md), [crate and trait choices](current-system/engine-boundary.md). |
| Host adapters | [Arrow, DataFusion, and DuckDB](integrations/README.md), [storage and ownership](integrations/storage-and-ownership.md). |
| Execution semantics | [Worked cases](definedness/worked-cases.md), [API alternatives](definedness/design-options.md), [current Vortex behavior](definedness/current-vortex.md). |
| Measurements | [ARM results](performance/local-measurements.md), [x86 results](performance/x86-measurements.md), [optimization candidates](performance/optimization-candidates.md). |
| Prior art | [Comparison and lessons](prior-art.md), including Velox, Arrow, DuckDB, DataFusion, ClickHouse, Polars, Presto, Spark, and Substrait. |
