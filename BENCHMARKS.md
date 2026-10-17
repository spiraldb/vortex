<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Benchmark measurements

The repository retains the reports, raw timings, summaries, commands, compiler metadata, and source
snapshots from the RowFn experiments. Historical files use the earlier `rofl` name.

**These measurements describe their recorded source.** They do not benchmark the later package
split, concrete text dispatch, rename, or standalone export. No fresh timings were collected for
this index.

## Start with the reports

| Report | Answers |
| --- | --- |
| [Full Arrow and Vortex comparison](research/row-fn-engine/compute-options.md) | Which cases were faster or slower, and how much optimization remains? |
| [Arrow investigation leads](research/row-fn-engine/arrow-performance-opportunities.md) | Which measured differences suggest native-kernel improvements? |
| [ARM/x86 host comparison](research/row-fn-engine/measurements/2026-09-29-host-comparison/README.md) | How do selected shared functions behave on Arrow and Vortex across targets? |

The main Arrow refresh used Arrow 59.3.0, an Apple M4 Max, Rust 1.98.0, LLVM 22.1.8, 16 CGUs, and
no LTO. Results are medians of three process medians. Input construction and output planning are
outside timing. Result allocation and publication are inside it.

The selected x86 runs used an Intel Xeon Platinum 8488C on EC2. Compare ratios within each target,
rather than absolute ARM and x86 times. The reports distinguish native kernels from composed
references and comparisons with different output storage.

## Trace a result to its source

The September 30 refresh is the main Arrow inventory:

| Artifact | Contains |
| --- | --- |
| [comparisons.csv](research/row-fn-engine/measurements/2026-09-30-compute-options/comparisons.csv) | Labelled cases, baseline kind, target, absolute times, ratios, and source dataset. |
| [summary.csv](research/row-fn-engine/measurements/2026-09-30-compute-options/summary.csv) | Per-process medians for the benchmark cases. |
| [Timing directory](research/row-fn-engine/measurements/2026-09-30-compute-options) | Raw `*-1.txt`, `*-2.txt`, `*-3.txt` timings and fixture preflights. |
| [runs.json](research/row-fn-engine/measurements/2026-09-30-compute-options/runs.json) | Commands, run order, elapsed time, output filenames, and exit codes. |
| [build.json](research/row-fn-engine/measurements/2026-09-30-compute-options/build.json) | Compiler, environment, base commit, build command, and executable hashes. |
| [source.patch](research/row-fn-engine/measurements/2026-09-30-compute-options/source.patch) | Tracked changes over the recorded Vortex base. |
| [untracked-source.tar.gz](research/row-fn-engine/measurements/2026-09-30-compute-options/untracked-source.tar.gz) | Untracked source needed to describe the measured checkout. |

A base commit alone is insufficient because the experiments included uncommitted code. The patch
and source archive preserve that distinction. Executable hashes are recorded, but the executables
and old machine paths are not a portable benchmark environment.

## Other runs and qualifications

<details>
<summary>Historical measurements and supporting evidence</summary>

| Record | Scope |
| --- | --- |
| [Initial extraction](research/row-fn-engine/measurements/2026-09-25-rofl/README.md) | Correctness checks, execution boundaries, and first performance results. |
| [Additional scalar kernels](research/row-fn-engine/measurements/2026-09-25-rofl/scalar-kernels.md) | Multiplication, string predicates, and concatenation. |
| [Diagnosis](research/row-fn-engine/measurements/2026-09-29-diagnosis/README.md) | Kernel algorithms, string bindings, and list traversal. |
| [Optimization](research/row-fn-engine/measurements/2026-09-29-optimization/README.md) | Before/after comparisons with their source and compiler evidence. |
| [Scalar families](research/row-fn-engine/measurements/2026-09-29-families/final/README.md) | Additional arithmetic, comparisons, patterns, substrings, and lengths. |
| [Unsafe UTF-8 experiment](research/row-fn-engine/measurements/2026-09-29-assume-valid-utf8/README.md) | A separate Vortex experiment that skips validation and null sanitation. |
| [Selected Arrow wins](research/row-fn-engine/measurements/2026-10-02-arrow-opportunities/winning-cases.json) | The selected cases and paired ratios used by the investigation report. |

</details>

The unsafe Vortex experiment does not establish a safe optimization and is separate from the Arrow
execution paths. Arrow wins in these fixtures do not establish an improvement on current Arrow
mainline or the speedup of a future Arrow patch. Other cases regress.

The published non-Markdown measurement artifacts were compared byte for byte with the retained
local originals on October 6, 2026. All 247 matched. Reports retain their original aggregation and
source qualifications.

For a new comparison, use the [comparison guide](rowfn/COMPARING.md), preserve the exact compiled
source and configuration, and record both absolute times and ratios. Benchmark execution remains
opt-in.
