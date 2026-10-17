<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow scalar family measurements

This directory records complete `rofl-arrow` invocations beside matched Arrow 59.3.0 kernels.
The benchmark source is `rofl-examples/benches/arrow_families.rs`. Each fixture checks its
results before Divan times it. The benchmark covers 0, 64, 1,024, and 16,384 logical rows.

Build the benchmark with the repository's `bench` profile. That profile uses 16 code generation
units and disables LTO. Supply the resulting executable to the runner:

```sh
cargo rustc --locked --profile bench -p rofl-examples --bench arrow_families --message-format=json
python3 research/row-fn-engine/measurements/2026-09-29-families/run.py --binary /path/to/arrow_families-executable
python3 research/row-fn-engine/measurements/2026-09-29-families/summarize.py
```

Pass `--output-dir research/row-fn-engine/measurements/2026-09-29-families/post-cache` to both
scripts to keep a second run beside the first. The runner creates that child directory and refuses
to overwrite an earlier result. The summarizer reads and writes results in the same directory.

The runner accepts an already-built binary. It does not rebuild it or prove its build profile.
Keep the build command and its output with the results when that distinction matters. The runner
records the binary hash, source revision, dirty worktree state, lockfile hash, compiler version,
system details, exact commands, exit codes, and elapsed process times in `runs.json`.

The runner first calls Divan with `--test`. It then makes three serial timing runs. Each run uses
the OS timer, 1,000 requested samples, a 0.03-second minimum, and a 0.1-second maximum per case.
The second run reverses case order. No row-count filter is passed, so every configured size runs.
Raw Divan output stays in `preflight.txt` and `run-1.txt` through `run-3.txt`. The runner refuses
to overwrite those files. Preserve a completed run before starting another measurement.

The summarizer requires the same native and RowFn cases at all four sizes in all three runs. It
writes `summary.csv` and a 16,384-row table in `summary-16384.md`. Each time is the median of three
process medians, in microseconds per invocation. `rofl_native_ratio` divides those two medians.
`median_paired_ratio` is the median of the three within-run ratios. A ratio above 1 means RowFn
was slower in this workload. Compare the absolute times and all three runs before interpreting a
large ratio for a very short operation.

These timings compare complete invocation cost with the public Arrow kernel call. They do not
isolate the cost of dispatch, decoding, collection, or allocation. They are local measurements,
not a claim about other CPUs or compiler settings.
