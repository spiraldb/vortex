<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Final scalar-family comparison

This run compares complete `rofl-arrow` invocations with matched Arrow 59.3.0 kernels. It uses the
bench profile with 16 code generation units and no LTO. The host was an Apple M4 Max running
`rustc 1.98.0` for `aarch64-apple-darwin`. Each fixture checked its result before timing. The
runner measured 62 cases at 0, 64, 1,024, and 16,384 rows in three serial process runs.

At 16,384 rows, numeric comparisons took about 1.14 to 1.16 times Arrow's time. Checked integer
subtraction and negation were faster than the matched Arrow kernels. Byte length on Utf8 took 8.0
times Arrow's time because Arrow reads offsets without visiting string values. Short Utf8View
equality took 5.68 times Arrow's time, which leaves a clear string-binding gap. Scalar LIKE took
about 5 times Arrow's time. Array regex with repeated patterns took 0.91 to 0.94 times Arrow's time.

`RegexpIsMatch` and Arrow cache distinct compiled patterns within an invocation. The example LIKE
function also caches distinct valid patterns. Arrow's array LIKE kernel only reuses the previous
pattern. The alternating LIKE case therefore compares different cache policies: Arrow took 97.13 ms,
and RowFn took 0.467 ms. That ratio is evidence about pattern reuse, not framework overhead. Scalar
LIKE shows the opposite limit, where pattern reuse alone does not close the gap.

The [16,384-row table](summary-16384.md) and [all measurements](summary.csv) contain process medians
and paired ratios. [Run metadata](runs.json) records the binary hash, compiler, system, command,
preflight result, and raw output paths. The [parent report](../README.md) explains the runner and
the earlier experiments. `build.txt` records the build command. `source.patch` and
`untracked-source.tar.gz` preserve the measured source. These results are local to this workload
and target. They do not establish cross-platform performance or safety.
