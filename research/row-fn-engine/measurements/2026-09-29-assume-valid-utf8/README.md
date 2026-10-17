<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Assume valid UTF-8 views

Skipping the Vortex RowFn decoder's validation pass substantially improves these string benchmarks.
On x86, extracted byte length falls from 158.6 to 4.179 microseconds per 16,384 rows.
Arrow's native kernel takes 3.682 microseconds in the candidate binary. Other string operations
improve by 2.1 to 4.4 times, but remain slower than Arrow.

The experiment used `ct/rofl-assume-valid-utf8`, based on
`24a96cece436409dc4f60f94c6b846d6af017804` plus the uncommitted extraction work.
The [patch](experiment.patch) removes `validate_and_fix` from the existing `Utf8Column` decoder.
Both the retained Vortex RowFn executor and the extracted adapter use that decoder.

This skips UTF-8 validation, view bounds and prefix checks, validity traversal, and null-view sanitation.
Every view is assumed readable and valid, including views at null slots. The benchmark builders
satisfy this assumption. Safe Vortex construction paths do not enforce it for arbitrary inputs,
so this branch can cause undefined behavior with other inputs. The result measures removal of the
whole validation pass, not just its UTF-8 checker.

## EC2 results

Times are microseconds per 16,384-row invocation. Each cell is the median of three process medians.
The Arrow columns come from the candidate binary. Both variants also measured both Arrow controls.

| Function | Vortex rofl before | Vortex rofl after | rofl-arrow | Arrow native |
| --- | ---: | ---: | ---: | ---: |
| Wrapping negate | 3.541 | 3.515 | 3.738 | 3.329 |
| Wrapping multiply | 4.931 | 5.034 | 5.701 | 4.610 |
| Byte length, view | 158.600 | 4.179 | 4.589 | 3.682 |
| Short view equality | 230.100 | 52.200 | 15.080 | 14.170 |
| View less than scalar | 272.000 | 118.200 | 87.780 | 33.480 |
| Literal LIKE scalar | 333.300 | 157.300 | 115.200 | 30.310 |
| Starts with scalar | 223.200 | 69.410 | 38.980 | 30.060 |

The isolated byte-length input decode falls from 151.1 microseconds to 158.5 nanoseconds.
The retained RowFn byte-length implementation falls from 195.2 to 34.17 microseconds.
Its callback requests `as_str().len()`, while the portable function uses the view's byte length.
These controls are not benchmarks of Vortex's production byte-length kernel.

Unchanged Arrow string controls move by up to about 10% between the two binaries at this row count.
That variation limits small comparisons. It does not account for the large Vortex string gains.
No compiler-output investigation ran, so the remaining differences are not attributed to specific
vectorization or inlining decisions.

## Apple results

The local M4 Max run uses the same fixtures and aggregation.

| Function | Vortex rofl before | Vortex rofl after | rofl-arrow | Arrow native |
| --- | ---: | ---: | ---: | ---: |
| Wrapping negate | 2.979 | 2.958 | 2.187 | 1.614 |
| Wrapping multiply | 5.749 | 5.749 | 4.957 | 3.645 |
| Byte length, view | 88.290 | 3.624 | 3.374 | 2.583 |
| Short view equality | 142.200 | 45.830 | 13.950 | 6.166 |
| View less than scalar | 178.200 | 89.910 | 60.330 | 18.540 |
| Literal LIKE scalar | 205.000 | 115.700 | 87.490 | 16.830 |
| Starts with scalar | 136.900 | 47.990 | 21.290 | 17.080 |

The retained RowFn byte-length implementation falls from 112.4 to 23.49 microseconds.
The isolated input decode falls from 89.04 microseconds to 73.96 nanoseconds.

## Method and artifacts

The unchanged [host comparison benchmark](../../../../rowfn-examples/benches/host_comparison.rs)
covers seven functions, an existing-executor control, and five decode controls at three row counts.
The row counts are 64, 1,024, and 16,384. Input construction, output planning, and Vortex context
creation stay outside the timed calls. String fixtures contain nulls and sliced arrays.
Each fixture compares both portable implementations against Arrow's native result before timing.

Both machines use Rust 1.98.0, Arrow 59.3.0, 16 code generation units, and no LTO.
The EC2 instance is a `c7i.4xlarge` VM with an Intel Xeon Platinum 8488C.
Both EC2 builds use `RUSTFLAGS='-C target-cpu=native'`. The Apple builds use the repository's
default target flags. All builds finish before timing starts on their machine.
EC2 timing runs on CPU 2, with its SMT sibling, CPU 10, offline.

Each machine builds the baseline, saves its executable, then builds the candidate and saves it.
Six serial processes run in this order: before, after, after, before, before, after.
Each executable receives `--bench --min-time 0.1 --max-time 0.3`.
Both executables also run with `--test` before measurement. These preflight runs passed on both hosts.

[summary.csv](summary.csv) contains every case, the three individual process medians, and their
aggregate medians. Raw files use `apple-before-1.txt`, `apple-after-1.txt`, and corresponding
`ec2-` names, with run numbers 1 through 3. The `*-preflight.txt` files retain preflight output.
[environment.json](environment.json) records the local environment and source archive hash.
[environment.txt](environment.txt) records the EC2 compiler, CPU, and source and executable hashes.
[apple-sha256.json](apple-sha256.json) records the local decoder and executable hashes.
[SHA256SUMS](SHA256SUMS) contains hashes checked against every downloaded EC2 text artifact.
Final adapter comment updates and documentation were added after timing. Executable code was unchanged.

The source diff received review, and `git diff --check` passed. No formatter, linter, sanitizer,
Miri run, or full test suite ran. Existing malformed-input and null-sanitation tests describe the
validated implementation and are incompatible with this experimental assumption.

The EC2 instance `i-026864dd922fdece5` reached `terminated` after the results were downloaded.
Its temporary security group and SSH key pair were deleted and their absence was checked.
