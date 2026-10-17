<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

The [September 29 diagnosis](../2026-09-29-diagnosis/README.md) investigates these regressions and
replaces the portable prefix and suffix comparisons. The measurements below describe the earlier
function bodies preserved in this report's source snapshot.

# Additional Arrow scalar kernels

RowFn is one implementation option for scalar functions. A function package can also register a
whole-batch implementation. Boolean NOT remains a natural packed-buffer operation, and its earlier
result should not set expectations for suitable row functions.

This comparison adds six functions that Arrow 59.3.0 already implements: checked multiplication,
string prefix, suffix, containment, ASCII-insensitive equality, and concatenation. The portable
function definitions also execute on Vortex without changes to either adapter.

## Results

Times are microseconds per complete invocation at 16,384 logical rows. Each entry is the median of
three run medians. A ratio above one means rofl takes more time.

| Function and input shape | Arrow kernel | rofl | rofl / Arrow |
| --- | ---: | ---: | ---: |
| Checked multiply, scalar RHS | 10.740 | 6.958 | 0.65 |
| Checked multiply, array RHS | 10.200 | 6.082 | 0.60 |
| Checked multiply, dense arrays | 9.749 | 5.457 | 0.56 |
| Starts with, scalar pattern | 16.740 | 67.830 | 4.05 |
| Starts with, array patterns | 70.680 | 79.370 | 1.12 |
| Ends with, scalar pattern | 17.580 | 74.870 | 4.26 |
| Ends with, array patterns | 88.200 | 83.040 | 0.94 |
| Contains, scalar pattern | 86.660 | 154.700 | 1.79 |
| Contains, array patterns | 200.900 | 230.100 | 1.15 |
| Contains, dense input and scalar pattern | 100.300 | 162.400 | 1.62 |
| ASCII-insensitive equality, scalar pattern | 27.950 | 67.490 | 2.41 |
| ASCII-insensitive equality, array patterns | 69.080 | 52.990 | 0.77 |
| Concatenate, inline results | 95.080 | 493.600 | 5.19 |
| Concatenate, external results | 122.600 | 726.800 | 5.93 |
| Concatenate, dense external results | 129.900 | 701.100 | 5.40 |
| Starts with, Utf8 input and scalar pattern | 15.790 | 69.410 | 4.40 |
| Contains, Utf8 input and scalar pattern | 86.080 | 154.000 | 1.79 |

String inputs use Utf8View unless the table says Utf8. Cases are nullable unless marked dense.
Array operands have independent null distributions. String arrays are sliced at offset three.
The corpus mixes empty, ASCII, Unicode, short, and long values, with successful and unsuccessful
matches. Short concatenation results fit inline, while external concatenation results exceed 12 bytes.

The constants and per-row patterns are different workloads. Their ratios compare each workload with
its matched implementation, not one input distribution with another.

At 64 rows, rofl takes 0.64 to 0.69 microseconds for multiplication, compared with Arrow's 0.086 to
0.127 microseconds. String predicates take 0.89 to 1.58 microseconds through rofl, compared with
0.115 to 0.995 microseconds through Arrow. Large-batch gains do not remove small-batch invocation cost.

The [CSV](scalar-summary.csv) retains every repetition and its range, including 0 and 1,024 rows.
The complete [commands](extended-runs.json) and raw outputs are retained:
[run one](extended-scalar-1.txt), [run two](extended-scalar-2.txt), and [run three](extended-scalar-3.txt).

## What the comparison includes

The baseline calls Arrow's public `numeric::mul`, `like::{starts_with, ends_with, contains,
eq_ignore_ascii_case}`, and `concat_elements_dyn` kernels. Both concatenation paths return Utf8View
and own their result bytes. No handwritten reference loop substitutes for these kernels.

Both containment paths use `memchr::memmem`. A scalar pattern prepares one searcher per invocation.
The rofl prepared value owns its needle because the current preparation signature cannot return a
borrow of its constant argument. Arrow borrows the scalar pattern. Array patterns use `memmem::find`
on both paths. This allocation difference is included in the complete invocation measurement.

Prefix, suffix, and ASCII equality use Rust string operations in the shared function. Arrow has its
own byte comparisons and Utf8View prefix/suffix iterators. Those specialized paths are a reason to
retain host-native implementations where they help. These results do not isolate dispatch overhead
from differences in row algorithms or storage access.

Concatenation exposes a sink API limitation. `WriteUtf8` accepts one complete string, so the function
allocates and joins a temporary string before copying it into output storage. Arrow's kernel writes
the two parts directly. The measured gap includes that extra allocation and copy, as well as adapter
validation and traversal. A multipart writer is a possible follow-up, not implemented by this run.

Multiplication uses deferred overflow evidence in rofl and Arrow's checked multiplication kernel.
Separate tests verify that valid-row overflow fails and overflow only in null payloads is suppressed.
The timed multiplication data does not overflow. This benchmark therefore measures the accepted
path, not retry cost.

## Verification and method

Seven new cross-host tests passed in both [debug](scalar-tests.log) and
[optimized](scalar-optimized-tests.log) profiles. They cover the four
predicates, changed scalar patterns, null patterns, string ownership, independent nulls, and overflow.
All 136 benchmark cases compared their Arrow and rofl results before timing
([preflight log](scalar-preflight.log)). No comparison failed.
The initial preflight found a fixture bug: the native wrapper constructed `Scalar` even for an array
operand. This was fixed before the three timed runs.

The compiler and profile match the [first run](README.md#measurement-method): Rust 1.98.0, Apple M4
Max, 16 CGUs, no LTO, and the repository's target flags. Runs were serial, with reversed case order
in the middle repetition. Each case used the OS timer with adaptive sample sizes and a 0.1-second
minimum. No compilation ran during timing. Results describe this corpus on this ARM machine.

No formatter, linter, Miri, sanitizer, x86 run, or additional compiler-output experiment ran for these
new functions. The earlier generated-code findings apply to the earlier retry comparison only.

## Reproduce

```sh
cargo nextest run --locked -p rofl-examples --test scalar_kernels
cargo nextest run --locked --cargo-profile bench -p rofl-examples --test scalar_kernels
cargo bench --locked -p rofl-examples --bench arrow_scalar --no-run
```

Pass the executable printed by Cargo to the runner:

```sh
python3 research/row-fn-engine/measurements/2026-09-25-rofl/run.py \
  --scalar target/release/deps/arrow_scalar-604cd22b3782d6c3 --prefix extended
python3 research/row-fn-engine/measurements/2026-09-25-rofl/summarize.py \
  --suite scalar --prefix extended --output scalar-summary.csv
```

The executable hash and source hashes are in [scalar-provenance.json](scalar-provenance.json).
The [source supplement](scalar-source.tar.gz) overlays the first run's source snapshot. No dependency
version changed. The example package added direct edges to the already locked `memchr` and
`arrow-string` packages.
