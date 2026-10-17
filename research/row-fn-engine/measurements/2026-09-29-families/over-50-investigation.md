<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Investigation of Arrow cases more than 50% slower

The [original family run](final/summary-16384.md) found several complete RowFn invocations
that took more than 1.5 times the matched Arrow kernel's time. This investigation used
the same 16,384-row fixtures on an Apple M4 Max. The benchmark profile had 16 code
generation units and no LTO. Rust 1.98 generated the code. The worktree contained
uncommitted changes, so these measurements describe this experiment, not a release.

The following cases had three focused, serial process runs after the changes. Times are
medians of process medians in microseconds per invocation. The ratio is the median of
the three paired ratios. The original ratio is from the earlier, wider family run.
The [per-run medians](over-50-runs.csv) retain the measurements behind this table.

| Case | Original ratio | Current Arrow | Current RowFn | Current ratio |
| --- | ---: | ---: | ---: | ---: |
| WrappingNegate | 1.98 | 1.62 | 2.33 | 1.44 |
| WrappingMultiply | 1.64 | 3.71 | 4.98 | 1.34 |
| ByteLengthUtf8 | 8.00 | 0.78 | 2.10 | 2.67 |
| StringEqualViewShort | 5.68 | 6.08 | 13.74 | 2.26 |
| StringEqualUtf8 | 3.12 | 17.74 | 35.33 | 1.97 |
| StringLessThanViewScalar | 3.03 | 15.54 | 60.39 | 3.88 |
| LikeLiteralScalar | 5.02 | 23.22 | 83.29 | 3.56 |

The numeric owned-output collector had a concrete code generation problem. Its 64-row
chunks kept the arithmetic loop scalar. The Arrow primitive binding now selects a
straight output loop for compatible sources. The generated 64-bit negate loop contains
two-lane ARM vector negate instructions. The 64-bit multiply loop remains scalar on
this target, which has no 64-bit integer vector multiply instruction. Both cases became
faster. The remaining difference includes complete invocation costs and is not a
measured lower bound for the framework.

Array-only traversal now uses a source without the scalar-versus-array choice inside
each row. This improved string length and string equality. The short Utf8View equality
path also compares validated inline headers directly. It uses the actual Arrow view
representation, including its zero padding. It still checks the inline length per row.
Arrow selects a whole-array inline path before its row loop. The remaining gap does not
show that a RowFn callback must be slow. It shows that this binding does not yet select
the same specialized loop.

Byte length on Utf8 remains 2.67 times slower. Arrow subtracts adjacent offsets and
retains the input validity bitmap. The RowFn string binding constructs a `&str` for
each row and checks which Arrow string layout it owns. LargeUtf8 showed the same
pattern in the broader follow-up run, at about 2.24 times Arrow's time. A dedicated
offset input binding could keep this as row-wise work without creating strings.

The scalar Utf8View ordering case is worse after the array-only Boolean source change.
Its RowFn time rose from about 55.6 to 60.4 microseconds, while Arrow's time in these
runs fell from about 18.4 to 15.5 microseconds. Arrow hoists the scalar string outside
the bit-collection loop. The generic RowFn source still obtains it during row access.
Source inspection supports this missing scalar specialization, but the measurements do
not separate that cost from other binding and packing costs. An alternative Boolean
packing loop was slower and was removed.

Literal LIKE has a more direct missing constant optimization. Preparation compiles its
scalar pattern once, but each row still looks up the matcher and selects its matcher
variant. Arrow selects its predicate variant outside the row loop. A control benchmark
used the same literal-prefix predicate through the shared RowFn `StartsWith` function.
In one process run, Arrow `starts_with` took 22.49 microseconds and RowFn took 21.33.
In that run, Arrow LIKE took 22.54 microseconds and RowFn LIKE took 82.81. This points
to the portable LIKE implementation and its per-row matcher selection, not a necessary
cost of the RowFn framework. The control has one run and needs repetition before it
can establish a stable ratio.

Other large ratios need separate treatment. Arrow Boolean NOT transforms a whole
bitmap and is not a useful RowFn target. A zero-width fixed-size list has almost no
row work, so its ratio largely measures setup. List scaling also compares Arrow's
flat child loop with a per-row list sink. The earlier scalar regexp ratio of 2.04 did
not reproduce in a later focused run, where it was about 1.29. No regexp optimization
was made in this investigation. String concatenation still has distinct output-sink
costs. These cases should not be described as one vectorization defect.

The other family cases above 1.5 times Arrow's time share these code paths.
FloatNegate and BitwiseNot use the numeric owned-output collector. BitLength uses
the string-length binding. Other view comparisons use the same inline-key or
borrowed-byte operations as the equality and ordering cases above. NotLikeScalar
uses the LIKE matcher path. Array-pattern LIKE also pays for matcher lookup, but
the relative cost varies with pattern complexity. These are source-level groupings,
not fresh measurements of every case after the changes.

The benchmark fixtures check output equality before timing. Focused benchmarks and
target-matched LLVM IR and assembly inspection ran. One Vortex addition control still
showed about a 1.17 times extracted-to-existing ratio. The deferred Boolean retry path
was not remeasured. No full test suite, formatter, linter, x86 benchmark, or safety
tool ran. The current results do not prove cross-platform safety or performance.
