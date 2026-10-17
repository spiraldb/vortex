<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Shared functions on Arrow and Vortex

The [host comparison benchmark](../../../../rowfn-examples/benches/host_comparison.rs)
calls the same portable function definitions through Arrow and Vortex. It also calls
the corresponding Arrow kernels. These are full invocations, with input construction
and output planning outside the timed region. The Vortex execution context is also
prepared outside that region. Each fixture checks both RowFn outputs against Arrow's
kernel before timing.

The inputs have the same logical values. Array operands are sliced by three rows.
String inputs have nulls, and scalar operands are explicit. Arrow uses `Utf8View`
and Vortex uses `VarBinView` for these cases. Their storage and validation contracts
differ, so the three times do not isolate the framework's cost.

Both runs used Rust 1.98.0, Arrow 59.3.0, 16 code generation units, no LTO, and three
serial processes. Each value below is the median of three process medians in
microseconds per 16,384-row invocation. The raw [Apple runs](rofl-host-threeway-1.txt)
and [EC2 runs](rofl-host-run-1.txt) include 64 and 1,024 rows. Files ending in
`-2.txt` and `-3.txt` contain the other process runs.

| Case | Apple Arrow | Apple rofl-arrow | Apple Vortex rofl | EC2 Arrow | EC2 rofl-arrow | EC2 Vortex rofl |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Wrapping negate | 1.520 | 2.290 | 2.999 | 2.780 | 3.459 | 3.327 |
| Wrapping multiply | 3.770 | 5.040 | 5.915 | 3.509 | 4.448 | 3.904 |
| Byte length, view | 2.603 | 3.499 | 90.790 | 3.254 | 4.052 | 143.000 |
| Short view equality | 6.291 | 13.990 | 140.900 | 13.100 | 13.240 | 207.200 |
| View less than scalar | 15.700 | 62.660 | 179.200 | 28.810 | 74.590 | 249.500 |
| Literal LIKE scalar | 17.700 | 86.990 | 205.500 | 29.260 | 115.600 | 306.400 |
| Starts with scalar | 18.160 | 21.660 | 134.400 | 29.070 | 38.950 | 204.300 |

The Apple run used an M4 Max and the default target. The EC2 run used a dedicated
`c7i.4xlarge` with an Intel Xeon Platinum 8488C. It is a virtual instance, not bare
metal. All EC2 crates used `-C target-cpu=native`. Timing ran on CPU 2 with its SMT
sibling offline. No compilation ran during timing. Results from these two machines
must be compared as ratios within each binary, not as absolute cross-machine times.

The Vortex string gap begins before the row callback. Its existing `Utf8Column`
decoder validates every valid view and sanitizes null views on each invocation.
The Arrow binding borrows an already validated `StringViewArray`. A decode-only
benchmark of the first Vortex string input took 86.12 microseconds on Apple and
139.3 microseconds on EC2 for byte length. The full Vortex invocations took 90.79
and 143.0 microseconds. Short equality decodes two arrays. One short-array decode
took 43.7 microseconds on Apple and 78.63 on EC2. These are measured component
costs, not a subtraction proof for all other work. The [decode runs](rofl-host-decode-1.txt)
on Apple and the EC2 host runs retain the other cases.

The retained Vortex RowFn executor uses the same `Utf8Column` decoder. A matched
byte-length implementation took 114.0 microseconds on Apple and 178.2 on EC2.
The extracted Vortex implementation took 90.79 and 143.0 microseconds. The
[legacy Apple runs](rofl-host-legacy-1.txt) and EC2 runs retain those timings.
Thus the large Vortex-to-Arrow string gap is not evidence that extraction added
that cost. Vortex must keep validation until its input type or decoder supplies
an equally sound proof that the views are valid.

The two hosts do share dispatch and row operations, but they compile different
monomorphizations. For example, Arrow's text binding supplies an inline view key.
The Vortex binding currently does not. The x86 short equality result is near
Arrow's kernel through `rofl-arrow`, while the Apple result is about 2.2 times
Arrow's time. Neither result establishes a portable performance guarantee.

The [boundary runs](rofl-boundaries-run-1.txt) give two additional Vortex controls
on EC2. At 16,384 rows, retained Vortex addition took 3.299 microseconds and
extracted Vortex addition took 4.035, about 22% slower. A nullable Boolean
predicate without a failure took 6.992 versus 7.769 microseconds. With overflow
only in null payloads, the retained executor took 20.76 microseconds and the
extracted executor took 35.92, about 73% slower. That retry result is a real
unresolved executor regression. The Arrow adapter took 38.01 microseconds in the
same retry fixture. The benchmark's hand-written Arrow baseline is not a native
Arrow scalar function, so it should not be used as an Arrow catalog comparison.

The source snapshot was based on commit `24a96cece436409dc4f60f94c6b846d6af017804`
with uncommitted changes. The transferred source archive had SHA-256
`3a1b3bc71fca22f0c284f610180bf68b5fbf5513693ee62d160bf1df95284a2b`.
The benchmark source had SHA-256
`0c0bce6f08216ab3222fe7d3b3915724bdd91a8ed3e6bdfea5528d4f27ae781b`.
The downloaded EC2 logs matched their instance copies by SHA-256. The EC2
instance, its SSH key pair, and its security group were removed after the logs
were verified.

A [follow-up experiment](../2026-09-29-assume-valid-utf8/README.md) skips the Vortex decoder's
validation pass under an unsafe input assumption. It includes fresh before/after runs on both hosts.
