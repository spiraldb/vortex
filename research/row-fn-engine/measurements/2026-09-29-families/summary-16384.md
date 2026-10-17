<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow scalar families at 16,384 rows

Times are microseconds per invocation. Each time is the median of three process medians.
A ratio above 1 means the complete RowFn invocation was slower than the Arrow kernel.
The paired ratio is the median of three within-run ratios. See `summary.csv` for all sizes.

| Case | Arrow native | RowFn | Ratio | Paired ratio |
| --- | ---: | ---: | ---: | ---: |
| LikeComplexArray | 317.300 | 305000.000 | 961.235 | 958.693 |
| RegexpArray | 699.900 | 359700.000 | 513.931 | 513.931 |
| RegexpAlternatingArray | 731.300 | 353800.000 | 483.796 | 481.754 |
| LikeLiteralScalar | 17.120 | 72.290 | 4.223 | 4.232 |
| BitwiseNot | 1.499 | 3.208 | 2.140 | 2.140 |
| WrappingNegate | 1.520 | 3.208 | 2.111 | 2.111 |
| FloatNegate | 1.614 | 3.208 | 1.988 | 2.000 |
| NotLikeScalar | 37.540 | 66.330 | 1.767 | 1.811 |
| WrappingMultiply | 3.645 | 5.208 | 1.429 | 1.429 |
| LikeAlternatingArray | 108700.000 | 147200.000 | 1.354 | 1.358 |
| BitwiseOr | 3.395 | 4.457 | 1.313 | 1.313 |
| FloatDivide | 3.416 | 4.457 | 1.305 | 1.293 |
| FloatSubtract | 3.437 | 4.457 | 1.297 | 1.297 |
| BitwiseXor | 3.458 | 4.457 | 1.289 | 1.301 |
| FloatAdd | 3.478 | 4.457 | 1.281 | 1.282 |
| FloatMultiply | 3.478 | 4.457 | 1.281 | 1.281 |
| WrappingSub | 3.499 | 4.457 | 1.274 | 1.259 |
| BitwiseAnd | 3.520 | 4.457 | 1.266 | 1.266 |
| BitwiseAndNot | 3.499 | 4.416 | 1.262 | 1.262 |
| ShiftLeft | 3.562 | 4.457 | 1.251 | 1.251 |
| ILikeComplexScalar | 392.200 | 488.500 | 1.246 | 1.194 |
| ShiftRight | 3.582 | 4.458 | 1.245 | 1.245 |
| SubstringCharsUtf8 | 94.040 | 115.200 | 1.225 | 1.225 |
| FloatNotEqual | 2.812 | 3.270 | 1.163 | 1.163 |
| LessThan | 2.791 | 3.229 | 1.157 | 1.157 |
| GreaterThan | 2.791 | 3.228 | 1.157 | 1.165 |
| Equal | 2.812 | 3.249 | 1.155 | 1.157 |
| FloatEqual | 2.812 | 3.249 | 1.155 | 1.155 |
| LessThanOrEqual | 2.812 | 3.249 | 1.155 | 1.155 |
| NotEqual | 2.812 | 3.249 | 1.155 | 1.146 |
| GreaterThanOrEqual | 2.812 | 3.229 | 1.148 | 1.157 |
| RegexpScalar | 231.300 | 258.800 | 1.119 | 1.116 |
| FloatGreaterThanOrEqual | 4.832 | 5.165 | 1.069 | 1.069 |
| FloatGreaterThan | 4.832 | 5.124 | 1.060 | 1.060 |
| FloatLessThanOrEqual | 4.832 | 5.124 | 1.060 | 1.060 |
| FloatRemainder | 77.060 | 79.330 | 1.029 | 1.031 |
| FloatLessThan | 4.832 | 4.666 | 0.966 | 0.966 |
| SubstringUtf8 | 97.160 | 92.740 | 0.955 | 0.954 |
| NotILikeScalar | 185.500 | 139.800 | 0.754 | 0.750 |
| CheckedSub | 9.791 | 4.290 | 0.438 | 0.436 |
| CheckedSubNullable | 10.080 | 4.249 | 0.422 | 0.422 |
| Remainder | 21.450 | 9.041 | 0.421 | 0.421 |
| CheckedNegate | 15.660 | 3.228 | 0.206 | 0.206 |
