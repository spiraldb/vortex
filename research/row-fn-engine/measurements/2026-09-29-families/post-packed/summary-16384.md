<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow scalar families at 16,384 rows

Times are microseconds per invocation. Each time is the median of three process medians.
A ratio above 1 means the complete RowFn invocation was slower than the Arrow kernel.
The paired ratio is the median of three within-run ratios. See `summary.csv` for all sizes.

| Case | Arrow native | RowFn | Ratio | Paired ratio |
| --- | ---: | ---: | ---: | ---: |
| ByteLengthUtf8 | 0.708 | 6.082 | 8.588 | 8.588 |
| BitLengthUtf8 | 0.927 | 6.458 | 6.967 | 6.965 |
| LikeLiteralScalar | 17.080 | 86.240 | 5.049 | 4.988 |
| ByteLengthLargeUtf8 | 1.551 | 7.290 | 4.700 | 4.674 |
| NotLikeScalar | 37.120 | 85.080 | 2.292 | 2.292 |
| BitLengthView | 2.812 | 5.999 | 2.133 | 2.119 |
| ByteLengthView | 2.812 | 5.916 | 2.104 | 2.120 |
| BitwiseNot | 1.593 | 3.187 | 2.001 | 2.001 |
| WrappingNegate | 1.593 | 3.187 | 2.001 | 2.001 |
| FloatNegate | 1.624 | 3.208 | 1.975 | 1.988 |
| BitLengthLargeUtf8 | 4.749 | 7.707 | 1.623 | 1.623 |
| LikeComplexArray | 321.400 | 516.500 | 1.607 | 1.607 |
| WrappingMultiply | 4.290 | 6.040 | 1.408 | 1.412 |
| RegexpScalar | 200.100 | 264.200 | 1.320 | 1.328 |
| FloatSubtract | 3.458 | 4.458 | 1.289 | 1.277 |
| FloatAdd | 3.478 | 4.458 | 1.282 | 1.281 |
| FloatDivide | 3.499 | 4.458 | 1.274 | 1.274 |
| ShiftRight | 3.520 | 4.457 | 1.266 | 1.266 |
| BitwiseXor | 3.541 | 4.457 | 1.259 | 1.259 |
| WrappingSub | 3.541 | 4.457 | 1.259 | 1.259 |
| BitwiseOr | 3.520 | 4.416 | 1.255 | 1.255 |
| FloatMultiply | 3.562 | 4.457 | 1.251 | 1.251 |
| BitwiseAnd | 3.541 | 4.416 | 1.247 | 1.247 |
| BitwiseAndNot | 3.541 | 4.416 | 1.247 | 1.259 |
| ShiftLeft | 3.582 | 4.457 | 1.244 | 1.244 |
| LikeLiteralViaPredicate | 17.490 | 21.330 | 1.220 | 1.220 |
| ILikeComplexScalar | 375.900 | 443.700 | 1.180 | 1.155 |
| LessThanOrEqual | 2.791 | 3.270 | 1.172 | 1.164 |
| GreaterThan | 2.791 | 3.249 | 1.164 | 1.164 |
| GreaterThanOrEqual | 2.791 | 3.249 | 1.164 | 1.164 |
| FloatNotEqual | 2.812 | 3.270 | 1.163 | 1.163 |
| NotEqual | 2.812 | 3.270 | 1.163 | 1.170 |
| SubstringCharsUtf8 | 99.660 | 114.800 | 1.152 | 1.152 |
| Equal | 2.812 | 3.229 | 1.148 | 1.148 |
| FloatEqual | 2.812 | 3.229 | 1.148 | 1.148 |
| LessThan | 2.812 | 3.229 | 1.148 | 1.148 |
| FloatGreaterThanOrEqual | 4.833 | 5.165 | 1.069 | 1.069 |
| SubstringUtf8 | 94.160 | 99.910 | 1.061 | 1.061 |
| FloatGreaterThan | 4.833 | 5.124 | 1.060 | 1.060 |
| FloatLessThan | 4.833 | 5.124 | 1.060 | 1.060 |
| FloatLessThanOrEqual | 4.833 | 5.124 | 1.060 | 1.060 |
| FloatRemainder | 78.040 | 79.040 | 1.013 | 1.022 |
| RegexpAlternatingArray | 735.700 | 631.800 | 0.859 | 0.859 |
| RegexpArray | 683.900 | 564.700 | 0.826 | 0.817 |
| NotILikeScalar | 183.300 | 148.100 | 0.808 | 0.817 |
| CheckedSub | 9.791 | 4.249 | 0.434 | 0.434 |
| CheckedSubNullable | 10.080 | 4.249 | 0.422 | 0.422 |
| Remainder | 23.660 | 8.999 | 0.380 | 0.380 |
| CheckedNegate | 22.410 | 3.228 | 0.144 | 0.144 |
| LikeAlternatingArray | 99570.000 | 464.900 | 0.005 | 0.005 |
