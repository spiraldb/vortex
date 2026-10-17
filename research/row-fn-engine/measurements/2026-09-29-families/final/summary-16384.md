<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow scalar families at 16,384 rows

Times are microseconds per invocation. Each time is the median of three process medians.
A ratio above 1 means the complete RowFn invocation was slower than the Arrow kernel.
The paired ratio is the median of three within-run ratios. See `summary.csv` for all sizes.

| Case | Arrow native | RowFn | Ratio | Paired ratio |
| --- | ---: | ---: | ---: | ---: |
| ByteLengthUtf8 | 0.755 | 6.041 | 8.001 | 8.001 |
| BitLengthUtf8 | 0.989 | 7.582 | 7.665 | 7.623 |
| StringEqualViewShort | 6.124 | 34.790 | 5.681 | 5.681 |
| LikeLiteralScalarDense | 16.160 | 82.450 | 5.102 | 5.097 |
| LikeLiteralScalar | 16.620 | 83.410 | 5.019 | 5.062 |
| ByteLengthLargeUtf8 | 1.562 | 7.249 | 4.641 | 4.641 |
| StringEqualLargeUtf8 | 14.660 | 52.080 | 3.553 | 3.499 |
| StringEqualUtf8 | 17.040 | 53.240 | 3.124 | 3.124 |
| StringLessThanViewScalar | 18.370 | 55.600 | 3.027 | 2.992 |
| StringLessThanView | 22.790 | 58.020 | 2.546 | 2.547 |
| StringGreaterThanOrEqualView | 22.700 | 57.450 | 2.531 | 2.526 |
| StringGreaterThanView | 22.700 | 57.080 | 2.515 | 2.510 |
| StringLessThanOrEqualView | 22.740 | 57.160 | 2.514 | 2.509 |
| NotLikeScalar | 37.950 | 87.120 | 2.296 | 2.291 |
| BitLengthView | 2.791 | 6.124 | 2.194 | 2.164 |
| ByteLengthView | 2.812 | 5.916 | 2.104 | 2.104 |
| RegexpScalar | 202.500 | 414.000 | 2.044 | 2.041 |
| BitwiseNot | 1.593 | 3.208 | 2.014 | 2.014 |
| FloatNegate | 1.614 | 3.208 | 1.988 | 2.000 |
| WrappingNegate | 1.614 | 3.187 | 1.975 | 1.975 |
| BitLengthLargeUtf8 | 4.749 | 9.166 | 1.930 | 1.930 |
| LikeComplexArray | 315.400 | 550.500 | 1.745 | 1.741 |
| StringNotEqualView | 18.910 | 31.080 | 1.644 | 1.641 |
| WrappingMultiply | 3.687 | 6.040 | 1.638 | 1.638 |
| StringEqualViewLong | 24.450 | 39.830 | 1.629 | 1.634 |
| ILikeComplexScalar | 388.800 | 583.300 | 1.500 | 1.226 |
| LikeLiteralViaPredicateDense | 16.160 | 21.200 | 1.312 | 1.309 |
| FloatSubtract | 3.437 | 4.457 | 1.297 | 1.293 |
| FloatAdd | 3.458 | 4.458 | 1.289 | 1.289 |
| FloatDivide | 3.458 | 4.457 | 1.289 | 1.289 |
| LikeLiteralViaPredicate | 16.620 | 21.240 | 1.278 | 1.278 |
| BitwiseOr | 3.499 | 4.457 | 1.274 | 1.274 |
| BitwiseAndNot | 3.520 | 4.457 | 1.266 | 1.266 |
| BitwiseAnd | 3.499 | 4.416 | 1.262 | 1.262 |
| FloatMultiply | 3.541 | 4.457 | 1.259 | 1.259 |
| BitwiseXor | 3.520 | 4.416 | 1.255 | 1.255 |
| WrappingSub | 3.520 | 4.416 | 1.255 | 1.255 |
| ShiftLeft | 3.562 | 4.457 | 1.251 | 1.251 |
| ShiftRight | 3.604 | 4.457 | 1.237 | 1.230 |
| SubstringCharsUtf8 | 96.200 | 113.500 | 1.180 | 1.180 |
| FloatEqual | 2.791 | 3.229 | 1.157 | 1.157 |
| Equal | 2.791 | 3.228 | 1.157 | 1.157 |
| GreaterThan | 2.791 | 3.228 | 1.157 | 1.157 |
| FloatNotEqual | 2.832 | 3.270 | 1.155 | 1.162 |
| GreaterThanOrEqual | 2.812 | 3.229 | 1.148 | 1.148 |
| LessThan | 2.812 | 3.228 | 1.148 | 1.148 |
| NotEqual | 2.832 | 3.249 | 1.147 | 1.147 |
| LessThanOrEqual | 2.812 | 3.208 | 1.141 | 1.141 |
| FloatLessThanOrEqual | 4.832 | 5.165 | 1.069 | 1.069 |
| FloatGreaterThan | 4.832 | 5.124 | 1.060 | 1.060 |
| FloatGreaterThanOrEqual | 4.832 | 5.124 | 1.060 | 1.069 |
| FloatLessThan | 4.832 | 5.124 | 1.060 | 1.060 |
| FloatRemainder | 78.910 | 80.370 | 1.019 | 1.019 |
| SubstringUtf8 | 94.080 | 92.410 | 0.982 | 0.982 |
| RegexpArray | 679.500 | 640.900 | 0.943 | 0.940 |
| RegexpAlternatingArray | 738.400 | 674.200 | 0.913 | 0.919 |
| NotILikeScalar | 181.900 | 149.600 | 0.822 | 0.824 |
| CheckedSub | 9.791 | 4.249 | 0.434 | 0.434 |
| CheckedSubNullable | 10.160 | 4.249 | 0.418 | 0.418 |
| Remainder | 22.910 | 8.999 | 0.393 | 0.393 |
| CheckedNegate | 15.370 | 3.207 | 0.209 | 0.209 |
| LikeAlternatingArray | 97130.000 | 467.400 | 0.005 | 0.005 |
