<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Arrow scalar families at 16,384 rows

Times are microseconds per invocation. Each time is the median of three process medians.
A ratio above 1 means the complete RowFn invocation was slower than the Arrow kernel.
The paired ratio is the median of three within-run ratios. See `summary.csv` for all sizes.

| Case | Arrow native | RowFn | Ratio | Paired ratio |
| --- | ---: | ---: | ---: | ---: |
| ByteLengthUtf8 | 0.693 | 6.082 | 8.783 | 8.783 |
| BitLengthUtf8 | 0.990 | 6.458 | 6.527 | 6.568 |
| LikeLiteralScalar | 17.700 | 90.200 | 5.096 | 5.281 |
| ByteLengthLargeUtf8 | 1.572 | 7.249 | 4.611 | 4.611 |
| LikeComplexArray | 317.900 | 895.200 | 2.816 | 2.816 |
| NotLikeScalar | 39.370 | 95.160 | 2.417 | 2.376 |
| BitLengthView | 2.832 | 5.999 | 2.118 | 2.118 |
| ByteLengthView | 2.832 | 5.874 | 2.074 | 2.089 |
| BitwiseNot | 1.603 | 3.208 | 2.001 | 1.988 |
| WrappingNegate | 1.604 | 3.208 | 2.000 | 2.000 |
| FloatNegate | 1.645 | 3.208 | 1.950 | 1.937 |
| WrappingMultiply | 3.645 | 6.040 | 1.657 | 1.648 |
| BitLengthLargeUtf8 | 4.707 | 7.707 | 1.637 | 1.637 |
| RegexpScalar | 201.600 | 307.400 | 1.525 | 1.525 |
| RegexpArray | 666.300 | 1011.000 | 1.517 | 1.518 |
| RegexpAlternatingArray | 719.200 | 1047.000 | 1.456 | 1.458 |
| BitwiseAndNot | 3.395 | 4.457 | 1.313 | 1.313 |
| FloatAdd | 3.416 | 4.458 | 1.305 | 1.305 |
| FloatDivide | 3.416 | 4.457 | 1.305 | 1.305 |
| FloatSubtract | 3.520 | 4.458 | 1.266 | 1.278 |
| BitwiseAnd | 3.520 | 4.457 | 1.266 | 1.266 |
| FloatMultiply | 3.541 | 4.458 | 1.259 | 1.271 |
| BitwiseOr | 3.541 | 4.457 | 1.259 | 1.259 |
| BitwiseXor | 3.541 | 4.457 | 1.259 | 1.259 |
| ShiftLeft | 3.582 | 4.499 | 1.256 | 1.256 |
| WrappingSub | 3.562 | 4.457 | 1.251 | 1.251 |
| ILikeComplexScalar | 362.700 | 449.000 | 1.238 | 1.261 |
| ShiftRight | 3.604 | 4.458 | 1.237 | 1.237 |
| LikeLiteralViaPredicate | 17.740 | 21.240 | 1.197 | 1.275 |
| SubstringCharsUtf8 | 98.370 | 117.200 | 1.191 | 1.212 |
| FloatNotEqual | 2.812 | 3.270 | 1.163 | 1.155 |
| NotEqual | 2.812 | 3.270 | 1.163 | 1.163 |
| FloatEqual | 2.812 | 3.249 | 1.155 | 1.155 |
| GreaterThan | 2.812 | 3.249 | 1.155 | 1.155 |
| GreaterThanOrEqual | 2.812 | 3.249 | 1.155 | 1.155 |
| LessThan | 2.812 | 3.249 | 1.155 | 1.148 |
| LessThanOrEqual | 2.812 | 3.249 | 1.155 | 1.155 |
| Equal | 2.812 | 3.228 | 1.148 | 1.155 |
| FloatLessThanOrEqual | 4.833 | 5.166 | 1.069 | 1.069 |
| FloatGreaterThanOrEqual | 4.833 | 5.165 | 1.069 | 1.069 |
| FloatGreaterThan | 4.833 | 5.124 | 1.060 | 1.060 |
| FloatLessThan | 4.833 | 5.124 | 1.060 | 1.060 |
| FloatRemainder | 78.490 | 79.410 | 1.012 | 1.013 |
| SubstringUtf8 | 96.410 | 96.540 | 1.001 | 1.003 |
| NotILikeScalar | 181.800 | 163.100 | 0.897 | 0.896 |
| CheckedSub | 9.791 | 4.249 | 0.434 | 0.434 |
| CheckedSubNullable | 9.999 | 4.249 | 0.425 | 0.425 |
| Remainder | 23.660 | 9.041 | 0.382 | 0.382 |
| CheckedNegate | 18.160 | 3.228 | 0.178 | 0.178 |
| LikeAlternatingArray | 99420.000 | 784.200 | 0.008 | 0.008 |
