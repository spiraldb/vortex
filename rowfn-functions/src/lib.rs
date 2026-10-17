// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Portable strict row functions for the experimental `rowfn` framework.
//!
//! Function definitions share their semantic dispatch and row operations across hosts. The default
//! package has no host dependencies. Optional `arrow` and `vortex` features provide native mappings
//! without changing the function definitions or registering them in a host catalog.

#![deny(missing_docs)]

mod text_dispatch;

mod domains;
pub use domains::ComparisonHost;
pub use domains::ComparisonKind;
pub use domains::FloatWidth;
pub use domains::Integer;
pub use domains::IntegerHost;
pub use domains::ListHost;
pub use domains::NoOptions;
pub use domains::TextComparisonHost;
pub use domains::TimestampHost;
pub use domains::TimestampTicks;

mod functions;
pub use functions::Add;
pub use functions::AdjustTicks;
pub use functions::Divide;
pub use functions::Multiply;
pub use functions::Not;
pub use functions::PositiveSum;
pub use functions::Scale;
pub use functions::Seven;
pub use functions::Trim;

mod integer_kernels;
pub use integer_kernels::BitwiseAnd;
pub use integer_kernels::BitwiseAndNot;
pub use integer_kernels::BitwiseNot;
pub use integer_kernels::BitwiseOr;
pub use integer_kernels::BitwiseXor;
pub use integer_kernels::MultiplyWrapping;
pub use integer_kernels::Negate;
pub use integer_kernels::Remainder;
pub use integer_kernels::ShiftLeft;
pub use integer_kernels::ShiftRight;
pub use integer_kernels::Subtract;

mod float_kernels;
pub use float_kernels::FloatArithmetic;
pub use float_kernels::FloatNegate;

mod comparisons;
pub use comparisons::Equal;
pub use comparisons::GreaterThan;
pub use comparisons::GreaterThanOrEqual;
pub use comparisons::LessThan;
pub use comparisons::LessThanOrEqual;
pub use comparisons::NotEqual;
pub use comparisons::StringEqual;
pub use comparisons::StringGreaterThan;
pub use comparisons::StringGreaterThanOrEqual;
pub use comparisons::StringLessThan;
pub use comparisons::StringLessThanOrEqual;
pub use comparisons::StringNotEqual;

mod like;
pub use like::Like;
pub use like::RegexpIsMatch;

mod substring;
pub use substring::SubstringBytes;
pub use substring::SubstringChars;

mod text;
pub use text::Concat;
pub use text::StringPredicate;

mod text_length;
pub use text_length::BitLength;
pub use text_length::ByteLength;
pub use text_length::TextLengthHost;

#[cfg(any(feature = "arrow", feature = "vortex"))]
mod bindings;
