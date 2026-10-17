// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Matched Arrow kernels and new strict RowFn families.
//!
//! Each fixture compares logical output before timing. The Arrow baseline excludes planning, as
//! the public Arrow kernels do, while the RowFn measurement includes the complete invocation.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, LargeStringArray, Scalar, StringArray, StringViewArray};
use arrow_schema::{ArrowError, Field};
use divan::{Bencher, black_box};
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{
    BitLength, BitwiseAnd, BitwiseAndNot, BitwiseNot, BitwiseOr, BitwiseXor, ByteLength, Equal, GreaterThan,
    GreaterThanOrEqual, FloatArithmetic, FloatNegate, LessThan, LessThanOrEqual, Like,
    MultiplyWrapping, Negate, NoOptions, NotEqual,
    RegexpIsMatch, Remainder, ShiftLeft, ShiftRight, StringPredicate, SubstringBytes, SubstringChars, Subtract,
    StringEqual, StringGreaterThan, StringGreaterThanOrEqual, StringLessThan,
    StringLessThanOrEqual, StringNotEqual,
};
use vortex_error::VortexExpect;

const SIZES: &[usize] = &[0, 64, 1024, 16_384];

#[derive(Clone, Copy, Debug)]
enum Case {
    CheckedSub,
    CheckedSubNullable,
    WrappingSub,
    WrappingMultiply,
    FloatAdd,
    FloatSubtract,
    FloatMultiply,
    FloatDivide,
    FloatRemainder,
    FloatNegate,
    Remainder,
    CheckedNegate,
    WrappingNegate,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseAndNot,
    ShiftLeft,
    ShiftRight,
    BitwiseNot,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    FloatEqual,
    FloatNotEqual,
    FloatLessThan,
    FloatLessThanOrEqual,
    FloatGreaterThan,
    FloatGreaterThanOrEqual,
    LikeLiteralScalar,
    LikeLiteralScalarDense,
    LikeLiteralViaPredicate,
    LikeLiteralViaPredicateDense,
    LikeComplexArray,
    LikeAlternatingArray,
    ILikeComplexScalar,
    NotLikeScalar,
    NotILikeScalar,
    RegexpScalar,
    RegexpArray,
    RegexpAlternatingArray,
    SubstringUtf8,
    SubstringCharsUtf8,
    ByteLengthUtf8,
    ByteLengthLargeUtf8,
    ByteLengthView,
    BitLengthUtf8,
    BitLengthLargeUtf8,
    BitLengthView,
    StringEqualUtf8,
    StringEqualLargeUtf8,
    StringEqualViewShort,
    StringEqualViewLong,
    StringNotEqualView,
    StringLessThanView,
    StringLessThanOrEqualView,
    StringGreaterThanView,
    StringGreaterThanOrEqualView,
    StringLessThanViewScalar,
}

const CASES: &[Case] = &[
    Case::CheckedSub,
    Case::CheckedSubNullable,
    Case::WrappingSub,
    Case::WrappingMultiply,
    Case::FloatAdd,
    Case::FloatSubtract,
    Case::FloatMultiply,
    Case::FloatDivide,
    Case::FloatRemainder,
    Case::FloatNegate,
    Case::Remainder,
    Case::CheckedNegate,
    Case::WrappingNegate,
    Case::BitwiseAnd,
    Case::BitwiseOr,
    Case::BitwiseXor,
    Case::BitwiseAndNot,
    Case::ShiftLeft,
    Case::ShiftRight,
    Case::BitwiseNot,
    Case::Equal,
    Case::NotEqual,
    Case::LessThan,
    Case::LessThanOrEqual,
    Case::GreaterThan,
    Case::GreaterThanOrEqual,
    Case::FloatEqual,
    Case::FloatNotEqual,
    Case::FloatLessThan,
    Case::FloatLessThanOrEqual,
    Case::FloatGreaterThan,
    Case::FloatGreaterThanOrEqual,
    Case::LikeLiteralScalar,
    Case::LikeLiteralScalarDense,
    Case::LikeLiteralViaPredicate,
    Case::LikeLiteralViaPredicateDense,
    Case::LikeComplexArray,
    Case::LikeAlternatingArray,
    Case::ILikeComplexScalar,
    Case::NotLikeScalar,
    Case::NotILikeScalar,
    Case::RegexpScalar,
    Case::RegexpArray,
    Case::RegexpAlternatingArray,
    Case::SubstringUtf8,
    Case::SubstringCharsUtf8,
    Case::ByteLengthUtf8,
    Case::ByteLengthLargeUtf8,
    Case::ByteLengthView,
    Case::BitLengthUtf8,
    Case::BitLengthLargeUtf8,
    Case::BitLengthView,
    Case::StringEqualUtf8,
    Case::StringEqualLargeUtf8,
    Case::StringEqualViewShort,
    Case::StringEqualViewLong,
    Case::StringNotEqualView,
    Case::StringLessThanView,
    Case::StringLessThanOrEqualView,
    Case::StringGreaterThanView,
    Case::StringGreaterThanOrEqualView,
    Case::StringLessThanViewScalar,
];

impl Case {
    fn is_float(self) -> bool {
        matches!(
            self,
            Self::FloatAdd | Self::FloatSubtract | Self::FloatMultiply | Self::FloatDivide
                | Self::FloatRemainder | Self::FloatNegate | Self::FloatEqual
                | Self::FloatNotEqual | Self::FloatLessThan | Self::FloatLessThanOrEqual
                | Self::FloatGreaterThan | Self::FloatGreaterThanOrEqual
        )
    }

    fn is_string(self) -> bool {
        self.is_string_comparison() || matches!(
            self,
            Self::LikeLiteralScalar | Self::LikeLiteralScalarDense
                | Self::LikeLiteralViaPredicate | Self::LikeLiteralViaPredicateDense
                | Self::LikeComplexArray | Self::LikeAlternatingArray
                | Self::ILikeComplexScalar
                | Self::NotLikeScalar | Self::NotILikeScalar | Self::RegexpScalar | Self::RegexpArray
                | Self::RegexpAlternatingArray | Self::SubstringUtf8 | Self::SubstringCharsUtf8
                | Self::ByteLengthUtf8 | Self::ByteLengthLargeUtf8 | Self::ByteLengthView
                | Self::BitLengthUtf8 | Self::BitLengthLargeUtf8 | Self::BitLengthView
        )
    }

    fn is_string_comparison(self) -> bool {
        matches!(self,
            Self::StringEqualUtf8 | Self::StringEqualLargeUtf8 | Self::StringEqualViewShort
                | Self::StringEqualViewLong | Self::StringNotEqualView
                | Self::StringLessThanView | Self::StringLessThanOrEqualView
                | Self::StringGreaterThanView | Self::StringGreaterThanOrEqualView
                | Self::StringLessThanViewScalar)
    }

    fn unary(self) -> bool {
        matches!(self, Self::CheckedNegate | Self::WrappingNegate | Self::BitwiseNot
            | Self::FloatNegate
            | Self::SubstringUtf8 | Self::SubstringCharsUtf8
            | Self::ByteLengthUtf8 | Self::ByteLengthLargeUtf8 | Self::ByteLengthView
            | Self::BitLengthUtf8 | Self::BitLengthLargeUtf8 | Self::BitLengthView)
    }

    fn scalar(self) -> bool {
        matches!(self, Self::LikeLiteralScalar | Self::LikeLiteralScalarDense
            | Self::LikeLiteralViaPredicate | Self::LikeLiteralViaPredicateDense
            | Self::ILikeComplexScalar | Self::NotLikeScalar | Self::NotILikeScalar
            | Self::RegexpScalar | Self::StringLessThanViewScalar)
    }
}

fn operand(column: ArrayRef, scalar: bool) -> ArrowOperand {
    ArrowOperand {
        dtype: Field::new("input", column.data_type().clone(), column.logical_null_count() != 0),
        column,
        scalar,
    }
}

fn boolean<T: Array + 'static>(array: T) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(array))
}

struct Fixture {
    case: Case,
    inputs: Vec<ArrowOperand>,
    output: Field,
    rows: usize,
}

impl Fixture {
    fn new(case: Case, rows: usize) -> Result<Self, ArrowError> {
        let inputs = if case.is_string() {
            let values = [
                "arrow and Rust",
                "ARROW and rust",
                "a longer arrow payload",
                "other values",
                "λ arrow",
                "",
            ];
            let byte_values = [
                "arrow and Rust",
                "ARROW and rust",
                "a longer arrow payload",
                "other values",
                "omega arrow",
                "",
            ];
            let short_values = ["a", "b", "aa", "ab", "é", ""];
            let common_prefix_values = [
                "common-prefix-alpha", "common-prefix-beta", "common-prefix-gamma",
                "common-prefix-delta", "common-prefix-epsilon", "common-prefix-zeta",
            ];
            let source_values: &[&str] = if matches!(case, Case::StringEqualViewShort) {
                &short_values
            } else if matches!(case, Case::StringEqualViewLong) {
                &common_prefix_values
            } else if matches!(case, Case::SubstringUtf8) {
                &byte_values
            } else {
                &values
            };
            let text: Vec<_> = (0..rows + 3)
                .map(|index| {
                    let value = source_values[index % source_values.len()];
                    (matches!(case, Case::LikeLiteralScalarDense | Case::LikeLiteralViaPredicateDense)
                        || !index.is_multiple_of(9)).then_some(value)
                })
                .collect();
            let column: ArrayRef = if matches!(case, Case::ByteLengthLargeUtf8 | Case::BitLengthLargeUtf8
                | Case::StringEqualLargeUtf8) {
                Arc::new(LargeStringArray::from(text))
            } else if matches!(case, Case::SubstringUtf8 | Case::SubstringCharsUtf8
                | Case::ByteLengthUtf8 | Case::BitLengthUtf8 | Case::StringEqualUtf8) {
                Arc::new(StringArray::from(text))
            } else {
                Arc::new(StringViewArray::from(text))
            };
            let mut inputs = vec![operand(column.slice(3, rows), false)];
            if !case.unary() {
                if case.is_string_comparison() {
                    let right: Vec<_> = if case.scalar() {
                        vec![Some("arrow and Rust")]
                    } else {
                        (0..rows + 3).map(|index| {
                            (!index.is_multiple_of(11)).then_some(
                                source_values[(index + index % 3) % source_values.len()])
                        }).collect()
                    };
                    let column: ArrayRef = if matches!(case, Case::StringEqualLargeUtf8) {
                        Arc::new(LargeStringArray::from(right))
                    } else if matches!(case, Case::StringEqualUtf8) {
                        Arc::new(StringArray::from(right))
                    } else {
                        Arc::new(StringViewArray::from(right))
                    };
                    inputs.push(operand(if case.scalar() { column } else { column.slice(3, rows) }, case.scalar()));
                } else {
                    let pattern = match case {
                        Case::LikeLiteralScalar | Case::LikeLiteralScalarDense => "arrow%",
                        Case::LikeLiteralViaPredicate | Case::LikeLiteralViaPredicateDense => "arrow",
                        Case::ILikeComplexScalar => "%ARROW_%",
                        Case::NotLikeScalar => "%rust",
                        Case::NotILikeScalar => "%RUST",
                        Case::RegexpScalar | Case::RegexpArray | Case::RegexpAlternatingArray => "^.*arrow.*$",
                        Case::LikeComplexArray | Case::LikeAlternatingArray => "%arrow_%",
                        _ => unreachable!("string pattern case"),
                    };
                    let patterns: Vec<_> = if case.scalar() {
                        vec![Some(pattern)]
                    } else {
                        (0..rows + 3).map(|index| {
                            let pattern = match case {
                                Case::LikeAlternatingArray if index.is_multiple_of(2) => "%Rust%",
                                Case::RegexpAlternatingArray if index.is_multiple_of(2) => "^.*Rust.*$",
                                _ => pattern,
                            };
                            (!index.is_multiple_of(11)).then_some(pattern)
                        }).collect()
                    };
                    let column: ArrayRef = Arc::new(StringViewArray::from(patterns));
                    inputs.push(operand(if case.scalar() { column } else { column.slice(3, rows) }, case.scalar()));
                }
            }
            inputs
        } else if case.is_float() {
            let values: Vec<_> = (0..rows + 3)
                .map(|index| (!index.is_multiple_of(8)).then_some((index % 1024) as f64 + 0.25))
                .collect();
            let left: ArrayRef = Arc::new(Float64Array::from(values));
            let mut inputs = vec![operand(left.slice(3, rows), false)];
            if !case.unary() {
                let right: Vec<_> = (0..rows + 3)
                    .map(|index| Some((index % 7) as f64 + 1.5))
                    .collect();
                let right: ArrayRef = Arc::new(Float64Array::from(right));
                inputs.push(operand(right.slice(3, rows), false));
            }
            inputs
        } else {
            let values: Vec<_> = (0..rows + 3)
                .map(|index| (!matches!(case, Case::CheckedSubNullable) || !index.is_multiple_of(8))
                    .then_some((index % 1024) as i64 + 1))
                .collect();
            let left: ArrayRef = Arc::new(Int64Array::from(values));
            let mut inputs = vec![operand(left.slice(3, rows), false)];
            if !case.unary() {
                let right: Vec<_> = (0..rows + 3)
                    .map(|index| Some((index % 7) as i64 + 1))
                    .collect();
                let right: ArrayRef = Arc::new(Int64Array::from(right));
                inputs.push(operand(right.slice(3, rows), false));
            }
            inputs
        };
        Self::finish(case, inputs, rows)
    }

    fn finish(case: Case, inputs: Vec<ArrowOperand>, rows: usize) -> Result<Self, ArrowError> {
        let fields: Vec<_> = inputs.iter().map(|input| input.dtype.clone()).collect();
        macro_rules! plan {
            ($function:expr) => {
                rowfn_arrow::plan(&$function, &NoOptions, &fields)?
            };
        }
        let output = match case {
            Case::CheckedSub | Case::CheckedSubNullable => plan!(Subtract::<true>),
            Case::WrappingSub => plan!(Subtract::<false>),
            Case::WrappingMultiply => plan!(MultiplyWrapping),
            Case::FloatAdd => plan!(FloatArithmetic::Add),
            Case::FloatSubtract => plan!(FloatArithmetic::Subtract),
            Case::FloatMultiply => plan!(FloatArithmetic::Multiply),
            Case::FloatDivide => plan!(FloatArithmetic::Divide),
            Case::FloatRemainder => plan!(FloatArithmetic::Remainder),
            Case::FloatNegate => plan!(FloatNegate),
            Case::Remainder => plan!(Remainder),
            Case::CheckedNegate => plan!(Negate::<true>),
            Case::WrappingNegate => plan!(Negate::<false>),
            Case::BitwiseAnd => plan!(BitwiseAnd),
            Case::BitwiseOr => plan!(BitwiseOr),
            Case::BitwiseXor => plan!(BitwiseXor),
            Case::BitwiseAndNot => plan!(BitwiseAndNot),
            Case::ShiftLeft => plan!(ShiftLeft),
            Case::ShiftRight => plan!(ShiftRight),
            Case::BitwiseNot => plan!(BitwiseNot),
            Case::Equal => plan!(Equal),
            Case::NotEqual => plan!(NotEqual),
            Case::LessThan => plan!(LessThan),
            Case::LessThanOrEqual => plan!(LessThanOrEqual),
            Case::GreaterThan => plan!(GreaterThan),
            Case::GreaterThanOrEqual => plan!(GreaterThanOrEqual),
            Case::FloatEqual => plan!(Equal),
            Case::FloatNotEqual => plan!(NotEqual),
            Case::FloatLessThan => plan!(LessThan),
            Case::FloatLessThanOrEqual => plan!(LessThanOrEqual),
            Case::FloatGreaterThan => plan!(GreaterThan),
            Case::FloatGreaterThanOrEqual => plan!(GreaterThanOrEqual),
            Case::LikeLiteralScalar | Case::LikeLiteralScalarDense
                | Case::LikeComplexArray | Case::LikeAlternatingArray => plan!(Like::<false, false>),
            Case::LikeLiteralViaPredicate | Case::LikeLiteralViaPredicateDense => plan!(StringPredicate::StartsWith),
            Case::ILikeComplexScalar => plan!(Like::<true, false>),
            Case::NotLikeScalar => plan!(Like::<false, true>),
            Case::NotILikeScalar => plan!(Like::<true, true>),
            Case::RegexpScalar | Case::RegexpArray | Case::RegexpAlternatingArray => plan!(RegexpIsMatch),
            Case::SubstringUtf8 => plan!(SubstringBytes::new(1, Some(4))),
            Case::SubstringCharsUtf8 => plan!(SubstringChars::new(1, Some(4))),
            Case::ByteLengthUtf8 | Case::ByteLengthLargeUtf8 | Case::ByteLengthView => plan!(ByteLength),
            Case::BitLengthUtf8 | Case::BitLengthLargeUtf8 | Case::BitLengthView => plan!(BitLength),
            Case::StringEqualUtf8 | Case::StringEqualLargeUtf8
                | Case::StringEqualViewShort | Case::StringEqualViewLong => plan!(StringEqual),
            Case::StringNotEqualView => plan!(StringNotEqual),
            Case::StringLessThanView | Case::StringLessThanViewScalar => plan!(StringLessThan),
            Case::StringLessThanOrEqualView => plan!(StringLessThanOrEqual),
            Case::StringGreaterThanView => plan!(StringGreaterThan),
            Case::StringGreaterThanOrEqualView => plan!(StringGreaterThanOrEqual),
        };
        let fixture = Self { case, inputs, output, rows };
        fixture.assert_equivalent()?;

        Ok(fixture)
    }

    fn invoke(&self) -> Result<ArrayRef, ArrowError> {
        macro_rules! invoke {
            ($function:expr) => {
                rowfn_arrow::invoke(&$function, &NoOptions, &self.inputs, self.rows, self.output.clone())?.array
            };
        }
        Ok(match self.case {
            Case::CheckedSub | Case::CheckedSubNullable => invoke!(Subtract::<true>),
            Case::WrappingSub => invoke!(Subtract::<false>),
            Case::WrappingMultiply => invoke!(MultiplyWrapping),
            Case::FloatAdd => invoke!(FloatArithmetic::Add),
            Case::FloatSubtract => invoke!(FloatArithmetic::Subtract),
            Case::FloatMultiply => invoke!(FloatArithmetic::Multiply),
            Case::FloatDivide => invoke!(FloatArithmetic::Divide),
            Case::FloatRemainder => invoke!(FloatArithmetic::Remainder),
            Case::FloatNegate => invoke!(FloatNegate),
            Case::Remainder => invoke!(Remainder),
            Case::CheckedNegate => invoke!(Negate::<true>),
            Case::WrappingNegate => invoke!(Negate::<false>),
            Case::BitwiseAnd => invoke!(BitwiseAnd),
            Case::BitwiseOr => invoke!(BitwiseOr),
            Case::BitwiseXor => invoke!(BitwiseXor),
            Case::BitwiseAndNot => invoke!(BitwiseAndNot),
            Case::ShiftLeft => invoke!(ShiftLeft),
            Case::ShiftRight => invoke!(ShiftRight),
            Case::BitwiseNot => invoke!(BitwiseNot),
            Case::Equal => invoke!(Equal),
            Case::NotEqual => invoke!(NotEqual),
            Case::LessThan => invoke!(LessThan),
            Case::LessThanOrEqual => invoke!(LessThanOrEqual),
            Case::GreaterThan => invoke!(GreaterThan),
            Case::GreaterThanOrEqual => invoke!(GreaterThanOrEqual),
            Case::FloatEqual => invoke!(Equal),
            Case::FloatNotEqual => invoke!(NotEqual),
            Case::FloatLessThan => invoke!(LessThan),
            Case::FloatLessThanOrEqual => invoke!(LessThanOrEqual),
            Case::FloatGreaterThan => invoke!(GreaterThan),
            Case::FloatGreaterThanOrEqual => invoke!(GreaterThanOrEqual),
            Case::LikeLiteralScalar | Case::LikeLiteralScalarDense
                | Case::LikeComplexArray | Case::LikeAlternatingArray => invoke!(Like::<false, false>),
            Case::LikeLiteralViaPredicate | Case::LikeLiteralViaPredicateDense => invoke!(StringPredicate::StartsWith),
            Case::ILikeComplexScalar => invoke!(Like::<true, false>),
            Case::NotLikeScalar => invoke!(Like::<false, true>),
            Case::NotILikeScalar => invoke!(Like::<true, true>),
            Case::RegexpScalar | Case::RegexpArray | Case::RegexpAlternatingArray => invoke!(RegexpIsMatch),
            Case::SubstringUtf8 => invoke!(SubstringBytes::new(1, Some(4))),
            Case::SubstringCharsUtf8 => invoke!(SubstringChars::new(1, Some(4))),
            Case::ByteLengthUtf8 | Case::ByteLengthLargeUtf8 | Case::ByteLengthView => invoke!(ByteLength),
            Case::BitLengthUtf8 | Case::BitLengthLargeUtf8 | Case::BitLengthView => invoke!(BitLength),
            Case::StringEqualUtf8 | Case::StringEqualLargeUtf8
                | Case::StringEqualViewShort | Case::StringEqualViewLong => invoke!(StringEqual),
            Case::StringNotEqualView => invoke!(StringNotEqual),
            Case::StringLessThanView | Case::StringLessThanViewScalar => invoke!(StringLessThan),
            Case::StringLessThanOrEqualView => invoke!(StringLessThanOrEqual),
            Case::StringGreaterThanView => invoke!(StringGreaterThan),
            Case::StringGreaterThanOrEqualView => invoke!(StringGreaterThanOrEqual),
        })
    }

    fn native(&self) -> Result<ArrayRef, ArrowError> {
        let left = &self.inputs[0].column;
        let right = self.inputs.get(1).map(|input| &input.column);
        let scalar;
        let rhs: Option<&dyn arrow_array::Datum> = match right {
            Some(right) if self.case.scalar() => {
                scalar = Scalar::new(right);
                Some(&scalar)
            }
            Some(right) => Some(right),
            None => None,
        };
        let integer = || left.as_any().downcast_ref::<Int64Array>()
            .vortex_expect("integer benchmark input");
        let integer_rhs = || right.vortex_expect("binary integer input")
            .as_any().downcast_ref::<Int64Array>().vortex_expect("integer benchmark rhs");
        match self.case {
            Case::CheckedSub | Case::CheckedSubNullable =>
                arrow_arith::numeric::sub(left, rhs.vortex_expect("binary operation")),
            Case::WrappingSub =>
                arrow_arith::numeric::sub_wrapping(left, rhs.vortex_expect("binary operation")),
            Case::WrappingMultiply =>
                arrow_arith::numeric::mul_wrapping(left, rhs.vortex_expect("binary operation")),
            Case::FloatAdd => arrow_arith::numeric::add(left, rhs.vortex_expect("binary operation")),
            Case::FloatSubtract => arrow_arith::numeric::sub(left, rhs.vortex_expect("binary operation")),
            Case::FloatMultiply => arrow_arith::numeric::mul(left, rhs.vortex_expect("binary operation")),
            Case::FloatDivide => arrow_arith::numeric::div(left, rhs.vortex_expect("binary operation")),
            Case::FloatRemainder => arrow_arith::numeric::rem(left, rhs.vortex_expect("binary operation")),
            Case::FloatNegate => arrow_arith::numeric::neg(left),
            Case::Remainder =>
                arrow_arith::numeric::rem(left, rhs.vortex_expect("binary operation")),
            Case::CheckedNegate => arrow_arith::numeric::neg(left),
            Case::WrappingNegate => arrow_arith::numeric::neg_wrapping(left),
            Case::BitwiseAnd => boolean(arrow_arith::bitwise::bitwise_and(integer(), integer_rhs())?),
            Case::BitwiseOr => boolean(arrow_arith::bitwise::bitwise_or(integer(), integer_rhs())?),
            Case::BitwiseXor => boolean(arrow_arith::bitwise::bitwise_xor(integer(), integer_rhs())?),
            Case::BitwiseAndNot => boolean(arrow_arith::bitwise::bitwise_and_not(integer(), integer_rhs())?),
            Case::ShiftLeft => boolean(arrow_arith::bitwise::bitwise_shift_left(integer(), integer_rhs())?),
            Case::ShiftRight => boolean(arrow_arith::bitwise::bitwise_shift_right(integer(), integer_rhs())?),
            Case::BitwiseNot => boolean(arrow_arith::bitwise::bitwise_not(integer())?),
            Case::Equal => boolean(arrow_ord::cmp::eq(left, rhs.vortex_expect("binary operation"))?),
            Case::NotEqual => boolean(arrow_ord::cmp::neq(left, rhs.vortex_expect("binary operation"))?),
            Case::LessThan => boolean(arrow_ord::cmp::lt(left, rhs.vortex_expect("binary operation"))?),
            Case::LessThanOrEqual => boolean(arrow_ord::cmp::lt_eq(left, rhs.vortex_expect("binary operation"))?),
            Case::GreaterThan => boolean(arrow_ord::cmp::gt(left, rhs.vortex_expect("binary operation"))?),
            Case::GreaterThanOrEqual => boolean(arrow_ord::cmp::gt_eq(left, rhs.vortex_expect("binary operation"))?),
            Case::FloatEqual => boolean(arrow_ord::cmp::eq(left, rhs.vortex_expect("binary operation"))?),
            Case::FloatNotEqual => boolean(arrow_ord::cmp::neq(left, rhs.vortex_expect("binary operation"))?),
            Case::FloatLessThan => boolean(arrow_ord::cmp::lt(left, rhs.vortex_expect("binary operation"))?),
            Case::FloatLessThanOrEqual => boolean(arrow_ord::cmp::lt_eq(left, rhs.vortex_expect("binary operation"))?),
            Case::FloatGreaterThan => boolean(arrow_ord::cmp::gt(left, rhs.vortex_expect("binary operation"))?),
            Case::FloatGreaterThanOrEqual => boolean(arrow_ord::cmp::gt_eq(left, rhs.vortex_expect("binary operation"))?),
            Case::LikeLiteralScalar | Case::LikeLiteralScalarDense
                | Case::LikeComplexArray | Case::LikeAlternatingArray =>
                boolean(arrow_string::like::like(left, rhs.vortex_expect("binary operation"))?),
            Case::LikeLiteralViaPredicate | Case::LikeLiteralViaPredicateDense =>
                boolean(arrow_string::like::starts_with(left, rhs.vortex_expect("binary operation"))?),
            Case::ILikeComplexScalar =>
                boolean(arrow_string::like::ilike(left, rhs.vortex_expect("binary operation"))?),
            Case::NotLikeScalar =>
                boolean(arrow_string::like::nlike(left, rhs.vortex_expect("binary operation"))?),
            Case::NotILikeScalar =>
                boolean(arrow_string::like::nilike(left, rhs.vortex_expect("binary operation"))?),
            Case::RegexpScalar | Case::RegexpArray | Case::RegexpAlternatingArray => {
                let left = left.as_any().downcast_ref::<StringViewArray>()
                    .vortex_expect("string-view benchmark input");
                let right = right.vortex_expect("regex pattern input")
                    .as_any().downcast_ref::<StringViewArray>()
                    .vortex_expect("string-view regex pattern");
                if self.case.scalar() {
                    boolean(arrow_string::regexp::regexp_is_match_scalar(left, right.value(0), None)?)
                } else {
                    boolean(arrow_string::regexp::regexp_is_match(
                        left, right, None::<&StringViewArray>,
                    )?)
                }
            }
            Case::SubstringUtf8 => arrow_string::substring::substring(left, 1, Some(4)),
            Case::SubstringCharsUtf8 => boolean(arrow_string::substring::substring_by_char(
                left.as_any().downcast_ref::<StringArray>().vortex_expect("UTF-8 input"),
                1,
                Some(4),
            )?),
            Case::ByteLengthUtf8 | Case::ByteLengthLargeUtf8 | Case::ByteLengthView =>
                arrow_string::length::length(left.as_ref()),
            Case::BitLengthUtf8 | Case::BitLengthLargeUtf8 | Case::BitLengthView =>
                arrow_string::length::bit_length(left.as_ref()),
            Case::StringEqualUtf8 | Case::StringEqualLargeUtf8
                | Case::StringEqualViewShort | Case::StringEqualViewLong =>
                boolean(arrow_ord::cmp::eq(left, rhs.vortex_expect("string comparison rhs"))?),
            Case::StringNotEqualView =>
                boolean(arrow_ord::cmp::neq(left, rhs.vortex_expect("string comparison rhs"))?),
            Case::StringLessThanView | Case::StringLessThanViewScalar =>
                boolean(arrow_ord::cmp::lt(left, rhs.vortex_expect("string comparison rhs"))?),
            Case::StringLessThanOrEqualView =>
                boolean(arrow_ord::cmp::lt_eq(left, rhs.vortex_expect("string comparison rhs"))?),
            Case::StringGreaterThanView =>
                boolean(arrow_ord::cmp::gt(left, rhs.vortex_expect("string comparison rhs"))?),
            Case::StringGreaterThanOrEqualView =>
                boolean(arrow_ord::cmp::gt_eq(left, rhs.vortex_expect("string comparison rhs"))?),
        }
    }

    fn assert_equivalent(&self) -> Result<(), ArrowError> {
        let native = self.native()?;
        let rowfn = self.invoke()?;
        if matches!(self.case, Case::SubstringUtf8 | Case::SubstringCharsUtf8) {
            let native = native.as_any().downcast_ref::<StringArray>()
                .vortex_expect("Arrow substring output");
            let rowfn = rowfn.as_any().downcast_ref::<StringViewArray>()
                .vortex_expect("RowFn substring output");
            assert_eq!(native.iter().collect::<Vec<_>>(), rowfn.iter().collect::<Vec<_>>(),
                "{:?}, {} rows", self.case, self.rows);
        } else {
            assert_eq!(native.to_data(), rowfn.to_data(), "{:?}, {} rows", self.case, self.rows);
        }

        Ok(())
    }
}

#[divan::bench(args = CASES, consts = SIZES)]
fn arrow_native<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| black_box(&fixture).native().vortex_expect("validated Arrow kernel"));
}

#[divan::bench(args = CASES, consts = SIZES)]
fn arrow_rowfn<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| black_box(&fixture).invoke().vortex_expect("validated RowFn invocation"));
}

fn main() {
    divan::main();
}
