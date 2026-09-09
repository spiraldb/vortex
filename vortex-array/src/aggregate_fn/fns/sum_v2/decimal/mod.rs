// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use itertools::Itertools;
use num_traits::AsPrimitive;
use num_traits::CheckedAdd;
use num_traits::CheckedMul;
use num_traits::NumOps;
use vortex_buffer::BitBuffer;
use vortex_buffer::Buffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_mask::Mask;

use super::SumState;
use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::arrays::ConstantArray;
use crate::arrays::DecimalArray;
use crate::builtins::ArrayBuiltins;
use crate::dtype::DType;
use crate::dtype::DecimalDType;
use crate::dtype::DecimalType;
use crate::dtype::FieldNames;
use crate::dtype::NativeDecimalType;
use crate::dtype::Nullability;
use crate::dtype::StructFields;
use crate::dtype::i256;
use crate::match_each_decimal_value_type;
use crate::scalar::DecimalValue;
use crate::scalar::Scalar;
use crate::scalar_fn::fns::operators::Operator;

const VALUE_FIELD: &str = "value";
const CARRY_FIELD: &str = "carry";

fn carry_dtype() -> DecimalDType {
    DecimalDType::new(38, 0)
}

pub(super) fn decimal_partial_dtype(dtype: DType) -> DType {
    let DType::Decimal(..) = dtype else {
        return dtype;
    };
    DType::Struct(
        StructFields::new(
            FieldNames::from_iter([VALUE_FIELD, CARRY_FIELD]),
            vec![
                dtype.as_nonnullable(),
                DType::Decimal(carry_dtype(), Nullability::NonNullable),
            ],
        ),
        dtype.nullability(),
    )
}

pub(super) fn decimal_partial_scalar(
    value: DecimalValue,
    dtype: DecimalDType,
    nullability: Nullability,
) -> Scalar {
    let value = value.cast::<i256>().vortex_expect("decimal fits i256");
    let limit = i256::from_i128(10).wrapping_pow(dtype.precision().into());
    // Decimal scalars must fit their precision, including in partial states. Preserve excess
    // separately so later partials can cancel it. The largest carry is at precision 39,
    // where an i256 accumulator's quotient has at most 38 digits.
    Scalar::struct_(
        decimal_partial_dtype(DType::Decimal(dtype, nullability)),
        [
            Scalar::decimal(
                DecimalValue::I256(value % limit),
                dtype,
                Nullability::NonNullable,
            ),
            Scalar::decimal(
                DecimalValue::I256(value / limit),
                carry_dtype(),
                Nullability::NonNullable,
            ),
        ],
    )
}

pub(super) fn decimal_partial_value(
    partial: &Scalar,
    dtype: DecimalDType,
) -> VortexResult<DecimalValue> {
    let fields = partial.as_struct();
    let value = fields
        .field(VALUE_FIELD)
        .ok_or_else(|| vortex_err!("Decimal sum partial is missing value"))?;
    let carry = fields
        .field(CARRY_FIELD)
        .ok_or_else(|| vortex_err!("Decimal sum partial is missing carry"))?;
    let value = DecimalValue::try_from(&value)?
        .cast::<i256>()
        .vortex_expect("decimal fits i256");
    let carry = DecimalValue::try_from(&carry)?
        .cast::<i256>()
        .vortex_expect("decimal fits i256");
    let limit = i256::from_i128(10).wrapping_pow(dtype.precision().into());
    let value = carry
        .checked_mul(&limit)
        .and_then(|carry| carry.checked_add(&value))
        .map(DecimalValue::I256)
        .ok_or_else(|| vortex_err!("Decimal sum partial exceeds its native accumulator"))?;
    match_each_decimal_value_type!(DecimalType::smallest_decimal_value_type(&dtype), |I| {
        value
            .cast::<I>()
            .map(DecimalValue::from)
            .ok_or_else(|| vortex_err!("Decimal sum partial exceeds its native accumulator"))
    })
}

pub(super) fn add_decimal(
    value: &mut DecimalValue,
    other: DecimalValue,
    dtype: DecimalDType,
) -> bool {
    match_each_decimal_value_type!(DecimalType::smallest_decimal_value_type(&dtype), |I| {
        let lhs: I = value
            .cast()
            .vortex_expect("partial fits native accumulator");
        let rhs: I = other
            .cast()
            .vortex_expect("partial fits native accumulator");
        match CheckedAdd::checked_add(&lhs, &rhs) {
            Some(sum) => {
                *value = DecimalValue::from(sum);
                false
            }
            None => true,
        }
    })
}

pub(super) fn multiply_decimal(
    value: DecimalValue,
    len: usize,
    dtype: DecimalDType,
) -> Option<DecimalValue> {
    let value = value.cast::<i256>().vortex_expect("decimal fits i256");
    let product = DecimalValue::I256(value.checked_mul(&i256::from_i128(len as i128))?);
    match_each_decimal_value_type!(DecimalType::smallest_decimal_value_type(&dtype), |I| {
        product.cast::<I>().map(DecimalValue::from)
    })
}

pub(super) fn finalize_decimal(partials: ArrayRef) -> VortexResult<ArrayRef> {
    if !matches!(partials.dtype(), DType::Struct(..)) {
        return Ok(partials);
    }
    let value = partials.get_item(VALUE_FIELD)?;
    let carry = partials.get_item(CARRY_FIELD)?;
    let zero = ConstantArray::new(
        Scalar::decimal(
            DecimalValue::I128(0),
            carry_dtype(),
            Nullability::NonNullable,
        ),
        partials.len(),
    )
    .into_array();
    value.mask(carry.binary(zero, Operator::Eq)?)
}

/// Accumulate a decimal array into the sum state.
/// Native addition is checked; precision is checked when the aggregate is finalized.
pub(super) fn accumulate_decimal(
    inner: &mut SumState,
    d: &DecimalArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    let mask = d.as_ref().validity()?.execute_mask(d.as_ref().len(), ctx)?;
    let validity = match &mask {
        Mask::AllTrue(_) => None,
        Mask::Values(mask_values) => Some(mask_values.bit_buffer()),
        Mask::AllFalse(_) => {
            return Ok(false);
        }
    };

    let SumState::Decimal { value, dtype } = inner else {
        vortex_panic!("expected decimal sum state for decimal input");
    };

    let values_type = DecimalType::smallest_decimal_value_type(dtype);
    let sum = match_each_decimal_value_type!(d.values_type(), |T| {
        match_each_decimal_value_type!(values_type, |I| {
            let initial: I = value
                .cast()
                .vortex_expect("cannot fail to cast initial value");
            sum_decimal_value(initial, d.buffer::<T>(), validity)
        })
    });
    Ok(match sum {
        Some(sum) => {
            *value = sum;
            false
        }
        None => true,
    })
}

fn sum_decimal_value<T, I>(
    initial: I,
    values: Buffer<T>,
    validity: Option<&BitBuffer>,
) -> Option<DecimalValue>
where
    T: AsPrimitive<I>,
    I: NumOps + CheckedAdd + Copy + NativeDecimalType + 'static,
    bool: AsPrimitive<I>,
    DecimalValue: From<I>,
{
    let sum = match validity {
        Some(v) => sum_decimal_with_validity(values, v, initial),
        None => sum_decimal(values, initial),
    };

    sum.map(DecimalValue::from)
}

fn sum_decimal<T: AsPrimitive<I>, I: Copy + CheckedAdd + 'static>(
    values: Buffer<T>,
    initial: I,
) -> Option<I> {
    let mut sum = initial;
    for v in values.iter() {
        let v: I = v.as_();
        sum = sum.checked_add(&v)?;
    }
    Some(sum)
}

fn sum_decimal_with_validity<T, I>(values: Buffer<T>, validity: &BitBuffer, initial: I) -> Option<I>
where
    T: AsPrimitive<I>,
    I: NumOps + CheckedAdd + Copy + 'static,
    bool: AsPrimitive<I>,
{
    let mut sum = initial;
    for (v, valid) in values.iter().zip_eq(validity) {
        let v: I = v.as_() * valid.as_();

        sum = sum.checked_add(&v)?;
    }
    Some(sum)
}

#[cfg(test)]
mod tests;
