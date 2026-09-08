// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use num_traits::CheckedAdd;
use num_traits::Zero;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::Chunked;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::dtype::BigCast;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::DecimalType;
use vortex_array::dtype::MAX_PRECISION;
use vortex_array::dtype::Nullability::Nullable;
use vortex_array::dtype::i256;
use vortex_array::match_each_decimal_value_type;
use vortex_array::match_each_integer_ptype;
use vortex_array::scalar::DecimalValue;
use vortex_array::scalar::Scalar;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

#[cfg(test)]
mod tests;

/// Reference sum of chunked or canonical arrays using native arithmetic.
/// Checks overflow after each group, preserving chunk boundaries.
/// Returns `None` only for floats, whose rounding depends on addition order.
pub fn sum_canonical_array(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<Scalar>> {
    Ok(Some(match array.dtype() {
        DType::Bool(_) => Scalar::from(accumulate(array, Some(0u64), &|_| true, ctx)?),
        DType::Primitive(ptype, _) if ptype.is_unsigned_int() => {
            Scalar::from(accumulate(array, Some(0u64), &|_| true, ctx)?)
        }
        DType::Primitive(ptype, _) if ptype.is_signed_int() => {
            Scalar::from(accumulate(array, Some(0i64), &|_| true, ctx)?)
        }
        DType::Primitive(..) => return Ok(None),
        DType::Decimal(input_dtype, _) => {
            let output_dtype = DecimalDType::new(
                (input_dtype.precision() + 10).min(MAX_PRECISION),
                input_dtype.scale(),
            );
            let limit = i256::from_i128(10)
                .checked_pow(output_dtype.precision().into())
                .vortex_expect("10^76 fits in i256");
            let values_type = DecimalType::smallest_decimal_value_type(&output_dtype);
            match_each_decimal_value_type!(values_type, |I| {
                let limit = <I as BigCast>::from(limit)
                    .vortex_expect("precision limit fits native accumulator");
                match accumulate(
                    array,
                    Some(I::zero()),
                    &|&value| -limit < value && value < limit,
                    ctx,
                )? {
                    Some(value) => {
                        Scalar::decimal(DecimalValue::from(value), output_dtype, Nullable)
                    }
                    None => Scalar::null(DType::Decimal(output_dtype, Nullable)),
                }
            })
        }
        _ => vortex_bail!("Unsupported sum dtype: {}", array.dtype()),
    }))
}

fn accumulate<T>(
    array: &ArrayRef,
    initial: Option<T>,
    fits: &impl Fn(&T) -> bool,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<T>>
where
    T: BigCast + Copy + Zero + CheckedAdd,
{
    let Some(initial) = initial else {
        return Ok(None);
    };
    if array.is_empty() {
        return Ok(Some(initial));
    }

    if let Some(chunked) = array.as_opt::<Chunked>() {
        // A nested chunked array has its own partial; canonical chunks share its running total.
        let mut partial = Some(T::zero());
        for chunk in chunked.non_empty_chunks() {
            partial = accumulate(chunk, partial, fits, ctx)?;
        }
        Ok(partial
            .and_then(|partial| initial.checked_add(&partial))
            .filter(fits))
    } else {
        // Canonical batches share the parent's running total. Native overflow is checked on
        // each addition, while decimal precision is checked only at the end of this group.
        Ok(native_values::<T>(array, ctx)?
            .into_iter()
            .try_fold(initial, |sum, value| sum.checked_add(&value))
            .filter(fits))
    }
}

fn native_values<T: BigCast>(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Vec<T>> {
    let canonical = array.clone().execute::<Canonical>(ctx)?;
    let valid = canonical
        .clone()
        .into_array()
        .validity()?
        .execute_mask(canonical.len(), ctx)?
        .to_bit_buffer();
    Ok(match canonical {
        Canonical::Bool(array) => vec![
            T::from((array.to_bit_buffer() & valid).true_count() as u64)
                .vortex_expect("boolean count fits accumulator"),
        ],
        Canonical::Primitive(array) => match_each_integer_ptype!(array.ptype(), |P| {
            array
                .as_slice::<P>()
                .iter()
                .zip(valid.iter())
                .filter_map(|(&value, valid)| valid.then_some(value))
                .map(|value| T::from(value).vortex_expect("integer value fits accumulator"))
                .collect()
        }),
        Canonical::Decimal(array) => match_each_decimal_value_type!(array.values_type(), |D| {
            array
                .buffer::<D>()
                .iter()
                .zip(valid.iter())
                .filter_map(|(&value, valid)| valid.then_some(value))
                .map(|value| T::from(value).vortex_expect("decimal value fits accumulator"))
                .collect()
        }),
        _ => vortex_bail!("Unsupported sum dtype: {}", array.dtype()),
    })
}
