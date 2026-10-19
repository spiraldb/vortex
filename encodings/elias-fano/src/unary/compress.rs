// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use num_traits::AsPrimitive;
use vortex_array::ArrayView;
use vortex_array::arrays::Primitive;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability::NonNullable;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::validity::Validity;
use vortex_buffer::BitBuffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;

use super::array::Unary;
use super::array::UnaryArray;
use super::array::bit_len;

/// Encode `array` as a [`Unary`] array.
///
/// Returns `None` when the values cannot, or should not, be stored this way:
/// - the dtype is not a non-nullable unsigned integer;
/// - a value is below the one before it;
/// - the encoding would take more bits than the plain array's `len × bit width`. This keeps sparse
///   values from growing past their input, and bounds what the encoder allocates.
pub fn unary_encode(array: ArrayView<'_, Primitive>) -> VortexResult<Option<UnaryArray>> {
    let &DType::Primitive(ptype, NonNullable) = array.dtype() else {
        return Ok(None);
    };
    if !ptype.is_unsigned_int() {
        return Ok(None);
    }
    let len = array.len();
    if len == 0 {
        return Unary::try_new(BitBuffer::empty(), 0, 0, ptype, 0).map(Some);
    }

    match_each_unsigned_integer_ptype!(ptype, |T| {
        let values = array.as_slice::<T>();
        if !values.is_sorted() {
            return Ok(None);
        }
        let base: u64 = values[0].as_();
        let max: u64 = values[len - 1].as_();
        let Some(bits) = bit_len(len, max - base) else {
            return Ok(None);
        };
        if len
            .checked_mul(ptype.bit_width())
            .is_none_or(|plain| bits > plain)
        {
            return Ok(None);
        }

        // Each position is below `bits`, which fits a `usize`, so the conversion is exact.
        let positions = values.iter().enumerate().map(|(index, &value)| {
            AsPrimitive::<usize>::as_(AsPrimitive::<u64>::as_(value) - base) + index
        });
        Unary::try_new(
            BitBuffer::from_indices(bits, positions),
            base,
            max,
            ptype,
            len,
        )
        .map(Some)
    })
}

/// Decode every value with one walk over the set bits.
pub(super) fn unary_decode(array: &UnaryArray) -> PrimitiveArray {
    let len = array.len();
    let bits = array.bits();
    let base = array.base();

    // `validate` counted exactly `len` set bits, which keeps every value within `[base, max]`, so
    // the loop has nothing to check.
    match_each_unsigned_integer_ptype!(array.dtype().as_ptype(), |T| {
        let mut values = BufferMut::<T>::with_capacity(len);
        let mut index = 0;
        bits.for_each_set_index(|position| {
            let value = base + AsPrimitive::<u64>::as_(position - index);
            values.push(value.as_());
            index += 1;
        });
        PrimitiveArray::new(values, Validity::NonNullable)
    })
}
