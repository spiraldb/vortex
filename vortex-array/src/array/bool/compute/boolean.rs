use arrow_arith::boolean;
use arrow_array::cast::AsArray as _;
use arrow_array::{Array as _, BooleanArray};
use arrow_buffer::{buffer_bin_and_not, BooleanBuffer};
use vortex_error::VortexResult;

use crate::array::BoolArray;
use crate::arrow::FromArrowArray as _;
use crate::compute::{AndFn, OrFn};
use crate::{Array, IntoCanonical};

impl OrFn for BoolArray {
    fn or(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::or(lhs, rhs)?;

        null_as_false(&array)
    }
}

impl AndFn for BoolArray {
    fn and(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::and(lhs, rhs)?;

        null_as_false(&array)
    }
}

/// Mask all null values of a Arrow boolean array to false
fn null_as_false(array: &BooleanArray) -> VortexResult<Array> {
    let inner_bool_buffer = array.values();

    match array.nulls() {
        None => Ok(Array::from_arrow(array, false)),
        Some(nulls) => {
            let buff = buffer_bin_and_not(
                inner_bool_buffer.inner(),
                inner_bool_buffer.offset(),
                nulls.buffer(),
                nulls.offset(),
                inner_bool_buffer.len(),
            );
            let bool_buffer =
                BooleanBuffer::new(buff, inner_bool_buffer.offset(), inner_bool_buffer.len());
            let arr = BooleanArray::from(bool_buffer);
            Ok(Array::from_arrow(&arr, false))
        }
    }
}
