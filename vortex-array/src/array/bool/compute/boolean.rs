use arrow_arith::boolean;
use arrow_array::cast::AsArray;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::arrow::FromArrowArray;
use crate::compute::{AndFn, OrFn};
use crate::{Array, ArrayData, IntoArray, IntoCanonical};

impl OrFn for BoolArray {
    fn or(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::or(lhs, rhs)?;

        Ok(ArrayData::from_arrow(&array, true).into_array())
    }
}

impl AndFn for BoolArray {
    fn and(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::and(lhs, rhs)?;

        Ok(ArrayData::from_arrow(&array, true).into_array())
    }
}
