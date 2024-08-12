use arrow_arith::boolean;
use arrow_array::cast::AsArray as _;
use arrow_array::BooleanArray;
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

        let not_null = BooleanArray::from(
            array
                .iter()
                .map(|v| v.unwrap_or_default())
                .collect::<Vec<_>>(),
        );

        Ok(Array::from_arrow(&not_null, false))
    }
}

impl AndFn for BoolArray {
    fn and(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::and(lhs, rhs)?;

        let not_null = BooleanArray::from(
            array
                .iter()
                .map(|v| v.unwrap_or_default())
                .collect::<Vec<_>>(),
        );

        Ok(Array::from_arrow(&not_null, false))
    }
}
