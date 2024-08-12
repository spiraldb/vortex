use arrow_arith::boolean;
use arrow_array::cast::AsArray as _;
use vortex_error::VortexResult;

use crate::array::BoolArray;
use crate::compute::{AndFn, OrFn};
use crate::{Array, IntoArray, IntoCanonical};

impl OrFn for BoolArray {
    fn or(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::or(lhs, rhs)?;
        let not_null = BoolArray::from_iter(array.iter().map(|v| Some(v.unwrap_or_default())));

        Ok(not_null.into_array())
    }
}

impl AndFn for BoolArray {
    fn and(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let lhs = lhs.as_boolean();

        let rhs = array.clone().into_canonical()?.into_arrow();
        let rhs = rhs.as_boolean();

        let array = boolean::and(lhs, rhs)?;
        let not_null = BoolArray::from_iter(array.iter().map(|v| Some(v.unwrap_or_default())));

        Ok(not_null.into_array())
    }
}
