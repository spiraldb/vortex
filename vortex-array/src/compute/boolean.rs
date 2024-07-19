use arrow_array::cast::AsArray;
use vortex_error::VortexResult;

use crate::{arrow::FromArrowArray, Array, ArrayData, IntoArray, IntoArrayVariant, IntoCanonical};

pub trait AndFn {
    fn and(&self, array: &Array) -> VortexResult<Array>;
}

pub trait OrFn {
    fn or(&self, array: &Array) -> VortexResult<Array>;
}

pub fn and(lhs: &Array, rhs: &Array) -> VortexResult<Array> {
    if let Some(selection) = lhs.with_dyn(|lhs| lhs.and().map(|lhs| lhs.and(rhs))) {
        return selection;
    }

    let lhs = lhs.clone().into_bool()?.into_canonical()?.into_arrow();
    let lhs_bool = lhs.as_boolean();
    let rhs = rhs.clone().into_bool()?.into_canonical()?.into_arrow();
    let rhs_bool = rhs.as_boolean();

    let data =
        ArrayData::from_arrow(&arrow_arith::boolean::and(lhs_bool, rhs_bool)?, true).into_array();

    Ok(data)
}

pub fn or(lhs: &Array, rhs: &Array) -> VortexResult<Array> {
    if let Some(selection) = lhs.with_dyn(|lhs| lhs.and().map(|lhs| lhs.and(rhs))) {
        return selection;
    }

    let lhs = lhs.clone().into_bool()?.into_canonical()?.into_arrow();
    let lhs_bool = lhs.as_boolean();
    let rhs = rhs.clone().into_bool()?.into_canonical()?.into_arrow();
    let rhs_bool = rhs.as_boolean();

    let data =
        ArrayData::from_arrow(&arrow_arith::boolean::or(lhs_bool, rhs_bool)?, true).into_array();

    Ok(data)
}
