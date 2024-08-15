use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};

use crate::compute::expr::VortexExpr;
use crate::{Array, ArrayDType, IntoArrayVariant};

pub trait FindFn {
    fn find(&self, predicate: &dyn VortexExpr) -> VortexResult<Array>;
}

pub fn find(array: &Array, predicate: &dyn VortexExpr) -> VortexResult<Array> {
    if let Some(matching_indices) = array.with_dyn(|c| c.find().map(|t| t.find(predicate))) {
        return matching_indices;
    }
    // if filter is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply filter to the flattened primitive array
    match array.dtype() {
        DType::Primitive(..) => {
            let flat = array.clone().into_primitive()?;
            flat.find(predicate)
        }
        _ => Err(vortex_err!(
            NotImplemented: "filter_indices",
            array.encoding().id()
        )),
    }
}
