use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_expr::Disjunction;

use crate::{Array, ArrayDType, IntoArrayVariant};

pub trait FilterIndicesFn {
    fn filter_indices(&self, predicate: &Disjunction) -> VortexResult<Array>;
}

pub fn filter_indices(array: &Array, predicate: &Disjunction) -> VortexResult<Array> {
    if let Some(matching_indices) =
        array.with_dyn(|c| c.filter_indices().map(|t| t.filter_indices(predicate)))
    {
        return matching_indices;
    }
    // if filter is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply filter to the flattened primitive array
    match array.dtype() {
        DType::Primitive(..) => {
            let flat = array.clone().into_primitive()?;
            flat.filter_indices(predicate)
        }
        _ => Err(vortex_err!(
            NotImplemented: "filter_indices",
            array.encoding().id()
        )),
    }
}
