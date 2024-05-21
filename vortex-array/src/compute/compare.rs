use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_expr::operators::Operator;

use crate::{Array, ArrayDType};

pub trait CompareFn {
    fn compare(&self, array: &Array, predicate: Operator) -> VortexResult<Array>;
}

pub fn compare(array: &Array, other: &Array, predicate: Operator) -> VortexResult<Array> {
    if let Some(matching_indices) =
        array.with_dyn(|c| c.compare().map(|t| t.compare(other, predicate)))
    {
        return matching_indices;
    }
    // if compare is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply filter to the flattened primitive array
    match array.dtype() {
        DType::Primitive(..) => {
            let flat = array.clone().flatten_primitive()?;
            flat.compare(other, predicate)
        }
        _ => Err(vortex_err!(
            NotImplemented: "compare",
            array.encoding().id()
        )),
    }
}
