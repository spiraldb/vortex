use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_expr::Operator;

use crate::{Array, ArrayDType, IntoArrayVariant};

pub trait CompareFn {
    fn compare(&self, array: &Array, predicate: Operator) -> VortexResult<Array>;
}

pub fn compare(left: &Array, right: &Array, predicate: Operator) -> VortexResult<Array> {
    if let Some(matching_indices) =
        left.with_dyn(|lhs| lhs.compare().map(|rhs| rhs.compare(right, predicate)))
    {
        return matching_indices;
    }

    // if compare is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply filter to the flattened primitive array
    match left.dtype() {
        DType::Primitive(..) => {
            let flat = left.clone().into_primitive()?;
            flat.compare(right, predicate)
        }
        _ => Err(vortex_err!(
            NotImplemented: "compare",
            left.encoding().id()
        )),
    }
}
